//! One open database: activation, commits, reads, checkpoints and collection.
//!
//! This file implements `spec/DurableDb.tla`. The names match: a writer
//! *claims* an epoch, *revokes* lower claims, reads the newest manifest and
//! the frames after it, and publishes its own manifest. From then on every
//! acknowledgement and every deletion waits for a claim check that began
//! after the bytes it depends on were stored.
use crate::{
    Error, Result,
    io::{Io, Reader, retry},
    layout::{
        Claim, ConfigChange, DbConfig, Epoch, FORMAT, Frame, FrameTxn, INTERNAL, Layout, Manifest,
        is_receipt, receipt_key,
    },
    mem::Mem,
    store::{StoreError, list_all, list_from},
    table::{Entry, Table, TableBuilder, TableMeta, TableScan},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc, oneshot};

/// Tuning that applies to every database on a node.
#[derive(Debug, Clone)]
pub struct Options {
    /// Address written into claims so peers can forward to the owner.
    pub node: String,
    /// Flush the in-memory table to an immutable table at this size.
    pub flush_bytes: usize,
    /// Flush after this many frames, bounding what a takeover replays.
    pub flush_frames: u64,
    /// Flush after this long without a commit.
    pub idle_flush: Duration,
    pub max_frame_bytes: usize,
    /// Frames that may be uploading at once. Acknowledgement is in order,
    /// so with a long-tailed store a deep pipeline makes every commit wait
    /// for the slowest of many uploads; batching, not depth, gives throughput.
    pub max_inflight_frames: usize,
    /// Claim checks that may be in flight at once.
    pub max_checks: usize,
    /// Identical uploads that may race for one small object. 1 disables racing.
    pub upload_attempts: usize,
    /// How many of them start at once.
    pub upload_eager: usize,
    /// Delay before each further attempt starts while none has finished.
    pub hedge_after: Duration,
    /// How long a commit may wait on storage before the writer gives up.
    pub store_deadline: Duration,
    /// Sweep lower epochs' leftovers again this long after taking ownership.
    pub resweep_after: Duration,
    /// Retries are recognized for this many revisions.
    pub receipt_window: u64,
    pub block_bytes: usize,
    /// A merge starts a new table when the one it is writing reaches this
    /// size, so that it holds this much in memory whatever the size of the
    /// database.
    pub table_bytes: usize,
    /// Keys remembered for range checks, and the bytes they may take.
    pub recent_keys: usize,
    pub recent_bytes: usize,
    /// The least a database waits after a merge of everything that its
    /// quota asked for and that made little room, before the next.
    pub quota_rest: Duration,
    /// A read looks in every run of tables. A database with more runs than
    /// this merges the newest that are too many, and only then does a
    /// merge give way to merges of what is flushed behind it; with more
    /// than twice as many, writes wait while it merges. One at least.
    pub max_runs: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            node: String::new(),
            flush_bytes: 8 << 20,
            flush_frames: 512,
            idle_flush: Duration::from_secs(5),
            max_frame_bytes: 4 << 20,
            max_inflight_frames: 2,
            max_checks: 2,
            upload_attempts: 1,
            upload_eager: 1,
            hedge_after: Duration::from_millis(150),
            store_deadline: Duration::from_secs(30),
            resweep_after: Duration::from_secs(30),
            receipt_window: 100_000,
            block_bytes: 32 << 10,
            table_bytes: 32 << 20,
            recent_keys: 100_000,
            recent_bytes: 8 << 20,
            quota_rest: Duration::from_secs(1),
            max_runs: 12,
        }
    }
}

pub struct Ctx {
    pub io: Arc<Io>,
    pub opts: Options,
    /// Prefix of every object key: empty or ending in `/`.
    pub root: String,
}

#[derive(Debug, Clone)]
pub enum Check {
    /// The key's current version must equal `version` (0 means absent).
    Key { key: Bytes, version: u64 },
    /// No key in `lo..hi` may have been written after revision `since`.
    Range { lo: Bytes, hi: Bytes, since: u64 },
}

#[derive(Debug, Clone, Default)]
pub struct Txn {
    pub checks: Vec<Check>,
    /// `None` deletes the key.
    pub writes: Vec<(Bytes, Option<Bytes>)>,
    /// The whole database must still be at this revision.
    pub if_revision: Option<u64>,
    pub idempotency_key: Option<String>,
}

impl Txn {
    /// Identifies the request body, so a reused idempotency key is caught.
    fn digest(&self) -> Bytes {
        let mut hash = Sha256::new();
        for (key, value) in &self.writes {
            hash.update((key.len() as u64).to_le_bytes());
            hash.update(key);
            match value {
                Some(value) => {
                    hash.update((value.len() as u64 + 1).to_le_bytes());
                    hash.update(value);
                }
                None => hash.update(0u64.to_le_bytes()),
            }
        }
        Bytes::copy_from_slice(&hash.finalize()[..16])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct Scan {
    pub lo: Bytes,
    /// Exclusive; empty means unbounded.
    pub hi: Bytes,
    pub reverse: bool,
    pub limit: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ReadReq {
    pub keys: Vec<Bytes>,
    pub scans: Vec<Scan>,
    /// Linearizable: confirm ownership with storage before answering.
    pub strong: bool,
    /// Otherwise: answer from memory at this revision or newer.
    pub min_revision: u64,
}

/// What one item of a result is counted as, beyond its key and value.
pub const ITEM_BYTES: usize = 64;

/// Asked as a result grows, with the bytes it holds so far: each item's key
/// and value plus `ITEM_BYTES`. Answers how many more it may take, or
/// refuses, and the read stops there instead of assembling a result that
/// will not be sent.
pub trait Room: Sync {
    fn room(&self, held: usize) -> Result<usize>;
}

/// Room for anything.
struct Unmetered;

impl Room for Unmetered {
    fn room(&self, _held: usize) -> Result<usize> {
        Ok(usize::MAX)
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub items: Vec<Entry>,
    /// Whether entries remain beyond the last item.
    pub more: bool,
}

#[derive(Debug)]
pub struct ReadResp {
    pub revision: u64,
    /// `(version, value)` per requested key.
    pub values: Vec<Option<(u64, Bytes)>>,
    pub scans: Vec<ScanResult>,
}

#[derive(Debug, Clone)]
pub struct Info {
    pub revision: u64,
    pub bytes: u64,
    pub tables: usize,
    /// A read of a key looks in one table of each run.
    pub runs: usize,
    pub epoch: String,
    pub config: DbConfig,
}

type Reply = oneshot::Sender<Result<Vec<Result<Committed>>>>;

enum Cmd {
    Commit { txns: Vec<Txn>, reply: Reply },
    Config { change: ConfigChange, reply: Reply },
    Close(oneshot::Sender<()>),
}

struct State {
    mem: Mem,
    /// Newest first.
    tables: Arc<Vec<Arc<Table>>>,
    table_bytes: u64,
    /// How many runs `tables` are.
    runs: usize,
    /// The newest acknowledged revision: what readers see.
    confirmed: u64,
    confirmed_frame: u64,
    config: DbConfig,
}

/// Owned by whoever holds the maintenance lock.
struct Maint {
    /// The newest manifest generation sent. Never reused: a failed upload may
    /// still arrive, and must not be confused with a later manifest.
    generation: u32,
    /// The newest generation stored and followed by a successful claim check.
    checked: u32,
    /// Revision and frame contained in the tables of the newest manifest.
    revision: u64,
    covered: u64,
    sealed: bool,
    next_table: u32,
    /// Tables replaced by a merge, awaiting deletion.
    garbage: Vec<String>,
    /// Tables uploaded since the last confirmed manifest. One whose manifest
    /// is never confirmed becomes garbage at the next that is.
    unpublished: Vec<String>,
}

#[derive(Default)]
struct Checks {
    waiting: Vec<oneshot::Sender<Result<()>>>,
    running: usize,
}

struct Recovered {
    mem: Mem,
    tables: Vec<Arc<Table>>,
    revision: u64,
    config: DbConfig,
    created: bool,
    next_table: u32,
    /// The newest manifest says the database is deleted.
    deleted: bool,
}

pub struct Db {
    pub name: String,
    /// What logs call this database; see `layout::tag`.
    tag: String,
    ctx: Arc<Ctx>,
    uploads: Uploader,
    layout: Layout,
    epoch: Epoch,
    claim_key: String,
    state: RwLock<State>,
    tx: mpsc::Sender<Cmd>,
    checks: Mutex<Checks>,
    closed: OnceLock<Error>,
    /// A release or a deletion has begun. Shared with the reads of a
    /// merge, which stop waiting for the store then.
    closing: Arc<AtomicBool>,
    flush_requested: AtomicBool,
    wake_writer: Notify,
    wake_maint: Notify,
    /// Woken when a release or a deletion begins, or the database closes.
    leave: Notify,
    /// A write was put off for the quota: everything is to be merged, so
    /// that what was overwritten or deleted is no longer counted.
    reclaim: AtomicBool,
    /// Milliseconds after `started` before which that is not asked again.
    reclaim_rests_until: AtomicU64,
    /// Something was deleted that no merge for the quota has taken in yet.
    reclaim_deleted: AtomicBool,
    /// How many runs the merge under way takes, with one of the server's
    /// turns to merge; none when no merge is.
    merging: AtomicUsize,
    maint: tokio::sync::Mutex<Maint>,
    started: Instant,
    last_used_ms: AtomicU64,
    last_commit_ms: AtomicU64,
    /// The newest revision handed out, acknowledged or not.
    newest: AtomicU64,
    /// The newest acknowledged revision, for readers waiting on one.
    confirmed_tx: tokio::sync::watch::Sender<u64>,
    /// Keys written recently. The writer records and checks; the server
    /// may shorten it.
    recent: Mutex<Recent>,
}

/// A turn at merging tables: one of the few that run at once on a server.
type Turn = tokio::sync::OwnedSemaphorePermit;

/// What a merge under way tells the disk cache with a table that is
/// stored meanwhile: the tables it has read through, and those it has yet
/// to read through or has stored itself. Both are empty outside a merge.
type Beside<'a> = (&'a [Arc<Table>], &'a [Arc<Table>]);

/// A merge under way: the tables it makes, or `None` when a release began.
/// Boxed, since a merge that gives way to another holds that one.
type Merging<'a> = Pin<Box<dyn Future<Output = Result<Option<Vec<Arc<Table>>>>> + Send + 'a>>;

/// Objects above this size are uploaded once at a time: racing them would
/// spend real bandwidth, and they are never on the path of a commit.
const RACE_BYTES: usize = 256 << 10;

/// Uploads for one database, counting those still running in the background.
#[derive(Clone)]
struct Uploader {
    ctx: Arc<Ctx>,
    running: Arc<AtomicU64>,
}

impl Uploader {
    fn new(ctx: &Arc<Ctx>) -> Self {
        Self {
            ctx: ctx.clone(),
            running: Arc::new(AtomicU64::new(0)),
        }
    }

    /// One upload, raced. The first PUT starts at once with `upload_eager - 1`
    /// spares beside it; while none has finished, another spare starts every
    /// `hedge_after`, up to `upload_attempts` in all. The first success wins.
    /// Object stores have long upload tails, and a duplicate of immutable
    /// bytes under our own key is harmless. Losers run to completion in the
    /// background, so a connection or upload URL is never abandoned
    /// mid-request.
    ///
    /// A spare is the store's to refuse (`put_spare`). One that was not sent
    /// has stored nothing and has used no attempt: it is asked for again
    /// after the same wait.
    async fn raced(&self, key: &str, body: &Bytes) -> Result<(), StoreError> {
        let opts = &self.ctx.opts;
        let attempts = if body.len() > RACE_BYTES {
            1
        } else {
            opts.upload_attempts.max(1)
        };
        if attempts == 1 {
            return self.ctx.io.store.put(key, body.clone()).await;
        }
        let (done, mut results) = mpsc::unbounded_channel();
        let start = |spare: bool| {
            let (store, key, body) = (self.ctx.io.store.clone(), key.to_owned(), body.clone());
            let (running, done) = (self.running.clone(), done.clone());
            running.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(async move {
                let result = if spare {
                    store.put_spare(&key, body).await
                } else {
                    store.put(&key, body).await.map(|()| true)
                };
                running.fetch_sub(1, Ordering::AcqRel);
                let _ = done.send((spare, result));
            });
        };
        // `first`: the upload that must be sent is under way. `left`:
        // attempts not yet used by an upload that was sent.
        let (mut first, mut spares, mut left) = (true, 0, attempts - 1);
        start(false);
        for _ in 1..opts.upload_eager.clamp(1, attempts) {
            left -= 1;
            spares += 1;
            start(true);
        }
        let mut failure = None;
        loop {
            let event = tokio::select! {
                result = results.recv() => Some(result.expect("this function holds a sender")),
                _ = tokio::time::sleep(opts.hedge_after), if left > 0 => None,
            };
            let another = match event {
                Some((_, Ok(true))) => return Ok(()),
                Some((_, Ok(false))) => {
                    spares -= 1;
                    left += 1;
                    false
                }
                // In place of the one that failed: no more load than before.
                Some((spare, Err(error))) => {
                    failure = Some(error);
                    if spare {
                        spares -= 1;
                    } else {
                        first = false;
                    }
                    true
                }
                None => true,
            };
            // The upload that must be sent is never left to the spares.
            if left > 0 && (another || !first) {
                left -= 1;
                let spare = first;
                if spare {
                    spares += 1;
                } else {
                    first = true;
                }
                start(spare);
            } else if !first && spares == 0 {
                return Err(failure.expect("the first upload failed"));
            }
        }
    }

    /// Upload immutable bytes until they are stored. Repeats are harmless:
    /// the key is ours alone and the content never changes.
    async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        let deadline = Instant::now() + self.ctx.opts.store_deadline;
        let mut delay = Duration::from_millis(10);
        loop {
            match self.raced(key, &body).await {
                Ok(()) => return Ok(()),
                Err(StoreError::Unavailable(reason)) if Instant::now() + delay < deadline => {
                    let object = crate::layout::object(key);
                    tracing::debug!(%reason, object, "upload failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Wait, briefly, until no duplicate upload is still on its way.
    async fn settle(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.running.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn read_frames(ctx: &Ctx, layout: &Layout, epoch: Epoch, after: u64) -> Result<Vec<Frame>> {
    let (prefix, start) = (layout.epoch_frames(epoch), layout.frame(epoch, after));
    let listed = retry(|| list_from(&*ctx.io.store, &prefix, Some(&start))).await?;
    // Frames upload in parallel, so only the unbroken run is committed.
    let mut keys = Vec::new();
    for (i, item) in listed.iter().enumerate() {
        if layout.frame_of(&item.key) != Some(after + 1 + i as u64) {
            break;
        }
        keys.push(item.key.clone());
    }
    let mut frames = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(32) {
        let fetched = futures::future::try_join_all(chunk.iter().map(|key| async move {
            retry(|| ctx.io.store.get(key))
                .await?
                .ok_or_else(|| Error::Busy("the log was collected; a newer owner exists".into()))
        }))
        .await?;
        for body in fetched {
            frames.push(Frame::decode(&body)?);
        }
    }
    Ok(frames)
}

impl Db {
    /// Take ownership of `name` and load its state.
    ///
    /// With `create`, an absent database is created with that configuration;
    /// the returned flag says whether that happened.
    pub async fn open(
        ctx: Arc<Ctx>,
        name: &str,
        create: Option<DbConfig>,
    ) -> Result<(Arc<Db>, bool)> {
        let layout = Layout::new(&ctx.root, name);
        let store = &*ctx.io.store;
        // The current picture. Only a hint for choosing an epoch number.
        let (claims_prefix, manifests_prefix) = (layout.claims(), layout.manifests());
        let (claims, manifests) = tokio::try_join!(
            retry(|| list_all(store, &claims_prefix)),
            retry(|| list_all(store, &manifests_prefix)),
        )?;
        if manifests.iter().any(|m| m.key == layout.tombstone()) {
            return Err(Error::Gone);
        }
        // A database that exists has a manifest, stored before its creation
        // was acknowledged. Without one there is nothing to take over, and a
        // claim left by a creation that never finished is not worth another.
        if manifests.is_empty() && create.is_none() {
            return Err(Error::NotFound);
        }
        let highest = claims
            .iter()
            .chain(&manifests)
            .filter_map(|item| layout.epoch_of(&item.key))
            .map(|epoch| epoch.number)
            .max()
            .unwrap_or(0);
        let epoch = Epoch::after(highest);
        let claim_key = layout.claim(epoch);

        // Claim. One attempt: a claim whose PUT might still be in flight could
        // reappear after a successor deleted it, so it is never used.
        let claim = serde_json::to_vec(&Claim {
            node: ctx.opts.node.clone(),
        })
        .expect("claim encodes");
        store.put_once(&claim_key, Bytes::from(claim)).await?;

        let uploads = Uploader::new(&ctx);
        let recovered = match Self::recover(&ctx, &uploads, &layout, epoch, create).await {
            Ok(recovered) => recovered,
            Err(error) => {
                // The claim is given back only when something above this
                // epoch stays in the bucket: a higher claim or manifest
                // (Busy) or the tombstone (Gone). After any other failure
                // it stays, as after a crash. This epoch may have revoked a
                // lower one whose first manifest is still on its way; with
                // both claims gone, the next epoch could be numbered below
                // that manifest, serve, and then be outranked by it.
                if matches!(error, Error::Busy(_) | Error::Gone) {
                    let _ = retry(|| store.delete(&claim_key)).await;
                }
                return Err(error);
            }
        };
        // From here the first manifest may be in flight. If this fails the
        // claim stays, exactly as after a crash: deleting it would let a
        // lower epoch serve and then be shadowed when the manifest arrives.
        Self::start(&ctx, uploads, name, &layout, epoch, recovered).await
    }

    /// Revoke lower epochs and rebuild the newest state from the bucket.
    async fn recover(
        ctx: &Arc<Ctx>,
        uploads: &Uploader,
        layout: &Layout,
        epoch: Epoch,
        create: Option<DbConfig>,
    ) -> Result<Recovered> {
        let store = &*ctx.io.store;
        // Every claim stored before ours is visible now. A higher one wins.
        let (claims_prefix, manifests_prefix) = (layout.claims(), layout.manifests());
        let claims = retry(|| list_all(store, &claims_prefix)).await?;
        let mut lower = Vec::new();
        for item in &claims {
            match layout.epoch_of(&item.key) {
                Some(other) if other > epoch => {
                    return Err(Error::Busy(
                        "another node is taking ownership of this database".into(),
                    ));
                }
                Some(other) if other < epoch => lower.push(item.key.clone()),
                _ => {}
            }
        }
        // Revoke. After this no lower epoch can acknowledge or collect.
        futures::future::try_join_all(lower.iter().map(|key| retry(move || store.delete(key))))
            .await?;

        // Only now is it safe to read what they wrote.
        let manifests = retry(|| list_all(store, &manifests_prefix)).await?;
        if manifests.iter().any(|m| m.key == layout.tombstone()) {
            return Err(Error::Gone);
        }
        let mut newest: Option<(Epoch, u32, &str)> = None;
        for item in &manifests {
            let (Some(other), Some(generation)) =
                (layout.epoch_of(&item.key), layout.generation_of(&item.key))
            else {
                continue;
            };
            if other > epoch {
                return Err(Error::Busy("a newer owner of this database exists".into()));
            }
            if newest.is_none_or(|(e, g, _)| (other, generation) > (e, g)) {
                newest = Some((other, generation, &item.key));
            }
        }

        let mut mem = Mem::default();
        let (tables, revision, config, replayed, created) = match newest {
            None => {
                let Some(config) = create else {
                    return Err(Error::NotFound);
                };
                (Vec::new(), 0, config, false, true)
            }
            Some((predecessor, _, key)) => {
                let body = retry(|| store.get(key)).await?.ok_or_else(|| {
                    Error::Busy("the newest manifest was collected; a newer owner exists".into())
                })?;
                let manifest = Manifest::decode(&body)?;
                if manifest.deleted {
                    // Its owner may never have learned whether this took
                    // effect. It does now: this epoch carries it forward.
                    return Ok(Recovered {
                        mem,
                        tables: Vec::new(),
                        revision: manifest.revision,
                        config: manifest.config,
                        created: false,
                        next_table: 1,
                        deleted: true,
                    });
                }
                let tables: Vec<_> = manifest.tables.into_iter().map(Table::new).collect();
                let mut revision = manifest.revision;
                let mut config = manifest.config;
                let frames = if manifest.sealed {
                    Vec::new()
                } else {
                    read_frames(ctx, layout, predecessor, manifest.covered).await?
                };
                let replayed = !frames.is_empty();
                for frame in frames {
                    if frame.first_revision != revision + 1 {
                        return Err(Error::Corrupt(format!(
                            "frame starts at revision {} after revision {revision}",
                            frame.first_revision
                        )));
                    }
                    for txn in frame.txns {
                        revision += 1;
                        for (key, value) in txn.writes {
                            mem.put(key, revision, value, revision);
                        }
                        if let Some(next) = txn.config {
                            config = next;
                        }
                    }
                }
                (tables, revision, config, replayed, false)
            }
        };
        // Fold replayed frames into a table so that a manifest never refers
        // to another epoch's log.
        let mut tables = tables;
        let mut next_table = 1;
        if replayed {
            let entries = mem.snapshot(revision);
            let bottom = tables.is_empty();
            let mut builder = TableBuilder::new(ctx.opts.block_bytes);
            for entry in &entries {
                if keep(entry, bottom, revision, ctx.opts.receipt_window) {
                    builder.add(entry);
                }
            }
            if !builder.is_empty() {
                let (body, meta) = builder.finish(layout.table(epoch, next_table));
                next_table += 1;
                uploads.put(&meta.key, body).await?;
                tables.insert(0, Table::new(meta));
            }
            mem.drain(revision);
        }
        Ok(Recovered {
            mem,
            tables,
            revision,
            config,
            created,
            next_table,
            deleted: false,
        })
    }

    /// Publish this epoch's first manifest and begin serving.
    async fn start(
        ctx: &Arc<Ctx>,
        uploads: Uploader,
        name: &str,
        layout: &Layout,
        epoch: Epoch,
        recovered: Recovered,
    ) -> Result<(Arc<Db>, bool)> {
        let Recovered {
            mem,
            tables,
            revision,
            config,
            created,
            next_table,
            deleted,
        } = recovered;
        let manifest = Manifest {
            format: FORMAT,
            revision,
            covered: 0,
            sealed: false,
            tables: tables.iter().map(|t| t.meta.clone()).collect(),
            config: config.clone(),
            deleted,
        };
        uploads
            .put(&layout.manifest(epoch, 0), manifest.encode())
            .await?;
        // The recovered state may end in transactions the previous owner
        // never acknowledged. They become permanent only now: this manifest
        // is stored, and if the claim still stands, every later owner starts
        // from it. Until this check nothing recovered may be shown to anyone.
        let claim_key = layout.claim(epoch);
        if !retry(|| ctx.io.store.exists(&claim_key)).await? {
            return Err(Error::Busy("ownership moved during activation".into()));
        }
        if deleted {
            // A deletion recovered from a predecessor is final now, for the
            // same reason. Finish it instead of serving.
            uploads.settle().await;
            Self::erase(ctx, layout).await?;
            return Err(Error::Gone);
        }

        let (tx, rx) = mpsc::channel(4096);
        let db = Arc::new(Db {
            name: name.to_owned(),
            tag: crate::layout::tag(name),
            ctx: ctx.clone(),
            uploads,
            claim_key: layout.claim(epoch),
            layout: layout.clone(),
            epoch,
            state: RwLock::new(State {
                mem,
                table_bytes: tables.iter().map(|t| t.meta.size).sum(),
                runs: runs(&tables).count(),
                tables: Arc::new(tables),
                confirmed: revision,
                confirmed_frame: 0,
                config,
            }),
            tx,
            checks: Mutex::default(),
            closed: OnceLock::new(),
            closing: Arc::new(AtomicBool::new(false)),
            flush_requested: AtomicBool::new(false),
            wake_writer: Notify::new(),
            wake_maint: Notify::new(),
            leave: Notify::new(),
            reclaim: AtomicBool::new(false),
            reclaim_rests_until: AtomicU64::new(0),
            reclaim_deleted: AtomicBool::new(false),
            merging: AtomicUsize::new(0),
            maint: tokio::sync::Mutex::new(Maint {
                generation: 0,
                checked: 0,
                revision,
                covered: 0,
                sealed: false,
                next_table,
                garbage: Vec::new(),
                unpublished: Vec::new(),
            }),
            started: Instant::now(),
            last_used_ms: AtomicU64::new(0),
            last_commit_ms: AtomicU64::new(0),
            newest: AtomicU64::new(revision),
            confirmed_tx: tokio::sync::watch::channel(revision).0,
            // Nothing is known about writes before this epoch.
            recent: Mutex::new(Recent::new(
                revision,
                ctx.opts.recent_keys,
                ctx.opts.recent_bytes,
            )),
        });
        tokio::spawn(Writer::run(db.clone(), rx, revision));
        tokio::spawn(db.clone().maintain());
        tracing::debug!(db = %db.tag, epoch = %epoch, revision, created, "database opened");
        Ok((db, created))
    }

    // ------------------------------------------------------------------
    // Ownership
    // ------------------------------------------------------------------

    pub fn is_closed(&self) -> bool {
        self.closed.get().is_some()
    }

    fn closed_error(&self) -> Error {
        self.closed
            .get()
            .cloned()
            .unwrap_or_else(|| Error::Busy("database is closing".into()))
    }

    /// Stop serving. Nothing in flight will be acknowledged.
    fn poison(&self, error: Error) {
        if self.closed.set(error.clone()).is_ok() {
            tracing::info!(db = %self.tag, epoch = %self.epoch, %error, "database closed");
        }
        self.wake_writer.notify_one();
        self.wake_maint.notify_one();
        self.leave.notify_waiters();
        // Wake readers waiting for a revision that will now never arrive.
        self.confirmed_tx.send_modify(|_| {});
    }

    /// Resolves once a claim check that began after this call has finished.
    /// `Ok` proves this node owned the database at some instant in between.
    pub async fn check(self: &Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut checks = self.checks.lock();
            checks.waiting.push(tx);
            if checks.running < self.ctx.opts.max_checks {
                checks.running += 1;
                tokio::spawn(self.clone().check_rounds());
            }
        }
        rx.await
            .unwrap_or_else(|_| Err(Error::Busy("claim check abandoned".into())))
    }

    async fn check_rounds(self: Arc<Self>) {
        loop {
            let batch = {
                let mut checks = self.checks.lock();
                if checks.waiting.is_empty() {
                    checks.running -= 1;
                    return;
                }
                std::mem::take(&mut checks.waiting)
            };
            let result = match self.closed.get() {
                Some(error) => Err(error.clone()),
                None => match retry(|| self.ctx.io.store.exists(&self.claim_key)).await {
                    Ok(true) => Ok(()),
                    Ok(false) => {
                        let error = Error::Lost("another node took ownership".into());
                        self.poison(error.clone());
                        Err(error)
                    }
                    Err(error) => Err(error),
                },
            };
            for waiter in batch {
                let _ = waiter.send(result.clone());
            }
        }
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    fn touch(&self) {
        self.last_used_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub fn idle_for(&self) -> Duration {
        let last = Duration::from_millis(self.last_used_ms.load(Ordering::Relaxed));
        self.started.elapsed().saturating_sub(last)
    }

    pub fn info(&self) -> Info {
        let state = self.state.read();
        Info {
            revision: state.confirmed,
            bytes: state.table_bytes + state.mem.bytes() as u64,
            tables: state.tables.len(),
            runs: state.runs,
            epoch: self.epoch.to_string(),
            config: state.config.clone(),
        }
    }

    pub fn config(&self) -> DbConfig {
        self.state.read().config.clone()
    }

    /// The newest revision this owner has handed out. It is at least the
    /// revision any conflict it reported was caused by.
    pub fn newest_revision(&self) -> u64 {
        self.newest.load(Ordering::Acquire)
    }

    /// Bytes held in memory that no table contains yet.
    pub fn mem_bytes(&self) -> usize {
        self.state.read().mem.bytes()
    }

    /// Bytes of recently written keys remembered for range checks.
    pub fn recent_bytes(&self) -> usize {
        self.recent.lock().bytes
    }

    /// Remember at most `bytes` of recently written keys from now on, and
    /// forget the oldest beyond that. A range check that reaches back past
    /// what was forgotten is refused, and its transaction reads again.
    pub fn remember_at_most(&self, bytes: usize) {
        let mut recent = self.recent.lock();
        recent.capacity_bytes = bytes.min(self.ctx.opts.recent_bytes);
        recent.trim();
    }

    /// Ask for a flush regardless of thresholds, to give memory back.
    pub fn request_flush(&self) {
        self.flush_requested.store(true, Ordering::Release);
        self.wake_maint.notify_one();
    }

    /// The newest version of `key`, including writes not yet acknowledged.
    async fn latest(&self, key: &[u8]) -> Result<Option<(u64, Option<Bytes>)>> {
        loop {
            let (hit, tables) = {
                let state = self.state.read();
                (state.mem.get(key, u64::MAX), state.tables.clone())
            };
            if hit.is_some() {
                return Ok(hit);
            }
            let found = async {
                for table in tables.iter() {
                    if let Some(entry) = table.get(&self.ctx.io, key).await? {
                        return Ok(Some((entry.version, entry.value)));
                    }
                }
                Ok(None)
            };
            match found.await {
                Err(Error::Lost(_)) if self.replaced(&tables) => {}
                other => return other,
            }
        }
    }

    /// Whether this owner has moved on from `tables`. A merge deletes the
    /// tables it replaced, so a lookup that began before it can find one of
    /// them gone. That is this owner's own doing, not a takeover, and the
    /// lookup starts over on the tables that replaced them.
    fn replaced(&self, tables: &Arc<Vec<Arc<Table>>>) -> bool {
        !Arc::ptr_eq(tables, &self.state.read().tables)
    }

    pub async fn read(self: &Arc<Self>, request: ReadReq) -> Result<ReadResp> {
        self.read_within(request, &Unmetered).await
    }

    /// A read whose result is accounted for while it is built.
    pub async fn read_within(
        self: &Arc<Self>,
        mut request: ReadReq,
        room: &dyn Room,
    ) -> Result<ReadResp> {
        self.touch();
        if let Some(error) = self.closed.get() {
            return Err(error.clone());
        }
        for scan in &mut request.scans {
            // Keys that begin with U+0000 are the database's own.
            if scan.lo.first().is_none_or(|byte| *byte == INTERNAL) {
                scan.lo = Bytes::from_static(&[INTERNAL + 1]);
            }
        }
        if !request.strong && request.min_revision > self.state.read().confirmed {
            if request.min_revision > self.newest.load(Ordering::Acquire) {
                // The caller has seen a newer revision than this node has
                // ever produced. Either ownership moved, or the revision is
                // not from this database.
                self.check().await?;
                return Err(Error::Invalid(
                    "min_revision is ahead of the database".into(),
                ));
            }
            // The revision exists here but is not acknowledged yet, as after
            // a conflict with a transaction still being stored. Wait for it.
            let mut confirmed = self.confirmed_tx.subscribe();
            let reached = confirmed
                .wait_for(|revision| *revision >= request.min_revision || self.is_closed());
            match tokio::time::timeout(self.ctx.opts.store_deadline, reached).await {
                Ok(Ok(_)) if !self.is_closed() => {}
                _ => return Err(self.closed_error()),
            }
        }
        // A strong read may use any snapshot taken after it arrived, provided
        // a claim check that began after it arrived succeeds.
        let check = async {
            if request.strong {
                self.check().await
            } else {
                Ok(())
            }
        };
        let (checked, response) = tokio::join!(check, self.read_snapshot(&request, room));
        checked?;
        response
    }

    async fn read_snapshot(&self, request: &ReadReq, room: &dyn Room) -> Result<ReadResp> {
        loop {
            let tables = self.state.read().tables.clone();
            match self.read_once(request, room).await {
                Err(Error::Lost(_)) if self.replaced(&tables) => {}
                other => return other,
            }
        }
    }

    async fn read_once(&self, request: &ReadReq, room: &dyn Room) -> Result<ReadResp> {
        // Everything the result comes to hold is reported as it is found.
        let held = AtomicUsize::new(0);
        let spend = |bytes: usize| {
            room.room(
                held.fetch_add(bytes, Ordering::Relaxed)
                    .saturating_add(bytes),
            )
        };
        // One lock hold fixes the revision, the tables and everything the
        // in-memory table contributes.
        let (revision, tables, hits, ranges) = {
            let state = self.state.read();
            let snapshot = state.confirmed;
            let hits: Vec<_> = request
                .keys
                .iter()
                .map(|key| state.mem.get(key, snapshot))
                .collect();
            let mut ranges = Vec::with_capacity(request.scans.len());
            for scan in &request.scans {
                let live = scan.limit.saturating_add(1);
                let most = spend(0)? / ITEM_BYTES;
                let entries = state
                    .mem
                    .range(&scan.lo, &scan.hi, scan.reverse, snapshot, live, most)
                    .ok_or(Error::TooLarge)?;
                spend(entries.len() * ITEM_BYTES)?;
                ranges.push(entries);
            }
            (snapshot, state.tables.clone(), hits, ranges)
        };
        let io = &self.ctx.io;
        let lookups = request.keys.iter().zip(hits).map(|(key, hit)| {
            let (tables, spend) = (&tables, &spend);
            async move {
                let mut found = hit;
                if found.is_none() {
                    for table in tables.iter() {
                        if let Some(entry) = table.get(io, key).await? {
                            found = Some((entry.version, entry.value));
                            break;
                        }
                    }
                }
                let found = found.and_then(|(version, value)| Some((version, value?)));
                spend(ITEM_BYTES + found.as_ref().map_or(0, |(_, value)| value.len()))?;
                Ok::<_, Error>(found)
            }
        });
        // A few at a time: one request does not start a storage call for
        // every key it names, and stops at the first refusal.
        let lookups: Vec<_> = lookups.collect();
        let values: Vec<_> = futures::stream::iter(lookups)
            .buffered(32)
            .try_collect()
            .await?;
        let mut scans = Vec::with_capacity(request.scans.len());
        for (scan, mem) in request.scans.iter().zip(ranges) {
            let found = merge(io, mem, &tables, scan).await?;
            spend(found.items.iter().map(Entry::weight).sum())?;
            scans.push(found);
        }
        Ok(ReadResp {
            revision,
            values,
            scans,
        })
    }

    // ------------------------------------------------------------------
    // Writes
    // ------------------------------------------------------------------

    /// Apply transactions in order, stopping at the first that fails. The
    /// outer error means the outcome is unknown and the request is retryable.
    pub async fn commit(&self, txns: Vec<Txn>) -> Result<Vec<Result<Committed>>> {
        self.touch();
        self.send(|reply| Cmd::Commit { txns, reply }).await
    }

    /// Change the configuration. The writer applies `change` to the
    /// configuration as it then stands, so concurrent changes all take effect.
    pub async fn configure(&self, change: ConfigChange) -> Result<Committed> {
        let mut results = self.send(|reply| Cmd::Config { change, reply }).await?;
        results
            .pop()
            .unwrap_or_else(|| Err(Error::Busy("no result".into())))
    }

    async fn send(&self, command: impl FnOnce(Reply) -> Cmd) -> Result<Vec<Result<Committed>>> {
        if self.closing.load(Ordering::Acquire) {
            return Err(self.closed_error());
        }
        let (reply, result) = oneshot::channel();
        self.tx
            .send(command(reply))
            .await
            .map_err(|_| self.closed_error())?;
        result.await.map_err(|_| self.closed_error())?
    }

    // ------------------------------------------------------------------
    // Checkpoints and collection
    // ------------------------------------------------------------------

    async fn maintain(self: Arc<Self>) {
        let _fuse = Fuse(self.clone());
        // Activation ended with a claim check after this epoch's manifest
        // was stored, so what earlier epochs left behind can go. Holding the
        // lock makes a release wait for the sweep.
        {
            let _maint = self.maint.lock().await;
            self.sweep().await;
        }
        // A merge may be due: one that the last owner was killed in, or
        // one for the table of what it had not flushed. It is not left to
        // wait for a write.
        self.wake_maint.notify_one();
        // A revoked owner can still store an object or two before it
        // notices. Sweep once more after it has had time to.
        let mut resweep = Some(tokio::time::Instant::now() + self.ctx.opts.resweep_after);
        let mut unflushed = false;
        // Merges take turns across the server. This database's place in
        // the queue, and its turn once that has come: it goes on flushing
        // while it waits.
        let mut queued: Option<Pin<Box<dyn Future<Output = Option<Turn>> + Send>>> = None;
        let mut turn: Option<Turn> = None;
        loop {
            // An idle database with nothing to flush sleeps until it is used.
            let idle = async {
                if unflushed {
                    tokio::time::sleep(self.ctx.opts.idle_flush).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let straggler = async {
                match resweep {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let granted = async {
                match queued.as_mut() {
                    Some(waiting) => waiting.await,
                    None => std::future::pending().await,
                }
            };
            let sweep_now = tokio::select! {
                _ = self.wake_maint.notified() => false,
                _ = idle => false,
                _ = straggler => true,
                granted = granted => {
                    (turn, queued) = (granted, None);
                    false
                }
            };
            if self.is_closed() || self.closing.load(Ordering::Acquire) {
                return;
            }
            let mut maint = self.maint.lock().await;
            // A release or a deletion may have finished while this waited.
            if self.is_closed() || self.closing.load(Ordering::Acquire) {
                return;
            }
            if sweep_now {
                resweep = None;
                if self.check().await.is_ok() {
                    self.sweep().await;
                }
            }
            unflushed = self.state.read().confirmed_frame > maint.covered;
            if self.should_flush(&maint)
                && let Err(error) = self.flush(&mut maint, false, (&[], &[])).await
            {
                report(&self.tag, "flush failed", &error);
                // Storage is failing: a turn would only be kept from others.
                (turn, queued) = (None, None);
                continue;
            }
            let mut failed = false;
            loop {
                match self.compact(&mut maint, &mut turn).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(error) => {
                        report(&self.tag, "compaction failed", &error);
                        failed = true;
                        break;
                    }
                }
            }
            unflushed = self.state.read().confirmed_frame > maint.covered;
            // A merge that failed is tried again when the database is next
            // written to, not at once: asking for another turn now would
            // repeat it for as long as storage is failing.
            let due = self.plan(&self.state.read().tables).is_some();
            if failed || !due || self.closing() {
                (turn, queued) = (None, None);
            } else if turn.is_none() && queued.is_none() {
                let merges = self.ctx.io.merges.clone();
                queued = Some(Box::pin(async move { merges.acquire_owned().await.ok() }));
            }
        }
    }

    fn closing(&self) -> bool {
        self.is_closed() || self.closing.load(Ordering::Acquire)
    }

    /// Whether a database of `runs` would have more than writes may wait
    /// for, twice as many as it may keep, once the merge under way is
    /// published: the runs it takes are then one. Never while no merge is:
    /// a database that waits for its turn, or whose merge failed, could do
    /// nothing for the writes it refused, and the next it takes begins that
    /// merge again. Nor for runs the merge will take: one that begins with
    /// many, as after a kill or a long wait for its turn, takes writes.
    fn crowded(&self, runs: usize) -> bool {
        let taking = self.merging.load(Ordering::Acquire);
        let after = (runs + 1).saturating_sub(taking);
        taking > 0 && after > self.ctx.opts.max_runs.max(1).saturating_mul(2)
    }

    /// Resolves when a release or a deletion has begun, or the database
    /// has closed.
    async fn leaving(&self) {
        let mut leave = std::pin::pin!(self.leave.notified());
        loop {
            // Listening before looking: one that begins between the two
            // is not missed.
            leave.as_mut().enable();
            if self.closing() {
                return;
            }
            leave.as_mut().await;
            leave.set(self.leave.notified());
        }
    }

    /// How many of the newest tables to merge now, if any, and whether
    /// that is all of them for the quota's sake.
    fn plan(&self, tables: &[Arc<Table>]) -> Option<(usize, bool)> {
        if self.reclaim.load(Ordering::Acquire) && runs(tables).nth(1).is_some() {
            return Some((tables.len(), true));
        }
        merge_plan(tables, u64::MAX, self.ctx.opts.max_runs).map(|count| (count, false))
    }

    /// Ask for everything to be merged, for a write that the quota has no
    /// room for. `false` when the last such merge gained too little for
    /// another to be made yet.
    ///
    /// A merge of everything rewrites the database, and a database that is
    /// full of what can still be read could ask for one with every write.
    /// So after one that removed less than an eighth of what it merged, the
    /// next waits four times as long as it took, and a second at least:
    /// writes that do not fit are refused meanwhile, as they are when there
    /// is nothing to merge. Nothing but time ends the wait. If a deletion
    /// did, one deletion and one write, over and over, would have a full
    /// database rewritten for each.
    fn reclaim(&self) -> bool {
        let now = self.started.elapsed().as_millis() as u64;
        if now < self.reclaim_rests_until.load(Ordering::Acquire) {
            return false;
        }
        self.reclaim.store(true, Ordering::Release);
        self.request_flush();
        true
    }

    fn should_flush(&self, maint: &Maint) -> bool {
        let state = self.state.read();
        let pending = state.confirmed_frame.saturating_sub(maint.covered);
        let idle = (self.started.elapsed().as_millis() as u64)
            .saturating_sub(self.last_commit_ms.load(Ordering::Relaxed));
        (pending > 0 && self.flush_requested.swap(false, Ordering::AcqRel))
            || state.mem.bytes() >= self.ctx.opts.flush_bytes
            || pending >= self.ctx.opts.flush_frames
            || (pending > 0 && idle >= self.ctx.opts.idle_flush.as_millis() as u64)
    }

    /// Move acknowledged writes from the in-memory table into a new table.
    /// `beside` is of the merge that gives way to this flush, if one does.
    async fn flush(
        self: &Arc<Self>,
        maint: &mut Maint,
        seal: bool,
        beside: Beside<'_>,
    ) -> Result<()> {
        let (revision, frame, entries, mut tables) = {
            let state = self.state.read();
            (
                state.confirmed,
                state.confirmed_frame,
                state.mem.snapshot(state.confirmed),
                (*state.tables).clone(),
            )
        };
        if frame == maint.covered && seal == maint.sealed {
            return Ok(());
        }
        if !entries.is_empty() {
            let bottom = tables.is_empty();
            let window = self.ctx.opts.receipt_window;
            let block_bytes = self.ctx.opts.block_bytes;
            let key = self.layout.table(self.epoch, maint.next_table);
            let built = tokio::task::spawn_blocking(move || {
                let mut builder = TableBuilder::new(block_bytes);
                for entry in entries.iter().filter(|e| keep(e, bottom, revision, window)) {
                    builder.add(entry);
                }
                (!builder.is_empty()).then(|| builder.finish(key))
            })
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
            if let Some((body, meta)) = built {
                let table = self.store_table(maint, body, meta, beside).await?;
                tables.insert(0, table);
            }
        }
        self.publish(maint, tables, revision, frame, seal, Some(revision))
            .await
    }

    /// Upload a table that the next manifest will list. While a merge
    /// runs, `done` are the tables it has read through and `kept` those it
    /// has yet to and those it has stored: in the disk cache the table
    /// takes the room of the first before that of others in use, and
    /// never that of the second.
    async fn store_table(
        &self,
        maint: &mut Maint,
        body: Bytes,
        meta: TableMeta,
        (done, kept): Beside<'_>,
    ) -> Result<Arc<Table>> {
        maint.next_table += 1;
        maint.unpublished.push(meta.key.clone());
        self.uploads.put(&meta.key, body.clone()).await?;
        let done: Vec<&str> = done.iter().map(|table| table.meta.key.as_str()).collect();
        let kept: Vec<&str> = kept.iter().map(|table| table.meta.key.as_str()).collect();
        self.ctx.io.remember(&meta.key, body, &done, &kept).await;
        Ok(Table::new(meta))
    }

    /// Merge the newest tables when they have grown comparable to the next,
    /// and this database has a turn or can take one. Returns whether it did.
    async fn compact(self: &Arc<Self>, maint: &mut Maint, turn: &mut Option<Turn>) -> Result<bool> {
        // A merge for the quota is of everything that has been written:
        // what is not in a table yet is put in one first. Without it, a
        // value that was written over a moment ago would be kept.
        if self.reclaim.load(Ordering::Acquire) {
            // What has been deleted up to here is in the tables it merges.
            self.reclaim_deleted.store(false, Ordering::Release);
            if self.state.read().confirmed_frame > maint.covered {
                self.flush(maint, false, (&[], &[])).await?;
            }
        }
        let tables = self.state.read().tables.clone();
        let Some((count, reclaiming)) = self.plan(&tables) else {
            return Ok(false);
        };
        // A merge holds one table of its output in memory; only a few run
        // at once.
        if turn.is_none() {
            *turn = self.ctx.io.merges.clone().try_acquire_owned().ok();
        }
        let Some(_turn) = turn.take() else {
            return Ok(false);
        };
        let _merging = UnderWay::begin(&self.merging, runs(&tables[..count]).count());
        let began = Instant::now();
        let Some(merged) = self.merge(maint, &tables, count, true).await? else {
            return Ok(false);
        };
        self.publish_merged(maint, &tables, count, &merged).await?;
        if reclaiming {
            let size = |tables: &[Arc<Table>]| tables.iter().map(|t| t.meta.size).sum::<u64>();
            let (before, after) = (size(&tables), size(&merged));
            if after > before - before / 8 {
                let rest = (began.elapsed() * RECLAIM_REST).max(self.ctx.opts.quota_rest);
                let until = self.started.elapsed() + rest;
                self.reclaim_rests_until
                    .store(until.as_millis() as u64, Ordering::Release);
            }
        }
        if count == tables.len() {
            // Whatever asked for it has it now.
            self.reclaim.store(false, Ordering::Release);
        }
        Ok(true)
    }

    /// The `count` newest of `tables` merged into new tables, each stored
    /// before the next is begun, for the caller to publish. `None` when a
    /// release began meanwhile. With `behind`, it gives way to merges of
    /// what is flushed behind it as it does to the flushes.
    fn merge<'a>(
        self: &'a Arc<Self>,
        maint: &'a mut Maint,
        tables: &'a [Arc<Table>],
        count: usize,
        behind: bool,
    ) -> Merging<'a> {
        Box::pin(async move {
            // A merge is use. A database that was released for being idle
            // while it merged would have the merge begun again from its
            // start by its next owner, and a merge that takes longer than
            // a database may be idle for would never end.
            self.touch();
            let bottom = count == tables.len();
            let io = &self.ctx.io;
            let everything = Scan {
                lo: Bytes::new(),
                hi: Bytes::new(),
                reverse: false,
                limit: usize::MAX,
            };
            let mut scans: Vec<RunScan> = runs(&tables[..count])
                .filter_map(|run| {
                    RunScan::over(io, run, &everything, Reader::Merge(self.closing.clone()))
                })
                .collect();
            let mut heads = Vec::with_capacity(scans.len());
            for scan in &mut scans {
                heads.push(scan.next().await?);
            }
            // The result is written as tables of limited size, each
            // uploaded before the next is begun.
            let (block_bytes, table_bytes) = (self.ctx.opts.block_bytes, self.ctx.opts.table_bytes);
            let mut merged: Vec<Arc<Table>> = Vec::new();
            let mut builder = TableBuilder::new(block_bytes);
            // What this merge has stored, less what the merges it gave way
            // to have merged.
            let mut spare = 0;
            loop {
                let next = first_key(&heads, false);
                if !builder.is_empty() && (next.is_none() || builder.len() >= table_bytes) {
                    let full = std::mem::replace(&mut builder, TableBuilder::new(block_bytes));
                    let (body, mut meta) =
                        full.finish(self.layout.table(self.epoch, maint.next_table));
                    meta.part = merged.len() as u32;
                    // In the disk cache the table takes the room of those
                    // the merge has read through, and not of those it is
                    // coming to or has stored.
                    let done: Vec<Arc<Table>> =
                        scans.iter().flat_map(|scan| &scan.done).cloned().collect();
                    let coming = scans.iter().flat_map(RunScan::unread);
                    let mut kept: Vec<Arc<Table>> = coming.chain(&merged).cloned().collect();
                    let table = self.store_table(maint, body, meta, (&done, &kept)).await?;
                    kept.push(table.clone());
                    spare += table.meta.size;
                    merged.push(table);
                    self.touch();
                    // Between tables, a merge gives way to what must not
                    // wait for it: a release, and writes that are due to
                    // be flushed.
                    if next.is_some() && self.closing() {
                        return Ok(None);
                    }
                    // And, while the database has more runs than writes
                    // may wait for, to merges of what was flushed behind
                    // it: no write falls due that would give way for them,
                    // and its stored tables are what they may take.
                    let crowded = behind && self.crowded(self.state.read().runs);
                    if next.is_some() && (self.should_flush(maint) || crowded) {
                        // What is published meanwhile discards tables no
                        // manifest lists. These are not finished, not lost.
                        let at = maint.unpublished.len() - merged.len();
                        let mine = maint.unpublished.split_off(at);
                        let beside = (done.as_slice(), kept.as_slice());
                        let behind = behind.then_some((&mut spare, scans.as_mut_slice()));
                        let gave = self.give_way(maint, tables.len(), beside, behind).await;
                        maint.unpublished.extend(mine);
                        gave?;
                        // A release that began meanwhile has waited for
                        // that, and waits for no table more.
                        if self.closing() {
                            return Ok(None);
                        }
                    }
                }
                let Some(best) = next else { break };
                let entry = heads[best].take().expect("chosen head exists");
                for i in 0..scans.len() {
                    if i == best
                        || heads[i]
                            .as_ref()
                            .is_some_and(|other| other.key == entry.key)
                    {
                        heads[i] = scans[i].next().await?;
                    }
                }
                if keep(&entry, bottom, maint.revision, self.ctx.opts.receipt_window) {
                    builder.add(&entry);
                }
            }
            Ok(Some(merged))
        })
    }

    /// What a merge does between two of its tables for the writes that are
    /// due: they are flushed, and with `behind` the tables flushed since
    /// the merge began are merged among themselves as `plan_behind` has
    /// it, which is when the database has more runs than it may.
    /// `under` is how many tables the database had when the merge began,
    /// and `beside` is what that merge tells the disk cache with the
    /// flushed table. `behind` is what the merge has stored and not yet
    /// given way for, and its scans.
    ///
    /// Every flush is a run, and a read looks in every run. A merge of
    /// 16 GiB takes two minutes on a local directory and far longer on a
    /// bucket, and writes went on being flushed behind it at a run for
    /// every 8 MiB: a server killed during one left 27 runs, and reads of
    /// that database were a quarter as fast as with seven.
    ///
    /// The merges it gives way to take no more together than it has itself
    /// stored, so it has half the merging at least and ends, however fast
    /// the database is written: it holds one of a server's turns to merge
    /// until then. They give way to flushes and to no merge of their own.
    /// While one runs, the merge that waits gives up what its scans had
    /// read ahead, and with that the turns to read ahead, of which a
    /// server has sixteen. A merge it gave way to tells the disk cache of
    /// its own tables and not of those the waiting merge is coming to: in
    /// a full cache its tables may take their room, and the waiting merge
    /// then reads those from the bucket.
    async fn give_way(
        self: &Arc<Self>,
        maint: &mut Maint,
        under: usize,
        beside: Beside<'_>,
        behind: Option<(&mut u64, &mut [RunScan])>,
    ) -> Result<()> {
        self.flush(maint, false, beside).await?;
        let Some((spare, scans)) = behind else {
            return Ok(());
        };
        while !self.closing() {
            let tables = self.state.read().tables.clone();
            let most = self.ctx.opts.max_runs;
            let Some(count) = plan_behind(&tables, under, spare, most) else {
                break;
            };
            scans.iter_mut().for_each(RunScan::rest);
            let Some(merged) = self.merge(maint, &tables, count, false).await? else {
                break;
            };
            self.publish_merged(maint, &tables, count, &merged).await?;
            // What fell due meanwhile does not wait for a table of the
            // merge that gave way as well.
            if self.should_flush(maint) {
                self.flush(maint, false, beside).await?;
            }
        }
        Ok(())
    }

    /// Publish what a merge of the `count` newest of `tables` made, in
    /// their place. Tables flushed meanwhile are newer than everything
    /// merged.
    async fn publish_merged(
        self: &Arc<Self>,
        maint: &mut Maint,
        tables: &[Arc<Table>],
        count: usize,
        merged: &[Arc<Table>],
    ) -> Result<()> {
        let current = self.state.read().tables.clone();
        let mut result = current[..current.len() - tables.len()].to_vec();
        result.extend(merged.iter().cloned());
        result.extend(tables[count..].iter().cloned());
        let (revision, covered, sealed) = (maint.revision, maint.covered, maint.sealed);
        self.publish(maint, result, revision, covered, sealed, None)
            .await
    }

    /// Store the next manifest, confirm ownership, switch readers to its
    /// tables, and only then delete what it no longer references.
    async fn publish(
        self: &Arc<Self>,
        maint: &mut Maint,
        tables: Vec<Arc<Table>>,
        revision: u64,
        covered: u64,
        sealed: bool,
        flushed: Option<u64>,
    ) -> Result<()> {
        maint.generation += 1;
        let generation = maint.generation;
        let manifest = Manifest {
            format: FORMAT,
            revision,
            covered,
            sealed,
            tables: tables.iter().map(|t| t.meta.clone()).collect(),
            config: self.state.read().config.clone(),
            deleted: false,
        };
        self.uploads
            .put(
                &self.layout.manifest(self.epoch, generation),
                manifest.encode(),
            )
            .await?;
        self.check().await?;
        let kept: HashSet<&str> = tables.iter().map(|t| t.meta.key.as_str()).collect();
        let replaced: Vec<String> = {
            let state = self.state.read();
            state
                .tables
                .iter()
                .filter(|t| !kept.contains(t.meta.key.as_str()))
                .map(|t| t.meta.key.clone())
                .collect()
        };
        // Tables uploaded for an earlier manifest that was never confirmed
        // are referenced by nothing a successor could choose any more.
        let orphaned: Vec<String> = std::mem::take(&mut maint.unpublished)
            .into_iter()
            .filter(|key| !kept.contains(key.as_str()))
            .collect();
        {
            let mut state = self.state.write();
            state.table_bytes = tables.iter().map(|t| t.meta.size).sum();
            state.runs = runs(&tables).count();
            state.tables = Arc::new(tables);
            if let Some(upto) = flushed {
                state.mem.drain(upto);
            }
        }
        maint.checked = generation;
        maint.revision = revision;
        maint.covered = covered;
        maint.sealed = sealed;
        // Nothing that begins now reads them, and the room they have in
        // the disk cache is what the tables in their place are read into.
        self.ctx.io.discard(&replaced).await;
        self.ctx.io.discard(&orphaned).await;
        maint.garbage.extend(replaced);
        maint.garbage.extend(orphaned);
        self.collect(maint, false).await;
        Ok(())
    }

    /// Delete this epoch's covered frames, older manifests and replaced
    /// tables. What fails is collected after the next flush, or by the next
    /// owner's sweep.
    ///
    /// A store can take long over a purge: one that was slow to list held
    /// a deletion up for seven minutes behind a collection. So a collection
    /// gives way to a release or a deletion at once, in the middle of a
    /// purge if it is in one. `last` is the collection a release makes of
    /// what it leaves, which is the one made while the database is closing.
    async fn collect(&self, maint: &mut Maint, last: bool) {
        let purges = self.purge_garbage(maint);
        let failed = if last {
            purges.await
        } else {
            tokio::select! {
                biased;
                () = self.leaving() => return,
                failed = purges => failed,
            }
        };
        if let Some(error) = failed {
            tracing::debug!(db = %self.tag, %error, "collection incomplete; will retry");
        }
    }

    /// The purges of a collection, until the store does not answer one:
    /// it is not asked for the rest, which wait with that one. A purge that
    /// fails in another way has failed for its own keys, and the others are
    /// still made. A key leaves `garbage` when it is gone.
    async fn purge_garbage(&self, maint: &mut Maint) -> Option<StoreError> {
        let store = &*self.ctx.io.store;
        let all = |_: &str| true;
        let down = |error: &StoreError| matches!(error, StoreError::Unavailable(_));
        let frames = self.layout.epoch_frames(self.epoch);
        let frames_end = self.layout.frame(self.epoch, maint.covered + 1);
        let manifests = self.layout.epoch_manifests(self.epoch);
        let manifests_end = self.layout.manifest(self.epoch, maint.checked);
        let mut failed = None;
        if maint.covered > 0
            && let Err(error) = store.purge(&frames, Some(&frames_end), &all).await
        {
            if down(&error) {
                return Some(error);
            }
            failed = Some(error);
        }
        if let Err(error) = store.purge(&manifests, Some(&manifests_end), &all).await {
            if down(&error) {
                return Some(error);
            }
            failed = Some(error);
        }
        let mut at = maint.garbage.len();
        while at > 0 {
            at -= 1;
            let key = maint.garbage[at].clone();
            match store.purge(&key, None, &|candidate| candidate == key).await {
                Ok(_) => drop(maint.garbage.swap_remove(at)),
                Err(error) if down(&error) => return Some(error),
                Err(error) => failed = Some(error),
            }
        }
        failed
    }

    /// Delete everything lower epochs left that this epoch does not reference.
    async fn sweep(&self) {
        let store = &*self.ctx.io.store;
        let referenced: HashSet<String> = self
            .state
            .read()
            .tables
            .iter()
            .map(|t| t.meta.key.clone())
            .collect();
        // One listing of the whole database: anything of a lower epoch goes,
        // except the tables this epoch's manifest still names.
        let doomed = |key: &str| {
            self.layout
                .epoch_of(key)
                .is_some_and(|epoch| epoch < self.epoch && !referenced.contains(key))
        };
        if let Err(error) = store.purge(self.layout.everything(), None, &doomed).await {
            tracing::debug!(db = %self.tag, %error, "sweep incomplete");
        }
    }

    /// Hand the database back: finish what is in flight, store a sealed
    /// manifest, and delete the claim so the next owner opens without
    /// revoking anyone or replaying anything.
    pub async fn close(self: &Arc<Self>) {
        if self.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        self.leave.notify_waiters();
        let (done, stopped) = oneshot::channel();
        if self.tx.send(Cmd::Close(done)).await.is_ok() {
            let _ = stopped.await;
        }
        let mut maint = self.maint.lock().await;
        if !self.is_closed() {
            match self.flush(&mut maint, true, (&[], &[])).await {
                Ok(()) => {
                    // Duplicates of raced uploads may still be arriving.
                    // Let them land, then collect once more, so that what
                    // is left is exactly the sealed manifest and its tables.
                    self.uploads.settle().await;
                    self.collect(&mut maint, true).await;
                    let store = &*self.ctx.io.store;
                    if retry(|| store.delete(&self.claim_key)).await.is_ok() {
                        let _ = store
                            .purge(&self.claim_key, None, &|key| key == self.claim_key)
                            .await;
                    }
                }
                Err(error) => report(&self.tag, "release without a sealed manifest", &error),
            }
        }
        self.poison(Error::Busy("database is closing".into()));
    }

    /// Delete the database for good.
    ///
    /// Deleting is a change of state like any other: a manifest of this
    /// epoch that says "deleted", then a claim check. If a successor took
    /// over before the manifest was stored, its manifests outrank this one
    /// and nothing was deleted. An error means the outcome is unknown until
    /// the next owner reads the bucket; repeating the request settles it.
    pub async fn destroy(self: &Arc<Self>) -> Result<()> {
        self.closing.store(true, Ordering::Release);
        self.leave.notify_waiters();
        let (done, stopped) = oneshot::channel();
        if self.tx.send(Cmd::Close(done)).await.is_ok() {
            let _ = stopped.await;
        }
        let mut maint = self.maint.lock().await;
        if let Some(error) = self.closed.get() {
            return Err(error.clone());
        }
        maint.generation += 1;
        let manifest = Manifest {
            format: FORMAT,
            revision: maint.revision,
            covered: maint.covered,
            sealed: maint.sealed,
            tables: Vec::new(),
            config: self.state.read().config.clone(),
            deleted: true,
        };
        let key = self.layout.manifest(self.epoch, maint.generation);
        let confirmed = match self.uploads.put(&key, manifest.encode()).await {
            Ok(()) => self.check().await,
            Err(error) => Err(error),
        };
        if let Err(error) = confirmed {
            // The manifest may still arrive, so this epoch never serves
            // again and keeps its claim, exactly as after a crash.
            self.poison(Error::Busy("a deletion was interrupted".into()));
            return Err(error);
        }
        self.poison(Error::Gone);
        self.uploads.settle().await;
        Self::erase(&self.ctx, &self.layout).await
    }

    /// Finish a deletion that is already final: record it with the
    /// tombstone, then remove every other object. The tombstone is written
    /// only here, after the deletion is final, so any node may trust it
    /// without holding a claim, and may repeat this.
    pub async fn erase(ctx: &Ctx, layout: &Layout) -> Result<()> {
        let store = &*ctx.io.store;
        let tombstone = layout.tombstone();
        retry(|| store.put(&tombstone, Bytes::from_static(b"{}"))).await?;
        let doomed = |key: &str| key != tombstone;
        retry(|| store.purge(layout.everything(), None, &doomed)).await?;
        Ok(())
    }
}

/// Closes the database if the task that holds this panics, so that a defect
/// stops one database until it is opened again instead of leaving it
/// accepting requests it can never answer.
struct Fuse(Arc<Db>);

impl Drop for Fuse {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.poison(Error::Busy(
                "an internal task of this database failed".into(),
            ));
        }
    }
}

/// Losing ownership in the middle of maintenance is routine; anything else
/// deserves attention.
fn report(db: &str, what: &str, error: &Error) {
    if matches!(error, Error::Lost(_)) {
        tracing::debug!(db, %error, "{what}");
    } else {
        tracing::warn!(db, %error, "{what}");
    }
}

/// Whether an entry belongs in a table being written.
fn keep(entry: &Entry, bottom: bool, revision: u64, receipt_window: u64) -> bool {
    // A tombstone must stay while an older table might hold the key.
    if entry.value.is_none() && bottom {
        return false;
    }
    // Receipts are written once, so dropping an old one exposes nothing.
    !(is_receipt(&entry.key) && entry.version + receipt_window < revision)
}

/// The tables in runs, newest first. A run is what one merge wrote: tables
/// whose key ranges follow one another. It is read, weighed and merged as
/// the single table it would be if size allowed.
fn runs(tables: &[Arc<Table>]) -> impl Iterator<Item = &[Arc<Table>]> {
    let mut rest = tables;
    std::iter::from_fn(move || {
        let mut len = usize::from(!rest.is_empty());
        // Believed only where the keys bear it out: a server that knows
        // nothing of runs may have merged part of one.
        while len > 0
            && len < rest.len()
            && rest[len].meta.part > 0
            && rest[len - 1].meta.last < rest[len].meta.first
        {
            len += 1;
        }
        let (run, older) = rest.split_at(len);
        rest = older;
        (len > 0).then_some(run)
    })
}

/// After a merge of everything that the quota asked for and that gained
/// little, the next waits this many times as long as it took.
const RECLAIM_REST: u32 = 4;

/// How many of the newest tables to merge, if any: whole runs, taken while
/// they are together at least half the size of the next.
///
/// A read looks in every run, so no more than `max_runs` are left: with
/// more, the newest that are too many are merged first, whatever their
/// sizes and with nothing else. They are the smallest. To merge everything
/// for them rewrote a database of 16 GiB for a dozen tables of 8 MiB that
/// a server had flushed and not merged when it was killed; and to go on
/// from them by the rule above can come to the same, with every one of
/// those runs still read for as long as that takes.
///
/// `most` is how many bytes the merge may take. When the runs chosen are
/// more than that, it is of the newest of them that are not, if those are
/// two at least: they are runs the rule would have merged before it came
/// to the others.
fn merge_plan(tables: &[Arc<Table>], most: u64, max_runs: usize) -> Option<usize> {
    let max_runs = max_runs.max(1);
    let runs: Vec<(usize, u64)> = runs(tables)
        .map(|run| (run.len(), run.iter().map(|table| table.meta.size).sum()))
        .collect();
    let mut count = 1;
    if runs.len() > max_runs {
        count = runs.len() - max_runs + 1;
    } else {
        let mut size = runs.first()?.1;
        while count < runs.len() && size * 2 >= runs[count].1 {
            size += runs[count].1;
            count += 1;
        }
    }
    let size = |count: usize| runs[..count].iter().map(|(_, size)| size).sum::<u64>();
    while count > 1 && size(count) > most {
        count -= 1;
    }
    (count > 1).then(|| runs[..count].iter().map(|(tables, _)| tables).sum())
}

/// The runs a merge takes, set while it runs, whatever way it ends.
struct UnderWay<'a>(&'a AtomicUsize);

impl<'a> UnderWay<'a> {
    fn begin(taking: &'a AtomicUsize, runs: usize) -> Self {
        taking.store(runs, Ordering::Release);
        Self(taking)
    }
}

impl Drop for UnderWay<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// How many of the newest tables to merge while a merge of the `under`
/// oldest waits: of those flushed since it began, and of no more bytes
/// than `spare`, from which they are taken. None while the database has
/// no more than `max_runs` runs: what is merged behind a merge is merged
/// again with what that merge makes, and each such byte is sent to the
/// store once more. On a bucket that is as fast as its link, those bytes
/// are taken from the commits.
fn plan_behind(
    tables: &[Arc<Table>],
    under: usize,
    spare: &mut u64,
    max_runs: usize,
) -> Option<usize> {
    if runs(tables).count() <= max_runs {
        return None;
    }
    let count = merge_plan(&tables[..tables.len() - under], *spare, max_runs)?;
    *spare -= tables[..count].iter().map(|t| t.meta.size).sum::<u64>();
    Some(count)
}

/// The source holding the next key in scan order; the earliest source wins a tie.
fn first_key(heads: &[Option<Entry>], reverse: bool) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, head) in heads.iter().enumerate() {
        let Some(entry) = head else { continue };
        let better = best.is_none_or(|b| {
            let current = &heads[b].as_ref().expect("best exists").key;
            if reverse {
                entry.key > *current
            } else {
                entry.key < *current
            }
        });
        if better {
            best = Some(i);
        }
    }
    best
}

enum Source {
    Mem(std::vec::IntoIter<Entry>),
    Run(Box<RunScan>),
}

impl Source {
    async fn next(&mut self) -> Result<Option<Entry>> {
        match self {
            Self::Mem(entries) => Ok(entries.next()),
            Self::Run(scan) => scan.next().await,
        }
    }
}

/// A scan of one run. Its tables do not overlap, so they are visited one
/// after another, and one that the scan never reaches is never opened.
struct RunScan {
    io: Arc<Io>,
    /// Tables still to visit, the next one last.
    rest: Vec<Arc<Table>>,
    lo: Bytes,
    hi: Bytes,
    reverse: bool,
    /// Who reads: a merge keeps nothing of what it reads.
    reader: Reader,
    current: Option<TableScan>,
    /// The table `current` reads, and those read through before it.
    at: Option<Arc<Table>>,
    done: Vec<Arc<Table>>,
}

impl RunScan {
    /// `None` when no table of the run can hold a key the scan selects.
    fn over(io: &Arc<Io>, run: &[Arc<Table>], scan: &Scan, reader: Reader) -> Option<Self> {
        let within = |table: &&Arc<Table>| {
            let meta = &table.meta;
            meta.entries > 0
                && meta.last.as_bytes() >= scan.lo.as_ref()
                && (scan.hi.is_empty() || meta.first.as_bytes() < scan.hi.as_ref())
        };
        let mut rest: Vec<Arc<Table>> = run.iter().filter(within).cloned().collect();
        if !scan.reverse {
            rest.reverse();
        }
        (!rest.is_empty()).then(|| Self {
            io: io.clone(),
            rest,
            lo: scan.lo.clone(),
            hi: scan.hi.clone(),
            reverse: scan.reverse,
            reader,
            current: None,
            at: None,
            done: Vec::new(),
        })
    }

    /// The tables the scan has yet to read through: the one it is in and
    /// those after it.
    fn unread(&self) -> impl Iterator<Item = &Arc<Table>> {
        self.at.iter().chain(&self.rest)
    }

    /// Give up what was read ahead, and the turn held for it, while the
    /// scan waits.
    fn rest(&mut self) {
        if let Some(scan) = &mut self.current {
            scan.rest();
        }
    }

    async fn next(&mut self) -> Result<Option<Entry>> {
        loop {
            if let Some(scan) = &mut self.current
                && let Some(entry) = scan.next().await?
            {
                return Ok(Some(entry));
            }
            self.done.extend(self.at.take());
            self.at = self.rest.pop();
            let Some(table) = &self.at else {
                return Ok(None);
            };
            self.current = Some(table.scan(
                self.io.clone(),
                self.lo.clone(),
                self.hi.clone(),
                self.reverse,
                self.reader.clone(),
            ));
        }
    }
}

/// Largest response a single scan returns before asking the client to continue.
const SCAN_BYTES: usize = 8 << 20;

/// Merge the in-memory entries with the tables, newest source first.
async fn merge(
    io: &Arc<Io>,
    mem: Vec<Entry>,
    tables: &[Arc<Table>],
    scan: &Scan,
) -> Result<ScanResult> {
    if !scan.hi.is_empty() && scan.lo >= scan.hi {
        return Ok(ScanResult {
            items: Vec::new(),
            more: false,
        });
    }
    let mut sources = vec![Source::Mem(mem.into_iter())];
    sources.extend(
        runs(tables)
            .filter_map(|run| RunScan::over(io, run, scan, Reader::Request))
            .map(|run| Source::Run(Box::new(run))),
    );
    let mut heads = Vec::with_capacity(sources.len());
    for source in &mut sources {
        heads.push(source.next().await?);
    }
    let mut items = Vec::new();
    let mut bytes = 0;
    while let Some(best) = first_key(&heads, scan.reverse) {
        let entry = heads[best].take().expect("chosen head exists");
        for i in 0..sources.len() {
            if i == best
                || heads[i]
                    .as_ref()
                    .is_some_and(|other| other.key == entry.key)
            {
                heads[i] = sources[i].next().await?;
            }
        }
        if entry.value.is_none() || entry.key.first() == Some(&INTERNAL) {
            continue;
        }
        if items.len() >= scan.limit || bytes >= SCAN_BYTES {
            return Ok(ScanResult { items, more: true });
        }
        bytes += entry.weight();
        items.push(entry);
    }
    Ok(ScanResult { items, more: false })
}

/// Keys written recently, for range checks. Bounded in number and in
/// bytes: it outlives the flush of the writes it remembers, so nothing
/// else limits what it holds.
struct Recent {
    keys: BTreeMap<Bytes, u64>,
    order: VecDeque<(u64, Bytes)>,
    /// Writes at or below this revision may have been forgotten.
    horizon: u64,
    capacity: usize,
    bytes: usize,
    capacity_bytes: usize,
}

impl Recent {
    /// What remembering one write costs besides its key.
    const EACH: usize = 96;

    fn new(horizon: u64, capacity: usize, capacity_bytes: usize) -> Self {
        Self {
            keys: BTreeMap::new(),
            order: VecDeque::new(),
            horizon,
            capacity,
            bytes: 0,
            capacity_bytes,
        }
    }

    fn record(&mut self, key: Bytes, revision: u64) {
        self.bytes += key.len() + Self::EACH;
        self.keys.insert(key.clone(), revision);
        self.order.push_back((revision, key));
        self.trim();
    }

    fn trim(&mut self) {
        while self.order.len() > self.capacity || self.bytes > self.capacity_bytes {
            let Some((old, key)) = self.order.pop_front() else {
                break;
            };
            self.bytes -= key.len() + Self::EACH;
            self.horizon = self.horizon.max(old);
            if self.keys.get(&key) == Some(&old) {
                self.keys.remove(&key);
            }
        }
    }

    fn check(&self, lo: &Bytes, hi: &Bytes, since: u64) -> Result<()> {
        if since < self.horizon {
            return Err(Error::Conflict(
                "the range was read too long ago to verify; read it again".into(),
            ));
        }
        if !hi.is_empty() && lo >= hi {
            return Ok(());
        }
        let changed = if hi.is_empty() {
            self.keys
                .range(lo.clone()..)
                .any(|(_, revision)| *revision > since)
        } else {
            self.keys
                .range(lo.clone()..hi.clone())
                .any(|(_, revision)| *revision > since)
        };
        if changed {
            return Err(Error::Conflict(
                "a key in a range this transaction read has changed".into(),
            ));
        }
        Ok(())
    }
}

struct Pending {
    frame: u64,
    last_revision: u64,
    stored: bool,
}

struct Waiter {
    /// The revision that must be acknowledged before this request is answered.
    needs: u64,
    /// Whether the request added a transaction to a frame. If so, the check
    /// that acknowledges the frame also vouches for the rest of the answer.
    fresh: bool,
    reply: Reply,
    results: Vec<Result<Committed>>,
}

/// The single task that orders a database's transactions.
struct Writer {
    db: Arc<Db>,
    /// The newest revision handed out, acknowledged or not.
    assigned: u64,
    next_frame: u64,
    frames: VecDeque<Pending>,
    waiters: Vec<Waiter>,
    configs: Vec<(u64, DbConfig)>,
    checking: bool,
}

impl Writer {
    async fn run(db: Arc<Db>, mut rx: mpsc::Receiver<Cmd>, revision: u64) {
        let _fuse = Fuse(db.clone());
        let mut writer = Writer {
            db: db.clone(),
            assigned: revision,
            next_frame: 1,
            frames: VecDeque::new(),
            waiters: Vec::new(),
            configs: Vec::new(),
            checking: false,
        };
        let (stored_tx, mut stored_rx) = mpsc::unbounded_channel::<(u64, Result<()>)>();
        let (checked_tx, mut checked_rx) = mpsc::unbounded_channel::<(u64, Result<()>)>();
        let mut stopping: Option<oneshot::Sender<()>> = None;
        loop {
            if db.is_closed() {
                break;
            }
            // Frames are acknowledged in order, by a check that begins after
            // every one of them is stored.
            if !writer.checking
                && let Some(target) = writer.stored_prefix()
            {
                writer.checking = true;
                let (db, checked_tx) = (db.clone(), checked_tx.clone());
                tokio::spawn(async move {
                    let result = db.check().await;
                    let _ = checked_tx.send((target, result));
                });
            }
            if stopping.is_some() && writer.frames.is_empty() {
                break;
            }
            let room = writer.frames.len() < db.ctx.opts.max_inflight_frames;
            tokio::select! {
                biased;
                Some((frame, result)) = stored_rx.recv() => match result {
                    Ok(()) => writer.mark_stored(frame),
                    Err(error) => db.poison(error),
                },
                Some((target, result)) = checked_rx.recv() => {
                    writer.checking = false;
                    match result {
                        Ok(()) => writer.confirm(target),
                        Err(error) => db.poison(error),
                    }
                }
                _ = db.wake_writer.notified() => {}
                command = rx.recv(), if stopping.is_none() && room => {
                    let Some(first) = command else { break };
                    let mut commands = vec![first];
                    while commands.len() < 1024 {
                        match rx.try_recv() {
                            Ok(command) => commands.push(command),
                            Err(_) => break,
                        }
                    }
                    stopping = writer.intake(commands, &stored_tx).await;
                }
            }
        }
        let error = db.closed_error();
        for waiter in writer.waiters.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        rx.close();
        while let Ok(command) = rx.try_recv() {
            match command {
                Cmd::Commit { reply, .. } | Cmd::Config { reply, .. } => {
                    let _ = reply.send(Err(error.clone()));
                }
                Cmd::Close(done) => {
                    let _ = done.send(());
                }
            }
        }
        if let Some(done) = stopping {
            let _ = done.send(());
        }
    }

    fn stored_prefix(&self) -> Option<u64> {
        self.frames
            .iter()
            .take_while(|f| f.stored)
            .last()
            .map(|f| f.frame)
    }

    fn mark_stored(&mut self, frame: u64) {
        if let Some(pending) = self.frames.iter_mut().find(|f| f.frame == frame) {
            pending.stored = true;
        }
    }

    fn confirm(&mut self, target: u64) {
        let mut confirmed = None;
        while self.frames.front().is_some_and(|f| f.frame <= target) {
            confirmed = self.frames.pop_front().map(|f| f.last_revision);
        }
        let Some(revision) = confirmed else { return };
        let db = self.db.clone();
        {
            let mut state = db.state.write();
            state.confirmed = revision;
            state.confirmed_frame = target;
            while self.configs.first().is_some_and(|(at, _)| *at <= revision) {
                state.config = self.configs.remove(0).1;
            }
        }
        db.confirmed_tx.send_replace(revision);
        db.last_commit_ms
            .store(db.started.elapsed().as_millis() as u64, Ordering::Relaxed);
        self.release(revision);
        db.wake_maint.notify_one();
    }

    /// Answer every request whose transactions are all acknowledged.
    ///
    /// A refusal or a recognized retry is a statement about the current
    /// state, so like a read it is only as good as this node's ownership. A
    /// request that stored nothing is therefore answered after a claim check
    /// of its own; a stale owner reports "ownership moved", never "conflict".
    fn release(&mut self, confirmed: u64) {
        let mut unchecked = Vec::new();
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].needs <= confirmed {
                let waiter = self.waiters.swap_remove(i);
                if waiter.fresh {
                    let _ = waiter.reply.send(Ok(waiter.results));
                } else {
                    unchecked.push(waiter);
                }
            } else {
                i += 1;
            }
        }
        if !unchecked.is_empty() {
            let db = self.db.clone();
            tokio::spawn(async move {
                let checked = db.check().await;
                for waiter in unchecked {
                    let _ = waiter.reply.send(checked.clone().map(|()| waiter.results));
                }
            });
        }
    }

    /// Validate commands against the newest state, apply the survivors to the
    /// in-memory table as unacknowledged versions, and upload them as frames.
    async fn intake(
        &mut self,
        commands: Vec<Cmd>,
        stored_tx: &mpsc::UnboundedSender<(u64, Result<()>)>,
    ) -> Option<oneshot::Sender<()>> {
        let mut stopping = None;
        let mut txns: Vec<FrameTxn> = Vec::new();
        let mut first_revision = self.assigned + 1;
        let mut bytes = 0;
        for command in commands {
            if self.db.is_closed() || stopping.is_some() {
                let error = self.db.closed_error();
                match command {
                    Cmd::Commit { reply, .. } | Cmd::Config { reply, .. } => {
                        let _ = reply.send(Err(error));
                    }
                    Cmd::Close(done) => {
                        let _ = done.send(());
                    }
                }
                continue;
            }
            match command {
                Cmd::Close(done) => stopping = Some(done),
                Cmd::Config { change, reply } => {
                    let mut config = match self.configs.last() {
                        Some((_, pending)) => pending.clone(),
                        None => self.db.config(),
                    };
                    change.apply(&mut config);
                    self.assigned += 1;
                    self.configs.push((self.assigned, config.clone()));
                    txns.push(FrameTxn {
                        writes: Vec::new(),
                        config: Some(config),
                    });
                    self.waiters.push(Waiter {
                        needs: self.assigned,
                        fresh: true,
                        reply,
                        results: vec![Ok(Committed {
                            revision: self.assigned,
                        })],
                    });
                }
                Cmd::Commit {
                    txns: requested,
                    reply,
                } => {
                    let mut results = Vec::with_capacity(requested.len());
                    let mut needs = 0;
                    let mut broken = None;
                    let before = self.assigned;
                    for txn in requested {
                        if results.iter().any(Result::is_err) {
                            results.push(Err(Error::Conflict(
                                "not attempted: an earlier transaction in this request failed"
                                    .into(),
                            )));
                            continue;
                        }
                        match self.apply(txn, &mut txns, &mut bytes).await {
                            Ok(revision) => {
                                needs = needs.max(revision);
                                results.push(Ok(Committed { revision }));
                            }
                            // Storage failed underneath validation: nothing
                            // more can be decided by this writer.
                            Err(
                                error
                                @ (Error::Lost(_) | Error::Unavailable(_) | Error::Corrupt(_)),
                            ) => {
                                broken = Some(error);
                                break;
                            }
                            Err(error) => results.push(Err(error)),
                        }
                    }
                    if let Some(error) = broken {
                        self.db.poison(error.clone());
                        let _ = reply.send(Err(error));
                    } else {
                        let fresh = self.assigned > before;
                        if !fresh {
                            // Nothing was stored, so the answer describes the
                            // state it was judged against, including writes
                            // still on their way to the bucket. It waits
                            // until all of them are acknowledged.
                            needs = self.assigned;
                        }
                        self.waiters.push(Waiter {
                            needs,
                            fresh,
                            reply,
                            results,
                        });
                    }
                }
            }
            if bytes >= self.db.ctx.opts.max_frame_bytes {
                self.upload(first_revision, std::mem::take(&mut txns), stored_tx);
                first_revision = self.assigned + 1;
                bytes = 0;
            }
        }
        if !txns.is_empty() {
            self.upload(first_revision, txns, stored_tx);
        }
        self.db.newest.store(self.assigned, Ordering::Release);
        // Requests that changed nothing are answered at once.
        let confirmed = self.db.state.read().confirmed;
        self.release(confirmed);
        stopping
    }

    fn upload(
        &mut self,
        first_revision: u64,
        txns: Vec<FrameTxn>,
        stored_tx: &mpsc::UnboundedSender<(u64, Result<()>)>,
    ) {
        let frame = self.next_frame;
        self.next_frame += 1;
        self.frames.push_back(Pending {
            frame,
            last_revision: self.assigned,
            stored: false,
        });
        let body = Frame {
            first_revision,
            txns,
        }
        .encode();
        let (db, stored_tx) = (self.db.clone(), stored_tx.clone());
        tokio::spawn(async move {
            let key = db.layout.frame(db.epoch, frame);
            let result = db.uploads.put(&key, body).await;
            let _ = stored_tx.send((frame, result));
        });
    }

    /// Returns the transaction's revision: a new one, or the original's when
    /// this is a retry of a transaction already applied.
    async fn apply(&mut self, txn: Txn, out: &mut Vec<FrameTxn>, bytes: &mut usize) -> Result<u64> {
        let db = self.db.clone();
        if txn.writes.iter().any(|(key, _)| {
            key.first().is_none_or(|byte| *byte == INTERNAL) || std::str::from_utf8(key).is_err()
        }) {
            return Err(Error::Invalid(
                "a key is non-empty UTF-8 and does not begin with U+0000".into(),
            ));
        }
        let receipt = match &txn.idempotency_key {
            None => None,
            Some(id) => {
                let key = receipt_key(id);
                let digest = txn.digest();
                if let Some((version, Some(stored))) = db.latest(&key).await?
                    && version + db.ctx.opts.receipt_window >= self.assigned
                {
                    return if stored == digest {
                        Ok(version)
                    } else {
                        Err(Error::Invalid(
                            "this idempotency key was used for a different transaction".into(),
                        ))
                    };
                }
                Some((key, digest))
            }
        };
        if let Some(expected) = txn.if_revision
            && expected != self.assigned
        {
            return Err(Error::Conflict(format!(
                "the database is at revision {}",
                self.assigned
            )));
        }
        for check in &txn.checks {
            match check {
                Check::Key { key, version } => {
                    let current = match db.latest(key).await? {
                        Some((current, Some(_))) => current,
                        _ => 0,
                    };
                    if current != *version {
                        return Err(Error::Conflict(format!(
                            "key {:?} is at version {current}, not {version}",
                            String::from_utf8_lossy(key)
                        )));
                    }
                }
                Check::Range { lo, hi, since } => self.db.recent.lock().check(lo, hi, *since)?,
            }
        }
        let added: usize = txn
            .writes
            .iter()
            .filter_map(|(key, value)| value.as_ref().map(|value| key.len() + value.len()))
            .sum();
        let size: usize = added
            + txn
                .writes
                .iter()
                .filter(|(_, v)| v.is_none())
                .map(|(k, _)| k.len())
                .sum::<usize>();
        let mut state = db.state.write();
        if added > 0
            && state.table_bytes + (state.mem.bytes() + added) as u64 > state.config.max_bytes
        {
            // What was overwritten or deleted is counted until a merge has
            // removed it, and deleting counts for a little more. So a
            // database with more than one run, or with one and writes that
            // are not in a table yet, may have room once all of it is
            // merged: it is, and the write is asked for again. One with
            // nothing to merge is full.
            let merged = runs(&state.tables).count() + usize::from(!state.mem.is_empty()) < 2;
            if merged {
                return Err(Error::Quota);
            }
            if db.reclaim() {
                return Err(Error::Busy(
                    "the database is at its quota and is merging away what was overwritten or deleted; retry shortly".into(),
                ));
            }
            // The last merge made little room and the next is not due. A
            // write that waits for what has been deleted since is told so.
            if db.reclaim_deleted.load(Ordering::Acquire) {
                return Err(Error::Busy(
                    "the database is at its quota, and what was deleted from it has not been merged away yet; retry later".into(),
                ));
            }
            return Err(Error::Quota);
        }
        // Writes arriving faster than they can be flushed are refused rather
        // than accumulated without bound.
        if state.mem.bytes() > db.ctx.opts.flush_bytes.saturating_mul(4) {
            drop(state);
            db.request_flush();
            return Err(Error::Busy(
                "the database is flushing earlier writes; retry shortly".into(),
            ));
        }
        // And faster than they can be merged: every flush is a run, and a
        // read looks in each. Over a link that carried less than the merges
        // had to send, a database of 1 GiB was left with 32 runs.
        if db.crowded(state.runs) {
            return Err(Error::Busy(
                "the database is merging earlier writes; retry shortly".into(),
            ));
        }
        if size > added {
            // A deletion makes room, once it has been merged away.
            db.reclaim_deleted.store(true, Ordering::Release);
        }
        self.assigned += 1;
        let revision = self.assigned;
        let confirmed = state.confirmed;
        let mut writes = txn.writes;
        if let Some((key, digest)) = receipt {
            writes.push((key, Some(digest)));
        }
        for (key, value) in &writes {
            state
                .mem
                .put(key.clone(), revision, value.clone(), confirmed);
            self.db.recent.lock().record(key.clone(), revision);
        }
        drop(state);
        *bytes += size + 64;
        out.push(FrameTxn {
            writes,
            config: None,
        });
        Ok(revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(size: u64, first: &str, last: &str, part: u32) -> Arc<Table> {
        Table::new(TableMeta {
            key: format!("t/{first}-{last}-{size}"),
            size,
            entries: 1,
            first: first.into(),
            last: last.into(),
            part,
        })
    }

    fn lengths(tables: &[Arc<Table>]) -> Vec<usize> {
        runs(tables).map(<[_]>::len).collect()
    }

    #[test]
    fn tables_written_by_one_merge_are_one_run() {
        assert_eq!(lengths(&[]), Vec::<usize>::new());
        let tables = [
            table(10, "m", "p", 0),
            table(30, "a", "c", 0),
            table(30, "d", "f", 1),
            table(30, "g", "i", 2),
            table(500, "a", "z", 0),
        ];
        assert_eq!(lengths(&tables), [1, 3, 1]);
        // A table says it continues a run only where its keys follow the
        // last one's. A server that knows nothing of runs may have merged
        // the first tables of one, leaving the rest behind.
        let tables = [
            table(60, "a", "h", 0),
            table(30, "g", "i", 2),
            table(30, "j", "l", 3),
        ];
        assert_eq!(lengths(&tables), [1, 2]);
        let tables = [table(60, "a", "f", 0), table(30, "g", "i", 2)];
        assert_eq!(lengths(&tables), [2]);
    }

    #[test]
    fn runs_are_merged_whole_when_they_are_comparable_in_size() {
        // As many runs as a database may have: twelve.
        let limit = Options::default().max_runs;
        let run = |size: u64, pieces: u32, from: u8| -> Vec<Arc<Table>> {
            (0..pieces)
                .map(|part| {
                    let letter =
                        |offset: u32| ((from + (part * 2 + offset) as u8) as char).to_string();
                    table(size / u64::from(pieces), &letter(0), &letter(1), part)
                })
                .collect()
        };
        // One run, however many tables it has, is left alone.
        assert_eq!(merge_plan(&run(400, 4, b'a'), u64::MAX, limit), None);
        assert_eq!(merge_plan(&[], u64::MAX, limit), None);
        // A small new table waits until it is half the size of the run below.
        let small = [vec![table(100, "a", "z", 0)], run(400, 4, b'a')].concat();
        assert_eq!(merge_plan(&small, u64::MAX, limit), None);
        let grown = [vec![table(200, "a", "z", 0)], run(400, 4, b'a')].concat();
        assert_eq!(merge_plan(&grown, u64::MAX, limit), Some(5));
        // The merge takes whole runs, and as many as have become comparable.
        let three = [run(90, 3, b'a'), run(120, 2, b'a'), run(1000, 5, b'a')].concat();
        assert_eq!(merge_plan(&three, u64::MAX, limit), Some(5));
        let all = [run(300, 3, b'a'), run(400, 2, b'a'), run(1000, 5, b'a')].concat();
        assert_eq!(merge_plan(&all, u64::MAX, limit), Some(10));
        // With more runs than twelve, the newest that are too many are
        // merged whatever their sizes, and not the rest for their sake.
        let many: Vec<Arc<Table>> = (0..15).map(|i| table(1 << (2 * i), "a", "z", 0)).collect();
        assert_eq!(merge_plan(&many[3..], u64::MAX, limit), None);
        assert_eq!(merge_plan(&many[2..], u64::MAX, limit), Some(2));
        assert_eq!(merge_plan(&many, u64::MAX, limit), Some(4));
        // Nor any that the rule would take after them: of these fourteen
        // it would take all, and twelve are merged once three are one.
        let level: Vec<Arc<Table>> = (0..14).map(|_| table(8, "a", "z", 0)).collect();
        assert_eq!(merge_plan(&level, u64::MAX, limit), Some(3));
        assert_eq!(merge_plan(&level[2..], u64::MAX, limit), Some(12));
        // A merge that may take so many bytes and no more is of the
        // newest of those runs that are within them, when two are.
        assert_eq!(merge_plan(&all, 700, limit), Some(5));
        assert_eq!(merge_plan(&all, 699, limit), None);
        assert_eq!(merge_plan(&level, 23, limit), Some(2));
        assert_eq!(merge_plan(&level, 15, limit), None);
        // While a merge of the two oldest of these waits, the two flushed
        // since it began are merged and not its own, which the rule would
        // take with them; and what they take is not there for the next.
        let waiting = [run(100, 1, b'a'), run(100, 1, b'a')].concat();
        let behind = [run(40, 1, b'a'), run(40, 1, b'a'), waiting.clone()].concat();
        // With three runs at most, the four here are too many.
        let mut spare = 1000;
        assert_eq!(merge_plan(&behind, spare, limit), Some(4));
        assert_eq!(plan_behind(&behind, 2, &mut spare, 3), Some(2));
        assert_eq!(spare, 920);
        let later = [run(10, 1, b'a'), run(15, 1, b'a'), waiting].concat();
        spare = 24;
        assert_eq!(plan_behind(&later, 2, &mut spare, 3), None);
        assert_eq!(spare, 24);
        spare = 25;
        assert_eq!(plan_behind(&later, 2, &mut spare, 3), Some(2));
        assert_eq!(spare, 0);
        // No limit is less than one run.
        assert_eq!(
            merge_plan(&behind, u64::MAX, 0),
            merge_plan(&behind, u64::MAX, 1)
        );
        // With two at most, three of the four are too many, and one of
        // those is the waiting merge's: it is not taken.
        spare = 1000;
        assert_eq!(plan_behind(&behind, 2, &mut spare, 2), Some(2));
        // And none are merged behind it while the database has no more
        // runs than it may: these are four.
        spare = 1000;
        assert_eq!(plan_behind(&behind, 2, &mut spare, 4), None);
        assert_eq!(spare, 1000);
    }

    #[test]
    fn recent_writes_are_remembered_within_a_number_and_a_size() {
        let key = |i: u64, len: usize| Bytes::from(format!("{i:0len$}"));
        let (lo, hi) = (Bytes::new(), Bytes::new());
        // Few enough to remember: a range is known unchanged since 0, and
        // known changed since before the last write.
        let mut recent = Recent::new(0, 1000, 1 << 20);
        for i in 1..=1000 {
            recent.record(key(i, 16), i);
        }
        assert!(recent.check(&lo, &hi, 1000).is_ok());
        assert!(matches!(
            recent.check(&lo, &hi, 999),
            Err(Error::Conflict(_))
        ));
        assert!(recent.check(&key(0, 16), &key(1, 16), 0).is_ok());
        // One more, and the oldest is forgotten: what happened since then
        // can no longer be vouched for.
        recent.record(key(1001, 16), 1001);
        assert_eq!((recent.order.len(), recent.horizon), (1000, 1));
        assert!(recent.check(&key(0, 16), &key(1, 16), 0).is_err());
        assert!(recent.check(&key(0, 16), &key(1, 16), 1).is_ok());
        // Long keys are forgotten sooner: by size, not by number.
        let mut recent = Recent::new(0, 100_000, 1 << 20);
        for i in 1..=10_000 {
            recent.record(key(i, 1024), i);
        }
        assert!(recent.bytes <= 1 << 20);
        assert_eq!(recent.order.len(), (1 << 20) / (1024 + Recent::EACH));
        assert_eq!(recent.keys.len(), recent.order.len());
        assert_eq!(recent.horizon as usize, 10_000 - recent.order.len());
        // A key written again is remembered once, at its newest revision.
        let mut recent = Recent::new(0, 10, 1 << 20);
        for revision in 1..=25 {
            recent.record(key(revision % 3, 8), revision);
        }
        assert_eq!((recent.keys.len(), recent.order.len()), (3, 10));
        assert_eq!(recent.bytes, 10 * (8 + Recent::EACH));
    }

    #[test]
    fn a_manifest_names_a_part_only_when_there_is_one() {
        let alone = table(10, "a", "b", 0);
        let text = serde_json::to_string(&alone.meta).unwrap();
        assert!(!text.contains("part"), "{text}");
        assert_eq!(
            serde_json::from_str::<TableMeta>(&text).unwrap(),
            alone.meta
        );
        let third = table(10, "a", "b", 2);
        let text = serde_json::to_string(&third.meta).unwrap();
        assert!(text.contains("\"part\":2"), "{text}");
        assert_eq!(
            serde_json::from_str::<TableMeta>(&text).unwrap(),
            third.meta
        );
    }
}
