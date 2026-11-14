//! One open database: activation, commits, reads, checkpoints and collection.
//!
//! This file implements `spec/DurableDb.tla`. The names match: a writer
//! *claims* an epoch, *revokes* lower claims, reads the newest manifest and
//! the frames after it, and publishes its own manifest. From then on every
//! acknowledgement and every deletion waits for a claim check that began
//! after the bytes it depends on were stored.
use crate::{
    Error, Result,
    io::{Io, retry},
    layout::{
        Claim, DbConfig, Epoch, FORMAT, Frame, FrameTxn, INTERNAL, Layout, Manifest, is_receipt,
        receipt_key,
    },
    mem::Mem,
    store::{StoreError, list_all, list_from},
    table::{Entry, Table, TableBuilder, TableScan},
};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// Identical uploads that may race for one object. 1 disables racing.
    pub upload_attempts: usize,
    /// Delay before each further attempt starts; zero starts them together.
    pub hedge_after: Duration,
    /// How long a commit may wait on storage before the writer gives up.
    pub store_deadline: Duration,
    /// Retries are recognized for this many revisions.
    pub receipt_window: u64,
    pub block_bytes: usize,
    /// Keys remembered for range checks.
    pub recent_keys: usize,
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
            hedge_after: Duration::ZERO,
            store_deadline: Duration::from_secs(30),
            receipt_window: 100_000,
            block_bytes: 32 << 10,
            recent_keys: 100_000,
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
    pub epoch: String,
    pub config: DbConfig,
}

type Reply = oneshot::Sender<Result<Vec<Result<Committed>>>>;

/// Chooses the keys a purge removes.
type Doomed<'a> = &'a (dyn for<'k> Fn(&'k str) -> bool + Sync);

enum Cmd {
    Commit { txns: Vec<Txn>, reply: Reply },
    Config { config: DbConfig, reply: Reply },
    Close(oneshot::Sender<()>),
}

struct State {
    mem: Mem,
    /// Newest first.
    tables: Arc<Vec<Arc<Table>>>,
    table_bytes: u64,
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
}

pub struct Db {
    pub name: String,
    ctx: Arc<Ctx>,
    layout: Layout,
    epoch: Epoch,
    claim_key: String,
    state: RwLock<State>,
    tx: mpsc::Sender<Cmd>,
    checks: Mutex<Checks>,
    closed: OnceLock<Error>,
    closing: AtomicBool,
    flush_requested: AtomicBool,
    wake_writer: Notify,
    wake_maint: Notify,
    maint: tokio::sync::Mutex<Maint>,
    started: Instant,
    last_used_ms: AtomicU64,
    last_commit_ms: AtomicU64,
}

/// Objects above this size are uploaded once at a time: racing them would
/// spend real bandwidth, and they are never on the path of a commit.
const RACE_BYTES: usize = 256 << 10;

/// One upload, raced: up to `upload_attempts` identical PUTs, each started
/// `hedge_after` after the previous one; the first success wins. Object
/// stores have long upload tails, and a duplicate of immutable bytes under
/// our own key is harmless. Losers run to completion in the background, so
/// a connection or upload URL is never abandoned mid-request.
async fn put_raced(ctx: &Ctx, key: &str, body: &Bytes) -> Result<(), StoreError> {
    let attempts = if body.len() > RACE_BYTES {
        1
    } else {
        ctx.opts.upload_attempts.max(1)
    };
    if attempts == 1 {
        return ctx.io.store.put(key, body.clone()).await;
    }
    let (done, mut results) = mpsc::unbounded_channel();
    let start = |done: mpsc::UnboundedSender<Result<(), StoreError>>| {
        let (store, key, body) = (ctx.io.store.clone(), key.to_owned(), body.clone());
        tokio::spawn(async move {
            let _ = done.send(store.put(&key, body).await);
        });
    };
    start(done.clone());
    let mut waiting = attempts - 1;
    if ctx.opts.hedge_after.is_zero() {
        while waiting > 0 {
            waiting -= 1;
            start(done.clone());
        }
    }
    let mut running = attempts - waiting;
    let mut failure = None;
    loop {
        tokio::select! {
            result = results.recv() => match result {
                Some(Ok(())) => return Ok(()),
                Some(Err(error)) => {
                    running -= 1;
                    failure = Some(error);
                    if waiting > 0 {
                        waiting -= 1;
                        running += 1;
                        start(done.clone());
                    } else if running == 0 {
                        return Err(failure.expect("a failure was recorded"));
                    }
                }
                None => return Err(failure.unwrap_or_else(|| StoreError::Unavailable("no upload attempt".into()))),
            },
            _ = tokio::time::sleep(ctx.opts.hedge_after), if waiting > 0 => {
                waiting -= 1;
                running += 1;
                start(done.clone());
            }
        }
    }
}

/// Upload immutable bytes until they are stored. Repeats are harmless: the
/// key is ours alone and the content never changes.
async fn put_retry(ctx: &Ctx, key: &str, body: Bytes) -> Result<()> {
    let deadline = Instant::now() + ctx.opts.store_deadline;
    let mut delay = Duration::from_millis(10);
    loop {
        match put_raced(ctx, key, &body).await {
            Ok(()) => return Ok(()),
            Err(StoreError::Unavailable(reason)) if Instant::now() + delay < deadline => {
                tracing::debug!(%reason, key, "upload failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            Err(error) => return Err(error.into()),
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
                .ok_or_else(|| Error::Lost(format!("{key} was collected during recovery")))
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
        if manifests.is_empty() && claims.is_empty() && create.is_none() {
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
        store.put(&claim_key, Bytes::from(claim)).await?;

        let recovered = match Self::recover(&ctx, &layout, epoch, create).await {
            Ok(recovered) => recovered,
            Err(error) => {
                // Nothing of this epoch but its claim exists, so giving the
                // claim back is safe.
                let _ = retry(|| store.delete(&claim_key)).await;
                return Err(error);
            }
        };
        // From here the first manifest may be in flight. If this fails the
        // claim stays, exactly as after a crash: deleting it would let a
        // lower epoch serve and then be shadowed when the manifest arrives.
        Self::start(&ctx, name, &layout, epoch, recovered).await
    }

    /// Revoke lower epochs and rebuild the newest state from the bucket.
    async fn recover(
        ctx: &Arc<Ctx>,
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
                put_retry(ctx, &meta.key, body).await?;
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
        })
    }

    /// Publish this epoch's first manifest and begin serving.
    async fn start(
        ctx: &Arc<Ctx>,
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
        } = recovered;
        let manifest = Manifest {
            format: FORMAT,
            revision,
            covered: 0,
            sealed: false,
            tables: tables.iter().map(|t| t.meta.clone()).collect(),
            config: config.clone(),
        };
        put_retry(ctx, &layout.manifest(epoch, 0), manifest.encode()).await?;
        // The recovered state may end in transactions the previous owner
        // never acknowledged. They become permanent only now: this manifest
        // is stored, and if the claim still stands, every later owner starts
        // from it. Until this check nothing recovered may be shown to anyone.
        let claim_key = layout.claim(epoch);
        if !retry(|| ctx.io.store.exists(&claim_key)).await? {
            return Err(Error::Busy("ownership moved during activation".into()));
        }

        let (tx, rx) = mpsc::channel(4096);
        let db = Arc::new(Db {
            name: name.to_owned(),
            ctx: ctx.clone(),
            claim_key: layout.claim(epoch),
            layout: layout.clone(),
            epoch,
            state: RwLock::new(State {
                mem,
                table_bytes: tables.iter().map(|t| t.meta.size).sum(),
                tables: Arc::new(tables),
                confirmed: revision,
                confirmed_frame: 0,
                config,
            }),
            tx,
            checks: Mutex::default(),
            closed: OnceLock::new(),
            closing: AtomicBool::new(false),
            flush_requested: AtomicBool::new(false),
            wake_writer: Notify::new(),
            wake_maint: Notify::new(),
            maint: tokio::sync::Mutex::new(Maint {
                generation: 0,
                checked: 0,
                revision,
                covered: 0,
                sealed: false,
                next_table,
                garbage: Vec::new(),
            }),
            started: Instant::now(),
            last_used_ms: AtomicU64::new(0),
            last_commit_ms: AtomicU64::new(0),
        });
        tokio::spawn(Writer::run(db.clone(), rx, revision));
        tokio::spawn(db.clone().maintain());
        tracing::debug!(db = %db.name, epoch = %epoch, revision, created, "database opened");
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
            tracing::info!(db = %self.name, epoch = %self.epoch, %error, "database closed");
        }
        self.wake_writer.notify_one();
        self.wake_maint.notify_one();
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
            epoch: self.epoch.to_string(),
            config: state.config.clone(),
        }
    }

    pub fn config(&self) -> DbConfig {
        self.state.read().config.clone()
    }

    /// Bytes held in memory that no table contains yet.
    pub fn mem_bytes(&self) -> usize {
        self.state.read().mem.bytes()
    }

    /// Ask for a flush regardless of thresholds, to give memory back.
    pub fn request_flush(&self) {
        self.flush_requested.store(true, Ordering::Release);
        self.wake_maint.notify_one();
    }

    /// The newest version of `key`, including writes not yet acknowledged.
    async fn latest(&self, key: &[u8]) -> Result<Option<(u64, Option<Bytes>)>> {
        let (hit, tables) = {
            let state = self.state.read();
            (state.mem.get(key, u64::MAX), state.tables.clone())
        };
        if hit.is_some() {
            return Ok(hit);
        }
        for table in tables.iter() {
            if let Some(entry) = table.get(&self.ctx.io, key).await? {
                return Ok(Some((entry.version, entry.value)));
            }
        }
        Ok(None)
    }

    pub async fn read(self: &Arc<Self>, request: ReadReq) -> Result<ReadResp> {
        self.touch();
        if let Some(error) = self.closed.get() {
            return Err(error.clone());
        }
        if !request.strong && request.min_revision > self.state.read().confirmed {
            // The caller has seen a newer revision than this node has. Either
            // ownership moved, or the token is not from this database.
            self.check().await?;
            if request.min_revision > self.state.read().confirmed {
                return Err(Error::Invalid(
                    "min_revision is ahead of the database".into(),
                ));
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
        let (checked, response) = tokio::join!(check, self.read_snapshot(&request));
        checked?;
        response
    }

    async fn read_snapshot(&self, request: &ReadReq) -> Result<ReadResp> {
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
            let ranges: Vec<_> = request
                .scans
                .iter()
                .map(|scan| {
                    state
                        .mem
                        .range(&scan.lo, &scan.hi, scan.reverse, snapshot, scan.limit + 1)
                })
                .collect();
            (snapshot, state.tables.clone(), hits, ranges)
        };
        let io = &self.ctx.io;
        let values =
            futures::future::try_join_all(request.keys.iter().zip(hits).map(|(key, hit)| {
                let tables = &tables;
                async move {
                    if let Some((version, value)) = hit {
                        return Ok(value.map(|value| (version, value)));
                    }
                    for table in tables.iter() {
                        if let Some(entry) = table.get(io, key).await? {
                            return Ok(entry.value.map(|value| (entry.version, value)));
                        }
                    }
                    Ok::<_, Error>(None)
                }
            }))
            .await?;
        let mut scans = Vec::with_capacity(request.scans.len());
        for (scan, mem) in request.scans.iter().zip(ranges) {
            scans.push(merge(io, mem, &tables, scan).await?);
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

    pub async fn set_config(&self, config: DbConfig) -> Result<Committed> {
        let mut results = self.send(|reply| Cmd::Config { config, reply }).await?;
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
        // Activation ended with a claim check after this epoch's manifest
        // was stored, so what earlier epochs left behind can go.
        self.sweep().await;
        let mut unflushed = false;
        loop {
            // An idle database with nothing to flush sleeps until it is used.
            if unflushed {
                tokio::select! {
                    _ = self.wake_maint.notified() => {}
                    _ = tokio::time::sleep(self.ctx.opts.idle_flush) => {}
                }
            } else {
                self.wake_maint.notified().await;
            }
            if self.is_closed() || self.closing.load(Ordering::Acquire) {
                return;
            }
            let mut maint = self.maint.lock().await;
            unflushed = self.state.read().confirmed_frame > maint.covered;
            if self.should_flush(&maint)
                && let Err(error) = self.flush(&mut maint, false).await
            {
                tracing::warn!(db = %self.name, %error, "flush failed");
                continue;
            }
            unflushed = self.state.read().confirmed_frame > maint.covered;
            loop {
                match self.compact(&mut maint).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(error) => {
                        tracing::warn!(db = %self.name, %error, "compaction failed");
                        break;
                    }
                }
            }
        }
    }

    fn should_flush(&self, maint: &Maint) -> bool {
        let state = self.state.read();
        let pending = state.confirmed_frame - maint.covered;
        let idle =
            self.started.elapsed().as_millis() as u64 - self.last_commit_ms.load(Ordering::Relaxed);
        (pending > 0 && self.flush_requested.swap(false, Ordering::AcqRel))
            || state.mem.bytes() >= self.ctx.opts.flush_bytes
            || pending >= self.ctx.opts.flush_frames
            || (pending > 0 && idle >= self.ctx.opts.idle_flush.as_millis() as u64)
    }

    /// Move acknowledged writes from the in-memory table into a new table.
    async fn flush(self: &Arc<Self>, maint: &mut Maint, seal: bool) -> Result<()> {
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
                maint.next_table += 1;
                put_retry(&self.ctx, &meta.key, body).await?;
                tables.insert(0, Table::new(meta));
            }
        }
        self.publish(maint, tables, revision, frame, seal, Some(revision))
            .await
    }

    /// Merge the newest tables when they have grown comparable to the next.
    async fn compact(self: &Arc<Self>, maint: &mut Maint) -> Result<bool> {
        let tables = self.state.read().tables.clone();
        let Some(count) = merge_plan(&tables) else {
            return Ok(false);
        };
        let bottom = count == tables.len();
        let io = &self.ctx.io;
        let mut scans: Vec<TableScan> = tables[..count]
            .iter()
            .map(|table| table.scan(io.clone(), Bytes::new(), Bytes::new(), false))
            .collect();
        let mut heads = Vec::with_capacity(count);
        for scan in &mut scans {
            heads.push(scan.next().await?);
        }
        let mut builder = TableBuilder::new(self.ctx.opts.block_bytes);
        while let Some(best) = first_key(&heads, false) {
            let entry = heads[best].take().expect("chosen head exists");
            for i in 0..count {
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
        let mut merged: Vec<Arc<Table>> = Vec::new();
        if !builder.is_empty() {
            let (body, meta) = builder.finish(self.layout.table(self.epoch, maint.next_table));
            maint.next_table += 1;
            put_retry(&self.ctx, &meta.key, body).await?;
            merged.push(Table::new(meta));
        }
        merged.extend(tables[count..].iter().cloned());
        let (revision, covered, sealed) = (maint.revision, maint.covered, maint.sealed);
        self.publish(maint, merged, revision, covered, sealed, None)
            .await?;
        Ok(true)
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
        };
        put_retry(
            &self.ctx,
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
        {
            let mut state = self.state.write();
            state.table_bytes = tables.iter().map(|t| t.meta.size).sum();
            state.tables = Arc::new(tables);
            if let Some(upto) = flushed {
                state.mem.drain(upto);
            }
        }
        maint.checked = generation;
        maint.revision = revision;
        maint.covered = covered;
        maint.sealed = sealed;
        maint.garbage.extend(replaced);
        self.collect(maint).await;
        Ok(())
    }

    /// Delete this epoch's covered frames, older manifests and replaced
    /// tables. Failures are retried by the next collection.
    async fn collect(&self, maint: &mut Maint) {
        let store = &*self.ctx.io.store;
        let all = |_: &str| true;
        let frames = self.layout.epoch_frames(self.epoch);
        let frames_end = self.layout.frame(self.epoch, maint.covered + 1);
        let manifests = self.layout.epoch_manifests(self.epoch);
        let manifests_end = self.layout.manifest(self.epoch, maint.checked);
        let mut failed = None;
        if maint.covered > 0
            && let Err(error) = store.purge(&frames, Some(&frames_end), &all).await
        {
            failed = Some(error);
        }
        if let Err(error) = store.purge(&manifests, Some(&manifests_end), &all).await {
            failed = Some(error);
        }
        let mut remaining = Vec::new();
        for key in maint.garbage.drain(..) {
            if let Err(error) = store.purge(&key, None, &|candidate| candidate == key).await {
                failed = Some(error);
                remaining.push(key);
            }
        }
        maint.garbage = remaining;
        if let Some(error) = failed {
            tracing::debug!(db = %self.name, %error, "collection incomplete; will retry");
        }
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
        let all = |_: &str| true;
        let unreferenced = |key: &str| !referenced.contains(key);
        let jobs: [(String, String, Doomed<'_>); 4] = [
            (self.layout.claims(), self.layout.claim(self.epoch), &all),
            (
                self.layout.manifests(),
                self.layout.epoch_manifests(self.epoch),
                &all,
            ),
            (
                self.layout.frames(),
                self.layout.epoch_frames(self.epoch),
                &all,
            ),
            (
                self.layout.tables(),
                self.layout.epoch_tables(self.epoch),
                &unreferenced,
            ),
        ];
        for (prefix, end, doomed) in jobs {
            if let Err(error) = store.purge(&prefix, Some(&end), doomed).await {
                tracing::debug!(db = %self.name, %error, "sweep incomplete");
            }
        }
    }

    /// Hand the database back: finish what is in flight, store a sealed
    /// manifest, and delete the claim so the next owner opens without
    /// revoking anyone or replaying anything.
    pub async fn close(self: &Arc<Self>) {
        if self.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        let (done, stopped) = oneshot::channel();
        if self.tx.send(Cmd::Close(done)).await.is_ok() {
            let _ = stopped.await;
        }
        let mut maint = self.maint.lock().await;
        if !self.is_closed() {
            match self.flush(&mut maint, true).await {
                Ok(()) => {
                    let store = &*self.ctx.io.store;
                    if retry(|| store.delete(&self.claim_key)).await.is_ok() {
                        let _ = store
                            .purge(&self.claim_key, None, &|key| key == self.claim_key)
                            .await;
                    }
                }
                Err(error) => {
                    tracing::warn!(db = %self.name, %error, "release without a sealed manifest")
                }
            }
        }
        self.poison(Error::Busy("database is closing".into()));
    }

    /// Delete the database for good: a tombstone first, then every object.
    pub async fn destroy(self: &Arc<Self>) -> Result<()> {
        self.closing.store(true, Ordering::Release);
        let (done, stopped) = oneshot::channel();
        if self.tx.send(Cmd::Close(done)).await.is_ok() {
            let _ = stopped.await;
        }
        let _maint = self.maint.lock().await;
        // Only the owner may decide this.
        self.check().await?;
        let store = &*self.ctx.io.store;
        let tombstone = self.layout.tombstone();
        retry(|| store.put(&tombstone, Bytes::from_static(b"{}"))).await?;
        self.poison(Error::Gone);
        let doomed = |key: &str| key != tombstone;
        retry(|| store.purge(self.layout.everything(), None, &doomed)).await?;
        Ok(())
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

/// How many of the newest tables to merge, if any.
fn merge_plan(tables: &[Arc<Table>]) -> Option<usize> {
    const MAX_TABLES: usize = 12;
    let mut count = 1;
    let mut size = tables.first()?.meta.size;
    while count < tables.len() && size * 2 >= tables[count].meta.size {
        size += tables[count].meta.size;
        count += 1;
    }
    if count >= 2 {
        Some(count)
    } else if tables.len() > MAX_TABLES {
        Some(tables.len())
    } else {
        None
    }
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
    Table(TableScan),
}

impl Source {
    async fn next(&mut self) -> Result<Option<Entry>> {
        match self {
            Self::Mem(entries) => Ok(entries.next()),
            Self::Table(scan) => scan.next().await,
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
    let mut sources = vec![Source::Mem(mem.into_iter())];
    for table in tables {
        let meta = &table.meta;
        let outside = meta.entries == 0
            || meta.last.as_bytes() < scan.lo.as_ref()
            || (!scan.hi.is_empty() && meta.first.as_bytes() >= scan.hi.as_ref());
        if !outside {
            sources.push(Source::Table(table.scan(
                io.clone(),
                scan.lo.clone(),
                scan.hi.clone(),
                scan.reverse,
            )));
        }
    }
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

/// Keys written recently, for range checks.
struct Recent {
    keys: BTreeMap<Bytes, u64>,
    order: VecDeque<(u64, Bytes)>,
    /// Writes at or below this revision may have been forgotten.
    horizon: u64,
    capacity: usize,
}

impl Recent {
    fn record(&mut self, key: Bytes, revision: u64) {
        self.keys.insert(key.clone(), revision);
        self.order.push_back((revision, key));
        while self.order.len() > self.capacity {
            let (old, key) = self.order.pop_front().expect("non-empty");
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
    recent: Recent,
    frames: VecDeque<Pending>,
    waiters: Vec<Waiter>,
    configs: Vec<(u64, DbConfig)>,
    checking: bool,
}

impl Writer {
    async fn run(db: Arc<Db>, mut rx: mpsc::Receiver<Cmd>, revision: u64) {
        let capacity = db.ctx.opts.recent_keys;
        let mut writer = Writer {
            db: db.clone(),
            assigned: revision,
            next_frame: 1,
            // Nothing is known about writes before this epoch.
            recent: Recent {
                keys: BTreeMap::new(),
                order: VecDeque::new(),
                horizon: revision,
                capacity,
            },
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
                Cmd::Config { config, reply } => {
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
            let result = put_retry(&db.ctx, &key, body).await;
            let _ = stored_tx.send((frame, result));
        });
    }

    /// Returns the transaction's revision: a new one, or the original's when
    /// this is a retry of a transaction already applied.
    async fn apply(&mut self, txn: Txn, out: &mut Vec<FrameTxn>, bytes: &mut usize) -> Result<u64> {
        let db = self.db.clone();
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
                Check::Range { lo, hi, since } => self.recent.check(lo, hi, *since)?,
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
            return Err(Error::Quota);
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
            self.recent.record(key.clone(), revision);
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
