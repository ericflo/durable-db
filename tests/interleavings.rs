//! Particular orderings of storage requests, set up one request at a time.
//!
//! Each node sees the shared bucket through a `Gate` that can hold a chosen
//! request back or make it fail. Nothing here breaks the storage contract:
//! a held request is a slow one. Every test states a property and the exact
//! interleaving that once violated it, or would if a rule were dropped.
use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    db::{Check, Ctx, Db, Options, ReadReq, Scan, Txn},
    io::{AHEAD_MERGES, AHEAD_SCANS, DiskCache, Io},
    layout::{DbConfig, Layout},
    metrics::Metrics,
    node::{Limits, Node},
    store::{Listed, ObjectStore, StoreError, StoreResult, memory::MemoryStore},
    table::{Entry, Table, TableBuilder},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Put,
    PutOnce,
    Get,
    GetRange,
    Exists,
    Purge,
}

enum Action {
    /// The request is slow: it reaches storage only when released.
    Hold,
    /// The request never reaches storage and reports a passing failure,
    /// which the caller may retry.
    Fail,
    /// The request never reaches storage and reports a failure that is
    /// not retried.
    Break,
}

/// Whether a request for this key, the n-th of its kind, is the one meant.
type Matches = Box<dyn Fn(&str, u64) -> bool + Send + Sync>;

/// A read of a table: its key and, of a part of it, where that begins and
/// ends.
type Read = (String, Option<(u64, u64)>);

struct Rule {
    op: Op,
    matches: Matches,
    action: Action,
    remaining: usize,
}

/// One node's view of a shared in-memory bucket, with requests that can be
/// made slow or made to fail. Nothing here breaks the storage contract.
struct Gate {
    inner: Arc<MemoryStore>,
    rules: Mutex<Vec<Rule>>,
    held: Mutex<Vec<(Op, String, Arc<Semaphore>)>>,
    /// Every read of a table that is asked of the store.
    reads: Mutex<Vec<Read>>,
    /// Send no spare uploads, as a store with nothing to spare does, and
    /// count those that were asked for.
    no_spares: AtomicBool,
    spares_refused: AtomicUsize,
}

impl Gate {
    fn new(inner: &Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: inner.clone(),
            rules: Mutex::default(),
            held: Mutex::default(),
            reads: Mutex::default(),
            no_spares: AtomicBool::new(false),
            spares_refused: AtomicUsize::new(0),
        })
    }

    fn rule(
        &self,
        op: Op,
        action: Action,
        remaining: usize,
        matches: impl Fn(&str, u64) -> bool + Send + Sync + 'static,
    ) {
        self.rules.lock().unwrap().push(Rule {
            op,
            matches: Box::new(matches),
            action,
            remaining,
        });
    }

    fn clear(&self) {
        self.rules.lock().unwrap().clear();
    }

    fn held(&self) -> Vec<(Op, String)> {
        self.held
            .lock()
            .unwrap()
            .iter()
            .map(|(op, key, _)| (*op, key.clone()))
            .collect()
    }

    /// The tables asked for whole so far.
    fn fetched(&self) -> Vec<String> {
        let reads = self.reads.lock().unwrap();
        let whole = reads.iter().filter(|(_, part)| part.is_none());
        whole.map(|(key, _)| key.clone()).collect()
    }

    /// The parts of tables asked for so far.
    fn ranges(&self) -> Vec<(u64, u64)> {
        let reads = self.reads.lock().unwrap();
        reads.iter().filter_map(|(_, part)| *part).collect()
    }

    fn read(&self, key: &str, part: Option<(u64, u64)>) {
        if key.contains("/t/") {
            self.reads.lock().unwrap().push((key.to_owned(), part));
        }
    }

    fn release(&self, op: Op) {
        let held = self.held.lock().unwrap();
        for (_, _, gate) in held.iter().filter(|(o, _, _)| *o == op) {
            gate.add_permits(1);
        }
    }

    async fn wait_held(&self, op: Op) -> String {
        loop {
            if let Some((_, key)) = self.held().into_iter().find(|(o, _)| *o == op) {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn pass(&self, op: Op, key: &str, at: u64) -> StoreResult<()> {
        let hold = {
            let mut rules = self.rules.lock().unwrap();
            let Some(rule) = rules
                .iter_mut()
                .find(|r| r.op == op && r.remaining > 0 && (r.matches)(key, at))
            else {
                return Ok(());
            };
            rule.remaining -= 1;
            match rule.action {
                Action::Fail => return Err(StoreError::Unavailable("injected".into())),
                Action::Break => return Err(StoreError::Fatal("injected".into())),
                Action::Hold => {
                    let gate = Arc::new(Semaphore::new(0));
                    self.held
                        .lock()
                        .unwrap()
                        .push((op, key.to_owned(), gate.clone()));
                    gate
                }
            }
        };
        hold.acquire().await.unwrap().forget();
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for Gate {
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.pass(Op::Put, key, 0).await?;
        self.inner.put(key, body).await
    }
    async fn put_once(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.pass(Op::PutOnce, key, 0).await?;
        self.inner.put_once(key, body).await
    }
    async fn put_spare(&self, key: &str, body: Bytes) -> StoreResult<bool> {
        if self.no_spares.load(Ordering::SeqCst) {
            self.spares_refused.fetch_add(1, Ordering::SeqCst);
            return Ok(false);
        }
        self.put(key, body).await.map(|()| true)
    }
    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        self.read(key, None);
        self.pass(Op::Get, key, 0).await?;
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
        self.read(key, Some((start, end)));
        self.pass(Op::GetRange, key, start).await?;
        self.inner.get_range(key, start, end).await
    }
    async fn exists(&self, key: &str) -> StoreResult<bool> {
        self.pass(Op::Exists, key, 0).await?;
        self.inner.exists(key).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        self.inner.list_dirs(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> StoreResult<()> {
        self.inner.delete(key).await
    }
    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        self.pass(Op::Purge, prefix, 0).await?;
        self.inner.purge(prefix, end, doomed).await
    }
}

fn ctx_with(store: Arc<dyn ObjectStore>, opts: Options) -> Arc<Ctx> {
    Arc::new(Ctx {
        io: Io::new(store, 32 << 20, None),
        opts,
        root: "root/".into(),
    })
}

fn ctx(store: Arc<dyn ObjectStore>) -> Arc<Ctx> {
    ctx_with(store, Options::default())
}

fn b(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

fn put(key: &str, value: &str) -> Txn {
    Txn {
        writes: vec![(b(key), Some(b(value)))],
        ..Txn::default()
    }
}

async fn commit(db: &Arc<Db>, txn: Txn) -> Result<u64, Error> {
    db.commit(vec![txn]).await?.remove(0).map(|c| c.revision)
}

async fn get(db: &Arc<Db>, key: &[u8]) -> Result<Option<(u64, String)>, Error> {
    let response = db
        .read(ReadReq {
            keys: vec![Bytes::copy_from_slice(key)],
            strong: true,
            ..ReadReq::default()
        })
        .await?;
    Ok(response.values[0]
        .clone()
        .map(|(version, value)| (version, String::from_utf8(value.to_vec()).unwrap())))
}

/// How many writes and deletes the bucket has been sent.
fn written(store: &MemoryStore) -> u64 {
    use std::sync::atomic::Ordering::SeqCst;
    store.puts.load(SeqCst) + store.deletes.load(SeqCst)
}

fn claims(store: &MemoryStore) -> Vec<String> {
    store
        .keys()
        .into_iter()
        .filter(|k| k.contains("/c/"))
        .collect()
}

/// A takeover that fails after it has revoked must keep its claim.
///
///   X and Y list at the same moment and pick the same epoch number; say
///     X < Y by nonce. X's claim upload is slow.
///   Y claims, reads the state and sends its first manifest; that upload is slow.
///   Z claims above both, revokes Y, then fails to read the manifest.
///   X's claim arrives.
///   Y's manifest arrives. Y finds its claim gone and serves nothing, but
///     its manifest is in the bucket.
///
/// Had Z given its claim back, X would have found no higher claim and no
/// higher manifest, served, and acknowledged a write; Y's manifest, above
/// X's epoch, would then have hidden that write from every later owner.
#[tokio::test]
async fn an_acknowledged_write_survives_a_takeover_that_failed_after_revoking() {
    let bucket = Arc::new(MemoryStore::new());
    let node = || {
        let gate = Gate::new(&bucket);
        (gate.clone(), ctx(gate))
    };

    // An existing database, released cleanly: one sealed manifest, no claim.
    let (_, first) = node();
    let (db, _) = Db::open(first, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, put("base", "1")).await.unwrap();
    db.close().await;
    assert!(claims(&bucket).is_empty());

    // Two nodes list at the same moment; both claim uploads are slow.
    let (gate_a, ctx_a) = node();
    let (gate_b, ctx_b) = node();
    gate_a.rule(Op::PutOnce, Action::Hold, 1, |_, _| true);
    gate_b.rule(Op::PutOnce, Action::Hold, 1, |_, _| true);
    let open_a = tokio::spawn(Db::open(ctx_a, "app", None));
    let open_b = tokio::spawn(Db::open(ctx_b, "app", None));
    let claim_a = gate_a.wait_held(Op::PutOnce).await;
    let claim_b = gate_b.wait_held(Op::PutOnce).await;
    let (gate_x, open_x, gate_y, open_y) = if claim_a < claim_b {
        (gate_a, open_a, gate_b, open_b)
    } else {
        (gate_b, open_b, gate_a, open_a)
    };

    // Y's claim arrives; Y recovers and sends its first manifest, slowly.
    gate_y.rule(Op::Put, Action::Hold, 1, |key, _| key.contains("/m/"));
    gate_y.release(Op::PutOnce);
    gate_y.wait_held(Op::Put).await;

    // Z takes over from Y, revokes it, then cannot read the manifest.
    let (gate_z, ctx_z) = node();
    gate_z.rule(Op::Get, Action::Fail, usize::MAX, |key, _| {
        key.contains("/m/")
    });
    let failed = Db::open(ctx_z, "app", None).await;
    assert!(
        matches!(failed, Err(Error::Unavailable(_))),
        "Z: {:?}",
        failed.err()
    );
    // Z revoked Y, whose manifest is still on its way. Z's claim must stay
    // to keep every lower epoch, X's included, from serving.
    assert_eq!(claims(&bucket).len(), 1, "Z's claim stays");

    // X's claim arrives. Whatever X acknowledges must last.
    gate_x.release(Op::PutOnce);
    let acknowledged = match open_x.await.unwrap() {
        Ok((x, _)) => commit(&x, put("w", "acknowledged")).await.ok(),
        Err(_) => None,
    };

    // Y's manifest arrives.
    gate_y.release(Op::Put);
    assert!(open_y.await.unwrap().is_err());

    // A fresh node with nothing but the bucket.
    let (_, fresh) = node();
    let (db, _) = Db::open(fresh, "app", None).await.unwrap();
    assert_eq!(
        get(&db, b"w").await.unwrap().map(|(revision, _)| revision),
        acknowledged,
        "an acknowledged write is gone"
    );
    assert_eq!(get(&db, b"base").await.unwrap(), Some((1, "1".into())));
}

/// A range that ends before it starts is empty: a check on it passes and a
/// scan of it returns nothing. It once stopped the database's writer.
#[tokio::test]
async fn an_inverted_range_is_empty() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    assert_eq!(commit(&db, put("m", "1")).await.unwrap(), 1);
    let inverted = Txn {
        checks: vec![Check::Range {
            lo: b("b"),
            hi: b("a"),
            since: 1,
        }],
        writes: vec![(b("x"), Some(b("1")))],
        ..Txn::default()
    };
    assert_eq!(commit(&db, inverted).await.unwrap(), 2);
    let scanned = db
        .read(ReadReq {
            scans: vec![Scan {
                lo: b("z"),
                hi: b("a"),
                reverse: false,
                limit: 10,
            }],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    assert!(scanned.scans[0].items.is_empty());
    assert_eq!(commit(&db, put("y", "1")).await.unwrap(), 3);
}

/// A lookup by the writer that overlaps this owner's own merge must not be
/// mistaken for a takeover.
#[tokio::test]
async fn a_lookup_that_overlaps_a_merge_does_not_close_the_database() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx(gate.clone());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();

    // Two tables of about 512 KiB each, with the merge of them held back.
    let merges = ctx.io.merges.acquire_many(2).await.unwrap();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut noise = |len: usize| {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            out.extend_from_slice(&seed.to_le_bytes());
        }
        Bytes::from(out)
    };
    let mut versions = Vec::new();
    for (round, prefix) in ["a", "b"].into_iter().enumerate() {
        let txn = Txn {
            writes: (0..64)
                .map(|i| (b(&format!("{prefix}{i:03}")), Some(noise(8 << 10))))
                .collect(),
            ..Txn::default()
        };
        versions.push(commit(&db, txn).await.unwrap());
        db.request_flush();
        while db.info().tables != round + 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let tables: Vec<String> = bucket
        .keys()
        .into_iter()
        .filter(|k| k.contains("/t/"))
        .collect();
    assert_eq!(tables.len(), 2);
    let oldest = tables[0].clone();

    // The writer validates a version check on a key in the older table. Its
    // read of that table's first block is slow.
    let slow = oldest.clone();
    gate.rule(Op::GetRange, Action::Hold, 1, move |key, start| {
        key == slow && start == 0
    });
    let checked = Txn {
        checks: vec![Check::Key {
            key: b("a000"),
            version: versions[0],
        }],
        writes: vec![(b("z"), Some(b("1")))],
        ..Txn::default()
    };
    let pending = {
        let db = db.clone();
        tokio::spawn(async move { commit(&db, checked).await })
    };
    gate.wait_held(Op::GetRange).await;

    // Meanwhile the merge runs, publishes, and collects the tables it replaced.
    drop(merges);
    while bucket.keys().contains(&oldest) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(db.info().tables, 1);
    assert_eq!(claims(&bucket).len(), 1, "this node is still the owner");

    gate.release(Op::GetRange);
    let outcome = pending.await.unwrap();
    assert!(!db.is_closed(), "the owner closed its own database");
    assert!(outcome.is_ok(), "{outcome:?}");
}

/// A refusal describes the state it was judged against, so it is reported
/// only once that state is acknowledged. Here a compare-and-set is judged
/// against a write whose upload never completes: it must not be told
/// "conflict, the key is at version 2" when version 2 never comes to exist.
#[tokio::test]
async fn a_conflict_is_not_reported_against_a_write_that_never_commits() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let (db, _) = Db::open(ctx(gate.clone()), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    assert_eq!(commit(&db, put("k", "1")).await.unwrap(), 1);

    // A write to k whose frame upload never completes.
    gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
        key.contains("/l/")
    });
    let stuck = {
        let db = db.clone();
        tokio::spawn(async move { commit(&db, put("k", "2")).await })
    };
    gate.wait_held(Op::Put).await;

    // A compare-and-set from what every reader sees: k at version 1.
    assert_eq!(get(&db, b"k").await.unwrap(), Some((1, "1".into())));
    let cas = Txn {
        checks: vec![Check::Key {
            key: b("k"),
            version: 1,
        }],
        writes: vec![(b("k"), Some(b("3")))],
        ..Txn::default()
    };
    let refused = {
        let db = db.clone();
        tokio::spawn(async move { commit(&db, cas).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!refused.is_finished(), "answered from unacknowledged state");

    // Another node takes over; the first write was never stored.
    let (successor, _) = Db::open(ctx(Gate::new(&bucket)), "app", None)
        .await
        .unwrap();
    assert_eq!(get(&successor, b"k").await.unwrap(), Some((1, "1".into())));
    // The old owner learns it was replaced and answers neither request.
    gate.release(Op::Put);
    assert!(matches!(stuck.await.unwrap(), Err(Error::Lost(_))));
    assert!(matches!(refused.await.unwrap(), Err(Error::Lost(_))));
}

/// A scan sees every acknowledged write, whatever else is in memory.
#[tokio::test]
async fn a_scan_from_the_start_sees_unflushed_writes() {
    let (db, _) = Db::open(
        ctx(Arc::new(MemoryStore::new())),
        "app",
        Some(DbConfig::default()),
    )
    .await
    .unwrap();
    commit(&db, put("a", "old")).await.unwrap();
    db.request_flush();
    while db.info().tables != 1 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    for (i, (key, value)) in [("a", "new"), ("b", "1"), ("c", "1"), ("d", "1")]
        .into_iter()
        .enumerate()
    {
        let txn = Txn {
            idempotency_key: Some(format!("request-{i}")),
            ..put(key, value)
        };
        commit(&db, txn).await.unwrap();
    }
    let response = db
        .read(ReadReq {
            scans: vec![Scan {
                lo: Bytes::new(),
                hi: Bytes::new(),
                reverse: false,
                limit: 2,
            }],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    let items: Vec<(String, u64, String)> = response.scans[0]
        .items
        .iter()
        .map(|e| {
            let text = |bytes: &Bytes| String::from_utf8(bytes.to_vec()).unwrap();
            (text(&e.key), e.version, text(e.value.as_ref().unwrap()))
        })
        .collect();
    assert_eq!(
        items,
        [("a".into(), 2, "new".into()), ("b".into(), 3, "1".into())]
    );
}

/// Keys are UTF-8 text that does not begin with U+0000, which is reserved
/// for the database's own records. The engine refuses anything else.
#[tokio::test]
async fn a_key_that_is_not_text_is_refused() {
    let (db, _) = Db::open(
        ctx(Arc::new(MemoryStore::new())),
        "app",
        Some(DbConfig::default()),
    )
    .await
    .unwrap();
    for key in [&[0xff, 0x01][..], &[0x00, b'r'][..], &[][..]] {
        let txn = Txn {
            writes: vec![(Bytes::copy_from_slice(key), Some(b("v")))],
            ..Txn::default()
        };
        assert!(matches!(commit(&db, txn).await, Err(Error::Invalid(_))));
    }
    assert_eq!(commit(&db, put("plain", "v")).await.unwrap(), 1);
}

/// The same with two nodes and no simultaneous listing: Z fails after
/// revoking Y, then tries again, as a node does. Its second epoch must be
/// numbered above Y's, which only Z's surviving claim guarantees.
#[tokio::test]
async fn a_retried_takeover_is_numbered_above_the_epoch_it_revoked() {
    let bucket = Arc::new(MemoryStore::new());
    let node = || {
        let gate = Gate::new(&bucket);
        (gate.clone(), ctx(gate))
    };
    let (_, first) = node();
    let (db, _) = Db::open(first, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, put("base", "1")).await.unwrap();
    db.close().await;

    // Y opens; its first manifest upload is slow.
    let (gate_y, ctx_y) = node();
    gate_y.rule(Op::Put, Action::Hold, 1, |key, _| key.contains("/m/"));
    let open_y = tokio::spawn(Db::open(ctx_y, "app", None));
    let manifest_y = gate_y.wait_held(Op::Put).await;

    // Z takes over, revokes Y, and fails to read the manifest.
    let (gate_z, ctx_z) = node();
    gate_z.rule(Op::Get, Action::Fail, usize::MAX, |key, _| {
        key.contains("/m/")
    });
    assert!(matches!(
        Db::open(ctx_z.clone(), "app", None).await,
        Err(Error::Unavailable(_))
    ));

    // Storage recovers and Z tries again.
    gate_z.clear();
    let (z, _) = Db::open(ctx_z, "app", None).await.unwrap();
    let manifests_z = format!("root/db/app/m/{}/", z.info().epoch);
    assert!(
        manifests_z > manifest_y,
        "{manifests_z} is below {manifest_y}"
    );
    let acknowledged = commit(&z, put("w", "acknowledged")).await.unwrap();

    // Y's manifest arrives, and changes nothing.
    gate_y.release(Op::Put);
    assert!(open_y.await.unwrap().is_err());
    z.close().await;
    let (_, fresh) = node();
    let (db, _) = Db::open(fresh, "app", None).await.unwrap();
    assert_eq!(
        get(&db, b"w").await.unwrap(),
        Some((acknowledged, "acknowledged".into()))
    );
}

/// A creation that stopped after its claim leaves a claim and no database.
/// Requests for that name are answered from a listing: they find no
/// manifest, and write nothing, however many of them there are.
#[tokio::test]
async fn a_database_that_was_never_created_is_not_claimed() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    gate.rule(Op::Put, Action::Break, usize::MAX, |key, _| {
        key.contains("/m/")
    });
    let creation = Db::open(ctx(gate), "app", Some(DbConfig::default())).await;
    assert!(creation.is_err());
    let left = bucket.keys();
    assert_eq!(left, claims(&bucket));
    assert_eq!(left.len(), 1);

    let writes = written(&bucket);
    for _ in 0..3 {
        let opened = Db::open(ctx(bucket.clone()), "app", None).await;
        assert!(matches!(opened, Err(Error::NotFound)));
    }
    assert_eq!(bucket.keys(), left);
    assert_eq!(written(&bucket), writes);

    // Creating it still works, and removes the abandoned claim.
    let (db, created) = Db::open(ctx(bucket.clone()), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    assert!(created);
    commit(&db, put("k", "v")).await.unwrap();
    assert_eq!(claims(&bucket).len(), 1);
    assert_ne!(claims(&bucket), left);
}

const TOMBSTONE: &str = "root/db/app/m/~";

/// A deletion is a manifest of the deleting epoch. A successor that took
/// over before that manifest was stored outranks it, so nothing is deleted,
/// and no tombstone is left behind to take effect later.
#[tokio::test]
async fn a_takeover_during_a_deletion_keeps_the_database() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let (stale, fresh) = (ctx(gate.clone()), ctx(bucket.clone()));
    let (first, _) = Db::open(stale, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&first, put("a", "1")).await.unwrap();

    gate.rule(Op::Put, Action::Hold, 1, |key, _| key.contains("/m/"));
    let deleting = tokio::spawn({
        let first = first.clone();
        async move { first.destroy().await }
    });
    gate.wait_held(Op::Put).await;
    // The deletion's manifest is on its way. Another node takes over now.
    let (second, _) = Db::open(fresh.clone(), "app", None).await.unwrap();
    assert_eq!(commit(&second, put("b", "2")).await.unwrap(), 2);
    gate.release(Op::Put);
    assert!(matches!(deleting.await.unwrap(), Err(Error::Lost(_))));

    assert_eq!(commit(&second, put("c", "3")).await.unwrap(), 3);
    second.close().await;
    assert!(!bucket.keys().iter().any(|key| key == TOMBSTONE));
    let (third, _) = Db::open(fresh, "app", None).await.unwrap();
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
        assert_eq!(get(&third, key.as_bytes()).await.unwrap().unwrap().1, value);
    }
}

/// The owner stored the deletion but stopped before it could confirm it.
/// The next node to take the database finds it deleted and finishes the job.
#[tokio::test]
async fn an_unconfirmed_deletion_is_settled_by_the_next_owner() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let (db, _) = Db::open(ctx(gate.clone()), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, put("a", "1")).await.unwrap();
    gate.rule(Op::Exists, Action::Break, usize::MAX, |_, _| true);
    assert!(db.destroy().await.is_err());
    assert!(db.is_closed(), "it must not serve again");
    assert!(!bucket.keys().iter().any(|key| key == TOMBSTONE));

    assert!(matches!(
        Db::open(ctx(bucket.clone()), "app", None).await,
        Err(Error::Gone)
    ));
    assert_eq!(bucket.keys(), [TOMBSTONE]);
}

/// A deletion that is final but whose cleanup was cut short, first before
/// the tombstone and then before the purge, is finished by repeating it.
#[tokio::test]
async fn an_interrupted_cleanup_is_finished_by_repeating_the_deletion() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx(gate.clone());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, put("a", "1")).await.unwrap();

    gate.rule(Op::Put, Action::Break, usize::MAX, |key, _| {
        key.ends_with("m/~")
    });
    assert!(db.destroy().await.is_err());
    assert!(matches!(get(&db, b"a").await, Err(Error::Gone)));
    gate.clear();

    gate.rule(Op::Purge, Action::Break, usize::MAX, |_, _| true);
    assert!(Db::open(ctx.clone(), "app", None).await.is_err());
    assert!(bucket.len() > 1);
    gate.clear();

    assert!(matches!(
        Db::open(ctx.clone(), "app", None).await,
        Err(Error::Gone)
    ));
    Db::erase(&ctx, &Layout::new("root/", "app")).await.unwrap();
    assert_eq!(bucket.keys(), [TOMBSTONE]);
}

/// Writes that arrive faster than they can be flushed are refused, and a
/// deletion is such a write: it occupies memory until a table holds it.
#[tokio::test]
async fn deletions_are_held_back_while_a_flush_is_stuck() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let opts = Options {
        flush_bytes: 8 << 10,
        ..Options::default()
    };
    let (db, _) = Db::open(
        ctx_with(gate.clone(), opts),
        "app",
        Some(DbConfig::default()),
    )
    .await
    .unwrap();
    // With a table below them, deletions have to be written to one too.
    commit(&db, put("kept", "1")).await.unwrap();
    db.request_flush();
    while db.info().tables != 1 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // From here on, no table reaches the bucket.
    gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
        key.contains("/t/")
    });
    let delete = |i: usize| Txn {
        writes: vec![(b(&format!("key-{i:06}")), None)],
        ..Txn::default()
    };
    let mut accepted = 0;
    let refused = loop {
        match commit(&db, delete(accepted)).await {
            Ok(_) => accepted += 1,
            Err(error) => break error,
        }
        assert!(accepted < 2_000, "deletions were never held back");
    };
    assert!(matches!(refused, Error::Busy(_)), "{refused:?}");
    // Four times the flush size, at 138 bytes for each of these.
    assert!((100..400).contains(&accepted), "{accepted}");
    // Once the flush gets through, they are accepted again.
    gate.clear();
    gate.release(Op::Put);
    loop {
        match commit(&db, delete(accepted)).await {
            Ok(_) => break,
            Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(2)).await,
            Err(error) => panic!("{error:?}"),
        }
    }
}

/// A table is fetched whole for the disk cache and is in memory until it is
/// written there. However many tables are read at once, two are being
/// fetched; the others are fetched when they are read again.
#[tokio::test]
async fn an_empty_disk_cache_is_filled_two_tables_at_a_time() {
    const SIZE: u64 = 4096;
    // A read of this much of a table has it fetched.
    const PART: u64 = SIZE / 16;
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let io = Io::new(gate.clone(), 1 << 20, Some(disk));
    let disk = io.disk.clone().unwrap();
    let tables: Vec<String> = (0..12)
        .map(|i| format!("root/db/app/t/1/{i:08x}"))
        .collect();
    for key in &tables {
        let body = Bytes::from(vec![7u8; SIZE as usize]);
        store.put(key, body).await.unwrap();
    }
    let read_all = || async {
        for key in &tables {
            assert_eq!(
                io.range(key, SIZE, 10, 10 + PART, true)
                    .await
                    .unwrap()
                    .len() as u64,
                PART
            );
        }
    };
    gate.rule(Op::Get, Action::Hold, usize::MAX, |_, _| true);
    read_all().await;
    read_all().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(gate.held().len(), 2);
    assert_eq!(disk.used(), 0);

    gate.clear();
    gate.release(Op::Get);
    // Every table reaches the disk: a pass of reads then misses none.
    let misses = || disk.misses.load(std::sync::atomic::Ordering::Relaxed);
    let filled = async {
        loop {
            let before = misses();
            read_all().await;
            if misses() == before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), filled)
        .await
        .expect("every table reaches the disk cache");
    // A table that a read missed while it was being fetched is fetched
    // again, and one that is put there twice is counted once.
    let body = Bytes::from(vec![7u8; SIZE as usize]);
    io.remember(&tables[0], body, &[], &[]).await;
    assert_eq!(disk.used(), tables.len() as u64 * SIZE);
}

/// A server that read more than its disk cache holds fetched every table a
/// read missed, whole, into the place of one that was as much in use. With
/// a cache of 1 GiB and a database of 4 GiB it read a gigabyte a second
/// from its store, for reads that needed a block each (finding 43). A table
/// is fetched into room that is free. When there is none, the block is read.
#[tokio::test]
async fn a_table_in_use_is_not_put_out_of_the_disk_cache_for_another() {
    // Larger than the end of a table that is read for its index, so that
    // no read here is of a whole table.
    const SIZE: u64 = 400 << 10;
    // A read of this much of a table has it fetched, where there is room.
    const PART: u64 = SIZE / 16;
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let dir = tempfile::tempdir().unwrap();
    let tables: Vec<String> = (0..8).map(|i| format!("root/db/app/t/1/{i:08x}")).collect();
    for key in &tables {
        let body = Bytes::from(vec![7u8; SIZE as usize]);
        store.put(key, body).await.unwrap();
    }
    // Room for three of the eight.
    let disk = DiskCache::open(dir.path(), 3 * SIZE + 1000).unwrap();
    let io = Io::new(gate.clone(), 1 << 20, Some(disk));
    let disk = io.disk.clone().unwrap();
    let read_all = || async {
        for key in &tables {
            assert_eq!(
                io.range(key, SIZE, 10, 10 + PART, true)
                    .await
                    .unwrap()
                    .len() as u64,
                PART
            );
        }
    };
    // Reads until `count` tables have been fetched and three are in the cache.
    let fetched = |count: usize| {
        let (gate, disk, read_all) = (&gate, &disk, &read_all);
        async move {
            let reached = async {
                while gate.fetched().len() < count || disk.used() < 3 * SIZE {
                    read_all().await;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(60), reached)
                .await
                .expect("tables are fetched into free room");
        }
    };
    fetched(3).await;
    // Each of the three was fetched once, and the five are not fetched
    // however often they are read.
    let first = gate.fetched();
    for _ in 0..20 {
        read_all().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(gate.fetched(), first);
    assert_eq!(first.len(), 3);
    assert_eq!(disk.used(), 3 * SIZE);
    // The three are read from the disk and the five from the store, the
    // bytes that were asked for.
    let count = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed);
    let (hits, misses, asked) = (count(&disk.hits), count(&disk.misses), gate.ranges().len());
    read_all().await;
    assert_eq!(
        (count(&disk.hits), count(&disk.misses)),
        (hits + 3, misses + 5)
    );
    assert_eq!(gate.ranges()[asked..], [(10, 10 + PART); 5]);

    // A table this server wrote is taken, and the least recently used of
    // the three makes way for it. That one is not fetched back.
    let written = "root/db/app/t/1/000000ff".to_owned();
    io.remember(&written, Bytes::from(vec![9u8; SIZE as usize]), &[], &[])
        .await;
    assert_eq!(disk.used(), 3 * SIZE);
    for _ in 0..20 {
        read_all().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(gate.fetched(), first);
    // What a merge replaced leaves its room to a table that is read.
    io.discard(&[written]).await;
    assert_eq!(disk.used(), 2 * SIZE);
    fetched(4).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(gate.fetched().len(), 4);
    assert_eq!(disk.used(), 3 * SIZE);

    // A table that a read asked for whole is kept by the same rule: not in
    // the place of one in use, and at once where there is room.
    let whole = gate.fetched();
    let absent = tables.iter().find(|key| !whole.contains(key)).unwrap();
    let all = io.range(absent, SIZE, 0, SIZE, true).await.unwrap();
    assert_eq!(all.len() as u64, SIZE);
    let misses = count(&disk.misses);
    assert_eq!(
        io.range(absent, SIZE, 10, 20, true).await.unwrap().len(),
        10
    );
    assert_eq!(count(&disk.misses), misses + 1);
    assert_eq!(disk.used(), 3 * SIZE);
    io.discard(&whole[3..]).await;
    assert_eq!(disk.used(), 2 * SIZE);
    let all = io.range(absent, SIZE, 0, SIZE + 100, true).await.unwrap();
    assert_eq!(all.len() as u64, SIZE);
    assert_eq!(disk.used(), 3 * SIZE);
    assert_eq!(
        io.range(absent, SIZE, 10, 20, true).await.unwrap().len(),
        10
    );
    assert_eq!(count(&disk.misses), misses + 2);
    assert_eq!(gate.fetched(), whole);
}

/// A merge reads each of its tables once, and they leave the disk cache
/// when it is done. A server that merges tables it does not have there,
/// such as those an earlier server wrote, reads them from the store and
/// does not keep them.
#[tokio::test]
async fn a_merge_does_not_keep_the_tables_it_reads() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    // An earlier server flushed one table.
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    db.close().await;
    // This one has an empty disk cache.
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: small_tables(),
        root: "root/".into(),
    });
    let cached = || std::fs::read_dir(dir.path()).unwrap().count();
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    assert_eq!(cached(), 0);
    // Its flush is followed by a merge of both tables. The first table the
    // merge stores is held: it has read from both by then.
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    gate.wait_held(Op::Put).await;
    // The table it flushed is in its cache, and the other is not.
    assert_eq!(cached(), 1);
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while db.info().tables < 4 || cached() != db.info().tables {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the cache holds the tables of the manifest and no others");
    every_key_is_of(&db, 2).await;
}

/// A table that a merge stores is put into the disk cache, and the least
/// recently used made way for it. Those were the tables the merge had not
/// come to yet, which it then read from the store: a merge of a whole
/// database, with a cache as large as that database, fetched most of it
/// again. A table the merge is coming to does not make way: one it has
/// read through does, and when there is none the table is not kept.
#[tokio::test]
async fn a_merge_puts_out_the_tables_it_has_read_and_not_those_it_is_coming_to() {
    // How large a table a flush of each makes.
    let flush_bytes = |txn: Txn| async move {
        let ctx = ctx_with(Arc::new(MemoryStore::new()), small_tables());
        let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
            .await
            .unwrap();
        commit(&db, txn).await.unwrap();
        db.request_flush();
        flushed(&db).await;
        let bytes = db.info().bytes;
        db.close().await;
        bytes
    };
    let (round_bytes, wide_bytes) = (flush_bytes(round(1)).await, flush_bytes(wide(3)).await);
    assert!(wide_bytes > 256 << 10);
    let small = || put("x", &noise(9, 4500));
    let other_bytes = flush_bytes(small()).await;

    // A cache of the three and a table and a half of a merge: more than
    // the first merge reads and stores.
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let dir = tempfile::tempdir().unwrap();
    let table_bytes = small_tables().table_bytes as u64;
    let budget = wide_bytes + round_bytes + other_bytes + 3 * table_bytes / 2;
    let disk = DiskCache::open(dir.path(), budget).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: small_tables(),
        root: "root/".into(),
    });
    let disk = ctx.io.disk.clone().unwrap();
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    let merged = |more_than: usize| {
        let (db, disk) = (db.clone(), disk.clone());
        async move {
            let merging = async {
                while db.mem_bytes() > 0 || db.info().tables <= more_than {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(5), merging)
                .await
                .expect("the flush is followed by a merge");
            assert!(disk.used() <= budget);
            db.info().tables
        }
    };
    // Two rounds are merged into a run of several tables, which the cache
    // holds when the two are gone.
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    commit(&db, round(2)).await.unwrap();
    let run = merged(2).await;
    assert!(run >= 6);
    let left = |most: u64| {
        let disk = disk.clone();
        async move {
            let leaving = async {
                while disk.used() > most {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let left = tokio::time::timeout(Duration::from_secs(5), leaving).await;
            assert!(left.is_ok(), "the cache holds {} of {most}", disk.used());
        }
    };
    left(db.info().bytes).await;
    assert_eq!(disk.used(), db.info().bytes);
    // Another database has a small table there.
    let (other, _) = Db::open(ctx, "other", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&other, small()).await.unwrap();
    other.request_flush();
    flushed(&other).await;
    assert_eq!(disk.used(), db.info().bytes + other_bytes);

    // The wide flush fills the cache but for the table and a half. The
    // merge of it and the run reads the wide table first, and has stored
    // ten times the tables of the run before it reads more of the run
    // than its first table. The first of those has room. The second
    // takes that of the other database's table, which is all the cache
    // holds that the merge is not coming to and has not stored. Those
    // after it are not kept, until the merge has read the wide table
    // through and they take its room.
    let room = budget - wide_bytes - disk.used();
    assert!(room > table_bytes + 1000);
    hold_table(&gate, "app", 2);
    commit(&db, wide(3)).await.unwrap();
    // While its first table is on its way, a write is made that the merge
    // gives way to when the table is stored. The table of that flush
    // would fit where the other database's is and the merge's first: it
    // does not fit beside the first, and puts out neither that nor a
    // table the merge has yet to read.
    gate.wait_held(Op::Put).await;
    let more = noise(7, (room + other_bytes) as usize - 1000);
    commit(&db, put("more", &more)).await.unwrap();
    db.request_flush();
    gate.release(Op::Put);
    assert!(merged(10 * run).await > 10 * run);
    left(db.info().bytes - wide_bytes / 2).await;
    assert!(disk.used() > 0);
    // Each table was in the cache until the merge had read it, and the
    // first it stored is there still: the store was asked for no part of
    // a table.
    assert_eq!(get(&db, b"a0000").await.unwrap().unwrap().1, value(3, 0));
    assert_eq!(gate.ranges(), []);
    assert!(gate.fetched().is_empty());
    assert_eq!(get(&db, b"more").await.unwrap().unwrap().1, more);
    other.close().await;
    let scan = |lo: &str| {
        let scan = Scan {
            lo: b(lo),
            hi: Bytes::new(),
            reverse: false,
            limit: 1000,
        };
        ReadReq {
            scans: vec![scan],
            strong: true,
            ..ReadReq::default()
        }
    };
    let items = &db.read(scan("")).await.unwrap().scans[0].items;
    assert_eq!(items.len(), 1000);
    assert_eq!(items[999].value, Some(b(&value(3, 999))));
    let items = &db.read(scan("k0199")).await.unwrap().scans[0].items;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].value, Some(b(&value(2, 199))));
    assert_eq!(items[1].value, Some(b(&more)));
}

/// A merge of tables that are in the disk cache leaves its result there,
/// and with it the tables of other databases. The tables it has read
/// through make way for those it stores: they are gone when it is
/// published. Left to the order of their use, the tables it stored made
/// way for one another and those it had read stayed to the end, so that a
/// merge of a whole database left half of it in the cache, and put out
/// what else was in use.
#[tokio::test]
async fn a_merge_of_tables_in_the_cache_leaves_its_result_and_the_others_there() {
    // A quarter of the keys of a round.
    let part = |round: u64| {
        let writes = (0..50).map(|i| (b(&format!("k{i:04}")), Some(b(&value(round, i)))));
        Txn {
            writes: writes.collect(),
            ..Txn::default()
        }
    };
    // How large a table a flush of each makes.
    let flush_bytes = |txn: Txn| async move {
        let ctx = ctx_with(Arc::new(MemoryStore::new()), small_tables());
        let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
            .await
            .unwrap();
        commit(&db, txn).await.unwrap();
        db.request_flush();
        flushed(&db).await;
        let bytes = db.info().bytes;
        db.close().await;
        bytes
    };
    let (round_bytes, part_bytes) = (flush_bytes(round(1)).await, flush_bytes(part(2)).await);

    // A cache of two rounds, the quarter and half a table of a merge.
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let dir = tempfile::tempdir().unwrap();
    let table_bytes = small_tables().table_bytes as u64;
    let budget = 2 * round_bytes + part_bytes + table_bytes / 2;
    let disk = DiskCache::open(dir.path(), budget).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: Options {
            quota_rest: Duration::ZERO,
            ..small_tables()
        },
        root: "root/".into(),
    });
    let disk = ctx.io.disk.clone().unwrap();
    let config = DbConfig {
        max_bytes: 44 << 10,
        ..DbConfig::default()
    };
    let (db, _) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    // A write that the quota has no room for has everything merged.
    let merged = |from: usize| {
        let db = db.clone();
        async move {
            let more = put("more", &noise(7, 13_000));
            assert!(matches!(commit(&db, more).await, Err(Error::Busy(_))));
            let merging = async {
                while db.info().tables == from {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(5), merging)
                .await
                .expect("the tables are merged for the quota");
            db.info().tables
        }
    };
    // What the cache holds once the tables a merge replaced have left it.
    let holds = |bytes: u64| {
        let disk = disk.clone();
        async move {
            let leaving = async {
                while disk.used() != bytes {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let left = tokio::time::timeout(Duration::from_secs(5), leaving).await;
            assert!(left.is_ok(), "the cache holds {} of {bytes}", disk.used());
        }
    };
    // A round and a quarter of it again are merged into a run of several
    // tables, which the cache has room for beside the two.
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    commit(&db, part(2)).await.unwrap();
    db.request_flush();
    flushed(&db).await;
    assert_eq!(db.info().tables, 2);
    let run = merged(2).await;
    assert!(run >= 6);
    holds(db.info().bytes).await;

    // Another database's table and a quarter of the first again fill the
    // cache but for the half table.
    let (other, _) = Db::open(ctx, "other", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&other, round(1)).await.unwrap();
    flushed(&other).await;
    commit(&db, part(3)).await.unwrap();
    db.request_flush();
    flushed(&db).await;
    assert_eq!(db.info().tables, run + 1);
    let all = |db: &Arc<Db>, other: &Arc<Db>| db.info().bytes + other.info().bytes;
    assert_eq!(disk.used(), all(&db, &other));
    assert!(budget - disk.used() < table_bytes);

    // The run and the quarter are merged. Each table of the result takes
    // the room of a table of the run that the merge has read through, and
    // the other database's table stays: the cache holds all of both.
    assert_eq!(merged(run + 1).await, run);
    holds(all(&db, &other)).await;
    // The store was asked for no part of a table.
    assert_eq!(gate.ranges(), []);
    assert!(gate.fetched().is_empty());
    assert_eq!(get(&db, b"k0007").await.unwrap().unwrap().1, value(3, 7));
    assert_eq!(get(&db, b"k0107").await.unwrap().unwrap().1, value(1, 107));
    assert_eq!(
        get(&other, b"k0107").await.unwrap().unwrap().1,
        value(1, 107)
    );
}

/// The tables a merge has stored are read by nothing until it is
/// published. On a slow store that is longer than the five minutes after
/// which a table counts as unread, and its first tables then made way for
/// its later ones, before the tables it had read through. They are in use
/// for as long as the merge is.
#[tokio::test]
async fn a_merge_that_outlasts_the_time_a_table_is_in_use_keeps_its_result() {
    // A quarter of the keys of a round.
    let part = |round: u64| {
        let writes = (0..50).map(|i| (b(&format!("k{i:04}")), Some(b(&value(round, i)))));
        Txn {
            writes: writes.collect(),
            ..Txn::default()
        }
    };
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let dir = tempfile::tempdir().unwrap();
    let opts = || Options {
        quota_rest: Duration::ZERO,
        ..small_tables()
    };
    // A write that the quota has no room for has everything merged.
    async fn merged(db: &Arc<Db>, from: usize) -> usize {
        let more = put("more", &noise(7, 13_000));
        assert!(matches!(commit(db, more).await, Err(Error::Busy(_))));
        let merging = async {
            while db.info().tables == from {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), merging)
            .await
            .expect("the tables are merged for the quota");
        db.info().tables
    }
    // An earlier server, with room to spare in its cache, merged a round
    // and a quarter of it into a run of several tables, and flushed the
    // quarter again.
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: opts(),
        root: "root/".into(),
    });
    let config = DbConfig {
        max_bytes: 44 << 10,
        ..DbConfig::default()
    };
    let (db, _) = Db::open(ctx, "app", Some(config)).await.unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    commit(&db, part(2)).await.unwrap();
    db.request_flush();
    flushed(&db).await;
    let run = merged(&db, 2).await;
    assert!(run >= 6);
    commit(&db, part(3)).await.unwrap();
    db.request_flush();
    flushed(&db).await;
    let held = db.info().bytes;
    db.close().await;

    // This one's cache holds those tables and a table and a half more,
    // and what goes unused for five milliseconds counts as unread.
    let table_bytes = small_tables().table_bytes as u64;
    let idle = Duration::from_millis(5);
    let disk = DiskCache::open_idle(dir.path(), held + 3 * table_bytes / 2, idle).unwrap();
    assert_eq!(disk.used(), held);
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: opts(),
        root: "root/".into(),
    });
    let disk = ctx.io.disk.clone().unwrap();
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    assert_eq!(db.info().tables, run + 1);
    // Each table it uploads takes up to 30 ms.
    gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
        key.contains("/db/app/t/")
    });
    let slow = gate.clone();
    let store = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(30)).await;
            slow.release(Op::Put);
        }
    });
    assert_eq!(merged(&db, run + 1).await, run);
    store.abort();
    // The cache holds all of the result, and the store was asked for no
    // part of a table.
    let left = async {
        while disk.used() != db.info().bytes {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let left = tokio::time::timeout(Duration::from_secs(5), left).await;
    let bytes = db.info().bytes;
    assert!(left.is_ok(), "the cache holds {} of {bytes}", disk.used());
    assert_eq!(gate.ranges(), []);
    assert!(gate.fetched().is_empty());
    assert_eq!(get(&db, b"k0007").await.unwrap().unwrap().1, value(3, 7));
    assert_eq!(get(&db, b"k0107").await.unwrap().unwrap().1, value(1, 107));
}

/// A table that reads missed is fetched in the background, and a merge
/// may replace it meanwhile. It is not kept when it arrives: nothing reads
/// it again, and it would count as in use for as long as a table that is
/// read. After a merge of everything in a cache of the database's size,
/// one such table kept a table of the result out for five minutes.
#[tokio::test]
async fn a_fetch_that_arrives_when_its_table_has_left_the_manifest_is_not_kept() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    // An earlier server flushed one table.
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, wide(1)).await.unwrap();
    flushed(&db).await;
    db.close().await;
    // This one has an empty disk cache. A read has the table's end from
    // the store, more than a sixteenth of it, and the table is asked for
    // whole. That request is slow.
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: small_tables(),
        root: "root/".into(),
    });
    let disk = ctx.io.disk.clone().unwrap();
    let cached = || std::fs::read_dir(dir.path()).unwrap().count();
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    gate.rule(Op::Get, Action::Hold, 1, |key, _| key.contains("/t/"));
    assert_eq!(get(&db, b"a0007").await.unwrap().unwrap().1, value(1, 7));
    gate.wait_held(Op::Get).await;
    assert_eq!(cached(), 0);
    // Every key is written again, and the flush is followed by a merge
    // of both tables, which leave the manifest.
    commit(&db, wide(2)).await.unwrap();
    let merged = async {
        while db.mem_bytes() > 0 || db.info().tables < 3 || disk.used() != db.info().bytes {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the cache holds the tables of the manifest and no others");
    // The table arrives, and is not kept.
    gate.release(Op::Get);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(gate.fetched().len(), 1);
    assert_eq!((disk.used(), cached()), (db.info().bytes, db.info().tables));
    assert_eq!(get(&db, b"a0007").await.unwrap().unwrap().1, value(2, 7));
}

/// A merge that fails has stored some of its tables, and what a server
/// stores is in its disk cache. The next manifest lists none of them, and
/// they leave the cache as they leave the bucket, with what a merge that
/// succeeds replaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tables_that_no_manifest_lists_leave_the_disk_cache() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let ctx = Arc::new(Ctx {
        io: Io::new(gate.clone(), 32 << 20, Some(disk)),
        opts: small_tables(),
        root: "root/".into(),
    });
    let disk = ctx.io.disk.clone().unwrap();
    let cached = || std::fs::read_dir(dir.path()).unwrap().count();
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush gets its table through, and the merge its first.
    // The merge's second is refused, and every table after that.
    let tables = Arc::new(AtomicUsize::new(0));
    let seen = tables.clone();
    gate.rule(Op::Put, Action::Break, usize::MAX, move |key, _| {
        key.contains("/t/") && seen.fetch_add(1, Ordering::SeqCst) >= 2
    });
    commit(&db, round(2)).await.unwrap();
    flushed(&db).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!((db.info().tables, cached()), (2, 3));
    // Storage recovers, and the next write brings a flush, whose manifest
    // does not list the table that merge left, and the merge.
    gate.clear();
    commit(&db, round(3)).await.unwrap();
    let merged = async {
        while db.mem_bytes() > 0
            || db.info().tables < 4
            || cached() != db.info().tables
            || disk.used() != db.info().bytes
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the cache holds the tables of the manifest and no others");
    every_key_is_of(&db, 3).await;
}

/// A table that reads miss is fetched whole once they have had a sixteenth
/// of it from the store, and until then they are given the bytes they asked
/// for. A table of 32 MiB that was fetched for one read of a value of 4 KiB
/// was 8,000 times what was read; this way no fetch is more than sixteen.
#[tokio::test]
async fn a_table_is_fetched_when_reads_have_had_a_sixteenth_of_it() {
    const SIZE: u64 = 400 << 10;
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let dir = tempfile::tempdir().unwrap();
    let tables: Vec<String> = (0..2).map(|i| format!("root/db/app/t/1/{i:08x}")).collect();
    for key in &tables {
        let body = Bytes::from(vec![7u8; SIZE as usize]);
        store.put(key, body).await.unwrap();
    }
    // Room for both.
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let io = Io::new(gate.clone(), 1 << 20, Some(disk));
    let disk = io.disk.clone().unwrap();
    // 255 reads of a hundred bytes are just short of a sixteenth. What a
    // merge reads meanwhile is not counted, however much it is, and
    // neither is what is read of another table.
    for i in 0..255 {
        if i == 254 {
            for _ in 0..4 {
                let read = io.range(&tables[0], SIZE, 0, SIZE / 2, false);
                assert_eq!(read.await.unwrap().len() as u64, SIZE / 2);
            }
            let read = io.range(&tables[1], SIZE, 0, 25_500, true);
            assert_eq!(read.await.unwrap().len(), 25_500);
        }
        let read = io.range(&tables[0], SIZE, i * 100, i * 100 + 100, true);
        assert_eq!(read.await.unwrap().len(), 100);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(gate.fetched().is_empty());
    assert_eq!(disk.used(), 0);
    assert_eq!(gate.ranges().len(), 260);
    // The read that makes it a sixteenth has the table fetched.
    let read = io.range(&tables[0], SIZE, 0, 100, true);
    assert_eq!(read.await.unwrap().len(), 100);
    let there = async {
        while disk.used() < SIZE {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), there)
        .await
        .expect("the table reaches the disk cache");
    assert_eq!(gate.fetched(), [tables[0].clone()]);
    // Once it has left the cache, it is counted from the beginning.
    assert!(io.forget(&tables[0]).await);
    for i in 0..255 {
        let read = io.range(&tables[0], SIZE, i * 100, i * 100 + 100, true);
        assert_eq!(read.await.unwrap().len(), 100);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(gate.fetched().len(), 1);
    assert_eq!(disk.used(), 0);
}

/// A fetch that fails gives back the room it was promised, and the table
/// is fetched after a later read of it.
#[tokio::test]
async fn a_fetch_that_fails_gives_its_room_back() {
    const SIZE: u64 = 400 << 10;
    const PART: u64 = SIZE / 16;
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let dir = tempfile::tempdir().unwrap();
    let key = "root/db/app/t/1/00000001";
    let body = Bytes::from(vec![7u8; SIZE as usize]);
    store.put(key, body).await.unwrap();
    // Room for the one table and no more.
    let disk = DiskCache::open(dir.path(), SIZE + 1000).unwrap();
    let io = Io::new(gate.clone(), 1 << 20, Some(disk));
    let disk = io.disk.clone().unwrap();
    gate.rule(Op::Get, Action::Break, 2, |_, _| true);
    let there = async {
        while disk.used() < SIZE {
            assert_eq!(
                io.range(key, SIZE, 10, 10 + PART, true)
                    .await
                    .unwrap()
                    .len() as u64,
                PART
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), there)
        .await
        .expect("the table is fetched once the store gives it");
    // Two fetches that failed, and the one that did not.
    assert_eq!(gate.fetched().len(), 3);
}

/// A table of `count` entries, `k000000` and on, each with a value of
/// `value_bytes` that does not compress, in blocks of `block_bytes`.
async fn stored_table(
    store: &MemoryStore,
    count: usize,
    value_bytes: usize,
    block_bytes: usize,
) -> Arc<Table> {
    let mut builder = TableBuilder::new(block_bytes);
    for i in 0..count {
        builder.add(&Entry {
            key: b(&format!("k{i:06}")),
            version: 1,
            value: Some(b(&noise(i as u64 + 1, value_bytes))),
        });
    }
    let (body, meta) = builder.finish(format!("root/db/app/t/1/{count:08x}"));
    store.put(&meta.key, body).await.unwrap();
    Table::new(meta)
}

fn keys(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("k{i:06}")).collect()
}

async fn scanned(
    table: &Arc<Table>,
    io: &Arc<Io>,
    (lo, hi): (&str, &str),
    reverse: bool,
    keep: bool,
) -> Vec<String> {
    let mut scan = table.scan(io.clone(), b(lo), b(hi), reverse, keep);
    let mut keys = Vec::new();
    while let Some(entry) = scan.next().await.unwrap() {
        keys.push(String::from_utf8(entry.key.to_vec()).unwrap());
    }
    keys
}

const ALL: (&str, &str) = ("", "");

/// A scan that asked for one block at a time asked a bucket a thousand
/// times for a table of 32 MiB, and a merge that is not read from the disk
/// cache is such a scan of every table it merges. A scan asks for one
/// block, then for two, then for four, up to 64 and to 1 MiB, and for none
/// past its own end.
#[tokio::test]
async fn a_scan_reads_ahead_of_itself_and_not_past_its_end() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let io = Io::new(gate.clone(), 1 << 20, None);
    // 2,000 entries of 1 KiB, five to a block: 400 blocks, some 2 MiB.
    let table = stored_table(&store, 2000, 1000, 4096).await;
    assert!(table.meta.size > 1 << 20);
    // The first read of a table is of its end, where its index is, and
    // the blocks that came with that are kept.
    assert_eq!(
        scanned(&table, &io, ("", "k000001"), false, false).await,
        keys(0..1)
    );
    let held = io.cache_bytes();
    let lengths = |from: usize| -> Vec<u64> {
        let ranges = gate.ranges();
        ranges[from..]
            .iter()
            .map(|(start, end)| end - start)
            .collect()
    };

    // The whole table, forward: each read begins where the last ended and
    // is at most twice as long, the first is one block and the longest 64.
    let asked = gate.ranges().len();
    assert_eq!(scanned(&table, &io, ALL, false, false).await, keys(0..2000));
    let ranges = gate.ranges()[asked..].to_vec();
    assert!(ranges.len() >= 8 && ranges.len() <= 14, "{ranges:?}");
    assert_eq!(ranges[0].0, 0);
    assert!(ranges[0].1 < 6000, "{ranges:?}");
    for pair in ranges.windows(2) {
        assert_eq!(pair[1].0, pair[0].1, "{ranges:?}");
        let (last, next) = (pair[0].1 - pair[0].0, pair[1].1 - pair[1].0);
        assert!(next <= 2 * last + 6000, "{ranges:?}");
    }
    let longest = lengths(asked).into_iter().max().unwrap();
    assert!(longest > 32 * 5000 && longest <= 64 * 5200, "{ranges:?}");

    // In reverse: each read ends where the last began.
    let asked = gate.ranges().len();
    let mut reversed = keys(0..2000);
    reversed.reverse();
    assert_eq!(scanned(&table, &io, ALL, true, false).await, reversed);
    let ranges = gate.ranges()[asked..].to_vec();
    assert!(ranges.len() >= 8 && ranges.len() <= 14, "{ranges:?}");
    for pair in ranges.windows(2) {
        assert_eq!(pair[1].1, pair[0].0, "{ranges:?}");
    }
    assert_eq!(ranges.last().unwrap().0, 0);

    // Forty entries: the blocks that hold them and the one after, which
    // shows the scan its end. Reading on as far as the last read was long
    // would be half as much again.
    for reverse in [false, true] {
        let asked = gate.ranges().len();
        let mut selected = keys(100..140);
        if reverse {
            selected.reverse();
        }
        let found = scanned(&table, &io, ("k000100", "k000140"), reverse, false).await;
        assert_eq!(found, selected);
        let ranges = gate.ranges()[asked..].to_vec();
        assert!(ranges.len() <= 4, "{ranges:?}");
        assert!(lengths(asked).iter().sum::<u64>() < 52_000, "{ranges:?}");
    }
    // A merge reads so, and what it read is not kept in memory.
    assert_eq!(io.cache_bytes(), held);
    // What a request scans is, and the store is not asked for it again.
    let range = ("k000100", "k000140");
    assert_eq!(
        scanned(&table, &io, range, false, true).await,
        keys(100..140)
    );
    assert!(io.cache_bytes() > held + 40_000);
    let asked = gate.ranges().len();
    assert_eq!(
        scanned(&table, &io, range, false, true).await,
        keys(100..140)
    );
    assert_eq!(gate.ranges().len(), asked);

    // Blocks of 100 KiB: a read is of no more than 1 MiB of them.
    let large = stored_table(&store, 60, 100_000, 4096).await;
    let asked = gate.ranges().len();
    assert_eq!(scanned(&large, &io, ALL, false, false).await, keys(0..60));
    let forward = lengths(asked + 1);
    let longest = *forward.iter().max().unwrap();
    assert!(longest > 3 << 18 && longest <= 1 << 20, "{forward:?}");
    let asked = gate.ranges().len();
    let mut reversed = keys(0..60);
    reversed.reverse();
    assert_eq!(scanned(&large, &io, ALL, true, false).await, reversed);
    let backward = lengths(asked);
    let longest = *backward.iter().max().unwrap();
    assert!(longest > 3 << 18 && longest <= 1 << 20, "{backward:?}");
}

/// Every read of a table needs its index, and the memory cache counted an
/// index as it counts a block. Reads of other tables then put it out, and
/// the next read of its table had the end of the table from the store
/// again. An index that a read asked for within the last minute stays.
#[tokio::test]
async fn an_index_that_reads_ask_for_is_not_put_out_for_blocks() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    // Two tables of some 2 MiB, 400 blocks each, and a cache of 1 MiB.
    let first = stored_table(&store, 2000, 1000, 4096).await;
    let other = stored_table(&store, 1999, 1000, 4096).await;
    // How often the end of the first table, where its index is, was asked
    // of the store.
    let ends = |from: usize| {
        let ranges = gate.ranges();
        let end = |(_, end): &&(u64, u64)| *end == first.meta.size;
        ranges[from..].iter().filter(end).count()
    };
    for (idle, again) in [(Duration::from_secs(60), 0), (Duration::ZERO, 1)] {
        let io = Io::new_idle(gate.clone(), 1 << 20, None, idle);
        let asked = gate.ranges().len();
        assert!(first.get(&io, b"k000000").await.unwrap().is_some());
        assert_eq!(ends(asked), 1);
        // Twice the cache in blocks of the other table, each asked for
        // twice: a block that is asked for again is kept before what has
        // not been.
        for i in (0..1995).step_by(5).flat_map(|i| [i, i + 1]) {
            let key = format!("k{i:06}");
            assert!(other.get(&io, key.as_bytes()).await.unwrap().is_some());
        }
        assert!(io.cache_bytes() <= 1 << 20);
        // Without the minute, the index went as a block that was asked
        // for once.
        assert!(first.get(&io, b"k001000").await.unwrap().is_some());
        assert_eq!(ends(asked), 1 + again, "{idle:?}");
    }
}

/// Two readers may have the bytes of a damaged copy at once. The one that
/// meets the damage first discards the copy. The other meets it in what it
/// read before that and finds no copy to discard: it reads the store's all
/// the same, where it was answered that the table is damaged.
#[tokio::test]
async fn a_damaged_copy_that_another_reader_discarded_is_read_from_the_store() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let table = stored_table(&store, 2000, 1000, 4096).await;
    // What a merge asks for as it reads the table through: the eighth of
    // its reads after the first block is of 64 blocks.
    let plain = Io::new(gate.clone(), 1 << 20, None);
    assert_eq!(
        scanned(&table, &plain, ALL, false, false).await,
        keys(0..2000)
    );
    let (start, end) = gate.ranges()[8];
    assert!(end - start > 200_000, "{start} {end}");

    // A server that wrote the table has a copy of it, of which the second
    // half of that read is then damaged.
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
    let io = Io::new(gate.clone(), 1 << 20, Some(disk));
    let disk = io.disk.clone().unwrap();
    let body = store.get(&table.meta.key).await.unwrap().unwrap();
    io.remember(&table.meta.key, body, &[], &[]).await;
    let copy = std::fs::read_dir(dir.path()).unwrap().flatten().next();
    let copy = copy.unwrap().path();
    let mut bytes = std::fs::read(&copy).unwrap();
    let (middle, end) = ((start + end) as usize / 2, end as usize);
    for byte in bytes[middle..end].iter_mut().step_by(97) {
        *byte ^= 0x5a;
    }
    std::fs::write(&copy, bytes).unwrap();

    // A merge reads the copy as far as that read, whose first block is
    // whole.
    let table = Table::new(table.meta.clone());
    let hits = || disk.hits.load(Ordering::Relaxed);
    let mut scan = table.scan(io.clone(), b(""), b(""), false, false);
    let mut found = vec![scan.next().await.unwrap().unwrap()];
    let first = hits();
    while hits() < first + 7 {
        found.push(scan.next().await.unwrap().unwrap());
    }
    // Another reader met the damage and discarded the copy.
    assert!(io.forget(&table.meta.key).await);
    assert_eq!(disk.used(), 0);
    // The merge comes to the damage in what it had read, and reads the
    // store's from there.
    let asked = gate.ranges().len();
    while let Some(entry) = scan.next().await.unwrap() {
        found.push(entry);
    }
    let found: Vec<String> = found
        .iter()
        .map(|entry| String::from_utf8(entry.key.to_vec()).unwrap())
        .collect();
    assert_eq!(found, keys(0..2000));
    assert!(gate.ranges().len() > asked);
    assert_eq!(hits(), first + 7);
}

/// What a scan read ahead is in memory until the scan has come to it, and
/// a server may be asked for a thousand scans at once. So sixteen scans of
/// requests read ahead at a time, and the others read a block at a time,
/// as every scan did before. Merges have sixteen turns of their own, for
/// the runs of tables they read.
#[tokio::test]
async fn no_more_than_sixteen_scans_read_ahead_at_a_time() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let io = Io::new(gate.clone(), 1 << 20, None);
    // Tables of some 300 KiB for the scans that hold a turn, and of 2 MiB
    // for those whose reads are looked at.
    let mut small = Vec::new();
    for n in 0..32 {
        small.push(stored_table(&store, 300 + n, 1000, 4096).await);
    }
    let mut large = Vec::new();
    for n in 0..5 {
        large.push(stored_table(&store, 2000 + n, 1000, 4096).await);
    }
    // What a scan asked the store for, after the end of its table where
    // the index is: how many reads, and the longest.
    let read = |from: usize| -> (usize, u64) {
        let ranges = gate.ranges();
        let lengths = ranges[from + 1..].iter().map(|(start, end)| end - start);
        (lengths.clone().count(), lengths.max().unwrap_or(0))
    };
    // A scan as far as its second read, which is of two blocks and has
    // it hold a turn.
    let begun = |table: &Arc<Table>, keep: bool| {
        let mut scan = table.scan(io.clone(), b(""), b(""), false, keep);
        async move {
            for _ in 0..6 {
                scan.next().await.unwrap().unwrap();
            }
            scan
        }
    };
    let half = ("", "k001000");

    // Sixteen scans of requests hold the sixteen turns.
    let mut scans = Vec::new();
    for table in &small[..16] {
        scans.push(begun(table, true).await);
    }
    assert_eq!(io.ahead.available_permits(), 0);
    // The seventeenth reads a block at a time.
    let asked = gate.ranges().len();
    assert_eq!(
        scanned(&large[0], &io, half, false, true).await,
        keys(0..1000)
    );
    let (reads, longest) = read(asked);
    assert!(reads >= 200 && longest < 6000, "{reads} {longest}");
    // A merge has a turn of its own.
    let asked = gate.ranges().len();
    assert_eq!(scanned(&large[1], &io, ALL, false, false).await.len(), 2001);
    let (reads, longest) = read(asked);
    assert!(reads < 20 && longest > 100_000, "{reads} {longest}");
    // One of the sixteen goes on to its end. It gives its turn back
    // before it asks for one, so it reads ahead as before.
    let asked = gate.ranges().len();
    let mut first = scans.remove(0);
    let mut found = 6;
    while first.next().await.unwrap().is_some() {
        found += 1;
    }
    assert_eq!(found, 300);
    let ranges = gate.ranges()[asked..].to_vec();
    assert!(ranges.iter().any(|(start, end)| end - start > 6000));
    // At its end nothing is ahead of it, and it holds no turn though it
    // is still there: a merge keeps the scans of the runs it has read.
    assert_eq!(io.ahead.available_permits(), 1);
    drop(first);
    assert_eq!(io.ahead.available_permits(), 1);
    // With a turn free, the next scan reads ahead.
    let asked = gate.ranges().len();
    assert_eq!(
        scanned(&large[2], &io, half, false, true).await,
        keys(0..1000)
    );
    let (reads, longest) = read(asked);
    assert!(reads < 20 && longest > 100_000, "{reads} {longest}");
    assert_eq!(io.ahead.available_permits(), 1);
    drop(scans);
    assert_eq!(io.ahead.available_permits(), AHEAD_SCANS);

    // Sixteen runs of tables that merges read hold the merges' turns,
    // and the seventeenth is read a block at a time.
    let mut merges = Vec::new();
    for table in &small[16..] {
        merges.push(begun(table, false).await);
    }
    assert_eq!(io.merge_ahead.available_permits(), 0);
    let asked = gate.ranges().len();
    assert_eq!(scanned(&large[3], &io, ALL, false, false).await.len(), 2003);
    let (reads, longest) = read(asked);
    assert!(reads >= 300 && longest < 6000, "{reads} {longest}");
    // The requests' turns are not theirs to take.
    assert_eq!(io.ahead.available_permits(), AHEAD_SCANS);
    drop(merges);
    assert_eq!(io.merge_ahead.available_permits(), AHEAD_MERGES);

    // A block of more than 1 MiB is read by itself. Nothing is ahead of
    // the scan then, and it holds no turn.
    let huge = stored_table(&store, 4, 1_500_000, 4096).await;
    let mut scan = huge.scan(io.clone(), b(""), b(""), false, false);
    for _ in 0..3 {
        scan.next().await.unwrap().unwrap();
        assert_eq!(io.merge_ahead.available_permits(), AHEAD_MERGES);
    }
    let asked = gate.ranges().len();
    assert_eq!(scanned(&large[4], &io, ALL, false, false).await.len(), 2004);
    let (reads, longest) = read(asked);
    assert!(reads < 20 && longest > 100_000, "{reads} {longest}");
}

/// A merge reads each of its tables once. A table it reads from the store
/// is not fetched for the disk cache, where what the merge replaces has no
/// place; what a request scans is. And a copy in the disk cache that turns
/// out to be damaged in the middle of a scan is discarded, and the store's
/// read.
#[tokio::test]
async fn what_a_merge_reads_is_not_fetched_for_the_disk_cache() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new(&store);
    let dir = tempfile::tempdir().unwrap();
    let process = || {
        let disk = DiskCache::open(dir.path(), 64 << 20).unwrap();
        Io::new(gate.clone(), 1 << 20, Some(disk))
    };
    let io = process();
    let disk = io.disk.clone().unwrap();
    let table = stored_table(&store, 2000, 1000, 4096).await;
    assert_eq!(scanned(&table, &io, ALL, false, false).await, keys(0..2000));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(gate.fetched().is_empty());
    assert_eq!(disk.used(), 0);

    // A request's scan of forty entries, a fiftieth of the table, does
    // not have it fetched. One of a quarter of it does, once.
    assert_eq!(
        scanned(&table, &io, ("k000100", "k000140"), false, true).await,
        keys(100..140)
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(gate.fetched().is_empty());
    assert_eq!(
        scanned(&table, &io, ("k000500", "k001000"), false, true).await,
        keys(500..1000)
    );
    let there = async {
        while disk.used() < table.meta.size {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), there)
        .await
        .expect("the table reaches the disk cache");
    assert_eq!(gate.fetched().len(), 1);
    // From then on the store is not asked, by a merge either.
    let asked = gate.ranges().len();
    assert_eq!(scanned(&table, &io, ALL, false, false).await, keys(0..2000));
    assert_eq!(scanned(&table, &io, ALL, false, true).await, keys(0..2000));
    assert_eq!(gate.ranges().len(), asked);
    assert_eq!(gate.fetched().len(), 1);
    // And nothing is read ahead in a table that is on the disk, where a
    // read costs no request: a scan of it takes none of the turns to.
    let mut scan = table.scan(io.clone(), b(""), b(""), false, true);
    for _ in 0..1500 {
        scan.next().await.unwrap().unwrap();
    }
    assert_eq!(io.ahead.available_permits(), AHEAD_SCANS);
    assert_eq!(gate.ranges().len(), asked);
    drop(scan);

    // Damage the blocks of the second quarter of the copy. Its index is
    // whole, and so are the blocks a scan reads first: it comes to the
    // damage in the middle of what it has read ahead.
    let copy = std::fs::read_dir(dir.path()).unwrap().flatten().next();
    let copy = copy.unwrap().path();
    let mut bytes = std::fs::read(&copy).unwrap();
    let quarter = bytes.len() / 4;
    for byte in bytes[quarter..2 * quarter].iter_mut().step_by(97) {
        *byte ^= 0x5a;
    }
    std::fs::write(&copy, bytes).unwrap();
    // A new process: empty memory, the same cache directory.
    let io = process();
    let table = Table::new(table.meta.clone());
    assert_eq!(scanned(&table, &io, ALL, false, false).await, keys(0..2000));
    assert_eq!(io.disk.as_ref().unwrap().used(), 0);
    assert_eq!(gate.fetched().len(), 1);
}

/// A store may decline a spare upload. That stores nothing: the commit waits
/// for an upload that was sent. It uses none of the attempts either, so the
/// spares are sent once the store has room, and an upload that failed is
/// sent again whatever the store spares.
#[tokio::test]
async fn a_spare_that_is_not_sent_is_not_an_upload() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let server = ctx_with(
        gate.clone(),
        Options {
            upload_attempts: 4,
            upload_eager: 2,
            hedge_after: Duration::from_millis(5),
            ..Options::default()
        },
    );
    let (db, _) = Db::open(server, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    let frame = |key: &str, _| key.contains("/l/");
    let frames_held = || gate.held().iter().filter(|(_, key)| frame(key, 0)).count();
    let refused = || gate.spares_refused.load(Ordering::SeqCst);

    // The first upload is slow and the store spares nothing.
    gate.no_spares.store(true, Ordering::SeqCst);
    gate.rule(Op::Put, Action::Hold, usize::MAX, frame);
    let slow = {
        let db = db.clone();
        tokio::spawn(async move { commit(&db, put("k", "v")).await })
    };
    // A spare is asked for again and again, and none of that is an upload.
    let patience = Duration::from_secs(60);
    tokio::time::timeout(patience, async {
        while refused() < 5 && !slow.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a spare is asked for again");
    assert!(!slow.is_finished(), "nothing was stored");
    assert_eq!(frames_held(), 1);
    // The store has room again: all three spares are sent, and no fifth upload.
    gate.no_spares.store(false, Ordering::SeqCst);
    tokio::time::timeout(patience, async {
        while frames_held() < 4 && !slow.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the spares are sent");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!slow.is_finished());
    assert_eq!(frames_held(), 4);
    gate.clear();
    gate.release(Op::Put);
    assert_eq!(slow.await.unwrap().unwrap(), 1);

    // The first upload fails, twice, and the store spares nothing.
    gate.no_spares.store(true, Ordering::SeqCst);
    gate.rule(Op::Put, Action::Fail, 2, frame);
    let retried = tokio::time::timeout(Duration::from_secs(5), commit(&db, put("k", "w")));
    assert_eq!(retried.await.expect("it is sent again").unwrap(), 2);
    assert_eq!(get(&db, b"k").await.unwrap(), Some((2, "w".into())));
    db.close().await;
}

/// Options under which two flushes of `round` make a merge of several tables.
fn small_tables() -> Options {
    Options {
        flush_bytes: 16 << 10,
        idle_flush: Duration::from_secs(3600),
        block_bytes: 512,
        table_bytes: 4 << 10,
        ..Options::default()
    }
}

/// `small_tables`, of a database that may have three runs: with a fourth,
/// a merge gives way to merges of what was flushed behind it.
fn few_runs() -> Options {
    Options {
        max_runs: 3,
        ..small_tables()
    }
}

/// Text that does not compress.
fn noise(mut seed: u64, len: usize) -> String {
    let mut out = String::with_capacity(len + 16);
    while out.len() < len {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.push_str(&format!("{seed:016x}"));
    }
    out.truncate(len);
    out
}

fn value(round: u64, i: usize) -> String {
    format!("{round}-{}", noise(round * 1000 + i as u64 + 1, 120))
}

/// 200 keys of 26 KiB together: more than a flush waits for.
fn round(round: u64) -> Txn {
    let writes = (0..200).map(|i| (b(&format!("k{i:04}")), Some(b(&value(round, i)))));
    Txn {
        writes: writes.collect(),
        ..Txn::default()
    }
}

/// 2400 keys before every key of a round, 300 KiB together: a table of
/// more than is read of one at once.
fn wide(round: u64) -> Txn {
    let writes = (0..2400).map(|i| (b(&format!("a{i:04}")), Some(b(&value(round, i)))));
    Txn {
        writes: writes.collect(),
        ..Txn::default()
    }
}

async fn flushed(db: &Arc<Db>) {
    let unflushed = async {
        while db.mem_bytes() > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), unflushed)
        .await
        .expect("the writes are flushed");
}

async fn every_key_is_of(db: &Arc<Db>, round: u64) {
    let response = db
        .read(ReadReq {
            scans: vec![Scan {
                lo: Bytes::new(),
                hi: Bytes::new(),
                reverse: false,
                limit: 1000,
            }],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    let items = &response.scans[0].items;
    assert_eq!(items.len(), 200);
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item.value, Some(b(&value(round, i))), "key {i}");
    }
}

/// Holds the `nth` table that `name` uploads from now on.
fn hold_table(gate: &Gate, name: &'static str, nth: usize) {
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
        key.contains(&format!("/db/{name}/t/"))
            && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == nth
    });
}

fn tables_stored(store: &MemoryStore, name: &str) -> usize {
    let prefix = format!("/db/{name}/t/");
    store.keys().iter().filter(|k| k.contains(&prefix)).count()
}

/// A merge of a large database takes as long as writing it again. Writes
/// that fall due for a flush meanwhile are flushed between two of the
/// tables the merge writes, and the table they make stays above its result.
#[tokio::test]
async fn a_merge_gives_way_to_a_flush() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush is followed by a merge of both tables into seven or
    // so. Its third is held.
    hold_table(&gate, "app", 4);
    commit(&db, round(2)).await.unwrap();
    gate.wait_held(Op::Put).await;
    commit(&db, round(3)).await.unwrap();
    assert!(db.mem_bytes() > 0);
    // Let it go, and hold the second table after it. That is the merge's
    // fourth only if a flush came first.
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    flushed(&db).await;
    every_key_is_of(&db, 3).await;
    // The merge ends, and what it publishes keeps the newer table on top.
    gate.clear();
    gate.release(Op::Put);
    for _ in 0..60 {
        every_key_is_of(&db, 3).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    db.close().await;
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    every_key_is_of(&db, 3).await;
    // Every table in the bucket is one the manifest lists.
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// A server that is killed in a merge leaves the runs it was merging, and
/// every one is read until they are merged. The server that opens the
/// database begins the merge then: it waited for a write to, or for half a
/// minute.
#[tokio::test]
async fn a_merge_that_is_due_when_a_database_is_opened_begins_then() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush is followed by a merge of both tables, whose first
    // table never arrives.
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    gate.wait_held(Op::Put).await;
    assert_eq!((db.mem_bytes(), db.info().runs), (0, 2));
    // Another server takes the database, and nothing is written to it.
    gate.clear();
    let other = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(other, "app", None).await.unwrap();
    let merged = async {
        while db.info().runs != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the merge begins when the database is opened");
    every_key_is_of(&db, 2).await;
}

/// The key of the `nth` upload that was held since the test began.
async fn held_upload(gate: &Gate, nth: usize) -> String {
    let held = async {
        while gate.held().len() < nth {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        gate.held()[nth - 1].1.clone()
    };
    tokio::time::timeout(Duration::from_secs(5), held)
        .await
        .expect("an upload is held")
}

/// The first ten keys of a round, 1.4 KiB, and a flush of them asked for:
/// a table a third the size of those a merge makes of `small_tables`, as a
/// flush is a quarter of a table. Without the tenth, that key is deleted.
async fn few(db: &Arc<Db>, round: u64, tenth: bool) {
    let write = |i: usize| {
        let value = (tenth || i != 9).then(|| b(&value(round, i)));
        (b(&format!("k{i:04}")), value)
    };
    let txn = Txn {
        writes: (0..10).map(write).collect(),
        ..Txn::default()
    };
    commit(db, txn).await.unwrap();
    db.request_flush();
}

/// Every key is there with what the second round wrote, but the first ten
/// with what `round` did, and without the tenth where that was deleted.
async fn the_first_ten_are_of(db: &Arc<Db>, round: u64, tenth: bool) {
    let all = Scan {
        lo: Bytes::new(),
        hi: Bytes::new(),
        reverse: false,
        limit: 1000,
    };
    let request = ReadReq {
        scans: vec![all],
        strong: true,
        ..ReadReq::default()
    };
    let items = db.read(request).await.unwrap().scans.remove(0).items;
    let keys = (0..200).filter(|i| tenth || *i != 9);
    let expected: Vec<_> = keys
        .map(|i| Some(b(&value(if i < 10 { round } else { 2 }, i))))
        .collect();
    let found: Vec<_> = items.into_iter().map(|item| item.value).collect();
    assert!(found == expected, "{} keys", found.len());
}

/// Every table in the bucket is one the manifest lists, once what was
/// being merged has been.
async fn only_the_tables_listed_are_stored(bucket: &MemoryStore, db: &Arc<Db>) {
    let settled = async {
        while tables_stored(bucket, "app") != db.info().tables {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), settled)
        .await
        .expect("every table in the bucket is one the manifest lists");
}

/// Every flush is a run, and a read looks in every run. A merge gave way
/// to flushes and to nothing else, so behind a long one the runs went on
/// growing in number: a server killed while it merged 16 GiB left 27. A
/// merge that has given way to a flush merges what has been flushed since
/// it began, when that has grown to runs the rule would merge and is no
/// more than the merge has itself stored, and then goes on. Its own
/// tables, stored and not yet published, are still there after the
/// manifest of the other.
#[tokio::test]
async fn a_merge_gives_way_to_a_merge_of_what_was_flushed_behind_it() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), few_runs());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    let manifest = |nth: usize| {
        let seen = std::sync::atomic::AtomicUsize::new(0);
        gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
            key.contains("/db/app/m/")
                && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == nth
        });
    };
    let held = |nth: usize| held_upload(&gate, nth);
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush is followed by a merge of both tables into a dozen
    // or so. Its first is held, and ten keys are written.
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    assert!(held(1).await.contains("/db/app/t/"));
    few(&db, 3, true).await;
    // The merge gives way to a flush of that: one table behind it, and
    // nothing to merge it with. The merge's second table is held, and the
    // ten keys are written again, but that the last is deleted: the merge
    // that gave way has that key in both its tables.
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    assert!(held(2).await.contains("/db/app/t/"));
    assert_eq!((db.mem_bytes(), db.info().tables), (0, 3));
    few(&db, 4, false).await;
    // It gives way to a flush again, and now to a merge of the two tables
    // flushed behind it, whose manifest is the second from here.
    gate.clear();
    manifest(2);
    gate.release(Op::Put);
    assert!(held(3).await.contains("/db/app/m/"));
    let before = db.info();
    assert_eq!((db.mem_bytes(), before.tables, before.runs), (0, 4, 4));
    // That manifest is let through, and the next table held: the third of
    // the merge that gave way.
    gate.clear();
    hold_table(&gate, "app", 1);
    gate.release(Op::Put);
    assert!(held(4).await.contains("/db/app/t/"));
    // Two tables of the same keys became one: a table's worth is gone.
    let after = db.info();
    assert!(after.bytes + 1_000 < before.bytes, "{before:?} {after:?}");
    assert_eq!((after.tables, after.runs), (3, 3));
    the_first_ten_are_of(&db, 4, false).await;
    // The two tables that were merged have been deleted, and the two that
    // the first merge had stored are still there beside those listed.
    assert_eq!(tables_stored(&bucket, "app"), after.tables + 2);
    // The first merge ends, and what it publishes keeps the newer tables
    // on top: the key that was deleted stays deleted, which it would not
    // had the merge of the newer tables left out what says so.
    gate.clear();
    gate.release(Op::Put);
    for _ in 0..60 {
        the_first_ten_are_of(&db, 4, false).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    db.close().await;
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    the_first_ten_are_of(&db, 4, false).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// A merge reads ahead in its tables, and holds one of a server's sixteen
/// turns to for each run it reads. While it waits for a merge it gave way
/// to, it gives them up with what it had read ahead: that merge, and those
/// of other databases, would read a block at a time without.
#[tokio::test]
async fn a_merge_that_waits_gives_up_its_turns_to_read_ahead() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), few_runs());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    let manifest = |nth: usize| {
        let seen = std::sync::atomic::AtomicUsize::new(0);
        gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
            key.contains("/db/app/m/")
                && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == nth
        });
    };
    // Two tables of 300 KiB, more than is read of one at once, are merged.
    commit(&db, wide(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 2);
    commit(&db, wide(2)).await.unwrap();
    held_upload(&gate, 1).await;
    few(&db, 3, true).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    // The merge has read ahead in both, and holds two turns.
    assert_eq!(ctx.io.merge_ahead.available_permits(), AHEAD_MERGES - 2);
    few(&db, 4, true).await;
    // It gives way to a merge of the two tables flushed behind it, which
    // has read them and is about to be published.
    gate.clear();
    manifest(2);
    gate.release(Op::Put);
    assert!(held_upload(&gate, 3).await.contains("/db/app/m/"));
    assert_eq!(db.info().runs, 4);
    assert_eq!(ctx.io.merge_ahead.available_permits(), AHEAD_MERGES);
    // It reads on from where it was.
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while db.info().runs != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(20), merged)
        .await
        .expect("both merges end");
    only_the_tables_listed_are_stored(&bucket, &db).await;
    let table = scanned_wide(&db).await;
    assert_eq!(table.len(), 2400);
    for (i, found) in table.iter().enumerate() {
        assert_eq!(found, &Some(b(&value(2, i))), "key {i}");
    }
}

/// The keys that `wide` wrote, in order, with what a scan finds of each.
async fn scanned_wide(db: &Arc<Db>) -> Vec<Option<Bytes>> {
    let mut found = Vec::new();
    let mut lo = b("a");
    loop {
        let scan = Scan {
            lo: lo.clone(),
            hi: b("b"),
            reverse: false,
            limit: 1000,
        };
        let request = ReadReq {
            scans: vec![scan],
            strong: true,
            ..ReadReq::default()
        };
        let items = db.read(request).await.unwrap().scans.remove(0).items;
        let Some(last) = items.last() else {
            return found;
        };
        lo = Bytes::from([last.key.as_ref(), b"\0"].concat());
        found.extend(items.into_iter().map(|item| item.value));
    }
}

/// A merge that another gave way to gives way to flushes and to no merge:
/// what is flushed behind it waits until it has ended. Were it to give way
/// as the first does, a database written fast enough would have merges
/// waiting on merges without end, the first of them with one of a server's
/// two turns to merge.
#[tokio::test]
async fn a_merge_that_another_gave_way_to_gives_way_to_no_merge() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), few_runs());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    // Thirty-five keys from `from`, 4.7 KiB, and a flush of them asked for.
    let some = |round: u64, from: usize| {
        let write = |i: usize| (b(&format!("k{i:04}")), Some(b(&value(round, i))));
        let txn = Txn {
            writes: (from..from + 35).map(write).collect(),
            ..Txn::default()
        };
        let db = db.clone();
        async move {
            commit(&db, txn).await.unwrap();
            db.request_flush();
        }
    };
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // A merge of two rounds is held at its first table, and at its fourth
    // with one table flushed behind it.
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    some(3, 0).await;
    gate.clear();
    hold_table(&gate, "app", 4);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    assert_eq!((db.mem_bytes(), db.info().tables), (0, 3));
    // A second is flushed behind it, of other keys: the merge of the two
    // makes three tables, and is held at its first.
    some(4, 100).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 3).await;
    assert_eq!((db.mem_bytes(), db.info().tables), (0, 4));
    // Ten keys are flushed behind that merge, which is held at its second
    // table, and ten more.
    few(&db, 5, true).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 4).await;
    assert_eq!((db.mem_bytes(), db.info().tables), (0, 5));
    few(&db, 6, true).await;
    // The next manifest after that flush's is of the merge of the two
    // tables of thirty-five keys: the two of ten have not been merged
    // before it, and are listed with its three tables.
    gate.clear();
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
        key.contains("/db/app/m/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
    });
    gate.release(Op::Put);
    assert!(held_upload(&gate, 5).await.contains("/db/app/m/"));
    assert_eq!((db.mem_bytes(), db.info().runs), (0, 6));
    gate.clear();
    hold_table(&gate, "app", 1);
    gate.release(Op::Put);
    held_upload(&gate, 6).await;
    let info = db.info();
    assert_eq!((info.runs, info.tables), (5, 7), "{info:?}");
    gate.clear();
    gate.release(Op::Put);
    // The two of ten are merged then, and the first merge ends: three
    // runs, each more than twice the one before it.
    let merged = async {
        while db.info().runs != 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the merges end");
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// The merges a merge gives way to are together no larger than what it has
/// itself stored: it keeps half the merging and ends, however fast the
/// database is written. Here two rounds are flushed behind a merge that
/// has stored two tables, a sixth of them, and they wait.
#[tokio::test]
async fn a_merge_gives_way_to_no_merge_larger_than_what_it_has_stored() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), few_runs());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    commit(&db, round(3)).await.unwrap();
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    commit(&db, round(4)).await.unwrap();
    // The fourth round is flushed, and the next manifest after that
    // flush's is of the merge that gave way: it has stored all its tables
    // meanwhile, and the two rounds are listed as they were flushed.
    gate.clear();
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
        key.contains("/db/app/m/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
    });
    gate.release(Op::Put);
    assert!(held_upload(&gate, 3).await.contains("/db/app/m/"));
    let info = db.info();
    assert_eq!((db.mem_bytes(), info.tables, info.runs), (0, 4, 4));
    // It is published, and the two tables it merged are gone from the
    // bucket: nothing is stored there but what is listed, when the first
    // table of the next merge is held. Had the rounds been merged behind
    // it, this would be its own third table, with two stored before it.
    gate.clear();
    hold_table(&gate, "app", 1);
    gate.release(Op::Put);
    held_upload(&gate, 4).await;
    let info = db.info();
    assert_eq!(info.runs, 3, "{info:?}");
    assert_eq!(tables_stored(&bucket, "app"), info.tables);
    // The rounds are merged with it then.
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while db.info().runs != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the rounds flushed behind the merge are merged after it");
    every_key_is_of(&db, 4).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// Every flush is a run and a read looks in each, so writes wait while a
/// database has more than twice the runs it may: with one at most, three.
/// Here a merge is held while a flush is made behind it, and a write is told
/// to wait until the merge is published.
#[tokio::test]
async fn writes_wait_while_a_database_has_far_more_runs_than_it_may() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let opts = Options {
        max_runs: 1,
        ..small_tables()
    };
    let ctx = ctx_with(gate.clone(), opts);
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // A merge of the two rounds is held at its first table.
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    // It stores that table, gives way to the flush of ten keys, and is held
    // at its second.
    few(&db, 3, true).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    assert_eq!((db.mem_bytes(), db.info().runs), (0, 3));
    assert!(matches!(commit(&db, round(4)).await, Err(Error::Busy(_))));
    // Once the merge is published the write is taken.
    gate.clear();
    gate.release(Op::Put);
    let taken = async {
        loop {
            match commit(&db, round(4)).await {
                Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(1)).await,
                other => return other,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), taken)
        .await
        .expect("the write is taken when the runs are fewer")
        .unwrap();
    every_key_is_of(&db, 4).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// A merge that failed is begun again when the database is next written to,
/// and a write that is told to wait for runs is that. Here the merge of a
/// database with too many runs fails twice, and the writes that wait for it
/// have it begun a third time: no write would be taken that began it.
#[tokio::test]
async fn a_write_that_waits_for_runs_begins_again_a_merge_that_failed() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let opts = Options {
        max_runs: 1,
        ..small_tables()
    };
    let ctx = ctx_with(gate.clone(), opts);
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    few(&db, 3, true).await;
    // After the flush of the ten keys, the next two tables fail: the
    // merge's second, and the first of the merge begun again.
    gate.clear();
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Break, 2, move |key, _| {
        key.contains("/db/app/t/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 1
    });
    gate.release(Op::Put);
    let failed = async {
        while gate
            .rules
            .lock()
            .unwrap()
            .iter()
            .any(|rule| rule.remaining > 0)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), failed)
        .await
        .expect("both merges fail");
    assert_eq!(db.info().runs, 3);
    let taken = async {
        loop {
            match commit(&db, round(4)).await {
                Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(1)).await,
                other => return other,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), taken)
        .await
        .expect("a write that waits has the merge begun again")
        .unwrap();
    every_key_is_of(&db, 4).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// A merge that reads a table from a store that fails for a while waits for
/// it, as an upload does, where a request gives up after a second or so: a
/// merge that failed was begun again from its start, at the next write, and
/// one of a large database over a link that resets now and then would not
/// end. Here seven reads of tables fail, one more than a request's read is
/// tried again for.
#[tokio::test]
async fn a_merge_waits_for_a_store_that_fails_for_a_while() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    gate.rule(Op::GetRange, Action::Fail, 7, |key, _| {
        key.contains("/db/app/t/")
    });
    commit(&db, round(2)).await.unwrap();
    flushed(&db).await;
    let merged = async {
        while db.info().runs != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(20), merged)
        .await
        .expect("the merge ends without another write");
    // The reads failed: the merge read its tables from the store.
    assert!(
        gate.rules
            .lock()
            .unwrap()
            .iter()
            .all(|rule| rule.remaining == 0)
    );
    every_key_is_of(&db, 2).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// A merge gives way to merges only when the database has more runs than
/// it may: what is merged behind a merge is merged again with what that
/// merge makes, and is sent to the store once more for it. Here a database
/// that may have twelve runs has four, and the two tables flushed behind
/// its merge wait for it, though it has stored more than both.
#[tokio::test]
async fn a_merge_gives_way_to_no_merge_while_the_runs_are_few() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    few(&db, 3, true).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    few(&db, 4, true).await;
    // The next manifest after that flush's is the merge's own.
    gate.clear();
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
        key.contains("/db/app/m/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
    });
    gate.release(Op::Put);
    assert!(held_upload(&gate, 3).await.contains("/db/app/m/"));
    let info = db.info();
    assert_eq!((db.mem_bytes(), info.tables, info.runs), (0, 4, 4));
    // It is published, and the two tables are merged then: when the
    // first table of that merge is held, nothing is stored but what is
    // listed. Had they been merged behind it, this would be its own third
    // table, with two stored before it.
    gate.clear();
    hold_table(&gate, "app", 1);
    gate.release(Op::Put);
    held_upload(&gate, 4).await;
    let info = db.info();
    assert_eq!(info.runs, 3, "{info:?}");
    assert_eq!(tables_stored(&bucket, "app"), info.tables);
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while db.info().runs != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the tables flushed behind the merge are merged after it");
    the_first_ten_are_of(&db, 4, true).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// What falls due while a merge that another gave way to runs is flushed
/// when that merge is published, before the one that waited stores its
/// next table: it would otherwise wait for a table of each.
#[tokio::test]
async fn what_falls_due_during_a_merge_another_gave_way_to_is_flushed_after_it() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), few_runs());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 2);
    commit(&db, round(2)).await.unwrap();
    held_upload(&gate, 1).await;
    few(&db, 3, true).await;
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 2).await;
    few(&db, 4, true).await;
    // The merge of the two tables flushed behind the first is about to be
    // published when ten keys more are written.
    gate.clear();
    let seen = std::sync::atomic::AtomicUsize::new(0);
    gate.rule(Op::Put, Action::Hold, 1, move |key, _| {
        key.contains("/db/app/m/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
    });
    gate.release(Op::Put);
    assert!(held_upload(&gate, 3).await.contains("/db/app/m/"));
    few(&db, 5, true).await;
    assert!(db.mem_bytes() > 0);
    // The second table stored from here is held. The first was the flush
    // of those keys, and not a table of the merge that waited.
    gate.clear();
    hold_table(&gate, "app", 2);
    gate.release(Op::Put);
    held_upload(&gate, 4).await;
    let info = db.info();
    assert_eq!((db.mem_bytes(), info.tables, info.runs), (0, 4, 4));
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while db.info().runs != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the merges end");
    the_first_ten_are_of(&db, 5, true).await;
    only_the_tables_listed_are_stored(&bucket, &db).await;
}

/// Two merges run at once on a server. A database that has to wait for its
/// turn goes on flushing, and merges when the turn comes without having to
/// be written to again.
#[tokio::test]
async fn a_database_waiting_to_merge_goes_on_flushing() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let open = |name: &'static str| {
        let ctx = ctx.clone();
        async move {
            Db::open(ctx, name, Some(DbConfig::default()))
                .await
                .unwrap()
                .0
        }
    };
    // Two databases are held in the middle of a merge.
    let mut merging = Vec::new();
    for name in ["a", "b"] {
        let db = open(name).await;
        commit(&db, round(1)).await.unwrap();
        flushed(&db).await;
        hold_table(&gate, name, 2);
        commit(&db, round(2)).await.unwrap();
        merging.push(db);
    }
    while gate.held().len() < 2 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // A third has two tables to merge and no turn. Its next writes are
    // flushed all the same.
    let db = open("c").await;
    for i in 1..=3 {
        commit(&db, round(i)).await.unwrap();
        flushed(&db).await;
        assert_eq!(db.info().tables, i as usize);
    }
    every_key_is_of(&db, 3).await;
    // When the others are done, it merges.
    gate.clear();
    gate.release(Op::Put);
    let merged = async {
        while tables_stored(&bucket, "c") <= 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the waiting database gets its turn");
    every_key_is_of(&db, 3).await;
    for db in merging {
        every_key_is_of(&db, 2).await;
    }
}

/// A release does not wait for a merge to finish: the merge stops after
/// the table it is writing, and what it wrote is removed.
#[tokio::test]
async fn a_release_does_not_wait_for_a_merge() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    hold_table(&gate, "app", 3);
    commit(&db, round(2)).await.unwrap();
    gate.wait_held(Op::Put).await;
    let released = tokio::spawn({
        let db = db.clone();
        async move { db.close().await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!released.is_finished());
    // The table being written is finished. No other is begun.
    gate.clear();
    gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
        key.contains("/t/")
    });
    gate.release(Op::Put);
    tokio::time::timeout(Duration::from_secs(5), released)
        .await
        .expect("the release does not wait for the rest of the merge")
        .unwrap();
    assert_eq!(gate.held().len(), 1);
    assert_eq!(tables_stored(&bucket, "app"), 2);
    let (db, _) = Db::open(ctx, "app", None).await.unwrap();
    every_key_is_of(&db, 2).await;
}

/// Nor for a merge that gave way to another. The one that waited stops as
/// soon as the other has ended: left to find out after its next table, a
/// release waited for one table more. And a merge that gave way to a
/// flush begins no merge once a release has begun.
#[tokio::test]
async fn a_release_does_not_wait_for_a_merge_that_gave_way_to_another() {
    // The release begins while the second upload after the merge gave way
    // is held, which is the one table of the merge it gave way to, and
    // that merge is published; and while the first is, which is the flush
    // before it, and then there is no such merge.
    for (nth, left) in [(2, 3), (1, 4)] {
        let bucket = Arc::new(MemoryStore::new());
        let gate = Gate::new(&bucket);
        let ctx = ctx_with(gate.clone(), few_runs());
        let (db, _) = Db::open(ctx.clone(), "app", Some(DbConfig::default()))
            .await
            .unwrap();
        commit(&db, round(1)).await.unwrap();
        flushed(&db).await;
        // A merge is held at its first table and then at its second, with
        // ten keys flushed behind it each time, as in the test of that.
        hold_table(&gate, "app", 2);
        commit(&db, round(2)).await.unwrap();
        held_upload(&gate, 1).await;
        few(&db, 3, true).await;
        gate.clear();
        hold_table(&gate, "app", 2);
        gate.release(Op::Put);
        held_upload(&gate, 2).await;
        few(&db, 4, true).await;
        gate.clear();
        hold_table(&gate, "app", nth);
        gate.release(Op::Put);
        held_upload(&gate, 3).await;
        assert_eq!(db.info().tables, 2 + nth);
        let released = tokio::spawn({
            let db = db.clone();
            async move { db.close().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!released.is_finished());
        // The table being written is finished. No merge begins another.
        gate.clear();
        gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
            key.contains("/t/")
        });
        gate.release(Op::Put);
        tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .expect("the release waits for neither merge")
            .unwrap();
        assert_eq!(gate.held().len(), 3, "{nth}");
        // What the merge that waited had stored is removed.
        assert_eq!(tables_stored(&bucket, "app"), left, "{nth}");
        let (db, _) = Db::open(ctx, "app", None).await.unwrap();
        the_first_ten_are_of(&db, 4, true).await;
    }
}

/// A database with one write flushed, and the purge of the frames that
/// flush covered held back, as by a store that is slow to list them.
async fn collecting(gate: &Arc<Gate>) -> Arc<Db> {
    let (db, _) = Db::open(ctx(gate.clone()), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, put("key", "1")).await.unwrap();
    gate.rule(Op::Purge, Action::Hold, 1, |prefix, _| {
        prefix.contains("/db/app/l/")
    });
    db.request_flush();
    gate.wait_held(Op::Purge).await;
    db
}

/// A MinIO took longer than twenty seconds over each listing, and a
/// deletion waited seven minutes for a collection that waited for them.
#[tokio::test]
async fn a_deletion_does_not_wait_for_a_collection() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let db = collecting(&gate).await;
    tokio::time::timeout(Duration::from_secs(5), db.destroy())
        .await
        .expect("the deletion does not wait for the collection")
        .unwrap();
    assert_eq!(gate.held().len(), 1);
    assert_eq!(bucket.keys(), [TOMBSTONE]);
}

#[tokio::test]
async fn a_release_does_not_wait_for_a_collection() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let db = collecting(&gate).await;
    tokio::time::timeout(Duration::from_secs(5), db.close())
        .await
        .expect("the release does not wait for the collection");
    assert_eq!(gate.held().len(), 1);
    // It collected what it left all the same: a sealed manifest and a
    // table, and no claim for the next owner to revoke.
    let left = |part: &str| bucket.keys().iter().filter(|k| k.contains(part)).count();
    assert_eq!(
        (left("/c/"), left("/l/"), left("/m/"), left("/t/")),
        (0, 0, 1, 1),
        "{:?}",
        bucket.keys()
    );
    let (db, _) = Db::open(ctx(gate.clone()), "app", None).await.unwrap();
    let found = get(&db, b"key").await.unwrap();
    assert_eq!(found.map(|(_, value)| value).as_deref(), Some("1"));
}

/// A store that does not answer one purge of a collection is not asked for
/// the rest of them. What is left is collected after the next flush, or by
/// the next owner.
#[tokio::test]
async fn a_collection_ends_at_a_purge_the_store_does_not_answer() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let (db, _) = Db::open(ctx(gate.clone()), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    let count = |part: &str| bucket.keys().iter().filter(|k| k.contains(part)).count();
    gate.rule(Op::Purge, Action::Fail, usize::MAX, |prefix, _| {
        prefix.contains("/db/app/l/")
    });
    for value in ["1", "2"] {
        commit(&db, put("key", value)).await.unwrap();
        db.request_flush();
        flushed(&db).await;
    }
    // A release waits for the collection of the last flush, and makes one
    // of its own. None of them purged the manifests before its last: the
    // first of all is still there.
    db.close().await;
    assert!(count("/l/") >= 2, "{:?}", bucket.keys());
    assert!(count("/m/") >= 4, "{:?}", bucket.keys());
    let first = |key: &String| key.contains("/m/") && key.ends_with("/00000000");
    assert!(bucket.keys().iter().any(first), "{:?}", bucket.keys());
    gate.clear();
    let (db, _) = Db::open(ctx(gate.clone()), "app", None).await.unwrap();
    let swept = async {
        while count("/l/") > 0 || count("/m/") > 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), swept)
        .await
        .expect("the next owner removes what was left");
    let found = get(&db, b"key").await.unwrap();
    assert_eq!(found.map(|(_, value)| value).as_deref(), Some("2"));

    // A table that a merge replaced, and whose purge was not answered, is
    // purged after a later flush.
    gate.rule(Op::Purge, Action::Fail, usize::MAX, |prefix, _| {
        prefix.contains("/db/app/t/")
    });
    for value in ["3", "4", "5", "6"] {
        commit(&db, put("key", value)).await.unwrap();
        db.request_flush();
        flushed(&db).await;
    }
    let replaced = async {
        while count("/t/") <= db.info().tables {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), replaced)
        .await
        .expect("a merge replaces tables");
    gate.clear();
    let collected = async {
        while count("/t/") > db.info().tables {
            commit(&db, put("key", "7")).await.unwrap();
            db.request_flush();
            flushed(&db).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), collected)
        .await
        .expect("what was not purged is purged later");
}

/// A round of a server's housekeeping releases what has been idle. It
/// does not wait for the release, which waits for the store.
#[tokio::test]
async fn housekeeping_does_not_wait_for_a_release() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let limits = Limits {
        idle_close: Duration::ZERO,
        ..Limits::default()
    };
    let node = Node::new(ctx(gate.clone()), limits, Arc::new(Metrics::default()));
    node.create("app", DbConfig::default()).await.unwrap();
    // The manifest that a release seals the database with is slow.
    gate.rule(Op::Put, Action::Hold, 1, |key, _| key.contains("/m/"));
    tokio::time::timeout(Duration::from_secs(5), node.tend())
        .await
        .expect("the round does not wait for the release");
    gate.wait_held(Op::Put).await;
    assert_eq!((node.open_count(), node.releasing()), (0, 1));
    // The next round does not release it a second time.
    node.tend().await;
    assert_eq!(node.releasing(), 1);
    // A server that stops does wait for it.
    let stopped = tokio::spawn({
        let node = node.clone();
        async move { node.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!stopped.is_finished());
    gate.release(Op::Put);
    tokio::time::timeout(Duration::from_secs(5), stopped)
        .await
        .expect("the release ends when the store answers")
        .unwrap();
    assert_eq!(node.releasing(), 0);
    assert!(claims(&bucket).is_empty());
}

/// A merge that fails is tried again when the database is next written
/// to. It is not repeated at once, which would go on for as long as
/// storage is failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_that_fails_waits_for_the_next_write() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let (db, _) = Db::open(ctx, "app", Some(DbConfig::default()))
        .await
        .unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush gets its table through. Every table after it, which
    // are the merge's, is refused.
    let tables = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = tables.clone();
    gate.rule(Op::Put, Action::Break, usize::MAX, move |key, _| {
        key.contains("/t/") && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 1
    });
    commit(&db, round(2)).await.unwrap();
    flushed(&db).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let attempts = tables.load(std::sync::atomic::Ordering::SeqCst) - 1;
    assert!((1..=2).contains(&attempts), "{attempts} attempts");
    assert_eq!(db.info().tables, 2);
    // Storage recovers, and the next write brings the merge with it.
    gate.clear();
    commit(&db, round(3)).await.unwrap();
    let merged = async {
        while db.info().tables < 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), merged)
        .await
        .expect("the merge is made after the next write");
    every_key_is_of(&db, 3).await;
}

/// A write that the quota has no room for asks for a merge of everything
/// and is put off. Those that ask while it is made are served by it: it is
/// not made again for them, though a flush in the middle of it has left a
/// table above its result.
#[tokio::test]
async fn a_merge_for_the_quota_is_made_once_for_all_who_waited() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let ctx = ctx_with(gate.clone(), small_tables());
    let config = DbConfig {
        max_bytes: 44 << 10,
        ..DbConfig::default()
    };
    let (db, _) = Db::open(ctx, "app", Some(config)).await.unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // A quarter of the keys again: a second table, too small to be merged
    // with the first for its size, and 7 KiB that can no longer be read.
    let writes = (0..50).map(|i| (b(&format!("k{i:04}")), Some(b(&value(2, i)))));
    let again = Txn {
        writes: writes.collect(),
        ..Txn::default()
    };
    commit(&db, again).await.unwrap();
    db.request_flush();
    flushed(&db).await;
    assert_eq!(db.info().tables, 2);
    // Every table uploaded from here on is noted, and the second is held:
    // it is the merge's second.
    let uploads = Arc::new(Mutex::new(Vec::new()));
    let seen = uploads.clone();
    gate.rule(Op::Put, Action::Hold, usize::MAX, move |key, _| {
        if key.contains("/db/app/t/") {
            seen.lock().unwrap().push(key.to_owned());
        }
        false
    });
    hold_table(&gate, "app", 2);
    let more = put("more", &noise(7, 12_000));
    assert!(matches!(
        commit(&db, more.clone()).await,
        Err(Error::Busy(_))
    ));
    gate.wait_held(Op::Put).await;
    // A small write is taken meanwhile, and the large one asks again.
    commit(&db, put("small", "1")).await.unwrap();
    assert!(matches!(
        commit(&db, more.clone()).await,
        Err(Error::Busy(_))
    ));
    gate.release(Op::Put);
    let taken = async {
        while matches!(commit(&db, more.clone()).await, Err(Error::Busy(_))) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), taken)
        .await
        .expect("the write is taken once the merge has made room");
    // Time for a second merge, were one to be made.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The merge's first two tables are in the bucket still: nothing has
    // been merged with them since.
    let (uploads, stored) = (uploads.lock().unwrap().clone(), bucket.keys());
    assert!(uploads.len() > 3, "{uploads:?}");
    for table in &uploads[..2] {
        assert!(stored.contains(table), "{table} was merged again");
    }
    only_the_tables_listed_are_stored(&bucket, &db).await;
    assert_eq!(get(&db, b"small").await.unwrap().unwrap().1, "1");
    assert_eq!(get(&db, b"k0007").await.unwrap().unwrap().1, value(2, 7));
    assert_eq!(get(&db, b"k0107").await.unwrap().unwrap().1, value(1, 107));
}

/// A merge of everything that made no room is not made again for four
/// times as long as it took: a database is rewritten for its quota a fifth
/// of the time at most, however large it is and however slow its store.
#[tokio::test]
async fn a_merge_for_the_quota_that_made_no_room_is_waited_after() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let opts = Options {
        // No least wait: only the four times.
        quota_rest: Duration::ZERO,
        ..small_tables()
    };
    let ctx = ctx_with(gate.clone(), opts);
    let config = DbConfig {
        max_bytes: 44 << 10,
        ..DbConfig::default()
    };
    let (db, _) = Db::open(ctx, "app", Some(config)).await.unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // One table and a write that is in none: everything is merged for a
    // write with no room. The first table uploaded is the small write's,
    // the second is the merge's first, and it takes half a second.
    commit(&db, put("small/1", "1")).await.unwrap();
    hold_table(&gate, "app", 2);
    let more = put("more", &noise(7, 20_000));
    assert!(matches!(
        commit(&db, more.clone()).await,
        Err(Error::Busy(_))
    ));
    gate.wait_held(Op::Put).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    gate.release(Op::Put);
    let refused = async {
        loop {
            match commit(&db, more.clone()).await {
                Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(1)).await,
                definite => return definite,
            }
        }
    };
    let definite = tokio::time::timeout(Duration::from_secs(5), refused).await;
    assert!(matches!(definite, Ok(Err(Error::Quota))), "{definite:?}");
    // Two seconds, then. After more than one of them a write with no room
    // is still refused, though there is something to merge again.
    commit(&db, put("small/2", "2")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert!(matches!(commit(&db, more).await, Err(Error::Quota)));
}

/// A server releases a database that nobody has used for a while. One that
/// is merging is in use: a merge that takes longer than a database may be
/// idle for would otherwise be let go every time, and begun again from its
/// start by the next owner.
#[tokio::test]
async fn a_database_is_not_released_as_idle_while_it_merges() {
    let bucket = Arc::new(MemoryStore::new());
    let gate = Gate::new(&bucket);
    let limits = Limits {
        idle_close: Duration::from_millis(500),
        ..Limits::default()
    };
    let ctx = ctx_with(gate.clone(), small_tables());
    let node = Node::new(ctx, limits, Arc::new(Metrics::default()));
    node.create("app", DbConfig::default()).await.unwrap();
    let db = node.db("app").await.unwrap();
    commit(&db, round(1)).await.unwrap();
    flushed(&db).await;
    // The second flush takes most of the time a database may be idle, so
    // that the merge which follows it begins when the database has nearly
    // been idle for that long. The merge is of seven tables or so. Every
    // one of them is held for 120 ms, which is longer together than a
    // database may be idle, and the server's housekeeping runs each time.
    gate.rule(Op::Put, Action::Hold, usize::MAX, |key, _| {
        key.contains("/db/app/t/")
    });
    commit(&db, round(2)).await.unwrap();
    gate.wait_held(Op::Put).await;
    tokio::time::sleep(Duration::from_millis(450)).await;
    gate.release(Op::Put);
    let mut merged = 1;
    loop {
        let next = async {
            while gate.held().len() == merged {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        if tokio::time::timeout(Duration::from_millis(300), next)
            .await
            .is_err()
        {
            break;
        }
        merged += 1;
        tokio::time::sleep(Duration::from_millis(120)).await;
        node.tend().await;
        assert_eq!(node.open_count(), 1, "released at table {merged}");
        gate.release(Op::Put);
    }
    assert!(merged > 5, "{merged} tables");
    every_key_is_of(&db, 2).await;
    // And when the merge has ended and nobody has used it since, it goes.
    tokio::time::sleep(Duration::from_millis(600)).await;
    node.tend().await;
    assert_eq!(node.open_count(), 0);
    node.shutdown().await;
}
