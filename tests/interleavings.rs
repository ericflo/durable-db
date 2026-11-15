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
    io::{DiskCache, Io},
    layout::{DbConfig, Layout},
    metrics::Metrics,
    node::{Limits, Node},
    store::{Listed, ObjectStore, StoreError, StoreResult, memory::MemoryStore},
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
        self.pass(Op::Get, key, 0).await?;
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
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
            assert_eq!(io.range(key, SIZE, 10, 20).await.unwrap().len(), 10);
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
    io.remember(&tables[0], body).await;
    assert_eq!(disk.used(), tables.len() as u64 * SIZE);
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
    assert_eq!(tables_stored(&bucket, "app"), db.info().tables);
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
