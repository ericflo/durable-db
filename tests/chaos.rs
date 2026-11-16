//! Randomized failure injection.
//!
//! Several nodes share one store that delays requests, drops them, loses
//! acknowledgements, and applies some writes long after reporting them as
//! failed. Nodes crash without releasing their claims, and zombie owners
//! keep running until they notice. Throughout, and at the end:
//!
//! * every acknowledged write is present, at the revision it was acknowledged;
//! * a counter incremented by compare-and-set equals the acknowledged count,
//!   so nothing was applied twice and nothing forked;
//! * a linearizable read never returns a revision older than an
//!   acknowledgement that preceded it;
//! * once a deletion is acknowledged, or any client is told the database is
//!   gone, nothing is acknowledged again, and only the tombstone remains.
use bytes::Bytes;
use durable_db::{
    Error,
    db::{Check, Ctx, Db, Options, ReadReq, Scan, Txn},
    io::Io,
    layout::{DbConfig, Layout},
    store::{
        Listed, ObjectStore, StoreError, StoreResult,
        memory::{Faults, MemoryStore},
    },
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

/// A write that was given up on arrives within this long.
const LATENESS: Duration = Duration::from_millis(1500);

/// One node's connection to the shared store. When `writes_left` runs out
/// the node has stopped as far as the bucket can tell: nothing more of its
/// arrives, at whatever point of whatever it was doing.
struct Link {
    store: Arc<MemoryStore>,
    writes_left: AtomicI64,
}

impl Link {
    fn write(&self) -> StoreResult<()> {
        match self.writes_left.fetch_sub(1, Ordering::SeqCst) {
            left if left > 0 => Ok(()),
            _ => Err(StoreError::Fatal("this node has stopped".into())),
        }
    }
    fn read(&self) -> StoreResult<()> {
        match self.writes_left.load(Ordering::SeqCst) {
            left if left > 0 => Ok(()),
            _ => Err(StoreError::Fatal("this node has stopped".into())),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for Link {
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.write()?;
        self.store.put(key, body).await
    }
    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        self.read()?;
        self.store.get(key).await
    }
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
        self.read()?;
        self.store.get_range(key, start, end).await
    }
    async fn exists(&self, key: &str) -> StoreResult<bool> {
        self.read()?;
        self.store.exists(key).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>> {
        self.read()?;
        self.store.list(prefix, after, limit).await
    }
    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        self.read()?;
        self.store.list_dirs(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> StoreResult<()> {
        self.write()?;
        self.store.delete(key).await
    }
    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        self.write()?;
        self.store.purge(prefix, end, doomed).await
    }
}

struct Node {
    link: Arc<Link>,
    ctx: Arc<Ctx>,
    name: String,
    db: Mutex<Option<Arc<Db>>>,
}

impl Node {
    async fn db(&self) -> Result<Arc<Db>, Error> {
        let current = self.db.lock().unwrap().clone();
        if let Some(db) = current.filter(|db| !db.is_closed()) {
            return Ok(db);
        }
        let (db, _) = Db::open(self.ctx.clone(), &self.name, Some(DbConfig::default())).await?;
        *self.db.lock().unwrap() = Some(db.clone());
        Ok(db)
    }
}

struct World {
    /// Percentage of requests sent to the node that served the previous one.
    /// Low values make ownership change hands on nearly every request.
    sticky: usize,
    last: AtomicU64,
    /// A node that receives no requests for now, or none.
    cut_off: AtomicU64,
    nodes: Vec<Node>,
    /// A node that serves no client: it only looks, when the test says so.
    observer: Node,
    rng: Mutex<StdRng>,
    /// Acknowledged appends: key -> (revision, value).
    appended: Mutex<BTreeMap<String, (u64, String)>>,
    increments: AtomicU64,
    newest_ack: AtomicU64,
    stop: AtomicBool,
}

impl World {
    /// Three nodes serving database `name` from one store.
    fn new(
        name: &str,
        seed: u64,
        sticky: usize,
        store: &Arc<MemoryStore>,
        opts: &Options,
    ) -> Arc<Self> {
        let node = |_| {
            let link = Arc::new(Link {
                store: store.clone(),
                writes_left: AtomicI64::new(i64::MAX),
            });
            Node {
                ctx: context(link.clone(), opts),
                link,
                name: name.to_owned(),
                db: Mutex::new(None),
            }
        };
        Arc::new(World {
            sticky,
            last: AtomicU64::new(0),
            cut_off: AtomicU64::new(u64::MAX),
            nodes: (0..3).map(node).collect(),
            observer: node(3),
            rng: Mutex::new(StdRng::seed_from_u64(seed ^ 0x9e37_79b9)),
            appended: Mutex::default(),
            increments: AtomicU64::new(0),
            newest_ack: AtomicU64::new(0),
            stop: AtomicBool::new(false),
        })
    }

    fn pick(&self, n: usize) -> usize {
        self.rng.lock().unwrap().random_range(0..n)
    }

    fn node(&self) -> &Node {
        let cut_off = self.cut_off.load(Ordering::SeqCst);
        if self.pick(100) >= self.sticky || self.last.load(Ordering::SeqCst) == cut_off {
            let mut next = self.pick(self.nodes.len()) as u64;
            if next == cut_off {
                next = (next + 1) % self.nodes.len() as u64;
            }
            self.last.store(next, Ordering::SeqCst);
        }
        &self.nodes[self.last.load(Ordering::SeqCst) as usize]
    }

    fn acknowledged(&self, revision: u64) {
        self.newest_ack.fetch_max(revision, Ordering::SeqCst);
    }

    /// Run one transaction to a definite answer, retrying the identical
    /// request on any node while its outcome is unknown.
    async fn decide(&self, txn: Txn) -> Result<u64, Error> {
        loop {
            let node = self.node();
            let outcome = match node.db().await {
                Ok(db) => db
                    .commit(vec![txn.clone()])
                    .await
                    .map(|mut results| results.remove(0)),
                Err(error) => Err(error),
            };
            match outcome {
                Ok(Ok(committed)) => {
                    self.acknowledged(committed.revision);
                    return Ok(committed.revision);
                }
                // Refused before it was given a revision: while the
                // database flushes or merges earlier writes. Asked again.
                Ok(Err(Error::Busy(_))) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => assert!(error.retryable(), "{error:?}"),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn read(&self, request: ReadReq) -> durable_db::db::ReadResp {
        loop {
            let node = self.node();
            let floor = self.newest_ack.load(Ordering::SeqCst);
            if let Ok(db) = node.db().await
                && let Ok(response) = db.read(request.clone()).await
            {
                assert!(
                    !request.strong || response.revision >= floor,
                    "a linearizable read returned revision {} after revision {floor} was acknowledged",
                    response.revision
                );
                return response;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn appender(self: Arc<Self>, worker: usize, count: usize) {
        for i in 0..count {
            let key = format!("append/{worker}/{i:05}");
            let value = format!("{worker}-{i}-{}", "v".repeat(self.pick(200)));
            let txn = Txn {
                writes: vec![(Bytes::from(key.clone()), Some(Bytes::from(value.clone())))],
                idempotency_key: Some(format!("append-{worker}-{i}")),
                ..Txn::default()
            };
            let revision = self
                .decide(txn)
                .await
                .expect("an unconditional write succeeds");
            self.appended.lock().unwrap().insert(key, (revision, value));
        }
    }

    async fn incrementer(self: Arc<Self>, worker: usize, count: usize) {
        let key = Bytes::from_static(b"counter");
        let mut attempt = 0;
        let mut done = 0;
        while done < count {
            attempt += 1;
            let seen = self
                .read(ReadReq {
                    keys: vec![key.clone()],
                    strong: true,
                    ..ReadReq::default()
                })
                .await;
            let (version, value) = match &seen.values[0] {
                Some((version, value)) => (
                    *version,
                    std::str::from_utf8(value).unwrap().parse::<u64>().unwrap(),
                ),
                None => (0, 0),
            };
            let txn = Txn {
                checks: vec![Check::Key {
                    key: key.clone(),
                    version,
                }],
                writes: vec![(key.clone(), Some(Bytes::from((value + 1).to_string())))],
                idempotency_key: Some(format!("increment-{worker}-{attempt}")),
                ..Txn::default()
            };
            match self.decide(txn).await {
                Ok(_) => {
                    done += 1;
                    self.increments.fetch_add(1, Ordering::SeqCst);
                }
                Err(Error::Conflict(_)) => {}
                Err(error) => panic!("{error:?}"),
            }
        }
    }

    /// Write until told the database is gone. `gone` is set as soon as any
    /// client learns that; an acknowledgement of a request that began after
    /// that would mean the deletion was not final.
    async fn until_gone(self: Arc<Self>, worker: usize, gone: Arc<AtomicBool>) {
        for i in 0.. {
            let txn = Txn {
                writes: vec![(
                    Bytes::from(format!("late/{worker}/{i:05}")),
                    Some(Bytes::from_static(b"v")),
                )],
                ..Txn::default()
            };
            let already = gone.load(Ordering::SeqCst);
            let outcome = match self.node().db().await {
                Ok(db) => db.commit(vec![txn]).await.map(|mut r| r.remove(0)),
                Err(error) => Err(error),
            };
            match outcome {
                Ok(Ok(committed)) => assert!(
                    !already,
                    "revision {} was acknowledged after the database was reported gone",
                    committed.revision
                ),
                Err(Error::Gone) | Ok(Err(Error::Gone)) => {
                    gone.store(true, Ordering::SeqCst);
                    return;
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Delete the database, repeating the request until some node answers
    /// that it is done. This is the adversary's turn. The node that has
    /// been serving is cut off for a while, so that another takes the
    /// database over, and then receives the first request: it deletes
    /// without knowing it was replaced. A deleting node also often stops
    /// after a few of its writes or a short time, wherever that leaves the
    /// bucket. Later requests go to any node, or to one that was not
    /// involved at all.
    async fn delete(&self) {
        let cut_off = self.last.load(Ordering::SeqCst) as usize;
        self.cut_off.store(cut_off as u64, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100 + self.pick(400) as u64)).await;
        self.cut_off.store(u64::MAX, Ordering::SeqCst);
        let mut first = Some(&self.nodes[cut_off]);
        loop {
            let node = match first.take() {
                Some(node) => node,
                None if self.pick(2) == 0 => &self.observer,
                None => self.node(),
            };
            match node.db().await {
                Ok(db) => {
                    let writes = self.pick(5) as i64;
                    if writes < 4 {
                        node.link.writes_left.store(writes, Ordering::SeqCst);
                    }
                    let patience = Duration::from_millis(20 + self.pick(400) as u64);
                    let outcome = tokio::time::timeout(patience, db.destroy()).await;
                    if let Ok(Ok(())) = outcome {
                        return;
                    }
                    // The node restarts with nothing.
                    node.db.lock().unwrap().take();
                    node.link.writes_left.store(i64::MAX, Ordering::SeqCst);
                }
                Err(Error::Gone) => return,
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(1 + self.pick(60) as u64)).await;
        }
    }

    /// Crash nodes (forget the handle; the zombie runs on) or release them.
    async fn saboteur(self: Arc<Self>) {
        while !self.stop.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(40 + self.pick(200) as u64)).await;
            let node = &self.nodes[self.pick(self.nodes.len())];
            let db = node.db.lock().unwrap().take();
            if let Some(db) = db
                && self.pick(3) == 0
            {
                db.close().await;
            }
        }
    }
}

fn context(store: Arc<dyn ObjectStore>, opts: &Options) -> Arc<Ctx> {
    Arc::new(Ctx {
        io: Io::new(store, 8 << 20, None),
        opts: opts.clone(),
        root: String::new(),
    })
}

async fn run(seed: u64) {
    let faults = Faults {
        drop_request: 0.04,
        drop_response: 0.04,
        late: 0.03,
        max_lateness: LATENESS,
        max_delay: Duration::from_millis(15),
    };
    let store = Arc::new(MemoryStore::with_faults(seed, faults.clone()));
    let opts = Options {
        flush_bytes: 8 << 10,
        flush_frames: 6,
        idle_flush: Duration::from_millis(30),
        block_bytes: 1 << 10,
        // Small enough that merges write their result in several tables.
        table_bytes: 4 << 10,
        store_deadline: Duration::from_secs(5),
        ..Options::default()
    };
    let ctx = || context(store.clone(), &opts);
    let sticky = [0, 80, 97][(seed % 3) as usize];
    let world = World::new("app", seed, sticky, &store, &opts);
    let saboteur = tokio::spawn(world.clone().saboteur());
    let mut workers = Vec::new();
    for worker in 0..3 {
        workers.push(tokio::spawn(world.clone().appender(worker, 40)));
        workers.push(tokio::spawn(world.clone().incrementer(worker, 15)));
    }
    for worker in workers {
        worker.await.unwrap();
    }
    world.stop.store(true, Ordering::SeqCst);
    saboteur.await.unwrap();

    // A fresh node with nothing but the bucket, once everything that was
    // on its way there has arrived.
    store.set_faults(Faults::default());
    tokio::time::sleep(LATENESS).await;
    let (db, _) = Db::open(ctx(), "app", None).await.unwrap();
    let everything = db
        .read(ReadReq {
            keys: vec![Bytes::from_static(b"counter")],
            scans: vec![Scan {
                lo: Bytes::from_static(b"append/"),
                hi: Bytes::from_static(b"append0"),
                reverse: false,
                limit: 10_000,
            }],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    let counter: u64 = std::str::from_utf8(&everything.values[0].as_ref().unwrap().1)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        counter,
        world.increments.load(Ordering::SeqCst),
        "seed {seed}: counter"
    );
    let appended = world.appended.lock().unwrap().clone();
    let found: BTreeMap<String, (u64, String)> = everything.scans[0]
        .items
        .iter()
        .map(|e| {
            let text = |bytes: &Bytes| String::from_utf8(bytes.to_vec()).unwrap();
            (text(&e.key), (e.version, text(e.value.as_ref().unwrap())))
        })
        .collect();
    let differences: Vec<String> = appended
        .iter()
        .filter(|(key, expected)| found.get(*key) != Some(expected))
        .map(|(key, (revision, _))| {
            format!(
                "{key}: acknowledged at {revision}, found {:?}",
                found.get(key).map(|f| f.0)
            )
        })
        .chain(
            found
                .keys()
                .filter(|key| !appended.contains_key(*key))
                .map(|key| format!("{key}: never acknowledged")),
        )
        .collect();
    assert!(differences.is_empty(), "seed {seed}: {differences:#?}");
    assert!(everything.revision >= world.newest_ack.load(Ordering::SeqCst));

    // Released cleanly, the bucket holds one manifest, its tables, and no log.
    db.close().await;
    let keys = store.keys();
    let stray: Vec<_> = keys
        .iter()
        .filter(|key| key.contains("/l/") || key.contains("/c/"))
        .collect();
    assert!(stray.is_empty(), "seed {seed}: left behind {stray:#?}");

    // Deletion, under the same faults and with writers on every node:
    // first of the database above, then of several small ones, because
    // the dangerous interleavings are narrow.
    for round in 0..6 {
        let (name, world) = match round {
            0 => ("app".to_owned(), world.clone()),
            _ => {
                let name = format!("gone{round}");
                let seed = seed.wrapping_mul(31) + round;
                let sticky = [97, 0, 97, 80, 97][round as usize - 1];
                (name.clone(), World::new(&name, seed, sticky, &store, &opts))
            }
        };
        store.set_faults(faults.clone());
        world.stop.store(false, Ordering::SeqCst);
        let gone = Arc::new(AtomicBool::new(false));
        let saboteur = tokio::spawn(world.clone().saboteur());
        let writers: Vec<_> = (0..3)
            .map(|worker| tokio::spawn(world.clone().until_gone(worker, gone.clone())))
            .collect();
        tokio::time::sleep(Duration::from_millis(50 + world.pick(300) as u64)).await;
        world.delete().await;
        gone.store(true, Ordering::SeqCst);
        for writer in writers {
            writer.await.unwrap();
        }
        world.stop.store(true, Ordering::SeqCst);
        saboteur.await.unwrap();

        store.set_faults(Faults::default());
        tokio::time::sleep(LATENESS).await;
        assert!(
            matches!(
                Db::open(ctx(), &name, Some(DbConfig::default())).await,
                Err(Error::Gone)
            ),
            "seed {seed}: deleted database {name} came back"
        );
        // Repeating the deletion removes whatever stragglers uploaded since.
        let layout = Layout::new("", &name);
        Db::erase(&ctx(), &layout).await.unwrap();
        let left: Vec<_> = store
            .keys()
            .into_iter()
            .filter(|key| key.starts_with(layout.everything()))
            .collect();
        assert_eq!(left, [layout.tombstone()], "seed {seed}");
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn acknowledged_writes_survive_arbitrary_faults() {
    let seeds: u64 = std::env::var("CHAOS_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let first: u64 = std::env::var("CHAOS_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // The clock of these schedules is not the wall clock, so one that never
    // ends would not time out by itself. It is stopped from outside, and
    // named.
    let current = Arc::new(std::sync::Mutex::new((first, std::time::Instant::now())));
    let watched = current.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let (seed, started) = *watched.lock().unwrap();
            if started.elapsed() > Duration::from_secs(300) {
                eprintln!("seed {seed} has not finished in five minutes");
                std::process::abort();
            }
        }
    });
    for seed in first..first + seeds {
        let started = std::time::Instant::now();
        *current.lock().unwrap() = (seed, started);
        run(seed).await;
        eprintln!("seed {seed} held in {:?}", started.elapsed());
    }
}
