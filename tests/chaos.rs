//! Randomized failure injection.
//!
//! Several nodes share one store that delays requests, drops them, and loses
//! acknowledgements. Nodes crash without releasing their claims, and zombie
//! owners keep running until they notice. Throughout, and at the end:
//!
//! * every acknowledged write is present, at the revision it was acknowledged;
//! * a counter incremented by compare-and-set equals the acknowledged count,
//!   so nothing was applied twice and nothing forked;
//! * a linearizable read never returns a revision older than an
//!   acknowledgement that preceded it.
use bytes::Bytes;
use durable_db::{
    Error,
    db::{Check, Ctx, Db, Options, ReadReq, Scan, Txn},
    io::Io,
    layout::DbConfig,
    store::{
        ObjectStore,
        memory::{Faults, MemoryStore},
    },
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

struct Node {
    ctx: Arc<Ctx>,
    db: Mutex<Option<Arc<Db>>>,
}

impl Node {
    async fn db(&self) -> Result<Arc<Db>, Error> {
        let current = self.db.lock().unwrap().clone();
        if let Some(db) = current.filter(|db| !db.is_closed()) {
            return Ok(db);
        }
        let (db, _) = Db::open(self.ctx.clone(), "app", Some(DbConfig::default())).await?;
        *self.db.lock().unwrap() = Some(db.clone());
        Ok(db)
    }
}

struct World {
    /// Percentage of requests sent to the node that served the previous one.
    /// Low values make ownership change hands on nearly every request.
    sticky: usize,
    last: AtomicU64,
    nodes: Vec<Node>,
    rng: Mutex<StdRng>,
    /// Acknowledged appends: key -> (revision, value).
    appended: Mutex<BTreeMap<String, (u64, String)>>,
    increments: AtomicU64,
    newest_ack: AtomicU64,
    stop: AtomicBool,
}

impl World {
    fn pick(&self, n: usize) -> usize {
        self.rng.lock().unwrap().random_range(0..n)
    }

    fn node(&self) -> &Node {
        if self.pick(100) >= self.sticky {
            self.last
                .store(self.pick(self.nodes.len()) as u64, Ordering::SeqCst);
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

async fn run(seed: u64) {
    let faults = Faults {
        drop_request: 0.04,
        drop_response: 0.04,
        max_delay: Duration::from_millis(15),
    };
    let store = Arc::new(MemoryStore::with_faults(seed, faults));
    let opts = Options {
        flush_bytes: 8 << 10,
        flush_frames: 6,
        idle_flush: Duration::from_millis(30),
        block_bytes: 1 << 10,
        store_deadline: Duration::from_secs(5),
        ..Options::default()
    };
    let ctx = |store: Arc<MemoryStore>| {
        Arc::new(Ctx {
            io: Io::new(store as Arc<dyn ObjectStore>, 8 << 20, None),
            opts: opts.clone(),
            root: String::new(),
        })
    };
    let world = Arc::new(World {
        sticky: [0, 80, 97][(seed % 3) as usize],
        last: AtomicU64::new(0),
        nodes: (0..3)
            .map(|_| Node {
                ctx: ctx(store.clone()),
                db: Mutex::new(None),
            })
            .collect(),
        rng: Mutex::new(StdRng::seed_from_u64(seed ^ 0x9e37_79b9)),
        appended: Mutex::default(),
        increments: AtomicU64::new(0),
        newest_ack: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });
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

    // A fresh node with nothing but the bucket.
    store.set_faults(Faults::default());
    let (db, _) = Db::open(ctx(store.clone()), "app", None).await.unwrap();
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
    for seed in first..first + seeds {
        let started = std::time::Instant::now();
        run(seed).await;
        eprintln!("seed {seed} held in {:?}", started.elapsed());
    }
}
