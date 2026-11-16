//! The engine against in-memory and directory storage: the data model,
//! recovery, ownership transfer, checkpoints and collection.
use bytes::Bytes;
use durable_db::{
    Error,
    db::{Check, Ctx, Db, Options, ReadReq, Scan, Txn},
    io::{DiskCache, Io},
    layout::{ConfigChange, DbConfig},
    metrics::Metrics,
    node::{Limits, Node},
    store::{ObjectStore, fs::FsStore, memory::MemoryStore},
};
use std::{sync::Arc, time::Duration};

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

fn delete(key: &str) -> Txn {
    Txn {
        writes: vec![(b(key), None)],
        ..Txn::default()
    }
}

async fn create(ctx: &Arc<Ctx>, name: &str) -> Arc<Db> {
    Db::open(ctx.clone(), name, Some(DbConfig::default()))
        .await
        .unwrap()
        .0
}

async fn open(ctx: &Arc<Ctx>, name: &str) -> Arc<Db> {
    Db::open(ctx.clone(), name, None).await.unwrap().0
}

async fn commit(db: &Arc<Db>, txn: Txn) -> Result<u64, Error> {
    db.commit(vec![txn]).await?.remove(0).map(|c| c.revision)
}

async fn get(db: &Arc<Db>, key: &str) -> Option<(u64, String)> {
    let response = db
        .read(ReadReq {
            keys: vec![b(key)],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    response.values[0]
        .clone()
        .map(|(version, value)| (version, String::from_utf8(value.to_vec()).unwrap()))
}

async fn scan(
    db: &Arc<Db>,
    lo: &str,
    hi: &str,
    reverse: bool,
    limit: usize,
) -> (Vec<String>, bool) {
    let response = db
        .read(ReadReq {
            scans: vec![Scan {
                lo: b(lo),
                hi: b(hi),
                reverse,
                limit,
            }],
            strong: true,
            ..ReadReq::default()
        })
        .await
        .unwrap();
    let result = &response.scans[0];
    (
        result
            .items
            .iter()
            .map(|e| String::from_utf8(e.key.to_vec()).unwrap())
            .collect(),
        result.more,
    )
}

#[tokio::test]
async fn commits_reads_and_scans() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    assert!(matches!(
        Db::open(ctx.clone(), "app", None).await,
        Err(Error::NotFound)
    ));
    let db = create(&ctx, "app").await;
    assert_eq!(db.info().revision, 0);

    assert_eq!(commit(&db, put("b", "2")).await.unwrap(), 1);
    let many = Txn {
        writes: vec![
            (b("a"), Some(b("1"))),
            (b("c"), Some(b("3"))),
            (b("d"), Some(b("4"))),
        ],
        ..Txn::default()
    };
    assert_eq!(commit(&db, many).await.unwrap(), 2);
    assert_eq!(get(&db, "a").await, Some((2, "1".into())));
    assert_eq!(get(&db, "b").await, Some((1, "2".into())));
    assert_eq!(get(&db, "zz").await, None);

    let delete = Txn {
        writes: vec![(b("c"), None)],
        ..Txn::default()
    };
    assert_eq!(commit(&db, delete).await.unwrap(), 3);
    assert_eq!(get(&db, "c").await, None);

    assert_eq!(
        scan(&db, "", "", false, 10).await,
        (vec!["a".into(), "b".into(), "d".into()], false)
    );
    assert_eq!(
        scan(&db, "", "", true, 2).await,
        (vec!["d".into(), "b".into()], true)
    );
    assert_eq!(
        scan(&db, "b", "d", false, 10).await,
        (vec!["b".into()], false)
    );
    assert_eq!(scan(&db, "", "", false, 0).await, (vec![], true));
}

#[tokio::test]
async fn checks_make_transactions_conditional() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let db = create(&ctx, "app").await;
    commit(&db, put("k", "v1")).await.unwrap();

    // A key check names the version that was read; 0 means absent.
    let stale = Txn {
        checks: vec![Check::Key {
            key: b("k"),
            version: 0,
        }],
        ..put("k", "v2")
    };
    assert!(matches!(commit(&db, stale).await, Err(Error::Conflict(_))));
    let fresh = Txn {
        checks: vec![
            Check::Key {
                key: b("k"),
                version: 1,
            },
            Check::Key {
                key: b("absent"),
                version: 0,
            },
        ],
        ..put("k", "v2")
    };
    assert_eq!(commit(&db, fresh).await.unwrap(), 2);

    // A whole-database check.
    let behind = Txn {
        if_revision: Some(1),
        ..put("k", "v3")
    };
    assert!(matches!(commit(&db, behind).await, Err(Error::Conflict(_))));
    let current = Txn {
        if_revision: Some(2),
        ..put("k", "v3")
    };
    assert_eq!(commit(&db, current).await.unwrap(), 3);

    // A range check fails only when a key inside the range changed since.
    let range = |since| Check::Range {
        lo: b("m"),
        hi: b("n"),
        since,
    };
    commit(&db, put("m1", "x")).await.unwrap();
    let inside = Txn {
        checks: vec![range(3)],
        ..put("out", "y")
    };
    assert!(matches!(commit(&db, inside).await, Err(Error::Conflict(_))));
    let after = Txn {
        checks: vec![range(4)],
        ..put("out", "y")
    };
    assert_eq!(commit(&db, after).await.unwrap(), 5);
    assert_eq!(get(&db, "k").await, Some((3, "v3".into())));
}

#[tokio::test]
async fn a_request_stops_at_its_first_failed_transaction() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let db = create(&ctx, "app").await;
    let results = db
        .commit(vec![
            put("a", "1"),
            Txn {
                if_revision: Some(99),
                ..put("b", "2")
            },
            put("c", "3"),
        ])
        .await
        .unwrap();
    assert_eq!(results[0].as_ref().unwrap().revision, 1);
    assert!(results[1].is_err() && results[2].is_err());
    assert_eq!(scan(&db, "", "", false, 10).await.0, ["a"]);
}

#[tokio::test]
async fn an_identical_retry_returns_the_original_result() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let db = create(&ctx, "app").await;
    let once = |value: &str| Txn {
        idempotency_key: Some("request-1".into()),
        ..put("k", value)
    };
    assert_eq!(commit(&db, once("v")).await.unwrap(), 1);
    commit(&db, put("other", "x")).await.unwrap();
    assert_eq!(commit(&db, once("v")).await.unwrap(), 1);
    assert!(matches!(
        commit(&db, once("different")).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(db.info().revision, 2);

    // The receipt survives a restart and stays out of scans.
    db.close().await;
    let db = open(&ctx, "app").await;
    assert_eq!(commit(&db, once("v")).await.unwrap(), 1);
    assert_eq!(scan(&db, "", "", false, 10).await.0, ["k", "other"]);
}

#[tokio::test]
async fn acknowledged_writes_survive_a_crash_and_a_clean_restart() {
    let store = Arc::new(MemoryStore::new());
    let ctx = ctx(store.clone());
    let db = create(&ctx, "app").await;
    for i in 0..50 {
        commit(&db, put(&format!("k{i:03}"), &format!("v{i}")))
            .await
            .unwrap();
    }
    // A crash: the process disappears without releasing its claim.
    drop(db);
    let db = open(&ctx, "app").await;
    assert_eq!(db.info().revision, 50);
    assert_eq!(get(&db, "k049").await, Some((50, "v49".into())));
    commit(&db, put("after", "crash")).await.unwrap();

    db.close().await;
    let db = open(&ctx, "app").await;
    assert_eq!(db.info().revision, 51);
    assert_eq!(scan(&db, "", "", false, 100).await.0.len(), 51);
    assert_eq!(get(&db, "after").await, Some((51, "crash".into())));
}

#[tokio::test]
async fn a_takeover_revokes_the_previous_owner() {
    let store = Arc::new(MemoryStore::new());
    let (first, second) = (ctx(store.clone()), ctx(store.clone()));
    let old = create(&first, "app").await;
    commit(&old, put("k", "old")).await.unwrap();

    let new = open(&second, "app").await;
    assert_eq!(get(&new, "k").await, Some((1, "old".into())));
    assert_eq!(commit(&new, put("k", "new")).await.unwrap(), 2);

    // The old owner can neither acknowledge a write nor answer a strong read.
    assert!(matches!(
        commit(&old, put("k", "zombie")).await,
        Err(Error::Lost(_))
    ));
    let strong = ReadReq {
        keys: vec![b("k")],
        strong: true,
        ..ReadReq::default()
    };
    assert!(matches!(old.read(strong).await, Err(Error::Lost(_))));
    assert_eq!(get(&new, "k").await, Some((2, "new".into())));
}

#[tokio::test]
async fn a_stale_owner_serves_a_consistent_snapshot_until_asked_for_newer() {
    let store = Arc::new(MemoryStore::new());
    let (first, second) = (ctx(store.clone()), ctx(store.clone()));
    let old = create(&first, "app").await;
    commit(&old, put("k", "old")).await.unwrap();
    let new = open(&second, "app").await;
    commit(&new, put("k", "new")).await.unwrap();

    let session = |min_revision| ReadReq {
        keys: vec![b("k")],
        min_revision,
        ..ReadReq::default()
    };
    let stale = old.read(session(1)).await.unwrap();
    assert_eq!(
        (stale.revision, stale.values[0].clone().unwrap().1),
        (1, b("old"))
    );
    // A client that has seen revision 2 is never shown revision 1.
    assert!(matches!(old.read(session(2)).await, Err(Error::Lost(_))));
}

#[tokio::test]
async fn flushes_merges_and_collects() {
    let store = Arc::new(MemoryStore::new());
    let opts = Options {
        flush_bytes: 4 << 10,
        flush_frames: 8,
        idle_flush: Duration::from_millis(20),
        block_bytes: 512,
        ..Options::default()
    };
    let ctx = ctx_with(store.clone(), opts);
    let db = create(&ctx, "app").await;
    let value = "x".repeat(100);
    // A database this small refuses a write while it is four flushes
    // behind, as it should: such a write is sent again.
    for round in 0..6 {
        for i in 0..200 {
            committed(&db, put(&format!("k{i:04}"), &format!("{round}-{value}"))).await;
        }
    }
    for i in (0..200).step_by(2) {
        committed(
            &db,
            Txn {
                writes: vec![(b(&format!("k{i:04}")), None)],
                ..Txn::default()
            },
        )
        .await;
    }
    // The last writes are flushed after a pause, and nothing adds a table
    // after that. How many tables are then at rest depends on the sizes the
    // flushes had, and so on the machine's speed: runs are merged from the
    // newest while they are together at least half the next (`merge_plan`),
    // and as many as a database may have may remain.
    let most = Options::default().max_runs;
    let settled = async {
        while db.mem_bytes() > 0 || db.info().tables > most {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), settled)
        .await
        .expect("the writes are flushed and merged");
    let (keys, more) = scan(&db, "", "", false, 1000).await;
    assert_eq!((keys.len(), more), (100, false));
    assert_eq!(get(&db, "k0001").await.unwrap().1, format!("5-{value}"));
    assert_eq!(get(&db, "k0000").await, None);
    let (reversed, _) = scan(&db, "k0100", "k0110", true, 3).await;
    assert_eq!(reversed, ["k0109", "k0107", "k0105"]);
    let info = db.info();
    assert!(info.tables >= 1 && info.tables <= most, "{info:?}");

    // A sealed database is one claimless manifest plus the tables it names.
    db.close().await;
    let keys = store.keys();
    assert!(
        keys.iter()
            .all(|key| !key.contains("/c/") && !key.contains("/l/")),
        "{keys:#?}"
    );
    assert_eq!(
        keys.iter().filter(|key| key.contains("/m/")).count(),
        1,
        "{keys:#?}"
    );
    assert_eq!(
        keys.iter().filter(|key| key.contains("/t/")).count(),
        info_tables(&ctx).await
    );

    let db = open(&ctx, "app").await;
    assert_eq!(scan(&db, "", "", false, 1000).await.0.len(), 100);
    assert_eq!(db.info().revision, 1300);
}

async fn info_tables(ctx: &Arc<Ctx>) -> usize {
    let db = open(ctx, "app").await;
    let tables = db.info().tables;
    db.close().await;
    tables
}

#[tokio::test]
async fn the_quota_refuses_growth_but_not_deletes() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let config = DbConfig {
        max_bytes: 2_000,
        ..DbConfig::default()
    };
    let (db, created) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    assert!(created);
    commit(&db, put("a", &"x".repeat(1_000))).await.unwrap();
    assert!(matches!(
        commit(&db, put("b", &"x".repeat(1_500))).await,
        Err(Error::Quota)
    ));
    commit(
        &db,
        Txn {
            writes: vec![(b("a"), None)],
            ..Txn::default()
        },
    )
    .await
    .unwrap();
    // Raising the limit is an ordinary logged change.
    db.configure(ConfigChange {
        max_bytes: Some(1 << 20),
        ..ConfigChange::default()
    })
    .await
    .unwrap();
    commit(&db, put("b", &"x".repeat(1_500))).await.unwrap();
    db.close().await;
    assert_eq!(open(&ctx, "app").await.config().max_bytes, 1 << 20);
}

/// A write that the quota has no room for, asked again for as long as the
/// database says it is making room.
async fn commit_at_quota(db: &Arc<Db>, txn: Txn) -> Result<u64, Error> {
    let began = std::time::Instant::now();
    while began.elapsed() < Duration::from_secs(60) {
        match commit(db, txn.clone()).await {
            Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            definite => return definite,
        }
    }
    panic!("the database went on saying that it was making room");
}

async fn flushed(db: &Arc<Db>, tables: usize) {
    db.request_flush();
    while db.info().tables != tables {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `rest` is the least a database waits after a merge for its quota that
/// made little room.
fn at_quota(max_bytes: u64, rest: Duration) -> (Arc<Ctx>, DbConfig) {
    let opts = Options {
        // Nothing is flushed but when the test asks.
        idle_flush: Duration::from_secs(3600),
        quota_rest: rest,
        ..Options::default()
    };
    let config = DbConfig {
        max_bytes,
        ..DbConfig::default()
    };
    (ctx_with(Arc::new(MemoryStore::new()), opts), config)
}

/// A database that reached its quota stayed full when its keys were
/// deleted: what they held was counted until a merge of everything, and
/// nothing made one.
#[tokio::test]
async fn what_was_deleted_is_merged_away_before_the_quota_refuses() {
    let (ctx, config) = at_quota(256 << 10, Duration::from_secs(3600));
    let (db, _) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    for i in 0..20 {
        commit(&db, put(&format!("k{i:02}"), &noise(i + 1, 10_000)))
            .await
            .unwrap();
    }
    let large = put("large", &noise(99, 100_000));
    // Nothing is in a table yet, so nothing can be merged away: full.
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Quota)
    ));
    flushed(&db, 1).await;
    // Nor from one table and nothing else.
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Quota)
    ));
    let full = db.info().bytes;
    commit(
        &db,
        Txn {
            writes: (0..20).map(|i| (b(&format!("k{i:02}")), None)).collect(),
            ..Txn::default()
        },
    )
    .await
    .unwrap();
    // Deleting is itself counted, until it is merged.
    assert!(db.info().bytes > full);
    // The write is put off, not refused, and then taken.
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Busy(_))
    ));
    commit_at_quota(&db, large).await.unwrap();
    assert!(db.info().bytes < 150_000, "{}", db.info().bytes);
    assert_eq!(get(&db, "k07").await, None);
    assert!(get(&db, "large").await.is_some());
    // A merge that made room is not one to wait after: what is written
    // over next is merged away as soon as its room is asked for.
    for i in 0..13 {
        commit(&db, put(&format!("k{i:02}"), &noise(i + 1, 10_000)))
            .await
            .unwrap();
    }
    // These are flushed, and the next is written after them: the merge is
    // asked for with a write that is in no table, and takes it with it.
    // Were the write in the flush, a merge of the two tables would leave
    // out what it wrote over, and the write that follows would have room.
    db.request_flush();
    while db.mem_bytes() > 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    commit(&db, put("large", "now small")).await.unwrap();
    let other = put("other", &noise(98, 100_000));
    assert!(matches!(
        commit(&db, other.clone()).await,
        Err(Error::Busy(_))
    ));
    commit_at_quota(&db, other).await.unwrap();
    assert_eq!(get(&db, "large").await.unwrap().1, "now small");
}

#[tokio::test]
async fn what_was_overwritten_is_merged_away_before_the_quota_refuses() {
    let (ctx, config) = at_quota(256 << 10, Duration::from_secs(3600));
    let (db, _) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    for i in 0..20 {
        commit(&db, put(&format!("k{i:02}"), &noise(i + 1, 10_000)))
            .await
            .unwrap();
    }
    flushed(&db, 1).await;
    for i in 0..4 {
        commit(&db, put(&format!("k{i:02}"), &noise(100 + i, 10_000)))
            .await
            .unwrap();
    }
    // Two tables, the newer too small to be merged with the older for its
    // size: 240,000 bytes are counted and 200,000 can be read.
    flushed(&db, 2).await;
    let another = put("another", &noise(200, 30_000));
    assert!(matches!(
        commit(&db, another.clone()).await,
        Err(Error::Busy(_))
    ));
    commit_at_quota(&db, another).await.unwrap();
    assert_eq!(db.info().tables, 1);
    assert_eq!(get(&db, "k02").await.unwrap().1, noise(102, 10_000));
    assert_eq!(get(&db, "k12").await.unwrap().1, noise(13, 10_000));
}

/// A merge of everything rewrites the database. One that is full of what
/// can still be read is not rewritten for every write it has no room for.
#[tokio::test]
async fn a_database_that_stays_full_is_not_merged_for_every_write() {
    let (ctx, config) = at_quota(256 << 10, Duration::from_secs(3600));
    let (db, _) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    for i in 0..20 {
        commit(&db, put(&format!("k{i:02}"), &noise(i + 1, 10_000)))
            .await
            .unwrap();
    }
    flushed(&db, 1).await;
    let large = put("large", &noise(99, 100_000));
    // One table and a deletion that is not in one: everything is merged,
    // a twentieth is gained, and the write is refused.
    commit(&db, delete("k19")).await.unwrap();
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Busy(_))
    ));
    assert!(matches!(
        commit_at_quota(&db, large.clone()).await,
        Err(Error::Quota)
    ));
    assert_eq!(db.info().tables, 1);
    // The same again at once: refused without a merge.
    commit(&db, put("small/2", "2")).await.unwrap();
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Quota)
    ));
    flushed(&db, 2).await;
    assert!(matches!(
        commit(&db, large.clone()).await,
        Err(Error::Quota)
    ));
    assert_eq!(db.info().tables, 2);
    assert_eq!(get(&db, "small/2").await.unwrap().1, "2");
    // Nor does a deletion bring the next merge forward: one deletion and
    // one write, over and over, would have everything merged for each. The
    // write is told to wait, since what was deleted will be merged away,
    // and nothing is merged while the wait lasts.
    for i in 0..3 {
        commit(&db, delete(&format!("k{i:02}"))).await.unwrap();
        assert!(matches!(
            commit(&db, large.clone()).await,
            Err(Error::Busy(_))
        ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(db.info().tables, 2);
    }
    assert!(get(&db, "k13").await.is_some());
}

/// The wait ends: a database that is full of what can still be read is
/// merged for its quota again, later.
#[tokio::test]
async fn a_database_that_stays_full_is_merged_again_after_a_while() {
    let (ctx, config) = at_quota(256 << 10, Duration::from_millis(50));
    let (db, _) = Db::open(ctx.clone(), "app", Some(config)).await.unwrap();
    for i in 0..20 {
        commit(&db, put(&format!("k{i:02}"), &noise(i + 1, 10_000)))
            .await
            .unwrap();
    }
    flushed(&db, 1).await;
    let large = put("large", &noise(99, 100_000));
    for round in 0..2 {
        commit(&db, put(&format!("small/{round}"), "1"))
            .await
            .unwrap();
        // Refused while the wait lasts, the second time round; then put
        // off for a merge, which makes no room.
        let put_off = async {
            while !matches!(commit(&db, large.clone()).await, Err(Error::Busy(_))) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(20), put_off)
            .await
            .expect("the wait ends");
        assert!(matches!(
            commit_at_quota(&db, large.clone()).await,
            Err(Error::Quota)
        ));
        assert_eq!(db.info().tables, 1);
    }
    // What is deleted now is merged away when the wait is over, and the
    // write that needs its room is told to wait until then, not refused.
    commit(
        &db,
        Txn {
            writes: (0..10).map(|i| (b(&format!("k{i:02}")), None)).collect(),
            ..Txn::default()
        },
    )
    .await
    .unwrap();
    commit_at_quota(&db, large).await.unwrap();
    assert_eq!(get(&db, "k03").await, None);
    assert!(get(&db, "k13").await.is_some());
}

#[tokio::test]
async fn a_deleted_database_stays_deleted() {
    let store = Arc::new(MemoryStore::new());
    let ctx = ctx(store.clone());
    let db = create(&ctx, "app").await;
    commit(&db, put("k", "v")).await.unwrap();
    db.destroy().await.unwrap();
    assert_eq!(store.keys(), ["root/db/app/m/~"]);
    assert!(matches!(
        Db::open(ctx.clone(), "app", None).await,
        Err(Error::Gone)
    ));
    assert!(matches!(
        Db::open(ctx.clone(), "app", Some(DbConfig::default())).await,
        Err(Error::Gone)
    ));
}

#[tokio::test]
async fn a_directory_behaves_like_a_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        flush_frames: 4,
        idle_flush: Duration::from_millis(20),
        ..Options::default()
    };
    let store = || Arc::new(FsStore::open(dir.path(), true).unwrap());
    let first = ctx_with(store(), opts.clone());
    let db = create(&first, "app").await;
    for i in 0..20 {
        commit(&db, put(&format!("k{i:02}"), "v")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(db);
    // A second process on the same directory takes over like any other node.
    let second = ctx_with(store(), opts);
    let db = open(&second, "app").await;
    assert_eq!(scan(&db, "", "", false, 100).await.0.len(), 20);
    assert_eq!(commit(&db, put("k20", "v")).await.unwrap(), 21);
    db.close().await;
}

#[tokio::test]
async fn the_disk_cache_serves_tables_and_survives_damage() {
    let store = Arc::new(MemoryStore::new());
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        flush_frames: 8,
        idle_flush: Duration::from_millis(20),
        block_bytes: 512,
        ..Options::default()
    };
    // Each call is a new process: empty memory, the same cache directory.
    let process = || {
        let disk = DiskCache::open(dir.path(), 4 << 20).unwrap();
        Arc::new(Ctx {
            io: Io::new(store.clone(), 1 << 20, Some(disk)),
            opts: opts.clone(),
            root: "root/".into(),
        })
    };
    let first = process();
    let db = create(&first, "app").await;
    // Values that do not compress to nothing, so that what the cache holds
    // does not depend on how many tables they were merged into.
    for i in 0..300 {
        commit(&db, put(&format!("k{i:04}"), &noise(i + 1, 200)))
            .await
            .unwrap();
    }
    db.close().await;
    assert!(first.io.disk.as_ref().unwrap().used() > 10_000);
    // What the merges replaced left the cache when it left the manifest.
    let listed = listed_tables(&store).await;
    assert_eq!(
        first.io.disk.as_ref().unwrap().used(),
        listed.iter().map(|table| table.0).sum::<u64>()
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), listed.len());

    let second = process();
    let db = open(&second, "app").await;
    assert_eq!(scan(&db, "", "", false, 1000).await.0.len(), 300);
    let disk = second.io.disk.as_ref().unwrap();
    assert!(disk.hits.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert_eq!(disk.misses.load(std::sync::atomic::Ordering::Relaxed), 0);
    db.close().await;

    // Damage every cached file. Checksums catch it and the bucket is read.
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        let mut bytes = std::fs::read(entry.path()).unwrap();
        for byte in bytes.iter_mut().step_by(97) {
            *byte ^= 0x5a;
        }
        std::fs::write(entry.path(), bytes).unwrap();
    }
    let third = process();
    let db = open(&third, "app").await;
    assert_eq!(get(&db, "k0123").await.unwrap().1, noise(124, 200));
    assert_eq!(scan(&db, "", "", true, 1000).await.0.len(), 300);

    // A cache that is opened with a smaller budget is fitted to it.
    let small = DiskCache::open(dir.path(), 1).unwrap();
    assert_eq!(small.used(), 0);
    let tiny = Io::new(store.clone(), 1 << 20, Some(small));
    tiny.remember("anything", Bytes::from_static(b"x"), &[], &[])
        .await;
    assert_eq!(tiny.disk.as_ref().unwrap().used(), 1);
}

/// A change is applied to the configuration as it stands when its turn
/// comes, so none of several made at once is lost.
#[tokio::test]
async fn configuration_changes_made_at_once_all_take_effect() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let db = create(&ctx, "app").await;
    let changes = (0..40).map(|i| {
        let db = db.clone();
        let change = match i % 4 {
            0 => ConfigChange {
                max_bytes: Some(7 << 20),
                ..ConfigChange::default()
            },
            1 => ConfigChange {
                max_requests_per_second: Some(9),
                ..ConfigChange::default()
            },
            _ => ConfigChange {
                revoke_tokens: true,
                ..ConfigChange::default()
            },
        };
        async move { db.configure(change).await }
    });
    for changed in futures::future::join_all(changes).await {
        changed.unwrap();
    }
    let config = db.config();
    assert_eq!(
        (
            config.max_bytes,
            config.max_requests_per_second,
            config.token_generation
        ),
        (7 << 20, 9, 20)
    );
    db.close().await;
    assert_eq!(open(&ctx, "app").await.config(), config);
}

/// Room for a result of `most` bytes; remembers the most it was asked for.
struct Limited {
    most: usize,
    asked: std::sync::atomic::AtomicUsize,
}

impl Limited {
    fn new(most: usize) -> Self {
        Self {
            most,
            asked: Default::default(),
        }
    }

    fn asked(&self) -> usize {
        self.asked.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl durable_db::db::Room for Limited {
    fn room(&self, held: usize) -> Result<usize, Error> {
        self.asked
            .fetch_max(held, std::sync::atomic::Ordering::Relaxed);
        self.most.checked_sub(held).ok_or(Error::TooLarge)
    }
}

/// A read asks for room as its result grows, and stops when there is none,
/// long before it has gathered everything that was asked for.
#[tokio::test]
async fn a_read_stops_when_there_is_no_more_room_for_its_result() {
    let ctx = ctx(Arc::new(MemoryStore::new()));
    let db = create(&ctx, "app").await;
    let value = "v".repeat(1000);
    let keys: Vec<Bytes> = (0..200).map(|i| b(&format!("k{i:03}"))).collect();
    let writes = keys.iter().map(|key| (key.clone(), Some(b(&value))));
    committed(
        &db,
        Txn {
            writes: writes.collect(),
            ..Txn::default()
        },
    )
    .await;
    let everything = Scan {
        lo: Bytes::new(),
        hi: Bytes::new(),
        reverse: false,
        limit: 1000,
    };
    let by_key = ReadReq {
        keys: keys.clone(),
        ..ReadReq::default()
    };
    let by_scan = ReadReq {
        scans: vec![everything.clone()],
        ..ReadReq::default()
    };
    // From memory first, then from a table.
    for flushed in [false, true] {
        if flushed {
            db.request_flush();
            while db.info().tables != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let tight = Limited::new(10_000);
        let refused = db.read_within(by_key.clone(), &tight).await;
        assert!(matches!(refused, Err(Error::TooLarge)), "{flushed}");
        // It stopped near the limit, not at the 200,000 bytes asked for.
        assert!(tight.asked() < 60_000, "{}", tight.asked());
        let refused = db.read_within(by_scan.clone(), &Limited::new(10_000)).await;
        assert!(matches!(refused, Err(Error::TooLarge)), "{flushed}");
        // With room, the same reads say how much they came to hold.
        let ample = Limited::new(1 << 20);
        let found = db.read_within(by_key.clone(), &ample).await.unwrap();
        assert_eq!(found.values.iter().flatten().count(), 200);
        assert!(
            (200_000..220_000).contains(&ample.asked()),
            "{}",
            ample.asked()
        );
        let ample = Limited::new(1 << 20);
        let found = db.read_within(by_scan.clone(), &ample).await.unwrap();
        assert_eq!(found.scans[0].items.len(), 200);
        assert!(ample.asked() >= 200_000);
    }
    // Deleted keys still in memory hide what a table holds. A scan has to
    // step over each of them, and counts them as it does.
    committed(
        &db,
        Txn {
            writes: keys.iter().map(|key| (key.clone(), None)).collect(),
            ..Txn::default()
        },
    )
    .await;
    let tight = Limited::new(5_000);
    let refused = db.read_within(by_scan.clone(), &tight).await;
    assert!(matches!(refused, Err(Error::TooLarge)));
    // It was stopped while stepping over them, not once it held them all.
    assert!(tight.asked() <= 5_000, "{}", tight.asked());
    let ample = Limited::new(1 << 20);
    let found = db.read_within(by_scan, &ample).await.unwrap();
    assert!(found.scans[0].items.is_empty());
    assert!(ample.asked() >= 200 * 64, "{}", ample.asked());
}

/// Commit, waiting out a database that is busy flushing.
async fn committed(db: &Arc<Db>, txn: Txn) -> u64 {
    loop {
        match commit(db, txn.clone()).await {
            Err(Error::Busy(_)) => tokio::time::sleep(Duration::from_millis(2)).await,
            done => return done.unwrap(),
        }
    }
}

/// Text that does not compress, so that tables have the size of their data.
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

/// The tables the newest manifest lists: size, first key, last key, part.
async fn listed_tables(store: &MemoryStore) -> Vec<(u64, String, String, u64)> {
    let newest = store
        .keys()
        .into_iter()
        .filter(|key| key.contains("/m/") && !key.ends_with('~'))
        .max()
        .expect("a manifest");
    let manifest: serde_json::Value =
        serde_json::from_slice(&store.get(&newest).await.unwrap().unwrap()).unwrap();
    let text = |value: &serde_json::Value| value.as_str().unwrap().to_owned();
    manifest["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| {
            (
                table["size"].as_u64().unwrap(),
                text(&table["first"]),
                text(&table["last"]),
                table["part"].as_u64().unwrap_or(0),
            )
        })
        .collect()
}

/// A merge writes its result as tables of limited size, so it holds that
/// much in memory whatever the size of the database. They are read as the
/// one table they stand for, by this owner and by the next.
#[tokio::test]
async fn a_merge_writes_tables_of_limited_size() {
    const TABLE: usize = 8 << 10;
    let store = Arc::new(MemoryStore::new());
    let opts = Options {
        flush_bytes: 16 << 10,
        idle_flush: Duration::from_millis(10),
        block_bytes: 512,
        table_bytes: TABLE,
        ..Options::default()
    };
    let ctx = ctx_with(store.clone(), opts);
    let db = create(&ctx, "app").await;
    let key = |i: usize| format!("k{i:04}");
    let value =
        |round: u64, i: usize| format!("{round}-{}", noise(round * 1000 + i as u64 + 1, 120));
    for round in 0..5 {
        let writes = (0..400).map(|i| (b(&key(i)), Some(b(&value(round, i)))));
        committed(
            &db,
            Txn {
                writes: writes.collect(),
                ..Txn::default()
            },
        )
        .await;
        db.request_flush();
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    // Merging settles on one run: tables that follow one another in key order.
    let settled = async {
        loop {
            let tables = listed_tables(&store).await;
            let parts: Vec<u64> = tables.iter().map(|table| table.3).collect();
            if tables.len() > 3 && parts == (0..tables.len() as u64).collect::<Vec<_>>() {
                return tables;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let tables = tokio::time::timeout(Duration::from_secs(10), settled)
        .await
        .expect("the tables are merged into one run");
    // 400 keys of about 130 bytes: 52 KiB, in tables of 8 KiB and a little.
    assert!((6..=9).contains(&tables.len()), "{tables:?}");
    for (i, (size, first, last, _)) in tables.iter().enumerate() {
        assert!(*size as usize <= TABLE + (2 << 10), "{tables:?}");
        assert!(first <= last);
        assert!(i == 0 || tables[i - 1].2 < *first, "{tables:?}");
    }
    assert_eq!(db.info().tables, tables.len());
    // Whoever reads, the newest owner or the next, sees one ordered map.
    for reopened in [false, true] {
        let db = if reopened {
            db.close().await;
            open(&ctx, "app").await
        } else {
            db.clone()
        };
        let everything: Vec<String> = (0..400).map(key).collect();
        let (forward, more) = scan(&db, "", "", false, 1000).await;
        assert_eq!((&forward, more), (&everything, false));
        let (backward, _) = scan(&db, "", "", true, 1000).await;
        assert_eq!(
            backward,
            everything.iter().rev().cloned().collect::<Vec<_>>()
        );
        for i in [0, 1, 57, 199, 200, 398, 399] {
            assert_eq!(get(&db, &key(i)).await.unwrap().1, value(4, i), "{i}");
        }
        assert_eq!(get(&db, "k0400").await, None);
        // Ranges that begin and end inside tables, and pages that end where
        // one table gives way to the next.
        for (_, first, last, _) in &tables {
            let (page, more) = scan(&db, last, "", false, 3).await;
            let from: usize = last[1..].parse().unwrap();
            let expected: Vec<String> = (from..400).take(3).map(key).collect();
            assert_eq!((page, more), (expected, from + 3 < 400), "{last}");
            let (page, _) = scan(&db, "", &format!("{first}\u{1}"), true, 3).await;
            let upto: usize = first[1..].parse().unwrap();
            let expected: Vec<String> = (0..=upto).rev().take(3).map(key).collect();
            assert_eq!(page, expected, "{first}");
        }
        let (middle, more) = scan(&db, "k0100", "k0300", false, 1000).await;
        assert_eq!((middle.len(), more), (200, false));
        if reopened {
            // Deletions, flushed to a table of their own above the run,
            // hide every table of it.
            let deletes = (0..400).map(|i| (b(&key(i)), None));
            committed(
                &db,
                Txn {
                    writes: deletes.collect(),
                    ..Txn::default()
                },
            )
            .await;
            db.request_flush();
            while db.info().tables != tables.len() + 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(scan(&db, "", "", false, 1000).await.0.len(), 0);
            assert_eq!(scan(&db, "k0100", "k0300", true, 1000).await.0.len(), 0);
            assert_eq!(get(&db, &key(57)).await, None);
        }
    }
}

/// A table's index is held by the cache, not by the table. With far more
/// index than the cache has room for, what is in memory stays within the
/// cache, and every key is still found.
#[tokio::test]
async fn table_indexes_stay_within_the_cache() {
    const CACHE: u64 = 1 << 20;
    let store = Arc::new(MemoryStore::new());
    let ctx = Arc::new(Ctx {
        io: Io::new(store.clone(), CACHE, None),
        opts: Options {
            flush_bytes: 128 << 10,
            idle_flush: Duration::from_millis(10),
            block_bytes: 256,
            table_bytes: 64 << 10,
            ..Options::default()
        },
        root: "root/".into(),
    });
    let db = create(&ctx, "app").await;
    // Keys of a kilobyte with one-byte values: every block holds one key,
    // so the index is as large as the data. Four megabytes of each.
    let key = |i: usize| format!("{i:04}-{}", noise(i as u64 + 1, 995));
    for batch in 0..40 {
        let writes = (batch * 100..batch * 100 + 100).map(|i| (b(&key(i)), Some(b("1"))));
        committed(
            &db,
            Txn {
                writes: writes.collect(),
                ..Txn::default()
            },
        )
        .await;
    }
    db.request_flush();
    while db.mem_bytes() > 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let indexes = || {
        ctx.io
            .index_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    // Whatever was read last is in use; everything else must fit the cache.
    let within = |what: &str| {
        assert!(indexes() <= CACHE + (256 << 10), "{what}: {}", indexes());
        assert!(ctx.io.cache_bytes() <= CACHE, "{what}");
    };
    within("after writing");
    for i in (0..4000).step_by(7) {
        assert_eq!(get(&db, &key(i)).await.unwrap().1, "1", "{i}");
        within("while looking keys up");
    }
    let (keys, more) = scan(&db, "", "", false, 5000).await;
    assert_eq!((keys.len(), more), (4000, false));
    within("after a scan of everything");
    let (keys, _) = scan(&db, "2000", "3000", true, 5000).await;
    assert_eq!(keys.len(), 1000);
    within("after a scan backwards");
    // A full cache keeps nothing it has seen only once, so a scan of
    // everything does not displace what is in use. A table that is read
    // again and again keeps its index: a read of it then asks storage for
    // no more than a read that touches no table does.
    let asked = || store.gets.load(std::sync::atomic::Ordering::Relaxed);
    let before = asked();
    assert_eq!(get(&db, "zzzz").await, None);
    let nothing = asked() - before;
    for _ in 0..4 {
        get(&db, &key(5)).await.unwrap();
    }
    let before = asked();
    get(&db, &key(5)).await.unwrap();
    assert_eq!(asked() - before, nothing);
    assert!(indexes() > 0);
    within("after reading one table repeatedly");
    // Tables that are gone hold nothing.
    db.close().await;
    drop(db);
    let db = open(&ctx, "app").await;
    assert_eq!(get(&db, &key(3999)).await.unwrap().1, "1");
    within("after reopening");
}

/// Each database remembers the keys it wrote recently, to check ranges
/// against. All of them together stay within a budget: those that remember
/// most give up their oldest, and their range checks from before that are
/// refused rather than passed.
#[tokio::test]
async fn databases_together_remember_a_limited_amount_of_written_keys() {
    const BUDGET: usize = 256 << 10;
    let limits = Limits {
        recent_bytes: BUDGET,
        ..Limits::default()
    };
    let node = Node::new(
        ctx(Arc::new(MemoryStore::new())),
        limits,
        Arc::new(Metrics::default()),
    );
    let write = |name: &'static str, keys: std::ops::Range<usize>| {
        let node = node.clone();
        async move {
            let db = node.db(name).await.unwrap();
            let writes =
                keys.map(|i| (b(&format!("{i:05}-{}", noise(i as u64, 900))), Some(b("1"))));
            committed(
                &db,
                Txn {
                    writes: writes.collect(),
                    ..Txn::default()
                },
            )
            .await
        }
    };
    let since = |name: &'static str, since: u64| {
        let node = node.clone();
        async move {
            let db = node.db(name).await.unwrap();
            let check = Check::Range {
                lo: b("9"),
                hi: Bytes::new(),
                since,
            };
            commit(
                &db,
                Txn {
                    checks: vec![check],
                    ..Txn::default()
                },
            )
            .await
        }
    };
    // Four databases write 200 KiB of keys each, and one writes a little.
    let large = ["a", "b", "c", "d"];
    let mut first = Vec::new();
    for name in large.into_iter().chain(["small"]) {
        node.create(name, DbConfig::default()).await.unwrap();
        first.push(write(name, 0..1).await);
    }
    for name in large {
        write(name, 1..200).await;
    }
    assert!(node.recent_bytes() > 3 * BUDGET);
    for (name, first) in large.into_iter().chain(["small"]).zip(&first) {
        since(name, *first).await.unwrap();
    }

    node.tend().await;
    assert!(node.recent_bytes() <= BUDGET);
    let each = node.db("a").await.unwrap().recent_bytes();
    assert!(each > BUDGET / 5 && each <= BUDGET / 4, "{each}");
    // What a database forgot it no longer vouches for.
    for (name, first) in large.into_iter().zip(&first) {
        let refused = since(name, *first).await.unwrap_err();
        assert!(matches!(refused, Error::Conflict(_)), "{refused:?}");
    }
    // The one that remembered little lost nothing.
    since("small", first[4]).await.unwrap();

    // It stays that way while they go on writing.
    for name in large {
        write(name, 200..400).await;
        assert!(node.db(name).await.unwrap().recent_bytes() <= BUDGET / 4);
    }
    assert!(node.recent_bytes() <= BUDGET);
    let latest = write("a", 400..401).await;
    since("a", latest).await.unwrap();
}

/// A server tends its databases once a second for as long as it runs, and
/// begins by putting them in order of how long each has been idle. That is
/// read from a clock, so it changes while they are being put in order, and
/// an order that changes under a sort is one the sort may refuse with a
/// panic. Tending then stopped for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn databases_in_use_are_tended() {
    let node = Node::new(
        ctx(Arc::new(MemoryStore::new())),
        Limits::default(),
        Arc::new(Metrics::default()),
    );
    let names: Vec<String> = (0..96).map(|i| format!("db{i:02}")).collect();
    for name in &names {
        node.create(name, DbConfig::default()).await.unwrap();
    }
    let busy = tokio::spawn({
        let (node, names) = (node.clone(), names.clone());
        async move {
            loop {
                for name in &names {
                    get(&node.db(name).await.unwrap(), "key").await;
                }
            }
        }
    });
    for _ in 0..2000 {
        node.tend().await;
    }
    busy.abort();
    assert_eq!(node.open_names().len(), names.len());
}

/// What is logged on this thread, for a test that runs on one.
#[derive(Clone, Default)]
struct Logged(Arc<parking_lot::Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for Logged {
    type Writer = Logged;

    fn make_writer(&self) -> Logged {
        self.clone()
    }
}

/// A round that does not end leaves a server without its housekeeping as
/// one that panicked did, and a log must say so for as long as it lasts.
#[tokio::test(start_paused = true)]
async fn a_round_of_housekeeping_that_does_not_end_is_reported() {
    let logged = Logged::default();
    let to_logged = tracing_subscriber::fmt().with_writer(logged.clone());
    let _logging = tracing::subscriber::set_default(to_logged.with_ansi(false).finish());
    let rounds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = rounds.clone();
    let round = move || {
        let round = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move {
            // The third round takes 45 s, and the thirteenth does not end.
            match round {
                2 => tokio::time::sleep(Duration::from_secs(45)).await,
                12 => std::future::pending().await,
                _ => {}
            }
        }
    };
    let task = tokio::spawn(durable_db::server::every(Duration::from_secs(1), round));
    let rounds = || rounds.load(std::sync::atomic::Ordering::Relaxed);
    // The third began after 2 s and ended after 47. Since then there has
    // been a round a second, and not one for each second the third took.
    tokio::time::sleep(Duration::from_millis(52_500)).await;
    assert_eq!(rounds(), 9);
    // The thirteenth began after 56 s.
    tokio::time::sleep(Duration::from_millis(97_500)).await;
    assert_eq!(rounds(), 13);
    assert!(!task.is_finished());
    task.abort();
    let logged = String::from_utf8(logged.0.lock().clone()).unwrap();
    let said: Vec<&str> = logged
        .lines()
        .filter_map(|line| line.split("a round of housekeeping ").nth(1))
        .collect();
    assert_eq!(
        said,
        [
            "has run for 30 s and has not ended",
            "has run for 30 s and has not ended",
            "has run for 60 s and has not ended",
            "has run for 90 s and has not ended",
        ],
        "{logged}"
    );
}

/// What a server does every second is one task for as long as it runs:
/// following its peers, handing databases over, tending them. A round of it
/// that panics must not be the last.
#[tokio::test]
async fn a_round_of_housekeeping_that_panics_is_one_round_lost() {
    let rounds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = rounds.clone();
    let round = move || {
        let round = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move { assert!(round != 1, "the second round panics") }
    };
    let task = tokio::spawn(durable_db::server::every(Duration::from_millis(1), round));
    let went_on = async {
        while rounds.load(std::sync::atomic::Ordering::Relaxed) < 5 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), went_on)
        .await
        .expect("housekeeping stopped at the round that panicked");
    assert!(!task.is_finished());
    task.abort();
}
