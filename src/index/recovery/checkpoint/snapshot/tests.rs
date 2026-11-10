use super::*;
use crate::{
    index::recovery::checkpoint::{Builder, Header},
    journal::{QueueLimits, Writer},
    ownership::root::{Cut, Identity},
    storage::{Object, Remote, Version},
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use uuid::Uuid;

struct Files {
    inner: Remote,
    reads: AtomicUsize,
    fail: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Semaphore,
    keys: std::sync::Mutex<Vec<String>>,
}
impl Files {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Remote::new(Arc::new(object_store::memory::InMemory::new()), "snapshot")
                .unwrap(),
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            keys: std::sync::Mutex::new(Vec::new()),
        })
    }
}
#[async_trait]
impl Storage for Files {
    async fn get_range(&self, k: &str, r: std::ops::Range<u64>, s: u64) -> anyhow::Result<Bytes> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().unwrap().push(k.to_owned());
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.add_permits(1);
            futures::future::pending::<()>().await;
        }
        ensure!(
            !self.fail.swap(false, Ordering::SeqCst),
            "injected read failure"
        );
        self.inner.get_range(k, r, s).await
    }
    async fn put(&self, k: &str, b: Bytes) -> anyhow::Result<()> {
        self.inner.put(k, b).await
    }
    async fn get(&self, _: &str, _: usize) -> anyhow::Result<Option<Object>> {
        panic!("use bounded ranges")
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        panic!("no read writes")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        panic!("no read listings")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        panic!("no read deletion")
    }
}
fn cache(bytes: usize) -> Arc<Cache> {
    Arc::new(Cache::new(bytes, None).unwrap())
}
fn db(generation: u128) -> Database<'static> {
    Database {
        name: if generation == 9 {
            "app-neighbor"
        } else {
            "app"
        },
        generation: Uuid::from_u128(generation),
    }
}
fn value(generation: u128, n: usize) -> Option<Bytes> {
    match n % 3 {
        0 => None,
        1 => Some(Bytes::new()),
        _ => Some(Bytes::from(vec![generation as u8; 128])),
    }
}
async fn fixture(files: Arc<dyn Storage>) -> Reference {
    let mut builder = Builder::new(files.clone());
    for generation in 7..=9 {
        let database = db(generation);
        let mut prefix = database.name.as_bytes().to_vec();
        prefix.push(0);
        prefix.extend_from_slice(database.generation.as_bytes());
        let mut meta = 3u64.to_be_bytes().to_vec();
        meta.extend_from_slice(&1u64.to_be_bytes());
        let values: Vec<_> = (0..180).filter_map(|n| value(generation, n)).collect();
        meta.extend_from_slice(&(values.len() as u64).to_be_bytes());
        meta.extend_from_slice(
            &(values.iter().map(|v| 6 + v.len() as u64).sum::<u64>()).to_be_bytes(),
        );
        builder
            .push(&prefix, 0, b"", 3, Some(meta.into()))
            .await
            .unwrap();
        for r in 2..=3 {
            builder
                .push(
                    &prefix,
                    1,
                    format!("r-{r}").as_bytes(),
                    r,
                    Some(Bytes::from(vec![r as u8; 32])),
                )
                .await
                .unwrap();
        }
        // Deliberately tiny tables put metadata, receipts and values on
        // different leaves; the complete tree spans multiple manifest nodes.
        builder.flush().await.unwrap();
        for n in 0..180 {
            builder
                .push(
                    &prefix,
                    2,
                    format!("k/{n:04}").as_bytes(),
                    (n % 3 + 1) as u64,
                    value(generation, n),
                )
                .await
                .unwrap();
            if n % 3 == 2 {
                builder.flush().await.unwrap();
            }
        }
    }
    let tables = builder.tree.tables;
    assert_eq!(tables, 183);
    let tree = builder.tree.finish().await.unwrap();
    assert_eq!(tree.unwrap().0, 1);
    let writer = Writer::for_root(files.clone(), Uuid::from_u128(42), QueueLimits::default());
    Header {
        cut: Cut {
            identity: Identity {
                epoch: 1,
                incarnation: Uuid::from_u128(42),
            },
            prefix: writer.durable_prefix(),
        },
        tree,
        tables,
        rows: builder.rows,
        databases: 3,
    }
    .upload(files)
    .await
    .unwrap()
}
async fn open(files: Arc<Files>, reference: Reference, memory: usize) -> DatabaseSnapshot {
    Snapshot::open(files, cache(memory), reference)
        .await
        .unwrap()
        .database(db(7))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn seeks_include_exact_table_and_manifest_bounds_and_isolate_generations() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let snapshot = Snapshot::open(files.clone(), cache(1024 * 1024), reference)
        .await
        .unwrap();
    assert_eq!(files.reads.load(Ordering::SeqCst), 1);
    for generation in 7..=9 {
        let database = snapshot.database(db(generation)).await.unwrap().unwrap();
        assert_eq!(database.revision(), 3);
        assert_eq!(database.receipt_floor(), 1);
        for n in 0..180 {
            let key = format!("k/{n:04}");
            let row = database.get(&key).await.unwrap().unwrap();
            assert_eq!(row.key, key.as_bytes());
            assert_eq!(row.revision, (n % 3 + 1) as u64);
            assert_eq!(row.value, value(generation, n));
            assert!(database.get(&format!("{key}-gap")).await.unwrap().is_none());
        }
        assert!(database.get("a").await.unwrap().is_none());
        assert!(database.get("z").await.unwrap().is_none());
        for (start, end) in [(0, 180), (2, 4), (2, 3), (127, 129), (179, 180), (180, 181)] {
            let a = format!("k/{start:04}");
            let b = format!("k/{end:04}");
            let mut cursor = database.scan(&a, Some(&b)).unwrap();
            for n in start..end.min(180) {
                let row = cursor.next().await.unwrap().unwrap();
                assert_eq!(row.key, format!("k/{n:04}").as_bytes());
                assert_eq!(row.value, value(generation, n));
            }
            assert!(cursor.next().await.unwrap().is_none());
            assert!(cursor.next().await.unwrap().is_none());
            let mut cursor = database.scan_ordered(&a, Some(&b), true).unwrap();
            for n in (start..end.min(180)).rev() {
                let row = cursor.next().await.unwrap().unwrap();
                assert_eq!(row.key, format!("k/{n:04}").as_bytes());
                assert_eq!(row.value, value(generation, n));
            }
            assert!(cursor.next().await.unwrap().is_none());
        }
        let mut cursor = database.scan("", None).unwrap();
        let mut count = 0;
        while cursor.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 180);
        let mut cursor = database.scan_ordered("", None, true).unwrap();
        for n in (0..180).rev() {
            assert_eq!(
                cursor.next().await.unwrap().unwrap().key,
                format!("k/{n:04}").as_bytes()
            );
        }
        assert!(cursor.next().await.unwrap().is_none());
    }
    for database in [
        db(6),
        db(10),
        Database {
            name: "ap",
            generation: Uuid::from_u128(7),
        },
    ] {
        assert!(snapshot.database(database).await.unwrap().is_none());
    }
    assert!(
        snapshot
            .database(Database {
                name: "app",
                generation: Uuid::nil()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn receipt_resolution_checks_identity_and_retention_before_snapshot_age() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let view = open(files, reference, 1024 * 1024).await;
    for snapshot in [0, 1, 3, u64::MAX] {
        assert_eq!(
            view.resolve("r-2", [2; 32], snapshot).await.unwrap(),
            Resolution::Committed(2)
        );
    }
    assert_eq!(
        view.resolve("r-2", [3; 32], 0).await.unwrap(),
        Resolution::Reused
    );
    assert_eq!(
        view.resolve("r-1", [1; 32], 0).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.resolve("unused", [0; 32], 1).await.unwrap(),
        Resolution::Missing
    );
    assert!(view.resolve("unused", [0; 32], 4).await.is_err());
    for request in [String::new(), "x".repeat(129)] {
        assert!(view.resolve(&request, [0; 32], 0).await.is_err());
    }
    for key in [String::new(), "x".repeat(1025)] {
        assert!(view.get(&key).await.is_err());
    }
    assert!(view.scan("z", Some("a")).is_err());
    assert!(view.scan("a", Some("a")).is_err());
    assert!(view.scan(&"x".repeat(1025), None).is_err());
}

#[tokio::test]
async fn touched_database_receipts_are_bounded_and_restart_cleanly_after_interruption() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let view = open(files.clone(), reference, 0).await;
    for (count, bytes) in [(1, 1000), (2, 85)] {
        assert!(view.restore_state(count, bytes).await.is_err());
    }
    files.fail.store(true, Ordering::SeqCst);
    assert!(view.restore_state(2, 86).await.is_err());
    files.pause.store(true, Ordering::SeqCst);
    {
        let restoring = view.restore_state(2, 86);
        tokio::pin!(restoring);
        tokio::select! {
            _ = &mut restoring => panic!("restoration must remain pending"),
            permit = files.entered.acquire() => { permit.unwrap().forget(); }
        }
    }
    let state = view.restore_state(2, 86).await.unwrap();
    assert_eq!(state.receipt_usage(), (2, 86));
    assert_eq!(
        state.resolve("r-2", [2; 32], 0).unwrap(),
        Resolution::Committed(2)
    );
    assert_eq!(
        state.resolve("r-1", [1; 32], 0).unwrap(),
        Resolution::Expired
    );
}

#[tokio::test]
async fn cold_work_tracks_tree_height_and_warm_reads_need_no_provider_calls() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let shared = cache(256 * 1024);
    let snapshot = Snapshot::open(files.clone(), shared.clone(), reference)
        .await
        .unwrap();
    let database = snapshot.database(db(8)).await.unwrap().unwrap();
    database.get("k/0178").await.unwrap().unwrap();
    database.resolve("r-2", [2; 32], 0).await.unwrap();
    let mut cursor = database.scan("k/0177", None).unwrap();
    while cursor.next().await.unwrap().is_some() {}
    let reads = files.reads.load(Ordering::SeqCst);
    assert!(reads <= 12, "must not traverse 183 table indexes: {reads}");
    for _ in 0..8 {
        let reopened = Snapshot::open(files.clone(), shared.clone(), reference)
            .await
            .unwrap();
        let database = reopened.database(db(8)).await.unwrap().unwrap();
        assert!(database.get("k/0178").await.unwrap().is_some());
        assert!(database.get("k/0178-gap").await.unwrap().is_none());
        assert_eq!(
            database.resolve("r-2", [2; 32], 0).await.unwrap(),
            Resolution::Committed(2)
        );
        let mut cursor = database.scan("k/0177", None).unwrap();
        while cursor.next().await.unwrap().is_some() {}
    }
    assert_eq!(files.reads.load(Ordering::SeqCst), reads);
    assert!(shared.memory_bytes().await <= 256 * 1024);
    let cold = open(files.clone(), reference, 0).await;
    let before = files.reads.load(Ordering::SeqCst);
    let mut cursor = cold.scan_ordered("", Some("k/0179"), true).unwrap();
    for n in (177..179).rev() {
        assert_eq!(
            cursor.next().await.unwrap().unwrap().key,
            format!("k/{n:04}").as_bytes()
        );
    }
    let reads = files.reads.load(Ordering::SeqCst) - before;
    assert!(
        reads <= 6,
        "reverse page must seek past unrelated tables: {reads}"
    );
}

#[tokio::test]
async fn failed_and_canceled_scans_retire_instead_of_skipping_a_table() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let database = open(files.clone(), reference, 0).await;
    for reverse in [false, true] {
        let mut cursor = database.scan_ordered("", None, reverse).unwrap();
        files.fail.store(true, Ordering::SeqCst);
        assert!(cursor.next().await.is_err());
        assert!(cursor.next().await.is_err());
        let mut cursor = database.scan_ordered("", None, reverse).unwrap();
        let first = cursor.next().await.unwrap().unwrap();
        assert_eq!(
            first.key,
            if reverse {
                Bytes::from_static(b"k/0179")
            } else {
                Bytes::from_static(b"k/0000")
            }
        );
        // Drain the first tiny table so the cancellation interrupts new I/O.
        cursor.next().await.unwrap().unwrap();
        cursor.next().await.unwrap().unwrap();
        files.pause.store(true, Ordering::SeqCst);
        {
            let next = cursor.next();
            tokio::pin!(next);
            tokio::select! {
                _ = &mut next => panic!("read must remain pending"),
                permit = files.entered.acquire() => { permit.unwrap().forget(); }
            }
        }
        assert!(cursor.next().await.is_err());
        let mut fresh = database.scan_ordered("", None, reverse).unwrap();
        assert_eq!(
            fresh.next().await.unwrap().unwrap().key,
            if reverse {
                Bytes::from_static(b"k/0179")
            } else {
                Bytes::from_static(b"k/0000")
            }
        );
    }
    files.fail.store(true, Ordering::SeqCst);
    assert!(database.get("missing").await.is_err());
    assert!(database.get("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn corrupted_disk_cache_refetches_but_missing_authoritative_data_is_an_error() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let shared = Arc::new(Cache::new(0, Some((dir.path().into(), 1024 * 1024))).unwrap());
    let snapshot = Snapshot::open(files.clone(), shared.clone(), reference)
        .await
        .unwrap();
    let view = snapshot.database(db(7)).await.unwrap().unwrap();
    view.get("k/0002").await.unwrap().unwrap();
    let reads = files.reads.load(Ordering::SeqCst);
    let object = files.keys.lock().unwrap().last().unwrap().clone();
    drop((view, snapshot, shared));
    for file in std::fs::read_dir(dir.path()).unwrap() {
        let file = file.unwrap();
        if file.file_name() != "LOCK" {
            std::fs::write(file.path(), b"bad cache").unwrap();
        }
    }
    let shared = Arc::new(Cache::new(0, Some((dir.path().into(), 1024 * 1024))).unwrap());
    let snapshot = Snapshot::open(files.clone(), shared, reference)
        .await
        .unwrap();
    let view = snapshot.database(db(7)).await.unwrap().unwrap();
    assert_eq!(
        view.get("k/0002").await.unwrap().unwrap().value,
        value(7, 2)
    );
    assert!(files.reads.load(Ordering::SeqCst) > reads);
    files.inner.delete(&object).await.unwrap();
    // A validated cached immutable block remains usable; a cold read fails.
    assert!(view.get("k/0002").await.unwrap().is_some());
    let cold = open(files.clone(), reference, 0).await;
    assert!(cold.get("k/0002").await.is_err());
    let mut scan = cold.scan("k/0002", Some("k/0003")).unwrap();
    assert!(scan.next().await.is_err());
    assert!(scan.next().await.is_err());
}

#[tokio::test]
async fn missing_and_corrupt_referenced_pages_never_become_absent_keys() {
    let files = Files::new();
    let reference = fixture(files.clone()).await;
    async fn query(files: Arc<Files>, reference: Reference) -> anyhow::Result<Option<Entry>> {
        let image = Snapshot::open(files, cache(0), reference).await?;
        image
            .database(db(7))
            .await?
            .context("missing database")?
            .get("k/0179")
            .await
    }
    assert!(query(files.clone(), reference).await.unwrap().is_some());
    let keys: std::collections::BTreeSet<_> = files.keys.lock().unwrap().iter().cloned().collect();
    assert!(keys.len() >= 6);
    for key in keys {
        let original = files
            .inner
            .get(&key, crate::storage::MAX_RANGE_BYTES)
            .await
            .unwrap()
            .unwrap()
            .bytes;
        files
            .inner
            .put(&key, Bytes::from(vec![0; original.len()]))
            .await
            .unwrap();
        assert!(query(files.clone(), reference).await.is_err());
        files.inner.delete(&key).await.unwrap();
        assert!(query(files.clone(), reference).await.is_err());
        files.inner.put(&key, original).await.unwrap();
    }
    assert!(query(files, reference).await.unwrap().is_some());
}
