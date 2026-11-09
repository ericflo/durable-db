use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::*,
    publication::{Authority, Bucket, Published},
    storage::{Object, Remote, Storage, Version},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, Notify};

struct Counted {
    inner: Arc<dyn Storage>,
    gets: Arc<AtomicUsize>,
}
#[async_trait]
impl Storage for Counted {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, max).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        self.inner.cas(key, old, bytes).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
}
struct Metadata {
    gets: Arc<AtomicUsize>,
    state: Mutex<Bucket>,
    files: Arc<dyn Storage>,
    pause: AtomicBool,
    lost_ack: AtomicBool,
    paused: Notify,
    resume: Notify,
}
#[async_trait]
impl Authority for Metadata {
    async fn read(&self) -> anyhow::Result<Bucket> {
        Ok(self.state.lock().await.clone())
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let is_publication =
            info.get("durable-db-read-fence") == old.info.get("durable-db-read-fence");
        if is_publication && self.pause.swap(false, Ordering::SeqCst) {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        let mut state = self.state.lock().await;
        if state.revision != old.revision {
            return Ok(None);
        }
        let publication =
            info.get("durable-db-read-fence") == old.info.get("durable-db-read-fence");
        state.revision += 1;
        state.info = info;
        if publication && self.lost_ack.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost response after bucket publication");
        }
        Ok(Some(state.clone()))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.files.delete(key).await
    }
}
async fn setup() -> (Arc<Published>, Arc<Metadata>) {
    let files =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
    let gets = Arc::new(AtomicUsize::new(0));
    let files = Arc::new(Counted {
        inner: files,
        gets: gets.clone(),
    });
    let metadata = Arc::new(Metadata {
        gets,
        state: Mutex::new(Bucket {
            version: None,
            revision: 0,
            info: BTreeMap::from([("unrelated".into(), "preserved".into())]),
        }),
        files: files.clone(),
        pause: AtomicBool::new(false),
        lost_ack: AtomicBool::new(false),
        paused: Notify::new(),
        resume: Notify::new(),
    });
    (
        Arc::new(
            Published::open(files, metadata.clone(), "test")
                .await
                .unwrap(),
        ),
        metadata,
    )
}
fn engine(b2: Arc<Published>) -> Engine {
    Engine::new(b2, Arc::new(Cache::new(0, None).unwrap()))
}
fn request(revision: u64) -> CommitRequest {
    CommitRequest {
        expected_revision: revision,
        request_id: format!("r{revision}"),
        checks: vec![],
        writes: vec![Write {
            key: "a".into(),
            value: Some("aGVsbG8=".into()),
        }],
    }
}

#[tokio::test]
async fn cached_manifests_follow_fresh_authority_and_keep_read_bounds() {
    for memory in [false, true] {
        let (writer, metadata) = setup().await;
        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            Cache::new(
                if memory { 1024 * 1024 } else { 0 },
                if memory {
                    None
                } else {
                    Some((directory.path().to_owned(), 1024 * 1024))
                },
            )
            .unwrap(),
        );
        let reader = Published::open(metadata.files.clone(), metadata.clone(), "test")
            .await
            .unwrap()
            .with_cache(cache);
        // Remote manifests may exceed the data-page size limit.
        let initial = Bytes::from(vec![b'a'; 300 * 1024]);
        writer
            .cas("heads/a.json", None, initial.clone())
            .await
            .unwrap();
        let old = reader
            .get("heads/a.json", 512 * 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.bytes, initial);
        let before = metadata.gets.load(Ordering::SeqCst);
        let revision = metadata.read().await.unwrap().revision;
        assert_eq!(
            reader
                .get("heads/a.json", 512 * 1024)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            initial
        );
        assert_eq!(metadata.gets.load(Ordering::SeqCst), before);
        assert!(metadata.read().await.unwrap().revision > revision);
        assert!(reader.get("heads/a.json", 100).await.is_err());

        writer
            .cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"new"),
            )
            .await
            .unwrap();
        writer.collect_manifests().await.unwrap();
        // The cached old manifest remains present after remote collection. A
        // fresh request must nevertheless select the acknowledged replacement.
        assert_eq!(
            reader
                .get("heads/a.json", 100)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"new"[..]
        );
        assert!(metadata.gets.load(Ordering::SeqCst) > before);
    }
}
#[tokio::test]
async fn native_authority_preserves_metadata_and_compares_database_identity() {
    let (b2, metadata) = setup().await;
    assert!(
        b2.cas("heads/a.json", None, Bytes::from_static(b"one"))
            .await
            .unwrap()
    );
    let old = b2.get("heads/a.json", 100).await.unwrap().unwrap();
    assert!(
        b2.cas("heads/b.json", None, Bytes::from_static(b"other"))
            .await
            .unwrap()
    );
    assert!(
        b2.cas(
            "heads/a.json",
            Some(&old.version),
            Bytes::from_static(b"two")
        )
        .await
        .unwrap()
    );
    assert_eq!(
        b2.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"two"[..]
    );
    assert_eq!(
        metadata.read().await.unwrap().info["unrelated"],
        "preserved"
    );
    assert!(
        Published::open(metadata.files.clone(), metadata.clone(), "wrong-prefix")
            .await
            .is_err()
    );
}
#[tokio::test]
async fn manifest_collector_fences_a_paused_publisher_and_reclaims_its_orphan() {
    let (b2, metadata) = setup().await;
    b2.cas("heads/a.json", None, Bytes::from_static(b"one"))
        .await
        .unwrap();
    let old = b2.get("heads/a.json", 100).await.unwrap().unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let b2 = b2.clone();
        async move {
            b2.cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"two"),
            )
            .await
        }
    });
    metadata.paused.notified().await;
    assert_eq!(b2.collect_manifests().await.unwrap(), 1);
    assert_eq!(
        metadata
            .files
            .list("indexes/", None, 128)
            .await
            .unwrap()
            .len(),
        1
    );
    metadata.resume.notify_one();
    assert!(!task.await.unwrap().unwrap());
    assert_eq!(
        b2.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"one"[..]
    );
    assert_eq!(
        metadata
            .files
            .list("manifests/", None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn lost_bucket_ack_recovers_receipt_after_both_collectors_and_empty_cache() {
    let (b2, metadata) = setup().await;
    let e = engine(b2.clone());
    e.create("test", Limits::default()).await.unwrap();
    metadata.lost_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        e.commit("test", &request(0)).await,
        Err(Error::Storage(_))
    ));
    e.gc("test").await.unwrap();
    b2.collect_manifests().await.unwrap();
    let reopened = engine(b2.clone());
    assert!(reopened.commit("test", &request(0)).await.unwrap().replayed);
    assert_eq!(
        reopened
            .read(
                "test",
                &ReadRequest {
                    keys: vec!["a".into()],
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .values[0]
            .as_deref(),
        Some("aGVsbG8=")
    );
    assert_eq!(
        metadata
            .files
            .list("manifests/", None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn ten_thousand_databases_split_page_recover_and_collect() {
    let (store, metadata) = setup().await;
    // Permute insertion order to exercise both sides of splits and first-key changes.
    let mut expected = Vec::new();
    for n in 0..10_000 {
        let name = format!("site-{:05}", (n * 7919) % 10_000);
        let key = format!("heads/{name}.json");
        assert!(
            store
                .cas(&key, None, Bytes::from(name.clone()))
                .await
                .unwrap()
        );
        expected.push(key);
    }
    // Suffix ordering differs from database-name ordering for these names.
    for name in ["a", "a-", "a0", "a_", &"z".repeat(64)] {
        let key = format!("heads/{name}.json");
        assert!(
            store
                .cas(&key, None, Bytes::from(name.to_owned()))
                .await
                .unwrap()
        );
        expected.push(key);
    }
    expected.sort();
    let state = metadata.read().await.unwrap();
    let root: serde_json::Value = serde_json::from_str(&state.info["durable-db-root"]).unwrap();
    assert_eq!(root["count"], expected.len());
    assert!(root["height"].as_u64().unwrap() >= 3);
    assert!(serde_json::to_vec(&state.info).unwrap().len() < 1500);
    metadata.gets.store(0, Ordering::SeqCst);
    store
        .get("heads/site-05000.json", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        metadata.gets.load(Ordering::SeqCst),
        root["height"].as_u64().unwrap() as usize + 1
    );
    metadata.gets.store(0, Ordering::SeqCst);
    assert_eq!(
        store
            .list("heads/", Some("heads/site-04950.json"), 64)
            .await
            .unwrap()
            .len(),
        64
    );
    assert!(
        metadata.gets.load(Ordering::SeqCst) <= 9,
        "a listing must not hydrate the directory"
    );
    let mut actual = Vec::new();
    loop {
        let page = store
            .list("heads/", actual.last().map(String::as_str), 64)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        actual.extend(page);
    }
    assert_eq!(actual, expected);
    // Update a reused subtree after a root split, then reclaim every obsolete path.
    let old = store
        .get("heads/site-05000.json", 64)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .cas(
                "heads/site-05000.json",
                Some(&old.version),
                Bytes::from_static(b"updated")
            )
            .await
            .unwrap()
    );
    // Cursors bound each pass. Stop only after the entire index keyspace wraps.
    for pass in 0..1000 {
        store.collect_manifests().await.unwrap();
        let state = metadata.read().await.unwrap();
        if state.info["durable-db-index-gc"].is_empty() {
            break;
        }
        assert!(pass < 999, "directory collector did not complete a sweep");
    }
    let indexes = metadata.files.list("indexes/", None, 1000).await.unwrap();
    assert!(indexes.len() < 400, "only the live B+tree should remain");
    let reopened = Published::open(metadata.files.clone(), metadata.clone(), "test")
        .await
        .unwrap();
    for key in &expected {
        let obj = reopened.get(key, 100).await.unwrap().unwrap();
        let db = key
            .strip_prefix("heads/")
            .unwrap()
            .strip_suffix(".json")
            .unwrap();
        assert_eq!(obj.bytes, if db == "site-05000" { "updated" } else { db });
    }
    assert!(
        reopened
            .get("heads/absent.json", 100)
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn multiple_stateless_replicas_survive_both_collectors_and_reopen() {
    let (b2, _) = setup().await;
    engine(b2.clone())
        .create("test", Limits::default())
        .await
        .unwrap();
    let mut tasks = vec![];
    for worker in 0..3 {
        let b2 = b2.clone();
        tasks.push(tokio::spawn(async move {
            let e = engine(b2);
            for op in 0..12 {
                loop {
                    let r = e.info("test").await.unwrap().revision;
                    let mut req = request(r);
                    req.request_id = format!("worker-{worker}-op-{op}");
                    match e.commit("test", &req).await {
                        Ok(_) => break,
                        Err(Error::Conflict) => {}
                        Err(e) => panic!("{e}"),
                    }
                }
            }
        }));
    }
    let gc = tokio::spawn({
        let b2 = b2.clone();
        async move {
            let e = engine(b2.clone());
            for _ in 0..20 {
                let _ = e.gc("test").await;
                b2.collect_manifests().await.unwrap();
                tokio::task::yield_now().await;
            }
        }
    });
    for task in tasks {
        task.await.unwrap();
    }
    gc.await.unwrap();
    let e = engine(b2);
    assert_eq!(e.info("test").await.unwrap().revision, 36);
    assert_eq!(
        e.read(
            "test",
            &ReadRequest {
                keys: vec!["a".into()],
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .values[0]
            .as_deref(),
        Some("aGVsbG8=")
    );
}

#[tokio::test]
async fn directory_admission_reserves_read_and_collection_fences() {
    let (store, metadata) = setup().await;
    metadata
        .state
        .lock()
        .await
        .info
        .insert("padding".into(), "x".repeat(8600));
    // The directory itself fits below 9,000 bytes, but no room remains for its
    // mandatory future fences and manifest pointer. Reject before uploading.
    let before = metadata.files.list("manifests/", None, 100).await.unwrap();
    assert!(
        store
            .cas("heads/test.json", None, Bytes::from_static(b"head"))
            .await
            .is_err()
    );
    assert_eq!(
        before,
        metadata.files.list("manifests/", None, 100).await.unwrap()
    );
}

#[tokio::test]
async fn missing_index_is_not_evidence_that_manifests_are_garbage() {
    let (store, metadata) = setup().await;
    store
        .cas("heads/a.json", None, Bytes::from_static(b"live"))
        .await
        .unwrap();
    let state = metadata.read().await.unwrap();
    let root: serde_json::Value = serde_json::from_str(&state.info["durable-db-root"]).unwrap();
    let before = metadata.files.list("manifests/", None, 128).await.unwrap();
    metadata
        .files
        .delete(root["object"].as_str().unwrap())
        .await
        .unwrap();
    assert!(store.collect_manifests().await.is_err());
    assert_eq!(
        metadata.files.list("manifests/", None, 128).await.unwrap(),
        before
    );
    assert!(store.get("heads/a.json", 64).await.is_err());
    assert!(store.list("heads/", None, 64).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_databases_split_while_publishers_and_collectors_race() {
    let (store, metadata) = setup().await;
    let mut tasks = Vec::new();
    for worker in 0..3 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let e = engine(store);
            for n in 0..40 {
                let db = format!("worker-{worker}-{n:02}");
                let mut created = false;
                for _ in 0..100 {
                    match e.create(&db, Limits::default()).await {
                        Ok(_) => {
                            created = true;
                            break;
                        }
                        Err(Error::Conflict) => tokio::task::yield_now().await,
                        Err(e) => panic!("{e}"),
                    }
                }
                assert!(created, "directory publication must eventually progress");
                tokio::task::yield_now().await;
            }
        }));
    }
    let collector = tokio::spawn({
        let store = store.clone();
        async move {
            for _ in 0..80 {
                store.collect_manifests().await.unwrap();
                tokio::task::yield_now().await;
            }
        }
    });
    for task in tasks {
        task.await.unwrap();
    }
    collector.await.unwrap();
    let reopened = engine(Arc::new(
        Published::open(metadata.files.clone(), metadata.clone(), "test")
            .await
            .unwrap(),
    ));
    for worker in 0..3 {
        for n in 0..40 {
            assert_eq!(
                reopened
                    .info(&format!("worker-{worker}-{n:02}"))
                    .await
                    .unwrap()
                    .revision,
                0
            );
        }
    }
}

#[tokio::test]
async fn idle_page_collection_does_not_rewrite_the_directory() {
    let (store, metadata) = setup().await;
    let e = engine(store.clone());
    e.create("idle", Limits::default()).await.unwrap();
    let empty_root = metadata.read().await.unwrap().info["durable-db-root"].clone();
    assert_eq!(e.gc("idle").await.unwrap().deleted, 0);
    assert_eq!(
        metadata.read().await.unwrap().info["durable-db-root"],
        empty_root
    );
    e.commit("idle", &request(0)).await.unwrap();
    let live_root = metadata.read().await.unwrap().info["durable-db-root"].clone();
    let result = e.gc("idle").await.unwrap();
    assert_eq!(result.examined, 1);
    assert_eq!(result.deleted, 0);
    assert_eq!(
        metadata.read().await.unwrap().info["durable-db-root"],
        live_root
    );
}

#[tokio::test]
async fn publication_retries_read_barriers_and_rebases_unrelated_database_writes() {
    for change_root in [false, true] {
        let (store, metadata) = setup().await;
        store
            .cas("heads/a.json", None, Bytes::from_static(b"one"))
            .await
            .unwrap();
        let old = store.get("heads/a.json", 100).await.unwrap().unwrap();
        metadata.pause.store(true, Ordering::SeqCst);
        let writer = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .cas(
                        "heads/a.json",
                        Some(&old.version),
                        Bytes::from_static(b"two"),
                    )
                    .await
            }
        });
        metadata.paused.notified().await;
        // Advance only the register revision while preserving all reachability.
        for _ in 0..4 {
            metadata.read_fresh().await.unwrap();
        }
        if change_root {
            assert!(
                store
                    .cas("heads/b.json", None, Bytes::from_static(b"other"))
                    .await
                    .unwrap()
            );
        }
        metadata.resume.notify_one();
        assert!(writer.await.unwrap().unwrap());
        let value = store.get("heads/a.json", 100).await.unwrap().unwrap().bytes;
        assert_eq!(value, &b"two"[..]);
        // Rebase rewrites the path, not the immutable transaction manifest.
        assert_eq!(
            metadata
                .files
                .list("manifests/", None, 100)
                .await
                .unwrap()
                .len(),
            if change_root { 3 } else { 2 }
        );
        if change_root {
            assert_eq!(
                store.get("heads/b.json", 100).await.unwrap().unwrap().bytes,
                b"other"[..]
            );
        }
    }
}

async fn wait_paused(metadata: &Metadata) {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        metadata.paused.notified(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn rebasing_never_overwrites_a_competing_write_to_the_same_database() {
    let (store, metadata) = setup().await;
    assert!(
        store
            .cas("heads/a.json", None, Bytes::from_static(b"one"))
            .await
            .unwrap()
    );
    let old = store.get("heads/a.json", 100).await.unwrap().unwrap();
    let version = old.version.clone();
    metadata.pause.store(true, Ordering::SeqCst);
    let writer = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .cas("heads/a.json", Some(&version), Bytes::from_static(b"stale"))
                .await
        }
    });
    wait_paused(&metadata).await;
    assert!(
        store
            .cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"winner")
            )
            .await
            .unwrap()
    );
    metadata.resume.notify_one();
    assert!(!writer.await.unwrap().unwrap());
    assert_eq!(
        store.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"winner"[..]
    );
}

#[tokio::test]
async fn rebased_paths_survive_more_unrelated_writes_but_not_a_collection_fence() {
    for collect in [false, true] {
        let (store, metadata) = setup().await;
        assert!(
            store
                .cas("heads/a.json", None, Bytes::from_static(b"one"))
                .await
                .unwrap()
        );
        let old = store.get("heads/a.json", 100).await.unwrap().unwrap();
        metadata.pause.store(true, Ordering::SeqCst);
        let writer = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .cas(
                        "heads/a.json",
                        Some(&old.version),
                        Bytes::from_static(b"two"),
                    )
                    .await
            }
        });
        wait_paused(&metadata).await;
        assert!(
            store
                .cas("heads/b.json", None, Bytes::from_static(b"other"))
                .await
                .unwrap()
        );
        metadata.pause.store(true, Ordering::SeqCst);
        metadata.resume.notify_one();
        wait_paused(&metadata).await; // A rebuilt path is uploaded, awaiting its CAS.
        if collect {
            assert_eq!(store.collect_manifests().await.unwrap(), 1);
        } else {
            assert!(
                store
                    .cas("heads/c.json", None, Bytes::from_static(b"third"))
                    .await
                    .unwrap()
            );
        }
        metadata.resume.notify_one();
        assert_eq!(writer.await.unwrap().unwrap(), !collect);
        let reopened = Published::open(metadata.files.clone(), metadata.clone(), "test")
            .await
            .unwrap();
        assert_eq!(
            reopened
                .get("heads/a.json", 100)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            if collect { &b"one"[..] } else { &b"two"[..] }
        );
        assert_eq!(
            reopened
                .get("heads/b.json", 100)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"other"[..]
        );
        if !collect {
            assert_eq!(
                reopened
                    .get("heads/c.json", 100)
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes,
                b"third"[..]
            );
            assert_eq!(
                metadata
                    .files
                    .list("manifests/", None, 100)
                    .await
                    .unwrap()
                    .len(),
                4
            );
        }
    }
}

#[tokio::test]
async fn lost_acknowledgement_after_rebase_is_resolved_by_the_original_receipt() {
    let (store, metadata) = setup().await;
    let e = Arc::new(engine(store.clone()));
    e.create("a", Limits::default()).await.unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let writer = tokio::spawn({
        let e = e.clone();
        async move { e.commit("a", &request(0)).await }
    });
    wait_paused(&metadata).await;
    e.create("b", Limits::default()).await.unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    metadata.resume.notify_one();
    wait_paused(&metadata).await;
    metadata.lost_ack.store(true, Ordering::SeqCst);
    metadata.resume.notify_one();
    assert!(matches!(writer.await.unwrap(), Err(Error::Storage(_))));
    e.gc("a").await.unwrap();
    store.collect_manifests().await.unwrap();
    let reopened = engine(Arc::new(
        Published::open(metadata.files.clone(), metadata.clone(), "test")
            .await
            .unwrap(),
    ));
    assert!(reopened.commit("a", &request(0)).await.unwrap().replayed);
    assert_eq!(reopened.info("a").await.unwrap().revision, 1);
    assert_eq!(reopened.info("b").await.unwrap().revision, 0);
}
