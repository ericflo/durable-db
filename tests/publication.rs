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
    proposal_gets: Arc<AtomicUsize>,
    proposal_lists: Arc<AtomicUsize>,
    lost_proposal_ack: Arc<AtomicBool>,
}
#[async_trait]
impl Storage for Counted {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if key.starts_with("proposals/") {
            self.proposal_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, max).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        self.inner.cas(key, old, bytes).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await?;
        if key.starts_with("proposals/") && self.lost_proposal_ack.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost response after proposal upload");
        }
        Ok(())
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        if prefix == "proposals/" {
            self.proposal_lists.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
}
struct Metadata {
    gets: Arc<AtomicUsize>,
    proposal_gets: Arc<AtomicUsize>,
    proposal_lists: Arc<AtomicUsize>,
    lost_proposal_ack: Arc<AtomicBool>,
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
    setup_with_budget(None).await
}
async fn setup_with_budget(
    limits: Option<durable_db::upload_budget::UploadLimits>,
) -> (Arc<Published>, Arc<Metadata>) {
    let files =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
    let gets = Arc::new(AtomicUsize::new(0));
    let proposal_gets = Arc::new(AtomicUsize::new(0));
    let proposal_lists = Arc::new(AtomicUsize::new(0));
    let lost_proposal_ack = Arc::new(AtomicBool::new(false));
    let files = Arc::new(Counted {
        inner: files,
        gets: gets.clone(),
        proposal_gets: proposal_gets.clone(),
        proposal_lists: proposal_lists.clone(),
        lost_proposal_ack: lost_proposal_ack.clone(),
    });
    let metadata = Arc::new(Metadata {
        gets,
        proposal_gets,
        proposal_lists,
        lost_proposal_ack,
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
            Published::open_with_budget(files, metadata.clone(), "test", limits)
                .await
                .unwrap(),
        ),
        metadata,
    )
}
fn engine(b2: Arc<Published>) -> Engine {
    Engine::new(b2, Arc::new(Cache::new(0, None).unwrap()))
}

#[tokio::test]
async fn revocation_invalidates_an_abandoned_write_and_policy_receipts_survive_helping() {
    use durable_db::auth::Auth;
    let (published, metadata) = setup().await;
    let db = engine(published);
    db.create("test", Limits::default()).await.unwrap();
    db.create("helper", Limits::default()).await.unwrap();
    let enable = PolicyRequest {
        request_id: "enable".into(),
        expected_policy_revision: 0,
        change: PolicyAction::Enable,
    };
    metadata.lost_proposal_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        db.change_policy("test", &enable).await,
        Err(Error::Storage(_))
    ));
    engine(replica(&metadata).await)
        .commit("helper", &request(0))
        .await
        .unwrap();
    let enabled = db.change_policy("test", &enable).await.unwrap();
    assert!(
        enabled.replayed,
        "a different database's helper published the policy"
    );
    let auth = Auth::new(&"24".repeat(32)).unwrap();
    let (token, id) = auth
        .issue_for_policy(
            "test".into(),
            true,
            3600,
            enabled.policy.incarnation,
            enabled.policy.generation,
        )
        .unwrap();
    let access = auth.access(&token, "test", true).unwrap();
    metadata.lost_proposal_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        db.commit_with_access("test", &request(0), &access).await,
        Err(Error::Storage(_))
    ));
    let revoke = PolicyRequest {
        request_id: "revoke".into(),
        expected_policy_revision: 1,
        change: PolicyAction::Revoke { token_id: id },
    };
    engine(replica(&metadata).await)
        .change_policy("test", &revoke)
        .await
        .unwrap();
    // Later helpers still encounter the abandoned write but its expected
    // manifest identity precedes revocation, so it can never publish now.
    engine(replica(&metadata).await)
        .commit("helper", &request(1))
        .await
        .unwrap();
    assert_eq!(db.info("test").await.unwrap().revision, 0);
    assert!(matches!(
        db.commit_with_access("test", &request(0), &access).await,
        Err(Error::Unauthorized)
    ));
    db.gc("test").await.unwrap();
    db.maintain_storage().await.unwrap();
    assert_eq!(
        engine(replica(&metadata).await)
            .info("test")
            .await
            .unwrap()
            .policy
            .unwrap()
            .revoked_tokens,
        1
    );
}

#[tokio::test]
async fn protected_creation_receipt_survives_abandonment_helping_and_collection() {
    let (published, metadata) = setup().await;
    let db = engine(published);
    db.create("helper", Limits::default()).await.unwrap();
    let create = CreateRequest {
        request_id: "create-protected".into(),
        limits: Limits::default(),
    };
    metadata.lost_proposal_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        db.create_protected("test", &create).await,
        Err(Error::Storage(_))
    ));
    engine(replica(&metadata).await)
        .commit("helper", &request(0))
        .await
        .unwrap();
    let recovered = engine(replica(&metadata).await)
        .create_protected("test", &create)
        .await
        .unwrap();
    assert!(recovered.replayed);
    db.commit("test", &request(0)).await.unwrap();
    db.change_policy(
        "test",
        &PolicyRequest {
            request_id: "rotate".into(),
            expected_policy_revision: 1,
            change: PolicyAction::Rotate,
        },
    )
    .await
    .unwrap();
    db.gc("test").await.unwrap();
    db.maintain_storage().await.unwrap();
    let replay = engine(replica(&metadata).await)
        .create_protected("test", &create)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(replay.info).unwrap(),
        serde_json::to_value(recovered.info).unwrap()
    );
    assert_eq!(db.info("test").await.unwrap().revision, 1);
}
async fn replica(metadata: &Arc<Metadata>) -> Arc<Published> {
    Arc::new(
        Published::open(metadata.files.clone(), metadata.clone(), "test")
            .await
            .unwrap(),
    )
}
fn expected_race(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("outcome is uncertain")
        || message.starts_with("directory snapshot was collected")
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
    assert!(task.await.unwrap().is_err());
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
#[ignore = "capacity qualification; CI runs explicitly in release mode"]
async fn ten_thousand_databases_split_page_recover_and_collect() {
    for limits in [
        None,
        Some(durable_db::upload_budget::UploadLimits {
            bytes: 1 << 30,
            attempts: 100_000,
        }),
    ] {
        qualify_large_directory(limits, 10_000, false).await;
    }
}
#[tokio::test]
#[ignore = "extended capacity qualification; run explicitly in release mode"]
async fn hundred_thousand_databases_with_incremental_collection() {
    qualify_large_directory(
        Some(durable_db::upload_budget::UploadLimits {
            bytes: 16 << 30,
            attempts: 2_000_000,
        }),
        100_000,
        true,
    )
    .await;
}
async fn sweep_directory(store: &Published, metadata: &Metadata) {
    for pass in 0..10_000 {
        store.collect_manifests().await.unwrap();
        if metadata.read().await.unwrap().info["durable-db-index-gc"].is_empty() {
            return;
        }
        assert!(pass < 9_999, "directory collector did not complete a sweep");
    }
}
async fn qualify_large_directory(
    limits: Option<durable_db::upload_budget::UploadLimits>,
    count: usize,
    incremental_collection: bool,
) {
    let started = std::time::Instant::now();
    let (store, metadata) = setup_with_budget(limits).await;
    // Permute insertion order to exercise both sides of splits and first-key changes.
    let mut expected = Vec::new();
    for n in 0..count {
        let name = format!("site-{:05}", (n * 7919) % count);
        let key = format!("heads/{name}.json");
        assert!(
            store
                .cas(&key, None, Bytes::from(name.clone()))
                .await
                .unwrap()
        );
        expected.push(key);
        // Keep orphan accumulation bounded in the in-memory provider fixture.
        // This is a sequential capacity campaign; concurrent collection has
        // independent race tests and is not implied by this sweep.
        if incremental_collection && (n + 1) % 1000 == 0 {
            sweep_directory(&store, &metadata).await;
        }
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
    let root = if limits.is_some() {
        &root["directory"]
    } else {
        &root
    };
    assert_eq!(root["count"], expected.len());
    let height = root["height"].as_u64().unwrap() as usize;
    // With fanout 64, a three-level tree can still hold 100,005 entries.
    // Require a split root, without assuming a particular insertion occupancy.
    assert!(height >= 3);
    assert!(serde_json::to_vec(&state.info).unwrap().len() < 1500);
    metadata.gets.store(0, Ordering::SeqCst);
    store
        .get("heads/site-05000.json", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.gets.load(Ordering::SeqCst), height + 1);
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
        metadata.gets.load(Ordering::SeqCst) <= 2 * height + 3,
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
    sweep_directory(&store, &metadata).await;
    let mut index_count = 0;
    let mut after = None;
    loop {
        let page = metadata
            .files
            .list("indexes/", after.as_deref(), 1000)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        index_count += page.len();
        after = page.last().cloned();
    }
    assert!(
        index_count < count / 25,
        "only the live B+tree should remain"
    );
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
    eprintln!(
        "directory qualification: databases={} height={height} index_pages={index_count} elapsed_seconds={:.3}",
        expected.len(),
        started.elapsed().as_secs_f64()
    );
}
#[tokio::test]
async fn multiple_stateless_replicas_survive_both_collectors_and_reopen() {
    let (b2, metadata) = setup().await;
    engine(b2.clone())
        .create("test", Limits::default())
        .await
        .unwrap();
    let mut tasks = vec![];
    for worker in 0..3 {
        let b2 = replica(&metadata).await;
        tasks.push(tokio::spawn(async move {
            let e = engine(b2);
            for op in 0..12 {
                let mut done = false;
                for _ in 0..100 {
                    let r = e.info("test").await.unwrap().revision;
                    let mut req = request(r);
                    req.request_id = format!("worker-{worker}-op-{op}");
                    let mut result = e.commit("test", &req).await;
                    for _ in 0..20 {
                        if !matches!(&result, Err(Error::Storage(error)) if expected_race(error)) {
                            break;
                        }
                        // Preserve the full original request until receipt recovery
                        // or a fresh manifest establishes a definite conflict.
                        tokio::task::yield_now().await;
                        result = e.commit("test", &req).await;
                    }
                    match result {
                        Ok(_) => {
                            done = true;
                            break;
                        }
                        Err(Error::Conflict) => {}
                        Err(e) => panic!("{e}"),
                    }
                }
                assert!(done, "writer exhausted bounded recovery attempts");
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
        let store = replica(&metadata).await;
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
                        Err(error) if matches!(error, Error::Conflict)
                            || matches!(&error, Error::Storage(error) if expected_race(error)) => {
                            // This fixture exclusively owns this unique name.
                            // A submitted create can be helped after an unknown
                            // response, so observe it before trying again.
                            match e.info(&db).await {
                                Ok(info) => {
                                    assert_eq!(info.revision, 0);
                                    created = true;
                                    break;
                                }
                                Err(Error::NotFound) => {}
                                Err(error) => panic!("{error}"),
                            }
                            tokio::task::yield_now().await;
                        }
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
    let before = metadata.read().await.unwrap();
    let empty_root = before.info["durable-db-root"].clone();
    assert_eq!(e.maintain_database("idle").await.unwrap().deleted, 0);
    assert_eq!(metadata.read().await.unwrap().revision, before.revision + 1);
    assert_eq!(
        metadata.read().await.unwrap().info["durable-db-root"],
        empty_root
    );
    e.commit("idle", &request(0)).await.unwrap();
    let before = metadata.read().await.unwrap();
    let live_root = before.info["durable-db-root"].clone();
    let result = e.maintain_database("idle").await.unwrap();
    assert_eq!(metadata.read().await.unwrap().revision, before.revision + 1);
    assert_eq!(result.examined, 1);
    assert_eq!(result.deleted, 0);
    assert_eq!(
        metadata.read().await.unwrap().info["durable-db-root"],
        live_root
    );
}

#[tokio::test]
async fn publication_retries_reuse_discovery_without_replaying_uploads() {
    for lose_reply in [false, true] {
        let (store, metadata) = setup().await;
        // An abandoned proposal remains eligible for another caller's group.
        metadata.lost_proposal_ack.store(true, Ordering::SeqCst);
        assert!(
            store
                .cas("heads/b.json", None, Bytes::from_static(b"b"))
                .await
                .is_err()
        );
        metadata.pause.store(true, Ordering::SeqCst);
        let writer = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .cas("heads/a.json", None, Bytes::from_static(b"a"))
                    .await
            }
        });
        wait_paused(&metadata).await;
        assert_eq!(metadata.proposal_gets.load(Ordering::SeqCst), 1);
        assert_eq!(metadata.proposal_lists.load(Ordering::SeqCst), 1);
        // Defeat two comparisons without changing the target or data fence.
        metadata.read_fresh().await.unwrap();
        metadata.pause.store(true, Ordering::SeqCst);
        metadata.resume.notify_one();
        wait_paused(&metadata).await;
        assert_eq!(
            metadata.proposal_gets.load(Ordering::SeqCst),
            1,
            "a definite comparison conflict must not download the same batch again"
        );
        assert_eq!(metadata.proposal_lists.load(Ordering::SeqCst), 1);
        metadata.read_fresh().await.unwrap();
        metadata.lost_ack.store(lose_reply, Ordering::SeqCst);
        metadata.resume.notify_one();
        let result = writer.await.unwrap();
        if lose_reply {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap());
        }
        assert_eq!(metadata.proposal_gets.load(Ordering::SeqCst), 1);
        assert_eq!(metadata.proposal_lists.load(Ordering::SeqCst), 1);
        for (name, value) in [("a", b"a"), ("b", b"b")] {
            let object = store
                .get(&format!("heads/{name}.json"), 16)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(object.bytes, value.as_slice());
        }
        assert_eq!(
            metadata
                .files
                .list("manifests/", None, 16)
                .await
                .unwrap()
                .len(),
            2,
            "each proposal keeps its original immutable manifest across retries"
        );
    }
}

#[tokio::test]
async fn publication_retries_read_barriers_and_rebases_unrelated_database_writes() {
    for change_root in [false, true] {
        let (store, metadata) = setup().await;
        let other = replica(&metadata).await;
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
                other
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
    let other = replica(&metadata).await;
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
        other
            .cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"winner")
            )
            .await
            .unwrap()
    );
    metadata.resume.notify_one();
    assert!(writer.await.unwrap().is_err());
    assert_eq!(
        store.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"winner"[..]
    );
}

#[tokio::test]
async fn abandoned_origin_is_published_by_another_replica_in_one_group() {
    let (store, metadata) = setup().await;
    let other = replica(&metadata).await;
    store
        .cas("heads/a.json", None, Bytes::from_static(b"one"))
        .await
        .unwrap();
    let old = store.get("heads/a.json", 100).await.unwrap().unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let writer = tokio::spawn(async move {
        store
            .cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"two"),
            )
            .await
    });
    wait_paused(&metadata).await;
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    // The originating task and Published instance are gone. Helping is durable.
    let before = metadata.read().await.unwrap();
    assert!(
        other
            .cas("heads/b.json", None, Bytes::from_static(b"other"))
            .await
            .unwrap()
    );
    let after = metadata.read().await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    let reopened = replica(&metadata).await;
    assert_eq!(
        reopened
            .get("heads/a.json", 100)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"two"[..]
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
    other.collect_manifests().await.unwrap();
    assert!(
        metadata
            .files
            .list("proposals/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        metadata
            .files
            .list("manifests/", None, 128)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn lost_group_acknowledgement_recovers_every_original_receipt() {
    let (store, metadata) = setup().await;
    let other = replica(&metadata).await;
    let e = Arc::new(engine(store.clone()));
    let helper = engine(other);
    e.create("a", Limits::default()).await.unwrap();
    e.create("b", Limits::default()).await.unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let writer = tokio::spawn({
        let e = e.clone();
        async move { e.commit("a", &request(0)).await }
    });
    wait_paused(&metadata).await;
    metadata.lost_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        helper.commit("b", &request(0)).await,
        Err(Error::Storage(_))
    ));
    // The whole group committed despite the helper's lost response. The first
    // writer's stale CAS loses, then its own unique pointer proves success.
    metadata.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap().revision, 1);
    e.gc("a").await.unwrap();
    e.gc("b").await.unwrap();
    store.collect_manifests().await.unwrap();
    let reopened = engine(replica(&metadata).await);
    for db in ["a", "b"] {
        assert!(reopened.commit(db, &request(0)).await.unwrap().replayed);
        assert_eq!(reopened.info(db).await.unwrap().revision, 1);
    }
}

#[tokio::test]
async fn a_lost_proposal_upload_response_does_not_abort_its_future_publication() {
    let (store, metadata) = setup().await;
    let other = replica(&metadata).await;
    let e = engine(store);
    let helper = engine(other);
    e.create("a", Limits::default()).await.unwrap();
    e.create("b", Limits::default()).await.unwrap();
    metadata.lost_proposal_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        e.commit("a", &request(0)).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(e.info("a").await.unwrap().revision, 0);
    helper.commit("b", &request(0)).await.unwrap();
    assert_eq!(e.info("a").await.unwrap().revision, 1);
    assert!(e.commit("a", &request(0)).await.unwrap().replayed);
}

#[tokio::test]
async fn collection_invalidates_an_entire_loaded_selection() {
    let (store, metadata) = setup().await;
    let other = replica(&metadata).await;
    store
        .cas("heads/a.json", None, Bytes::from_static(b"a0"))
        .await
        .unwrap();
    store
        .cas("heads/b.json", None, Bytes::from_static(b"b0"))
        .await
        .unwrap();
    let a = store.get("heads/a.json", 100).await.unwrap().unwrap();
    let b = store.get("heads/b.json", 100).await.unwrap().unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let origin = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .cas("heads/a.json", Some(&a.version), Bytes::from_static(b"a1"))
                .await
        }
    });
    wait_paused(&metadata).await;
    origin.abort();
    assert!(origin.await.unwrap_err().is_cancelled());
    metadata.pause.store(true, Ordering::SeqCst);
    let helper = tokio::spawn(async move {
        other
            .cas("heads/b.json", Some(&b.version), Bytes::from_static(b"b1"))
            .await
    });
    wait_paused(&metadata).await;
    assert_eq!(store.collect_manifests().await.unwrap(), 2); // Both staged manifests, neither live head.
    metadata.resume.notify_one();
    assert!(helper.await.unwrap().is_err());
    assert_eq!(
        store.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"a0"[..]
    );
    assert_eq!(
        store.get("heads/b.json", 100).await.unwrap().unwrap().bytes,
        b"b0"[..]
    );
    assert!(
        metadata
            .files
            .list("proposals/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn malformed_proposals_never_authorize_a_directory_change() {
    let (store, metadata) = setup().await;
    for body in [b"not json".as_slice(), br#"{"database":"a","expected":null,"next":{"key":"manifests/foreign.00000000-0000-0000-0000-000000000001","sha256":"00"},"fence":null}"#] {
        metadata.files.put(&format!("proposals/a.{}", uuid::Uuid::new_v4()), Bytes::copy_from_slice(body)).await.unwrap();
    }
    assert!(
        store
            .cas("heads/b.json", None, Bytes::from_static(b"b"))
            .await
            .unwrap()
    );
    assert!(store.get("heads/a.json", 100).await.unwrap().is_none());
    store.collect_manifests().await.unwrap();
    assert!(
        metadata
            .files
            .list("proposals/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn selection_is_bounded_and_remaining_proposals_can_make_progress() {
    let (store, metadata) = setup().await;
    // Abandon more proposals than one group can select. Each replica vanishes
    // before its CAS, leaving no volatile coordinator for the next helper.
    for n in 0..65 {
        let origin = replica(&metadata).await;
        metadata.pause.store(true, Ordering::SeqCst);
        let task = tokio::spawn(async move {
            origin
                .cas(
                    &format!("heads/db-{n:02}.json"),
                    None,
                    Bytes::from_static(b"pending"),
                )
                .await
        });
        wait_paused(&metadata).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    store
        .cas("heads/helper.json", None, Bytes::from_static(b"first"))
        .await
        .unwrap();
    let state = metadata.read().await.unwrap();
    let root: serde_json::Value = serde_json::from_str(&state.info["durable-db-root"]).unwrap();
    assert_eq!(root["count"], 64);
    store
        .cas(
            "heads/helper-next.json",
            None,
            Bytes::from_static(b"second"),
        )
        .await
        .unwrap();
    // A full listing can contain one more candidate than the remaining group
    // slots. Wrap the cursor to revisit the candidate deferred on that page.
    store
        .cas("heads/helper-last.json", None, Bytes::from_static(b"third"))
        .await
        .unwrap();
    // The durable cursor progresses even if completed cleanup is delayed.
    for n in 0..65 {
        assert_eq!(
            store
                .get(&format!("heads/db-{n:02}.json"), 100)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"pending"[..]
        );
    }
    store.collect_manifests().await.unwrap();
    assert!(
        metadata
            .files
            .list("proposals/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
}
