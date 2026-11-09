use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::{CommitRequest, Engine, Limits, ReadRequest, Write},
    publication::{Authority, Bucket, Published},
    storage::{Object, Remote, Storage, Version},
    upload_budget::{UploadLimits, UploadRequest},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, Notify};

const FIELD: &str = "durable-db-upload-budget";
struct Files {
    inner: Remote,
    calls: AtomicUsize,
    bytes: AtomicUsize,
    pause: AtomicBool,
    pause_after: AtomicBool,
    hide_manifests: AtomicBool,
    hide_indexes: AtomicBool,
    paused: Notify,
    resume: Notify,
}
#[async_trait]
impl Storage for Files {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        if (key.starts_with("manifests/") && self.hide_manifests.load(Ordering::SeqCst))
            || (key.starts_with("indexes/") && self.hide_indexes.load(Ordering::SeqCst))
        {
            return Ok(None);
        }
        self.inner.get(key, max).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        unreachable!()
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.bytes.fetch_add(bytes.len(), Ordering::SeqCst);
        self.inner.put(key, bytes).await?;
        if self.pause_after.swap(false, Ordering::SeqCst) {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        Ok(())
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
    state: Mutex<Bucket>,
    files: Arc<Files>,
    lose_allocation: AtomicBool,
    lose_refill: AtomicBool,
    pause_data_fence: AtomicBool,
    data_fence_paused: Notify,
    data_fence_resume: Notify,
    lose_data_fence: AtomicBool,
}
fn counter(info: &BTreeMap<String, String>, field: &str) -> serde_json::Value {
    info.get(FIELD)
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap()["status"][field].clone())
        .unwrap_or_default()
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
        let data_fence = info.contains_key("durable-db-data-gc")
            && info.get("durable-db-fence") != old.info.get("durable-db-fence");
        if data_fence && self.pause_data_fence.swap(false, Ordering::SeqCst) {
            self.data_fence_paused.notify_one();
            self.data_fence_resume.notified().await;
        }
        let mut state = self.state.lock().await;
        if state.revision != old.revision {
            return Ok(None);
        }
        let allocated = counter(&info, "allocated") != counter(&old.info, "allocated");
        let refilled = counter(&info, "configuration_revision")
            != counter(&old.info, "configuration_revision");
        state.revision += 1;
        state.info = info;
        if (allocated && self.lose_allocation.swap(false, Ordering::SeqCst))
            || (refilled && self.lose_refill.swap(false, Ordering::SeqCst))
            || (data_fence && self.lose_data_fence.swap(false, Ordering::SeqCst))
        {
            anyhow::bail!("injected lost authority acknowledgement");
        }
        Ok(Some(state.clone()))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.files.delete(key).await
    }
}
fn fixture() -> Arc<Metadata> {
    Arc::new(Metadata {
        state: Mutex::new(Bucket {
            revision: 0,
            version: None,
            info: BTreeMap::from([("unrelated".into(), "retained".into())]),
        }),
        files: Arc::new(Files {
            inner: Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap(),
            calls: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            pause: AtomicBool::new(false),
            pause_after: AtomicBool::new(false),
            hide_manifests: AtomicBool::new(false),
            hide_indexes: AtomicBool::new(false),
            paused: Notify::new(),
            resume: Notify::new(),
        }),
        lose_allocation: AtomicBool::new(false),
        lose_refill: AtomicBool::new(false),
        pause_data_fence: AtomicBool::new(false),
        data_fence_paused: Notify::new(),
        data_fence_resume: Notify::new(),
        lose_data_fence: AtomicBool::new(false),
    })
}
async fn open(metadata: &Arc<Metadata>, limits: Option<UploadLimits>) -> Arc<Published> {
    Arc::new(
        Published::open_with_budget(metadata.files.clone(), metadata.clone(), "test", limits)
            .await
            .unwrap(),
    )
}
fn engine(storage: Arc<Published>) -> Engine {
    Engine::new(storage, Arc::new(Cache::new(0, None).unwrap()))
}
fn write(revision: u64) -> CommitRequest {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    CommitRequest {
        expected_revision: revision,
        request_id: format!("write-{revision}"),
        checks: vec![],
        writes: vec![Write {
            key: "key".into(),
            value: Some(STANDARD.encode(format!("value-{revision}"))),
        }],
    }
}
async fn paused(notify: &Notify) {
    tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn existing_page_collection_never_reclaims_the_new_journal_namespace() {
    use durable_db::journal::{QueueLimits, Reader, Writer};
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1024 * 1024,
            attempts: 64,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    for revision in 0..3 {
        db.commit("sample", &write(revision)).await.unwrap();
    }
    // Candidate journal bytes use a separate namespace and are not referenced
    // by the existing page manifests. Its future admission/collector is separate.
    let mut writer = Writer::new(
        metadata.files.clone(),
        uuid::Uuid::new_v4(),
        QueueLimits::default(),
    )
    .unwrap();
    let upload = writer
        .prepare(&[Bytes::from_static(b"candidate journal")])
        .unwrap();
    writer.complete(upload.send().await).unwrap();
    drop(writer.pop_durable().unwrap().unwrap());
    let prefix = writer.durable_prefix();
    assert_eq!(storage.collect_data().await.unwrap().deleted, 2);
    storage.maintenance().await.unwrap();
    let mut replay = Reader::replay(metadata.files.clone(), prefix);
    assert_eq!(
        replay
            .read_next()
            .await
            .unwrap()
            .unwrap()
            .records()
            .collect::<Vec<_>>(),
        vec![b"candidate journal".as_slice()]
    );
    assert!(replay.read_next().await.unwrap().is_none());
    assert_eq!(replay.finished_prefix(), Some(prefix));
}

#[tokio::test]
async fn exhausted_global_collection_reclaims_pages_without_uploads_and_preserves_reads() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1024 * 1024,
            attempts: 15,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    for n in 0..3 {
        db.commit("sample", &write(n)).await.unwrap();
    }
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 15);
    drop(db);
    drop(storage);
    let storage = open(&metadata, None).await;
    let before = storage.upload_budget().await.unwrap().unwrap();
    assert!(storage.put("blocks/refused", Bytes::new()).await.is_err());
    let result = storage.collect_data().await.unwrap();
    assert_eq!((result.examined, result.deleted), (3, 2));
    storage.maintenance().await.unwrap();
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 15);
    assert_eq!(storage.upload_budget().await.unwrap().unwrap(), before);
    let db = engine(storage);
    let read = db
        .read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(read.revision, 3);
    assert_eq!(read.values[0], write(2).writes[0].value);
    assert_eq!(db.gc("sample").await.unwrap().deleted, 0);
}

#[tokio::test]
async fn writer_cannot_adopt_a_fence_after_its_uploaded_page_was_collected() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 24,
            attempts: 100,
        }),
    )
    .await;
    let db = Arc::new(engine(storage.clone()));
    db.create("sample", Limits::default()).await.unwrap();
    metadata.files.pause_after.store(true, Ordering::SeqCst);
    let writer = db.clone();
    let pending = tokio::spawn(async move { writer.commit("sample", &write(0)).await });
    paused(&metadata.files.paused).await;
    let collector = open(&metadata, None).await;
    assert_eq!(collector.collect_data().await.unwrap().deleted, 1);
    metadata.files.resume.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(Error::Conflict)));
    assert_eq!(db.info("sample").await.unwrap().revision, 0);
    db.commit("sample", &write(0)).await.unwrap();
    assert_eq!(
        db.read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .values[0],
        write(0).writes[0].value
    );
}

#[tokio::test]
async fn exhausted_submitted_proposal_cannot_resurrect_pages_after_global_collection() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1024 * 1024,
            attempts: 6,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    assert!(matches!(
        db.commit("sample", &write(0)).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(storage.collect_data().await.unwrap().deleted, 1);
    storage
        .replenish_upload_budget(&UploadRequest {
            request_id: "fund".into(),
            expected_configuration_revision: 0,
            limits: UploadLimits {
                bytes: 1 << 24,
                attempts: 100,
            },
        })
        .await
        .unwrap();
    db.create("helper", Limits::default()).await.unwrap();
    assert_eq!(db.info("sample").await.unwrap().revision, 0);
    db.commit("sample", &write(0)).await.unwrap();
    assert_eq!(db.info("sample").await.unwrap().revision, 1);
}

#[tokio::test]
async fn exhausted_sweeps_resume_the_global_cursor_after_each_process_disappears() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1024 * 1024,
            attempts: 260,
        }),
    )
    .await;
    engine(storage.clone())
        .create("sample", Limits::default())
        .await
        .unwrap();
    for n in 0..257 {
        let name = if n % 2 == 0 { "sample" } else { "uncreated" };
        storage
            .put(
                &format!("blocks/{name}.{}", uuid::Uuid::new_v4()),
                Bytes::new(),
            )
            .await
            .unwrap();
    }
    drop(storage);
    for expected in [128, 128, 1, 0] {
        let storage = open(&metadata, None).await;
        let result = storage.collect_data().await.unwrap();
        assert_eq!((result.examined, result.deleted), (expected, expected));
    }
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 260);
    assert!(
        metadata
            .files
            .list("blocks/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine(open(&metadata, None).await)
            .info("sample")
            .await
            .unwrap()
            .revision,
        0
    );
}

#[tokio::test]
async fn concurrent_collectors_and_lost_fence_responses_never_delete_without_protection() {
    let metadata = fixture();
    let a = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 24,
            attempts: 100,
        }),
    )
    .await;
    let db = engine(a.clone());
    db.create("sample", Limits::default()).await.unwrap();
    db.commit("sample", &write(0)).await.unwrap();
    db.commit("sample", &write(1)).await.unwrap();
    let b = open(&metadata, None).await;
    metadata.lose_data_fence.store(true, Ordering::SeqCst);
    assert!(a.collect_data().await.is_err());
    assert_eq!(
        metadata
            .files
            .list("blocks/", None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
    metadata.pause_data_fence.store(true, Ordering::SeqCst);
    let first = a.clone();
    let pending = tokio::spawn(async move { first.collect_data().await });
    paused(&metadata.data_fence_paused).await;
    assert_eq!(b.collect_data().await.unwrap().deleted, 1);
    metadata.data_fence_resume.notify_one();
    assert_eq!(pending.await.unwrap().unwrap().deleted, 0);
    assert_eq!(
        db.read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .values[0],
        write(1).writes[0].value
    );
}

#[tokio::test]
async fn missing_protected_metadata_is_not_an_empty_database() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 24,
            attempts: 100,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    db.commit("sample", &write(0)).await.unwrap();
    db.commit("sample", &write(1)).await.unwrap();
    for missing in [&metadata.files.hide_indexes, &metadata.files.hide_manifests] {
        missing.store(true, Ordering::SeqCst);
        assert!(storage.collect_data().await.is_err());
        assert_eq!(
            metadata
                .files
                .list("blocks/", None, 10)
                .await
                .unwrap()
                .len(),
            2
        );
        missing.store(false, Ordering::SeqCst);
    }
    assert_eq!(storage.collect_data().await.unwrap().deleted, 1);
    assert_eq!(db.info("sample").await.unwrap().revision, 2);
}

#[tokio::test]
async fn prototype_format4_remains_budgeted_without_enabling_the_new_collector() {
    let metadata = fixture();
    drop(
        open(
            &metadata,
            Some(UploadLimits {
                bytes: 1 << 24,
                attempts: 100,
            }),
        )
        .await,
    );
    // Historical empty format-4 fixture, before any data or in-flight request.
    let mut state = metadata.state.lock().await;
    state.info.insert("durable-db-format".into(), "4".into());
    state.info.insert(
        "durable-db-root".into(),
        r#"{"upload_format":4,"directory":null}"#.into(),
    );
    drop(state);
    let storage = open(&metadata, None).await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    db.commit("sample", &write(0)).await.unwrap();
    assert!(storage.collect_data().await.is_err());
    assert!(storage.upload_budget().await.unwrap().is_some());
    assert_eq!(db.info("sample").await.unwrap().revision, 1);
}

#[tokio::test]
async fn collector_preserves_pages_retained_from_an_earlier_manifest() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 26,
            attempts: 128,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    let a = STANDARD.encode(vec![1; 60_000]);
    let b = STANDARD.encode(vec![2; 60_000]);
    let replacement = STANDARD.encode(vec![3; 60_000]);
    db.commit(
        "sample",
        &CommitRequest {
            expected_revision: 0,
            request_id: "two-pages".into(),
            checks: vec![],
            writes: vec![
                Write {
                    key: "a".into(),
                    value: Some(a),
                },
                Write {
                    key: "b".into(),
                    value: Some(b.clone()),
                },
                Write {
                    key: "c".into(),
                    value: Some(b.clone()),
                },
            ],
        },
    )
    .await
    .unwrap();
    assert_eq!(db.info("sample").await.unwrap().pages, 2);
    db.commit(
        "sample",
        &CommitRequest {
            expected_revision: 1,
            request_id: "replace-a".into(),
            checks: vec![],
            writes: vec![Write {
                key: "a".into(),
                value: Some(replacement.clone()),
            }],
        },
    )
    .await
    .unwrap();
    let result = storage.collect_data().await.unwrap();
    assert_eq!((result.examined, result.deleted), (3, 1));
    let read = db
        .read(
            "sample",
            &ReadRequest {
                keys: vec!["a".into(), "b".into(), "c".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(read.values, [Some(replacement), Some(b.clone()), Some(b)]);
}

#[tokio::test]
async fn collector_validates_manifest_semantics_even_when_its_digest_matches() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 24,
            attempts: 128,
        }),
    )
    .await;
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    db.commit("sample", &write(0)).await.unwrap();
    let before = storage
        .get("heads/sample.json", 512 * 1024)
        .await
        .unwrap()
        .unwrap();
    let mut malformed: serde_json::Value = serde_json::from_slice(&before.bytes).unwrap();
    malformed["pages"][0]["object"] =
        serde_json::Value::String(format!("blocks/foreign.{}", uuid::Uuid::new_v4()));
    // Deliberately bypass Engine validation through the trusted internal
    // publication boundary, yielding a correct digest over invalid content.
    assert!(
        storage
            .cas(
                "heads/sample.json",
                Some(&before.version),
                serde_json::to_vec(&malformed).unwrap().into()
            )
            .await
            .unwrap()
    );
    assert!(storage.collect_data().await.is_err());
    assert_eq!(
        metadata
            .files
            .list("blocks/", None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let current = storage
        .get("heads/sample.json", 512 * 1024)
        .await
        .unwrap()
        .unwrap();
    assert!(
        storage
            .cas("heads/sample.json", Some(&current.version), before.bytes)
            .await
            .unwrap()
    );
    assert_eq!(storage.collect_data().await.unwrap().deleted, 0);
    assert_eq!(db.info("sample").await.unwrap().revision, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format5_writers_and_both_collectors_race_then_reopen_without_caches() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let metadata = fixture();
    let a = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 27,
            attempts: 8192,
        }),
    )
    .await;
    let b = open(&metadata, None).await;
    for name in ["alpha", "beta"] {
        engine(a.clone())
            .create(name, Limits::default())
            .await
            .unwrap();
    }
    let mut tasks = vec![];
    for (name, storage) in [("alpha", a.clone()), ("beta", b.clone())] {
        tasks.push(tokio::spawn(async move {
            let db = engine(storage);
            for revision in 0..30 {
                let request = CommitRequest {
                    expected_revision: revision,
                    request_id: format!("{name}-{revision}"),
                    checks: vec![],
                    writes: vec![Write {
                        key: format!("key-{revision:03}"),
                        value: Some(STANDARD.encode(format!("{name}-{revision}"))),
                    }],
                };
                let mut committed = false;
                for _ in 0..200 {
                    match db.commit(name, &request).await {
                        Ok(_) => {
                            committed = true;
                            break;
                        }
                        Err(Error::Conflict | Error::Storage(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await
                        }
                        Err(error) => panic!("unexpected writer error: {error}"),
                    }
                }
                assert!(committed, "writer did not resolve its original request");
            }
        }));
    }
    for storage in [a.clone(), b.clone()] {
        tasks.push(tokio::spawn(async move {
            for _ in 0..60 {
                for result in [
                    storage.collect_data().await.map(|_| ()),
                    storage.collect_manifests().await.map(|_| ()),
                ] {
                    if let Err(error) = result {
                        let message = error.to_string();
                        assert!(
                            message.contains("snapshot was collected")
                                || message.contains("protected collector manifest is unavailable"),
                            "unexpected collector error: {message}"
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    drop(a);
    drop(b);
    let fresh = engine(open(&metadata, None).await);
    for name in ["alpha", "beta"] {
        let read = fresh
            .read(
                name,
                &ReadRequest {
                    keys: (0..30).map(|n| format!("key-{n:03}")).collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(read.revision, 30);
        assert_eq!(
            read.values,
            (0..30)
                .map(|n| Some(STANDARD.encode(format!("{name}-{n}"))))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn concurrent_replicas_cannot_reallocate_grants_after_complete_process_loss() {
    let metadata = fixture();
    let limits = UploadLimits {
        bytes: 32 * 1024 * 1024,
        attempts: 64,
    };
    let first = open(&metadata, Some(limits)).await;
    let mut replicas = vec![first];
    for _ in 0..7 {
        replicas.push(open(&metadata, None).await);
    }
    let mut tasks = vec![];
    for (n, replica) in replicas.iter().enumerate() {
        let replica = replica.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..40 {
                let _ = replica
                    .put(&format!("blocks/{n}-{i}"), Bytes::from_static(b"x"))
                    .await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let status = replicas[0].upload_budget().await.unwrap().unwrap();
    assert_eq!(status.allocated.attempts, 64);
    let actual = metadata.files.calls.load(Ordering::SeqCst);
    assert!((1..=64).contains(&actual));
    assert_eq!(metadata.files.bytes.load(Ordering::SeqCst), actual);
    assert!(status.allocated.bytes <= limits.bytes);
    drop(replicas);
    // Startup flags cannot reset or raise an existing authoritative ceiling.
    let restarted = open(
        &metadata,
        Some(UploadLimits {
            bytes: 1 << 40,
            attempts: 1 << 40,
        }),
    )
    .await;
    assert_eq!(restarted.upload_budget().await.unwrap().unwrap(), status);
    assert!(restarted.put("blocks/new", Bytes::new()).await.is_err());
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), actual);
    assert_eq!(metadata.state.lock().await.info["unrelated"], "retained");
}

#[tokio::test]
async fn lost_allocation_ack_strands_credit_and_never_uploads() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 10,
            attempts: 3,
        }),
    )
    .await;
    metadata.lose_allocation.store(true, Ordering::SeqCst);
    assert!(
        storage
            .put("blocks/one", Bytes::from_static(b"x"))
            .await
            .is_err()
    );
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        storage
            .upload_budget()
            .await
            .unwrap()
            .unwrap()
            .allocated
            .attempts,
        3
    );
    assert!(
        storage
            .put("blocks/retry", Bytes::from_static(b"x"))
            .await
            .is_err()
    );
    drop(storage);
    assert!(
        open(&metadata, None)
            .await
            .put("blocks/restart", Bytes::new())
            .await
            .is_err()
    );
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn uploads_arriving_after_origin_loss_remain_charged_and_zero_bytes_cost_an_attempt() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 0,
            attempts: 1,
        }),
    )
    .await;
    metadata.files.pause.store(true, Ordering::SeqCst);
    let origin = storage.clone();
    let pending = tokio::spawn(async move { origin.put("blocks/delayed", Bytes::new()).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        metadata.files.paused.notified(),
    )
    .await
    .unwrap();
    drop(storage);
    let replacement = open(&metadata, None).await;
    assert!(replacement.put("blocks/new", Bytes::new()).await.is_err());
    drop(replacement);
    metadata.files.resume.notify_one();
    pending.await.unwrap().unwrap();
    let replacement = open(&metadata, None).await;
    assert!(
        replacement
            .get("blocks/delayed", 0)
            .await
            .unwrap()
            .is_some()
    );
    assert!(replacement.put("blocks/again", Bytes::new()).await.is_err());
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refill_preserves_allocation_and_has_bounded_idempotent_receipts() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 10,
            attempts: 1,
        }),
    )
    .await;
    storage
        .put("blocks/one", Bytes::from_static(b"x"))
        .await
        .unwrap();
    let original = storage.upload_budget().await.unwrap().unwrap();
    let request = UploadRequest {
        request_id: "refill".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 20,
            attempts: 2,
        },
    };
    metadata.lose_refill.store(true, Ordering::SeqCst);
    assert!(storage.replenish_upload_budget(&request).await.is_err());
    let result = storage.replenish_upload_budget(&request).await.unwrap();
    assert!(result.replayed);
    assert_eq!(result.status.allocated, original.allocated);
    let wrong = UploadRequest {
        limits: UploadLimits {
            bytes: 30,
            attempts: 3,
        },
        ..request.clone()
    };
    assert!(matches!(
        storage.replenish_upload_budget(&wrong).await,
        Err(Error::Invalid(_))
    ));
    for revision in 1..=10 {
        let next = UploadRequest {
            request_id: format!("next-{revision}"),
            expected_configuration_revision: revision,
            limits: UploadLimits {
                bytes: 20 + revision,
                attempts: 2 + revision,
            },
        };
        assert!(
            !storage
                .replenish_upload_budget(&next)
                .await
                .unwrap()
                .replayed
        );
    }
    assert!(matches!(
        storage.replenish_upload_budget(&request).await,
        Err(Error::Conflict)
    ));
    assert_eq!(
        storage.upload_budget().await.unwrap().unwrap().allocated,
        original.allocated
    );
    let state = metadata.state.lock().await;
    assert!(state.info[FIELD].len() < 2304);
    assert_eq!(state.info["unrelated"], "retained");
    drop(state);
    let invalid = UploadRequest {
        request_id: "decrease".into(),
        expected_configuration_revision: 11,
        limits: UploadLimits {
            bytes: 1,
            attempts: 1,
        },
    };
    assert!(matches!(
        storage.replenish_upload_budget(&invalid).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn exhaustion_preserves_reads_and_metadata_collection_and_refuses_format3_activation() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 100_000,
            attempts: 4,
        }),
    )
    .await;
    engine(storage.clone())
        .create("sample", Limits::default())
        .await
        .unwrap();
    let orphan = format!("manifests/sample.{}", uuid::Uuid::new_v4());
    storage
        .put(&orphan, Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    drop(storage);
    let storage = open(&metadata, None).await;
    assert!(storage.put("blocks/refused", Bytes::new()).await.is_err());
    assert_eq!(
        engine(storage.clone())
            .info("sample")
            .await
            .unwrap()
            .revision,
        0
    );
    assert_eq!(storage.collect_manifests().await.unwrap(), 1);
    assert!(metadata.files.get(&orphan, 10).await.unwrap().is_none());
    assert_eq!(engine(storage).info("sample").await.unwrap().revision, 0);
    // The empty and populated format-4 envelope cannot deserialize as the old
    // required root fields; test the actual previous release separately too.
    let root: serde_json::Value =
        serde_json::from_str(&metadata.state.lock().await.info["durable-db-root"]).unwrap();
    assert_eq!(root["upload_format"], 5);
    assert!(root.get("object").is_none());
    let legacy = fixture();
    drop(open(&legacy, None).await);
    assert!(
        Published::open_with_budget(
            legacy.files.clone(),
            legacy,
            "test",
            Some(UploadLimits {
                bytes: 10,
                attempts: 1
            })
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn exhaustion_after_proposal_submission_is_unknown_and_recovery_can_help_it() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 100_000,
            attempts: 2,
        }),
    )
    .await;
    assert!(matches!(
        engine(storage.clone())
            .create("sample", Limits::default())
            .await,
        Err(Error::Storage(_))
    ));
    assert_eq!(
        metadata
            .files
            .list("proposals/", None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    storage
        .replenish_upload_budget(&UploadRequest {
            request_id: "recover".into(),
            expected_configuration_revision: 0,
            limits: UploadLimits {
                bytes: 1_000_000,
                attempts: 64,
            },
        })
        .await
        .unwrap();
    drop(storage);
    let recovered = engine(open(&metadata, None).await);
    recovered.create("helper", Limits::default()).await.unwrap();
    assert_eq!(recovered.info("sample").await.unwrap().revision, 0);
}

#[tokio::test]
async fn payload_and_attempt_ceilings_are_independent() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 5,
            attempts: 100,
        }),
    )
    .await;
    assert!(
        storage
            .put("blocks/too-big", Bytes::from_static(b"123456"))
            .await
            .is_err()
    );
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 0);
    storage
        .put("blocks/three", Bytes::from_static(b"123"))
        .await
        .unwrap();
    storage
        .put("blocks/two", Bytes::from_static(b"12"))
        .await
        .unwrap();
    assert!(
        storage
            .put("blocks/one", Bytes::from_static(b"1"))
            .await
            .is_err()
    );
    for n in 0..110 {
        let _ = storage
            .put(&format!("blocks/empty-{n}"), Bytes::new())
            .await;
    }
    assert_eq!(metadata.files.bytes.load(Ordering::SeqCst), 5);
    assert!(metadata.files.calls.load(Ordering::SeqCst) <= 100);
    let status = storage.upload_budget().await.unwrap().unwrap();
    assert_eq!(
        status.allocated,
        UploadLimits {
            bytes: 5,
            attempts: 100
        }
    );
}

#[tokio::test]
async fn replenishing_one_dimension_retains_previously_confirmed_credit_in_the_other() {
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 100,
            attempts: 64,
        }),
    )
    .await;
    for n in 0..64 {
        storage
            .put(&format!("blocks/{n}"), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 64);
    assert_eq!(metadata.files.bytes.load(Ordering::SeqCst), 64);
    assert!(storage.put("blocks/refused", Bytes::new()).await.is_err());

    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 100,
            attempts: 64,
        }),
    )
    .await;
    storage
        .put("blocks/first", Bytes::from_static(b"x"))
        .await
        .unwrap();
    storage
        .replenish_upload_budget(&UploadRequest {
            request_id: "bytes".into(),
            expected_configuration_revision: 0,
            limits: UploadLimits {
                bytes: 300,
                attempts: 64,
            },
        })
        .await
        .unwrap();
    metadata.lose_allocation.store(true, Ordering::SeqCst);
    assert!(
        storage
            .put("blocks/uncertain-allocation", Bytes::from(vec![0; 200]))
            .await
            .is_err()
    );
    // The lost grant is unusable. Previously confirmed rights survive it.
    storage
        .put("blocks/previous-credit", Bytes::from_static(b"y"))
        .await
        .unwrap();
    assert_eq!(metadata.files.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        storage.upload_budget().await.unwrap().unwrap().allocated,
        UploadLimits {
            bytes: 300,
            attempts: 32
        }
    );
}

#[tokio::test]
async fn upload_budget_http_is_administrator_only_and_revision_checked() {
    use durable_db::{
        api::{App, router},
        auth::Auth,
    };
    let metadata = fixture();
    let storage = open(
        &metadata,
        Some(UploadLimits {
            bytes: 5,
            attempts: 1,
        }),
    )
    .await;
    let auth = Auth::new(&"42".repeat(32)).unwrap();
    let admin = auth.administrator_token(60).unwrap();
    let metrics = auth.metrics_token(60).unwrap();
    let tenant = auth.issue("tenant".into(), true, 60).unwrap();
    let app = router(Arc::new(App::new(
        Arc::new(engine(storage.clone())),
        auth,
        8,
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/v1/storage/upload-budget",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    for token in [&metrics, &tenant, "missing"] {
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(token)
                .json(&UploadRequest {
                    request_id: "no".into(),
                    expected_configuration_revision: 0,
                    limits: UploadLimits {
                        bytes: 10,
                        attempts: 2
                    }
                })
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    let status = client.get(&url).bearer_auth(&admin).send().await.unwrap();
    assert_eq!(status.status(), 200);
    let status: durable_db::upload_budget::UploadStatus = status.json().await.unwrap();
    assert_eq!(
        status.limits,
        UploadLimits {
            bytes: 5,
            attempts: 1
        }
    );
    let request = UploadRequest {
        request_id: "http-refill".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 10,
            attempts: 2,
        },
    };
    for replayed in [false, true] {
        let response = client
            .post(&url)
            .bearer_auth(&admin)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .json::<durable_db::upload_budget::UploadResult>()
                .await
                .unwrap()
                .replayed,
            replayed
        );
    }
    let stale = UploadRequest {
        request_id: "stale".into(),
        ..request
    };
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&admin)
            .json(&stale)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    server.abort();
    server.await.unwrap_err();
}
