use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::{Engine, Limits},
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
    paused: Notify,
    resume: Notify,
}
#[async_trait]
impl Storage for Files {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
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
    state: Mutex<Bucket>,
    files: Arc<Files>,
    lose_allocation: AtomicBool,
    lose_refill: AtomicBool,
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
            paused: Notify::new(),
            resume: Notify::new(),
        }),
        lose_allocation: AtomicBool::new(false),
        lose_refill: AtomicBool::new(false),
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
    assert_eq!(root["upload_format"], 4);
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
