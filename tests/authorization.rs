use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    auth::{Access, Auth},
    cache::Cache,
    engine::{CommitRequest, Engine, Limits, PolicyAction, PolicyRequest, ReadRequest, Write},
    storage::{Object, Remote, Storage, Version},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct Faults {
    inner: Arc<dyn Storage>,
    pause_cas: AtomicBool,
    missing_block: AtomicBool,
    lost_ack: AtomicBool,
    reached: Notify,
    resume: Notify,
    uploads: AtomicUsize,
    block_reads: AtomicUsize,
}
impl Faults {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(
                Remote::new(
                    Arc::new(object_store::memory::InMemory::new()),
                    "authorization",
                )
                .unwrap(),
            ),
            pause_cas: AtomicBool::new(false),
            missing_block: AtomicBool::new(false),
            lost_ack: AtomicBool::new(false),
            reached: Notify::new(),
            resume: Notify::new(),
            uploads: AtomicUsize::new(0),
            block_reads: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl Storage for Faults {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        if key.starts_with("blocks/") {
            self.block_reads.fetch_add(1, Ordering::SeqCst);
            if self.missing_block.swap(false, Ordering::SeqCst) {
                self.reached.notify_one();
                self.resume.notified().await;
                return Ok(None);
            }
        }
        self.inner.get(key, max).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        if self.pause_cas.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.cas(key, old, bytes).await?;
        if self.lost_ack.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost publication response");
        }
        Ok(result)
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.uploads.fetch_add(1, Ordering::SeqCst);
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
fn engine(storage: Arc<dyn Storage>) -> Arc<Engine> {
    Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())))
}
fn change(revision: u64, action: PolicyAction) -> PolicyRequest {
    PolicyRequest {
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_policy_revision: revision,
        change: action,
    }
}
fn write() -> CommitRequest {
    CommitRequest {
        expected_revision: 0,
        request_id: "write".into(),
        checks: vec![],
        writes: vec![Write {
            key: "value".into(),
            value: Some("aGk=".into()),
        }],
    }
}
async fn credential(engine: &Engine, auth: &Auth) -> (Access, String) {
    let policy = engine.info("test").await.unwrap().policy.unwrap();
    let (token, id) = auth
        .issue_for_policy(
            "test".into(),
            true,
            3600,
            policy.incarnation,
            policy.generation,
        )
        .unwrap();
    (auth.access(&token, "test", true).unwrap(), id)
}

#[tokio::test]
async fn policy_is_durable_scoped_bounded_and_independent_of_data_revisions() {
    let storage = Faults::new();
    let db = engine(storage.clone());
    let auth = Auth::new(&"21".repeat(32)).unwrap();
    db.create("test", Limits::default()).await.unwrap();
    let legacy = auth
        .access(
            &auth.issue("test".into(), true, 3600).unwrap(),
            "test",
            true,
        )
        .unwrap();
    db.change_policy("test", &change(0, PolicyAction::Enable))
        .await
        .unwrap();
    assert!(matches!(
        db.info_with_access("test", &legacy).await,
        Err(Error::Unauthorized)
    ));
    let (access, id) = credential(&db, &auth).await;
    db.commit_with_access("test", &write(), &access)
        .await
        .unwrap();
    db.change_policy("test", &change(1, PolicyAction::Revoke { token_id: id }))
        .await
        .unwrap();
    let uploads = storage.uploads.load(Ordering::SeqCst);
    assert!(
        matches!(
            db.commit_with_access("test", &write(), &access).await,
            Err(Error::Unauthorized)
        ),
        "even an old receipt requires current authorization"
    );
    assert_eq!(storage.uploads.load(Ordering::SeqCst), uploads);
    assert!(
        matches!(
            engine(storage.clone())
                .read_with_access("test", &ReadRequest::default(), &access)
                .await,
            Err(Error::Unauthorized)
        ),
        "a cache-free replica enforces revocation"
    );
    let (current, _) = credential(&db, &auth).await;
    db.info_with_access("test", &current).await.unwrap();
    db.create("other", Limits::default()).await.unwrap();
    assert!(matches!(
        db.info_with_access("other", &current).await,
        Err(Error::Unauthorized)
    ));
    for revision in 2..129 {
        db.change_policy(
            "test",
            &change(
                revision,
                PolicyAction::Revoke {
                    token_id: uuid::Uuid::new_v4().to_string(),
                },
            ),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        db.info("test")
            .await
            .unwrap()
            .policy
            .unwrap()
            .revoked_tokens,
        128
    );
    assert!(matches!(
        db.change_policy(
            "test",
            &change(
                129,
                PolicyAction::Revoke {
                    token_id: uuid::Uuid::new_v4().to_string()
                }
            )
        )
        .await,
        Err(Error::Limit(_))
    ));
    let rotated = db
        .change_policy("test", &change(129, PolicyAction::Rotate))
        .await
        .unwrap()
        .policy;
    assert_eq!(rotated.generation, 1);
    assert_eq!(rotated.revoked_tokens, 0);
    assert!(matches!(
        db.info_with_access("test", &current).await,
        Err(Error::Unauthorized)
    ));
    let (current, _) = credential(&db, &auth).await;
    db.info_with_access("test", &current).await.unwrap();
    let again = db
        .change_policy("test", &change(130, PolicyAction::Enable))
        .await
        .unwrap()
        .policy;
    assert_eq!(again.incarnation, rotated.incarnation);
    assert_eq!(again.generation, rotated.generation);
    assert_eq!(db.info("test").await.unwrap().revision, 1);
    db.gc("test").await.unwrap();
    assert_eq!(db.info("test").await.unwrap().policy.unwrap(), again);
}

#[tokio::test]
async fn policy_receipts_recover_unknown_outcomes_and_eviction_cannot_reapply() {
    let storage = Faults::new();
    let db = engine(storage.clone());
    db.create("test", Limits::default()).await.unwrap();
    let enable = change(0, PolicyAction::Enable);
    storage.lost_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        db.change_policy("test", &enable).await,
        Err(Error::Storage(_))
    ));
    let recovered = engine(storage.clone())
        .change_policy("test", &enable)
        .await
        .unwrap();
    assert!(recovered.replayed);
    let rotate = change(1, PolicyAction::Rotate);
    storage.lost_ack.store(true, Ordering::SeqCst);
    assert!(db.change_policy("test", &rotate).await.is_err());
    let result = db.change_policy("test", &rotate).await.unwrap();
    assert!(result.replayed);
    assert_eq!(result.policy.generation, 1);
    let old = db.change_policy("test", &enable).await.unwrap();
    assert_eq!(
        old.policy, recovered.policy,
        "replays return the original result, not current state"
    );
    let mut altered = rotate.clone();
    altered.change = PolicyAction::Enable;
    assert!(matches!(
        db.change_policy("test", &altered).await,
        Err(Error::Invalid(_))
    ));
    for revision in 2..35 {
        db.change_policy("test", &change(revision, PolicyAction::Rotate))
            .await
            .unwrap();
    }
    assert!(matches!(
        db.change_policy("test", &rotate).await,
        Err(Error::Conflict)
    ));
    assert_eq!(
        db.info("test").await.unwrap().policy.unwrap().generation,
        34
    );
    assert_eq!(db.info("test").await.unwrap().revision, 0);
}

#[tokio::test]
async fn activation_and_revocation_fence_writes_prepared_under_the_old_policy() {
    for enable in [true, false] {
        let storage = Faults::new();
        let db = engine(storage.clone());
        let auth = Auth::new(&"22".repeat(32)).unwrap();
        db.create("test", Limits::default()).await.unwrap();
        let (access, request) = if enable {
            (
                auth.access(
                    &auth.issue("test".into(), true, 3600).unwrap(),
                    "test",
                    true,
                )
                .unwrap(),
                change(0, PolicyAction::Enable),
            )
        } else {
            db.change_policy("test", &change(0, PolicyAction::Enable))
                .await
                .unwrap();
            let (access, id) = credential(&db, &auth).await;
            (access, change(1, PolicyAction::Revoke { token_id: id }))
        };
        storage.pause_cas.store(true, Ordering::SeqCst);
        let writer = db.clone();
        let task =
            tokio::spawn(async move { writer.commit_with_access("test", &write(), &access).await });
        storage.reached.notified().await;
        engine(storage.clone())
            .change_policy("test", &request)
            .await
            .unwrap();
        storage.resume.notify_one();
        assert!(matches!(task.await.unwrap(), Err(Error::Conflict)));
        assert_eq!(db.info("test").await.unwrap().revision, 0);
        db.gc("test").await.unwrap();
        assert_eq!(db.info("test").await.unwrap().keys, 0);
    }
}

#[tokio::test]
async fn a_collected_read_restart_must_authorize_the_replacement_snapshot() {
    let storage = Faults::new();
    let db = engine(storage.clone());
    db.create("test", Limits::default()).await.unwrap();
    db.change_policy("test", &change(0, PolicyAction::Enable))
        .await
        .unwrap();
    let auth = Auth::new(&"23".repeat(32)).unwrap();
    let (access, id) = credential(&db, &auth).await;
    db.commit_with_access("test", &write(), &access)
        .await
        .unwrap();
    storage.missing_block.store(true, Ordering::SeqCst);
    let reader = db.clone();
    let task = tokio::spawn(async move {
        reader
            .read_with_access(
                "test",
                &ReadRequest {
                    keys: vec!["value".into()],
                    ..Default::default()
                },
                &access,
            )
            .await
    });
    storage.reached.notified().await;
    db.change_policy("test", &change(1, PolicyAction::Revoke { token_id: id }))
        .await
        .unwrap();
    storage.resume.notify_one();
    assert!(matches!(task.await.unwrap(), Err(Error::Unauthorized)));
    assert_eq!(
        storage.block_reads.load(Ordering::SeqCst),
        1,
        "the new snapshot is rejected before reading any blocks"
    );
}

#[tokio::test]
async fn malformed_policy_formats_fail_closed_before_data_io() {
    for variant in 0..5 {
        let storage = Faults::new();
        let db = engine(storage.clone());
        db.create("test", Limits::default()).await.unwrap();
        db.change_policy("test", &change(0, PolicyAction::Enable))
            .await
            .unwrap();
        let head = storage
            .get("heads/test.json", 512 * 1024)
            .await
            .unwrap()
            .unwrap();
        let mut manifest: serde_json::Value = serde_json::from_slice(&head.bytes).unwrap();
        match variant {
            0 => {
                manifest["format"] = 1.into();
            }
            1 => {
                manifest.as_object_mut().unwrap().remove("policy");
            }
            2 => {
                manifest["policy"]["incarnation"] = "bad".into();
            }
            3 => {
                manifest["policy"]["revision"] = 0.into();
            }
            _ => {
                manifest["policy"]["generation"] = 100.into();
            }
        }
        assert!(
            storage
                .inner
                .cas(
                    "heads/test.json",
                    Some(&head.version),
                    serde_json::to_vec(&manifest).unwrap().into()
                )
                .await
                .unwrap()
        );
        assert!(db.read("test", &ReadRequest::default()).await.is_err());
        assert!(db.commit("test", &write()).await.is_err());
        assert!(db.gc("test").await.is_err());
        assert_eq!(storage.uploads.load(Ordering::SeqCst), 0);
        assert_eq!(storage.block_reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_http_policy_activation_token_issuance_revocation_and_rotation() {
    use durable_db::api::{App, router};
    use serde_json::{Value, json};
    let db = engine(Faults::new());
    let auth = Auth::new(&"25".repeat(32)).unwrap();
    let admin = auth.admin_token();
    let metrics = auth.metrics_token(3600).unwrap();
    let legacy = auth.issue("test".into(), true, 3600).unwrap();
    db.create("test", Limits::default()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router(Arc::new(App::new(db, auth, 16))))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let endpoint = format!("{base}/v1/databases/test/access");
    let enable =
        json!({"request_id":"enable","expected_policy_revision":0,"change":{"action":"enable"}});
    for token in [&legacy, &metrics] {
        assert_eq!(
            client
                .post(&endpoint)
                .bearer_auth(token)
                .json(&enable)
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    let response = client
        .post(&endpoint)
        .bearer_auth(&admin)
        .json(&enable)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let enabled: Value = response.json().await.unwrap();
    for (path, method) in [("", "GET"), ("/read", "POST"), ("/commit", "POST")] {
        let body = if path == "/commit" {
            serde_json::to_value(write()).unwrap()
        } else {
            json!({})
        };
        let response = client
            .request(
                method.parse().unwrap(),
                format!("{base}/v1/databases/test{path}"),
            )
            .bearer_auth(&legacy)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
    }
    let minted: Value = client
        .post(format!("{base}/v1/tokens"))
        .bearer_auth(&admin)
        .json(&json!({"database":"test","write":true,"ttl_seconds":3600}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = minted["token"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(minted["token_id"].as_str().unwrap()).is_ok());
    let read_url = format!("{base}/v1/databases/test/read");
    assert_eq!(
        client
            .post(&read_url)
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let revoke = json!({"request_id":"revoke","expected_policy_revision":1,"change":{"action":"revoke","token_id":minted["token_id"]}});
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&admin)
            .json(&revoke)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(&read_url)
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let replay: Value = client
        .post(&endpoint)
        .bearer_auth(&admin)
        .json(&enable)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay["policy"], enabled["policy"]);
    assert_eq!(replay["replayed"], true);
    let rotate =
        json!({"request_id":"rotate","expected_policy_revision":2,"change":{"action":"rotate"}});
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&admin)
            .json(&rotate)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let current: Value = client
        .get(format!("{base}/v1/databases/test"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(current["revision"], 0);
    assert_eq!(current["policy"]["generation"], 1);
    server.abort();
    let _ = server.await;
}
