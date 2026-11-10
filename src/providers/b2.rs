//! B2 native metadata and physical version deletion; S3 API carries immutable bytes.
use crate::metrics::{self, Operation, Outcome};
use crate::publication::{Authority, Bucket, Published};
use crate::storage::Remote;
use anyhow::{Context, bail};
use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
mod retry;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Authorization {
    account_id: String,
    authorization_token: String,
    api_info: ApiInfo,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInfo {
    storage_api: StorageApi,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageApi {
    api_url: String,
    s3_api_url: String,
    allowed: Allowed,
}
#[derive(Clone, Deserialize)]
struct Allowed {
    capabilities: Vec<String>,
}

pub struct Native {
    client: Client,
    key_id: String,
    application_key: String,
    auth: RwLock<Authorization>,
    bucket_id: String,
    prefix: String,
}
impl Native {
    pub async fn connect(bucket_name: &str, prefix: &str) -> anyhow::Result<Published> {
        Self::connect_with_budget(bucket_name, prefix, None).await
    }
    pub async fn connect_with_budget(
        bucket_name: &str,
        prefix: &str,
        limits: Option<crate::upload_budget::UploadLimits>,
    ) -> anyhow::Result<Published> {
        Self::connect_storage(bucket_name, prefix)
            .await?
            .into_published(limits)
            .await
    }

    /// Bind B2's object and metadata transports without initializing a serving engine.
    pub async fn connect_storage(
        bucket_name: &str,
        prefix: &str,
    ) -> anyhow::Result<super::Connection> {
        let key_id =
            std::env::var("B2_APPLICATION_KEY_ID").context("B2_APPLICATION_KEY_ID is required")?;
        let application_key =
            std::env::var("B2_APPLICATION_KEY").context("B2_APPLICATION_KEY is required")?;
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .https_only(true)
            .user_agent(concat!("durable-db/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let auth = authorize(&client, &key_id, &application_key).await?;
        for required in [
            "listBuckets",
            "writeBuckets",
            "readFiles",
            "writeFiles",
            "listFiles",
            "deleteFiles",
        ] {
            if !auth
                .api_info
                .storage_api
                .allowed
                .capabilities
                .iter()
                .any(|c| c == required)
            {
                bail!("B2 application key lacks required capability: {required}");
            }
        }
        let endpoint = auth.api_info.storage_api.s3_api_url.clone();
        let url = reqwest::Url::parse(&endpoint)?;
        let region = url
            .host_str()
            .and_then(|h| h.strip_prefix("s3."))
            .and_then(|h| h.strip_suffix(".backblazeb2.com"))
            .context("unexpected B2 S3 endpoint")?
            .to_owned();
        let account = auth.account_id.clone();
        let mut native = Self {
            client,
            key_id,
            application_key,
            auth: RwLock::new(auth),
            bucket_id: String::new(),
            prefix: format!("{prefix}/"),
        };
        let (status, body) = native
            .call(
                "b2_list_buckets",
                &json!({"accountId":account,"bucketName":bucket_name}),
            )
            .await?;
        ensure_success(status, &body)?;
        let bucket = body["buckets"]
            .as_array()
            .filter(|b| b.len() == 1)
            .context("B2 bucket not found or inaccessible")?[0]
            .clone();
        if bucket["bucketType"] != "allPrivate" {
            bail!("durable-db requires a private B2 bucket");
        }
        native.bucket_id = bucket["bucketId"]
            .as_str()
            .context("missing bucket id")?
            .to_owned();
        let s3 = AmazonS3Builder::new()
            .with_bucket_name(bucket_name)
            .with_region(region)
            .with_endpoint(endpoint)
            .with_access_key_id(&native.key_id)
            .with_secret_access_key(&native.application_key);
        let uploads = Arc::new(super::transport::single_attempt(s3.clone())?);
        let files = Arc::new(
            Remote::new(
                Arc::new(
                    s3.with_http_connector(super::transport::Connector)
                        .build()?,
                ),
                prefix,
            )?
            .with_uploads(uploads),
        );
        let native = Arc::new(native);
        Ok(super::Connection::new(files, native, prefix))
    }
    async fn update(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let account = self.auth.read().await.account_id.clone();
        let wire = crate::publication::control::encode(&info)?;
        let (status, body) = self.call("b2_update_bucket", &json!({"accountId":account,"bucketId":self.bucket_id,"ifRevisionIs":old.revision,"bucketInfo":wire})).await?;
        if status == StatusCode::CONFLICT && body["code"] == "conflict" {
            return Ok(None);
        }
        ensure_success(status, &body)?;
        let revision = body["revision"].as_u64().context("missing B2 revision")?;
        if revision <= old.revision {
            bail!("B2 bucket revision did not advance");
        }
        let returned: BTreeMap<String, String> =
            serde_json::from_value(body["bucketInfo"].clone())?;
        if returned != wire {
            bail!("B2 publication response differs from requested metadata");
        }
        Ok(Some(Bucket {
            version: None,
            revision,
            info,
        }))
    }
    async fn call(&self, operation: &str, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
        let reply = if matches!(operation, "b2_list_buckets" | "b2_list_file_versions") {
            retry::read(|| self.call_once(operation, body)).await?
        } else {
            // In particular, a lost updateBucket response must not become a
            // definite conflict after an automatic transport/status retry.
            self.call_once(operation, body).await?
        };
        Ok((reply.status, reply.body))
    }
    async fn call_once(&self, operation: &str, body: &Value) -> anyhow::Result<retry::Reply> {
        for attempt in 0..2 {
            let auth = self.auth.read().await.clone();
            let metric = match operation {
                "b2_list_buckets" => Operation::NativeListBuckets,
                "b2_update_bucket" => Operation::NativeUpdateBucket,
                "b2_list_file_versions" => Operation::NativeListVersions,
                "b2_delete_file_version" => Operation::NativeDeleteVersion,
                _ => Operation::NativeOther,
            };
            let timer = metrics::REGISTRY.start(metric);
            let response = async {
                let response = self
                    .client
                    .post(format!(
                        "{}/b2api/v4/{operation}",
                        auth.api_info.storage_api.api_url
                    ))
                    .header("Authorization", &auth.authorization_token)
                    .json(body)
                    .send()
                    .await?;
                retry::Reply::read(response).await
            }
            .await;
            timer.finish(match &response {
                Ok(reply) => metrics::http_outcome(reply.status.as_u16()),
                Err(_) => Outcome::Error,
            });
            let result = response?;
            if attempt == 0
                && result.status == StatusCode::UNAUTHORIZED
                && matches!(
                    result.body["code"].as_str(),
                    Some("expired_auth_token" | "bad_auth_token")
                )
            {
                let mut current = self.auth.write().await;
                if current.authorization_token == auth.authorization_token {
                    *current = authorize(&self.client, &self.key_id, &self.application_key).await?;
                }
                continue;
            }
            return Ok(result);
        }
        bail!("B2 authentication refresh failed")
    }
}
async fn authorize(client: &Client, key_id: &str, key: &str) -> anyhow::Result<Authorization> {
    let timer = metrics::REGISTRY.start(Operation::NativeAuthorize);
    let result = async {
        let response = client
            .get("https://api.backblazeb2.com/b2api/v4/b2_authorize_account")
            .basic_auth(key_id, Some(key))
            .send()
            .await?;
        let (status, body) = json_response(response).await?;
        ensure_success(status, &body)?;
        Ok(serde_json::from_value(body)?)
    }
    .await;
    timer.finish(if result.is_ok() {
        Outcome::Ok
    } else {
        Outcome::Error
    });
    result
}
async fn json_response(mut response: reqwest::Response) -> anyhow::Result<(StatusCode, Value)> {
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > 1024 * 1024 {
            bail!("B2 API response exceeds budget");
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = if status.is_success() {
        serde_json::from_slice(&bytes)?
    } else {
        // A gateway may return an HTML error. Retain the HTTP status without
        // exposing its body or losing the read-only retry classification.
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    Ok((status, body))
}
fn ensure_success(status: StatusCode, _body: &Value) -> anyhow::Result<()> {
    // Never print provider messages: they can contain sensitive names or request material.
    if !status.is_success() {
        tracing::warn!(status = status.as_u16(), "B2 native API request rejected");
        bail!("B2 API returned HTTP {}", status.as_u16());
    }
    Ok(())
}
#[async_trait]
impl Authority for Native {
    async fn read(&self) -> anyhow::Result<Bucket> {
        let account = self.auth.read().await.account_id.clone();
        let (status, body) = self
            .call(
                "b2_list_buckets",
                &json!({"accountId":account,"bucketId":self.bucket_id}),
            )
            .await?;
        ensure_success(status, &body)?;
        let b = body["buckets"]
            .as_array()
            .filter(|b| b.len() == 1)
            .context("B2 bucket unavailable")?[0]
            .clone();
        Ok(Bucket {
            version: None,
            revision: b["revision"].as_u64().context("missing bucket revision")?,
            info: crate::publication::control::decode(serde_json::from_value(
                b["bucketInfo"].clone(),
            )?)?,
        })
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        self.update(old, info).await
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        if !["blocks/", "manifests/", "indexes/", "proposals/"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            bail!("invalid B2 collection namespace");
        }
        let name = format!("{}{key}", self.prefix);
        // Normally one version per fresh UUID; bounded batches also remove upload retry duplicates.
        let (status,body)=self.call("b2_list_file_versions",&json!({"bucketId":self.bucket_id,"prefix":name,"startFileName":name,"maxFileCount":100})).await?;
        ensure_success(status, &body)?;
        let files = body["files"]
            .as_array()
            .context("missing B2 version list")?;
        for file in files {
            if file["fileName"].as_str() != Some(name.as_str()) {
                continue;
            }
            let id = file["fileId"]
                .as_str()
                .context("missing B2 file version id")?;
            let (status, body) = self
                .call(
                    "b2_delete_file_version",
                    &json!({"fileName":name,"fileId":id}),
                )
                .await?;
            if status == StatusCode::BAD_REQUEST && body["code"] == "file_not_present" {
                continue;
            }
            ensure_success(status, &body)?;
        }
        // If a pathological number of duplicate versions remains, later sweeps repeat safely.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, routing::post};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Mock {
        stale: AtomicBool,
        listing_failures: AtomicUsize,
        listing_calls: AtomicUsize,
        update_calls: AtomicUsize,
        lose_update_ack: AtomicBool,
        pause_update: AtomicBool,
        update_started: tokio::sync::Notify,
        release_update: tokio::sync::Notify,
        update_finished: tokio::sync::Notify,
        bucket: Mutex<Bucket>,
        deleted: Mutex<Vec<String>>,
    }
    async fn listing(State(s): State<Arc<Mock>>) -> (StatusCode, Json<Value>) {
        s.listing_calls.fetch_add(1, Ordering::SeqCst);
        if s.listing_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"code":"unavailable"})),
            );
        }
        let b = s.bucket.lock().unwrap();
        if s.stale.swap(false, Ordering::SeqCst) {
            return (
                StatusCode::OK,
                Json(json!({"buckets":[{"revision":0,"bucketInfo":{"head":"obsolete"}}]})),
            );
        }
        (
            StatusCode::OK,
            Json(json!({"buckets":[{"revision":b.revision,"bucketInfo":b.info}]})),
        )
    }
    async fn update(State(s): State<Arc<Mock>>, Json(v): Json<Value>) -> (StatusCode, Json<Value>) {
        s.update_calls.fetch_add(1, Ordering::SeqCst);
        if s.pause_update.swap(false, Ordering::SeqCst) {
            // A provider can keep processing after its HTTP client disconnects.
            // Keep that operation separate from the response handler's lifetime.
            return tokio::spawn(async move {
                struct Finished(Arc<Mock>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.update_finished.notify_one();
                    }
                }
                let _finished = Finished(s.clone());
                s.update_started.notify_one();
                tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    s.release_update.notified(),
                )
                .await
                .expect("test must release the delayed provider operation");
                apply_update(&s, v)
            })
            .await
            .unwrap();
        }
        apply_update(&s, v)
    }
    fn apply_update(s: &Mock, v: Value) -> (StatusCode, Json<Value>) {
        assert_eq!(v["bucketId"], "test-bucket");
        assert!(v.get("bucketType").is_none() && v.get("lifecycleRules").is_none());
        let mut b = s.bucket.lock().unwrap();
        let proposed = serde_json::from_value(v["bucketInfo"].clone()).unwrap();
        // Real B2 accepts an identical-info no-op before checking ifRevisionIs.
        if b.info == proposed {
            return (
                StatusCode::OK,
                Json(json!({"revision":b.revision,"bucketInfo":b.info})),
            );
        }
        if v["ifRevisionIs"] != b.revision {
            return (StatusCode::CONFLICT, Json(json!({"code":"conflict"})));
        }
        b.revision += 1;
        b.info = proposed;
        if s.lose_update_ack.swap(false, Ordering::SeqCst) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"code":"lost_response"})),
            );
        }
        (
            StatusCode::OK,
            Json(json!({"revision":b.revision,"bucketInfo":b.info})),
        )
    }
    async fn versions(Json(v): Json<Value>) -> Json<Value> {
        assert_eq!(v["prefix"], "test/blocks/one");
        Json(
            json!({"files":[{"fileName":"test/blocks/one","fileId":"v1"},{"fileName":"test/blocks/one","fileId":"v2"},{"fileName":"test/blocks/one-other","fileId":"keep"}]}),
        )
    }
    async fn delete(State(s): State<Arc<Mock>>, Json(v): Json<Value>) -> Json<Value> {
        s.deleted
            .lock()
            .unwrap()
            .push(v["fileId"].as_str().unwrap().to_owned());
        Json(v)
    }
    async fn fixture() -> (Arc<Native>, Arc<Mock>, tokio::task::JoinHandle<()>) {
        let state = Arc::new(Mock {
            stale: AtomicBool::new(false),
            listing_failures: AtomicUsize::new(0),
            listing_calls: AtomicUsize::new(0),
            update_calls: AtomicUsize::new(0),
            lose_update_ack: AtomicBool::new(false),
            pause_update: AtomicBool::new(false),
            update_started: tokio::sync::Notify::new(),
            release_update: tokio::sync::Notify::new(),
            update_finished: tokio::sync::Notify::new(),
            bucket: Mutex::new(Bucket {
                version: None,
                revision: 1,
                info: BTreeMap::from([("head".into(), "current".into())]),
            }),
            deleted: Mutex::new(vec![]),
        });
        let app = Router::new()
            .route("/b2api/v4/b2_list_buckets", post(listing))
            .route("/b2api/v4/b2_update_bucket", post(update))
            .route("/b2api/v4/b2_list_file_versions", post(versions))
            .route("/b2api/v4/b2_delete_file_version", post(delete))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let native = Native {
            client: Client::new(),
            key_id: "test".into(),
            application_key: "test".into(),
            bucket_id: "test-bucket".into(),
            prefix: "test/".into(),
            auth: RwLock::new(Authorization {
                account_id: "account".into(),
                authorization_token: "token".into(),
                api_info: ApiInfo {
                    storage_api: StorageApi {
                        api_url: format!("http://{address}"),
                        s3_api_url: String::new(),
                        allowed: Allowed {
                            capabilities: vec![],
                        },
                    },
                },
            }),
        };
        (Arc::new(native), state, server)
    }

    #[tokio::test]
    async fn native_budget_bootstrap_uses_b2_revision_checks() {
        let (native, _, server) = fixture().await;
        let files =
            Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
        super::super::check_native_budget(super::super::Connection::new(files, native, "test"))
            .await;
        server.abort();
    }

    #[tokio::test]
    async fn cancelled_control_http_request_requires_revalidation_and_fences_late_application() {
        for apply_before_revalidation in [false, true] {
            cancelled_control_request(apply_before_revalidation).await;
        }
    }

    #[tokio::test]
    async fn root_controller_handles_cancelled_claims_and_lost_renewal_replies_over_http() {
        for apply_before_successor in [false, true] {
            root_controller_handoff(apply_before_successor).await;
        }
    }

    async fn root_controller_handoff(apply_before_successor: bool) {
        use crate::ownership::root::{Error as RootError, Outcome, Profile};
        use std::time::Duration;

        #[derive(Clone)]
        struct Clock(Arc<AtomicUsize>);
        impl crate::ownership::timing::Clock for Clock {
            fn read(&self) -> Option<Duration> {
                Some(Duration::from_secs(self.0.load(Ordering::SeqCst) as u64))
            }
        }
        let (native, state, server) = fixture().await;
        let files =
            Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
        let connection = super::super::Connection::new(files, native, "test");
        let clock = Clock(Arc::new(AtomicUsize::new(0)));
        let profile = Profile::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap();
        let mut old = connection.root(clock.clone(), profile).unwrap();
        let observed = old.observe().await.unwrap();
        let request = old.claim(observed).unwrap();
        state.pause_update.store(true, Ordering::SeqCst);
        let work = tokio::spawn(request.execute());

        let result = tokio::time::timeout(Duration::from_secs(10), async {
            state.update_started.notified().await;
            old.abandon();
            work.abort();
            assert!(work.await.err().unwrap().is_cancelled());
            if apply_before_successor {
                state.release_update.notify_one();
                state.update_finished.notified().await;
            }

            clock.0.store(5, Ordering::SeqCst);
            let mut successor = connection.root(clock.clone(), profile).unwrap();
            let observed = successor.observe().await.unwrap();
            let request = successor.claim(observed).unwrap();
            assert!(matches!(
                successor.finish(request.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            if !apply_before_successor {
                state.release_update.notify_one();
                state.update_finished.notified().await;
            }
            assert!(old.permission().unwrap().is_none());
            let observed = old.observe().await.unwrap();
            assert!(matches!(old.validate(observed), Err(RootError::NotOwner)));

            clock.0.store(27, Ordering::SeqCst);
            let observed = successor.observe().await.unwrap();
            let request = successor.validate(observed).unwrap();
            state.lose_update_ack.store(true, Ordering::SeqCst);
            assert!(matches!(
                successor.finish(request.execute().await).unwrap(),
                Outcome::Uncertain(_)
            ));
            assert!(matches!(successor.activate(), Err(RootError::NotReady)));
            assert!(successor.permission().unwrap().is_none());

            clock.0.store(28, Ordering::SeqCst);
            let observed = successor.observe().await.unwrap();
            let request = successor.validate(observed).unwrap();
            assert!(matches!(
                successor.finish(request.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            let request = successor.activate().unwrap();
            assert!(matches!(
                successor.finish(request.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            assert_eq!(
                successor.permission().unwrap().unwrap().epoch,
                if apply_before_successor { 2 } else { 1 }
            );
            assert_eq!(state.update_calls.load(Ordering::SeqCst), 5);
            assert_eq!(state.bucket.lock().unwrap().info["head"], "current");
            let mut recovery = successor.recovery().unwrap();
            if apply_before_successor {
                let mut journal = recovery.next().await.unwrap().unwrap();
                assert!(journal.read_next().await.unwrap().is_none());
                let request = successor.select_prefix(journal).unwrap();
                state.lose_update_ack.store(true, Ordering::SeqCst);
                assert!(matches!(
                    successor.finish(request.execute().await).unwrap(),
                    Outcome::Uncertain(_)
                ));
                assert_eq!(state.update_calls.load(Ordering::SeqCst), 6);
                let observed = successor.observe().await.unwrap();
                let request = successor.validate(observed).unwrap();
                assert!(matches!(
                    successor.finish(request.execute().await).unwrap(),
                    Outcome::Confirmed
                ));
                let mut recovered = successor.recovery().unwrap();
                let mut journal = recovered.next().await.unwrap().unwrap();
                assert!(journal.is_selected());
                assert!(journal.read_next().await.unwrap().is_none());
                assert_eq!(state.update_calls.load(Ordering::SeqCst), 7);
            } else {
                assert!(recovery.next().await.unwrap().is_none());
            }
            let mut replay = successor.replay().unwrap();
            assert!(replay.read_next().await.unwrap().is_none());
            assert!(replay.completed());
            let view = crate::transactions::recovery::restore(
                successor.replay().unwrap(),
                crate::transactions::recovery::tests::Memory::default(),
            )
            .await
            .unwrap();
            assert!(view.states.is_empty());
            assert!(view.values.is_empty());
            // The compatibility engine must refuse this experimental namespace.
            assert!(connection.into_published(None).await.is_err());
            assert_eq!(
                state.bucket.lock().unwrap().info["durable-db-format"],
                "owned-candidate-8"
            );
        })
        .await;
        server.abort();
        result.unwrap();
    }

    async fn cancelled_control_request(apply_before_revalidation: bool) {
        use crate::providers::control::Outcome;
        let (native, state, server) = fixture().await;
        let files =
            Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
        let connection = super::super::Connection::new(files, native, "test");
        let mut session = connection.control();
        let observed = session.observe().await.unwrap();
        let mut proposed = observed.metadata().clone();
        proposed.insert("head".into(), "late freeze".into());
        state.pause_update.store(true, Ordering::SeqCst);
        let request = session.begin(observed, proposed).unwrap();
        let work = tokio::spawn(request.execute());

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            state.update_started.notified().await;
            session.abandon();
            work.abort();
            assert!(work.await.err().unwrap().is_cancelled());

            if apply_before_revalidation {
                state.release_update.notify_one();
                state.update_finished.notified().await;
            }
            // Revalidation either discovers the abandoned write or fences it
            // before it applies. Neither cancellation nor a raw read does that.
            let observed = session.observe().await.unwrap();
            let metadata = observed.metadata().clone();
            let request = session.begin(observed, metadata).unwrap();
            assert!(matches!(
                session.finish(request.execute().await),
                Outcome::Confirmed
            ));
            if !apply_before_revalidation {
                state.release_update.notify_one();
                state.update_finished.notified().await;
            }
            let expected = if apply_before_revalidation {
                "late freeze"
            } else {
                "current"
            };
            assert_eq!(session.confirmed().unwrap().metadata()["head"], expected);
            let bucket = state.bucket.lock().unwrap();
            assert_eq!(bucket.info["head"], expected);
            assert_eq!(
                bucket.revision,
                if apply_before_revalidation { 3 } else { 2 }
            );
            assert_eq!(state.update_calls.load(Ordering::SeqCst), 2);
        })
        .await;
        server.abort();
        result.unwrap();
    }

    #[tokio::test]
    async fn publication_initialization_accepts_probes_but_rejects_unknown_protocols() {
        for foreign_protocol in [false, true] {
            let (native, state, server) = fixture().await;
            let files = Arc::new(
                Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap(),
            );
            let connection = super::super::Connection::new(files, native, "test");
            let register = connection.authority();
            let read = register.read_fresh().await.unwrap();
            assert!(read.info.contains_key("durable-db-read-fence"));
            if foreign_protocol {
                let mut info = read.info.clone();
                info.insert("durable-db-other-engine".into(), "initialized".into());
                register.replace(&read, info).await.unwrap().unwrap();
            }
            let result = connection.into_published(None).await;
            if foreign_protocol {
                assert!(result.is_err());
                assert!(
                    !state
                        .bucket
                        .lock()
                        .unwrap()
                        .info
                        .contains_key("durable-db-format")
                );
            } else {
                assert!(result.is_ok());
                assert_eq!(state.bucket.lock().unwrap().info["durable-db-format"], "3");
            }
            server.abort();
            server.await.unwrap_err();
        }
    }

    #[tokio::test]
    async fn connection_requires_fresh_comparison_for_identical_logical_metadata() {
        let (native, state, server) = fixture().await;
        let stale = Bucket {
            revision: 0,
            version: None,
            info: BTreeMap::new(),
        };
        let initial = native.read().await.unwrap();
        // Pin the provider quirk: even the revision-advance response check alone
        // cannot distinguish this stale, identical-info success from a mutation.
        let noop = native
            .replace(&stale, initial.info.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(noop.revision, initial.revision);

        let files =
            Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
        let connection = super::super::Connection::new(files, native, "test");
        let register = connection.authority();
        drop(connection);
        assert!(
            register
                .replace(&stale, initial.info.clone())
                .await
                .unwrap()
                .is_none()
        );
        let first = register
            .replace(&initial, initial.info.clone())
            .await
            .unwrap()
            .unwrap();
        let second = register
            .replace(&first, first.info.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.info["head"], initial.info["head"]);
        assert_eq!(second.info["head"], initial.info["head"]);
        assert_ne!(
            first.info["durable-db-probe"],
            second.info["durable-db-probe"]
        );
        assert!(second.revision > first.revision);
        // This payload is byte-for-byte equal to the live metadata before the
        // common layer stamps it. Replaying it must not validate an old revision.
        assert!(
            register
                .replace(&initial, second.info.clone())
                .await
                .unwrap()
                .is_none()
        );

        let calls = state.update_calls.load(Ordering::SeqCst);
        state.lose_update_ack.store(true, Ordering::SeqCst);
        assert!(
            register
                .replace(&second, second.info.clone())
                .await
                .is_err()
        );
        assert_eq!(state.update_calls.load(Ordering::SeqCst), calls + 1);
        let observed = register.read().await.unwrap();
        assert!(observed.revision > second.revision);
        assert_ne!(
            observed.info["durable-db-probe"],
            second.info["durable-db-probe"]
        );
        assert!(
            register
                .replace(&second, observed.info)
                .await
                .unwrap()
                .is_none()
        );
        server.abort();
        server.await.unwrap_err();
    }

    #[tokio::test]
    async fn native_http_barrier_rejects_stale_observation_and_deletes_exact_versions() {
        let (native, state, server) = fixture().await;
        state.stale.store(true, Ordering::SeqCst);
        state.listing_failures.store(2, Ordering::SeqCst);
        let read = native.read_fresh().await.unwrap();
        assert_eq!(read.revision, 2);
        assert_eq!(state.listing_calls.load(Ordering::SeqCst), 4);
        assert_eq!(read.info["head"], "current");
        assert!(
            native
                .replace(
                    &Bucket {
                        version: None,
                        revision: 0,
                        info: BTreeMap::new()
                    },
                    BTreeMap::new()
                )
                .await
                .unwrap()
                .is_none()
        );
        native.delete_versions("blocks/one").await.unwrap();
        assert_eq!(*state.deleted.lock().unwrap(), vec!["v1", "v2"]);
        let updates = state.update_calls.load(Ordering::SeqCst);
        state.lose_update_ack.store(true, Ordering::SeqCst);
        let mut next = read.info.clone();
        next.insert("head".into(), "changed".into());
        assert!(native.replace(&read, next).await.is_err());
        assert_eq!(
            state.update_calls.load(Ordering::SeqCst),
            updates + 1,
            "uncertain writes are never retried as fresh comparisons"
        );
        assert_eq!(native.read().await.unwrap().info["head"], "changed");
        let reads = state.listing_calls.load(Ordering::SeqCst);
        state.listing_failures.store(9, Ordering::SeqCst);
        assert!(native.read().await.is_err());
        assert_eq!(
            state.listing_calls.load(Ordering::SeqCst),
            reads + 3,
            "read retries have a hard attempt bound"
        );
        server.abort();
    }
}
