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
        let key_id =
            std::env::var("B2_APPLICATION_KEY_ID").context("B2_APPLICATION_KEY_ID is required")?;
        let application_key =
            std::env::var("B2_APPLICATION_KEY").context("B2_APPLICATION_KEY is required")?;
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
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
            .with_secret_access_key(&native.application_key)
            .build()?;
        let files = Arc::new(Remote::new(Arc::new(s3), prefix)?);
        let native = Arc::new(native);
        Published::open(files, native, prefix).await
    }
    async fn update(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let account = self.auth.read().await.account_id.clone();
        let (status, body) = self.call("b2_update_bucket", &json!({"accountId":account,"bucketId":self.bucket_id,"ifRevisionIs":old.revision,"bucketInfo":info})).await?;
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
        if returned != info {
            bail!("B2 publication response differs from requested metadata");
        }
        Ok(Some(Bucket {
            version: None,
            revision,
            info: returned,
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
            info: serde_json::from_value(b["bucketInfo"].clone())?,
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
        assert_eq!(v["bucketId"], "test-bucket");
        assert!(v.get("bucketType").is_none() && v.get("lifecycleRules").is_none());
        let mut b = s.bucket.lock().unwrap();
        if v["ifRevisionIs"] != b.revision {
            return (StatusCode::CONFLICT, Json(json!({"code":"conflict"})));
        }
        b.revision += 1;
        b.info = serde_json::from_value(v["bucketInfo"].clone()).unwrap();
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
    #[tokio::test]
    async fn native_http_barrier_rejects_stale_observation_and_deletes_exact_versions() {
        let state = Arc::new(Mock {
            stale: AtomicBool::new(true),
            listing_failures: AtomicUsize::new(2),
            listing_calls: AtomicUsize::new(0),
            update_calls: AtomicUsize::new(0),
            lose_update_ack: AtomicBool::new(false),
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
