//! B2 native metadata and physical version deletion; S3 API carries immutable bytes.
use super::{Authority, B2, Bucket};
use crate::storage::Remote;
use anyhow::{Context, bail};
use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

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
    pub async fn connect(bucket_name: &str, prefix: &str) -> anyhow::Result<B2> {
        let key_id =
            std::env::var("B2_APPLICATION_KEY_ID").context("B2_APPLICATION_KEY_ID is required")?;
        let application_key =
            std::env::var("B2_APPLICATION_KEY").context("B2_APPLICATION_KEY is required")?;
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
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
        let response = client
            .post(format!(
                "{}/b2api/v4/b2_list_buckets",
                auth.api_info.storage_api.api_url
            ))
            .header("Authorization", &auth.authorization_token)
            .json(&json!({"accountId":auth.account_id,"bucketName":bucket_name}))
            .send()
            .await?;
        let (status, body) = json_response(response).await?;
        ensure_success(status, &body)?;
        let bucket = body["buckets"]
            .as_array()
            .filter(|b| b.len() == 1)
            .context("B2 bucket not found or inaccessible")?[0]
            .clone();
        if bucket["bucketType"] != "allPrivate" {
            bail!("durable-db requires a private B2 bucket");
        }
        let bucket_id = bucket["bucketId"]
            .as_str()
            .context("missing bucket id")?
            .to_owned();
        let s3 = AmazonS3Builder::new()
            .with_bucket_name(bucket_name)
            .with_region(region)
            .with_endpoint(endpoint)
            .with_access_key_id(&key_id)
            .with_secret_access_key(&application_key)
            .build()?;
        let files = Arc::new(Remote::new(Arc::new(s3), prefix)?);
        let native = Arc::new(Self {
            client,
            key_id,
            application_key,
            auth: RwLock::new(auth),
            bucket_id,
            prefix: format!("{prefix}/"),
        });
        // Exercise the real native precondition. A concurrent startup may win instead.
        let before = native.read().await?;
        let mut info = before.info.clone();
        info.insert("durable-db-probe".into(), uuid::Uuid::new_v4().to_string());
        if native.replace(&before, info.clone()).await? && native.replace(&before, info).await? {
            bail!("B2 provider ignored bucket revision precondition");
        }
        B2::open(files, native, prefix).await
    }
    async fn call(&self, operation: &str, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
        for attempt in 0..2 {
            let auth = self.auth.read().await.clone();
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
            let result = json_response(response).await?;
            if attempt == 0
                && result.0 == StatusCode::UNAUTHORIZED
                && matches!(
                    result.1["code"].as_str(),
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
    let response = client
        .get("https://api.backblazeb2.com/b2api/v4/b2_authorize_account")
        .basic_auth(key_id, Some(key))
        .send()
        .await?;
    let (status, body) = json_response(response).await?;
    ensure_success(status, &body)?;
    Ok(serde_json::from_value(body)?)
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
    Ok((status, serde_json::from_slice(&bytes)?))
}
fn ensure_success(status: StatusCode, _body: &Value) -> anyhow::Result<()> {
    // Never print provider messages: they can contain sensitive names or request material.
    if !status.is_success() {
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
            revision: b["revision"].as_u64().context("missing bucket revision")?,
            info: serde_json::from_value(b["bucketInfo"].clone())?,
        })
    }
    async fn replace(&self, old: &Bucket, info: BTreeMap<String, String>) -> anyhow::Result<bool> {
        let account = self.auth.read().await.account_id.clone();
        let (status,body)=self.call("b2_update_bucket",&json!({"accountId":account,"bucketId":self.bucket_id,"ifRevisionIs":old.revision,"bucketInfo":info})).await?;
        if status == StatusCode::CONFLICT && body["code"] == "conflict" {
            return Ok(false);
        }
        ensure_success(status, &body)?;
        if body["revision"].as_u64().is_none_or(|r| r <= old.revision) {
            bail!("B2 bucket revision did not advance");
        }
        Ok(true)
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        if !key.starts_with("blocks/") && !key.starts_with("manifests/") {
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
