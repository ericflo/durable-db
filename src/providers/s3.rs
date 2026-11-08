//! S3 wire binding for the shared publication protocol. No transaction/GC algorithm here.
use crate::{
    publication::{Authority, Bucket, Published},
    storage::{Remote, Storage, Version},
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ObjectStore, PutMode, UpdateVersion,
    aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer},
    client::HttpRequestBody,
    path::Path,
    signer::Signer,
};
use reqwest::{Client, Method, StatusCode};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub struct S3 {
    store: Arc<AmazonS3>,
    files: Arc<Remote>,
    prefix: String,
    region: String,
    client: Client,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Versions {
    #[serde(default)]
    version: Vec<FileVersion>,
    #[serde(default)]
    delete_marker: Vec<FileVersion>,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FileVersion {
    key: String,
    version_id: String,
}
impl S3 {
    pub async fn connect(bucket: &str, prefix: &str) -> anyhow::Result<Published> {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let store = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(&region)
                .build()?,
        );
        let files = Arc::new(Remote::new(store.clone(), prefix)?);
        let this = Arc::new(Self {
            store,
            files: files.clone(),
            prefix: format!("{prefix}/"),
            region,
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        });
        Published::open(files, this, prefix).await
    }
    fn authority_key(&self) -> String {
        format!("{}authority.json", self.prefix)
    }
    async fn request(
        &self,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
    ) -> anyhow::Result<reqwest::Response> {
        // Reuse the configured S3 endpoint and credential chain, including workload identity.
        let mut url = self
            .store
            .signed_url(method.clone(), &Path::from(key), Duration::from_secs(30))
            .await?;
        url.set_query(None);
        {
            let mut pairs = url.query_pairs_mut();
            for (k, v) in query {
                pairs.append_pair(k, v);
            }
        }
        let credential = self.store.credentials().get_credential().await?;
        let mut signed = axum::http::Request::builder()
            .method(method.clone())
            .uri(url.as_str())
            .body(HttpRequestBody::from(Bytes::new()))?;
        AwsAuthorizer::new(&credential, "s3", &self.region)
            .with_sign_payload(false)
            .authorize(&mut signed, None);
        Ok(self
            .client
            .request(method, url)
            .headers(signed.headers().clone())
            .send()
            .await
            .map_err(reqwest::Error::without_url)?)
    }
    async fn versions(&self, key: &str) -> anyhow::Result<Vec<FileVersion>> {
        let mut response = self
            .request(
                Method::GET,
                "",
                &[("versions", ""), ("prefix", key), ("max-keys", "128")],
            )
            .await?;
        if !response.status().is_success() {
            bail!("S3 version listing failed: {}", response.status().as_u16());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                bail!("S3 version list exceeds budget");
            }
            bytes.extend_from_slice(&chunk);
        }
        let listed: Versions =
            quick_xml::de::from_reader(bytes.as_slice()).context("invalid S3 version list")?;
        Ok(listed
            .version
            .into_iter()
            .chain(listed.delete_marker)
            .filter(|v| v.key == key)
            .collect())
    }
    async fn delete_version(&self, file: &FileVersion) -> anyhow::Result<()> {
        let response = self
            .request(
                Method::DELETE,
                &file.key,
                &[("versionId", &file.version_id)],
            )
            .await?;
        if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
            bail!("S3 version deletion failed: {}", response.status().as_u16());
        }
        Ok(())
    }
}
#[async_trait]
impl Authority for S3 {
    async fn read(&self) -> anyhow::Result<Bucket> {
        let Some(object) = self.files.get("authority.json", 16384).await? else {
            return Ok(Bucket {
                revision: 0,
                info: BTreeMap::new(),
                version: None,
            });
        };
        let mut bucket: Bucket = serde_json::from_slice(&object.bytes)?;
        bucket.version = Some(object.version);
        Ok(bucket)
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let mut next = Bucket {
            revision: old
                .revision
                .checked_add(1)
                .context("publication revision exhausted")?,
            info,
            version: None,
        };
        let mode = old
            .version
            .as_ref()
            .map_or(PutMode::Create, |v| PutMode::Update(v.0.clone()));
        let bytes = serde_json::to_vec(&next)?;
        let result = match self
            .store
            .put_opts(&Path::from(self.authority_key()), bytes.into(), mode.into())
            .await
        {
            Ok(r) => r,
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        next.version = Some(Version(UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        }));
        Ok(Some(next))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        if !["blocks/", "manifests/", "indexes/"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            bail!("invalid collection namespace");
        }
        for file in self.versions(&format!("{}{key}", self.prefix)).await? {
            self.delete_version(&file).await?;
        }
        Ok(())
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        // Versioned S3 retains prior control-register writes. List before the common read
        // barrier, then delete immutable historical versions. Never delete the mutable null
        // version used by unversioned/suspended buckets.
        let candidates = self.versions(&self.authority_key()).await?;
        if candidates.iter().all(|v| v.version_id == "null") {
            return Ok(());
        }
        let current = self.read_fresh().await?;
        let protected = current
            .version
            .as_ref()
            .and_then(|v| v.0.version.as_deref());
        for file in candidates {
            if file.version_id != "null" && Some(file.version_id.as_str()) != protected {
                self.delete_version(&file).await?;
            }
        }
        Ok(())
    }
}
