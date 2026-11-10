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
        Self::connect_with_budget(bucket, prefix, None).await
    }
    pub async fn connect_with_budget(
        bucket: &str,
        prefix: &str,
        limits: Option<crate::upload_budget::UploadLimits>,
    ) -> anyhow::Result<Published> {
        Self::connect_storage(bucket, prefix)
            .await?
            .into_published(limits)
            .await
    }

    /// Bind S3's object and metadata transports without initializing a serving engine.
    pub async fn connect_storage(bucket: &str, prefix: &str) -> anyhow::Result<super::Connection> {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(&region);
        let this = Self::bind(builder, prefix, region)?;
        Ok(super::Connection::new(this.files.clone(), this, prefix))
    }
    fn bind(builder: AmazonS3Builder, prefix: &str, region: String) -> anyhow::Result<Arc<Self>> {
        // A conditional PUT that committed but returned 503 must remain
        // unknown. An SDK retry can receive 412 from that very commit and
        // incorrectly turn it into a definite failed comparison. Both control
        // and immutable uploads have one attempt; reads retain SDK retries.
        let store = Arc::new(super::transport::single_attempt(builder.clone())?);
        let files = Arc::new(
            Remote::new(
                Arc::new(
                    builder
                        .with_http_connector(super::transport::Connector)
                        .build()?,
                ),
                prefix,
            )?
            .with_uploads(store.clone()),
        );
        Ok(Arc::new(Self {
            store,
            files: files.clone(),
            prefix: format!("{prefix}/"),
            region,
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()?,
        }))
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
            .try_authorize(&mut signed, None)?;
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
        bucket.info = crate::publication::control::decode(bucket.info)?;
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
            info: crate::publication::control::encode(&info)?,
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
        next.info = info;
        Ok(Some(next))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        if !["blocks/", "manifests/", "indexes/", "proposals/"]
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, extract::State, http::HeaderMap, response::Response, routing::get};
    use std::sync::Mutex;

    struct Wire {
        absent: bool,
        puts: usize,
        object_puts: usize,
        object: Bytes,
        bucket: Bucket,
        etag: String,
        acknowledgement: u16,
    }

    async fn read_object(State(state): State<Arc<Mutex<Wire>>>) -> Response {
        let state = state.lock().unwrap();
        Response::builder()
            .header("etag", "\"object\"")
            .header("last-modified", "Sun, 08 Nov 2026 00:00:00 GMT")
            .body(Body::from(state.object.clone()))
            .unwrap()
    }

    async fn write_object(
        State(state): State<Arc<Mutex<Wire>>>,
        headers: HeaderMap,
        bytes: Bytes,
    ) -> Response {
        assert!(headers.get("if-match").is_none());
        assert!(headers.get("if-none-match").is_none());
        let mut state = state.lock().unwrap();
        state.object_puts += 1;
        state.object = bytes;
        Response::builder()
            .status(state.acknowledgement)
            .header("location", "/test/prefix/journal/frame")
            .body(Body::from("<Error><Code>InternalError</Code></Error>"))
            .unwrap()
    }

    async fn read(State(state): State<Arc<Mutex<Wire>>>) -> Response {
        let state = state.lock().unwrap();
        if state.absent {
            return Response::builder()
                .status(404)
                .body(Body::from("<Error><Code>NoSuchKey</Code></Error>"))
                .unwrap();
        }
        Response::builder()
            .header("etag", &state.etag)
            .header("last-modified", "Sun, 08 Nov 2026 00:00:00 GMT")
            .body(Body::from(serde_json::to_vec(&state.bucket).unwrap()))
            .unwrap()
    }

    async fn write(
        State(state): State<Arc<Mutex<Wire>>>,
        headers: HeaderMap,
        bytes: Bytes,
    ) -> Response {
        let mut state = state.lock().unwrap();
        state.puts += 1;
        let matches = if state.absent {
            headers.get("if-none-match").and_then(|h| h.to_str().ok()) == Some("*")
        } else {
            headers.get("if-match").and_then(|h| h.to_str().ok()) == Some(state.etag.as_str())
        };
        if !matches {
            return Response::builder()
                .status(412)
                .body(Body::from("<Error><Code>PreconditionFailed</Code></Error>"))
                .unwrap();
        }
        state.bucket = serde_json::from_slice(&bytes).unwrap();
        state.absent = false;
        state.etag = format!("\"revision-{}\"", state.bucket.revision);
        // The authoritative update has happened. Simulate a gateway/server
        // losing its successful acknowledgement and returning a transient error.
        Response::builder()
            .status(state.acknowledgement)
            .header("etag", &state.etag)
            .header("location", "/test/prefix/authority.json")
            .body(Body::from("<Error><Code>InternalError</Code></Error>"))
            .unwrap()
    }

    #[tokio::test]
    async fn conditional_update_does_not_retry_a_committed_error_into_a_false_comparison() {
        check_uncertainty(503).await;
    }

    #[tokio::test]
    async fn the_same_root_controller_recovers_creation_and_fences_late_s3_updates() {
        use crate::ownership::root::{Outcome, Profile};
        use std::{
            sync::atomic::{AtomicU64, Ordering},
            time::Duration,
        };
        #[derive(Clone)]
        struct Clock(Arc<AtomicU64>);
        impl crate::ownership::timing::Clock for Clock {
            fn read(&self) -> Option<Duration> {
                Some(Duration::from_secs(self.0.load(Ordering::SeqCst)))
            }
        }
        let (connection, state, task) = fixture(503, true).await;
        let clock = Clock(Arc::new(AtomicU64::new(0)));
        let profile = Profile::new(Duration::from_secs(10), Duration::from_secs(1)).unwrap();
        let mut old = connection.root(clock.clone(), profile).unwrap();
        let observed = old.observe().await.unwrap();
        let request = old.claim(observed).unwrap();
        assert!(matches!(
            old.finish(request.execute().await).unwrap(),
            Outcome::Uncertain(_)
        ));
        assert_eq!(state.lock().unwrap().puts, 1);
        assert!(old.permission().unwrap().is_none());
        state.lock().unwrap().acknowledgement = 200;
        clock.0.store(3, Ordering::SeqCst);
        let observed = old.observe().await.unwrap();
        let request = old.validate(observed).unwrap();
        assert!(matches!(
            old.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        clock.0.store(26, Ordering::SeqCst);
        let observed = old.observe().await.unwrap();
        let request = old.validate(observed).unwrap();
        assert!(matches!(
            old.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        let request = old.activate().unwrap();
        assert!(matches!(
            old.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        assert_eq!(old.permission().unwrap().unwrap().epoch, 1);

        clock.0.store(27, Ordering::SeqCst);
        let observed = old.observe().await.unwrap();
        let delayed = old.validate(observed).unwrap();
        old.abandon();
        let mut successor = connection.root(clock.clone(), profile).unwrap();
        let observed = successor.observe().await.unwrap();
        let request = successor.claim(observed).unwrap();
        assert!(matches!(
            successor.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        assert!(matches!(
            old.finish(delayed.execute().await).unwrap(),
            Outcome::Ignored
        ));
        assert_eq!(state.lock().unwrap().bucket.revision, 5);
        clock.0.store(50, Ordering::SeqCst);
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
        assert_eq!(successor.permission().unwrap().unwrap().epoch, 2);
        assert!(old.permission().unwrap().is_none());
        assert_eq!(state.lock().unwrap().puts, 8);
        task.abort();
        task.await.unwrap_err();
    }

    #[tokio::test]
    async fn conditional_update_does_not_follow_a_redirect_into_a_false_comparison() {
        for status in [301, 302, 303, 307, 308] {
            check_uncertainty(status).await;
        }
    }

    async fn fixture(
        acknowledgement: u16,
        absent: bool,
    ) -> (
        super::super::Connection,
        Arc<Mutex<Wire>>,
        tokio::task::JoinHandle<()>,
    ) {
        let initial = Bucket {
            revision: 0,
            version: None,
            info: BTreeMap::new(),
        };
        let state = Arc::new(Mutex::new(Wire {
            absent,
            puts: 0,
            object_puts: 0,
            object: Bytes::new(),
            bucket: initial,
            etag: "\"old\"".into(),
            acknowledgement,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route("/test/prefix/authority.json", get(read).put(write))
            .route(
                "/test/prefix/journal/frame",
                get(read_object).put(write_object),
            )
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let binding = S3::bind(
            AmazonS3Builder::new()
                .with_bucket_name("test")
                .with_region("us-east-1")
                .with_access_key_id("fixture")
                .with_secret_access_key("fixture")
                .with_endpoint(endpoint)
                .with_allow_http(true),
            "prefix",
            "us-east-1".into(),
        )
        .unwrap();
        let connection = super::super::Connection::new(binding.files.clone(), binding, "prefix");
        (connection, state, task)
    }

    async fn check_uncertainty(acknowledgement: u16) {
        let (connection, state, task) = fixture(acknowledgement, false).await;
        assert_eq!(
            state.lock().unwrap().puts,
            0,
            "binding must not initialize metadata"
        );
        let binding = connection.authority();
        let objects = connection.objects();
        drop(connection);
        let payload = Bytes::from_static(b"immutable frame");
        assert!(objects.put("journal/frame", payload.clone()).await.is_err());
        assert_eq!(
            objects
                .get("journal/frame", 64)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            payload
        );
        {
            let state = state.lock().unwrap();
            assert_eq!(state.object_puts, 1, "uncertain data PUT must not retry");
            assert_eq!(state.puts, 0, "data must not mutate the metadata register");
            assert!(
                state.bucket.info.is_empty(),
                "no publication format was initialized"
            );
        }
        let before = binding.read().await.unwrap();
        let next = BTreeMap::from([("fixture".into(), "committed".into())]);
        assert!(binding.replace(&before, next.clone()).await.is_err());
        assert_eq!(state.lock().unwrap().puts, 1);
        let observed = binding.read().await.unwrap();
        assert_eq!(observed.revision, 1);
        assert_eq!(observed.info["fixture"], next["fixture"]);
        assert!(observed.info.contains_key("durable-db-probe"));
        // Even identical logical metadata must compare the exact revision.
        assert!(
            binding
                .replace(&before, observed.info.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(state.lock().unwrap().puts, 2);
        assert_eq!(binding.read().await.unwrap().revision, 1);
        task.abort();
        task.await.unwrap_err();
    }
}
