//! Any S3-compatible bucket: Amazon S3, Backblaze B2, Cloudflare R2, MinIO.
//!
//! One code path for all of them. It uses only what Backblaze B2 implements:
//! PutObject, GetObject (with ranges), HeadObject, DeleteObject,
//! ListObjectsV2 and ListObjectVersions. In particular it never sends
//! `If-Match` or `If-None-Match`, which B2 rejects.
//!
//! Requests are signed with AWS Signature Version 4 and addressed path-style.
//!
//! One operation has a second wire form. On Backblaze, PutObject goes through
//! a gateway that measured two to three times slower than B2's own upload
//! call, with a far longer tail, and an upload is on the path of every
//! commit. So against a Backblaze endpoint `put` uses `b2_upload_file`
//! with the same key, the same bytes and the same credentials. The object
//! that results is the same object; nothing above this file can tell.
use super::{Listed, ObjectStore, StoreError, StoreResult};
use async_trait::async_trait;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub struct S3Config {
    /// For example `https://s3.us-west-002.backblazeb2.com`.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

pub struct S3Store {
    config: S3Config,
    host: String,
    client: reqwest::Client,
    b2: Option<Arc<B2Uploader>>,
}

fn unavailable(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

fn hex_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hmac(key: &[u8], message: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encode everything but RFC 3986 unreserved characters.
fn encode(text: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (keep_slash && byte == b'/')
        {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` for the current time, in UTC.
fn timestamp() -> (String, String) {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    let date = format!("{year:04}{month:02}{day:02}");
    let time = format!(
        "{date}T{:02}{:02}{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    );
    (date, time)
}

fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The text of the first `<tag>` element in `xml`.
fn tag<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(&xml[start..end])
}

/// Every `<tag>` element in `xml`, in order.
fn tags<'a>(xml: &'a str, name: &'a str) -> impl Iterator<Item = &'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let end = rest[start..].find(&close)? + start;
        let found = &rest[start..end];
        rest = &rest[end + close.len()..];
        Some(found)
    })
}

struct Reply {
    status: StatusCode,
    body: Bytes,
}

impl S3Store {
    pub fn new(config: S3Config) -> StoreResult<Self> {
        let endpoint = config.endpoint.trim_end_matches('/').to_owned();
        let host = endpoint
            .split_once("://")
            .map(|(_, host)| host.to_owned())
            .ok_or_else(|| {
                StoreError::Fatal("the endpoint must begin with http:// or https://".into())
            })?;
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(50))
            .pool_max_idle_per_host(256)
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| StoreError::Fatal(e.to_string()))?;
        Ok(Self {
            config: S3Config { endpoint, ..config },
            host,
            client,
            b2: None,
        })
    }

    /// Whether this endpoint is Backblaze B2.
    pub fn is_backblaze(&self) -> bool {
        self.host.ends_with(".backblazeb2.com")
    }

    /// Upload through `b2_upload_file` instead of PutObject.
    pub fn with_b2_uploads(mut self) -> Self {
        self.b2 = Some(Arc::new(B2Uploader {
            client: self.client.clone(),
            key_id: self.config.access_key_id.clone(),
            key: self.config.secret_access_key.clone(),
            bucket: self.config.bucket.clone(),
            session: tokio::sync::Mutex::new(None),
            pool: parking_lot::Mutex::new(B2Pool::default()),
        }));
        self
    }

    /// One signed request. `query` pairs must already be in the order given;
    /// they are sorted for signing.
    async fn send(
        &self,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
        range: Option<(u64, u64)>,
        body: Bytes,
    ) -> StoreResult<Reply> {
        let path = if key.is_empty() {
            format!("/{}", self.config.bucket)
        } else {
            format!("/{}/{}", self.config.bucket, encode(key, true))
        };
        let mut pairs: Vec<(String, String)> = query
            .iter()
            .map(|(name, value)| (encode(name, false), encode(value, false)))
            .collect();
        pairs.sort();
        let query_string = pairs
            .iter()
            .map(|(n, v)| format!("{n}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let payload_hash = hex_sha256(&body);
        let (date, time) = timestamp();
        let canonical = format!(
            "{method}\n{path}\n{query_string}\nhost:{}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{time}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}",
            self.host
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.config.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{time}\n{scope}\n{}",
            hex_sha256(canonical.as_bytes())
        );
        let mut signing = hmac(
            format!("AWS4{}", self.config.secret_access_key).as_bytes(),
            &date,
        );
        for part in [self.config.region.as_str(), "s3", "aws4_request"] {
            signing = hmac(&signing, part);
        }
        let signature = hex::encode(hmac(&signing, &to_sign));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}",
            self.config.access_key_id
        );
        let url = if query_string.is_empty() {
            format!("{}{path}", self.config.endpoint)
        } else {
            format!("{}{path}?{query_string}", self.config.endpoint)
        };
        // Generous for large tables, short enough that a stalled request is
        // abandoned and retried or hedged.
        let timeout = Duration::from_secs(20 + body.len() as u64 / (256 << 10));
        let mut request = self
            .client
            .request(method, url)
            .timeout(timeout)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", time)
            .header("authorization", authorization);
        if let Some((start, end)) = range {
            request = request.header("range", format!("bytes={start}-{}", end.saturating_sub(1)));
        }
        let response = request.body(body).send().await.map_err(unavailable)?;
        let status = response.status();
        let body = response.bytes().await.map_err(unavailable)?;
        if status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
        {
            return Err(StoreError::Unavailable(format!("{status}")));
        }
        Ok(Reply { status, body })
    }

    fn refused(&self, what: &str, reply: &Reply) -> StoreError {
        let text = String::from_utf8_lossy(&reply.body);
        let code = tag(&text, "Code").unwrap_or("");
        let message = tag(&text, "Message").unwrap_or("");
        StoreError::Fatal(format!(
            "{what} was refused: {} {code} {message}",
            reply.status
        ))
    }

    async fn delete_version(&self, key: &str, version: &str) -> StoreResult<()> {
        // An unversioned bucket reports the version "null".
        let query: &[(&str, &str)] = if version == "null" {
            &[]
        } else {
            &[("versionId", version)]
        };
        let reply = self
            .send(Method::DELETE, key, query, None, Bytes::new())
            .await?;
        if reply.status.is_success() || reply.status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(self.refused("DELETE", &reply))
        }
    }
}

/// Backblaze's upload protocol: authorize, obtain an upload URL, post files
/// to it. A URL names one storage pod and serves one upload at a time.
///
/// Pods differ: measured medians for the same 4 KiB upload ranged from 67 ms
/// to over 900 ms at one moment. So this keeps the URLs that prove fast,
/// drops the slow ones, and keeps trying new ones. Each upload takes either
/// the fastest known URL or an untried one, alternately, so that two racing
/// attempts are one of each.
struct B2Uploader {
    client: reqwest::Client,
    key_id: String,
    key: String,
    bucket: String,
    session: tokio::sync::Mutex<Option<B2Session>>,
    pool: parking_lot::Mutex<B2Pool>,
}

#[derive(Clone)]
struct B2Session {
    api_url: String,
    token: String,
    bucket_id: String,
}

struct B2Target {
    url: String,
    token: String,
    /// Smoothed latency of small uploads through this URL; 0 when untried.
    millis: f64,
}

#[derive(Default)]
struct B2Pool {
    /// Fastest first.
    proven: Vec<B2Target>,
    untried: Vec<B2Target>,
    explore: bool,
    fetching: usize,
}

/// Uploads up to this size measure a pod; larger ones mostly measure bandwidth.
const B2_SAMPLE_BYTES: usize = 256 << 10;
const B2_PROVEN: usize = 256;
const B2_UNTRIED: usize = 4;

impl B2Uploader {
    async fn json(
        &self,
        request: reqwest::RequestBuilder,
    ) -> StoreResult<(StatusCode, serde_json::Value)> {
        let response = request.send().await.map_err(unavailable)?;
        let status = response.status();
        let body = response.bytes().await.map_err(unavailable)?;
        if status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
        {
            return Err(StoreError::Unavailable(format!(
                "Backblaze answered {status}"
            )));
        }
        Ok((
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        ))
    }

    fn refused(what: &str, status: StatusCode, body: &serde_json::Value) -> StoreError {
        let field = |name: &str| body[name].as_str().unwrap_or("").to_owned();
        StoreError::Fatal(format!(
            "Backblaze refused {what}: {status} {} {}",
            field("code"),
            field("message")
        ))
    }

    async fn session(&self, renew: bool) -> StoreResult<B2Session> {
        let mut session = self.session.lock().await;
        if let (Some(current), false) = (&*session, renew) {
            return Ok(current.clone());
        }
        let (status, body) = self
            .json(
                self.client
                    .get("https://api.backblazeb2.com/b2api/v4/b2_authorize_account")
                    .timeout(Duration::from_secs(20))
                    .basic_auth(&self.key_id, Some(&self.key)),
            )
            .await?;
        if !status.is_success() {
            return Err(Self::refused("the application key", status, &body));
        }
        let api = &body["apiInfo"]["storageApi"];
        let text = |value: &serde_json::Value| value.as_str().map(str::to_owned);
        let (Some(api_url), Some(token)) =
            (text(&api["apiUrl"]), text(&body["authorizationToken"]))
        else {
            return Err(StoreError::Fatal(
                "unreadable Backblaze authorization".into(),
            ));
        };
        // A key restricted to buckets names them; otherwise look ours up.
        let allowed = api["allowed"]["buckets"].as_array().into_iter().flatten();
        let named = allowed
            .filter(|bucket| bucket["name"].as_str() == Some(self.bucket.as_str()))
            .find_map(|bucket| text(&bucket["id"]));
        let bucket_id = match named {
            Some(id) => id,
            None => {
                let lookup =
                    serde_json::json!({"accountId": body["accountId"], "bucketName": self.bucket});
                let (status, listed) = self
                    .json(
                        self.client
                            .post(format!("{api_url}/b2api/v4/b2_list_buckets"))
                            .timeout(Duration::from_secs(20))
                            .header("authorization", &token)
                            .body(lookup.to_string()),
                    )
                    .await?;
                match text(&listed["buckets"][0]["bucketId"]) {
                    Some(id) if status.is_success() => id,
                    _ => return Err(Self::refused("the bucket lookup", status, &listed)),
                }
            }
        };
        let fresh = B2Session {
            api_url,
            token,
            bucket_id,
        };
        *session = Some(fresh.clone());
        Ok(fresh)
    }

    async fn new_target(&self) -> StoreResult<B2Target> {
        for renew in [false, true] {
            let session = self.session(renew).await?;
            let (status, body) = self
                .json(
                    self.client
                        .post(format!("{}/b2api/v4/b2_get_upload_url", session.api_url))
                        .timeout(Duration::from_secs(20))
                        .header("authorization", &session.token)
                        .body(serde_json::json!({"bucketId": session.bucket_id}).to_string()),
                )
                .await?;
            match (
                body["uploadUrl"].as_str(),
                body["authorizationToken"].as_str(),
            ) {
                (Some(url), Some(token)) if status.is_success() => {
                    return Ok(B2Target {
                        url: url.to_owned(),
                        token: token.to_owned(),
                        millis: 0.0,
                    });
                }
                // The account token expired; authorize again, once.
                _ if status == StatusCode::UNAUTHORIZED && !renew => continue,
                _ => return Err(Self::refused("an upload URL", status, &body)),
            }
        }
        unreachable!("the second pass returns")
    }

    /// The next URL to use, topping up the untried ones in the background.
    async fn target(self: &Arc<Self>) -> StoreResult<B2Target> {
        let (taken, fetch) = {
            let mut pool = self.pool.lock();
            pool.explore = !pool.explore;
            let taken = if pool.explore && !pool.untried.is_empty() {
                pool.untried.pop()
            } else if !pool.proven.is_empty() {
                Some(pool.proven.remove(0))
            } else {
                pool.untried.pop()
            };
            let fetch = B2_UNTRIED.saturating_sub(pool.untried.len() + pool.fetching);
            pool.fetching += fetch;
            (taken, fetch)
        };
        for _ in 0..fetch {
            let uploader = self.clone();
            tokio::spawn(async move {
                let fetched = uploader.new_target().await;
                let mut pool = uploader.pool.lock();
                pool.fetching -= 1;
                if let Ok(target) = fetched {
                    pool.untried.push(target);
                }
            });
        }
        match taken {
            Some(target) => Ok(target),
            None => self.new_target().await,
        }
    }

    /// Keep a URL that just worked, ranked by how fast it has been.
    fn keep(&self, mut target: B2Target, elapsed: Duration, sampled: bool) {
        if sampled {
            let millis = elapsed.as_secs_f64() * 1000.0;
            target.millis = if target.millis == 0.0 {
                millis
            } else {
                0.7 * target.millis + 0.3 * millis
            };
        }
        let mut pool = self.pool.lock();
        // A pod much slower than the best known is not worth another upload.
        let best = pool.proven.first().map_or(f64::MAX, |t| t.millis);
        if target.millis > 150.0 && target.millis > 2.5 * best {
            return;
        }
        let at = pool
            .proven
            .partition_point(|other| other.millis <= target.millis);
        pool.proven.insert(at, target);
        pool.proven.truncate(B2_PROVEN);
    }

    async fn upload(self: &Arc<Self>, key: &str, body: Bytes) -> StoreResult<()> {
        use sha1::Digest as _;
        let target = self.target().await?;
        let sampled = body.len() <= B2_SAMPLE_BYTES;
        let timeout = Duration::from_secs(20 + body.len() as u64 / (256 << 10));
        let started = std::time::Instant::now();
        let (status, reply) = self
            .json(
                self.client
                    .post(&target.url)
                    .timeout(timeout)
                    .header("authorization", &target.token)
                    .header("x-bz-file-name", encode(key, true))
                    .header("content-type", "application/octet-stream")
                    .header("x-bz-content-sha1", hex::encode(sha1::Sha1::digest(&body)))
                    .body(body),
            )
            .await?;
        if status.is_success() {
            self.keep(target, started.elapsed(), sampled);
            return Ok(());
        }
        // An expired upload token is routine: the next attempt gets a new URL.
        if status == StatusCode::UNAUTHORIZED {
            return Err(StoreError::Unavailable(
                "the Backblaze upload URL expired".into(),
            ));
        }
        Err(Self::refused("an upload", status, &reply))
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()> {
        if let Some(b2) = &self.b2 {
            return b2.upload(key, body).await;
        }
        let reply = self.send(Method::PUT, key, &[], None, body).await?;
        if reply.status.is_success() {
            Ok(())
        } else {
            Err(self.refused("PUT", &reply))
        }
    }

    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        let reply = self.send(Method::GET, key, &[], None, Bytes::new()).await?;
        match reply.status {
            status if status.is_success() => Ok(Some(reply.body)),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(self.refused("GET", &reply)),
        }
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
        if start >= end {
            return Ok(self.exists(key).await?.then(Bytes::new));
        }
        let reply = self
            .send(Method::GET, key, &[], Some((start, end)), Bytes::new())
            .await?;
        match reply.status {
            status if status.is_success() => Ok(Some(reply.body)),
            StatusCode::NOT_FOUND => Ok(None),
            // The range begins past the end of the object.
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Some(Bytes::new())),
            _ => Err(self.refused("GET", &reply)),
        }
    }

    async fn exists(&self, key: &str) -> StoreResult<bool> {
        let reply = self
            .send(Method::HEAD, key, &[], None, Bytes::new())
            .await?;
        match reply.status {
            status if status.is_success() => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(self.refused("HEAD", &reply)),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>> {
        let mut out = Vec::new();
        let mut after = after.map(str::to_owned);
        while out.len() < limit {
            let page = (limit - out.len()).min(1000).to_string();
            let mut query = vec![
                ("list-type", "2"),
                ("prefix", prefix),
                ("max-keys", page.as_str()),
            ];
            if let Some(after) = &after {
                query.push(("start-after", after));
            }
            let reply = self
                .send(Method::GET, "", &query, None, Bytes::new())
                .await?;
            if !reply.status.is_success() {
                return Err(self.refused("LIST", &reply));
            }
            let xml = String::from_utf8_lossy(&reply.body);
            let before = out.len();
            for item in tags(&xml, "Contents") {
                let (Some(key), Some(size)) = (tag(item, "Key"), tag(item, "Size")) else {
                    return Err(StoreError::Fatal("unreadable listing".into()));
                };
                out.push(Listed {
                    key: unescape(key),
                    size: size.trim().parse().unwrap_or(0),
                });
            }
            if tag(&xml, "IsTruncated").map(str::trim) != Some("true") || out.len() == before {
                break;
            }
            after = out.last().map(|item| item.key.clone());
        }
        Ok(out)
    }

    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        // `0` follows `/`, so this skips everything below the last directory.
        let mut start = after.map(|after| format!("{prefix}{after}0"));
        while out.len() < limit {
            let page = (limit - out.len()).min(1000).to_string();
            let mut query = vec![
                ("list-type", "2"),
                ("prefix", prefix),
                ("delimiter", "/"),
                ("max-keys", page.as_str()),
            ];
            if let Some(start) = &start {
                query.push(("start-after", start));
            }
            let reply = self
                .send(Method::GET, "", &query, None, Bytes::new())
                .await?;
            if !reply.status.is_success() {
                return Err(self.refused("LIST", &reply));
            }
            let xml = String::from_utf8_lossy(&reply.body);
            let before = out.len();
            for group in tags(&xml, "CommonPrefixes") {
                if let Some(dir) = tag(group, "Prefix").map(unescape).and_then(|full| {
                    full.strip_prefix(prefix)
                        .map(|d| d.trim_end_matches('/').to_owned())
                }) {
                    out.push(dir);
                }
            }
            if tag(&xml, "IsTruncated").map(str::trim) != Some("true") || out.len() == before {
                break;
            }
            start = out.last().map(|dir| format!("{prefix}{dir}0"));
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        // On a versioned bucket this hides the key behind a delete marker,
        // which is all a reader needs. `purge` reclaims the bytes.
        let reply = self
            .send(Method::DELETE, key, &[], None, Bytes::new())
            .await?;
        if reply.status.is_success() || reply.status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(self.refused("DELETE", &reply))
        }
    }

    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        let mut removed = 0;
        let mut marker: Option<(String, String)> = None;
        loop {
            let mut query = vec![("versions", ""), ("prefix", prefix), ("max-keys", "1000")];
            if let Some((key, version)) = &marker {
                query.push(("key-marker", key));
                query.push(("version-id-marker", version));
            }
            let reply = self
                .send(Method::GET, "", &query, None, Bytes::new())
                .await?;
            if !reply.status.is_success() {
                return Err(self.refused("LIST versions", &reply));
            }
            let xml = String::from_utf8_lossy(&reply.body);
            let mut past_end = false;
            let mut select = |element: &str| {
                let mut chosen = Vec::new();
                for item in tags(&xml, element) {
                    let (Some(key), Some(version)) = (tag(item, "Key"), tag(item, "VersionId"))
                    else {
                        continue;
                    };
                    let key = unescape(key);
                    if end.is_some_and(|end| key.as_str() >= end) {
                        past_end = true;
                    } else if doomed(&key) {
                        chosen.push((key, unescape(version)));
                    }
                }
                chosen
            };
            let versions = select("Version");
            let markers = select("DeleteMarker");
            // Data first, markers last: removing a marker while data remains
            // would make a deleted key readable again.
            for batch in [&versions, &markers] {
                for chunk in batch.chunks(32) {
                    futures::future::try_join_all(
                        chunk
                            .iter()
                            .map(|(key, version)| self.delete_version(key, version)),
                    )
                    .await?;
                }
            }
            let mut keys: Vec<&String> = versions
                .iter()
                .chain(&markers)
                .map(|(key, _)| key)
                .collect();
            keys.sort();
            keys.dedup();
            removed += keys.len();
            let truncated = tag(&xml, "IsTruncated").map(str::trim) == Some("true");
            let next = tag(&xml, "NextKeyMarker")
                .map(unescape)
                .zip(tag(&xml, "NextVersionIdMarker").map(unescape));
            match next {
                Some(next) if truncated && !past_end => marker = Some(next),
                _ => return Ok(removed),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listings_parse() {
        let xml = "<ListBucketResult>\n <Contents>\n  <Key>a/b&amp;c</Key><Size>12</Size>\n </Contents>\n \
                   <Contents><Key>a/d</Key><Size>0</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
        let keys: Vec<(String, &str)> = tags(xml, "Contents")
            .map(|c| (unescape(tag(c, "Key").unwrap()), tag(c, "Size").unwrap()))
            .collect();
        assert_eq!(keys, [("a/b&c".to_owned(), "12"), ("a/d".to_owned(), "0")]);
        assert_eq!(tag(xml, "IsTruncated"), Some("false"));
        assert_eq!(tag(xml, "Missing"), None);
    }

    #[test]
    fn encoding_and_dates() {
        assert_eq!(encode("db/a b~c", true), "db/a%20b~c");
        assert_eq!(encode("a/b", false), "a%2Fb");
        let (date, time) = timestamp();
        assert!(
            date.len() == 8
                && time.len() == 16
                && time.starts_with(&date)
                && date.as_str() > "20260101"
        );
    }

    /// The published AWS Signature Version 4 key-derivation example.
    #[test]
    fn signing_key_matches_the_reference_vector() {
        let mut key = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215");
        for part in ["us-east-1", "iam", "aws4_request"] {
            key = hmac(&key, part);
        }
        assert_eq!(
            hex::encode(key),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }
}
