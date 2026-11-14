//! Any S3-compatible bucket: Amazon S3, Backblaze B2, Cloudflare R2, MinIO,
//! and stores that keep no versions, such as Garage.
//!
//! One code path for all of them. It uses only what Backblaze B2 implements:
//! PutObject, GetObject (with ranges), HeadObject, DeleteObject,
//! DeleteObjects, ListObjectsV2 and ListObjectVersions. In particular it
//! never sends `If-Match` or `If-None-Match`, which B2 rejects.
//!
//! Requests are signed with AWS Signature Version 4 and addressed path-style.
//!
//! A purge does without two of those where a store does not have them: it
//! removes versions one at a time where DeleteObjects is refused, and
//! removes keys where there is no ListObjectVersions, since such a store
//! keeps no versions.
//!
//! One operation has a second wire form. On Backblaze, PutObject goes through
//! a gateway that measured two to three times slower than B2's own upload
//! call, with a far longer tail, and an upload is on the path of every
//! commit. So against a Backblaze endpoint `put` uses `b2_upload_file`
//! with the same key, the same bytes and the same credentials. The object
//! that results is the same object; nothing above this file can tell.
use super::{Listed, ObjectStore, StoreError, StoreResult};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    /// Whether the bucket takes one request that removes many versions. A
    /// bucket that has refused it is not asked again.
    batches: AtomicBool,
    /// Whether the bucket lists its versions. One that answers that it has
    /// no such request is taken to keep none: what a purge removes there it
    /// removes by key, and it is not asked again.
    versions: AtomicBool,
}

/// A failed exchange. The URL names the bucket and the key, and neither
/// belongs in a message that may reach a client.
fn network(error: reqwest::Error) -> StoreError {
    let error = error.without_url();
    if tracing::enabled!(tracing::Level::DEBUG) {
        // What went wrong is in the causes, which a client is not told.
        let mut causes = String::new();
        let mut cause = std::error::Error::source(&error);
        while let Some(inner) = cause {
            causes = format!("{causes}: {inner}");
            cause = inner.source();
        }
        tracing::debug!("a storage request failed: {error}{causes}");
    }
    StoreError::Unavailable(error.to_string())
}

/// How long to wait for the first byte of an answer, and for each further
/// part of it.
const PATIENCE: Duration = Duration::from_secs(20);
/// How often a purge sends one of its requests again.
const PURGE_AGAIN: u32 = 2;
/// How many versions one request may remove, by the S3 API.
const BATCH: usize = 1000;

/// The whole body of a response. Its size is not known in advance, so the
/// limit is on each wait for more of it rather than on the transfer.
async fn body_of(mut response: reqwest::Response) -> StoreResult<Bytes> {
    let mut whole = Bytes::new();
    let mut joined: Option<BytesMut> = None;
    loop {
        let part = match tokio::time::timeout(PATIENCE, response.chunk()).await {
            Ok(part) => part.map_err(network)?,
            Err(_) => return Err(StoreError::Unavailable("a storage answer stalled".into())),
        };
        match part {
            Some(part) if whole.is_empty() && joined.is_none() => whole = part,
            Some(part) => joined
                .get_or_insert_with(|| BytesMut::from(&whole[..]))
                .extend_from_slice(&part),
            None => return Ok(joined.map_or(whole, BytesMut::freeze)),
        }
    }
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

/// The body of a request that removes these versions, if XML can name them
/// all: it has no way to write most control characters. A version with no
/// name is a key on a bucket that keeps no versions.
fn removal_of(versions: &[(String, String)]) -> Option<String> {
    let mut body = String::from("<Delete><Quiet>true</Quiet>");
    for (key, version) in versions {
        let unwritable = |c: char| c < ' ' || matches!(c, '\u{fffe}' | '\u{ffff}');
        if key.chars().chain(version.chars()).any(unwritable) {
            return None;
        }
        body.push_str("<Object><Key>");
        escape_into(&mut body, key);
        body.push_str("</Key>");
        if !version.is_empty() {
            body.push_str("<VersionId>");
            escape_into(&mut body, version);
            body.push_str("</VersionId>");
        }
        body.push_str("</Object>");
    }
    body.push_str("</Delete>");
    Some(body)
}

fn escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
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

impl Reply {
    /// Whether this says that the object is absent, and not the bucket.
    fn absent(&self) -> bool {
        self.status == StatusCode::NOT_FOUND
            && tag(&String::from_utf8_lossy(&self.body), "Code") != Some("NoSuchBucket")
    }
}

/// Where a listing continues.
enum Cursor {
    After(Option<String>),
    Token(String),
}

/// Where the page after `xml` begins, given the place its last entry puts
/// that; `None` when the listing is complete. A page may be cut short before
/// its first entry, and is then continued by the token it carries.
fn following(xml: &str, last: Option<String>) -> StoreResult<Option<Cursor>> {
    if tag(xml, "IsTruncated").map(str::trim) != Some("true") {
        return Ok(None);
    }
    match (last, tag(xml, "NextContinuationToken")) {
        (Some(last), _) => Ok(Some(Cursor::After(Some(last)))),
        (None, Some(token)) => Ok(Some(Cursor::Token(unescape(token)))),
        (None, None) => Err(StoreError::Unavailable(
            "a listing was cut short with no way to continue".into(),
        )),
    }
}

/// What a purge asks and is not answered, it asks again. Listing and
/// removing are both safe to repeat, and a purge that fails leaves what it
/// has not removed to the next collection.
async fn again<T, F>(mut ask: impl FnMut() -> F) -> StoreResult<T>
where
    F: Future<Output = StoreResult<T>>,
{
    let mut failures = 0;
    loop {
        match ask().await {
            Err(StoreError::Unavailable(_)) if failures < PURGE_AGAIN => {
                failures += 1;
                tokio::time::sleep(Duration::from_millis(25 << failures)).await;
            }
            answered => return answered,
        }
    }
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
            batches: AtomicBool::new(true),
            versions: AtomicBool::new(true),
        })
    }

    /// Whether this endpoint is Backblaze B2.
    pub fn is_backblaze(&self) -> bool {
        self.host.ends_with(".backblazeb2.com")
    }

    /// Whether the bucket still takes one request that removes many
    /// versions: not once it has refused one.
    pub fn removes_many(&self) -> bool {
        self.batches.load(Ordering::Relaxed)
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
        self.exchange(method, key, query, range, body, false).await
    }

    /// `send`, and with `checked` the request also carries a checksum of
    /// its body, which a request that removes many versions must.
    async fn exchange(
        &self,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
        range: Option<(u64, u64)>,
        body: Bytes,
        checked: bool,
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
        let digest = Sha256::digest(&body);
        let payload_hash = hex::encode(digest);
        // Every x-amz header that is sent is signed, in order of its name.
        let checksum = checked.then(|| base64::engine::general_purpose::STANDARD.encode(digest));
        let (checksum_line, signed) = match &checksum {
            Some(checksum) => (
                format!("x-amz-checksum-sha256:{checksum}\n"),
                "host;x-amz-checksum-sha256;x-amz-content-sha256;x-amz-date",
            ),
            None => (String::new(), "host;x-amz-content-sha256;x-amz-date"),
        };
        let (date, time) = timestamp();
        let canonical = format!(
            "{method}\n{path}\n{query_string}\nhost:{}\n{checksum_line}x-amz-content-sha256:{payload_hash}\nx-amz-date:{time}\n\n{signed}\n{payload_hash}",
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
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            self.config.access_key_id
        );
        let url = if query_string.is_empty() {
            format!("{}{path}", self.config.endpoint)
        } else {
            format!("{}{path}?{query_string}", self.config.endpoint)
        };
        // Generous for a large upload, short enough that a stalled request
        // is abandoned and retried or hedged.
        let patience = PATIENCE + Duration::from_secs(body.len() as u64 / (256 << 10));
        let mut request = self
            .client
            .request(method, url)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", time)
            .header("authorization", authorization);
        if let Some((start, end)) = range {
            request = request.header("range", format!("bytes={start}-{}", end.saturating_sub(1)));
        }
        if let Some(checksum) = checksum {
            request = request.header("x-amz-checksum-sha256", checksum);
        }
        let response = match tokio::time::timeout(patience, request.body(body).send()).await {
            Ok(response) => response.map_err(network)?,
            Err(_) => return Err(StoreError::Unavailable("a storage request stalled".into())),
        };
        let status = response.status();
        let body = body_of(response).await?;
        // S3 reports a connection it gave up on as a client error.
        let gave_up = status == StatusCode::BAD_REQUEST
            && tag(&String::from_utf8_lossy(&body), "Code") == Some("RequestTimeout");
        // A request the store has not implemented is refused, not delayed:
        // the next attempt would be answered the same.
        if (status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED)
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
            || gave_up
        {
            return Err(StoreError::Unavailable(format!("{status}")));
        }
        Ok(Reply { status, body })
    }

    /// One page of ListObjectsV2, as text.
    async fn page(
        &self,
        prefix: &str,
        dirs: bool,
        cursor: &Cursor,
        most: usize,
    ) -> StoreResult<String> {
        let most = most.min(1000).to_string();
        let mut query = vec![
            ("list-type", "2"),
            ("prefix", prefix),
            ("max-keys", most.as_str()),
        ];
        if dirs {
            query.push(("delimiter", "/"));
        }
        match cursor {
            Cursor::After(Some(after)) => query.push(("start-after", after)),
            Cursor::After(None) => {}
            Cursor::Token(token) => query.push(("continuation-token", token)),
        }
        let reply = self
            .send(Method::GET, "", &query, None, Bytes::new())
            .await?;
        if !reply.status.is_success() {
            return Err(self.refused("LIST", &reply));
        }
        let xml = String::from_utf8_lossy(&reply.body).into_owned();
        // Something between here and the bucket may answer in its place.
        if !xml.contains("<ListBucketResult") {
            return Err(StoreError::Unavailable(
                "a listing was answered by something other than the bucket".into(),
            ));
        }
        Ok(xml)
    }

    /// A purge on a bucket that keeps no versions. A key that is removed
    /// there is gone, so the keys are listed and removed by name.
    async fn purge_keys(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        let mut removed = 0;
        let mut cursor = Cursor::After(None);
        loop {
            let xml = again(|| self.page(prefix, false, &cursor, 1000)).await?;
            let mut chosen = Vec::new();
            let mut last = None;
            let mut past_end = false;
            for item in tags(&xml, "Contents") {
                let Some(key) = tag(item, "Key").map(unescape) else {
                    return Err(StoreError::Fatal("unreadable listing".into()));
                };
                if end.is_some_and(|end| key.as_str() >= end) {
                    past_end = true;
                } else if doomed(&key) {
                    chosen.push((key.clone(), String::new()));
                }
                last = Some(key);
            }
            self.delete_versions(&chosen).await?;
            removed += chosen.len();
            match following(&xml, last)? {
                Some(next) if !past_end => cursor = next,
                _ => return Ok(removed),
            }
        }
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

    /// One request of a purge that has no body.
    async fn purging(
        &self,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
    ) -> StoreResult<Reply> {
        again(|| self.exchange(method.clone(), key, query, None, Bytes::new(), false)).await
    }

    /// Remove these exact versions. One with no name is a key on a bucket
    /// that keeps no versions, and is removed as that.
    ///
    /// One request removes up to a thousand of them. A bucket that refuses
    /// it gets a request for each version, 32 at a time. That was the only
    /// form until 64 databases collecting at once were measured on
    /// Backblaze: the server opened 1,200 sockets, some were reset, and
    /// about one purge in four failed.
    async fn delete_versions(&self, versions: &[(String, String)]) -> StoreResult<()> {
        for chunk in versions.chunks(BATCH) {
            if self.batches.load(Ordering::Relaxed)
                && let Some(body) = removal_of(chunk)
            {
                if self.delete_batch(body).await? {
                    continue;
                }
                if self.batches.swap(false, Ordering::Relaxed) {
                    tracing::warn!(
                        "the bucket refused a request to remove many versions; each is removed with a request of its own"
                    );
                }
            }
            for few in chunk.chunks(32) {
                futures::future::try_join_all(few.iter().map(|(key, version)| async move {
                    // A version written before the bucket kept versions is
                    // named "null". It is removed by that name; without one,
                    // a bucket that keeps versions would only add a delete
                    // marker.
                    let query = [("versionId", version.as_str())];
                    let named = usize::from(!version.is_empty());
                    let reply = self.purging(Method::DELETE, key, &query[..named]).await?;
                    if reply.status.is_success() || reply.absent() {
                        Ok(())
                    } else {
                        Err(self.refused("DELETE", &reply))
                    }
                }))
                .await?;
            }
        }
        Ok(())
    }

    /// Send one request that removes many versions. `false` says that the
    /// bucket does not take such a request.
    async fn delete_batch(&self, body: String) -> StoreResult<bool> {
        let body = Bytes::from(body);
        // A version that the bucket says it did not remove is asked for
        // again with the others, like a request that was not answered.
        again(|| async {
            let query = [("delete", "")];
            let sent = self.exchange(Method::POST, "", &query, None, body.clone(), true);
            let reply = sent.await?;
            let text = String::from_utf8_lossy(&reply.body);
            if !reply.status.is_success() || !text.contains("<DeleteResult") {
                return Ok(false);
            }
            // The answer names only what was not removed.
            for error in tags(&text, "Error") {
                match (tag(error, "Key"), tag(error, "Code")) {
                    // A version that is not there was removed before, by
                    // this request when its answer was lost.
                    (Some(_), Some("NoSuchVersion" | "NoSuchKey")) => {}
                    (Some(_), code) => {
                        return Err(StoreError::Unavailable(format!(
                            "a version was not removed: {}",
                            code.unwrap_or("no reason given")
                        )));
                    }
                    // An error that names no key is about the request.
                    (None, _) => return Ok(false),
                }
            }
            Ok(true)
        })
        .await
    }
}

/// Backblaze's upload protocol: authorize, obtain an upload URL, post files
/// to it. A URL names one storage pod and serves one upload at a time.
///
/// Pods differ: measured medians for the same 4 KiB upload ranged from 67 ms
/// to over 900 ms at one moment, and of ten new pods four were fast. So
/// this keeps the URLs that prove fast, drops the slow ones, and keeps
/// trying new ones.
///
/// A new pod costs a call for its URL, a name lookup and a connection. A
/// first upload takes the fastest URL known and a new one only when none is
/// free. Spares, alternately, try a new pod or take a known one that first
/// uploads are not expected to need. Few new pods are tried at a time
/// however many uploads are under way: one new pod for every commit of 64
/// busy databases was 160 pods a second, and from a desktop one such upload
/// in fourteen then failed after five seconds without a connection and the
/// others took 0.8 s at the median.
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
    /// Smoothed latency of small uploads through this URL; 0 when unmeasured.
    millis: f64,
    issued: Instant,
}

impl B2Target {
    /// Backblaze honours an upload URL for 24 hours.
    fn usable(&self) -> bool {
        self.issued.elapsed() < Duration::from_secs(23 * 3600)
    }
}

/// What an upload is to the one who asked for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Want {
    /// It must be sent: the only upload of its object, or the first of several.
    First,
    /// A claim, which is sent once: the fastest pod known.
    Once,
    /// A duplicate beside a first upload, sent only with a URL to spare.
    Spare,
}

enum Taken {
    Url(B2Target),
    /// None is free: a new one has to be fetched.
    Fetch,
    /// Nothing to spare.
    Nothing,
}

#[derive(Default)]
struct B2Pool {
    /// Fastest first.
    proven: Vec<B2Target>,
    untried: Vec<B2Target>,
    explore: bool,
    fetching: usize,
    /// Uploads under way through URLs of unknown speed.
    exploring: usize,
    /// First uploads under way, and the most there were at once in the
    /// stretch of time before this one and in this one.
    firsts: usize,
    peak: [usize; 2],
    peak_from: Option<Instant>,
}

/// Uploads up to this size measure a pod; larger ones mostly measure bandwidth.
const B2_SAMPLE_BYTES: usize = 256 << 10;
const B2_PROVEN: usize = 256;
const B2_UNTRIED: usize = 4;
/// New pods on trial at once.
const B2_EXPLORING: usize = 8;
/// What a first upload to a new pod took at the median, its connection
/// included, when few were tried at a time.
const B2_SLOW_MS: f64 = 400.0;
/// How long the number of first uploads at once is remembered, at least.
const B2_PEAK: Duration = Duration::from_secs(5);

impl B2Pool {
    /// The URL for an upload, and whether its speed is unknown.
    fn take(&mut self, want: Want, now: Instant) -> (Taken, bool) {
        self.proven.retain(B2Target::usable);
        self.untried.retain(B2Target::usable);
        let taken = match want {
            Want::First | Want::Once => {
                if want == Want::First {
                    self.first_begins(now);
                }
                if self.proven.is_empty() {
                    self.untried.pop()
                } else {
                    Some(self.proven.remove(0))
                }
            }
            Want::Spare => {
                self.explore = !self.explore;
                let new = self.exploring < B2_EXPLORING && !self.untried.is_empty();
                // The fastest stay for the first uploads that are not under
                // way now and were a moment ago.
                let kept = self.peak[0].max(self.peak[1]).saturating_sub(self.firsts);
                if self.explore && new {
                    self.untried.pop()
                } else if self.proven.len() > kept {
                    Some(self.proven.remove(kept))
                } else if new {
                    self.untried.pop()
                } else {
                    return (Taken::Nothing, false);
                }
            }
        };
        let unknown = taken.as_ref().is_none_or(|target| target.millis == 0.0);
        if unknown {
            self.exploring += 1;
        }
        (taken.map_or(Taken::Fetch, Taken::Url), unknown)
    }

    fn first_begins(&mut self, now: Instant) {
        let age = self.peak_from.map(|from| now.duration_since(from));
        if age.is_none_or(|age| age >= B2_PEAK) {
            let before = if age.is_some_and(|age| age < 2 * B2_PEAK) {
                self.peak[1]
            } else {
                0
            };
            self.peak = [before, 0];
            self.peak_from = Some(now);
        }
        self.firsts += 1;
        self.peak[1] = self.peak[1].max(self.firsts);
    }

    /// An upload that `take` gave a URL to, or told to fetch one, has ended.
    fn ended(&mut self, want: Want, unknown: bool) {
        if want == Want::First {
            self.firsts -= 1;
        }
        if unknown {
            self.exploring -= 1;
        }
    }
}

/// Tells the pool that an upload has ended, however it ended.
struct Lease {
    uploader: Arc<B2Uploader>,
    want: Want,
    unknown: bool,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.uploader.pool.lock().ended(self.want, self.unknown);
    }
}

impl B2Uploader {
    async fn json(
        &self,
        request: reqwest::RequestBuilder,
    ) -> StoreResult<(StatusCode, serde_json::Value)> {
        let response = request.send().await.map_err(network)?;
        let status = response.status();
        let body = response.bytes().await.map_err(network)?;
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
                        issued: Instant::now(),
                    });
                }
                // The account token expired; authorize again, once.
                _ if status == StatusCode::UNAUTHORIZED && !renew => continue,
                _ => return Err(Self::refused("an upload URL", status, &body)),
            }
        }
        unreachable!("the second pass returns")
    }

    /// The URL for an upload, or none for a spare that cannot have one.
    /// Tops up the untried ones in the background.
    async fn target(self: &Arc<Self>, want: Want) -> StoreResult<Option<(B2Target, Lease)>> {
        let (taken, unknown, fetch) = {
            let mut pool = self.pool.lock();
            let (taken, unknown) = pool.take(want, Instant::now());
            let fetch = B2_UNTRIED.saturating_sub(pool.untried.len() + pool.fetching);
            pool.fetching += fetch;
            (taken, unknown, fetch)
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
        let lease = |uploader: &Arc<Self>| Lease {
            uploader: uploader.clone(),
            want,
            unknown,
        };
        match taken {
            Taken::Url(target) => Ok(Some((target, lease(self)))),
            Taken::Fetch => {
                let lease = lease(self);
                Ok(Some((self.new_target().await?, lease)))
            }
            Taken::Nothing => Ok(None),
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
        // Only large uploads have gone through it: its speed is still unknown.
        if target.millis == 0.0 {
            if pool.untried.len() < 2 * B2_UNTRIED {
                pool.untried.push(target);
            }
            return;
        }
        // A pod that is slow even for a new one is not worth another upload.
        // The others stay, in order: the slower of them are used only when
        // that many uploads are under way, where the alternative is a pod
        // nothing is known of.
        let best = pool.proven.first().map_or(f64::MAX, |t| t.millis);
        if target.millis > B2_SLOW_MS && target.millis > 2.5 * best {
            return;
        }
        let at = pool
            .proven
            .partition_point(|other| other.millis <= target.millis);
        pool.proven.insert(at, target);
        pool.proven.truncate(B2_PROVEN);
    }

    /// `false`: a spare that was not sent.
    async fn upload(self: &Arc<Self>, key: &str, body: Bytes, want: Want) -> StoreResult<bool> {
        use sha1::Digest as _;
        let Some((target, _lease)) = self.target(want).await? else {
            return Ok(false);
        };
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
            return Ok(true);
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
            return b2.upload(key, body, Want::First).await.map(|_| ());
        }
        let reply = self.send(Method::PUT, key, &[], None, body).await?;
        if reply.status.is_success() {
            Ok(())
        } else {
            Err(self.refused("PUT", &reply))
        }
    }

    async fn put_once(&self, key: &str, body: Bytes) -> StoreResult<()> {
        match &self.b2 {
            // The fastest storage pod known; an untried one only if none is.
            Some(b2) => b2.upload(key, body, Want::Once).await.map(|_| ()),
            None => self.put(key, body).await,
        }
    }

    async fn put_spare(&self, key: &str, body: Bytes) -> StoreResult<bool> {
        match &self.b2 {
            Some(b2) => b2.upload(key, body, Want::Spare).await,
            None => self.put(key, body).await.map(|()| true),
        }
    }

    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        let reply = self.send(Method::GET, key, &[], None, Bytes::new()).await?;
        match reply.status {
            status if status.is_success() => Ok(Some(reply.body)),
            _ if reply.absent() => Ok(None),
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
            _ if reply.absent() => Ok(None),
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
        let mut cursor = Cursor::After(after.map(str::to_owned));
        while out.len() < limit {
            let xml = self.page(prefix, false, &cursor, limit - out.len()).await?;
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
            let last = out[before..].last().map(|item| item.key.clone());
            match following(&xml, last)? {
                Some(next) => cursor = next,
                None => break,
            }
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
        // `0` follows `/`, so this skips everything below a directory.
        let past = |dir: &str| format!("{prefix}{dir}0");
        let mut cursor = Cursor::After(after.map(past));
        while out.len() < limit {
            let xml = self.page(prefix, true, &cursor, limit - out.len()).await?;
            let before = out.len();
            for group in tags(&xml, "CommonPrefixes") {
                let dir = tag(group, "Prefix").map(unescape).and_then(|full| {
                    full.strip_prefix(prefix)
                        .map(|dir| dir.trim_end_matches('/').to_owned())
                });
                match dir {
                    Some(dir) => out.push(dir),
                    None => return Err(StoreError::Fatal("unreadable listing".into())),
                }
            }
            // The page may also hold keys that lie directly below the prefix.
            let last = out[before..]
                .last()
                .map(|dir| past(dir))
                .into_iter()
                .chain(tags(&xml, "Contents").filter_map(|item| tag(item, "Key").map(unescape)))
                .max();
            match following(&xml, last)? {
                Some(next) => cursor = next,
                None => break,
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        // On a versioned bucket this hides the key behind a delete marker,
        // which is all a reader needs. `purge` reclaims the bytes.
        let reply = self
            .send(Method::DELETE, key, &[], None, Bytes::new())
            .await?;
        if reply.status.is_success() || reply.absent() {
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
        if !self.versions.load(Ordering::Relaxed) {
            return self.purge_keys(prefix, end, doomed).await;
        }
        let mut removed = 0;
        let mut counted: Option<String> = None;
        let mut marker: Option<(String, String)> = None;
        // Delete markers whose key the listing has not yet moved past.
        let mut held: Vec<(String, String)> = Vec::new();
        loop {
            let mut query = vec![("versions", ""), ("prefix", prefix), ("max-keys", "1000")];
            if let Some((key, version)) = &marker {
                query.push(("key-marker", key));
                query.push(("version-id-marker", version));
            }
            let reply = self.purging(Method::GET, "", &query).await?;
            if reply.status == StatusCode::NOT_IMPLEMENTED {
                if self.versions.swap(false, Ordering::Relaxed) {
                    tracing::warn!(
                        "the bucket does not list versions and is taken to keep none; what is collected is removed by key"
                    );
                }
                return self.purge_keys(prefix, end, doomed).await;
            }
            if !reply.status.is_success() {
                return Err(self.refused("LIST versions", &reply));
            }
            let xml = String::from_utf8_lossy(&reply.body);
            if !xml.contains("<ListVersionsResult") {
                return Err(StoreError::Unavailable(
                    "a listing was answered by something other than the bucket".into(),
                ));
            }
            let mut past_end = false;
            let mut greatest: Option<String> = None;
            let mut select = |element: &str| {
                let mut chosen = Vec::new();
                for item in tags(&xml, element) {
                    let (Some(key), Some(version)) = (tag(item, "Key"), tag(item, "VersionId"))
                    else {
                        return Err(StoreError::Fatal("unreadable version listing".into()));
                    };
                    let key = unescape(key);
                    if greatest.as_ref().is_none_or(|greatest| *greatest < key) {
                        greatest = Some(key.clone());
                    }
                    if end.is_some_and(|end| key.as_str() >= end) {
                        past_end = true;
                    } else if doomed(&key) {
                        chosen.push((key, unescape(version)));
                    }
                }
                Ok(chosen)
            };
            let versions = select("Version")?;
            let markers = select("DeleteMarker")?;
            let mut keys: Vec<&String> = versions
                .iter()
                .chain(&markers)
                .map(|(key, _)| key)
                .collect();
            keys.sort();
            keys.dedup();
            removed += keys
                .iter()
                .filter(|key| Some(**key) != counted.as_ref())
                .count();
            if let Some(last) = keys.last() {
                counted = Some((*last).clone());
            }
            let truncated = tag(&xml, "IsTruncated").map(str::trim) == Some("true");
            let next = tag(&xml, "NextKeyMarker")
                .map(unescape)
                .zip(tag(&xml, "NextVersionIdMarker").map(unescape));
            let more = truncated && !past_end;
            if more && next.is_none() {
                return Err(StoreError::Unavailable(
                    "a listing was cut short with no way to continue".into(),
                ));
            }
            // Data first. A delete marker goes only when no data remains
            // under its key, or the deleted key would be readable again; and
            // the versions of the last key on a page may run on to the next.
            self.delete_versions(&versions).await?;
            held.extend(markers);
            let ready: Vec<(String, String)> = match (&greatest, more) {
                (Some(greatest), true) => {
                    let (ready, wait) = held.into_iter().partition(|(key, _)| key < greatest);
                    held = wait;
                    ready
                }
                (None, true) => Vec::new(),
                (_, false) => std::mem::take(&mut held),
            };
            self.delete_versions(&ready).await?;
            if !more {
                return Ok(removed);
            }
            marker = next;
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

    fn url(millis: f64) -> B2Target {
        B2Target {
            url: String::new(),
            token: String::new(),
            millis,
            issued: Instant::now(),
        }
    }

    fn speed(taken: Taken) -> Option<f64> {
        match taken {
            Taken::Url(target) => Some(target.millis),
            Taken::Fetch | Taken::Nothing => None,
        }
    }

    /// However many objects are uploaded at once, few uploads go to pods
    /// that have not been tried. A first upload is never refused for that;
    /// the spares are.
    #[test]
    fn few_new_pods_are_on_trial_at_once() {
        let now = Instant::now();
        let mut pool = B2Pool {
            proven: (1..=64).map(|i| url(i as f64)).collect(),
            untried: (0..64).map(|_| url(0.0)).collect(),
            ..B2Pool::default()
        };
        // 64 objects were uploaded at once a moment ago.
        for _ in 0..64 {
            pool.take(Want::First, now);
        }
        for i in 1..=64 {
            pool.ended(Want::First, false);
            pool.proven.push(url(i as f64));
        }
        // Now each begins again, with a spare.
        let (mut known, mut new) = (0, 0);
        for _ in 0..64 {
            assert!(speed(pool.take(Want::First, now).0).is_some_and(|ms| ms > 0.0));
            match pool.take(Want::Spare, now) {
                (Taken::Url(_), false) => known += 1,
                (Taken::Url(_), true) => new += 1,
                (Taken::Nothing, false) => {}
                _ => panic!("a spare never waits for a URL to be fetched"),
            }
        }
        // Every known URL went to a first upload or was kept for one.
        assert_eq!(
            (known, new, pool.exploring),
            (0, B2_EXPLORING, B2_EXPLORING)
        );
        // With no URL known, every first upload is on a new pod, and no
        // spare adds to them.
        let mut cold = B2Pool {
            untried: (0..2).map(|_| url(0.0)).collect(),
            ..B2Pool::default()
        };
        for i in 0..2 * B2_EXPLORING {
            let (taken, unknown) = cold.take(Want::First, now);
            assert!(unknown && matches!(taken, Taken::Fetch) == (i >= 2));
        }
        cold.untried.push(url(0.0));
        let refused =
            |pool: &mut B2Pool| matches!(pool.take(Want::Spare, now), (Taken::Nothing, false));
        assert!(refused(&mut cold));
        // As many as may be on trial still are: the spare waits for one more
        // to end, and then tries the next new pod.
        for _ in 0..B2_EXPLORING {
            cold.ended(Want::First, true);
        }
        assert!(refused(&mut cold));
        cold.ended(Want::First, true);
        assert!(matches!(cold.take(Want::Spare, now), (Taken::Url(_), true)));
        assert_eq!(
            (cold.firsts, cold.exploring),
            (B2_EXPLORING - 1, B2_EXPLORING)
        );
    }

    /// A spare takes a known URL only beyond those that first uploads were
    /// using a moment ago, and that memory fades.
    #[test]
    fn a_spare_leaves_the_fastest_urls_to_first_uploads() {
        let now = Instant::now();
        let mut pool = B2Pool {
            proven: (1..=6).map(|i| url(10.0 * i as f64)).collect(),
            ..B2Pool::default()
        };
        // Four objects at once: their first uploads take the four fastest.
        for fastest in [10.0, 20.0, 30.0, 40.0] {
            assert_eq!(speed(pool.take(Want::First, now).0), Some(fastest));
        }
        // All four are under way: a spare takes the next.
        pool.explore = true;
        assert_eq!(speed(pool.take(Want::Spare, now).0), Some(50.0));
        // They end and their URLs come back. Four first uploads may begin
        // again at any moment, so one spare can be served and no second.
        for fastest in [40.0, 30.0, 20.0, 10.0] {
            pool.ended(Want::First, false);
            pool.proven.insert(0, url(fastest));
        }
        assert_eq!(speed(pool.take(Want::Spare, now).0), Some(60.0));
        assert!(matches!(pool.take(Want::Spare, now), (Taken::Nothing, _)));
        // One begins: the fastest is its own, and three stay for the others.
        assert_eq!(speed(pool.take(Want::First, now).0), Some(10.0));
        assert!(matches!(pool.take(Want::Spare, now), (Taken::Nothing, _)));
        pool.ended(Want::First, false);
        pool.proven.insert(0, url(10.0));
        // Later, with one object at a time, its spare takes the second fastest.
        let later = now + 2 * B2_PEAK;
        assert_eq!(speed(pool.take(Want::First, later).0), Some(10.0));
        assert_eq!(speed(pool.take(Want::Spare, later).0), Some(20.0));
        assert_eq!((pool.firsts, pool.exploring), (1, 0));
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
