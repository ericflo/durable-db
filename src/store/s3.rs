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
use md5::Md5;
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
    reads: Arc<Gate>,
    lists: Arc<Gate>,
    changes: Arc<Gate>,
    /// Whether the bucket takes one request that removes many versions. A
    /// bucket that has refused it is not asked again.
    batches: AtomicBool,
    /// Whether the bucket lists its versions. One that answers that it has
    /// no such request is taken to keep none: what a purge removes there it
    /// removes by key, and it is not asked again.
    versions: AtomicBool,
}

/// What keeps a server from pressing a store that refuses it. Backblaze
/// answered 503 to every upload for five and a half minutes, and in the first
/// half minute was sent 28,000 more, each in place of one it had refused.
///
/// One busy part of a store refuses a request and the next one, sent
/// elsewhere, is taken: nothing changes for that. After `REFUSALS` in a row
/// with none taken, requests fail here without being sent. When the wait is
/// over one request is sent to try the store, and the others go on failing
/// until it is answered: taken, and all are sent again; refused, and the wait
/// is doubled, to four seconds at most or to what the store asks for. One
/// that has had that long and no answer is not waited for: another is tried.
/// A request that fails unsent has stored nothing, which a caller must
/// already allow for in every failure.
///
/// A request that is not answered in time is not a refusal. The store is
/// slow with it, and whoever sent it waited out its patience to learn that,
/// so nothing is sent in its place any faster. 2.1.5 counted it as one. A
/// MinIO that took more than twenty seconds over each listing of what a
/// database had to collect, and answered every other read at once, was
/// taken to refuse reads: the reads it would have answered were not sent,
/// one time for 23 seconds, while the request that tried it went unanswered.
///
/// A store limits one kind of request apart from another, and a read that
/// is taken says nothing of uploads that are all refused. A listing costs a
/// store what no other read does, and Amazon S3 and Backblaze B2 price it
/// apart. So reads, listings, changes, calls of Backblaze's API and uploads
/// to its pods each have a gate of their own.
///
/// The waits are short. What spares the store is that one request is sent
/// where hundreds were, and a wait of four seconds does that as well as one
/// of a minute. What a long wait would cost is that long without the store
/// after it has begun to take requests again.
struct Gate {
    /// What is sent through it, for a log.
    what: &'static str,
    state: parking_lot::Mutex<Refusing>,
}

#[derive(Default)]
struct Refusing {
    /// Refusals since a request was last taken.
    run: u32,
    /// Nothing is sent before this, and after it what tries the store.
    until: Option<Instant>,
    /// Requests tried and refused since `until` was first set.
    tried: u32,
    /// When the one being tried now was sent.
    trying: Option<Instant>,
    /// How long the store last asked to be left alone.
    asked: Option<Duration>,
}

impl Refusing {
    /// How long nothing is sent after a refusal, and how long a request
    /// that tries the store may go unanswered before another is tried.
    fn wait(&self) -> Duration {
        (HOLD * (1 << self.tried)).max(self.asked.unwrap_or_default())
    }
}

/// Leave to send one request.
struct Pass {
    gate: Arc<Gate>,
    /// When it was sent, on the one that tries a store that has been
    /// refusing. Dropping it lets another be tried.
    trial: Option<Instant>,
    /// It was not answered in time.
    unanswered: AtomicBool,
}

impl Pass {
    /// The store asked to be left alone for this long.
    fn asked(&self, wait: Duration) {
        self.gate.state.lock().asked = Some(wait.min(HOLD_ASKED));
    }

    /// `what` was not answered in time.
    fn stalled(&self, what: &str) -> StoreError {
        self.unanswered.store(true, Ordering::Relaxed);
        StoreError::Unavailable(format!("a storage {what} stalled"))
    }

    /// A failed exchange. One that ran out of time was not answered.
    fn failed(&self, error: reqwest::Error) -> StoreError {
        if error.is_timeout() {
            self.unanswered.store(true, Ordering::Relaxed);
        }
        network(error)
    }
}

impl Drop for Pass {
    fn drop(&mut self) {
        if self.trial.is_some() {
            let mut state = self.gate.state.lock();
            if state.trying == self.trial {
                state.trying = None;
            }
        }
    }
}

/// What an answer changed.
#[derive(Debug, PartialEq, Eq)]
enum Turn {
    Nothing,
    /// Nothing is sent for this long.
    Held(Duration),
    /// Requests that were held are sent again.
    Freed,
}

impl Gate {
    fn new(what: &'static str) -> Arc<Self> {
        Arc::new(Self {
            what,
            state: parking_lot::Mutex::default(),
        })
    }

    fn enter(self: &Arc<Self>, now: Instant) -> StoreResult<Pass> {
        let mut state = self.state.lock();
        let trial = match state.until {
            None => None,
            Some(until)
                if until <= now && state.trying.is_none_or(|sent| sent + state.wait() <= now) =>
            {
                state.trying = Some(now);
                Some(now)
            }
            Some(_) => {
                return Err(StoreError::Unavailable(format!(
                    "the store has refused {}, and this one was not sent",
                    self.what
                )));
            }
        };
        Ok(Pass {
            gate: self.clone(),
            trial,
            unanswered: AtomicBool::new(false),
        })
    }

    /// The request that `pass` was given for has ended: `refused`, or
    /// answered with anything else, which a store that answers at all has
    /// taken.
    fn settle(&self, pass: &Pass, refused: bool, now: Instant) -> Turn {
        let mut state = self.state.lock();
        if !refused {
            let held = state.until.is_some();
            *state = Refusing::default();
            return if held { Turn::Freed } else { Turn::Nothing };
        }
        if pass.unanswered.load(Ordering::Relaxed) {
            return Turn::Nothing;
        }
        if state.until.is_none() {
            state.run += 1;
            if state.run < REFUSALS {
                return Turn::Nothing;
            }
        } else if pass.trial.is_some() {
            state.tried = (state.tried + 1).min(2);
        } else {
            // Sent before the store began to refuse, and refused after.
            return Turn::Nothing;
        }
        let wait = state.wait();
        state.until = Some(now + wait);
        Turn::Held(wait)
    }

    fn answered<T>(&self, pass: Pass, answer: &StoreResult<T>, now: Instant) {
        let said = match answer {
            Err(StoreError::Unavailable(said)) => Some(said),
            _ => None,
        };
        let turn = self.settle(&pass, said.is_some(), now);
        drop(pass);
        match (turn, said) {
            (Turn::Held(wait), Some(said)) => tracing::warn!(
                "the store refuses {}: {said}. None is sent for {} s",
                self.what,
                wait.as_secs()
            ),
            (Turn::Freed, _) => tracing::info!("the store takes {} again", self.what),
            _ => {}
        }
    }
}

/// What a store said as it refused: its code, and how long it asks to be
/// left alone. For a log, so only what a code is made of is kept.
fn said(code: Option<&str>, wait: Option<Duration>) -> String {
    let code = code.filter(|code| {
        (1..=40).contains(&code.len())
            && code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    });
    match (code, wait) {
        (Some(code), Some(wait)) => format!(" {code}, retry after {} s", wait.as_secs()),
        (Some(code), None) => format!(" {code}"),
        (None, Some(wait)) => format!(", retry after {} s", wait.as_secs()),
        (None, None) => String::new(),
    }
}

/// The `Retry-After` of an answer, when it is a number of seconds.
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds))
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
/// part of it. A second in this file's tests, one of which waits it out.
const PATIENCE: Duration = Duration::from_secs(if cfg!(test) { 1 } else { 20 });
/// How often a purge sends one of its requests again.
const PURGE_AGAIN: u32 = 2;
/// How many versions one request may remove, by the S3 API.
const BATCH: usize = 1000;
/// Refusals in a row, with no request taken between them, after which the
/// store is taken to be refusing this server and not to have one busy part.
const REFUSALS: u32 = 8;
/// How long nothing is sent after that. Then one request is tried, and each
/// one that is refused doubles the wait, up to four times this.
const HOLD: Duration = Duration::from_secs(1);
/// The longest wait a store is given when it asks for one.
const HOLD_ASKED: Duration = Duration::from_secs(60);
/// Google Cloud Storage answers a listing of versions in a shape of its own
/// unless this header asks for Amazon's. Other stores take no notice of it.
const AS_AMAZON: (&str, &str) = ("x-goog-interop-list-objects-format", "enabled");

/// The whole body of a response. Its size is not known in advance, so the
/// limit is on each wait for more of it rather than on the transfer.
async fn body_of(pass: &Pass, mut response: reqwest::Response) -> StoreResult<Bytes> {
    let mut whole = Bytes::new();
    let mut joined: Option<BytesMut> = None;
    loop {
        let part = match tokio::time::timeout(PATIENCE, response.chunk()).await {
            Ok(part) => part.map_err(|error| pass.failed(error))?,
            Err(_) => return Err(pass.stalled("answer")),
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
            reads: Gate::new("reads"),
            lists: Gate::new("listings"),
            changes: Gate::new("changes"),
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
            calls: Gate::new("calls of Backblaze's API"),
            uploads: Gate::new("uploads to Backblaze"),
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
        self.exchange(method, key, query, range, body, None).await
    }

    /// `send`, and the request also carries the header `also`, signed with
    /// the others. Not sent to a store that is refusing requests.
    async fn exchange(
        &self,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
        range: Option<(u64, u64)>,
        body: Bytes,
        also: Option<(&'static str, &str)>,
    ) -> StoreResult<Reply> {
        // The bucket itself is read only to list it.
        let gate = if method == Method::GET && key.is_empty() {
            &self.lists
        } else if method == Method::GET || method == Method::HEAD {
            &self.reads
        } else {
            &self.changes
        };
        let pass = gate.enter(Instant::now())?;
        let answer = self
            .request(&pass, method, key, query, range, body, also)
            .await;
        gate.answered(pass, &answer, Instant::now());
        answer
    }

    #[allow(clippy::too_many_arguments)]
    async fn request(
        &self,
        pass: &Pass,
        method: Method,
        key: &str,
        query: &[(&str, &str)],
        range: Option<(u64, u64)>,
        body: Bytes,
        also: Option<(&'static str, &str)>,
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
        // These headers are signed, in order of their names.
        let mut headers = vec![
            ("host", self.host.as_str()),
            ("x-amz-content-sha256", payload_hash.as_str()),
            ("x-amz-date", time.as_str()),
        ];
        headers.extend(also);
        headers.sort();
        let names: Vec<&str> = headers.iter().map(|(name, _)| *name).collect();
        let signed = names.join(";");
        let lines: String = headers
            .iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect();
        let canonical =
            format!("{method}\n{path}\n{query_string}\n{lines}\n{signed}\n{payload_hash}");
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
        if let Some((name, value)) = also {
            request = request.header(name, value);
        }
        let response = match tokio::time::timeout(patience, request.body(body).send()).await {
            Ok(response) => response.map_err(|error| pass.failed(error))?,
            Err(_) => return Err(pass.stalled("request")),
        };
        let status = response.status();
        let wait = retry_after(response.headers());
        let body = body_of(pass, response).await?;
        // What an answer that is an error says it is. Read from no other:
        // that may be megabytes of a table.
        let code = || tag(&String::from_utf8_lossy(&body), "Code").map(str::to_owned);
        // S3 reports a connection it gave up on as a client error.
        let gave_up =
            status == StatusCode::BAD_REQUEST && code().as_deref() == Some("RequestTimeout");
        // A request the store has not implemented is refused, not delayed:
        // the next attempt would be answered the same.
        if (status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED)
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
            || gave_up
        {
            if let Some(wait) = wait {
                pass.asked(wait);
            }
            let said = said(code().as_deref(), wait);
            return Err(StoreError::Unavailable(format!("{status}{said}")));
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
        also: Option<(&'static str, &str)>,
    ) -> StoreResult<Reply> {
        again(|| self.exchange(method.clone(), key, query, None, Bytes::new(), also)).await
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
                    let reply = self
                        .purging(Method::DELETE, key, &query[..named], None)
                        .await?;
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
        // The checksum Amazon documents as required of this request, and
        // the one every store that was run or read about accepts.
        let md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&body));
        // A version that the bucket says it did not remove is asked for
        // again with the others, like a request that was not answered.
        again(|| async {
            let query = [("delete", "")];
            let checksum = Some(("content-md5", md5.as_str()));
            let sent = self.exchange(Method::POST, "", &query, None, body.clone(), checksum);
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
    /// Calls of the API, and uploads to the pods it names.
    calls: Arc<Gate>,
    uploads: Arc<Gate>,
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
    /// One call of Backblaze's API. Not sent while it refuses them.
    async fn json(
        &self,
        request: reqwest::RequestBuilder,
    ) -> StoreResult<(StatusCode, serde_json::Value)> {
        let pass = self.calls.enter(Instant::now())?;
        let answer = Self::call(&pass, request).await;
        self.calls.answered(pass, &answer, Instant::now());
        answer
    }

    async fn call(
        pass: &Pass,
        request: reqwest::RequestBuilder,
    ) -> StoreResult<(StatusCode, serde_json::Value)> {
        let response = request.send().await.map_err(|error| pass.failed(error))?;
        let status = response.status();
        let wait = retry_after(response.headers());
        let body = response.bytes().await.map_err(|error| pass.failed(error))?;
        if status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
        {
            if let Some(wait) = wait {
                pass.asked(wait);
            }
            let reply: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let said = said(reply["code"].as_str(), wait);
            return Err(StoreError::Unavailable(format!(
                "Backblaze answered {status}{said}"
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
        // Before a URL is taken: an upload that is not sent would lose the
        // pool its URL, and the next would call for a new one.
        let pass = self.uploads.enter(Instant::now())?;
        let Some((target, _lease)) = self.target(want).await? else {
            return Ok(false);
        };
        let sampled = body.len() <= B2_SAMPLE_BYTES;
        let timeout = Duration::from_secs(20 + body.len() as u64 / (256 << 10));
        let started = std::time::Instant::now();
        let request = self
            .client
            .post(&target.url)
            .timeout(timeout)
            .header("authorization", &target.token)
            .header("x-bz-file-name", encode(key, true))
            .header("content-type", "application/octet-stream")
            .header("x-bz-content-sha1", hex::encode(sha1::Sha1::digest(&body)))
            .body(body);
        let answer = Self::call(&pass, request).await;
        self.uploads.answered(pass, &answer, Instant::now());
        let (status, reply) = answer?;
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
            let listed = self.purging(Method::GET, "", &query, Some(AS_AMAZON));
            let reply = listed.await?;
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

    fn no() -> StoreResult<()> {
        Err(StoreError::Unavailable("503".into()))
    }

    /// One request, answered at once. Whether it was sent.
    fn send(gate: &Arc<Gate>, now: Instant, taken: bool) -> bool {
        let Ok(pass) = gate.enter(now) else {
            return false;
        };
        gate.answered(pass, &if taken { Ok(()) } else { no() }, now);
        true
    }

    /// A gate whose store has just refused `REFUSALS` requests in a row.
    fn held(now: Instant) -> Arc<Gate> {
        let gate = Gate::new("reads");
        for _ in 0..REFUSALS {
            assert!(send(&gate, now, false));
        }
        assert!(!send(&gate, now, true));
        gate
    }

    #[test]
    fn a_store_that_refuses_now_and_then_is_sent_every_request() {
        let gate = Gate::new("reads");
        let now = Instant::now();
        for _ in 0..100 {
            for _ in 0..REFUSALS - 1 {
                assert!(send(&gate, now, false));
            }
            assert!(send(&gate, now, true));
        }
    }

    /// One request that was not answered in time.
    fn stall(gate: &Arc<Gate>, pass: Pass, now: Instant) {
        let answer: StoreResult<()> = Err(pass.stalled("request"));
        gate.answered(pass, &answer, now);
    }

    #[test]
    fn a_request_that_is_not_answered_is_not_a_refusal() {
        let start = Instant::now();
        let gate = Gate::new("listings");
        // However many there are: the store is slow, and all are sent.
        for _ in 0..10 * REFUSALS {
            stall(&gate, gate.enter(start).unwrap(), start);
        }
        // Nor is it a request that was taken: these are eight refusals
        // in a row.
        for _ in 0..REFUSALS - 1 {
            assert!(send(&gate, start, false));
        }
        stall(&gate, gate.enter(start).unwrap(), start);
        assert!(send(&gate, start, false));
        assert!(!send(&gate, start, true));
        // One that tries the store and is not answered leaves the wait as
        // it was: the next is tried at once.
        let late = start + HOLD + PATIENCE;
        stall(&gate, gate.enter(start + HOLD).unwrap(), late);
        assert!(send(&gate, late, false));
        assert!(!send(
            &gate,
            late + 2 * HOLD - Duration::from_millis(1),
            true
        ));
        assert!(send(&gate, late + 2 * HOLD, true));
    }

    #[test]
    fn a_request_that_tries_a_store_is_waited_for_no_longer_than_a_refusal() {
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let gate = held(start);
        let first = gate.enter(at(1000)).unwrap();
        assert!(gate.enter(at(1999)).is_err());
        // It has had a second, which is the wait: another is tried, and
        // that one has a second.
        let second = gate.enter(at(2000)).unwrap();
        assert!(second.trial.is_some());
        assert!(gate.enter(at(2999)).is_err());
        // The first ends unanswered. The second is still the one on trial.
        stall(&gate, first, at(2500));
        assert!(gate.enter(at(2999)).is_err());
        // It is refused. The wait is two seconds now, and that is how long
        // the next has.
        gate.answered(second, &no(), at(2999));
        assert!(gate.enter(at(4998)).is_err());
        let third = gate.enter(at(4999)).unwrap();
        assert!(gate.enter(at(6998)).is_err());
        let fourth = gate.enter(at(6999)).unwrap();
        // That one is taken, and all are sent.
        gate.answered(fourth, &Ok(()), at(7000));
        assert!(send(&gate, at(7000), true));
        drop(third);
    }

    #[test]
    fn a_store_that_refuses_is_tried_with_one_request_at_a_time() {
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let gate = held(start);
        // Nothing is sent for a second.
        assert!(!send(&gate, at(999), true));
        // Then one request, and no other while it is unanswered.
        let trial = gate.enter(at(1000)).unwrap();
        assert!(gate.enter(at(1000)).is_err());
        assert!(gate.enter(at(1199)).is_err());
        // It is refused: the wait doubles, from the answer, up to four
        // seconds.
        gate.answered(trial, &no(), at(1200));
        let mut now = 1200;
        for wait in [2000, 4000, 4000, 4000] {
            assert!(!send(&gate, at(now + wait - 1), true), "{wait}");
            now += wait;
            assert!(send(&gate, at(now), false), "{wait}");
            assert!(!send(&gate, at(now), true), "{wait}");
        }
        // It is taken: every request is sent again, and a refusal is one
        // refusal, as it was at first.
        assert!(send(&gate, at(now + 4000), true));
        for _ in 0..REFUSALS - 1 {
            assert!(send(&gate, at(now + 4000), false));
        }
        assert!(send(&gate, at(now + 4000), true));
        // And the first wait of the next time is one second again.
        for _ in 0..REFUSALS {
            assert!(send(&gate, at(now + 4000), false));
        }
        assert!(!send(&gate, at(now + 4999), true));
        assert!(send(&gate, at(now + 5000), true));
    }

    /// An upload that is a spare is given up when the other ends first.
    #[test]
    fn a_request_on_trial_that_is_given_up_lets_another_be_tried() {
        let start = Instant::now();
        let gate = held(start);
        let trial = gate.enter(start + HOLD).unwrap();
        assert!(gate.enter(start + HOLD).is_err());
        drop(trial);
        let trial = gate.enter(start + HOLD).unwrap();
        assert!(trial.trial.is_some());
        assert!(gate.enter(start + HOLD).is_err());
    }

    #[test]
    fn requests_under_way_when_a_store_refuses_change_nothing_by_being_refused() {
        let start = Instant::now();
        let gate = Gate::new("changes");
        let mut sent: Vec<Pass> = (0..40).map(|_| gate.enter(start).unwrap()).collect();
        for pass in sent.drain(..REFUSALS as usize) {
            gate.answered(pass, &no(), start);
        }
        // The others are answered through the wait and after it.
        for (i, pass) in sent.drain(..30).enumerate() {
            gate.answered(pass, &no(), start + HOLD * i as u32 / 10);
        }
        assert!(gate.enter(start + HOLD - Duration::from_millis(1)).is_err());
        // One is tried when the wait is over, and not later for them.
        let trial = gate.enter(start + 3 * HOLD).unwrap();
        assert!(trial.trial.is_some());
        // One of them is taken: the store takes requests, and all are sent.
        // The one on trial is refused after that, which is one refusal.
        gate.answered(sent.pop().unwrap(), &Ok(()), start + 3 * HOLD);
        assert!(send(&gate, start + 3 * HOLD, true));
        gate.answered(trial, &no(), start + 3 * HOLD);
        for _ in 0..REFUSALS - 2 {
            assert!(send(&gate, start + 3 * HOLD, false));
        }
        assert!(send(&gate, start + 3 * HOLD, true));
    }

    #[test]
    fn a_store_is_left_alone_for_as_long_as_it_asks() {
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let gate = Gate::new("reads");
        // As an answer that is under way says it.
        let asked = |seconds| {
            let pass = Pass {
                gate: gate.clone(),
                trial: None,
                unanswered: AtomicBool::new(false),
            };
            pass.asked(Duration::from_secs(seconds));
        };
        asked(5);
        for _ in 0..REFUSALS {
            assert!(send(&gate, start, false));
        }
        // Five seconds where one would have been waited, and two, and four.
        for now in [5000, 10000, 15000] {
            assert!(!send(&gate, at(now - 1), true), "{now}");
            assert!(send(&gate, at(now), false), "{now}");
        }
        // A store that asks for an hour is tried after a minute.
        asked(3600);
        assert!(send(&gate, at(20_000), false));
        assert!(!send(&gate, at(79_999), true));
        assert!(send(&gate, at(80_000), true));
        // What it asked is forgotten when it takes a request.
        for _ in 0..REFUSALS {
            assert!(send(&gate, at(80_000), false));
        }
        assert!(!send(&gate, at(80_999), true));
        assert!(send(&gate, at(81_000), true));
    }

    #[test]
    fn what_a_store_said_is_kept_for_a_log() {
        let secs = Duration::from_secs;
        assert_eq!(
            said(Some("SlowDown"), Some(secs(3))),
            " SlowDown, retry after 3 s"
        );
        assert_eq!(
            said(Some("service_unavailable"), None),
            " service_unavailable"
        );
        assert_eq!(said(None, Some(secs(1))), ", retry after 1 s");
        assert_eq!(said(None, None), "");
        // Not what a code is made of, or too long for one.
        assert_eq!(said(Some("a code\nand a line"), None), "");
        assert_eq!(said(Some(""), None), "");
        assert_eq!(
            said(Some(&"a".repeat(41)), Some(secs(2))),
            ", retry after 2 s"
        );
    }

    /// What a MinIO with much to list did: no listing was answered in time,
    /// and every other read was answered at once. And a table whose bytes
    /// stop coming.
    #[tokio::test]
    async fn a_store_that_is_slow_to_list_is_not_taken_to_refuse() {
        use axum::{Router, body::Body, routing::get};
        use futures::StreamExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let listings = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listed = listings.clone();
        let list = move || async move {
            listed.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(3 * PATIENCE).await;
        };
        let begun = || async {
            let part = Ok::<_, std::io::Error>(Bytes::from_static(b"begun"));
            let parts = futures::stream::once(async { part });
            Body::from_stream(parts.chain(futures::stream::pending()))
        };
        let router = Router::new()
            .route("/bucket", get(list))
            .route("/bucket/table", get(begun))
            .route("/bucket/{*key}", get(|| async { "value" }));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let store = S3Store::new(S3Config {
            endpoint: format!("http://{address}"),
            region: "us-east-1".into(),
            bucket: "bucket".into(),
            access_key_id: "id".into(),
            secret_access_key: "secret".into(),
        })
        .unwrap();
        let many = 2 * REFUSALS as usize;
        let lists = futures::future::join_all((0..many).map(|_| store.list("db/", None, 10)));
        let tables = futures::future::join_all((0..many).map(|_| store.get("table")));
        // A call of Backblaze's API has a time of its own to be answered in.
        let calls = Gate::new("calls of Backblaze's API");
        let call = async {
            let pass = calls.enter(Instant::now()).unwrap();
            let listing = reqwest::Client::new().get(format!("http://{address}/bucket"));
            let unanswered = listing.timeout(Duration::from_millis(100));
            assert!(B2Uploader::call(&pass, unanswered).await.is_err());
            pass.unanswered.load(Ordering::Relaxed)
        };
        let (lists, tables, call) = tokio::join!(lists, tables, call);
        for answer in lists {
            let said = answer.unwrap_err().to_string();
            assert!(said.ends_with("a storage request stalled"), "{said}");
        }
        for answer in tables {
            let said = answer.unwrap_err().to_string();
            assert!(said.ends_with("a storage answer stalled"), "{said}");
        }
        assert!(call);
        assert_eq!(listings.load(Ordering::SeqCst), many + 1);
        // Nothing is counted against the store: reads are sent, and so is
        // the next listing.
        for gate in [&store.lists, &store.reads] {
            assert!(gate.enter(Instant::now()).unwrap().trial.is_none());
            assert_eq!(gate.state.lock().run, 0);
        }
        assert_eq!(store.get("db/key").await.unwrap().unwrap(), "value");
        assert!(store.exists("db/key").await.unwrap());
    }

    /// Backblaze's API as far as an upload needs it: a call that names a
    /// pod, and pods that refuse every upload or take it.
    #[derive(Default)]
    struct Backblaze {
        refusing: AtomicBool,
        refusing_calls: AtomicBool,
        calls: std::sync::atomic::AtomicUsize,
        uploads: std::sync::atomic::AtomicUsize,
    }

    async fn backblaze() -> (Arc<B2Uploader>, Arc<Backblaze>) {
        use axum::{Json, Router, extract::State, response::IntoResponse, routing::post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state: Arc<Backblaze> = Arc::default();
        let url = move |State(state): State<Arc<Backblaze>>| async move {
            let n = state.calls.fetch_add(1, Ordering::SeqCst);
            if state.refusing_calls.load(Ordering::SeqCst) {
                let said = serde_json::json!({"status": 503, "code": "service_unavailable"});
                return (StatusCode::SERVICE_UNAVAILABLE, Json(said)).into_response();
            }
            Json(serde_json::json!({
                "uploadUrl": format!("http://{address}/pod/{n}"),
                "authorizationToken": "token",
            }))
            .into_response()
        };
        let upload = |State(state): State<Arc<Backblaze>>| async move {
            state.uploads.fetch_add(1, Ordering::SeqCst);
            if state.refusing.load(Ordering::SeqCst) {
                let said = serde_json::json!({"status": 503, "code": "service_unavailable"});
                let wait = [("retry-after", "2")];
                (StatusCode::SERVICE_UNAVAILABLE, wait, Json(said)).into_response()
            } else {
                Json(serde_json::json!({})).into_response()
            }
        };
        let router = Router::new()
            .route("/b2api/v4/b2_get_upload_url", post(url))
            .route("/pod/{n}", post(upload))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let uploader = B2Uploader {
            client: reqwest::Client::new(),
            calls: Gate::new("calls of Backblaze's API"),
            uploads: Gate::new("uploads to Backblaze"),
            key_id: String::new(),
            key: String::new(),
            bucket: String::new(),
            // Authorized: nothing here is sent to Backblaze.
            session: tokio::sync::Mutex::new(Some(B2Session {
                api_url: format!("http://{address}"),
                token: "token".into(),
                bucket_id: "bucket".into(),
            })),
            pool: parking_lot::Mutex::default(),
        };
        (Arc::new(uploader), state)
    }

    #[tokio::test]
    async fn backblaze_is_not_pressed_while_it_refuses_calls() {
        let (uploader, backblaze) = backblaze().await;
        let said = |target: StoreResult<B2Target>| target.err().unwrap().to_string();
        backblaze.refusing_calls.store(true, Ordering::SeqCst);
        for _ in 0..REFUSALS {
            let refused = said(uploader.new_target().await);
            let code = "Backblaze answered 503 Service Unavailable service_unavailable";
            assert!(refused.ends_with(code), "{refused}");
        }
        for _ in 0..20 {
            let unsent = said(uploader.new_target().await);
            assert!(unsent.ends_with("and this one was not sent"), "{unsent}");
        }
        assert_eq!(backblaze.calls.load(Ordering::SeqCst), REFUSALS as usize);
        // It takes them again: one is sent after a second, and then all.
        backblaze.refusing_calls.store(false, Ordering::SeqCst);
        tokio::time::sleep(HOLD).await;
        for _ in 0..3 {
            assert!(uploader.new_target().await.is_ok());
        }
        assert_eq!(
            backblaze.calls.load(Ordering::SeqCst),
            REFUSALS as usize + 3
        );
    }

    #[tokio::test]
    async fn backblaze_is_not_pressed_while_it_refuses_uploads() {
        let (uploader, backblaze) = backblaze().await;
        let body = Bytes::from_static(b"1");
        let counts = || {
            // Calls for URLs made in the background have ended by now.
            let calls = backblaze.calls.load(Ordering::SeqCst);
            (calls, backblaze.uploads.load(Ordering::SeqCst))
        };
        let settle = || tokio::time::sleep(Duration::from_millis(100));
        assert!(
            uploader
                .upload("k", body.clone(), Want::First)
                .await
                .unwrap()
        );
        backblaze.refusing.store(true, Ordering::SeqCst);
        for _ in 0..REFUSALS {
            let refused = uploader.upload("k", body.clone(), Want::First).await;
            let said = refused.unwrap_err().to_string();
            let code = "503 Service Unavailable service_unavailable, retry after 2 s";
            assert!(said.ends_with(code), "{said}");
        }
        settle().await;
        let before = counts();
        assert_eq!(before.1, 1 + REFUSALS as usize);
        // No upload is sent, and none takes a URL or calls for one.
        let urls = |uploader: &B2Uploader| {
            let pool = uploader.pool.lock();
            pool.proven.len() + pool.untried.len()
        };
        let kept = urls(&uploader);
        for want in [Want::First, Want::Once, Want::Spare].repeat(20) {
            let unsent = uploader.upload("k", body.clone(), want).await;
            let said = unsent.unwrap_err().to_string();
            assert!(said.ends_with("and this one was not sent"), "{said}");
        }
        settle().await;
        assert_eq!((counts(), urls(&uploader)), (before, kept));
        // Backblaze asked for two seconds, where one would have been
        // waited. Then one upload is sent, of however many there are.
        tokio::time::sleep(HOLD).await;
        assert!(
            uploader
                .upload("k", body.clone(), Want::First)
                .await
                .is_err()
        );
        assert_eq!(counts(), before);
        tokio::time::sleep(HOLD).await;
        let all = (0..20).map(|_| uploader.upload("k", body.clone(), Want::First));
        let all = futures::future::join_all(all).await;
        assert!(all.iter().all(Result::is_err));
        assert_eq!(counts().1, before.1 + 1);
        // It was refused, and for two seconds nothing is sent. Then one is
        // taken, and all are sent.
        backblaze.refusing.store(false, Ordering::SeqCst);
        tokio::time::sleep(HOLD).await;
        assert!(
            uploader
                .upload("k", body.clone(), Want::First)
                .await
                .is_err()
        );
        assert_eq!(counts().1, before.1 + 1);
        tokio::time::sleep(HOLD + Duration::from_millis(50)).await;
        assert!(
            uploader
                .upload("k", body.clone(), Want::First)
                .await
                .unwrap()
        );
        let all = (0..20).map(|_| uploader.upload("k", body.clone(), Want::First));
        let all = futures::future::join_all(all).await;
        assert!(all.iter().all(|sent| matches!(sent, Ok(true))));
        assert_eq!(counts().1, before.1 + 22);
    }
}
