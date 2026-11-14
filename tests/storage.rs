//! The storage bindings against the contract in `src/store/mod.rs`.
//!
//! The S3 binding runs against a stand-in written from the documented
//! behaviour of a bucket that keeps versions: PUT adds a version, DELETE
//! adds a delete marker, DELETE of a version removes exactly that entry,
//! and listings come in pages. It verifies the signature of every request
//! with an implementation of its own. This shows what the client does; what
//! a provider does is shown by `tests/bucket.rs`.
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::Response,
};
use bytes::Bytes;
use durable_db::{
    Error,
    io::Io,
    store::{
        ObjectStore, StoreError, dir_order,
        fs::FsStore,
        list_all, list_from,
        memory::MemoryStore,
        s3::{S3Config, S3Store},
    },
    table::{Entry, Table, TableBuilder},
};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

#[derive(Clone, Debug)]
struct Version {
    key: String,
    id: String,
    seq: u64,
    marker: bool,
    body: Bytes,
}

#[derive(Default)]
struct Bucket {
    versions: Vec<Version>,
    seq: u64,
    /// Entries in a listing page; a bucket may return fewer than asked for.
    page: usize,
    /// Keys visible at the time of each version listing.
    seen: Vec<Vec<String>>,
    /// Answer the n-th version listing (from 1) with 503.
    fail_version_listing: Option<usize>,
    /// Answer this many listings with an empty page that says there is more.
    empty_pages: usize,
    /// Whether such a page says how to continue.
    empty_pages_have_tokens: bool,
    /// Answer everything with this.
    canned: Option<(StatusCode, &'static str)>,
    unsigned: Vec<String>,
    requests: usize,
}

impl Bucket {
    fn latest(&self, key: &str) -> Option<&Version> {
        self.versions
            .iter()
            .filter(|v| v.key == key)
            .max_by_key(|v| v.seq)
    }

    fn visible(&self) -> Vec<String> {
        let mut keys: Vec<&str> = self.versions.iter().map(|v| v.key.as_str()).collect();
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .filter(|key| self.latest(key).is_some_and(|v| !v.marker))
            .map(str::to_owned)
            .collect()
    }

    fn add(&mut self, key: &str, marker: bool, body: Bytes) {
        self.seq += 1;
        self.versions.push(Version {
            key: key.to_owned(),
            id: format!("v{:06}", self.seq),
            seq: self.seq,
            marker,
            body,
        });
    }

    fn page_size(&self, asked: Option<String>) -> usize {
        let asked = asked.and_then(|m| m.parse().ok()).unwrap_or(1000);
        asked.min(if self.page == 0 { 1000 } else { self.page })
    }
}

type Shared = Arc<Mutex<Bucket>>;

fn uri_encode(bytes: &[u8], keep_slash: bool) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn percent_decode(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| text.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    out
}

fn mac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// Verify a signature the way the service does: from what arrived.
fn verify(
    method: &str,
    raw_path: &str,
    raw_query: &str,
    header: &dyn Fn(&str) -> Option<String>,
    body: &[u8],
) -> Result<(), String> {
    let authorization = header("authorization").ok_or("no authorization")?;
    let rest = authorization
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or("wrong algorithm")?;
    let field = |name: &str| {
        rest.split(',')
            .find_map(|part| part.trim().strip_prefix(name))
            .ok_or(format!("no {name}"))
    };
    let (credential, signed_headers, signature) = (
        field("Credential=")?,
        field("SignedHeaders=")?,
        field("Signature=")?,
    );
    let scope = credential.split_once('/').ok_or("no scope")?.1;
    let parts: Vec<&str> = scope.split('/').collect();
    let [date, region, service, _] = parts[..] else {
        return Err("malformed scope".into());
    };
    let payload = header("x-amz-content-sha256").ok_or("no payload hash")?;
    if payload != hex::encode(Sha256::digest(body)) {
        return Err("the payload hash does not match the body".into());
    }
    let mut pairs: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                uri_encode(&percent_decode(name), false),
                uri_encode(&percent_decode(value), false),
            )
        })
        .collect();
    pairs.sort();
    let query: Vec<String> = pairs.iter().map(|(n, v)| format!("{n}={v}")).collect();
    let mut headers = String::new();
    for name in signed_headers.split(';') {
        let value = header(name).ok_or(format!("signed header {name} is missing"))?;
        headers.push_str(&format!("{name}:{}\n", value.trim()));
    }
    let canonical = format!(
        "{method}\n{}\n{}\n{headers}\n{signed_headers}\n{payload}",
        uri_encode(&percent_decode(raw_path), true),
        query.join("&"),
    );
    let time = header("x-amz-date").ok_or("no date")?;
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{time}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut key = mac(format!("AWS4{SECRET}").as_bytes(), date.as_bytes());
    for part in [region, service, "aws4_request"] {
        key = mac(&key, part.as_bytes());
    }
    if hex::encode(mac(&key, to_sign.as_bytes())) == signature {
        Ok(())
    } else {
        Err(format!(
            "wrong signature for {method} {raw_path}?{raw_query}"
        ))
    }
}

/// The verifier itself, on the example in the S3 API reference.
#[test]
fn the_verifier_accepts_the_published_example() {
    let header = |name: &str| {
        let value = match name {
            "host" => "examplebucket.s3.amazonaws.com",
            "x-amz-date" => "20130524T000000Z",
            "x-amz-content-sha256" => {
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            }
            "authorization" => {
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
            }
            _ => return None,
        };
        Some(value.to_owned())
    };
    verify("GET", "/", "max-keys=2&prefix=J", &header, b"").unwrap();
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn reply(status: StatusCode, body: impl Into<Body>) -> Response {
    Response::builder()
        .status(status)
        .body(body.into())
        .unwrap()
}

const LISTING: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>";
const VERSIONS: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>";

async fn handle(State(bucket): State<Shared>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let raw_path = parts.uri.path().to_owned();
    let raw_query = parts.uri.query().unwrap_or("").to_owned();
    let header = |name: &str| {
        let value = parts.headers.get(name)?.to_str().ok()?;
        Some(value.to_owned())
    };
    let mut bucket = bucket.lock();
    bucket.requests += 1;
    if let Err(error) = verify(parts.method.as_str(), &raw_path, &raw_query, &header, &body) {
        bucket.unsigned.push(error);
        return reply(
            StatusCode::FORBIDDEN,
            "<Error><Code>SignatureDoesNotMatch</Code></Error>",
        );
    }
    if let Some((status, text)) = bucket.canned {
        return reply(status, text);
    }
    let text = |raw: &str| String::from_utf8(percent_decode(raw)).unwrap();
    let query: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .map(|(name, value)| (text(name), text(value)))
        .collect();
    let param = |name: &str| {
        let found = query.iter().find(|(n, _)| n == name);
        found.map(|(_, value)| value.clone())
    };
    let key = text(&raw_path)
        .strip_prefix("/bucket")
        .map(|rest| rest.trim_start_matches('/').to_owned())
        .unwrap_or_default();
    if !key.is_empty() {
        return object(
            &mut bucket,
            &parts.method,
            &key,
            param("versionId"),
            body,
            header("range"),
        );
    }
    let prefix = param("prefix").unwrap_or_default();
    if param("versions").is_some() {
        let visible = bucket.visible();
        bucket.seen.push(visible);
        if bucket.fail_version_listing == Some(bucket.seen.len()) {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                "<Error><Code>SlowDown</Code></Error>",
            );
        }
        // By key, and newest first within a key.
        let mut entries: Vec<Version> = bucket
            .versions
            .iter()
            .filter(|v| v.key.starts_with(&prefix))
            .cloned()
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
        if let Some(from) = param("key-marker") {
            let after =
                param("version-id-marker").and_then(|id| id.strip_prefix('v')?.parse::<u64>().ok());
            entries.retain(|v| v.key > from || (v.key == from && after.is_some_and(|a| v.seq < a)));
        }
        let size = bucket.page_size(param("max-keys"));
        let truncated = entries.len() > size;
        entries.truncate(size);
        let mut xml = format!("{VERSIONS}<IsTruncated>{truncated}</IsTruncated>");
        if let (true, Some(last)) = (truncated, entries.last()) {
            xml.push_str(&format!(
                "<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>{}</NextVersionIdMarker>",
                escape(&last.key),
                last.id
            ));
        }
        for v in &entries {
            let element = if v.marker { "DeleteMarker" } else { "Version" };
            xml.push_str(&format!(
                "<{element}><Key>{}</Key><VersionId>{}</VersionId><Size>{}</Size></{element}>",
                escape(&v.key),
                v.id,
                v.body.len()
            ));
        }
        return reply(StatusCode::OK, xml + "</ListVersionsResult>");
    }
    // A continuation token stands for the place the listing had reached.
    let start = match param("continuation-token") {
        Some(token) => String::from_utf8(hex::decode(token).unwrap()).unwrap(),
        None => param("start-after").unwrap_or_default(),
    };
    if bucket.empty_pages > 0 {
        bucket.empty_pages -= 1;
        let token = match bucket.empty_pages_have_tokens {
            true => format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                hex::encode(&start)
            ),
            false => String::new(),
        };
        return reply(
            StatusCode::OK,
            format!("{LISTING}<IsTruncated>true</IsTruncated>{token}</ListBucketResult>"),
        );
    }
    let delimiter = param("delimiter");
    let mut items: Vec<(bool, String, usize)> = Vec::new();
    for key in bucket.visible() {
        if !key.starts_with(&prefix) || key <= start {
            continue;
        }
        let rolled = delimiter.as_ref().and_then(|d| {
            let at = key[prefix.len()..].find(d.as_str())?;
            Some(key[..prefix.len() + at + d.len()].to_owned())
        });
        match rolled {
            Some(common) if items.last().is_some_and(|(c, k, _)| *c && *k == common) => {}
            Some(common) => items.push((true, common, 0)),
            None => {
                let size = bucket.latest(&key).unwrap().body.len();
                items.push((false, key, size));
            }
        }
    }
    let size = bucket.page_size(param("max-keys"));
    let truncated = items.len() > size;
    items.truncate(size);
    let mut xml = format!("{LISTING}<IsTruncated>{truncated}</IsTruncated>");
    if let (true, Some((_, last, _))) = (truncated, items.last()) {
        // After a common prefix, everything below it has been listed.
        let reached = match last.strip_suffix('/') {
            Some(dir) if delimiter.is_some() => format!("{dir}0"),
            _ => last.clone(),
        };
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            hex::encode(reached)
        ));
    }
    for (common, name, size) in &items {
        xml.push_str(&match common {
            true => format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                escape(name)
            ),
            false => format!(
                "<Contents><Key>{}</Key><Size>{size}</Size></Contents>",
                escape(name)
            ),
        });
    }
    reply(StatusCode::OK, xml + "</ListBucketResult>")
}

fn object(
    bucket: &mut Bucket,
    method: &Method,
    key: &str,
    version: Option<String>,
    body: Bytes,
    range: Option<String>,
) -> Response {
    let missing = || {
        reply(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code><Message>absent</Message></Error>",
        )
    };
    match *method {
        Method::PUT => {
            bucket.add(key, false, body);
            reply(StatusCode::OK, "")
        }
        Method::DELETE => {
            match version {
                Some(id) => bucket.versions.retain(|v| !(v.key == key && v.id == id)),
                None => bucket.add(key, true, Bytes::new()),
            }
            reply(StatusCode::NO_CONTENT, "")
        }
        Method::HEAD => match bucket.latest(key) {
            Some(v) if !v.marker => reply(StatusCode::OK, ""),
            _ => reply(StatusCode::NOT_FOUND, ""),
        },
        Method::GET => {
            let Some(body) = bucket
                .latest(key)
                .filter(|v| !v.marker)
                .map(|v| v.body.clone())
            else {
                return missing();
            };
            let Some(range) = range else {
                return reply(StatusCode::OK, body);
            };
            let (from, to) = range.trim_start_matches("bytes=").split_once('-').unwrap();
            let (from, to): (usize, usize) = (from.parse().unwrap(), to.parse().unwrap());
            if from >= body.len() {
                return reply(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "<Error><Code>InvalidRange</Code></Error>",
                );
            }
            reply(
                StatusCode::PARTIAL_CONTENT,
                body.slice(from..(to + 1).min(body.len())),
            )
        }
        _ => reply(StatusCode::METHOD_NOT_ALLOWED, ""),
    }
}

async fn s3() -> (S3Store, Shared) {
    let bucket: Shared = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().fallback(handle).with_state(bucket.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let store = S3Store::new(S3Config {
        endpoint: format!("http://{address}"),
        region: "us-east-1".into(),
        bucket: "bucket".into(),
        access_key_id: ACCESS.into(),
        secret_access_key: SECRET.into(),
    })
    .unwrap();
    (store, bucket)
}

async fn keys(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    let listed = list_all(store, prefix).await.unwrap();
    listed.into_iter().map(|item| item.key).collect()
}

const ONE: Bytes = Bytes::from_static(b"1");

/// What is signed must be what is sent, and what is listed must be what was
/// stored, for every character that needs encoding somewhere.
#[tokio::test]
async fn awkward_keys_are_signed_stored_and_listed() {
    let (store, bucket) = s3().await;
    let names = [
        "p/plain",
        "p/a b",
        "p/a+b",
        "p/a~b",
        "p/a%b",
        "p/a%41b",
        "p/a=b",
        "p/a&b",
        "p/a<b>c",
        "p/a'b\"c",
        "p/a*b!(c)",
        "p/a:b@c,d;e",
        "p/a?b#c",
        "p/a$b[c]",
        "p/ü/日本語",
        "p/&amp;",
        "p/<Key>x</Key>",
        "p/</Contents><Contents><Key>forged</Key><Size>1</Size>",
        "p/a\\b|c^d`e{f}",
    ];
    for name in names {
        let body = Bytes::from(name.as_bytes().to_vec());
        store.put(name, body).await.unwrap();
    }
    for name in names {
        let found = store.get(name).await.unwrap();
        assert_eq!(found.as_deref(), Some(name.as_bytes()), "{name}");
        assert!(store.exists(name).await.unwrap(), "{name}");
        let part = store.get_range(name, 0, 2).await.unwrap();
        assert_eq!(part.as_deref(), Some(&name.as_bytes()[..2]));
    }
    let mut want: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
    want.sort();
    assert_eq!(keys(&store, "p/").await, want);
    // One to a page, so that each awkward key is also sent as a cursor.
    bucket.lock().page = 1;
    assert_eq!(keys(&store, "p/").await, want);
    bucket.lock().page = 0;
    for name in names {
        assert_eq!(keys(&store, name).await, [name.to_owned()], "{name}");
        store.delete(name).await.unwrap();
        assert!(!store.exists(name).await.unwrap());
    }
    assert_eq!(
        store.purge("p/", None, &|_| true).await.unwrap(),
        names.len()
    );
    let bucket = bucket.lock();
    assert!(bucket.versions.is_empty(), "{:?}", bucket.versions);
    assert!(bucket.unsigned.is_empty(), "{:#?}", bucket.unsigned);
    assert!(bucket.requests > 100);
}

/// Only "no such key" means absent, and only a failure that may pass is
/// worth another attempt.
#[tokio::test]
async fn answers_are_read_for_what_they_say() {
    let (store, bucket) = s3().await;
    let outcome = |result: Result<(), StoreError>| match result {
        Ok(()) => "ok",
        Err(StoreError::Unavailable(_)) => "retry",
        Err(StoreError::Fatal(_)) => "fatal",
    };
    for (status, text, read, list, write) in [
        (
            403,
            "<Error><Code>AccessDenied</Code></Error>",
            "fatal",
            "fatal",
            "fatal",
        ),
        (
            503,
            "<Error><Code>SlowDown</Code></Error>",
            "retry",
            "retry",
            "retry",
        ),
        (500, "", "retry", "retry", "retry"),
        (429, "", "retry", "retry", "retry"),
        (
            400,
            "<Error><Code>RequestTimeout</Code></Error>",
            "retry",
            "retry",
            "retry",
        ),
        (
            400,
            "<Error><Code>InvalidArgument</Code></Error>",
            "fatal",
            "fatal",
            "fatal",
        ),
        (
            404,
            "<Error><Code>NoSuchBucket</Code></Error>",
            "fatal",
            "fatal",
            "fatal",
        ),
        (
            404,
            "<Error><Code>NoSuchKey</Code></Error>",
            "ok",
            "fatal",
            "ok",
        ),
        // Something on the way answered in the bucket's place.
        (
            200,
            "<html>Sign in to this network</html>",
            "ok",
            "retry",
            "ok",
        ),
    ] {
        bucket.lock().canned = Some((StatusCode::from_u16(status).unwrap(), text));
        let case = format!("{status} {text}");
        assert_eq!(outcome(store.get("k").await.map(drop)), read, "{case}");
        let listed = store.list("p/", None, 10).await;
        assert_eq!(outcome(listed.map(drop)), list, "{case}");
        let dirs = store.list_dirs("p/", None, 10).await;
        assert_eq!(outcome(dirs.map(drop)), list, "{case}");
        let purged = store.purge("p/", None, &|_| true).await;
        assert_eq!(outcome(purged.map(drop)), list, "{case}");
        assert_eq!(outcome(store.delete("k").await), write, "{case}");
        if status != 404 {
            assert_eq!(outcome(store.put("k", ONE).await), write, "{case}");
        }
    }
    assert!(bucket.lock().unsigned.is_empty());
}

#[tokio::test]
async fn paged_listings_and_purges_are_complete() {
    let (store, bucket) = s3().await;
    let mut want = Vec::new();
    for i in 0..23 {
        let key = format!("p/l/{i:04}");
        // One to three versions each, some hidden by a delete marker.
        for _ in 0..=(i % 3) {
            store.put(&key, ONE).await.unwrap();
        }
        if i % 4 == 0 {
            store.delete(&key).await.unwrap();
        } else {
            want.push(key);
        }
    }
    for page in [1, 2, 3, 7, 1000] {
        bucket.lock().page = page;
        assert_eq!(keys(&store, "p/").await, want, "page {page}");
        let some = store.list("p/", Some("p/l/0005"), 4).await.unwrap();
        let some: Vec<&str> = some.iter().map(|item| item.key.as_str()).collect();
        assert_eq!(some, ["p/l/0006", "p/l/0007", "p/l/0009", "p/l/0010"]);
    }
    bucket.lock().page = 3;
    // Up to a bound: every version and marker below it, nothing from it on.
    let removed = store.purge("p/l/", Some("p/l/0010"), &|_| true).await;
    assert_eq!(removed.unwrap(), 10);
    {
        let bucket = bucket.lock();
        assert!(bucket.versions.iter().all(|v| v.key.as_str() >= "p/l/0010"));
        assert!(bucket.versions.iter().any(|v| v.key == "p/l/0010"));
    }
    let removed = store.purge("p/", None, &|key| key != "p/l/0015").await;
    assert_eq!(removed.unwrap(), 12);
    let bucket = bucket.lock();
    assert!(bucket.versions.iter().all(|v| v.key == "p/l/0015"));
    assert!(bucket.unsigned.is_empty());
}

/// A revoked claim is a version under a delete marker. Whatever page
/// boundary falls between the two, and wherever a purge stops, the claim
/// never becomes readable again: a stale owner would resume on seeing it.
#[tokio::test]
async fn a_purge_never_shows_a_deleted_key() {
    for fail in [None, Some(2), Some(3)] {
        for page in 1..=5 {
            let (store, bucket) = s3().await;
            store.put("db/app/c/0001", ONE).await.unwrap();
            for revoked in ["db/app/c/0002", "db/app/c/0003"] {
                store.put(revoked, ONE).await.unwrap();
                store.put(revoked, ONE).await.unwrap();
                store.delete(revoked).await.unwrap();
            }
            store.put("db/app/c/0004", ONE).await.unwrap();
            {
                let mut bucket = bucket.lock();
                bucket.page = page;
                bucket.fail_version_listing = fail;
            }
            let purged = store.purge("db/app/", None, &|_| true).await;
            let case = format!("page {page}, failing listing {fail:?}");
            let bucket = bucket.lock();
            match fail.filter(|at| *at <= bucket.seen.len()) {
                Some(_) => assert!(purged.is_err(), "{case}"),
                None => {
                    assert_eq!(purged.unwrap(), 4, "{case}");
                    assert!(bucket.versions.is_empty(), "{case}: {:?}", bucket.versions);
                }
            }
            let shown: Vec<&String> = bucket.seen.iter().flatten().collect();
            let visible = bucket.visible();
            for revoked in ["db/app/c/0002", "db/app/c/0003"] {
                assert!(!shown.iter().any(|key| *key == revoked), "{case}");
                assert!(!visible.iter().any(|key| key == revoked), "{case}");
            }
        }
    }
}

/// A page may say there is more and hold nothing. Fewer keys than asked for
/// means the listing is complete, so such a page must not end it.
#[tokio::test]
async fn an_empty_page_does_not_end_a_listing() {
    let (store, bucket) = s3().await;
    for name in ["a", "b", "c"] {
        store.put(&format!("db/{name}/c/1"), ONE).await.unwrap();
    }
    {
        let mut bucket = bucket.lock();
        bucket.empty_pages = 2;
        bucket.empty_pages_have_tokens = true;
    }
    assert_eq!(keys(&store, "db/").await.len(), 3);
    bucket.lock().empty_pages = 2;
    assert_eq!(
        store.list_dirs("db/", Some("a"), 10).await.unwrap(),
        ["b", "c"]
    );
    // With no way to continue, the listing fails rather than ends.
    for dirs in [false, true] {
        {
            let mut bucket = bucket.lock();
            bucket.empty_pages = 1;
            bucket.empty_pages_have_tokens = false;
        }
        let cut_short = match dirs {
            true => store.list_dirs("db/", None, 10).await.map(drop),
            false => store.list("db/", None, 10).await.map(drop),
        };
        assert!(matches!(cut_short, Err(StoreError::Unavailable(_))));
    }
}

/// A bucket that began keeping versions after objects were written holds
/// them under the version "null".
#[tokio::test]
async fn a_purge_frees_versions_named_null() {
    let (store, bucket) = s3().await;
    bucket.lock().versions.push(Version {
        key: "p/old".into(),
        id: "null".into(),
        seq: 0,
        marker: false,
        body: ONE,
    });
    assert_eq!(store.purge("p/", None, &|_| true).await.unwrap(), 1);
    assert!(bucket.lock().versions.is_empty());
}

/// Directory names come in one order from every store, the order of the
/// keys below them, and paging by the last name returned misses none.
#[tokio::test]
async fn directories_are_listed_alike_by_every_store() {
    let names = [
        "app",
        "app-staging",
        "app.v2",
        "app0",
        "app_",
        "b",
        "B",
        "0",
    ];
    let mut want: Vec<&str> = names.to_vec();
    want.sort_by(|a, b| dir_order(a, b));
    assert_eq!(
        want,
        [
            "0",
            "B",
            "app-staging",
            "app.v2",
            "app",
            "app0",
            "app_",
            "b"
        ]
    );
    let dir = tempfile::tempdir().unwrap();
    let (s3, bucket) = s3().await;
    let stores: [(&str, Arc<dyn ObjectStore>); 3] = [
        ("memory", Arc::new(MemoryStore::new())),
        (
            "directory",
            Arc::new(FsStore::open(dir.path(), false).unwrap()),
        ),
        ("s3", Arc::new(s3)),
    ];
    for (label, store) in stores {
        for name in names {
            store.put(&format!("db/{name}/m/x"), ONE).await.unwrap();
            store.put(&format!("db/{name}/t/y"), ONE).await.unwrap();
        }
        store.put("db/app1", ONE).await.unwrap();
        for page in [1, 2, 1000] {
            bucket.lock().page = page;
            assert_eq!(
                store.list_dirs("db/", None, 100).await.unwrap(),
                want,
                "{label}"
            );
            for limit in [1, 3] {
                let mut paged = Vec::new();
                loop {
                    let after = paged.last().map(String::as_str);
                    let more = store.list_dirs("db/", after, limit).await.unwrap();
                    if more.is_empty() {
                        break;
                    }
                    paged.extend(more);
                }
                assert_eq!(paged, want, "{label}, {limit} at a time");
            }
            assert_eq!(
                store.list_dirs("db/", Some("app"), 2).await.unwrap(),
                ["app0", "app_"],
                "{label}"
            );
        }
    }
}

/// The directory store lists exactly as the flat key space does.
#[tokio::test]
async fn a_directory_lists_like_a_bucket() {
    let mut rng = StdRng::seed_from_u64(11);
    let parts = [
        "a", "b", "a-b", "a.b", "a_b", "ab", "A", "0", "~", "a~", "a0", "a-",
    ];
    for round in 0..12 {
        let dir = tempfile::tempdir().unwrap();
        let fs = FsStore::open(dir.path(), false).unwrap();
        let memory = MemoryStore::new();
        let mut stored: Vec<String> = Vec::new();
        for _ in 0..rng.random_range(1..40) {
            let depth = rng.random_range(1..4);
            let key = (0..depth)
                .map(|_| parts[rng.random_range(0..parts.len())])
                .collect::<Vec<_>>()
                .join("/");
            // On disk a name is an object or a directory, not both.
            let clash = |other: &String| {
                other.starts_with(&format!("{key}/")) || key.starts_with(&format!("{other}/"))
            };
            if stored.iter().any(clash) {
                continue;
            }
            fs.put(&key, ONE).await.unwrap();
            memory.put(&key, ONE).await.unwrap();
            stored.push(key);
        }
        let mut prefixes = vec![String::new(), "a".into(), "a/".into(), "a-".into()];
        for key in &stored {
            prefixes.push(key.clone());
            prefixes.push(key[..key.len() - 1].to_owned());
            prefixes.extend(key.rsplit_once('/').map(|(dir, _)| format!("{dir}/")));
        }
        for prefix in &prefixes {
            let below_an_object = |(dir, _): (&str, &str)| stored.iter().any(|key| key == dir);
            if prefix.rsplit_once('/').is_some_and(below_an_object) {
                continue;
            }
            for after in [None, Some("a"), Some("a/"), Some("a0")] {
                for limit in [1, 2, 1000] {
                    assert_eq!(
                        fs.list(prefix, after, limit).await.unwrap(),
                        memory.list(prefix, after, limit).await.unwrap(),
                        "round {round}: list({prefix:?}, {after:?}, {limit})"
                    );
                }
                assert_eq!(
                    list_from(&fs, prefix, after).await.unwrap(),
                    list_from(&memory, prefix, after).await.unwrap()
                );
            }
        }
    }
}

/// Several processes may serve one directory, and two of them may have the
/// same process id: two containers on one volume are both process 1. Two
/// stores in this process are in the same position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_stores_on_one_directory_do_not_mix_objects() {
    let dir = tempfile::tempdir().unwrap();
    let stores = [
        Arc::new(FsStore::open(dir.path(), false).unwrap()),
        Arc::new(FsStore::open(dir.path(), false).unwrap()),
    ];
    let body = |who: usize, i: usize| Bytes::from(format!("{who}:{i};").repeat(2000 + 700 * who));
    let writers: Vec<_> = (0..2)
        .map(|who| {
            let store = stores[who].clone();
            tokio::spawn(async move {
                for i in 0..300 {
                    let key = format!("db/{who}/l/{i:04}");
                    store.put(&key, body(who, i)).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    for who in 0..2 {
        for i in 0..300 {
            let found = stores[0].get(&format!("db/{who}/l/{i:04}")).await;
            assert_eq!(found.unwrap(), Some(body(who, i)), "{who}/{i}");
        }
    }
}

/// A write that fails leaves nothing behind, and nothing half-written.
#[tokio::test]
async fn a_failed_write_to_a_directory_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let fs = FsStore::open(dir.path(), true).unwrap();
    fs.put("db/app/c/1", ONE).await.unwrap();
    // On disk an object cannot also be a directory.
    assert!(fs.put("db/app/c/1/x", ONE).await.is_err());
    assert_eq!(fs.get("db/app/c/1").await.unwrap(), Some(ONE));
    assert_eq!(keys(&fs, "db/").await, ["db/app/c/1"]);
    assert_eq!(
        std::fs::read_dir(dir.path().join(".tmp")).unwrap().count(),
        0
    );
}

/// A footer whose fields were chosen to wrap around is damage like any other.
#[tokio::test]
async fn a_crafted_table_footer_is_reported_as_damage() {
    let mut builder = TableBuilder::new(96);
    builder.add(&Entry {
        key: Bytes::from_static(b"a"),
        version: 1,
        value: Some(ONE),
    });
    let (body, meta) = builder.finish("t/x".into());
    let size = body.len() as u64;
    let mut damaged = body.to_vec();
    let footer = damaged.len() - 32;
    let index_len = u32::MAX;
    let index_offset = size.wrapping_sub(32).wrapping_sub(u64::from(index_len));
    damaged[footer..footer + 8].copy_from_slice(&index_offset.to_le_bytes());
    damaged[footer + 8..footer + 12].copy_from_slice(&index_len.to_le_bytes());
    let store = Arc::new(MemoryStore::new());
    store.put("t/x", Bytes::from(damaged)).await.unwrap();
    let io = Io::new(store, 1 << 20, None);
    let found = Table::new(meta).get(&io, b"a").await;
    assert!(matches!(found, Err(Error::Corrupt(_))), "{found:?}");
}
