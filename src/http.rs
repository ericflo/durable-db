//! The HTTP API. See `docs/api.md` for the contract this implements.
use crate::{
    Error, Result,
    auth::{Access, Grant, Keys, expiry},
    cluster::{Cluster, FORWARDED},
    db::{Check, Info, ReadReq, Scan, Txn},
    layout::{DbConfig, INTERNAL},
    node::Node,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone)]
pub struct ApiLimits {
    pub max_body: usize,
    pub max_key: usize,
    pub max_value: usize,
    /// Writes plus checks in one transaction.
    pub max_operations: usize,
    /// Keys plus scans in one read.
    pub max_lookups: usize,
    pub max_scan_items: usize,
    /// Requests in flight on this node before new ones are refused.
    pub max_inflight: usize,
    /// Request bodies held in memory at once, across all requests.
    pub max_body_total: usize,
    /// Bytes one read may return.
    pub max_read_bytes: usize,
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            max_body: 32 << 20,
            max_key: 1024,
            max_value: 4 << 20,
            max_operations: 10_000,
            max_lookups: 10_000,
            max_scan_items: 10_000,
            max_inflight: 4096,
            max_body_total: 512 << 20,
            max_read_bytes: 64 << 20,
        }
    }
}

pub struct App {
    pub node: Arc<Node>,
    pub keys: Keys,
    pub cluster: Arc<Cluster>,
    pub limits: ApiLimits,
    pub inflight: tokio::sync::Semaphore,
    /// Permits of 1 KiB each for request bodies in memory.
    pub bodies: tokio::sync::Semaphore,
}

impl App {
    pub fn new(node: Arc<Node>, keys: Keys, cluster: Arc<Cluster>, limits: ApiLimits) -> Arc<Self> {
        Arc::new(Self {
            inflight: tokio::sync::Semaphore::new(limits.max_inflight),
            bodies: tokio::sync::Semaphore::new(limits.max_body_total >> 10),
            node,
            keys,
            cluster,
            limits,
        })
    }
}

pub fn router(app: Arc<App>) -> Router {
    let body_limit = app.limits.max_body;
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/databases", get(list))
        .route("/v1/databases/{*path}", any(database))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(app)
}

pub fn error_response(error: &Error) -> Response {
    let (status, code) = match error {
        Error::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        Error::Gone => (StatusCode::GONE, "deleted"),
        Error::Exists => (StatusCode::CONFLICT, "exists"),
        Error::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
        Error::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid"),
        Error::Quota => (StatusCode::INSUFFICIENT_STORAGE, "quota_exceeded"),
        Error::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "too_large"),
        Error::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Error::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
        Error::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
        Error::Busy(_) => (StatusCode::SERVICE_UNAVAILABLE, "busy"),
        Error::Lost(_) => (StatusCode::SERVICE_UNAVAILABLE, "ownership_moved"),
        Error::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"),
        Error::Corrupt(_) => (StatusCode::INTERNAL_SERVER_ERROR, "corrupt"),
    };
    let body = error_json(error, code);
    let mut response = (status, [(header::CONTENT_TYPE, "application/json")], body).into_response();
    if error.retryable() {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

fn error_code(error: &Error) -> &'static str {
    match error {
        Error::NotFound => "not_found",
        Error::Gone => "deleted",
        Error::Exists => "exists",
        Error::Conflict(_) => "conflict",
        Error::Invalid(_) => "invalid",
        Error::Quota => "quota_exceeded",
        Error::TooLarge => "too_large",
        Error::RateLimited => "rate_limited",
        Error::Unauthorized => "unauthorized",
        Error::Forbidden => "forbidden",
        Error::Busy(_) => "busy",
        Error::Lost(_) => "ownership_moved",
        Error::Unavailable(_) => "storage_unavailable",
        Error::Corrupt(_) => "corrupt",
    }
}

fn error_json(error: &Error, code: &str) -> String {
    json!({"error": {"code": code, "message": error.to_string(), "retryable": error.retryable()}})
        .to_string()
}

fn json_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

fn grant(app: &App, headers: &HeaderMap) -> Result<Grant> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(Error::Unauthorized)?;
    app.keys.verify(token.trim())
}

async fn ready(State(app): State<Arc<App>>) -> Response {
    // Ready means storage answers and the node is not shutting down. A node
    // that is not ready receives no traffic and is not a cluster member.
    if app.cluster.draining() {
        return error_response(&Error::Busy("this node is shutting down".into()));
    }
    match crate::io::retry(|| app.node.ctx.io.store.exists("ready-probe")).await {
        Ok(_) => "ready\n".into_response(),
        Err(error) => error_response(&error),
    }
}

async fn metrics(State(app): State<Arc<App>>) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    let io = &app.node.ctx.io;
    app.node
        .metrics
        .render(
            app.node.open_count(),
            io.cache_bytes(),
            io.block_hits.load(Relaxed),
            io.block_misses.load(Relaxed),
        )
        .into_response()
}

#[derive(Deserialize)]
struct ListQuery {
    after: Option<String>,
    limit: Option<usize>,
}

async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> Response {
    let listed = async {
        if !grant(&app, &headers)?.is_admin() {
            return Err(Error::Forbidden);
        }
        let limit = query.limit.unwrap_or(100).clamp(1, 1000);
        let names = app.node.list(query.after.as_deref(), limit).await?;
        let next = (names.len() == limit)
            .then(|| names.last().cloned())
            .flatten();
        Ok(json!({"databases": names, "next": next}).to_string())
    };
    match listed.await {
        Ok(body) => json_response(StatusCode::OK, body),
        Err(error) => error_response(&error),
    }
}

fn percent_decode(text: &str) -> Result<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok());
            let byte = hex.and_then(|h| u8::from_str_radix(h, 16).ok());
            out.push(byte.ok_or_else(|| invalid("malformed percent-encoding in the path"))?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| invalid("the path is not UTF-8"))
}

/// Everything below `/v1/databases/<name>`: decide who serves it, then serve.
async fn database(
    State(app): State<Arc<App>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    // Bodies are held in memory, so their total is budgeted before one is
    // read: many large requests at once are refused instead of exhausting
    // the server. A body without a declared length is charged the maximum.
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(app.limits.max_body);
    if declared > app.limits.max_body {
        // Answering while the client is still sending would close the
        // connection under it, and it would see a broken pipe instead of
        // this error. Discard what it sends, within bounds, then answer.
        use futures::StreamExt;
        let limit = app.limits.max_body.saturating_mul(2);
        let discard = async {
            let mut stream = body.into_data_stream();
            let mut seen = 0usize;
            while let Some(Ok(chunk)) = stream.next().await {
                seen += chunk.len();
                if seen > limit {
                    break;
                }
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), discard).await;
        return error_response(&Error::TooLarge);
    }
    let kib = (declared.div_ceil(1024)).max(1) as u32;
    let Ok(_body_budget) = app.bodies.try_acquire_many(kib) else {
        app.node
            .metrics
            .rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return error_response(&Error::Busy(
            "this node is holding too many request bodies; retry shortly".into(),
        ));
    };
    let Ok(body) = axum::body::to_bytes(body, app.limits.max_body).await else {
        return error_response(&Error::TooLarge);
    };
    let Ok(_permit) = app.inflight.try_acquire() else {
        app.node
            .metrics
            .rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return error_response(&Error::Busy("this node is at its request limit".into()));
    };
    let path = uri
        .path()
        .strip_prefix("/v1/databases/")
        .unwrap_or_default();
    let (name, rest) = path.split_once('/').unwrap_or((path, ""));
    // The signature is checked before anything touches storage or a peer.
    let grant = match grant(&app, &headers) {
        Ok(grant) => grant,
        Err(error) => return error_response(&error),
    };
    let forwarded = headers.contains_key(FORWARDED);
    let mut last = Error::Busy("no attempt was made".into());
    for _attempt in 0..4 {
        if !forwarded
            && !app.node.serving(name)
            && let Some(peer) = app.cluster.owner(name)
        {
            let target = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
            if let Some(response) = app
                .cluster
                .forward(&peer, method.clone(), target, &headers, body.clone())
                .await
            {
                app.node
                    .metrics
                    .forwarded
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return response;
            }
        }
        match serve(&app, &grant, &method, name, rest, uri.query(), &body).await {
            Ok(response) => return response,
            // Ownership moved or is moving. Route again: the database may
            // now belong to a peer, or be ours to reopen.
            Err(Served {
                error: error @ (Error::Lost(_) | Error::Busy(_)),
                repeatable: true,
            }) => last = error,
            Err(Served { error, .. }) => return error_response(&error),
        }
    }
    error_response(&last)
}

struct Served {
    error: Error,
    /// Whether running the request again cannot apply it twice.
    repeatable: bool,
}

impl From<Error> for Served {
    fn from(error: Error) -> Self {
        Self {
            error,
            repeatable: true,
        }
    }
}

async fn serve(
    app: &App,
    grant: &Grant,
    method: &Method,
    name: &str,
    rest: &str,
    query: Option<&str>,
    body: &Bytes,
) -> Result<Response, Served> {
    let node = &app.node;
    let started = Instant::now();
    let allow = |needed: Access| {
        if grant.allows(name, needed) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    };
    // A token minted before the database last revoked its tokens is void.
    let current = |config: &DbConfig| {
        if grant.is_admin() || grant.generation >= config.token_generation {
            node.admit(name, config)
        } else {
            Err(Error::Unauthorized)
        }
    };
    let observe = |histogram: &crate::metrics::Histogram| histogram.observe(started.elapsed());
    match (method.as_str(), rest) {
        ("PUT", "") => {
            allow(Access::Admin)?;
            let options: CreateBody = parse_or_default(body)?;
            let config = DbConfig {
                max_bytes: options.max_bytes.unwrap_or(DbConfig::default().max_bytes),
                max_requests_per_second: options.max_requests_per_second.unwrap_or(0),
                token_generation: 0,
            };
            let (info, created) = node.create(name, config).await?;
            observe(&node.metrics.admin);
            let status = if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            Ok(json_response(status, info_json(name, &info)))
        }
        ("GET", "") => {
            allow(Access::Read)?;
            let info = node.info(name).await?;
            current(&info.config)?;
            observe(&node.metrics.admin);
            Ok(json_response(StatusCode::OK, info_json(name, &info)))
        }
        ("PATCH", "") => {
            allow(Access::Admin)?;
            let change: ConfigureBody = parse(body)?;
            let info = node
                .configure(name, |config| {
                    if let Some(max_bytes) = change.max_bytes {
                        config.max_bytes = max_bytes;
                    }
                    if let Some(rate) = change.max_requests_per_second {
                        config.max_requests_per_second = rate;
                    }
                    if change.revoke_tokens {
                        config.token_generation += 1;
                    }
                })
                .await?;
            observe(&node.metrics.admin);
            Ok(json_response(StatusCode::OK, info_json(name, &info)))
        }
        ("DELETE", "") => {
            allow(Access::Admin)?;
            node.delete(name).await?;
            observe(&node.metrics.admin);
            Ok(json_response(
                StatusCode::OK,
                json!({"deleted": name}).to_string(),
            ))
        }
        ("POST", "tokens") => {
            allow(Access::Admin)?;
            let request: TokenBody = parse(body)?;
            if request.access == Access::Admin {
                return Err(invalid("a database token grants read or write access").into());
            }
            let info = node.info(name).await?;
            let token = app.keys.mint(&Grant {
                db: name.to_owned(),
                access: request.access,
                exp: expiry(request.ttl_seconds.unwrap_or(0)),
                generation: info.config.token_generation,
            });
            observe(&node.metrics.admin);
            Ok(json_response(
                StatusCode::OK,
                json!({"token": token}).to_string(),
            ))
        }
        ("POST", "read") => {
            allow(Access::Read)?;
            let request = read_request(app, parse(body)?)?;
            let db = node.db(name).await?;
            current(&db.config())?;
            let response = db
                .read(request.0)
                .await
                .inspect_err(|_| node.metrics.reads.fail())?;
            observe(&node.metrics.reads);
            let bytes: usize = response
                .values
                .iter()
                .flatten()
                .map(|(_, value)| value.len())
                .chain(
                    response
                        .scans
                        .iter()
                        .flat_map(|scan| scan.items.iter().map(|e| e.weight())),
                )
                .sum();
            if bytes > app.limits.max_read_bytes {
                return Err(invalid(format!(
                    "the result is {bytes} bytes; one read returns at most {}. Ask for fewer keys or scans",
                    app.limits.max_read_bytes
                ))
                .into());
            }
            Ok(json_response(
                StatusCode::OK,
                read_json(&response, &request.1),
            ))
        }
        ("POST", "commit") => {
            allow(Access::Write)?;
            let request: CommitBody = parse(body)?;
            let single = request.transactions.is_none();
            let txns = match request.transactions {
                Some(transactions) => {
                    if !request.writes.is_empty()
                        || !request.checks.is_empty()
                        || request.if_revision.is_some()
                        || request.idempotency_key.is_some()
                    {
                        return Err(invalid(
                            "send either one transaction or \"transactions\", not both",
                        )
                        .into());
                    }
                    transactions
                        .iter()
                        .map(|txn| transaction(app, body, txn))
                        .collect::<Result<Vec<_>>>()?
                }
                None => {
                    let only = TxnBody {
                        checks: request.checks,
                        writes: request.writes,
                        if_revision: request.if_revision,
                        idempotency_key: request.idempotency_key,
                    };
                    vec![transaction(app, body, &only)?]
                }
            };
            let repeatable = txns.iter().all(|txn| txn.idempotency_key.is_some());
            let db = node.db(name).await?;
            current(&db.config())?;
            let results = db.commit(txns).await.map_err(|error| {
                node.metrics.commits.fail();
                Served { error, repeatable }
            })?;
            observe(&node.metrics.commits);
            Ok(commit_response(results, single, db.newest_revision()))
        }
        (verb @ ("GET" | "PUT" | "DELETE"), path) if path.starts_with("keys/") => {
            let key = percent_decode(&path["keys/".len()..])?;
            valid_key(app, &key)?;
            let key_bytes = Bytes::from(key.into_bytes());
            let db = async {
                let db = node.db(name).await?;
                current(&db.config())?;
                Ok::<_, Error>(db)
            };
            if verb == "GET" {
                allow(Access::Read)?;
                let strong =
                    !query.is_some_and(|q| q.split('&').any(|p| p == "consistency=session"));
                let request = ReadReq {
                    keys: vec![key_bytes],
                    strong,
                    ..ReadReq::default()
                };
                let response = db
                    .await?
                    .read(request)
                    .await
                    .inspect_err(|_| node.metrics.reads.fail())?;
                observe(&node.metrics.reads);
                let Some((version, value)) = response.values.into_iter().next().flatten() else {
                    return Err(Error::NotFound.into());
                };
                let headers = [
                    ("content-type", "application/json".to_owned()),
                    ("x-ddb-version", version.to_string()),
                    ("x-ddb-revision", response.revision.to_string()),
                ];
                return Ok((headers, value).into_response());
            }
            allow(Access::Write)?;
            let value = if verb == "PUT" {
                let raw: &RawValue = serde_json::from_slice(body)
                    .map_err(|e| invalid(format!("the body is not JSON: {e}")))?;
                Some(stored_value(app, body, raw)?)
            } else {
                None
            };
            let txn = Txn {
                writes: vec![(key_bytes, value)],
                ..Txn::default()
            };
            let db = db.await?;
            let results = db.commit(vec![txn]).await.map_err(|error| {
                node.metrics.commits.fail();
                Served {
                    error,
                    repeatable: false,
                }
            })?;
            observe(&node.metrics.commits);
            Ok(commit_response(results, true, db.newest_revision()))
        }
        _ => Err(Error::Invalid(format!(
            "no such endpoint: {method} /v1/databases/{name}/{rest}"
        ))
        .into()),
    }
}

fn info_json(name: &str, info: &Info) -> String {
    json!({
        "name": name,
        "revision": info.revision,
        "bytes": info.bytes,
        "max_bytes": info.config.max_bytes,
        "max_requests_per_second": info.config.max_requests_per_second,
        "tables": info.tables,
    })
    .to_string()
}

fn parse<'a, T: Deserialize<'a>>(body: &'a Bytes) -> Result<T> {
    serde_json::from_slice(body).map_err(|e| invalid(format!("malformed request: {e}")))
}

fn parse_or_default<'a, T: Deserialize<'a> + Default>(body: &'a Bytes) -> Result<T> {
    if body.iter().all(u8::is_ascii_whitespace) {
        Ok(T::default())
    } else {
        parse(body)
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    max_bytes: Option<u64>,
    max_requests_per_second: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigureBody {
    max_bytes: Option<u64>,
    max_requests_per_second: Option<u32>,
    #[serde(default)]
    revoke_tokens: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenBody {
    access: Access,
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum Consistency {
    #[default]
    Strong,
    Session,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadBody {
    #[serde(default)]
    keys: Vec<String>,
    #[serde(default)]
    scans: Vec<ScanBody>,
    #[serde(default)]
    consistency: Consistency,
    #[serde(default)]
    min_revision: u64,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ScanBody {
    prefix: Option<String>,
    /// Inclusive.
    start: Option<String>,
    /// Exclusive.
    end: Option<String>,
    /// Continue past this key, in the scan's direction.
    after: Option<String>,
    #[serde(default)]
    reverse: bool,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CheckBody {
    Key { key: String, version: u64 },
    Range { range: ScanBody, revision: u64 },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody<'a> {
    key: String,
    #[serde(borrow)]
    value: Option<&'a RawValue>,
    #[serde(default)]
    delete: bool,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TxnBody<'a> {
    #[serde(default)]
    checks: Vec<CheckBody>,
    #[serde(default, borrow)]
    writes: Vec<WriteBody<'a>>,
    if_revision: Option<u64>,
    idempotency_key: Option<String>,
}

/// One transaction at the top level, or several under `transactions`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitBody<'a> {
    #[serde(borrow)]
    transactions: Option<Vec<TxnBody<'a>>>,
    #[serde(default)]
    checks: Vec<CheckBody>,
    #[serde(default, borrow)]
    writes: Vec<WriteBody<'a>>,
    if_revision: Option<u64>,
    idempotency_key: Option<String>,
}

fn valid_key(app: &App, key: &str) -> Result<()> {
    if key.is_empty() || key.len() > app.limits.max_key {
        return Err(invalid(format!(
            "a key is 1 to {} bytes",
            app.limits.max_key
        )));
    }
    if key.as_bytes()[0] == INTERNAL {
        return Err(invalid("a key cannot begin with U+0000"));
    }
    Ok(())
}

/// The smallest byte string greater than every string with this prefix;
/// empty when there is none.
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            break;
        }
    }
    end
}

/// The byte range `lo..hi` a scan description selects.
fn bounds(scan: &ScanBody) -> (Bytes, Bytes) {
    // Internal keys sort first; client scans begin after them.
    let mut lo = vec![INTERNAL + 1];
    let mut hi = Vec::new();
    let raise = |lo: &mut Vec<u8>, candidate: Vec<u8>| {
        if candidate > *lo {
            *lo = candidate;
        }
    };
    let lower = |hi: &mut Vec<u8>, candidate: Vec<u8>| {
        if hi.is_empty() || (!candidate.is_empty() && candidate < *hi) {
            *hi = candidate;
        }
    };
    if let Some(prefix) = &scan.prefix {
        raise(&mut lo, prefix.as_bytes().to_vec());
        lower(&mut hi, prefix_end(prefix.as_bytes()));
    }
    if let Some(start) = &scan.start {
        raise(&mut lo, start.as_bytes().to_vec());
    }
    if let Some(end) = &scan.end {
        lower(&mut hi, end.as_bytes().to_vec());
    }
    if let Some(after) = &scan.after {
        if scan.reverse {
            lower(&mut hi, after.as_bytes().to_vec());
        } else {
            let mut next = after.as_bytes().to_vec();
            next.push(0);
            raise(&mut lo, next);
        }
    }
    (Bytes::from(lo), Bytes::from(hi))
}

fn read_request(app: &App, body: ReadBody) -> Result<(ReadReq, Vec<usize>)> {
    if body.keys.len() + body.scans.len() > app.limits.max_lookups {
        return Err(invalid(format!(
            "a read has at most {} keys and scans",
            app.limits.max_lookups
        )));
    }
    let mut limits = Vec::with_capacity(body.scans.len());
    let mut scans = Vec::with_capacity(body.scans.len());
    for scan in &body.scans {
        let limit = scan.limit.unwrap_or(100);
        if limit == 0 || limit > app.limits.max_scan_items {
            return Err(invalid(format!(
                "a scan limit is 1 to {}",
                app.limits.max_scan_items
            )));
        }
        let (lo, hi) = bounds(scan);
        limits.push(limit);
        scans.push(Scan {
            lo,
            hi,
            reverse: scan.reverse,
            limit,
        });
    }
    for key in &body.keys {
        valid_key(app, key)?;
    }
    Ok((
        ReadReq {
            keys: body
                .keys
                .into_iter()
                .map(|key| Bytes::from(key.into_bytes()))
                .collect(),
            scans,
            strong: matches!(body.consistency, Consistency::Strong),
            min_revision: body.min_revision,
        },
        limits,
    ))
}

/// The stored form of a JSON value: its text exactly as sent, sharing the
/// request buffer.
fn stored_value(app: &App, body: &Bytes, raw: &RawValue) -> Result<Bytes> {
    let text = raw.get();
    if text.len() > app.limits.max_value {
        return Err(invalid(format!(
            "a value is at most {} bytes",
            app.limits.max_value
        )));
    }
    if text == "null" {
        return Err(invalid("null cannot be stored; delete the key instead"));
    }
    Ok(body.slice_ref(text.as_bytes()))
}

fn transaction(app: &App, body: &Bytes, txn: &TxnBody) -> Result<Txn> {
    if txn.writes.len() + txn.checks.len() > app.limits.max_operations {
        return Err(invalid(format!(
            "a transaction has at most {} writes and checks",
            app.limits.max_operations
        )));
    }
    if txn.writes.is_empty() {
        return Err(invalid("a transaction needs at least one write"));
    }
    let mut writes = Vec::with_capacity(txn.writes.len());
    let mut seen = std::collections::HashSet::with_capacity(txn.writes.len());
    for write in &txn.writes {
        valid_key(app, &write.key)?;
        if !seen.insert(write.key.as_str()) {
            return Err(invalid(format!(
                "key {:?} is written twice in one transaction",
                write.key
            )));
        }
        let value = match (write.value, write.delete) {
            (Some(raw), false) => Some(stored_value(app, body, raw)?),
            (None, true) => None,
            _ => return Err(invalid("a write has either a value or \"delete\": true")),
        };
        writes.push((Bytes::copy_from_slice(write.key.as_bytes()), value));
    }
    let mut checks = Vec::with_capacity(txn.checks.len());
    for check in &txn.checks {
        checks.push(match check {
            CheckBody::Key { key, version } => {
                valid_key(app, key)?;
                Check::Key {
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    version: *version,
                }
            }
            CheckBody::Range { range, revision } => {
                let (lo, hi) = bounds(range);
                Check::Range {
                    lo,
                    hi,
                    since: *revision,
                }
            }
        });
    }
    if let Some(key) = &txn.idempotency_key
        && (key.is_empty() || key.len() > 255)
    {
        return Err(invalid("an idempotency key is 1 to 255 bytes"));
    }
    Ok(Txn {
        checks,
        writes,
        if_revision: txn.if_revision,
        idempotency_key: txn.idempotency_key.clone(),
    })
}

/// A refusal, with the revision a retry should read at when it is a conflict.
fn refusal_json(error: &Error, newest: u64) -> String {
    let mut body = serde_json::from_str::<serde_json::Value>(&error_json(error, error_code(error)))
        .expect("error bodies are JSON");
    if matches!(error, Error::Conflict(_)) {
        body["error"]["revision"] = newest.into();
    }
    body.to_string()
}

fn commit_response(
    results: Vec<Result<crate::db::Committed>>,
    single: bool,
    newest: u64,
) -> Response {
    if single {
        return match results.into_iter().next() {
            Some(Ok(committed)) => json_response(
                StatusCode::OK,
                format!("{{\"revision\":{}}}", committed.revision),
            ),
            Some(Err(error @ Error::Conflict(_))) => {
                json_response(StatusCode::CONFLICT, refusal_json(&error, newest))
            }
            Some(Err(error)) => error_response(&error),
            None => error_response(&Error::Busy("no result".into())),
        };
    }
    let mut out = String::from("{\"results\":[");
    for (i, result) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match result {
            Ok(committed) => out.push_str(&format!("{{\"revision\":{}}}", committed.revision)),
            Err(error) => out.push_str(&refusal_json(error, newest)),
        }
    }
    out.push_str("]}");
    json_response(StatusCode::OK, out)
}

fn push_key(out: &mut Vec<u8>, key: &[u8]) {
    // Keys are UTF-8 by construction; escape them as JSON strings.
    let text = String::from_utf8_lossy(key);
    out.extend_from_slice(
        serde_json::to_string(text.as_ref())
            .expect("strings encode")
            .as_bytes(),
    );
}

/// Values are spliced in as stored: they are already JSON text.
fn read_json(response: &crate::db::ReadResp, limits: &[usize]) -> String {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(format!("{{\"revision\":{},\"values\":[", response.revision).as_bytes());
    for (i, value) in response.values.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        match value {
            Some((version, value)) => {
                out.extend_from_slice(b"{\"value\":");
                out.extend_from_slice(value);
                out.extend_from_slice(format!(",\"version\":{version}}}").as_bytes());
            }
            None => out.extend_from_slice(b"null"),
        }
    }
    out.extend_from_slice(b"],\"scans\":[");
    for (i, scan) in response.scans.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(b"{\"items\":[");
        for (j, entry) in scan.items.iter().enumerate() {
            if j > 0 {
                out.push(b',');
            }
            out.extend_from_slice(b"{\"key\":");
            push_key(&mut out, &entry.key);
            out.extend_from_slice(b",\"value\":");
            out.extend_from_slice(entry.value.as_deref().unwrap_or(b"null"));
            out.extend_from_slice(format!(",\"version\":{}}}", entry.version).as_bytes());
        }
        out.extend_from_slice(b"],\"next\":");
        let _ = limits;
        match scan.items.last().filter(|_| scan.more) {
            Some(last) => push_key(&mut out, &last.key),
            None => out.extend_from_slice(b"null"),
        }
        out.push(b'}');
    }
    out.extend_from_slice(b"]}");
    // Every piece is UTF-8: stored values were validated as JSON text.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(
        prefix: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        after: Option<&str>,
        reverse: bool,
    ) -> (Bytes, Bytes) {
        let own = |s: Option<&str>| s.map(str::to_owned);
        bounds(&ScanBody {
            prefix: own(prefix),
            start: own(start),
            end: own(end),
            after: own(after),
            reverse,
            limit: None,
        })
    }

    #[test]
    fn scan_bounds_combine_prefix_range_and_cursor() {
        assert_eq!(
            scan(None, None, None, None, false),
            (Bytes::from_static(&[1]), Bytes::new())
        );
        assert_eq!(
            scan(Some("a/"), None, None, None, false),
            (Bytes::from("a/"), Bytes::from("a0"))
        );
        assert_eq!(
            scan(Some("a/"), Some("a/m"), Some("a/t"), None, false),
            (Bytes::from("a/m"), Bytes::from("a/t"))
        );
        // A range wider than the prefix is clipped to it.
        assert_eq!(
            scan(Some("a/"), Some("A"), Some("z"), None, false),
            (Bytes::from("a/"), Bytes::from("a0"))
        );
        assert_eq!(
            scan(Some("a/"), None, None, Some("a/k"), false),
            (Bytes::from("a/k\0"), Bytes::from("a0"))
        );
        assert_eq!(
            scan(Some("a/"), None, None, Some("a/k"), true),
            (Bytes::from("a/"), Bytes::from("a/k"))
        );
        assert_eq!(prefix_end(&[b'a', 0xff, 0xff]), vec![b'b']);
        assert_eq!(prefix_end(&[0xff]), Vec::<u8>::new());
    }

    #[test]
    fn paths_decode() {
        assert_eq!(percent_decode("users%2F42%20x").unwrap(), "users/42 x");
        assert!(percent_decode("%zz").is_err());
        assert!(percent_decode("%ff").is_err());
    }
}
