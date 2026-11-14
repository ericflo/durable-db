//! The HTTP API. See `docs/api.md` for the contract this implements.
use crate::{
    Error, Result,
    auth::{Access, Grant, Keys, expiry},
    cluster::{Cluster, FORWARDED},
    db::{Check, Info, ReadReq, Room, Scan, Txn},
    layout::{ConfigChange, DbConfig, INTERNAL, NAME_RULE, valid_name},
    node::Node,
};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::{
    cell::Cell,
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct ApiLimits {
    pub max_body: usize,
    pub max_key: usize,
    pub max_value: usize,
    /// Writes plus checks in one commit request, across its transactions.
    pub max_operations: usize,
    /// Keys plus scans in one read.
    pub max_lookups: usize,
    pub max_scan_items: usize,
    /// Requests in flight on this node before new ones are refused.
    pub max_inflight: usize,
    /// Request bodies held in memory at once, across all requests. One
    /// database may hold a quarter of it.
    pub max_body_total: usize,
    /// Bytes one read may return: keys and values, and `ITEM_BYTES` an item.
    pub max_read_bytes: usize,
    /// Read results held in memory at once, from the moment they are built
    /// until the client has taken them. One database may hold a quarter.
    pub max_read_total: usize,
    /// Connections open at once; further ones wait to be accepted.
    pub max_connections: usize,
    /// How long a connection may take to send the head of a request, which
    /// is also how long it may stay idle between requests.
    pub head_timeout: Duration,
    /// How long a request body may take to arrive, on top of one second for
    /// every 256 KiB of it.
    pub body_timeout: Duration,
    /// How long a client may go without taking any of an answer.
    pub stall_timeout: Duration,
    /// How long requests in flight are given to finish at shutdown.
    pub drain_timeout: Duration,
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
            max_read_total: 512 << 20,
            max_connections: 8192,
            head_timeout: Duration::from_secs(75),
            body_timeout: Duration::from_secs(30),
            stall_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// A budget of bytes that every database draws on, and of which one
/// database may hold only a part: a tenant that fills its part is refused
/// while the others carry on.
pub struct Shares {
    total: usize,
    each: usize,
    /// What is held in all, and by each database that holds anything.
    held: Mutex<(usize, HashMap<String, usize>)>,
}

/// Bytes taken from a budget. Dropping it gives them back.
pub struct Held {
    shares: Arc<Shares>,
    name: String,
    bytes: usize,
}

impl Shares {
    /// One database's part is a quarter of `total`, and at least `largest`:
    /// what a single request may need.
    pub fn new(total: usize, largest: usize) -> Arc<Self> {
        Arc::new(Self {
            total,
            each: (total / 4).max(largest),
            held: Mutex::default(),
        })
    }

    /// `None` when the budget, or this database's part of it, is spent.
    pub fn take(self: &Arc<Self>, name: &str, bytes: usize) -> Option<Held> {
        let mut held = Held {
            shares: self.clone(),
            name: name.to_owned(),
            bytes: 0,
        };
        held.resize(bytes).then_some(held)
    }

    /// Bytes held now, and by how many databases.
    pub fn in_use(&self) -> (usize, usize) {
        let held = self.held.lock();
        (held.0, held.1.len())
    }
}

impl Held {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Hold `bytes` instead. Asking for more fails, and changes nothing,
    /// when it is not there to be had.
    pub fn resize(&mut self, bytes: usize) -> bool {
        let mut held = self.shares.held.lock();
        let (all, each) = &mut *held;
        let mine = each.get(&self.name).copied().unwrap_or(0);
        let (all_now, mine_now) = if bytes >= self.bytes {
            let more = bytes - self.bytes;
            if *all + more > self.shares.total || mine + more > self.shares.each {
                return false;
            }
            (*all + more, mine + more)
        } else {
            let less = self.bytes - bytes;
            (*all - less, mine - less)
        };
        *all = all_now;
        if mine_now == 0 {
            each.remove(&self.name);
        } else if let Some(mine) = each.get_mut(&self.name) {
            *mine = mine_now;
        } else {
            each.insert(self.name.clone(), mine_now);
        }
        self.bytes = bytes;
        true
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.resize(0);
    }
}

/// Bytes that give back what they hold once they have been written out or
/// abandoned.
struct Kept {
    bytes: Bytes,
    _held: Held,
}

impl AsRef<[u8]> for Kept {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// A response body that keeps `held` until the client has taken it.
pub fn kept(bytes: Bytes, held: Option<Held>) -> Body {
    match held {
        Some(held) => Body::from(Bytes::from_owner(Kept { bytes, _held: held })),
        None => Body::from(bytes),
    }
}

/// A result this small is not reserved for. What is held without a
/// reservation is bounded by the number of connections.
pub const FREE: usize = 16 << 10;
/// Reservations grow by this much at a time.
const STEP: usize = 256 << 10;

pub struct App {
    pub node: Arc<Node>,
    pub keys: Keys,
    pub cluster: Arc<Cluster>,
    pub limits: ApiLimits,
    pub inflight: tokio::sync::Semaphore,
    /// Request bodies in memory.
    pub bodies: Arc<Shares>,
    /// Read results in memory.
    pub results: Arc<Shares>,
    /// What this node sends a peer to show that a request was relayed.
    relay: HeaderValue,
    /// When storage was last asked whether it answers, and what it said.
    probed: tokio::sync::Mutex<Option<(Instant, Result<()>)>>,
}

impl App {
    pub fn new(node: Arc<Node>, keys: Keys, cluster: Arc<Cluster>, limits: ApiLimits) -> Arc<Self> {
        Arc::new(Self {
            inflight: tokio::sync::Semaphore::new(limits.max_inflight),
            bodies: Shares::new(limits.max_body_total, limits.max_body),
            // A result is held twice while it is encoded.
            results: Shares::new(limits.max_read_total, 2 * limits.max_read_bytes),
            relay: HeaderValue::from_str(&keys.relay_proof()).expect("hex is a header value"),
            probed: tokio::sync::Mutex::default(),
            node,
            keys,
            cluster,
            limits,
        })
    }

    fn rejected(&self) {
        self.node
            .metrics
            .rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/databases", get(list))
        .route("/v1/databases/{*path}", any(database))
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

fn json_response(status: StatusCode, body: impl Into<Body>) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.into(),
    )
        .into_response()
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
    // Anyone may ask this, so storage is asked at most every other second,
    // and everyone asking meanwhile shares the answer.
    let mut probed = app.probed.lock().await;
    if probed
        .as_ref()
        .is_none_or(|(at, _)| at.elapsed() >= Duration::from_secs(2))
    {
        let answers = crate::io::retry(|| app.node.ctx.io.store.exists("ready-probe")).await;
        *probed = Some((Instant::now(), answers.map(drop)));
    }
    match probed.as_ref() {
        Some((_, Err(error))) => error_response(error),
        _ => "ready\n".into_response(),
    }
}

async fn metrics(State(app): State<Arc<App>>) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    let io = &app.node.ctx.io;
    let mut text = app.node.metrics.render(
        app.node.open_count(),
        io.cache_bytes(),
        io.block_hits.load(Relaxed),
        io.block_misses.load(Relaxed),
    );
    // What requests hold in memory right now, against the two budgets.
    text.push_str(&format!(
        "durable_db_request_body_bytes {}\ndurable_db_read_result_bytes {}\ndurable_db_table_index_bytes {}\ndurable_db_recent_key_bytes {}\n",
        app.bodies.in_use().0,
        app.results.in_use().0,
        io.index_bytes.load(Relaxed),
        app.node.recent_bytes(),
    ));
    text.into_response()
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

/// Refuse a path that an HTTP client or a proxy may rewrite on the way:
/// `.` and `..` segments, however they are encoded, and backslashes. A
/// relayed request must name the same database and key on every node.
fn plain(path: &str) -> Result<()> {
    let dots = |segment: &str| {
        let (mut rest, mut dots) = (segment.as_bytes(), 0);
        loop {
            rest = match rest {
                [b'.', rest @ ..] | [b'%', b'2', b'e' | b'E', rest @ ..] => rest,
                [] => return dots == 1 || dots == 2,
                _ => return false,
            };
            dots += 1;
        }
    };
    if path.contains('\\') || path.split('/').any(dots) {
        return Err(invalid(
            "a path cannot hold a backslash or a segment that is \".\" or \"..\"; \
             reach such a key through /read and /commit",
        ));
    }
    Ok(())
}

/// What must hold before a request's body is read or a peer is asked to
/// serve it: who is calling, that the path names a database they may use,
/// and how much they say they will send.
fn admit(app: &App, uri: &Uri, name: &str, headers: &HeaderMap) -> Result<(Grant, usize)> {
    let grant = grant(app, headers)?;
    plain(uri.path())?;
    if !valid_name(name) {
        return Err(invalid(NAME_RULE));
    }
    if !grant.allows(name, Access::Read) {
        return Err(Error::Forbidden);
    }
    // A body whose length is not declared is reserved for as it arrives.
    let declared = match headers.get(header::CONTENT_LENGTH) {
        Some(length) => length
            .to_str()
            .ok()
            .and_then(|length| length.parse::<usize>().ok())
            .ok_or_else(|| invalid("Content-Length is not a number"))?,
        None => 0,
    };
    if declared > app.limits.max_body {
        return Err(Error::TooLarge);
    }
    Ok((grant, declared))
}

/// Refuse a request whose body has not been read. What the client is still
/// sending is taken, within bounds, and dropped: answering while it sends
/// would close the connection under it, and it would see a broken pipe
/// instead of the refusal.
async fn refuse(app: &App, error: Error, body: Body) -> Response {
    let most = app.limits.max_body.saturating_mul(2);
    let taking = async {
        let mut chunks = body.into_data_stream();
        let mut seen = 0usize;
        while let Some(Ok(chunk)) = chunks.next().await {
            seen += chunk.len();
            if seen > most {
                break;
            }
        }
    };
    let patience = app.limits.body_timeout.min(Duration::from_secs(10));
    let _ = tokio::time::timeout(patience, taking).await;
    error_response(&error)
}

/// Read a request body into memory. `held` already covers a declared
/// length; beyond it, the body is reserved for as it arrives.
async fn receive(app: &App, body: Body, held: &mut Held) -> Result<Bytes> {
    let most = app.limits.max_body;
    let mut chunks = body.into_data_stream();
    let mut parts: Vec<Bytes> = Vec::new();
    let mut size = 0usize;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| invalid("the request body did not arrive whole"))?;
        size += chunk.len();
        if size > most {
            return Err(Error::TooLarge);
        }
        if size > held.bytes() && !held.resize(size.next_multiple_of(STEP).min(most)) {
            return Err(Error::Busy(BODIES_FULL.into()));
        }
        parts.push(chunk);
    }
    if parts.len() <= 1 {
        return Ok(parts.pop().unwrap_or_default());
    }
    let mut whole = Vec::with_capacity(size);
    for part in parts {
        whole.extend_from_slice(&part);
    }
    Ok(whole.into())
}

const BODIES_FULL: &str = "this node is holding too many request bodies; retry shortly";

/// Everything below `/v1/databases/<name>`: decide who serves it, then serve.
async fn database(
    State(app): State<Arc<App>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let path = uri
        .path()
        .strip_prefix("/v1/databases/")
        .unwrap_or_default();
    let (name, rest) = path.split_once('/').unwrap_or((path, ""));
    // Nothing is read, held or asked of storage or a peer for a caller
    // without a token for this database.
    let (grant, declared) = match admit(&app, &uri, name, &headers) {
        Ok(admitted) => admitted,
        Err(error) => return refuse(&app, error, body).await,
    };
    // Bodies are held in memory, so they are budgeted before they are read:
    // many large requests at once are refused instead of exhausting the
    // server, and one database cannot use up what the others need.
    let Some(mut held) = app.bodies.take(name, declared) else {
        app.rejected();
        return refuse(&app, Error::Busy(BODIES_FULL.into()), body).await;
    };
    // A second for every 256 KiB that may arrive, on top of the allowance.
    let expected = if headers.contains_key(header::CONTENT_LENGTH) {
        declared
    } else {
        app.limits.max_body
    };
    let patience = app.limits.body_timeout + Duration::from_secs((expected >> 18) as u64);
    let body = match tokio::time::timeout(patience, receive(&app, body, &mut held)).await {
        Ok(Ok(body)) => body,
        Ok(Err(error)) => {
            if matches!(error, Error::Busy(_)) {
                app.rejected();
            }
            return error_response(&error);
        }
        Err(_) => {
            return json_response(
                StatusCode::REQUEST_TIMEOUT,
                json!({"error": {
                    "code": "timeout",
                    "message": "the request body did not arrive in time",
                    "retryable": true,
                }})
                .to_string(),
            );
        }
    };
    let Ok(_permit) = app.inflight.try_acquire() else {
        app.rejected();
        return error_response(&Error::Busy("this node is at its request limit".into()));
    };
    // Only a peer that holds the signing key can say that it relayed this.
    let forwarded = headers
        .get(FORWARDED)
        .is_some_and(|proof| app.keys.relayed(proof.as_bytes()));
    // A relayed answer to a read is held here until the client takes it.
    // Refusing one is harmless; the answer to a commit is small, and is
    // always passed on.
    let reads = method == Method::GET || rest == "read";
    let hold = reads.then_some((&app.results, name));
    let mut last = Error::Busy("no attempt was made".into());
    for attempt in 0..4u32 {
        // A handover takes a moment; give it one before trying again.
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(15 << attempt)).await;
        }
        if !forwarded
            && !app.node.serving(name)
            && let Some(peer) = app.cluster.owner(name)
        {
            let target = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
            if let Some(response) = app
                .cluster
                .forward(
                    &peer,
                    method.clone(),
                    target,
                    &headers,
                    body.clone(),
                    &app.relay,
                    hold,
                )
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

pub(crate) const RESULTS_FULL: &str = "this node is holding too many read results; retry shortly";

/// What a read's result holds, reserved as it is built: nothing while it is
/// small, then in steps, and twice over, because the result and its
/// encoding exist side by side for a moment.
struct Reserve<'a> {
    app: &'a App,
    name: &'a str,
    held: Mutex<Option<Held>>,
    /// Set when the read was stopped for want of room.
    full: std::sync::atomic::AtomicBool,
}

impl<'a> Reserve<'a> {
    fn new(app: &'a App, name: &'a str) -> Self {
        Self {
            app,
            name,
            held: Mutex::new(None),
            full: Default::default(),
        }
    }

    fn too_much(&self) -> Error {
        invalid(format!(
            "the result is larger than the {} bytes one read returns; ask for fewer keys or scans",
            self.app.limits.max_read_bytes
        ))
    }

    /// Why a read failed. One that was stopped here is not tried again
    /// as one that lost its database is: the answer would be the same.
    fn refusal(&self, error: Error) -> Served {
        Served {
            error: match error {
                Error::TooLarge => self.too_much(),
                other => other,
            },
            repeatable: !self.full.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// The reservation for the `size` bytes that are sent, the rest given
    /// back.
    fn settle(self, size: usize) -> Option<Held> {
        let mut held = self.held.into_inner()?;
        let _ = held.resize(size);
        Some(held)
    }
}

impl Room for Reserve<'_> {
    fn room(&self, size: usize) -> Result<usize> {
        let most = self.app.limits.max_read_bytes;
        if size > most {
            return Err(self.too_much());
        }
        if size > FREE {
            let want = 2 * size;
            let mut held = self.held.lock();
            let enough = match held.as_mut() {
                Some(held) => held.bytes() >= want || held.resize(want.next_multiple_of(STEP)),
                None => {
                    *held = self
                        .app
                        .results
                        .take(self.name, want.next_multiple_of(STEP));
                    held.is_some()
                }
            };
            if !enough {
                self.app.rejected();
                self.full.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(Error::Busy(RESULTS_FULL.into()));
            }
        }
        Ok(most - size)
    }
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
            let change = ConfigChange {
                max_bytes: change.max_bytes,
                max_requests_per_second: change.max_requests_per_second,
                revoke_tokens: change.revoke_tokens,
            };
            let info = node.configure(name, change).await?;
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
            let most = app.limits.max_lookups;
            let request = read_request(
                app,
                parse_within(body, most, || {
                    format!("a read has at most {most} keys and scans")
                })?,
            )?;
            let db = node.db(name).await?;
            current(&db.config())?;
            let reserve = Reserve::new(app, name);
            let response = db
                .read_within(request, &reserve)
                .await
                .map_err(|error| reserve.refusal(error))
                .inspect_err(|_| node.metrics.reads.fail())?;
            observe(&node.metrics.reads);
            let text = Bytes::from(read_json(&response));
            drop(response);
            let held = reserve.settle(text.len());
            Ok(json_response(StatusCode::OK, kept(text, held)))
        }
        ("POST", "commit") => {
            allow(Access::Write)?;
            let most = app.limits.max_operations;
            let too_many = || format!("a commit has at most {most} writes and checks");
            // Each transaction needs a write, so a request within the limit
            // holds at most twice as many items as it allows operations.
            let request: CommitBody = parse_within(body, most.saturating_mul(2), too_many)?;
            let single = request.transactions.is_none();
            let operations = request.writes.len()
                + request.checks.len()
                + request
                    .transactions
                    .iter()
                    .flat_map(|all| all.iter())
                    .fold(0usize, |sum, txn| sum + txn.writes.len() + txn.checks.len());
            if operations > most {
                return Err(invalid(too_many()).into());
            }
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
                        .map(|txn| transaction(app, txn))
                        .collect::<Result<Vec<_>>>()?
                }
                None => {
                    let only = TxnBody {
                        checks: request.checks,
                        writes: request.writes,
                        if_revision: request.if_revision,
                        idempotency_key: request.idempotency_key,
                    };
                    vec![transaction(app, &only)?]
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
                let reserve = Reserve::new(app, name);
                let response = db
                    .await?
                    .read_within(request, &reserve)
                    .await
                    .map_err(|error| reserve.refusal(error))
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
                let held = reserve.settle(value.len());
                return Ok((headers, kept(value, held)).into_response());
            }
            allow(Access::Write)?;
            let value = if verb == "PUT" {
                let raw: &RawValue = serde_json::from_slice(body)
                    .map_err(|e| invalid(format!("the body is not JSON: {e}")))?;
                Some(stored_value(app, raw)?)
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

thread_local! {
    /// How many more array items the request being parsed on this thread
    /// may hold, and whether it tried to hold more.
    static ITEMS: Cell<(usize, bool)> = const { Cell::new((usize::MAX, false)) };
}

/// Parse a request whose arrays hold at most `most` items between them.
/// The count is kept while parsing, so a body made of millions of small
/// items is refused without being turned into as many values first.
fn parse_within<'a, T: Deserialize<'a>>(
    body: &'a Bytes,
    most: usize,
    rule: impl FnOnce() -> String,
) -> Result<T> {
    ITEMS.set((most, false));
    let parsed = parse(body);
    let (_, exceeded) = ITEMS.replace((usize::MAX, false));
    if exceeded {
        return Err(invalid(rule()));
    }
    parsed
}

/// An array that counts its items against the request's allowance.
struct Capped<T>(Vec<T>);

impl<T> Default for Capped<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T> std::ops::Deref for Capped<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Capped<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Items<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Items<T> {
            type Value = Capped<T>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    let (left, _) = ITEMS.get();
                    if left == 0 {
                        ITEMS.set((0, true));
                        return Err(serde::de::Error::custom("too many items"));
                    }
                    ITEMS.set((left - 1, false));
                    items.push(item);
                }
                Ok(Capped(items))
            }
        }
        deserializer.deserialize_seq(Items(PhantomData))
    }
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
    keys: Capped<String>,
    #[serde(default)]
    scans: Capped<ScanBody>,
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

/// `{"key", "version"}` or `{"range", "revision"}`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckBody {
    key: Option<String>,
    version: Option<u64>,
    range: Option<ScanBody>,
    revision: Option<u64>,
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
    checks: Capped<CheckBody>,
    #[serde(default, borrow)]
    writes: Capped<WriteBody<'a>>,
    if_revision: Option<u64>,
    idempotency_key: Option<String>,
}

/// One transaction at the top level, or several under `transactions`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitBody<'a> {
    #[serde(borrow)]
    transactions: Option<Capped<TxnBody<'a>>>,
    #[serde(default)]
    checks: Capped<CheckBody>,
    #[serde(default, borrow)]
    writes: Capped<WriteBody<'a>>,
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

fn read_request(app: &App, body: ReadBody) -> Result<ReadReq> {
    let mut scans = Vec::with_capacity(body.scans.len());
    for scan in body.scans.iter() {
        let limit = scan.limit.unwrap_or(100);
        if limit == 0 || limit > app.limits.max_scan_items {
            return Err(invalid(format!(
                "a scan limit is 1 to {}",
                app.limits.max_scan_items
            )));
        }
        let (lo, hi) = bounds(scan);
        scans.push(Scan {
            lo,
            hi,
            reverse: scan.reverse,
            limit,
        });
    }
    for key in body.keys.iter() {
        valid_key(app, key)?;
    }
    Ok(ReadReq {
        keys: body
            .keys
            .0
            .into_iter()
            .map(|key| Bytes::from(key.into_bytes()))
            .collect(),
        scans,
        strong: matches!(body.consistency, Consistency::Strong),
        min_revision: body.min_revision,
    })
}

fn stored_value(app: &App, raw: &RawValue) -> Result<Bytes> {
    stored(&app.limits, raw)
}

/// The stored form of a JSON value: its text exactly as sent. A copy, so
/// that a stored value does not keep the whole request it came in alive.
fn stored(limits: &ApiLimits, raw: &RawValue) -> Result<Bytes> {
    let text = raw.get();
    if text.len() > limits.max_value {
        return Err(invalid(format!(
            "a value is at most {} bytes",
            limits.max_value
        )));
    }
    if text == "null" {
        return Err(invalid("null cannot be stored; delete the key instead"));
    }
    Ok(Bytes::copy_from_slice(text.as_bytes()))
}

fn transaction(app: &App, txn: &TxnBody) -> Result<Txn> {
    if txn.writes.is_empty() {
        return Err(invalid("a transaction needs at least one write"));
    }
    let mut writes = Vec::with_capacity(txn.writes.len());
    let mut seen = std::collections::HashSet::with_capacity(txn.writes.len());
    for write in txn.writes.iter() {
        valid_key(app, &write.key)?;
        if !seen.insert(write.key.as_str()) {
            return Err(invalid(format!(
                "key {:?} is written twice in one transaction",
                write.key
            )));
        }
        let value = match (write.value, write.delete) {
            (Some(raw), false) => Some(stored_value(app, raw)?),
            (None, true) => None,
            _ => return Err(invalid("a write has either a value or \"delete\": true")),
        };
        writes.push((Bytes::copy_from_slice(write.key.as_bytes()), value));
    }
    let mut checks = Vec::with_capacity(txn.checks.len());
    for check in txn.checks.iter() {
        checks.push(match check {
            CheckBody {
                key: Some(key),
                version: Some(version),
                range: None,
                revision: None,
            } => {
                valid_key(app, key)?;
                Check::Key {
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    version: *version,
                }
            }
            CheckBody {
                key: None,
                version: None,
                range: Some(range),
                revision: Some(revision),
            } => {
                let (lo, hi) = bounds(range);
                Check::Range {
                    lo,
                    hi,
                    since: *revision,
                }
            }
            _ => {
                return Err(invalid(
                    "a check has a \"key\" and its \"version\", or a \"range\" and a \"revision\"",
                ));
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
fn read_json(response: &crate::db::ReadResp) -> String {
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
    fn a_budget_is_shared_and_no_database_takes_it_all() {
        // A database's part is a quarter: 250.
        let shares = Shares::new(1000, 100);
        let first = shares.take("a", 200).unwrap();
        assert!(shares.take("a", 51).is_none());
        let mut second = shares.take("a", 50).unwrap();
        assert!(!second.resize(51));
        assert_eq!(second.bytes(), 50);
        let others: Vec<Held> = ["b", "c"]
            .iter()
            .map(|name| shares.take(name, 250).unwrap())
            .collect();
        // What is left for a fourth is its part, and then nothing for a fifth.
        assert!(shares.take("d", 251).is_none());
        let fourth = shares.take("d", 250).unwrap();
        assert!(shares.take("e", 1).is_none());
        assert_eq!(shares.in_use(), (1000, 4));
        // Everything comes back: by shrinking, and by letting go.
        assert!(second.resize(10));
        assert_eq!(shares.in_use(), (960, 4));
        drop(first);
        drop(second);
        assert_eq!(shares.in_use(), (750, 3));
        assert!(shares.take("e", 250).is_some());
        drop((others, fourth));
        assert_eq!(shares.in_use(), (0, 0));
        // A part is never too small for one request of the largest size.
        assert!(Shares::new(1000, 400).take("a", 400).is_some());
        // Nothing is free: an empty reservation is one all the same.
        let nothing = shares.take("a", 0).unwrap();
        assert_eq!((nothing.bytes(), shares.in_use()), (0, (0, 0)));
    }

    #[test]
    fn a_path_that_may_be_rewritten_is_refused() {
        for path in [
            "/v1/databases/app/keys/a/b",
            "/v1/databases/app/keys/..a",
            "/v1/databases/app/keys/a..",
            "/v1/databases/app/keys/...",
            "/v1/databases/app/keys/%2e%2e%2e",
            "/v1/databases/app/keys/.a.",
            "/v1/databases/app/keys/a%2F..%2Fb",
            "/v1/databases/app/keys/a%5Cb",
            "/v1/databases/app/keys/%2",
            "/v1/databases/a.b/keys//x",
        ] {
            assert!(plain(path).is_ok(), "{path}");
        }
        for path in [
            "/v1/databases/app/keys/..",
            "/v1/databases/app/keys/.",
            "/v1/databases/app/keys/a/../b",
            "/v1/databases/app/keys/a/./b",
            "/v1/databases/app/keys/%2e",
            "/v1/databases/app/keys/%2E%2e",
            "/v1/databases/app/keys/.%2E/x",
            "/v1/databases/app/keys/%2e./x",
            "/v1/databases/app/keys/a\\b",
            "/v1/databases/../x",
        ] {
            assert!(plain(path).is_err(), "{path}");
        }
    }

    #[test]
    fn arrays_are_counted_while_they_are_parsed() {
        let body = Bytes::from(r#"{"keys":["a","b","c"],"scans":[{},{}]}"#);
        let rule = || "the rule".to_owned();
        let Ok(parsed) = parse_within::<ReadBody>(&body, 5, rule) else {
            panic!("five items are allowed");
        };
        assert_eq!((parsed.keys.len(), parsed.scans.len()), (3, 2));
        let Err(Error::Invalid(message)) = parse_within::<ReadBody>(&body, 4, rule) else {
            panic!("five items are more than four");
        };
        assert_eq!(message, "the rule");
        // The allowance is one request's, and is gone when it has been parsed.
        assert!(parse::<ReadBody>(&body).is_ok());
        // Arrays inside arrays count against the same allowance.
        let nested = Bytes::from(
            r#"{"transactions":[{"writes":[{"key":"a","value":1},{"key":"b","value":2}]},{"writes":[{"key":"c","delete":true}]}]}"#,
        );
        assert!(parse_within::<CommitBody>(&nested, 5, rule).is_ok());
        assert!(parse_within::<CommitBody>(&nested, 4, rule).is_err());
        // Anything else wrong with a request is still reported as what it is.
        let broken = Bytes::from(r#"{"keys":["a",}"#);
        let Err(Error::Invalid(message)) = parse_within::<ReadBody>(&broken, 5, rule) else {
            panic!("the body is not JSON");
        };
        assert!(message.starts_with("malformed request"), "{message}");
    }

    #[test]
    fn a_stored_value_does_not_keep_its_request() {
        let app_limits = ApiLimits::default();
        let body = Bytes::from(format!("{{\"value\":{}{}}}", "7", " ".repeat(1 << 20)));
        #[derive(Deserialize)]
        struct One<'a> {
            #[serde(borrow)]
            value: &'a RawValue,
        }
        let one: One = serde_json::from_slice(&body).unwrap();
        let kept = stored(&app_limits, one.value).unwrap();
        assert_eq!(&kept[..], b"7");
        // The megabyte around it is not what the stored bytes point into.
        let within = body.as_ptr_range();
        assert!(!within.contains(&kept.as_ptr()));
        assert!(stored(&app_limits, serde_json::from_str("null").unwrap()).is_err());
    }

    #[test]
    fn paths_decode() {
        assert_eq!(percent_decode("users%2F42%20x").unwrap(), "users/42 x");
        assert!(percent_decode("%zz").is_err());
        assert!(percent_decode("%ff").is_err());
    }
}
