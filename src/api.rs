use crate::{
    Error, Result,
    auth::Auth,
    engine::{CommitRequest, Engine, Limits, MAX_BODY, ReadRequest},
    metrics::{self as telemetry, Operation},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

struct Bucket {
    tokens: f64,
    time: Instant,
    permits: Arc<Semaphore>,
}
#[derive(Hash, Eq, PartialEq)]
enum Principal {
    Administrator,
    Database(String),
}
pub struct App {
    pub engine: Arc<Engine>,
    pub auth: Auth,
    pub ready: AtomicBool,
    pub permits: Arc<Semaphore>,
    rates: Mutex<HashMap<Principal, Bucket>>,
    principal_concurrency: usize,
    pub requests: AtomicU64,
    pub rejected: AtomicU64,
}
impl App {
    pub fn new(engine: Arc<Engine>, auth: Auth, concurrency: usize) -> Self {
        Self {
            engine,
            auth,
            ready: AtomicBool::new(true),
            permits: Arc::new(Semaphore::new(concurrency)),
            rates: Mutex::new(HashMap::new()),
            principal_concurrency: (concurrency / 4).max(1),
            requests: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }
}
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/databases/{db}", get(info).post(create))
        .route("/v1/databases/{db}/read", post(read))
        .route("/v1/databases/{db}/commit", post(commit))
        .route("/v1/databases/{db}/gc", post(gc))
        .route("/v1/tokens", post(token))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(middleware::from_fn_with_state(app.clone(), admission))
        .with_state(app)
}
fn bearer(headers: &HeaderMap) -> Result<&str> {
    headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|s| s.len() <= 2048)
        .ok_or(Error::Unauthorized)
}
async fn admission(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    if request.uri().path().starts_with("/health/") {
        return next.run(request).await;
    }
    // MatchedPath contains the route template, never the tenant's path value.
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str());
    let operation = match route {
        Some("/v1/databases/{db}/read") => Operation::HttpRead,
        Some("/v1/databases/{db}/commit") => Operation::HttpCommit,
        Some("/v1/databases/{db}/gc") => Operation::HttpGc,
        Some("/v1/databases/{db}") if request.method() == axum::http::Method::POST => {
            Operation::HttpCreate
        }
        Some("/v1/databases/{db}") => Operation::HttpInfo,
        Some("/v1/tokens") => Operation::HttpToken,
        _ => Operation::HttpOther,
    };
    let timer = telemetry::REGISTRY.start(operation);
    let response = admit(app, request, next).await;
    timer.finish(telemetry::http_outcome(response.status().as_u16()));
    response
}
async fn admit(app: Arc<App>, request: Request, next: Next) -> Response {
    if request.uri().path().starts_with("/health/") {
        return next.run(request).await;
    }
    if !app.ready.load(Ordering::Relaxed) {
        return Error::Busy.into_response();
    }
    let token = match bearer(request.headers()) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let bucket = if app.auth.is_admin(token) {
        Principal::Administrator
    } else {
        match app.auth.claims(token) {
            Ok(c) => Principal::Database(c.database),
            Err(e) => return e.into_response(),
        }
    };
    let weight = if request.uri().path().ends_with("/commit") {
        5.0
    } else {
        1.0
    };
    let now = Instant::now();
    // The owned permit survives every await and is returned on cancellation,
    // timeout, extractor failure, or normal completion. All tokens for a
    // database share one allowance; request paths cannot select another one.
    let _principal_permit = {
        let mut rates = app.rates.lock().await;
        if rates.len() >= 4096 && !rates.contains_key(&bucket) {
            rates.retain(|_, b| {
                now.duration_since(b.time) < Duration::from_secs(60)
                    || Arc::strong_count(&b.permits) > 1
            });
            if rates.len() >= 4096 {
                return Error::Busy.into_response();
            }
        }
        let rate = rates.entry(bucket).or_insert(Bucket {
            tokens: 40.0,
            time: now,
            permits: Arc::new(Semaphore::new(app.principal_concurrency)),
        });
        let Ok(permit) = rate.permits.clone().try_acquire_owned() else {
            app.rejected.fetch_add(1, Ordering::Relaxed);
            return Error::Busy.into_response();
        };
        rate.tokens = (rate.tokens + now.duration_since(rate.time).as_secs_f64() * 20.0).min(40.0);
        rate.time = now;
        if rate.tokens < weight {
            app.rejected.fetch_add(1, Ordering::Relaxed);
            return Error::Busy.into_response();
        }
        rate.tokens -= weight;
        permit
    };
    let Ok(_permit) = app.permits.clone().try_acquire_owned() else {
        app.rejected.fetch_add(1, Ordering::Relaxed);
        return Error::Busy.into_response();
    };
    app.requests.fetch_add(1, Ordering::Relaxed);
    // Timeouts can have ambiguous write outcomes. Identical retries use receipts/revision guards.
    match tokio::time::timeout(Duration::from_secs(30), next.run(request)).await {
        Ok(response) => response,
        Err(_) => Error::Storage(anyhow::anyhow!("request deadline exceeded")).into_response(),
    }
}
async fn ready(State(app): State<Arc<App>>) -> StatusCode {
    if app.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Result<String> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(format!(
        "# TYPE durable_db_requests_total counter\ndurable_db_requests_total {}\n# TYPE durable_db_rejected_total counter\ndurable_db_rejected_total {}\n# TYPE durable_db_cache_bytes gauge\ndurable_db_cache_bytes {}\n",
        app.requests.load(Ordering::Relaxed),
        app.rejected.load(Ordering::Relaxed),
        app.engine.cache().memory_bytes().await
    ) + &telemetry::REGISTRY.render())
}
async fn create(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(limits): Json<Limits>,
) -> Result<Json<crate::engine::Info>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.engine.create(&db, limits).await?))
}
async fn info(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::engine::Info>> {
    app.auth.authorize(bearer(&headers)?, &db, false)?;
    Ok(Json(app.engine.info(&db).await?))
}
async fn read(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReadRequest>,
) -> Result<Json<crate::engine::ReadResult>> {
    app.auth.authorize(bearer(&headers)?, &db, false)?;
    Ok(Json(app.engine.read(&db, &request).await?))
}
async fn commit(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> Result<Json<crate::engine::CommitResult>> {
    app.auth.authorize(bearer(&headers)?, &db, true)?;
    Ok(Json(app.engine.commit(&db, &request).await?))
}
async fn gc(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::engine::GcResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.engine.gc(&db).await?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenRequest {
    database: String,
    write: bool,
    ttl_seconds: u64,
}
#[derive(Serialize)]
struct TokenResponse {
    token: String,
}
async fn token(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<TokenRequest>,
) -> Result<Json<TokenResponse>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    app.engine.info(&request.database).await?;
    Ok(Json(TokenResponse {
        token: app
            .auth
            .issue(request.database, request.write, request.ttl_seconds)?,
    }))
}
