use crate::{
    Error, Result,
    auth::Auth,
    engine::{CommitRequest, Engine, Limits, MAX_BODY, PolicyRequest, PolicyResult, ReadRequest},
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
    Metrics,
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
        .route("/v1/databases/{db}/access", post(access))
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
        Some("/v1/databases/{db}/access") => Operation::HttpAccess,
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
    } else if app.auth.is_metrics(token) {
        if request.uri().path() != "/metrics" || request.method() != axum::http::Method::GET {
            return Error::Unauthorized.into_response();
        }
        Principal::Metrics
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
        // Two fixed control principals may always create their entries even
        // when tenants fill the bounded map. Otherwise churn can lock out
        // administrative recovery and monitoring. At most 4,098 entries exist.
        if matches!(&bucket, Principal::Database(_))
            && rates.len() >= 4096
            && !rates.contains_key(&bucket)
        {
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
    let token = bearer(&headers)?;
    if !app.auth.is_admin(token) && !app.auth.is_metrics(token) {
        return Err(Error::Unauthorized);
    }
    Ok(format!(
        "# HELP durable_db_requests_total Requests admitted to handlers.\n# TYPE durable_db_requests_total counter\ndurable_db_requests_total {}\n# HELP durable_db_rejected_total Legacy rate and concurrency rejection count.\n# TYPE durable_db_rejected_total counter\ndurable_db_rejected_total {}\n# HELP durable_db_cache_bytes Accounted memory cache bytes including estimated entry overhead.\n# TYPE durable_db_cache_bytes gauge\ndurable_db_cache_bytes {}\n",
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
    let access = app.auth.access(bearer(&headers)?, &db, false)?;
    Ok(Json(app.engine.info_with_access(&db, &access).await?))
}
async fn read(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReadRequest>,
) -> Result<Json<crate::engine::ReadResult>> {
    let access = app.auth.access(bearer(&headers)?, &db, false)?;
    Ok(Json(
        app.engine.read_with_access(&db, &request, &access).await?,
    ))
}
async fn commit(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> Result<Json<crate::engine::CommitResult>> {
    let access = app.auth.access(bearer(&headers)?, &db, true)?;
    Ok(Json(
        app.engine
            .commit_with_access(&db, &request, &access)
            .await?,
    ))
}
async fn access(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PolicyRequest>,
) -> Result<Json<PolicyResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.engine.change_policy(&db, &request).await?))
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
    #[serde(skip_serializing_if = "Option::is_none")]
    token_id: Option<String>,
}
async fn token(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<TokenRequest>,
) -> Result<Json<TokenResponse>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    let info = app.engine.info(&request.database).await?;
    let (token, token_id) = if let Some(policy) = info.policy {
        let (token, id) = app.auth.issue_for_policy(
            request.database,
            request.write,
            request.ttl_seconds,
            policy.incarnation,
            policy.generation,
        )?;
        (token, Some(id))
    } else {
        (
            app.auth
                .issue(request.database, request.write, request.ttl_seconds)?,
            None,
        )
    };
    Ok(Json(TokenResponse { token, token_id }))
}
