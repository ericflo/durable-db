mod backend;
mod local;
pub use local::open as open_local;
mod native;
pub use backend::Backend;
pub use native::Native;
mod rate;
pub use rate::RateLimits;

use crate::{
    Error, Result,
    auth::{Access, Auth},
    engine::{CommitRequest, Limits, MAX_BODY, PolicyRequest, PolicyResult, ReadRequest},
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
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Instant;

struct Bucket {
    budget: rate::Budget,
    permits: Arc<Semaphore>,
}
#[derive(Hash, Eq, PartialEq)]
enum Principal {
    Administrator,
    Metrics,
    Database(String),
}
pub struct App {
    pub backend: Arc<dyn Backend>,
    pub auth: Auth,
    pub ready: AtomicBool,
    pub permits: Arc<Semaphore>,
    rates: Mutex<HashMap<Principal, Bucket>>,
    rate_limits: RateLimits,
    principal_concurrency: usize,
    pub requests: AtomicU64,
    pub rejected: AtomicU64,
}
impl App {
    pub fn new(backend: Arc<dyn Backend>, auth: Auth, concurrency: usize) -> Self {
        Self::with_rate_limits(backend, auth, concurrency, RateLimits::default())
    }

    /// Construct a server with an explicit per-principal request budget.
    /// Concurrency limits and database quotas remain independent of this budget.
    pub fn with_rate_limits(
        backend: Arc<dyn Backend>,
        auth: Auth,
        concurrency: usize,
        rate_limits: RateLimits,
    ) -> Self {
        Self {
            backend,
            auth,
            ready: AtomicBool::new(true),
            permits: Arc::new(Semaphore::new(concurrency)),
            rates: Mutex::new(HashMap::new()),
            rate_limits,
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
        .route(
            "/v1/databases/{db}",
            get(info).post(create).put(create_protected),
        )
        .route("/v1/databases/{db}/read", post(read))
        .route("/v1/databases/{db}/commit", post(commit))
        .route("/v1/databases/{db}/gc", post(gc))
        .route("/v1/databases/{db}/access", post(access))
        .route("/v1/tokens", post(token))
        .route(
            "/v1/databases/{db}/upload-budget",
            get(database_upload_budget).post(replenish_database_upload_budget),
        )
        .route(
            "/v1/storage/upload-budget",
            get(upload_budget).post(replenish_upload_budget),
        )
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
        Some("/v1/databases/{db}")
            if matches!(
                *request.method(),
                axum::http::Method::POST | axum::http::Method::PUT
            ) =>
        {
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
        rate::COMMIT_COST
    } else {
        1
    };
    // The owned permit survives every await and is returned on cancellation,
    // timeout, extractor failure, or normal completion. All tokens for a
    // database share one allowance; request paths cannot select another one.
    let _principal_permit = {
        let mut rates = app.rates.lock().await;
        // Sampling before the lock can reuse elapsed time or credit an old
        // interval after a later request has already consumed its budget.
        let now = Instant::now();
        // Two fixed control principals may always create their entries even
        // when tenants fill the bounded map. Otherwise churn can lock out
        // administrative recovery and monitoring. At most 4,098 entries exist.
        if matches!(&bucket, Principal::Database(_))
            && rates.len() >= 4096
            && !rates.contains_key(&bucket)
        {
            rates.retain(|_, b| !b.budget.can_evict(now) || Arc::strong_count(&b.permits) > 1);
            if rates.len() >= 4096 {
                return Error::Busy.into_response();
            }
        }
        let rate = rates.entry(bucket).or_insert_with(|| Bucket {
            budget: rate::Budget::new(now, app.rate_limits),
            permits: Arc::new(Semaphore::new(app.principal_concurrency)),
        });
        let Ok(permit) = rate.permits.clone().try_acquire_owned() else {
            app.rejected.fetch_add(1, Ordering::Relaxed);
            return Error::Busy.into_response();
        };
        if !rate.budget.take(now, weight) {
            app.rejected.fetch_add(1, Ordering::Relaxed);
            return Error::Busy.into_response();
        }
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
    if app.ready.load(Ordering::Relaxed) && app.backend.ready().await {
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
        app.backend.memory_bytes().await
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
    Ok(Json(app.backend.create(&db, limits).await?))
}
async fn database_upload_budget(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::upload_budget::UploadStatus>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    crate::engine::validate_db(&db)?;
    Ok(Json(app.backend.database_upload_budget(&db).await?))
}
async fn replenish_database_upload_budget(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<crate::upload_budget::UploadRequest>,
) -> Result<Json<crate::upload_budget::UploadResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    crate::engine::validate_db(&db)?;
    Ok(Json(
        app.backend
            .replenish_database_upload_budget(&db, &request)
            .await?,
    ))
}
async fn upload_budget(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Json<Option<crate::upload_budget::UploadStatus>>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.backend.upload_budget().await?))
}
async fn replenish_upload_budget(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<crate::upload_budget::UploadRequest>,
) -> Result<Json<crate::upload_budget::UploadResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.backend.replenish_upload_budget(&request).await?))
}
async fn create_protected(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<crate::engine::CreateRequest>,
) -> Result<Json<crate::engine::CreateResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.backend.create_protected(db, request).await?))
}
async fn info(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::engine::Info>> {
    let access = app.auth.access(bearer(&headers)?, &db, false)?;
    Ok(Json(app.backend.info(&db, &access).await?))
}
async fn read(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReadRequest>,
) -> Result<Json<crate::engine::ReadResult>> {
    let access = app.auth.access(bearer(&headers)?, &db, false)?;
    Ok(Json(app.backend.read(&db, &request, &access).await?))
}
async fn commit(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> Result<Json<crate::engine::CommitResult>> {
    let access = app.auth.access(bearer(&headers)?, &db, true)?;
    Ok(Json(app.backend.commit(db, request, access).await?))
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
    Ok(Json(app.backend.change_policy(db, request).await?))
}
async fn gc(
    State(app): State<Arc<App>>,
    Path(db): Path<String>,
    headers: HeaderMap,
) -> Result<Json<crate::engine::GcResult>> {
    if !app.auth.is_admin(bearer(&headers)?) {
        return Err(Error::Unauthorized);
    }
    Ok(Json(app.backend.gc(&db).await?))
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
    let info = app
        .backend
        .info(&request.database, &Access::Administrator)
        .await?;
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
