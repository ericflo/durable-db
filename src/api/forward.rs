//! One bounded forwarding hop to an advertised native owner. Discovery is a
//! hint; the receiving backend checks its incarnation and serving permission.
use super::Backend;
use crate::{
    Error, Result,
    auth::{Access, Auth},
    engine::{
        self, CommitRequest, CommitResult, CreateRequest, CreateResult, GcResult, Info, Limits,
        PolicyRequest, PolicyResult, ReadRequest, ReadResult,
    },
    ownership::root::{Root, Route},
    providers::Connection,
    upload_budget::{UploadRequest, UploadResult, UploadStatus},
};
use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};

pub(super) const OWNER_HEADER: &str = "x-durable-db-owner";
const MAX_RESPONSE: usize = 1024 * 1024;

/// HTTP backend for one explicitly selected partition. It discovers and caches
/// the owner; it neither claims ownership nor selects a database's partition.
/// A failed attempt is returned to the caller, never automatically replayed.
pub struct Forward {
    observation: Root,
    auth: Auth,
    peer: Mutex<Option<Arc<Peer>>>,
    permits: Semaphore,
}
struct Peer {
    route: Route,
    client: Client,
}
impl Forward {
    pub fn new(connection: &Connection, auth: Auth, capacity: usize) -> Result<Self> {
        if !(1..=128).contains(&capacity) {
            return Err(Error::Invalid("forwarding capacity must be 1..128".into()));
        }
        Ok(Self {
            observation: connection.root().map_err(anyhow::Error::from)?,
            auth,
            peer: Mutex::new(None),
            permits: Semaphore::new(capacity),
        })
    }
    async fn peer(&self) -> Result<Arc<Peer>> {
        let mut cached = self.peer.lock().await;
        if let Some(peer) = &*cached {
            return Ok(peer.clone());
        }
        let route = self
            .observation
            .observe()
            .await
            .map_err(anyhow::Error::from)?
            .route()
            .ok_or_else(|| Error::Storage(anyhow::anyhow!("owner has no open route")))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(25))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .pool_max_idle_per_host(2)
            .build()
            .map_err(anyhow::Error::from)?;
        // A client belongs to this route instance. Replacing the route drops
        // its pool when the last bounded in-flight request completes.
        let peer = Arc::new(Peer { route, client });
        *cached = Some(peer.clone());
        Ok(peer)
    }
    async fn call<T: DeserializeOwned, B: Serialize + Sync>(
        &self,
        method: Method,
        path: &str,
        body: &B,
        access: &Access,
    ) -> Result<T> {
        let _permit = self.permits.try_acquire().map_err(|_| Error::Busy)?;
        let bytes = serde_json::to_vec(body).map_err(anyhow::Error::from)?;
        if bytes.len() > engine::MAX_BODY {
            return Err(Error::Limit("forwarded request exceeds 1 MiB".into()));
        }
        let token = self.auth.relay(access)?;
        let peer = self.peer().await?;
        let result = async {
            let mut response = peer
                .client
                .request(method, format!("http://{}{path}", peer.route.address))
                .header(OWNER_HEADER, peer.route.identity.incarnation.to_string())
                .bearer_auth(token)
                .header("content-type", "application/json")
                .body(bytes)
                .send()
                .await
                .map_err(anyhow::Error::from)?;
            let status = response.status();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(anyhow::Error::from)? {
                if chunk.len() > MAX_RESPONSE - bytes.len() {
                    return Err(Error::Storage(anyhow::anyhow!(
                        "owner response exceeds bound"
                    )));
                }
                bytes.extend_from_slice(&chunk);
            }
            if status == StatusCode::OK {
                return Ok(serde_json::from_slice(&bytes).map_err(anyhow::Error::from)?);
            }
            Err(remote_error(status, &bytes))
        }
        .await;
        if matches!(result, Err(Error::Storage(_))) {
            let mut cached = self.peer.lock().await;
            if cached
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &peer))
            {
                *cached = None;
            }
        }
        result
    }
}

fn path(db: &str, suffix: &str) -> Result<String> {
    engine::validate_db(db)?;
    Ok(format!("/v1/databases/{db}{suffix}"))
}
fn remote_error(status: StatusCode, bytes: &[u8]) -> Error {
    #[derive(serde::Deserialize)]
    struct Reply {
        error: String,
        message: String,
    }
    if let Ok(reply) = serde_json::from_slice::<Reply>(bytes) {
        return match (status.as_u16(), reply.error.as_str()) {
            (400, "invalid_request") => Error::Invalid(reply.message),
            (401, "unauthorized") => Error::Unauthorized,
            (404, "not_found") => Error::NotFound,
            (409, "conflict") => Error::Conflict,
            (409, "receipt_expired") => Error::ReceiptExpired,
            (413, "limit_exceeded") => Error::Limit(reply.message),
            (429, "busy") => Error::Busy,
            (501, "unsupported_operation") => {
                Error::Unsupported("operation is unsupported by the owner")
            }
            _ => Error::Storage(anyhow::anyhow!(
                "owner request failed; outcome may be unknown"
            )),
        };
    }
    Error::Storage(anyhow::anyhow!(
        "invalid owner response; outcome may be unknown"
    ))
}

#[async_trait]
impl Backend for Forward {
    async fn memory_bytes(&self) -> usize {
        0
    } // No value cache at the forwarding layer.
    async fn info(&self, db: &str, access: &Access) -> Result<Info> {
        access.check_scope(db, false)?;
        self.call(Method::GET, &path(db, "")?, &(), access).await
    }
    async fn read(&self, db: &str, request: &ReadRequest, access: &Access) -> Result<ReadResult> {
        access.check_scope(db, false)?;
        request.validate()?;
        self.call(Method::POST, &path(db, "/read")?, request, access)
            .await
    }
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult> {
        access.check_scope(&db, true)?;
        request.decode()?;
        self.call(Method::POST, &path(&db, "/commit")?, &request, &access)
            .await
    }
    async fn create_protected(&self, db: String, request: CreateRequest) -> Result<CreateResult> {
        request.validate()?;
        self.call(
            Method::PUT,
            &path(&db, "")?,
            &request,
            &Access::Administrator,
        )
        .await
    }
    async fn change_policy(&self, db: String, request: PolicyRequest) -> Result<PolicyResult> {
        request.digest()?;
        self.call(
            Method::POST,
            &path(&db, "/access")?,
            &request,
            &Access::Administrator,
        )
        .await
    }
    async fn create(&self, db: &str, limits: Limits) -> Result<Info> {
        self.call(
            Method::POST,
            &path(db, "")?,
            &limits,
            &Access::Administrator,
        )
        .await
    }
    async fn gc(&self, db: &str) -> Result<GcResult> {
        self.call(Method::POST, &path(db, "/gc")?, &(), &Access::Administrator)
            .await
    }
    async fn database_upload_budget(&self, db: &str) -> Result<UploadStatus> {
        self.call(
            Method::GET,
            &path(db, "/upload-budget")?,
            &(),
            &Access::Administrator,
        )
        .await
    }
    async fn replenish_database_upload_budget(
        &self,
        db: &str,
        request: &UploadRequest,
    ) -> Result<UploadResult> {
        self.call(
            Method::POST,
            &path(db, "/upload-budget")?,
            request,
            &Access::Administrator,
        )
        .await
    }
    async fn upload_budget(&self) -> Result<Option<UploadStatus>> {
        self.call(
            Method::GET,
            "/v1/storage/upload-budget",
            &(),
            &Access::Administrator,
        )
        .await
    }
    async fn replenish_upload_budget(&self, request: &UploadRequest) -> Result<UploadResult> {
        self.call(
            Method::POST,
            "/v1/storage/upload-budget",
            request,
            &Access::Administrator,
        )
        .await
    }
}
