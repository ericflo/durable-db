//! One HTTP contract for the serving engine; storage algorithms stay below it.
use crate::{
    Error, Result,
    auth::Access,
    engine::{
        CommitRequest, CommitResult, CreateRequest, CreateResult, Engine, GcResult, Info, Limits,
        PolicyRequest, PolicyResult, ReadRequest, ReadResult,
    },
    upload_budget::{UploadRequest, UploadResult, UploadStatus},
};
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait Backend: Send + Sync {
    /// Select a concrete partition backend. HTTP pins this result for the whole
    /// request, including its expected-incarnation check. Leaf backends return None.
    async fn resolve(&self, _db: &str) -> Result<Option<Arc<dyn Backend>>> {
        Ok(None)
    }

    /// An identity check prevents a stale route reaching a reused address. It
    /// grants no serving permission; the backend must still validate responses.
    fn owner_incarnation(&self) -> Option<uuid::Uuid> {
        None
    }

    /// Called after admission closes; the caller bounds this best-effort wait.
    /// Shutdown hints grant no authority and interruption must preserve safety.
    async fn drain(&self) {}

    async fn ready(&self) -> bool {
        true
    }
    async fn memory_bytes(&self) -> usize;
    async fn info(&self, db: &str, access: &Access) -> Result<Info> {
        selected(self, db).await?.info(db, access).await
    }
    async fn read(&self, db: &str, request: &ReadRequest, access: &Access) -> Result<ReadResult> {
        selected(self, db).await?.read(db, request, access).await
    }
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult> {
        selected(self, &db).await?.commit(db, request, access).await
    }
    async fn create_protected(&self, db: String, request: CreateRequest) -> Result<CreateResult> {
        selected(self, &db)
            .await?
            .create_protected(db, request)
            .await
    }
    async fn change_policy(&self, db: String, request: PolicyRequest) -> Result<PolicyResult> {
        selected(self, &db).await?.change_policy(db, request).await
    }
    async fn create(&self, db: &str, limits: Limits) -> Result<Info> {
        if let Some(backend) = self.resolve(db).await? {
            return backend.create(db, limits).await;
        }
        Err(Error::Unsupported(
            "use idempotent PUT to create a protected database",
        ))
    }
    async fn gc(&self, db: &str) -> Result<GcResult> {
        if let Some(backend) = self.resolve(db).await? {
            return backend.gc(db).await;
        }
        Err(Error::Unsupported(
            "physical collection is not available on this backend",
        ))
    }
    async fn database_upload_budget(&self, db: &str) -> Result<UploadStatus> {
        if let Some(backend) = self.resolve(db).await? {
            return backend.database_upload_budget(db).await;
        }
        Err(Error::Unsupported(
            "database upload budgets are not available on this backend",
        ))
    }
    async fn replenish_database_upload_budget(
        &self,
        db: &str,
        request: &UploadRequest,
    ) -> Result<UploadResult> {
        if let Some(backend) = self.resolve(db).await? {
            return backend.replenish_database_upload_budget(db, request).await;
        }
        Err(Error::Unsupported(
            "database upload budgets are not available on this backend",
        ))
    }
    async fn upload_budget(&self) -> Result<Option<UploadStatus>> {
        Err(Error::Unsupported(
            "physical upload budgets are not available on this backend",
        ))
    }
    async fn replenish_upload_budget(&self, _request: &UploadRequest) -> Result<UploadResult> {
        Err(Error::Unsupported(
            "physical upload budgets are not available on this backend",
        ))
    }
}

async fn selected<B: Backend + ?Sized>(backend: &B, db: &str) -> Result<Arc<dyn Backend>> {
    backend
        .resolve(db)
        .await?
        .ok_or(Error::Unsupported("database operation is unavailable"))
}

#[async_trait]
impl Backend for Engine {
    async fn memory_bytes(&self) -> usize {
        self.cache().memory_bytes().await
    }
    async fn info(&self, db: &str, access: &Access) -> Result<Info> {
        self.info_with_access(db, access).await
    }
    async fn read(&self, db: &str, request: &ReadRequest, access: &Access) -> Result<ReadResult> {
        self.read_with_access(db, request, access).await
    }
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult> {
        self.commit_with_access(&db, &request, &access).await
    }
    async fn create_protected(&self, db: String, request: CreateRequest) -> Result<CreateResult> {
        Engine::create_protected(self, &db, &request).await
    }
    async fn change_policy(&self, db: String, request: PolicyRequest) -> Result<PolicyResult> {
        Engine::change_policy(self, &db, &request).await
    }
    async fn create(&self, db: &str, limits: Limits) -> Result<Info> {
        Engine::create(self, db, limits).await
    }
    async fn gc(&self, db: &str) -> Result<GcResult> {
        Engine::gc(self, db).await
    }
    async fn upload_budget(&self) -> Result<Option<UploadStatus>> {
        Engine::upload_budget(self).await
    }
    async fn replenish_upload_budget(&self, request: &UploadRequest) -> Result<UploadResult> {
        Engine::replenish_upload_budget(self, request).await
    }
}
