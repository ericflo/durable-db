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

#[async_trait]
pub trait Backend: Send + Sync {
    async fn ready(&self) -> bool {
        true
    }
    async fn memory_bytes(&self) -> usize;
    async fn info(&self, db: &str, access: &Access) -> Result<Info>;
    async fn read(&self, db: &str, request: &ReadRequest, access: &Access) -> Result<ReadResult>;
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult>;
    async fn create_protected(&self, db: String, request: CreateRequest) -> Result<CreateResult>;
    async fn change_policy(&self, db: String, request: PolicyRequest) -> Result<PolicyResult>;
    async fn create(&self, _db: &str, _limits: Limits) -> Result<Info> {
        Err(Error::Unsupported(
            "use idempotent PUT to create a protected database",
        ))
    }
    async fn gc(&self, _db: &str) -> Result<GcResult> {
        Err(Error::Unsupported(
            "physical collection is not available on this backend",
        ))
    }
    async fn database_upload_budget(&self, _db: &str) -> Result<UploadStatus> {
        Err(Error::Unsupported(
            "database upload budgets are not available on this backend",
        ))
    }
    async fn replenish_database_upload_budget(
        &self,
        _db: &str,
        _request: &UploadRequest,
    ) -> Result<UploadResult> {
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
