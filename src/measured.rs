//! Instrument the shared provider boundary without recording object identities.
use crate::{
    metrics::{self, Operation},
    publication::{Authority, Bucket},
    storage::{Object, Storage, Version},
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct Files(pub Arc<dyn Storage>);
#[async_trait]
impl Storage for Files {
    async fn upload_budget(&self) -> anyhow::Result<Option<crate::upload_budget::UploadStatus>> {
        self.0.upload_budget().await
    }
    async fn replenish_upload_budget(
        &self,
        request: &crate::upload_budget::UploadRequest,
    ) -> crate::Result<crate::upload_budget::UploadResult> {
        self.0.replenish_upload_budget(request).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        metrics::measure(
            Operation::ObjectGet,
            self.0.get_range(key, range, object_size),
        )
        .await
    }

    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        metrics::measure(Operation::ObjectGet, self.0.get(key, max)).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        metrics::measure(Operation::ObjectPut, self.0.put(key, bytes)).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        metrics::measure(Operation::ObjectList, self.0.list(prefix, after, limit)).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        self.0.cas(key, old, bytes).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.0.delete(key).await
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        self.0.maintenance().await
    }
}

pub(crate) struct Register(pub Arc<dyn Authority>);
#[async_trait]
impl Authority for Register {
    async fn read(&self) -> anyhow::Result<Bucket> {
        metrics::measure(Operation::AuthorityRead, self.0.read()).await
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        metrics::compare(Operation::AuthorityReplace, self.0.replace(old, info)).await
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        metrics::measure(Operation::PhysicalDelete, self.0.delete_versions(key)).await
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        self.0.maintenance().await
    }
}
