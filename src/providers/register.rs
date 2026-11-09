//! Make a metadata replacement a real conditional mutation on both providers.
use crate::publication::{Authority, Bucket};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

pub(super) struct Register(pub Arc<dyn Authority>);

#[async_trait]
impl Authority for Register {
    async fn read(&self) -> anyhow::Result<Bucket> {
        self.0.read().await
    }

    async fn replace(
        &self,
        old: &Bucket,
        mut info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        // B2 accepts identical metadata as a no-op even at a stale revision.
        // A fresh identity per invocation forces an actual comparison. Reuse
        // the existing reserved field so this costs no new metadata slot.
        info.insert("durable-db-probe".into(), Uuid::new_v4().to_string());
        // One attempt: an uncertain response must remain uncertain.
        self.0.replace(old, info).await
    }

    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.0.delete_versions(key).await
    }

    async fn maintenance(&self) -> anyhow::Result<()> {
        self.0.maintenance().await
    }
}
