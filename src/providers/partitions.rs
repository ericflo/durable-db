//! Independent native owners backed by the existing shared publication directory.
//! No clocks, extra database engine, per-partition bucket, or object CAS.
use super::Connection;
use crate::{
    cache::Cache,
    publication::{Authority, Bucket, Published},
    storage::{Object, Storage, Version},
    upload_budget::{UploadLimits, UploadRequest, UploadResult, UploadStatus},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};

const MAX_RECORD: usize = 16 * 1024;

/// Immutable database mapping. The number of partitions does not limit the
/// number of databases, and never changes when processes join or leave.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    format: u8,
    partitions: u32,
}
impl Layout {
    pub fn new(partitions: u32) -> anyhow::Result<Self> {
        let layout = Self {
            format: 1,
            partitions,
        };
        layout.validate()?;
        Ok(layout)
    }
    pub(crate) fn validate(self) -> anyhow::Result<()> {
        ensure!(
            self.format == 1 && (1..=1024).contains(&self.partitions),
            "invalid cluster layout"
        );
        Ok(())
    }
    pub fn partitions(self) -> u32 {
        self.partitions
    }
    pub fn partition(self, database: &str) -> crate::Result<u32> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        crate::engine::validate_db(database)?;
        let mut hash = Sha256::new();
        hash.update(b"durable-db.partition.v1\0");
        hash.update(database.as_bytes());
        Ok(
            u32::from_be_bytes(hash.finalize()[..4].try_into().expect("digest prefix"))
                % self.partitions,
        )
    }
}

/// Shared control directory. Database-to-partition mapping and placement remain
/// the caller's responsibility; changing either requires a transfer protocol.
/// Clones share only bounded caches and transport, never ownership permission.
#[derive(Clone)]
pub struct Partitions {
    directory: Arc<Published>,
}
impl Partitions {
    pub(super) async fn open(
        connection: Connection,
        limits: UploadLimits,
        cache: Arc<Cache>,
    ) -> anyhow::Result<Self> {
        let directory = Published::open_partitioned(
            connection.files,
            connection.authority,
            &connection.prefix,
            limits,
        )
        .await?
        .with_cache(cache);
        Ok(Self {
            directory: Arc::new(directory),
        })
    }

    /// Open a partition without acquiring it. Existing ceilings/counters are
    /// preserved. IDs are stable u32 values, not a bound on database count.
    pub async fn partition(&self, id: u32, limits: UploadLimits) -> anyhow::Result<Connection> {
        let connection = self.lazy_partition(id)?;
        connection.initialize_upload_budget(limits).await?;
        Ok(connection)
    }

    pub(crate) fn lazy_partition(&self, id: u32) -> anyhow::Result<Connection> {
        let name = format!("{id:08x}");
        let files = Arc::new(Files {
            directory: self.directory.clone(),
            prefix: format!("partitions/{name}/"),
        });
        let mut connection = Connection::new(
            files.clone(),
            Arc::new(Register {
                files,
                key: format!("heads/{name}.json"),
            }),
            &name,
        );
        // The shared directory instruments the actual provider calls. Counting
        // this virtual register/object scope again would inflate cost metrics.
        connection.measured = true;
        connection.with_lazy_upload_budget()
    }

    /// Install a mapping only in an empty directory, or verify the existing
    /// mapping. A different count requires explicit migration to a new namespace.
    pub async fn configure(&self, layout: Layout) -> anyhow::Result<()> {
        self.directory.configure_layout(layout).await
    }

    pub async fn upload_budget(&self) -> anyhow::Result<UploadStatus> {
        self.directory
            .upload_budget()
            .await?
            .context("missing partition directory budget")
    }
    pub async fn replenish_upload_budget(
        &self,
        request: &UploadRequest,
    ) -> crate::Result<UploadResult> {
        self.directory.replenish_upload_budget(request).await
    }
}

struct Files {
    directory: Arc<Published>,
    prefix: String,
}
impl Files {
    fn key(&self, key: &str) -> anyhow::Result<String> {
        // Canonical paths only. A trailing slash is allowed for LIST prefixes.
        ensure!(
            !key.is_empty()
                && key.trim_end_matches('/').split('/').all(|part| {
                    !part.is_empty()
                        && part != "."
                        && part != ".."
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                })
                && !key.contains("//"),
            "invalid partition object path"
        );
        Ok(format!("{}{key}", self.prefix))
    }
}
#[async_trait]
impl Storage for Files {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.directory.get(&self.key(key)?, max).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        size: u64,
    ) -> anyhow::Result<Bytes> {
        self.directory.get_range(&self.key(key)?, range, size).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.directory.put(&self.key(key)?, bytes).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        anyhow::bail!("partition objects are immutable; use the control register")
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let after = after.map(|key| self.key(key)).transpose()?;
        self.directory
            .list(&self.key(prefix)?, after.as_deref(), limit)
            .await?
            .into_iter()
            .map(|key| {
                let relative = key
                    .strip_prefix(&self.prefix)
                    .context("listing escaped partition")?;
                ensure!(relative.starts_with(prefix), "listing escaped prefix");
                Ok(relative.to_owned())
            })
            .collect()
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        super::collection_key(key)?;
        self.directory.delete(&self.key(key)?).await
    }
}

struct Register {
    files: Arc<Files>,
    key: String,
}
#[async_trait]
impl Authority for Register {
    async fn read(&self) -> anyhow::Result<Bucket> {
        let Some(object) = self.files.directory.get(&self.key, MAX_RECORD).await? else {
            return Ok(Bucket {
                revision: 0,
                version: None,
                info: BTreeMap::new(),
            });
        };
        let mut bucket: Bucket = serde_json::from_slice(&object.bytes)?;
        ensure!(bucket.revision > 0, "invalid partition revision");
        bucket.version = Some(object.version);
        Ok(bucket)
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let next = Bucket {
            revision: old
                .revision
                .checked_add(1)
                .context("partition revision exhausted")?,
            version: None,
            info,
        };
        let bytes = serde_json::to_vec(&next)?;
        ensure!(
            bytes.len() <= MAX_RECORD,
            "partition control record exceeds bound"
        );
        if !self
            .files
            .directory
            .cas(&self.key, old.version.as_ref(), bytes.into())
            .await?
        {
            return Ok(None);
        }
        // The directory owns its exact pointer token. Never invent a token or
        // turn an intervening publication/read failure into a definite conflict.
        let current = self.read().await?;
        ensure!(
            current.revision == next.revision && current.info == next.info,
            "partition changed after publication; outcome uncertain"
        );
        Ok(Some(current))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.files.delete(key).await
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        self.files.directory.maintenance().await
    }
}
