//! Single-directory native serving. Exclusion comes from Local's lifetime OS
//! lock, never elapsed time. This adapter cannot construct a cloud connection.
use super::{Backend, Native};
use crate::{
    cache::Cache,
    ownership::root::Outcome,
    providers::Connection,
    publication::{Authority, Bucket},
    storage::{Local, Storage, Version},
    transactions::live::{Limits, Opening},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use object_store::UpdateVersion;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

struct Register(Arc<Local>);
#[async_trait]
impl Authority for Register {
    async fn read(&self) -> anyhow::Result<Bucket> {
        let Some(object) = self.0.get("authority.json", 16384).await? else {
            return Ok(Bucket {
                revision: 0,
                version: None,
                info: BTreeMap::new(),
            });
        };
        let mut bucket: Bucket = serde_json::from_slice(&object.bytes)?;
        bucket.version = Some(object.version);
        Ok(bucket)
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let mut next = Bucket {
            revision: old
                .revision
                .checked_add(1)
                .context("local authority revision exhausted")?,
            info,
            version: None,
        };
        let bytes = serde_json::to_vec(&next)?;
        ensure!(bytes.len() <= 16384, "local authority exceeds bound");
        next.version = Some(Version(UpdateVersion {
            e_tag: Some(hex::encode(Sha256::digest(&bytes))),
            version: None,
        }));
        Ok(self
            .0
            .cas("authority.json", old.version.as_ref(), bytes.into())
            .await?
            .then_some(next))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        crate::providers::collection_key(key)?;
        self.0.delete(key).await
    }
}

/// Open a fresh or native local directory exclusively. Earlier local formats
/// require export with their original binary and restore into a new directory.
/// The directory must live on one host with working exclusive locks and fsync;
/// shared/network filesystems and copying live process memory are unsupported.
pub async fn open(
    data: PathBuf,
    cache: Arc<Cache>,
    capacity: usize,
    maintenance: Duration,
) -> anyhow::Result<impl Backend> {
    let directory = Arc::new(Local::open(data)?);
    directory.cleanup_temps()?;
    ensure!(
        directory.list("heads/", None, 1).await?.is_empty(),
        "legacy local directory: export with the previous binary and restore into a fresh directory"
    );
    if directory.get("authority.json", 16384).await?.is_none() {
        for prefix in ["journal/", "tables/", "catalogs/", "indexes/", "manifests/"] {
            ensure!(
                directory.list(prefix, None, 1).await?.is_empty(),
                "local authority is missing from a nonempty native directory"
            );
        }
    }
    let connection = Connection::new(
        directory.clone(),
        Arc::new(Register(directory.clone())),
        "local-exclusive",
    );
    let mut root = connection.exclusive_root(directory)?;
    let observed = root.observe().await?;
    let claim = root.claim(observed)?;
    ensure!(
        matches!(root.finish(claim.execute().await)?, Outcome::Confirmed),
        "local claim not confirmed"
    );
    let activate = root.activate()?;
    ensure!(
        matches!(root.finish(activate.execute().await)?, Outcome::Confirmed),
        "local activation not confirmed"
    );
    root.select_journals().await?;
    let owner = Opening::new(root, Limits::default(), cache)?
        .recover()
        .await?;
    Ok(Native::start(owner, capacity, maintenance)?)
}
