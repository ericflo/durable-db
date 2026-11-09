//! Shared immutable-manifest publication protocol for B2 and S3.
use crate::{
    cache::Cache,
    directory::{Directory, MissingNode, Pointer, Root, witness},
    metrics::{self, Event, Operation, Outcome},
    observation::Observations,
    storage::{Object, Storage, Version},
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const HEAD: &str = "durable-db-root";
const CURSOR: &str = "durable-db-gc";
const INDEX_CURSOR: &str = "durable-db-index-gc";
const MAX_INFO: usize = 9000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bucket {
    pub revision: u64,
    #[serde(skip)]
    pub version: Option<Version>,
    pub info: BTreeMap<String, String>,
}
#[async_trait]
pub trait Authority: Send + Sync {
    async fn read(&self) -> anyhow::Result<Bucket>;
    /// Read barrier shared by every provider; raw observations may be stale.
    async fn read_fresh(&self) -> anyhow::Result<Bucket> {
        metrics::measure(Operation::ReadBarrier, async {
            for attempt in 0..8 {
                let old = self.read().await?;
                let mut info = old.info.clone();
                info.insert("durable-db-read-fence".into(), Uuid::new_v4().to_string());
                budget(&info)?;
                if let Some(current) = self.replace(&old, info).await? {
                    return Ok(current);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
            }
            tracing::warn!("publication read barrier contention budget exceeded");
            bail!("publication read barrier contention budget exceeded")
        })
        .await
    }
    /// None only for a definite comparison mismatch. Errors may have committed.
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>>;
    async fn maintenance(&self) -> anyhow::Result<()> {
        Ok(())
    }
    /// Delete physical versions, not a hide marker.
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()>;
}
/// One shared publication protocol with a persistent, paginated database directory.
pub struct Published {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
    directory: Directory,
    observations: Observations,
    cache: Option<Arc<Cache>>,
}
impl Published {
    pub async fn open(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
    ) -> anyhow::Result<Self> {
        let files: Arc<dyn Storage> = Arc::new(crate::measured::Files(files));
        let authority: Arc<dyn Authority> = Arc::new(crate::measured::Register(authority));
        let before = authority.read().await?;
        let mut probe = before.info.clone();
        probe.insert("durable-db-probe".into(), Uuid::new_v4().to_string());
        budget(&probe)?;
        if authority.replace(&before, probe.clone()).await?.is_some() {
            // B2 accepts an identical-metadata no-op even with a stale revision.
            // A different value tests the actual exclusion guarantee.
            probe.insert("durable-db-probe".into(), Uuid::new_v4().to_string());
            if authority.replace(&before, probe).await?.is_some() {
                bail!("provider ignored publication comparison precondition");
            }
        }
        for _ in 0..8 {
            let bucket = authority.read().await?;
            if let Some(format) = bucket.info.get("durable-db-format") {
                if format != "3"
                    || bucket.info.get("durable-db-prefix").map(String::as_str) != Some(prefix)
                {
                    bail!("publication directory is initialized with a different format or prefix");
                }
                return Ok(Self {
                    cache: None,
                    observations: Observations::new(authority.clone()),
                    directory: Directory::new(files.clone()),
                    files,
                    authority,
                });
            }
            if bucket
                .info
                .keys()
                .any(|k| k.starts_with("durable-db-") && k.as_str() != "durable-db-probe")
            {
                bail!("unrecognized durable-db bucket metadata");
            }
            let mut info = bucket.info.clone();
            info.insert("durable-db-format".into(), "3".into());
            info.insert("durable-db-prefix".into(), prefix.into());
            budget(&info)?;
            if authority.replace(&bucket, info).await?.is_some() {
                return Ok(Self {
                    cache: None,
                    observations: Observations::new(authority.clone()),
                    directory: Directory::new(files.clone()),
                    files,
                    authority,
                });
            }
        }
        bail!("publication directory initialization is contended; retry startup")
    }
    /// Shares the engine's bounded cache; no extra unaccounted index cache.
    pub fn with_cache(mut self, cache: Arc<Cache>) -> Self {
        self.directory = self.directory.with_cache(cache.clone());
        self.cache = Some(cache);
        self
    }
    async fn publish(
        &self,
        key: &str,
        old: Option<&Version>,
        bytes: Bytes,
    ) -> anyhow::Result<bool> {
        let slot = slot(key)?;
        let bucket = self.authority.read().await?;
        let base_root = root(&bucket.info)?;
        let current = match self.directory.get(base_root.as_ref(), &slot).await {
            Err(e) if e.is::<MissingNode>() => return Ok(false),
            other => other?,
        };
        let identity = current.as_ref().map(serde_json::to_string).transpose()?;
        let matches = match old {
            None => current.is_none(),
            Some(v) => identity.as_ref() == v.0.e_tag.as_ref(),
        };
        if !matches {
            return Ok(false);
        }
        let pointer = Pointer {
            key: format!("manifests/{slot}.{}", Uuid::new_v4()),
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        let mut info = bucket.info.clone();
        budget(&info)?; // Reserve the bounded root and all maintenance fields before upload.
        self.files.put(&pointer.key, bytes.clone()).await?;
        if let Some(cache) = &self.cache {
            cache.insert(&pointer.key, bytes).await;
        }
        let mut updated = match self
            .directory
            .insert(base_root.as_ref(), &slot, pointer.clone())
            .await
        {
            Err(e) if e.is::<MissingNode>() => return Ok(false),
            other => other?,
        };
        info.insert(HEAD.into(), serde_json::to_string(&updated)?);
        budget(&info)?;
        let mut observed = bucket.clone();
        for attempt in 0..8 {
            // An error has an unknown outcome and must go back to receipt recovery.
            if self.authority.replace(&observed, info).await?.is_some() {
                return Ok(true);
            }
            if attempt == 7 {
                return Ok(false);
            }
            let latest = self.authority.read().await?;
            // A collector may have deleted our unpublished manifest or path.
            // Never reuse an upload across a collection fence, even when our
            // database pointer did not change. Fence identities cannot be reused.
            if latest.info.get("durable-db-fence") != bucket.info.get("durable-db-fence") {
                return Ok(false);
            }
            if latest.info.get(HEAD) != observed.info.get(HEAD) {
                let latest_root = root(&latest.info)?;
                let target = match self.directory.get(latest_root.as_ref(), &slot).await {
                    Err(e) if e.is::<MissingNode>() => return Ok(false),
                    other => other?,
                };
                // Engine-level collection and competing writes to this database
                // also change its immutable pointer. They must cause a restart.
                if target != current {
                    return Ok(false);
                }
                metrics::REGISTRY.event(Event::PublicationRebase, 1);
                // Only unrelated entries changed. Rebuild from the NEW root;
                // retaining the old path would erase their acknowledged writes.
                // All path objects are fresh. A later fence/root race is caught
                // by the next CAS; an ambiguous response still returns an error.
                updated = match self
                    .directory
                    .insert(latest_root.as_ref(), &slot, pointer.clone())
                    .await
                {
                    Err(e) if e.is::<MissingNode>() => return Ok(false),
                    other => other?,
                };
            }
            info = latest.info.clone();
            info.insert(HEAD.into(), serde_json::to_string(&updated)?);
            budget(&info)?;
            observed = latest;
            tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
        }
        Ok(false)
    }
    /// List before fencing. Test membership using bounded paths, never a full scan.
    /// Returns manifests removed; obsolete directory pages are collected as well.
    pub async fn collect_manifests(&self) -> anyhow::Result<usize> {
        let before = self.authority.read().await?;
        let manifests = self
            .files
            .list("manifests/", cursor(&before, CURSOR), 128)
            .await?;
        let indexes = self
            .files
            .list("indexes/", cursor(&before, INDEX_CURSOR), 128)
            .await?;
        let current = self.authority.read().await?;
        let protected = root(&current.info)?;
        let mut info = current.info.clone();
        for (field, candidates) in [(CURSOR, &manifests), (INDEX_CURSOR, &indexes)] {
            info.insert(
                field.into(),
                if candidates.len() == 128 {
                    candidates.last().unwrap().clone()
                } else {
                    String::new()
                },
            );
        }
        info.insert("durable-db-fence".into(), Uuid::new_v4().to_string());
        budget(&info)?;
        if self.authority.replace(&current, info).await?.is_none() {
            return Ok(0);
        }
        let mut deleted = 0;
        for key in manifests {
            metrics::REGISTRY.event(Event::ManifestCandidates, 1);
            let db = witness(&key, "manifests/")?;
            let pointer = self.directory.get(protected.as_ref(), db).await?;
            if pointer.is_none_or(|p| p.key != key) {
                self.authority.delete_versions(&key).await?;
                metrics::REGISTRY.event(Event::ManifestDeleteAck, 1);
                deleted += 1;
            }
        }
        for key in indexes {
            metrics::REGISTRY.event(Event::IndexCandidates, 1);
            if !self.directory.reachable(protected.as_ref(), &key).await? {
                self.authority.delete_versions(&key).await?;
                metrics::REGISTRY.event(Event::IndexDeleteAck, 1);
            }
        }
        Ok(deleted)
    }
}
fn slot(key: &str) -> anyhow::Result<String> {
    let db = key
        .strip_prefix("heads/")
        .and_then(|v| v.strip_suffix(".json"))
        .context("publication CAS requires a database head")?;
    crate::engine::validate_db(db)?;
    Ok(db.to_owned())
}
fn root(info: &BTreeMap<String, String>) -> anyhow::Result<Option<Root>> {
    info.get(HEAD)
        .map(|value| {
            let root: Root = serde_json::from_str(value)?;
            root.validate()?;
            Ok(root)
        })
        .transpose()
}
fn cursor<'a>(bucket: &'a Bucket, field: &str) -> Option<&'a str> {
    bucket
        .info
        .get(field)
        .filter(|v| !v.is_empty())
        .map(String::as_str)
}
fn budget(info: &BTreeMap<String, String>) -> anyhow::Result<()> {
    // Admission must leave room for later reads and maintenance. Otherwise a valid
    // create could fill the directory and prevent its own read/collection fences.
    let mut reserved = info.clone();
    for (field, size) in [
        ("durable-db-probe", 36),
        ("durable-db-read-fence", 36),
        ("durable-db-fence", 36),
        (CURSOR, 111),
        (INDEX_CURSOR, 109),
        (HEAD, 1024),
    ] {
        if reserved.get(field).is_none_or(|v| v.len() < size) {
            reserved.insert(field.into(), "0".repeat(size));
        }
    }
    if info.keys().any(|k| k.len() > 50) || serde_json::to_vec(&reserved)?.len() > MAX_INFO {
        bail!("publication authority metadata budget exceeded");
    }
    Ok(())
}
#[async_trait]
impl Storage for Published {
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        if !key.starts_with("heads/") {
            return self.files.get(key, max_bytes).await;
        }
        let slot = slot(key)?;
        for _ in 0..4 {
            let bucket = self.observations.read().await?;
            let root = root(&bucket.info)?;
            let pointer = match self.directory.get(root.as_ref(), &slot).await {
                Err(e) if e.is::<MissingNode>() => continue,
                other => other?,
            };
            let Some(pointer) = pointer else {
                return Ok(None);
            };
            let value = serde_json::to_string(&pointer)?;
            // Only immutable bytes are cached. The pointer above always comes
            // from a new remote barrier admitted after this request began.
            let cached = match &self.cache {
                Some(cache) => cache.get(&pointer.key, &pointer.sha256).await,
                None => None,
            };
            let bytes = match cached {
                Some(bytes) => {
                    anyhow::ensure!(bytes.len() <= max_bytes, "manifest exceeds read bound");
                    bytes
                }
                None => {
                    // Collection may remove an old manifest after pointer
                    // selection. Restart the complete observation in that case.
                    let Some(object) = self.files.get(&pointer.key, max_bytes).await? else {
                        continue;
                    };
                    if hex::encode(Sha256::digest(&object.bytes)) != pointer.sha256 {
                        bail!("published manifest checksum mismatch");
                    }
                    if let Some(cache) = &self.cache {
                        cache.insert(&pointer.key, object.bytes.clone()).await;
                    }
                    object.bytes
                }
            };
            return Ok(Some(Object {
                bytes,
                version: Version(UpdateVersion {
                    e_tag: Some(value.clone()),
                    version: Some(bucket.revision.to_string()),
                }),
            }));
        }
        bail!("published manifest changed repeatedly or is unavailable")
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        let timer = metrics::REGISTRY.start(Operation::Publication);
        let result = self.publish(key, old, bytes).await;
        timer.finish(match &result {
            Ok(true) => Outcome::Ok,
            Ok(false) => Outcome::Conflict,
            Err(_) => Outcome::Error,
        });
        result
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.files.put(key, bytes).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        if prefix == "heads/" {
            if limit > 128 {
                bail!("directory list page exceeds 128 entries");
            }
            let after = after
                .map(|s| s.strip_prefix("heads/").context("invalid directory cursor"))
                .transpose()?;
            for _ in 0..4 {
                let bucket = self.observations.read().await?;
                let root = root(&bucket.info)?;
                match self.directory.list(root.as_ref(), after, limit).await {
                    Err(e) if e.is::<MissingNode>() => continue,
                    result => return result,
                }
            }
            bail!("directory snapshot changed repeatedly or is unavailable")
        } else {
            self.files.list(prefix, after, limit).await
        }
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        if key.starts_with("heads/") {
            bail!("head deletion requires a database lifecycle transaction");
        }
        self.authority.delete_versions(key).await
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        self.authority.maintenance().await?;
        self.collect_manifests().await?;
        Ok(())
    }
}
