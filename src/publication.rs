//! Shared immutable-manifest publication protocol for B2 and S3.
use crate::storage::{Object, Storage, Version};
use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const HEAD: &str = "durable-db-heads";
const CURSOR: &str = "durable-db-gc";
const MAX_HEADS: usize = 32;
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
        bail!("publication read barrier contention budget exceeded")
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
#[derive(Serialize, Deserialize)]
struct Pointer {
    key: String,
    sha256: String,
}
/// One dedicated bucket per cluster, at most 32 databases in the initial format.
/// The bounded slots avoid an extra mutable index and preserve unrelated metadata.
pub struct Published {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
}
impl Published {
    pub async fn open(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
    ) -> anyhow::Result<Self> {
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
                if format != "2"
                    || bucket.info.get("durable-db-prefix").map(String::as_str) != Some(prefix)
                {
                    bail!("publication directory is initialized with a different format or prefix");
                }
                return Ok(Self { files, authority });
            }
            if bucket
                .info
                .keys()
                .any(|k| k.starts_with("durable-db-") && k.as_str() != "durable-db-probe")
            {
                bail!("unrecognized durable-db bucket metadata");
            }
            let mut info = bucket.info.clone();
            info.insert("durable-db-format".into(), "2".into());
            info.insert("durable-db-prefix".into(), prefix.into());
            budget(&info)?;
            if authority.replace(&bucket, info).await?.is_some() {
                return Ok(Self { files, authority });
            }
        }
        bail!("publication directory initialization is contended; retry startup")
    }
    /// List first, then fence the complete bucket pointer set before deleting any manifest.
    pub async fn collect_manifests(&self) -> anyhow::Result<usize> {
        let before = self.authority.read().await?;
        let after = before
            .info
            .get(CURSOR)
            .filter(|v| !v.is_empty())
            .map(String::as_str);
        let candidates = self.files.list("manifests/", after, 128).await?;
        let current = self.authority.read().await?;
        let protected: Vec<String> = heads(&current.info)?
            .values()
            .map(|v| parse_pointer(v).map(|p| p.key))
            .collect::<anyhow::Result<_>>()?;
        let mut info = current.info.clone();
        info.insert(
            CURSOR.into(),
            if candidates.len() == 128 {
                candidates.last().unwrap().clone()
            } else {
                String::new()
            },
        );
        info.insert("durable-db-fence".into(), Uuid::new_v4().to_string());
        budget(&info)?;
        if self.authority.replace(&current, info).await?.is_none() {
            return Ok(0);
        }
        let mut deleted = 0;
        for key in candidates {
            if !protected.contains(&key) {
                self.authority.delete_versions(&key).await?;
                deleted += 1;
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
fn heads(info: &BTreeMap<String, String>) -> anyhow::Result<BTreeMap<String, String>> {
    match info.get(HEAD) {
        Some(value) => Ok(serde_json::from_str(value)?),
        None => Ok(BTreeMap::new()),
    }
}
fn budget(info: &BTreeMap<String, String>) -> anyhow::Result<()> {
    if heads(info)?.len() > MAX_HEADS
        || info.keys().any(|k| k.len() > 50)
        || serde_json::to_vec(info)?.len() > MAX_INFO
    {
        bail!(
            "publication directory metadata budget exceeded (initial format: 32 databases per bucket)"
        );
    }
    Ok(())
}
fn parse_pointer(value: &str) -> anyhow::Result<Pointer> {
    let p: Pointer = serde_json::from_str(value)?;
    let id = p
        .key
        .strip_prefix("manifests/")
        .context("invalid published manifest namespace")?;
    Uuid::parse_str(id).context("invalid published manifest identity")?;
    if p.sha256.len() != 64 || !p.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid manifest digest");
    }
    Ok(p)
}
#[async_trait]
impl Storage for Published {
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        if !key.starts_with("heads/") {
            return self.files.get(key, max_bytes).await;
        }
        let slot = slot(key)?;
        for _ in 0..4 {
            let bucket = self.authority.read_fresh().await?;
            let directory = heads(&bucket.info)?;
            let Some(value) = directory.get(&slot) else {
                return Ok(None);
            };
            let pointer = parse_pointer(value)?;
            // Collection may remove an old manifest after this pointer read. Restart wholly.
            let Some(object) = self.files.get(&pointer.key, max_bytes).await? else {
                continue;
            };
            if hex::encode(Sha256::digest(&object.bytes)) != pointer.sha256 {
                bail!("published manifest checksum mismatch");
            }
            return Ok(Some(Object {
                bytes: object.bytes,
                version: Version(UpdateVersion {
                    e_tag: Some(value.clone()),
                    version: Some(bucket.revision.to_string()),
                }),
            }));
        }
        bail!("published manifest changed repeatedly or is unavailable")
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        let slot = slot(key)?;
        let bucket = self.authority.read().await?;
        let mut directory = heads(&bucket.info)?;
        let matches = match old {
            None => !directory.contains_key(&slot),
            // The engine version is the unique manifest identity. Metadata read barriers
            // and unrelated databases may advance the bucket without changing this head.
            Some(v) => directory.get(&slot) == v.0.e_tag.as_ref(),
        };
        if !matches {
            return Ok(false);
        }
        let pointer = Pointer {
            key: format!("manifests/{}", Uuid::new_v4()),
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        let mut info = bucket.info.clone();
        directory.insert(slot, serde_json::to_string(&pointer)?);
        info.insert(HEAD.into(), serde_json::to_string(&directory)?);
        budget(&info)?; // Reject admission before allocating an immutable manifest.
        self.files.put(&pointer.key, bytes).await?;
        // Never retry this upload/pointer against a later revision: a collector may have deleted it.
        self.authority
            .replace(&bucket, info)
            .await
            .map(|v| v.is_some())
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
            let bucket = self.authority.read().await?;
            let mut keys: Vec<_> = heads(&bucket.info)?
                .into_keys()
                .map(|d| format!("heads/{d}.json"))
                .filter(|k| after.is_none_or(|a| k.as_str() > a))
                .collect();
            keys.sort();
            keys.truncate(limit);
            Ok(keys)
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
