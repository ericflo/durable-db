//! Native B2 bucket revision CAS is the authority; file overwrites are never CAS.
use crate::storage::{Object, Storage, Version};
use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

pub mod native;
const HEAD: &str = "durable-db-head-";
const CURSOR: &str = "durable-db-gc";
const MAX_HEADS: usize = 32;
const MAX_INFO: usize = 9000;

#[derive(Clone, Debug)]
pub struct Bucket {
    pub revision: u64,
    pub info: BTreeMap<String, String>,
}
#[async_trait]
pub trait Authority: Send + Sync {
    async fn read(&self) -> anyhow::Result<Bucket>;
    /// False only for a definite revision mismatch; network failures are ambiguous.
    async fn replace(&self, old: &Bucket, info: BTreeMap<String, String>) -> anyhow::Result<bool>;
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
pub struct B2 {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
}
impl B2 {
    pub async fn open(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
    ) -> anyhow::Result<Self> {
        for _ in 0..8 {
            let bucket = authority.read().await?;
            if let Some(format) = bucket.info.get("durable-db-format") {
                if format != "1"
                    || bucket.info.get("durable-db-prefix").map(String::as_str) != Some(prefix)
                {
                    bail!("B2 bucket is initialized with a different format or prefix");
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
            info.insert("durable-db-format".into(), "1".into());
            info.insert("durable-db-prefix".into(), prefix.into());
            budget(&info)?;
            if authority.replace(&bucket, info).await? {
                return Ok(Self { files, authority });
            }
        }
        bail!("B2 bucket initialization is contended; retry startup")
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
        let protected: Vec<String> = current
            .info
            .iter()
            .filter(|(k, _)| k.starts_with(HEAD))
            .map(|(_, v)| parse_pointer(v).map(|p| p.key))
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
        if !self.authority.replace(&current, info).await? {
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
        .context("B2 CAS requires a database head")?;
    crate::engine::validate_db(db)?;
    Ok(format!("{HEAD}{db}"))
}
fn budget(info: &BTreeMap<String, String>) -> anyhow::Result<()> {
    if info.keys().filter(|k| k.starts_with(HEAD)).count() > MAX_HEADS
        || serde_json::to_vec(info)?.len() > MAX_INFO
    {
        bail!("B2 bucket metadata budget exceeded (initial format: 32 databases per bucket)");
    }
    Ok(())
}
fn parse_pointer(value: &str) -> anyhow::Result<Pointer> {
    let p: Pointer = serde_json::from_str(value)?;
    let id = p
        .key
        .strip_prefix("manifests/")
        .context("invalid B2 manifest namespace")?;
    Uuid::parse_str(id).context("invalid B2 manifest identity")?;
    if p.sha256.len() != 64 || !p.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid manifest digest");
    }
    Ok(p)
}
#[async_trait]
impl Storage for B2 {
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        if !key.starts_with("heads/") {
            return self.files.get(key, max_bytes).await;
        }
        let slot = slot(key)?;
        for _ in 0..4 {
            let bucket = self.authority.read().await?;
            let Some(value) = bucket.info.get(&slot) else {
                return Ok(None);
            };
            let pointer = parse_pointer(value)?;
            // Collection may remove an old manifest after this pointer read. Restart wholly.
            let Some(object) = self.files.get(&pointer.key, max_bytes).await? else {
                continue;
            };
            if hex::encode(Sha256::digest(&object.bytes)) != pointer.sha256 {
                bail!("B2 manifest checksum mismatch");
            }
            return Ok(Some(Object {
                bytes: object.bytes,
                version: Version(UpdateVersion {
                    e_tag: Some(value.clone()),
                    version: Some(bucket.revision.to_string()),
                }),
            }));
        }
        bail!("B2 manifest changed repeatedly or is unavailable")
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        let slot = slot(key)?;
        let bucket = self.authority.read().await?;
        let matches = match old {
            None => !bucket.info.contains_key(&slot),
            Some(v) => {
                bucket.info.get(&slot) == v.0.e_tag.as_ref()
                    && v.0.version.as_deref() == Some(bucket.revision.to_string().as_str())
            }
        };
        if !matches {
            return Ok(false);
        }
        let pointer = Pointer {
            key: format!("manifests/{}", Uuid::new_v4()),
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        let mut info = bucket.info.clone();
        info.insert(slot, serde_json::to_string(&pointer)?);
        budget(&info)?; // Reject admission before allocating an immutable manifest.
        self.files.put(&pointer.key, bytes).await?;
        // Never retry this upload/pointer against a later revision: a collector may have deleted it.
        self.authority.replace(&bucket, info).await
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
            let mut keys: Vec<_> = bucket
                .info
                .keys()
                .filter_map(|k| k.strip_prefix(HEAD))
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
        self.collect_manifests().await?;
        Ok(())
    }
}
