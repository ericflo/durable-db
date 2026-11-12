//! Shared immutable-manifest publication protocol for B2 and S3.
use crate::{
    cache::Cache,
    directory::{Directory, MissingNode, Pointer, Root, witness},
    metrics::{self, Event, Operation, Outcome},
    observation::Observations,
    storage::{Object, Storage, Version},
    upload_budget::{self, Allocator, UploadLimits, UploadRequest, UploadResult, UploadStatus},
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Semaphore;
use uuid::Uuid;
pub(crate) mod control;
mod data_collection;
mod proposal;

const HEAD: &str = "durable-db-root";
const CURSOR: &str = "durable-db-gc";
const INDEX_CURSOR: &str = "durable-db-index-gc";
const PROPOSAL_CURSOR: &str = "durable-db-proposal-gc";
const HELP_CURSOR: &str = "durable-db-proposal-scan";
const DATA_CURSOR: &str = "durable-db-data-gc";
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
    /// Every attempt must include a fresh, never-reused mutation identity in `info`.
    /// B2 may accept identical metadata despite a stale revision; callers must not
    /// treat a repeated payload as a new conditional validation or serving grant.
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
    helpers: Semaphore,
    proposal_cleanup: proposal::Cleanup,
    upload_budget: Option<Arc<Allocator>>,
    global_collection: bool,
}
impl Published {
    pub async fn open(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
    ) -> anyhow::Result<Self> {
        Self::open_with_budget(files, authority, prefix, None).await
    }
    /// `files.put` must send at most one immutable PUT attempt per invocation.
    /// The built-in B2/S3 bindings enforce this at both SDK and HTTP layers.
    pub async fn open_with_budget(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
        limits: Option<UploadLimits>,
    ) -> anyhow::Result<Self> {
        Self::open_directory(files, authority, prefix, limits, false).await
    }
    pub(crate) async fn open_partitioned(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
        limits: UploadLimits,
    ) -> anyhow::Result<Self> {
        Self::open_directory(files, authority, prefix, Some(limits), true).await
    }
    async fn open_directory(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
        limits: Option<UploadLimits>,
        partitioned: bool,
    ) -> anyhow::Result<Self> {
        if let Some(limits) = limits {
            limits.validate()?;
        }
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
                if !(if partitioned {
                    format == "6"
                } else {
                    ["3", "4", "5"].contains(&format.as_str())
                }) || bucket.info.get("durable-db-prefix").map(String::as_str) != Some(prefix)
                {
                    bail!("publication directory is initialized with a different format or prefix");
                }
                if format == "3" && limits.is_some() {
                    bail!(
                        "upload budget activation requires a fresh namespace; format-3 migration is unsupported"
                    );
                }
                root(&bucket.info)?;
                if format != "3" {
                    upload_budget::validate(&bucket)?;
                }
                return Ok(Self::initialized(files, authority, format));
            }
            if bucket
                .info
                .keys()
                .any(|k| k.starts_with("durable-db-") && !control::is_probe(k))
            {
                bail!("unrecognized durable-db bucket metadata");
            }
            let mut info = bucket.info.clone();
            let format = if partitioned {
                "6"
            } else if limits.is_some() {
                "5"
            } else {
                "3"
            };
            info.insert("durable-db-format".into(), format.into());
            info.insert("durable-db-prefix".into(), prefix.into());
            if let Some(limits) = limits {
                info.insert(upload_budget::FIELD.into(), upload_budget::initial(limits)?);
                info.insert(
                    HEAD.into(),
                    serde_json::to_string(&BudgetRoot {
                        upload_format: if partitioned { 6 } else { 5 },
                        directory: None,
                        layout: None,
                    })?,
                );
            }
            budget(&info)?;
            if authority.replace(&bucket, info).await?.is_some() {
                return Ok(Self::initialized(files, authority, format));
            }
        }
        bail!("publication directory initialization is contended; retry startup")
    }
    pub(crate) async fn configure_layout(
        &self,
        layout: crate::providers::partitions::Layout,
    ) -> anyhow::Result<()> {
        layout.validate()?;
        for _ in 0..8 {
            let old = self.authority.read_fresh().await?;
            anyhow::ensure!(
                budgeted_format(&old.info) == Some(6),
                "layout requires a partition directory"
            );
            root(&old.info)?;
            let mut envelope: BudgetRoot =
                serde_json::from_str(old.info.get(HEAD).context("missing partition root")?)?;
            if let Some(existing) = envelope.layout {
                anyhow::ensure!(
                    existing == layout,
                    "cluster layout differs; migration is required"
                );
                return Ok(());
            }
            anyhow::ensure!(
                envelope.directory.is_none(),
                "cluster layout requires an empty directory"
            );
            envelope.layout = Some(layout);
            let mut info = old.info.clone();
            info.insert(HEAD.into(), serde_json::to_string(&envelope)?);
            info.insert("durable-db-probe".into(), Uuid::new_v4().to_string());
            budget(&info)?;
            if self.authority.replace(&old, info).await?.is_some() {
                return Ok(());
            }
        }
        bail!("cluster layout initialization is contended; retry startup")
    }

    fn initialized(
        mut files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        format: &str,
    ) -> Self {
        let upload_budget = (format != "3").then(|| Arc::new(Allocator::new(authority.clone())));
        if let Some(allocator) = &upload_budget {
            files = Arc::new(upload_budget::Files {
                inner: files,
                allocator: allocator.clone(),
            });
        }
        Self {
            cache: None,
            helpers: Semaphore::new(1),
            proposal_cleanup: proposal::Cleanup::new(authority.clone()),
            observations: Observations::new(authority.clone()),
            directory: Directory::new(files.clone()),
            files,
            authority,
            upload_budget,
            global_collection: format == "5",
        }
    }
    /// An unvalidated control observation, never a data read or serving grant.
    /// The caller must subsequently compare this exact pointer or validate a
    /// fresh writer permit. Stale/missing metadata may cause refusal or retry.
    pub(crate) async fn observe_head(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> anyhow::Result<Option<Object>> {
        self.head(key, max_bytes, false).await
    }

    /// Return the exact published pointer, never a later readback's identity.
    /// Success records this invocation's publication, not continuing ownership.
    pub(crate) async fn compare_head(
        &self,
        key: &str,
        old: Option<&Version>,
        bytes: Bytes,
    ) -> anyhow::Result<Option<Version>> {
        let timer = metrics::REGISTRY.start(Operation::Publication);
        let result = self.publish(key, old, bytes).await;
        timer.finish(match &result {
            Ok(Some(_)) => Outcome::Ok,
            Ok(None) => Outcome::Conflict,
            Err(_) => Outcome::Error,
        });
        result
    }

    fn head_version(&self, pointer: &Pointer, bucket: &Bucket) -> anyhow::Result<Version> {
        Ok(Version(UpdateVersion {
            e_tag: Some(serde_json::to_string(pointer)?),
            version: Some(if self.global_collection {
                serde_json::to_string(&DataObservation {
                    format: 5,
                    fence: bucket.info.get("durable-db-fence").cloned(),
                })?
            } else {
                bucket.revision.to_string()
            }),
        }))
    }

    async fn head(
        &self,
        key: &str,
        max_bytes: usize,
        fresh: bool,
    ) -> anyhow::Result<Option<Object>> {
        let slot = slot(key)?;
        for _ in 0..4 {
            let bucket = if fresh {
                self.observations.read().await?
            } else {
                self.authority.read().await?
            };
            let root = root(&bucket.info)?;
            let pointer = match self.directory.get(root.as_ref(), &slot).await {
                Err(e) if e.is::<MissingNode>() => continue,
                other => other?,
            };
            let Some(pointer) = pointer else {
                return Ok(None);
            };
            // Only immutable bytes are cached. Raw observations remain hints;
            // fresh callers seal their group before the remote read barrier.
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
                version: self.head_version(&pointer, &bucket)?,
            }));
        }
        bail!("published manifest changed repeatedly or is unavailable")
    }

    /// Shares the engine's bounded cache; no extra unaccounted index cache.
    pub fn with_cache(mut self, cache: Arc<Cache>) -> Self {
        self.directory = self.directory.with_cache(cache.clone());
        self.cache = Some(cache);
        self
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
        let proposals = self
            .files
            .list("proposals/", cursor(&before, PROPOSAL_CURSOR), 128)
            .await?;
        let current = self.authority.read().await?;
        let protected = root(&current.info)?;
        let mut info = current.info.clone();
        for (field, candidates) in [
            (CURSOR, &manifests),
            (INDEX_CURSOR, &indexes),
            (PROPOSAL_CURSOR, &proposals),
        ] {
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
        // All listed proposals precede this fence. Even a helper retaining
        // their bytes cannot publish across it. No reference test is needed.
        for key in proposals {
            metrics::REGISTRY.event(Event::ProposalCandidates, 1);
            self.authority.delete_versions(&key).await?;
            metrics::REGISTRY.event(Event::ProposalDeleteAck, 1);
        }
        self.directory
            .collect_unreachable(&[protected], &manifests, &indexes, &*self.authority)
            .await
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetRoot {
    upload_format: u8,
    directory: Option<Root>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    layout: Option<crate::providers::partitions::Layout>,
}
fn root(info: &BTreeMap<String, String>) -> anyhow::Result<Option<Root>> {
    if let Some(format) = budgeted_format(info) {
        let envelope: BudgetRoot =
            serde_json::from_str(info.get(HEAD).context("missing budgeted root")?)?;
        anyhow::ensure!(
            envelope.upload_format == format,
            "unsupported budgeted root"
        );
        if let Some(layout) = envelope.layout {
            anyhow::ensure!(format == 6, "layout requires a partition directory");
            layout.validate()?;
        }
        if let Some(root) = &envelope.directory {
            root.validate()?;
        }
        return Ok(envelope.directory);
    }
    info.get(HEAD)
        .map(|value| {
            let root: Root = serde_json::from_str(value)?;
            root.validate()?;
            Ok(root)
        })
        .transpose()
}
fn write_root(info: &mut BTreeMap<String, String>, root: Root) -> anyhow::Result<()> {
    let encoded = if let Some(format) = budgeted_format(info) {
        let mut envelope: BudgetRoot =
            serde_json::from_str(info.get(HEAD).context("missing budgeted root")?)?;
        anyhow::ensure!(
            envelope.upload_format == format,
            "unsupported budgeted root"
        );
        envelope.directory = Some(root);
        serde_json::to_string(&envelope)?
    } else {
        serde_json::to_string(&root)?
    };
    info.insert(HEAD.into(), encoded);
    Ok(())
}
fn budgeted_format(info: &BTreeMap<String, String>) -> Option<u8> {
    match info.get("durable-db-format").map(String::as_str) {
        Some("4") => Some(4),
        Some("5") => Some(5),
        Some("6") => Some(6),
        _ => None,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataObservation {
    format: u8,
    fence: Option<String>,
}
fn cursor<'a>(bucket: &'a Bucket, field: &str) -> Option<&'a str> {
    bucket
        .info
        .get(field)
        .filter(|v| !v.is_empty())
        .map(String::as_str)
}
pub(crate) fn budget(info: &BTreeMap<String, String>) -> anyhow::Result<()> {
    if info.get("durable-db-format").map(String::as_str) == Some(crate::ownership::root::VERSION) {
        return crate::ownership::root::metadata_budget(info);
    }
    // Admission must leave room for later reads and maintenance. Otherwise a valid
    // create could fill the directory and prevent its own read/collection fences.
    let mut reserved = info.clone();
    for (field, size) in [
        ("durable-db-probe", 36),
        ("durable-db-read-fence", 36),
        ("durable-db-fence", 36),
        (CURSOR, 111),
        (INDEX_CURSOR, 109),
        (PROPOSAL_CURSOR, 111),
        (HELP_CURSOR, 111),
        (
            DATA_CURSOR,
            if info.get("durable-db-format").map(String::as_str) == Some("5") {
                111
            } else {
                0
            },
        ),
        (HEAD, 1024),
        (
            upload_budget::FIELD,
            if info.contains_key(upload_budget::FIELD) {
                2304
            } else {
                0
            },
        ),
    ] {
        if size > 0 && reserved.get(field).is_none_or(|v| v.len() < size) {
            reserved.insert(field.into(), "0".repeat(size));
        }
    }
    if info.keys().any(|k| k.len() > 50)
        || serde_json::to_vec(&control::reserved_wire(&reserved)?)?.len() > MAX_INFO
    {
        bail!("publication authority metadata budget exceeded");
    }
    Ok(())
}
#[async_trait]
impl Storage for Published {
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        anyhow::ensure!(
            !key.starts_with("heads/"),
            "range reads require immutable objects"
        );
        self.files.get_range(key, range, object_size).await
    }
    async fn upload_budget(&self) -> anyhow::Result<Option<UploadStatus>> {
        match &self.upload_budget {
            Some(allocator) => Ok(Some(allocator.status().await?)),
            None => Ok(None),
        }
    }
    async fn replenish_upload_budget(
        &self,
        request: &UploadRequest,
    ) -> crate::Result<UploadResult> {
        let allocator = self.upload_budget.as_ref().ok_or_else(|| {
            crate::Error::Invalid("this storage namespace has no upload budget".into())
        })?;
        allocator.replenish(request).await
    }
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        if !key.starts_with("heads/") {
            return self.files.get(key, max_bytes).await;
        }
        self.head(key, max_bytes, true).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        self.compare_head(key, old, bytes)
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
        if self.global_collection {
            self.collect_data().await?;
        }
        Ok(())
    }
}
