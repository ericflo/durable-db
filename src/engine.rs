use crate::{
    Error, Result,
    auth::Access,
    cache::Cache,
    storage::{Storage, Version},
};
use anyhow::{Context, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;
mod access;
mod creation;
pub use access::{PolicyAction, PolicyInfo, PolicyRequest, PolicyResult};
pub use creation::{CreateRequest, CreateResult};

pub const MAX_BODY: usize = 1024 * 1024;
pub(crate) const MAX_HEAD: usize = 512 * 1024;
const TARGET_BLOCK: usize = 128 * 1024;
const MAX_BLOCK: usize = 256 * 1024;
const MAX_PAGES: usize = 2048;
const MAX_WORK: usize = 8 * 1024 * 1024;
const MAX_RESULT: usize = 1024 * 1024;
const MAX_OPS: usize = 128;
type Block = BTreeMap<String, Vec<u8>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_bytes: u64,
    pub max_keys: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_keys: 100_000,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Page {
    first: String,
    last: String,
    object: String,
    digest: String,
    bytes: u64,
    keys: u64,
    encoded: usize,
}
#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    id: String,
    digest: String,
    revision: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    format: u32,
    revision: u64,
    fence: String,
    limits: Limits,
    bytes: u64,
    keys: u64,
    pages: Vec<Page>,
    receipts: Vec<Receipt>,
    gc_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    policy: Option<access::Policy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    creation: Option<creation::CreationReceipt>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Info {
    pub revision: u64,
    pub bytes: u64,
    pub keys: u64,
    pub pages: usize,
    pub limits: Limits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyInfo>,
}
impl Manifest {
    fn empty(limits: Limits, policy: Option<access::Policy>) -> Self {
        Self {
            format: if policy.is_some() { 2 } else { 1 },
            revision: 0,
            fence: Uuid::new_v4().to_string(),
            limits,
            bytes: 0,
            keys: 0,
            pages: Vec::new(),
            receipts: Vec::new(),
            gc_after: None,
            policy,
            creation: None,
        }
    }
    fn validate(&self, db: &str) -> anyhow::Result<()> {
        ensure!(
            ((self.format == 1 && self.policy.is_none())
                || (self.format == 2 && self.policy.is_some()))
                && self.pages.len() <= MAX_PAGES,
            "unsupported manifest"
        );
        if let Some(policy) = &self.policy {
            policy.validate()?;
        }
        if let Some(creation) = &self.creation {
            creation.validate(self)?;
        }
        Uuid::parse_str(&self.fence).context("invalid manifest fence")?;
        ensure!(
            self.limits.max_bytes > 0
                && self.limits.max_bytes <= 256 * 1024 * 1024
                && self.limits.max_keys > 0
                && self.limits.max_keys <= 1_000_000,
            "invalid stored quotas"
        );
        let mut bytes = 0u64;
        let mut keys = 0u64;
        let mut objects = BTreeSet::new();
        for page in &self.pages {
            validate_key(&page.first).context("invalid stored first key")?;
            validate_key(&page.last).context("invalid stored last key")?;
            validate_object(db, &page.object)?;
            ensure!(objects.insert(&page.object), "repeated page object");
            ensure!(
                page.first <= page.last
                    && page.keys > 0
                    && page.bytes >= page.keys
                    && page.encoded > 0
                    && page.encoded <= MAX_BLOCK
                    && valid_digest(&page.digest),
                "invalid page metadata"
            );
            bytes = bytes
                .checked_add(page.bytes)
                .context("page byte count overflow")?;
            keys = keys
                .checked_add(page.keys)
                .context("page key count overflow")?;
        }
        ensure!(
            !self.pages.windows(2).any(|p| p[0].last >= p[1].first),
            "overlapping or unordered pages"
        );
        ensure!(
            bytes == self.bytes
                && keys == self.keys
                && bytes <= self.limits.max_bytes
                && keys <= self.limits.max_keys,
            "manifest accounting does not match pages or quotas"
        );
        if let Some(cursor) = &self.gc_after {
            validate_object(db, cursor)?;
        }
        ensure!(self.receipts.len() <= 128, "too many stored receipts");
        let mut ids = BTreeSet::new();
        let mut previous = 0;
        for receipt in &self.receipts {
            ensure!(
                !receipt.id.is_empty()
                    && receipt.id.len() <= 128
                    && ids.insert(&receipt.id)
                    && valid_digest(&receipt.digest)
                    && receipt.revision > previous
                    && receipt.revision <= self.revision,
                "invalid stored receipt"
            );
            previous = receipt.revision;
        }
        Ok(())
    }
    fn info(&self) -> Info {
        Info {
            revision: self.revision,
            bytes: self.bytes,
            keys: self.keys,
            pages: self.pages.len(),
            limits: self.limits.clone(),
            policy: self.policy.as_ref().map(access::Policy::info),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub scans: Vec<Scan>,
}
impl ReadRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.keys.len() + self.scans.len() > MAX_OPS {
            return Err(Error::Limit("too many reads".into()));
        }
        for key in &self.keys {
            validate_key(key)?;
        }
        for scan in &self.scans {
            if scan.limit == 0
                || scan.limit > 1000
                || scan.prefix.len() > 1024
                || scan.prefix.contains('\0')
            {
                return Err(Error::Invalid(
                    "scan limit must be 1..1000 and prefix at most 1024 bytes without NUL".into(),
                ));
            }
            for key in [&scan.start, &scan.end, &scan.after].into_iter().flatten() {
                validate_key(key)?;
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scan {
    #[serde(default)]
    pub prefix: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub after: Option<String>,
    #[serde(default)]
    pub reverse: bool,
    pub limit: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pair {
    pub key: String,
    pub value: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    pub items: Vec<Pair>,
    pub next: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    pub revision: u64,
    pub values: Vec<Option<String>>,
    pub scans: Vec<ScanResult>,
}
impl ReadResult {
    pub(crate) fn validate_size(&self) -> Result<()> {
        if serde_json::to_vec(self)
            .context("serialize read response")?
            .len()
            > MAX_RESULT
        {
            return Err(Error::Limit(
                "encoded read result exceeds 1 MiB; reduce scan limit".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    pub key: String,
    pub value: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
    pub key: String,
    pub value: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub expected_revision: u64,
    pub request_id: String,
    #[serde(default)]
    pub checks: Vec<Check>,
    pub writes: Vec<Write>,
}
pub(crate) struct DecodedCommit {
    pub digest: [u8; 32],
    pub checks: Vec<(String, Option<Vec<u8>>)>,
    pub writes: BTreeMap<String, Option<Vec<u8>>>,
}
impl CommitRequest {
    /// Validate the complete request before storage I/O. The original wire
    /// fields, including order and duplicate writes, define retry identity.
    pub(crate) fn decode(&self) -> Result<DecodedCommit> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || self.writes.is_empty()
            || self.writes.len() + self.checks.len() > MAX_OPS
        {
            return Err(Error::Invalid(
                "commit needs a 1..128-byte request_id and 1..128 operations".into(),
            ));
        }
        let mut checks = Vec::new();
        let mut writes = BTreeMap::new();
        let mut size = self.request_id.len();
        for (key, value, check) in self
            .checks
            .iter()
            .map(|c| (&c.key, &c.value, true))
            .chain(self.writes.iter().map(|w| (&w.key, &w.value, false)))
        {
            validate_key(key)?;
            size += key.len() + value.as_ref().map_or(0, String::len);
            if size > MAX_BODY {
                return Err(Error::Limit("commit exceeds 1 MiB".into()));
            }
            let value = decode_value(value)?;
            if check {
                checks.push((key.clone(), value));
            } else {
                writes.insert(key.clone(), value);
            }
        }
        let encoded = serde_json::to_vec(self).context("serialize request")?;
        if encoded.len() > MAX_BODY {
            return Err(Error::Limit("commit exceeds 1 MiB".into()));
        }
        Ok(DecodedCommit {
            digest: Sha256::digest(encoded).into(),
            checks,
            writes,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResult {
    pub revision: u64,
    pub replayed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcResult {
    pub examined: usize,
    pub deleted: usize,
}

pub struct Engine {
    storage: Arc<dyn Storage>,
    cache: Arc<Cache>,
}
/// A collector must validate the complete protected manifest before treating
/// any absent page as unreachable. Missing/corrupt metadata is never emptiness.
pub(crate) fn protected_pages(db: &str, bytes: &[u8]) -> anyhow::Result<BTreeSet<String>> {
    ensure!(
        bytes.len() <= MAX_HEAD,
        "manifest exceeds collector read bound"
    );
    let manifest: Manifest = serde_json::from_slice(bytes)?;
    manifest.validate(db)?;
    Ok(manifest.pages.into_iter().map(|p| p.object).collect())
}
impl Engine {
    pub fn new(storage: Arc<dyn Storage>, cache: Arc<Cache>) -> Self {
        Self { storage, cache }
    }
    pub async fn maintain_storage(&self) -> Result<()> {
        self.storage.maintenance().await?;
        Ok(())
    }
    pub fn cache(&self) -> &Cache {
        &self.cache
    }
    pub async fn upload_budget(&self) -> Result<Option<crate::upload_budget::UploadStatus>> {
        Ok(self.storage.upload_budget().await?)
    }
    pub async fn replenish_upload_budget(
        &self,
        request: &crate::upload_budget::UploadRequest,
    ) -> Result<crate::upload_budget::UploadResult> {
        self.storage.replenish_upload_budget(request).await
    }
    pub async fn create(&self, db: &str, limits: Limits) -> Result<Info> {
        validate_db(db)?;
        validate_limits(&limits)?;
        let manifest = Manifest::empty(limits, None);
        if !self
            .storage
            .cas(&head(db), None, encode_head(&manifest)?)
            .await?
        {
            return Err(Error::Conflict);
        }
        Ok(manifest.info())
    }
    async fn manifest(&self, db: &str) -> Result<(Manifest, Version)> {
        validate_db(db)?;
        let obj = self
            .storage
            .get(&head(db), MAX_HEAD)
            .await?
            .ok_or(Error::NotFound)?;
        let manifest: Manifest = serde_json::from_slice(&obj.bytes).context("invalid manifest")?;
        manifest.validate(db)?;
        Ok((manifest, obj.version))
    }
    pub async fn info(&self, db: &str) -> Result<Info> {
        self.info_with_access(db, &Access::Administrator).await
    }
    pub async fn info_with_access(&self, db: &str, access: &Access) -> Result<Info> {
        let (manifest, _) = self.manifest(db).await?;
        manifest.authorize(db, access, false)?;
        Ok(manifest.info())
    }
    async fn block(&self, page: &Page) -> Result<Option<Block>> {
        let bytes = if let Some(bytes) = self.cache.get(&page.object, &page.digest).await {
            bytes
        } else {
            let Some(object) = self.storage.get(&page.object, MAX_BLOCK).await? else {
                return Ok(None);
            };
            if hex::encode(Sha256::digest(&object.bytes)) != page.digest {
                return Err(anyhow::anyhow!("block checksum mismatch").into());
            }
            self.cache.insert(&page.object, object.bytes.clone()).await;
            object.bytes
        };
        Ok(Some(decode_block(page, &bytes)?))
    }

    async fn load(
        &self,
        m: &Manifest,
        i: usize,
        loaded: &mut BTreeMap<usize, Block>,
        work: &mut usize,
    ) -> Result<bool> {
        if loaded.contains_key(&i) {
            return Ok(true);
        }
        *work = work.saturating_add(m.pages[i].encoded);
        if *work > MAX_WORK {
            return Err(Error::Limit(
                "transaction read budget exceeded; use smaller pages or transactions".into(),
            ));
        }
        let Some(block) = self.block(&m.pages[i]).await? else {
            return Ok(false);
        };
        loaded.insert(i, block);
        Ok(true)
    }
    pub async fn read(&self, db: &str, request: &ReadRequest) -> Result<ReadResult> {
        self.read_with_access(db, request, &Access::Administrator)
            .await
    }
    pub async fn read_with_access(
        &self,
        db: &str,
        request: &ReadRequest,
        access: &Access,
    ) -> Result<ReadResult> {
        validate_db(db)?;
        request.validate()?;
        // A collector may remove a block from an old snapshot. Never return partial reads;
        // restart the ENTIRE read from a fresh manifest, or reject expected_revision.
        'retry: for _ in 0..4 {
            let (m, _) = self.manifest(db).await?;
            m.authorize(db, access, false)?;
            if request.expected_revision.is_some_and(|v| v != m.revision) {
                return Err(Error::Conflict);
            }
            let mut loaded = BTreeMap::new();
            let mut work = 0;
            let mut size = 0;
            let mut result = ReadResult {
                revision: m.revision,
                values: Vec::new(),
                scans: Vec::new(),
            };
            for key in &request.keys {
                let value = if let Some(i) = page_for(&m, key) {
                    if !self.load(&m, i, &mut loaded, &mut work).await? {
                        continue 'retry;
                    }
                    loaded[&i].get(key).map(|v| STANDARD.encode(v))
                } else {
                    None
                };
                size += value.as_ref().map_or(0, String::len);
                if size > MAX_RESULT {
                    return Err(Error::Limit("read result exceeds 1 MiB".into()));
                }
                result.values.push(value);
            }
            for scan in &request.scans {
                let mut indices: Vec<_> = (0..m.pages.len())
                    .filter(|i| page_matches(&m.pages[*i], scan))
                    .collect();
                if scan.reverse {
                    indices.reverse();
                }
                let mut items = Vec::new();
                let mut more = false;
                'pages: for i in indices {
                    if !self.load(&m, i, &mut loaded, &mut work).await? {
                        continue 'retry;
                    }
                    let block = &loaded[&i];
                    let iter: Box<dyn Iterator<Item = (&String, &Vec<u8>)> + Send> = if scan.reverse
                    {
                        Box::new(block.iter().rev())
                    } else {
                        Box::new(block.iter())
                    };
                    for (key, value) in iter {
                        if !matches_scan(key, scan) {
                            continue;
                        }
                        if items.len() == scan.limit {
                            more = true;
                            break 'pages;
                        }
                        let value = STANDARD.encode(value);
                        size += key.len() + value.len();
                        if size > MAX_RESULT {
                            return Err(Error::Limit(
                                "read result exceeds 1 MiB; reduce scan limit".into(),
                            ));
                        }
                        items.push(Pair {
                            key: key.clone(),
                            value,
                        });
                    }
                }
                let next = if more {
                    items.last().map(|v| v.key.clone())
                } else {
                    None
                };
                result.scans.push(ScanResult { items, next });
            }
            // Escaped JSON keys and framing also consume the response budget.
            result.validate_size()?;
            return Ok(result);
        }
        Err(Error::Storage(anyhow::anyhow!(
            "snapshot unavailable after bounded retries"
        )))
    }
    pub async fn commit(&self, db: &str, request: &CommitRequest) -> Result<CommitResult> {
        self.commit_with_access(db, request, &Access::Administrator)
            .await
    }
    pub async fn commit_with_access(
        &self,
        db: &str,
        request: &CommitRequest,
        access: &Access,
    ) -> Result<CommitResult> {
        validate_db(db)?;
        let decoded = request.decode()?;
        let digest = hex::encode(decoded.digest);
        let (mut m, version) = self.manifest(db).await?;
        m.authorize(db, access, true)?;
        if let Some(receipt) = m.receipts.iter().find(|r| r.id == request.request_id) {
            if receipt.digest != digest {
                return Err(Error::Invalid(
                    "request_id was used for a different request".into(),
                ));
            }
            return Ok(CommitResult {
                revision: receipt.revision,
                replayed: true,
            });
        }
        if m.revision != request.expected_revision {
            return Err(Error::Conflict);
        }
        let mut loaded = BTreeMap::new();
        let mut work = 0;
        for key in decoded
            .writes
            .keys()
            .chain(decoded.checks.iter().map(|(key, _)| key))
        {
            if let Some(i) = page_for(&m, key)
                && !self.load(&m, i, &mut loaded, &mut work).await?
            {
                return Err(Error::Conflict);
            }
        }
        for (key, value) in &decoded.checks {
            let actual = page_for(&m, key).and_then(|i| loaded[&i].get(key));
            if actual != value.as_ref() {
                return Err(Error::Conflict);
            }
        }
        let mut changed = BTreeSet::new();
        let mut new_block = Block::new();
        for (key, value) in decoded.writes {
            let block = if let Some(i) = page_for(&m, &key) {
                changed.insert(i);
                loaded.get_mut(&i).expect("loaded above")
            } else {
                &mut new_block
            };
            if let Some(value) = value {
                block.insert(key, value);
            } else {
                block.remove(&key);
            }
        }
        let mut pages = Vec::new();
        let mut uploads = Vec::new();
        for (i, page) in m.pages.iter().enumerate() {
            if changed.contains(&i) {
                prepare_blocks(
                    db,
                    loaded.remove(&i).expect("loaded"),
                    &mut pages,
                    &mut uploads,
                )?;
            } else {
                pages.push(page.clone());
            }
        }
        if !new_block.is_empty() {
            prepare_blocks(db, new_block, &mut pages, &mut uploads)?;
        }
        pages.sort_by(|a, b| a.first.cmp(&b.first));
        m.bytes = pages.iter().map(|p| p.bytes).sum();
        m.keys = pages.iter().map(|p| p.keys).sum();
        if m.bytes > m.limits.max_bytes || m.keys > m.limits.max_keys || pages.len() > MAX_PAGES {
            return Err(Error::Limit("database quota or page limit exceeded".into()));
        }
        m.pages = pages;
        m.revision = m.revision.checked_add(1).context("revision exhausted")?;
        m.fence = Uuid::new_v4().to_string();
        if m.receipts.len() >= 128 {
            m.receipts.remove(0);
        }
        m.receipts.push(Receipt {
            id: request.request_id.clone(),
            digest,
            revision: m.revision,
        });
        let head_bytes = encode_head(&m)?;
        // Bound each attempt's upload amplification before any write reaches the provider.
        if uploads.iter().map(|(_, b)| b.len()).sum::<usize>() > MAX_WORK {
            return Err(Error::Limit("commit rewrite budget exceeds 8 MiB".into()));
        }
        // Uploads complete before the head CAS; each attempt uses globally fresh names.
        // On conflict/error do NOT eagerly delete: an ambiguous CAS may have committed.
        for (key, bytes) in uploads {
            self.storage.put(&key, bytes.clone()).await?;
            self.cache.insert(&key, bytes).await;
        }
        if !self
            .storage
            .cas(&head(db), Some(&version), head_bytes)
            .await?
        {
            return Err(Error::Conflict);
        }
        Ok(CommitResult {
            revision: m.revision,
            replayed: false,
        })
    }
    pub async fn gc(&self, db: &str) -> Result<GcResult> {
        let (before, _) = self.manifest(db).await?;
        self.gc_from(db, before).await
    }
    async fn gc_from(&self, db: &str, before: Manifest) -> Result<GcResult> {
        let prefix = format!("blocks/{db}.");
        // Protocol order is essential: finish listing BEFORE reading/fencing the head.
        let candidates = self
            .storage
            .list(&prefix, before.gc_after.as_deref(), 256)
            .await?;
        crate::metrics::REGISTRY.event(crate::metrics::Event::DataCandidates, candidates.len());
        let next_cursor = if candidates.len() == 256 {
            candidates.last().cloned()
        } else {
            None
        };
        let before_live: BTreeSet<_> = before.pages.iter().map(|p| &p.object).collect();
        // An earlier snapshot can justify doing NO work. A concurrent write
        // may leave new garbage for a later sweep, but this branch deletes
        // nothing and publishes nothing. Never use this snapshot to delete.
        if next_cursor == before.gc_after && candidates.iter().all(|k| before_live.contains(k)) {
            return Ok(GcResult {
                examined: candidates.len(),
                deleted: 0,
            });
        }
        // Any actual deletion/cursor change still reads AFTER listing and must
        // publish a successful fence. Cached pre-list observations cannot do so.
        let (mut m, version) = self.manifest(db).await?;
        let live: BTreeSet<_> = m.pages.iter().map(|p| p.object.clone()).collect();
        if next_cursor == m.gc_after && candidates.iter().all(|k| live.contains(k)) {
            return Ok(GcResult {
                examined: candidates.len(),
                deleted: 0,
            });
        }
        m.gc_after = next_cursor;
        m.fence = Uuid::new_v4().to_string();
        if !self
            .storage
            .cas(&head(db), Some(&version), encode_head(&m)?)
            .await?
        {
            return Err(Error::Conflict);
        }
        let mut result = GcResult {
            examined: candidates.len(),
            deleted: 0,
        };
        for key in candidates {
            if !live.contains(&key) {
                self.storage.delete(&key).await?;
                crate::metrics::REGISTRY.event(crate::metrics::Event::DataDeleteAck, 1);
                result.deleted += 1;
            }
        }
        Ok(result)
    }
    /// Merge one adjacent pair of small pages. Physical maintenance changes the CAS
    /// fence, but never the user-visible revision or the logical key/value state.
    pub async fn compact(&self, db: &str) -> Result<bool> {
        let (m, version) = self.manifest(db).await?;
        self.compact_from(db, &m, &version).await
    }
    /// Share the initial observation between compaction and no-op collection.
    /// A real deletion always obtains its own post-list observation and fence.
    pub async fn maintain_database(&self, db: &str) -> Result<GcResult> {
        let (m, version) = self.manifest(db).await?;
        if self.compact_from(db, &m, &version).await? {
            self.gc(db).await
        } else {
            self.gc_from(db, m).await
        }
    }
    async fn compact_from(&self, db: &str, before: &Manifest, version: &Version) -> Result<bool> {
        let Some(i) = before
            .pages
            .windows(2)
            .position(|p| p[0].encoded + p[1].encoded <= TARGET_BLOCK)
        else {
            return Ok(false);
        };
        let mut m = before.clone();
        let Some(mut block) = self.block(&m.pages[i]).await? else {
            return Err(Error::Conflict);
        };
        let Some(other) = self.block(&m.pages[i + 1]).await? else {
            return Err(Error::Conflict);
        };
        block.extend(other);
        let mut pages = Vec::new();
        let mut uploads = Vec::new();
        prepare_blocks(db, block, &mut pages, &mut uploads)?;
        if pages.len() != 1 {
            return Ok(false);
        }
        m.pages.splice(i..i + 2, pages);
        m.fence = Uuid::new_v4().to_string();
        let encoded = encode_head(&m)?;
        for (key, bytes) in uploads {
            self.storage.put(&key, bytes.clone()).await?;
            self.cache.insert(&key, bytes).await;
        }
        if !self.storage.cas(&head(db), Some(version), encoded).await? {
            return Err(Error::Conflict);
        }
        Ok(true)
    }
    pub async fn databases(&self, after: Option<&str>) -> Result<Vec<String>> {
        Ok(self
            .storage
            .list("heads/", after, 64)
            .await?
            .into_iter()
            .filter_map(|k| {
                k.strip_prefix("heads/")?
                    .strip_suffix(".json")
                    .map(str::to_owned)
            })
            .collect())
    }
}

fn validate_limits(limits: &Limits) -> Result<()> {
    if limits.max_bytes == 0
        || limits.max_bytes > 256 * 1024 * 1024
        || limits.max_keys == 0
        || limits.max_keys > 1_000_000
    {
        return Err(Error::Invalid(
            "quota outside supported bounds (1..256 MiB, 1..1,000,000 keys)".into(),
        ));
    }
    Ok(())
}
fn head(db: &str) -> String {
    format!("heads/{db}.json")
}
fn decode_block(page: &Page, bytes: &[u8]) -> anyhow::Result<Block> {
    ensure!(
        bytes.len() == page.encoded,
        "page length differs from manifest"
    );
    // Checksums prove byte identity; validate the decoded structure too.
    // A bad count must never let a request bypass MAX_WORK or its quota.
    let (block, trailing): (Block, &[u8]) =
        postcard::take_from_bytes(bytes).context("invalid block encoding")?;
    ensure!(trailing.is_empty(), "trailing bytes in page");
    let mut logical_bytes = 0u64;
    for (key, value) in &block {
        validate_key(key).context("invalid stored key")?;
        ensure!(value.len() <= 64 * 1024, "invalid stored value size");
        logical_bytes = logical_bytes
            .checked_add((key.len() + value.len()) as u64)
            .context("stored page byte count overflow")?;
    }
    ensure!(
        block.len() as u64 == page.keys
            && logical_bytes == page.bytes
            && block
                .first_key_value()
                .is_some_and(|(k, _)| k == &page.first)
            && block.last_key_value().is_some_and(|(k, _)| k == &page.last),
        "page contents differ from manifest accounting or boundaries"
    );
    // Postcard's map decoder accepts duplicate/out-of-order entries. Our
    // canonical BTreeMap encoding does not; reject ambiguous stored bytes.
    ensure!(
        postcard::to_stdvec(&block)? == bytes,
        "noncanonical page encoding"
    );
    Ok(block)
}
fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
}
fn validate_object(db: &str, object: &str) -> anyhow::Result<()> {
    let prefix = format!("blocks/{db}.");
    let id = object
        .strip_prefix(&prefix)
        .context("page object outside database namespace")?;
    Uuid::parse_str(id).context("invalid page object identity")?;
    Ok(())
}
pub fn validate_db(db: &str) -> Result<()> {
    if db.is_empty()
        || db.len() > 64
        || !db
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_".contains(&b))
    {
        return Err(Error::Invalid(
            "database must be 1..64 lowercase ASCII letters, digits, hyphens or underscores".into(),
        ));
    }
    Ok(())
}
fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 1024 || key.contains('\0') {
        return Err(Error::Invalid(
            "keys must be 1..1024 UTF-8 bytes without NUL".into(),
        ));
    }
    Ok(())
}
fn decode_value(value: &Option<String>) -> Result<Option<Vec<u8>>> {
    let Some(value) = value else { return Ok(None) };
    if value.len() > 88 * 1024 {
        return Err(Error::Limit("value exceeds 64 KiB".into()));
    }
    let value = STANDARD
        .decode(value)
        .map_err(|_| Error::Invalid("value must be standard base64".into()))?;
    if value.len() > 64 * 1024 {
        return Err(Error::Limit("value exceeds 64 KiB".into()));
    }
    Ok(Some(value))
}
fn encode_head(m: &Manifest) -> Result<Bytes> {
    let bytes = serde_json::to_vec(m).context("serialize manifest")?;
    if bytes.len() > MAX_HEAD {
        return Err(Error::Limit("manifest exceeds 512 KiB".into()));
    }
    Ok(bytes.into())
}
fn page_for(m: &Manifest, key: &str) -> Option<usize> {
    if m.pages.is_empty() {
        None
    } else {
        Some(
            m.pages
                .partition_point(|p| p.first.as_str() <= key)
                .saturating_sub(1),
        )
    }
}
fn matches_scan(key: &str, s: &Scan) -> bool {
    key.starts_with(&s.prefix)
        && s.start.as_ref().is_none_or(|v| key >= v.as_str())
        && s.end.as_ref().is_none_or(|v| key < v.as_str())
        && s.after.as_ref().is_none_or(|v| {
            if s.reverse {
                key < v.as_str()
            } else {
                key > v.as_str()
            }
        })
}
fn page_matches(p: &Page, s: &Scan) -> bool {
    if s.start.as_ref().is_some_and(|v| &p.last < v)
        || s.end.as_ref().is_some_and(|v| &p.first >= v)
    {
        return false;
    }
    if s.after.as_ref().is_some_and(|v| {
        if s.reverse {
            &p.first >= v
        } else {
            &p.last <= v
        }
    }) {
        return false;
    }
    p.last >= s.prefix && (p.first <= s.prefix || p.first.starts_with(&s.prefix))
}
fn prepare_blocks(
    db: &str,
    block: Block,
    pages: &mut Vec<Page>,
    uploads: &mut Vec<(String, Bytes)>,
) -> Result<()> {
    let mut chunk = Block::new();
    let mut size = 0;
    for (key, value) in block {
        let weight = key.len() + value.len() + 16;
        if size + weight > TARGET_BLOCK && !chunk.is_empty() {
            emit_block(db, &chunk, pages, uploads)?;
            chunk.clear();
            size = 0;
        }
        size += weight;
        chunk.insert(key, value);
    }
    if !chunk.is_empty() {
        emit_block(db, &chunk, pages, uploads)?;
    }
    Ok(())
}
fn emit_block(
    db: &str,
    block: &Block,
    pages: &mut Vec<Page>,
    uploads: &mut Vec<(String, Bytes)>,
) -> Result<()> {
    let bytes = postcard::to_stdvec(block).context("serialize block")?;
    if bytes.len() > MAX_BLOCK {
        return Err(anyhow::anyhow!("block exceeds size bound").into());
    }
    let object = format!("blocks/{db}.{}", Uuid::new_v4());
    pages.push(Page {
        first: block.first_key_value().expect("nonempty").0.clone(),
        last: block.last_key_value().expect("nonempty").0.clone(),
        object: object.clone(),
        digest: hex::encode(Sha256::digest(&bytes)),
        bytes: block.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
        keys: block.len() as u64,
        encoded: bytes.len(),
    });
    uploads.push((object, bytes.into()));
    Ok(())
}
