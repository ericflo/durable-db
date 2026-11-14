//! Nonrefundable, authority-backed admission for immutable upload attempts.
use crate::{
    Error, Result,
    metrics::{Event, REGISTRY},
    publication::{Authority, Bucket},
    storage::{NotSent, Object, Storage, Version},
};
use anyhow::{Context, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

mod database;
pub(crate) use database::{FIELD as DATABASE_FIELD, root as database_root};

pub(crate) const FIELD: &str = "durable-db-upload-budget";
const MAX_UNITS: u64 = (1 << 53) - 1;
const GRANT_BYTES: u64 = 8 * 1024 * 1024;
const GRANT_ATTEMPTS: u64 = 256;
const RECEIPTS: usize = 8;

#[derive(Debug, thiserror::Error)]
#[error("immutable upload budget exhausted")]
pub(crate) struct Exhausted {
    pub(crate) database: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadLimits {
    pub bytes: u64,
    pub attempts: u64,
}
impl UploadLimits {
    pub fn validate(self) -> anyhow::Result<()> {
        ensure!(
            self.bytes <= MAX_UNITS && self.attempts <= MAX_UNITS,
            "upload limits exceed the exact JSON integer range"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadStatus {
    pub configuration_revision: u64,
    pub limits: UploadLimits,
    pub allocated: UploadLimits,
    /// Tenant-only reserve. Admission rights are volatile; these unissued
    /// maintenance bytes remain durable until an attempt claims them by CAS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<MaintenanceBudget>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceBudget {
    pub funded_bytes: u64,
    pub allocated: UploadLimits,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadRequest {
    pub request_id: String,
    pub expected_configuration_revision: u64,
    pub limits: UploadLimits,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadResult {
    pub status: UploadStatus,
    pub replayed: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    id: String,
    digest: String,
    revision: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    status: UploadStatus,
    receipts: Vec<Receipt>,
}
impl Record {
    fn empty() -> Self {
        Self {
            status: UploadStatus {
                configuration_revision: 0,
                limits: UploadLimits::default(),
                allocated: UploadLimits::default(),
                maintenance: None,
            },
            receipts: vec![],
        }
    }
    fn validate(&self) -> anyhow::Result<()> {
        self.status.limits.validate()?;
        ensure!(
            self.status.allocated.bytes <= self.status.limits.bytes
                && self.status.allocated.attempts <= self.status.limits.attempts
                && self.status.configuration_revision <= MAX_UNITS
                && self.receipts.len() <= RECEIPTS,
            "invalid upload budget counters"
        );
        if let Some(maintenance) = &self.status.maintenance {
            ensure!(
                maintenance.allocated.attempts <= maintenance.allocated.bytes
                    && maintenance.allocated.bytes <= maintenance.funded_bytes
                    && maintenance.funded_bytes <= self.status.allocated.bytes,
                "invalid maintenance reserve"
            );
        }
        let mut previous = 0;
        let mut ids = std::collections::BTreeSet::new();
        for receipt in &self.receipts {
            ensure!(
                valid_id(&receipt.id)
                    && receipt.digest.len() == 64
                    && receipt.digest.bytes().all(|b| b.is_ascii_hexdigit())
                    && receipt.revision > previous
                    && receipt.revision <= self.status.configuration_revision
                    && ids.insert(&receipt.id),
                "invalid upload budget receipt"
            );
            previous = receipt.revision;
        }
        Ok(())
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}
fn record(bucket: &Bucket) -> anyhow::Result<Record> {
    record_info(&bucket.info)
}
fn record_info(info: &std::collections::BTreeMap<String, String>) -> anyhow::Result<Record> {
    ensure!(
        matches!(
            info.get("durable-db-format").map(String::as_str),
            Some("4" | "5" | "6" | crate::ownership::root::VERSION)
        ),
        "unsupported upload budget format"
    );
    let record: Record = serde_json::from_str(
        info.get(FIELD)
            .context("missing authoritative upload budget")?,
    )?;
    record.validate()?;
    ensure!(
        record.status.maintenance.is_none(),
        "tenant reserve in namespace budget"
    );
    Ok(record)
}
pub(crate) fn initial(limits: UploadLimits) -> anyhow::Result<String> {
    limits.validate()?;
    Ok(serde_json::to_string(&Record {
        status: UploadStatus {
            configuration_revision: 0,
            limits,
            allocated: UploadLimits::default(),
            maintenance: None,
        },
        receipts: vec![],
    })?)
}
pub(crate) fn validate(bucket: &Bucket) -> anyhow::Result<()> {
    record(bucket).map(|_| ())
}
pub(crate) fn validate_info(
    info: &std::collections::BTreeMap<String, String>,
) -> anyhow::Result<()> {
    record_info(info).map(|_| ())
}

pub(crate) async fn initialize_native(
    authority: &Arc<dyn Authority>,
    prefix: &str,
    limits: UploadLimits,
) -> anyhow::Result<()> {
    limits.validate()?;
    ensure!(
        !prefix.is_empty()
            && prefix.len() <= 128
            && prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
        "invalid native namespace"
    );
    for _ in 0..8 {
        let before = authority.read().await?;
        if before.info.contains_key("durable-db-format") {
            ensure!(
                before.info.get("durable-db-format").map(String::as_str)
                    == Some(crate::ownership::root::VERSION)
                    && before.info.get("durable-db-prefix").map(String::as_str) == Some(prefix),
                "native upload budget requires a fresh namespace or its existing ledger"
            );
            validate(&before)?;
            crate::ownership::root::metadata_budget(&before.info)?;
            return Ok(());
        }
        ensure!(
            !before.info.keys().any(|k| k.starts_with("durable-db-")
                && !["durable-db-probe", "durable-db-read-fence"].contains(&k.as_str())),
            "unrecognized native namespace metadata"
        );
        let mut info = before.info.clone();
        info.insert(
            "durable-db-format".into(),
            crate::ownership::root::VERSION.into(),
        );
        info.insert("durable-db-prefix".into(), prefix.into());
        info.insert(FIELD.into(), initial(limits)?);
        crate::ownership::root::metadata_budget(&info)?;
        if authority.replace(&before, info).await?.is_some() {
            return Ok(());
        }
    }
    bail!("native upload budget initialization contention budget exceeded")
}

pub(crate) struct Allocator {
    authority: Arc<dyn Authority>,
    database: Option<database::Ledger>,
    // Volatile rights only. Never save these to disk or reconstruct them from
    // the authority's cumulative allocation on restart or response loss.
    remaining: Mutex<UploadLimits>,
}
impl Allocator {
    pub(crate) fn new(authority: Arc<dyn Authority>) -> Self {
        Self {
            authority,
            database: None,
            remaining: Mutex::new(UploadLimits::default()),
        }
    }
    pub(crate) async fn status(&self) -> anyhow::Result<UploadStatus> {
        Ok(self
            .record(&self.authority.read_fresh().await?)
            .await?
            .status)
    }
    pub(crate) async fn charge(&self, bytes: u64) -> anyhow::Result<()> {
        let mut remaining = self.remaining.lock().await;
        self.allocate(&mut remaining, UploadLimits { bytes, attempts: 1 })
            .await?;
        // No await between confirmed allocation and spending this charge.
        remaining.bytes -= bytes;
        remaining.attempts -= 1;
        self.charged(bytes);
        Ok(())
    }
    /// Fund upcoming uploads before capturing a metadata comparison. This does
    /// not spend or reserve local rights for a caller; every PUT still charges.
    /// True means funding changed the authority; discard earlier observations.
    pub(crate) async fn prefund(&self, minimum: UploadLimits) -> anyhow::Result<bool> {
        self.allocate(&mut *self.remaining.lock().await, minimum)
            .await
    }
    async fn allocate(
        &self,
        remaining: &mut UploadLimits,
        minimum: UploadLimits,
    ) -> anyhow::Result<bool> {
        if remaining.bytes >= minimum.bytes && remaining.attempts >= minimum.attempts {
            return Ok(false);
        }
        // Replenish only the deficient dimensions. Existing rights remain
        // confirmed and usable even if the additional grant response is lost.
        let needed = UploadLimits {
            bytes: minimum.bytes.saturating_sub(remaining.bytes),
            attempts: minimum.attempts.saturating_sub(remaining.attempts),
        };
        for attempt in 0..8 {
            let before = self.authority.read().await?;
            let mut record = self.record(&before).await?;
            let available = UploadLimits {
                // Tenant grants prepay both admission and a separate durable
                // maintenance reserve. Namespace physical grants are unchanged.
                bytes: (record.status.limits.bytes - record.status.allocated.bytes)
                    / if self.database.is_some() { 2 } else { 1 },
                attempts: record.status.limits.attempts - record.status.allocated.attempts,
            };
            if available.bytes < needed.bytes || available.attempts < needed.attempts {
                if self.database.is_none() {
                    REGISTRY.event(Event::UploadBudgetExhausted, 1);
                }
                return Err(Exhausted {
                    database: self.database.is_some(),
                }
                .into());
            }
            let grant = UploadLimits {
                bytes: if needed.bytes > 0 {
                    GRANT_BYTES.max(needed.bytes).min(available.bytes)
                } else {
                    0
                },
                attempts: if needed.attempts > 0 {
                    GRANT_ATTEMPTS.max(needed.attempts).min(available.attempts)
                } else {
                    0
                },
            };
            record.status.allocated.bytes += grant.bytes;
            if let Some(maintenance) = &mut record.status.maintenance {
                maintenance.funded_bytes += grant.bytes;
                record.status.allocated.bytes += grant.bytes;
            }
            record.status.allocated.attempts += grant.attempts;
            if self.publish(&before, &record).await? {
                // An unknown CAS result exits above without adding any
                // unconfirmed local rights. Unused rights remain volatile.
                *remaining = UploadLimits {
                    bytes: remaining.bytes + grant.bytes,
                    attempts: remaining.attempts + grant.attempts,
                };
                if self.database.is_none() {
                    REGISTRY.event(Event::UploadGrantBytes, grant.bytes as usize);
                    REGISTRY.event(Event::UploadGrantAttempts, grant.attempts as usize);
                }
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
        }
        bail!("upload allocation contention budget exceeded")
    }
    pub(crate) async fn replenish(&self, request: &UploadRequest) -> Result<UploadResult> {
        if !valid_id(&request.request_id) {
            return Err(Error::Invalid(
                "upload request ID must be 1..64 ASCII letters, digits, - or _".into(),
            ));
        }
        request
            .limits
            .validate()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(request).map_err(anyhow::Error::from)?,
        ));
        for _ in 0..8 {
            let before = self.authority.read_fresh().await?;
            let mut record = self.record(&before).await?;
            if let Some(receipt) = record.receipts.iter().find(|r| r.id == request.request_id) {
                if receipt.digest != digest {
                    return Err(Error::Invalid(
                        "upload request ID was used for a different change".into(),
                    ));
                }
                return Ok(UploadResult {
                    status: record.status,
                    replayed: true,
                });
            }
            if record.status.configuration_revision != request.expected_configuration_revision {
                return Err(Error::Conflict);
            }
            if request.limits.bytes < record.status.limits.bytes
                || request.limits.attempts < record.status.limits.attempts
                || request.limits == record.status.limits
            {
                return Err(Error::Invalid(
                    "upload ceilings can only increase; cumulative allocation never resets".into(),
                ));
            }
            if record.status.configuration_revision == MAX_UNITS {
                return Err(Error::Invalid(
                    "upload configuration revision exhausted".into(),
                ));
            }
            record.status.configuration_revision += 1;
            record.status.limits = request.limits;
            record.receipts.push(Receipt {
                id: request.request_id.clone(),
                digest: digest.clone(),
                revision: record.status.configuration_revision,
            });
            if record.receipts.len() > RECEIPTS {
                record.receipts.remove(0);
            }
            if self.publish(&before, &record).await? {
                return Ok(UploadResult {
                    status: record.status,
                    replayed: false,
                });
            }
        }
        Err(Error::Busy)
    }
    async fn record(&self, before: &Bucket) -> anyhow::Result<Record> {
        match &self.database {
            Some(ledger) => Ok(ledger.read(before).await?.unwrap_or_else(|| {
                let mut record = Record::empty();
                record.status.maintenance = Some(MaintenanceBudget::default());
                record
            })),
            None => record(before),
        }
    }
    async fn publish(&self, before: &Bucket, record: &Record) -> anyhow::Result<bool> {
        if let Some(ledger) = &self.database {
            return ledger.publish(&*self.authority, before, record).await;
        }
        let mut info = before.info.clone();
        info.insert(FIELD.into(), serde_json::to_string(record)?);
        crate::publication::budget(&info)?;
        Ok(self.authority.replace(before, info).await?.is_some())
    }
    fn charged(&self, bytes: u64) {
        if self.database.is_none() {
            REGISTRY.event(Event::UploadChargedBytes, bytes as usize);
            REGISTRY.event(Event::UploadChargedAttempts, 1);
        }
    }
}

pub(crate) struct Files {
    pub(crate) inner: Arc<dyn Storage>,
    pub(crate) allocator: Arc<Allocator>,
}

/// A claimed checkpoint attempt cannot spend beyond its conservative allowance.
/// The underlying namespace allocator still charges every physical PUT.
pub(crate) struct Attempt {
    inner: Arc<dyn Storage>,
    remaining: std::sync::atomic::AtomicU64,
}
impl Attempt {
    pub(crate) fn bounded(inner: Arc<dyn Storage>, bytes: u64) -> Arc<dyn Storage> {
        Arc::new(Self {
            inner,
            remaining: std::sync::atomic::AtomicU64::new(bytes),
        })
    }
}
#[async_trait]
impl Storage for Attempt {
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;
        ensure!(!bytes.is_empty(), "empty checkpoint upload");
        self.remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(bytes.len() as u64)
            })
            .map_err(|_| anyhow::anyhow!("checkpoint exceeded its funded allowance"))?;
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        self.inner.get(key, max_bytes).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        bail!("checkpoint attempt only permits immutable writes")
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        bail!("checkpoint attempt cannot delete objects")
    }
}
#[async_trait]
impl Storage for Files {
    async fn upload_budget(&self) -> anyhow::Result<Option<UploadStatus>> {
        Ok(Some(self.allocator.status().await?))
    }
    async fn replenish_upload_budget(&self, request: &UploadRequest) -> Result<UploadResult> {
        self.allocator.replenish(request).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        self.inner.get_range(key, range, object_size).await
    }
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.inner.get(key, max).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.allocator
            .charge(bytes.len() as u64)
            .await
            .map_err(|e| e.context(NotSent))?;
        // All cloud bindings perform at most one PUT attempt here. A caller
        // retry invokes this boundary again, spending a fresh charge.
        self.inner.put(key, bytes).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        bail!("conditional replacement is not an immutable upload")
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
}
