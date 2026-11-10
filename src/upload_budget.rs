//! Nonrefundable, authority-backed admission for immutable upload attempts.
use crate::{
    Error, Result,
    metrics::{Event, REGISTRY},
    publication::{Authority, Bucket},
    storage::{Object, Storage, Version},
};
use anyhow::{Context, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) const FIELD: &str = "durable-db-upload-budget";
const MAX_UNITS: u64 = (1 << 53) - 1;
const GRANT_BYTES: u64 = 8 * 1024 * 1024;
const GRANT_ATTEMPTS: u64 = 32;
const RECEIPTS: usize = 8;

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
    fn validate(&self) -> anyhow::Result<()> {
        self.status.limits.validate()?;
        ensure!(
            self.status.allocated.bytes <= self.status.limits.bytes
                && self.status.allocated.attempts <= self.status.limits.attempts
                && self.status.configuration_revision <= MAX_UNITS
                && self.receipts.len() <= RECEIPTS,
            "invalid upload budget counters"
        );
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
    ensure!(
        matches!(
            bucket.info.get("durable-db-format").map(String::as_str),
            Some("4" | "5")
        ),
        "upload budget requires publication format 4 or 5"
    );
    let record: Record = serde_json::from_str(
        bucket
            .info
            .get(FIELD)
            .context("missing authoritative upload budget")?,
    )?;
    record.validate()?;
    Ok(record)
}
pub(crate) fn initial(limits: UploadLimits) -> anyhow::Result<String> {
    limits.validate()?;
    Ok(serde_json::to_string(&Record {
        status: UploadStatus {
            configuration_revision: 0,
            limits,
            allocated: UploadLimits::default(),
        },
        receipts: vec![],
    })?)
}
pub(crate) fn validate(bucket: &Bucket) -> anyhow::Result<()> {
    record(bucket).map(|_| ())
}

pub(crate) struct Allocator {
    authority: Arc<dyn Authority>,
    // Volatile rights only. Never save these to disk or reconstruct them from
    // the authority's cumulative allocation on restart or response loss.
    remaining: Mutex<UploadLimits>,
}
impl Allocator {
    pub(crate) fn new(authority: Arc<dyn Authority>) -> Self {
        Self {
            authority,
            remaining: Mutex::new(UploadLimits::default()),
        }
    }
    pub(crate) async fn status(&self) -> anyhow::Result<UploadStatus> {
        Ok(record(&self.authority.read_fresh().await?)?.status)
    }
    async fn charge(&self, bytes: u64) -> anyhow::Result<()> {
        let mut remaining = self.remaining.lock().await;
        if remaining.bytes >= bytes && remaining.attempts > 0 {
            remaining.bytes -= bytes;
            remaining.attempts -= 1;
            charged(bytes);
            return Ok(());
        }
        // Replenish only the deficient dimensions. Existing rights remain
        // confirmed and usable even if the additional grant response is lost.
        let needed = UploadLimits {
            bytes: bytes.saturating_sub(remaining.bytes),
            attempts: u64::from(remaining.attempts == 0),
        };
        for attempt in 0..8 {
            let before = self.authority.read().await?;
            let mut record = record(&before)?;
            let available = UploadLimits {
                bytes: record.status.limits.bytes - record.status.allocated.bytes,
                attempts: record.status.limits.attempts - record.status.allocated.attempts,
            };
            if available.bytes < needed.bytes || available.attempts < needed.attempts {
                REGISTRY.event(Event::UploadBudgetExhausted, 1);
                bail!("immutable upload budget exhausted");
            }
            let grant = UploadLimits {
                bytes: if needed.bytes > 0 {
                    GRANT_BYTES.max(needed.bytes).min(available.bytes)
                } else {
                    0
                },
                attempts: if needed.attempts > 0 {
                    GRANT_ATTEMPTS.min(available.attempts)
                } else {
                    0
                },
            };
            record.status.allocated.bytes += grant.bytes;
            record.status.allocated.attempts += grant.attempts;
            let mut info = before.info.clone();
            info.insert(FIELD.into(), serde_json::to_string(&record)?);
            crate::publication::budget(&info)?;
            if self.authority.replace(&before, info).await?.is_some() {
                // No await/cancellation point between confirmed allocation
                // and spending this request's charge. An unknown CAS result
                // exits above without adding any unconfirmed local rights.
                *remaining = UploadLimits {
                    bytes: remaining.bytes + grant.bytes - bytes,
                    attempts: remaining.attempts + grant.attempts - 1,
                };
                REGISTRY.event(Event::UploadGrantBytes, grant.bytes as usize);
                REGISTRY.event(Event::UploadGrantAttempts, grant.attempts as usize);
                charged(bytes);
                return Ok(());
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
            let mut record = record(&before)?;
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
            let mut info = before.info.clone();
            info.insert(
                FIELD.into(),
                serde_json::to_string(&record).map_err(anyhow::Error::from)?,
            );
            crate::publication::budget(&info)?;
            if self.authority.replace(&before, info).await?.is_some() {
                return Ok(UploadResult {
                    status: record.status,
                    replayed: false,
                });
            }
        }
        Err(Error::Busy)
    }
}
fn charged(bytes: u64) {
    REGISTRY.event(Event::UploadChargedBytes, bytes as usize);
    REGISTRY.event(Event::UploadChargedAttempts, 1);
}

pub(crate) struct Files {
    pub(crate) inner: Arc<dyn Storage>,
    pub(crate) allocator: Arc<Allocator>,
}
#[async_trait]
impl Storage for Files {
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
        self.allocator.charge(bytes.len() as u64).await?;
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
