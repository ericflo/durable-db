//! Name-bound ledgers share the existing directory and nonrefundable allocator.
use super::*;
use crate::cache::Cache;
use crate::directory::{Directory, Pointer, Root};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) const FIELD: &str = "durable-db-tenant-budgets";
pub(crate) const DEFAULT_LIMITS: UploadLimits = UploadLimits {
    bytes: 64 * 1024 * 1024,
    attempts: 16_384,
};

pub(super) struct Ledger {
    files: Arc<dyn Storage>,
    cache: Option<Arc<Cache>>,
    name: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    database: String,
    record: Record,
}
pub(crate) fn root(before: &Bucket) -> anyhow::Result<Option<Root>> {
    ensure!(
        before.info.get("durable-db-format").map(String::as_str)
            == Some(crate::ownership::root::VERSION),
        "unsupported database budget format"
    );
    super::validate(before)?;
    let root: Option<Root> = before
        .info
        .get(FIELD)
        .map(|s| serde_json::from_str(s))
        .transpose()?;
    if let Some(root) = &root {
        root.validate()?;
    }
    Ok(root)
}
impl Ledger {
    fn directory(&self) -> Directory {
        let directory = Directory::new(self.files.clone());
        match &self.cache {
            Some(cache) => directory.with_cache(cache.clone()),
            None => directory,
        }
    }
    pub(super) async fn read(&self, before: &Bucket) -> anyhow::Result<Option<Record>> {
        let Some(pointer) = self
            .directory()
            .get(root(before)?.as_ref(), &self.name)
            .await?
        else {
            return Ok(None);
        };
        let cached = match &self.cache {
            Some(cache) => cache.get(&pointer.key, &pointer.sha256).await,
            None => None,
        };
        let miss = cached.is_none();
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                self.files
                    .get(&pointer.key, 4096)
                    .await?
                    .context("missing database upload ledger")?
                    .bytes
            }
        };
        ensure!(
            bytes.len() <= 4096 && hex::encode(Sha256::digest(&bytes)) == pointer.sha256,
            "database upload ledger checksum mismatch"
        );
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        ensure!(
            envelope.database == self.name,
            "database upload ledger scope mismatch"
        );
        envelope.record.validate()?;
        ensure!(
            envelope.record.status.maintenance.is_some(),
            "missing maintenance reserve"
        );
        if miss && let Some(cache) = &self.cache {
            cache.insert(&pointer.key, bytes).await;
        }
        Ok(Some(envelope.record))
    }
    pub(super) async fn publish(
        &self,
        authority: &dyn Authority,
        before: &Bucket,
        record: &Record,
    ) -> anyhow::Result<bool> {
        self.publish_many(authority, before, &[(&self.name, record)], |_| Ok(()))
            .await
    }
    async fn publish_many(
        &self,
        authority: &dyn Authority,
        before: &Bucket,
        records: &[(&str, &Record)],
        check: impl Fn(&Bucket) -> anyhow::Result<()>,
    ) -> anyhow::Result<bool> {
        let directory = self.write_many(before, records).await?;
        let mut comparison = before.clone();
        for _ in 0..8 {
            let mut info = comparison.info.clone();
            info.insert(FIELD.into(), directory.clone());
            crate::publication::budget(&info)?;
            match authority.replace(&comparison, info).await {
                Ok(Some(_)) => return Ok(true),
                Ok(None) => (),
                Err(error) => {
                    // Every staged ledger has a fresh identity. Only this live
                    // invocation can confirm its exact directory once; counters
                    // or another root cannot reconstruct a grant. No PUT retry.
                    if let Ok(observed) = authority.read().await
                        && root(&observed).is_ok()
                        && check(&observed).is_ok()
                        && observed.info.get(FIELD) == Some(&directory)
                    {
                        tracing::info!(
                            "tenant ledger publication confirmed by exact root readback"
                        );
                        return Ok(true);
                    }
                    return Err(error);
                }
            }
            comparison = authority.read().await?;
            check(&comparison)?;
            root(&comparison)?; // Refuse a changed format or malformed budget.
            if comparison.info.get(FIELD) != before.info.get(FIELD)
                || !crate::ownership::root::Collection::same_metadata_fence(before, &comparison)
            {
                return Ok(false);
            }
        }
        bail!("ledger publication contention budget exceeded")
    }
    async fn write_many(
        &self,
        before: &Bucket,
        records: &[(&str, &Record)],
    ) -> anyhow::Result<String> {
        // Namespace grants can change the authority during these uploads.
        // Publication rebases only across unchanged ledgers and collection fences.
        let directory = self.directory();
        let mut next = root(before)?;
        for batch in records.chunks(crate::directory::MAX_UPDATES) {
            let mut pointers = BTreeMap::new();
            for (name, record) in batch {
                let key = format!("manifests/{}.{}", name, Uuid::new_v4());
                let bytes = Bytes::from(serde_json::to_vec(
                    &serde_json::json!({"database": name, "record": record}),
                )?);
                ensure!(bytes.len() <= 4096, "database upload ledger too large");
                let pointer = Pointer {
                    key: key.clone(),
                    sha256: hex::encode(Sha256::digest(&bytes)),
                };
                self.files.put(&key, bytes.clone()).await?;
                if let Some(cache) = &self.cache {
                    cache.insert(&key, bytes).await;
                }
                pointers.insert((*name).to_owned(), pointer);
            }
            next = Some(directory.insert_many(next.as_ref(), &pointers).await?);
        }
        Ok(serde_json::to_string(
            &next.context("empty ledger update")?,
        )?)
    }
}
impl Allocator {
    pub(crate) fn database(
        authority: Arc<dyn Authority>,
        files: Arc<dyn Storage>,
        cache: Option<Arc<Cache>>,
        name: &str,
    ) -> anyhow::Result<Self> {
        crate::engine::validate_db(name)?;
        Ok(Self {
            database: Some(Ledger {
                files,
                cache,
                name: name.into(),
            }),
            ..Self::new(authority)
        })
    }
    pub(crate) async fn initialize_database(&self) -> anyhow::Result<()> {
        let ledger = self.database.as_ref().context("expected database ledger")?;
        for _ in 0..8 {
            let before = self.authority.read().await?;
            if ledger.read(&before).await?.is_some() {
                return Ok(());
            }
            let mut record = Record::empty();
            record.status.limits = DEFAULT_LIMITS;
            record.status.maintenance = Some(MaintenanceBudget::default());
            if self.publish(&before, &record).await? {
                return Ok(());
            }
        }
        bail!("database upload budget initialization contention budget exceeded")
    }

    /// Atomically claim every tenant's allowance before any checkpoint PUT.
    /// An unresolved reply strands the attempt. Only the originating invocation
    /// can confirm its fresh directory; reopening never recreates rights.
    pub(crate) async fn fund_maintenance(
        charges: &[(Arc<Self>, u64)],
        check: impl Fn(&Bucket) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let Some((first, _)) = charges.first() else {
            return Ok(());
        };
        let mut remaining = Vec::with_capacity(charges.len());
        let mut previous: Option<&str> = None;
        for (allocator, _) in charges {
            let name = &allocator
                .database
                .as_ref()
                .context("expected tenant reserve")?
                .name;
            ensure!(
                Arc::ptr_eq(&first.authority, &allocator.authority)
                    && previous.is_none_or(|prior| prior < name.as_str()),
                "maintenance charges must be unique, ordered and in one namespace"
            );
            previous = Some(name);
            remaining.push(allocator.remaining.lock().await);
        }
        for _ in 0..8 {
            let before = first.authority.read().await?;
            check(&before)?;
            let mut records = Vec::with_capacity(charges.len());
            for (allocator, bytes) in charges {
                ensure!(*bytes > 0, "empty maintenance charge");
                let ledger = allocator
                    .database
                    .as_ref()
                    .context("expected tenant reserve")?;
                let mut record = ledger
                    .read(&before)
                    .await?
                    .context("missing tenant reserve")?;
                let reserve = record.status.maintenance.as_mut().unwrap();
                let extra = bytes.saturating_sub(reserve.funded_bytes - reserve.allocated.bytes);
                if extra > record.status.limits.bytes - record.status.allocated.bytes {
                    return Err(Exhausted { database: true }.into());
                }
                record.status.allocated.bytes += extra;
                reserve.funded_bytes += extra;
                reserve.allocated.bytes += bytes;
                reserve.allocated.attempts += 1;
                record.validate()?;
                records.push((ledger.name.as_str(), record));
            }
            // Retry work may consume reserve backing unused admission rights.
            // Forfeit those local rights before a possibly applied/lost CAS.
            for (remaining, (_, record)) in remaining.iter_mut().zip(&records) {
                let reserve = record.status.maintenance.as_ref().unwrap();
                remaining.bytes = remaining
                    .bytes
                    .min(reserve.funded_bytes - reserve.allocated.bytes);
            }
            let records: Vec<_> = records
                .iter()
                .map(|(name, record)| (*name, record))
                .collect();
            if first
                .database
                .as_ref()
                .unwrap()
                .publish_many(&*first.authority, &before, &records, &check)
                .await?
            {
                return Ok(());
            }
        }
        bail!("maintenance funding contention budget exceeded")
    }
}
