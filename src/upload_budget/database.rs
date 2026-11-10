//! Name-bound ledgers share the existing directory and nonrefundable allocator.
use super::*;
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
    pub(super) async fn read(&self, before: &Bucket) -> anyhow::Result<Option<Record>> {
        let Some(pointer) = Directory::new(self.files.clone())
            .get(root(before)?.as_ref(), &self.name)
            .await?
        else {
            return Ok(None);
        };
        let object = self
            .files
            .get(&pointer.key, 4096)
            .await?
            .context("missing database upload ledger")?;
        ensure!(
            hex::encode(Sha256::digest(&object.bytes)) == pointer.sha256,
            "database upload ledger checksum mismatch"
        );
        let envelope: Envelope = serde_json::from_slice(&object.bytes)?;
        ensure!(
            envelope.database == self.name,
            "database upload ledger scope mismatch"
        );
        envelope.record.validate()?;
        Ok(Some(envelope.record))
    }
    pub(super) async fn write(&self, before: &Bucket, record: &Record) -> anyhow::Result<String> {
        // These metadata uploads pass through the namespace allocator. Its CAS
        // can invalidate `before`; a retry rebuilds from the new directory root.
        let key = format!("manifests/{}.{}", self.name, Uuid::new_v4());
        let bytes = Bytes::from(serde_json::to_vec(
            &serde_json::json!({"database": self.name, "record": record}),
        )?);
        ensure!(bytes.len() <= 4096, "database upload ledger too large");
        let pointer = Pointer {
            key: key.clone(),
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        self.files.put(&key, bytes).await?;
        let root = Directory::new(self.files.clone())
            .insert_many(
                root(before)?.as_ref(),
                &BTreeMap::from([(self.name.clone(), pointer)]),
            )
            .await?;
        Ok(serde_json::to_string(&root)?)
    }
}
impl Allocator {
    pub(crate) fn database(
        authority: Arc<dyn Authority>,
        files: Arc<dyn Storage>,
        name: &str,
    ) -> anyhow::Result<Self> {
        crate::engine::validate_db(name)?;
        Ok(Self {
            database: Some(Ledger {
                files,
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
            if self.publish(&before, &record).await? {
                return Ok(());
            }
        }
        bail!("database upload budget initialization contention budget exceeded")
    }
}
