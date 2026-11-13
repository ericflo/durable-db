//! Positive, write-once permission. Absence always refuses; deletion is final.
use crate::{publication::Authority, storage::Storage};
use anyhow::ensure;
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"DDBPER01";
pub(super) const SIZE: usize = 24;
pub(super) fn key(id: Uuid) -> String {
    format!("permits/{}", id.simple())
}
fn bytes(id: Uuid) -> Bytes {
    let mut bytes = Vec::with_capacity(SIZE);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(id.as_bytes());
    bytes.into()
}
pub(crate) fn validate_key(value: &str) -> anyhow::Result<()> {
    let id = Uuid::parse_str(value.strip_prefix("permits/").unwrap_or_default())?;
    ensure!(!id.is_nil() && key(id) == value, "invalid permit key");
    Ok(())
}

// Consumed exactly once, before conditional publication of the incarnation.
// An unknown PUT can be resolved positively; it can never be retried.
pub(super) struct Prepared {
    files: Arc<dyn Storage>,
    id: Uuid,
}
impl Prepared {
    pub fn new(files: Arc<dyn Storage>, id: Uuid) -> Self {
        Self { files, id }
    }
    pub async fn execute(self) -> anyhow::Result<()> {
        if let Err(error) = self.files.put(&key(self.id), bytes(self.id)).await
            && !matches!(live(&self.files, self.id).await, Ok(true))
        {
            return Err(error.context("permit creation unresolved; claim not attempted"));
        }
        Ok(())
    }
}
pub(super) async fn live(files: &Arc<dyn Storage>, id: Uuid) -> anyhow::Result<bool> {
    let Some(object) = files.get(&key(id), SIZE).await? else {
        return Ok(false);
    };
    ensure!(object.bytes == bytes(id), "invalid permit contents");
    Ok(true)
}
pub(super) async fn revoke(
    files: &Arc<dyn Storage>,
    authority: &Arc<dyn Authority>,
    id: Uuid,
) -> anyhow::Result<()> {
    authority.delete_versions(&key(id)).await?;
    ensure!(!live(files, id).await?, "permit revocation not confirmed");
    Ok(())
}
