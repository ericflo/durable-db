//! Upload-free collection of shared native directories and provider history.
//! Uses PublicationSafety's list-before-fence protocol and Directory membership.
use super::*;
use crate::directory::{Directory, witness};

pub(crate) const FIELD: &str = "durable-db-tenant-gc";
const PAGE: usize = 128;
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    manifests: Option<String>,
    indexes: Option<String>,
}
impl Cursor {
    fn read(before: &Bucket) -> anyhow::Result<Self> {
        let cursor: Self = before
            .info
            .get(FIELD)
            .map(|s| serde_json::from_str(s))
            .transpose()?
            .unwrap_or_default();
        for (key, prefix) in [
            (&cursor.manifests, "manifests/"),
            (&cursor.indexes, "indexes/"),
        ] {
            if let Some(key) = key {
                witness(key, prefix)?;
            }
        }
        Ok(cursor)
    }
}
#[derive(Clone)]
pub(crate) struct Collection {
    authority: Arc<dyn Authority>,
    files: Arc<dyn Storage>,
    prefix: String,
    profile: crate::ownership::root::Profile,
}
impl Collection {
    pub(crate) fn new(
        authority: Arc<dyn Authority>,
        files: Arc<dyn Storage>,
        prefix: String,
        profile: crate::ownership::root::Profile,
    ) -> Self {
        Self {
            authority,
            files,
            prefix,
            profile,
        }
    }
    pub(crate) async fn collect(&self) -> anyhow::Result<usize> {
        let before = self.authority.read().await?;
        database::root(&before)?;
        // The S3 register also retains immutable historical versions. Reuse
        // its list-before-barrier cleanup even before any tenant exists; B2
        // has no corresponding object history. This never spends immutable PUT credit.
        self.authority.maintenance().await?;
        let cursor = Cursor::read(&before)?;
        let manifests = self
            .files
            .list("manifests/", cursor.manifests.as_deref(), PAGE)
            .await?;
        let indexes = self
            .files
            .list("indexes/", cursor.indexes.as_deref(), PAGE)
            .await?;
        ensure!(
            manifests.len() <= PAGE && indexes.len() <= PAGE,
            "collection listing exceeds its bound"
        );
        if manifests.is_empty()
            && indexes.is_empty()
            && cursor.manifests.is_none()
            && cursor.indexes.is_none()
        {
            return Ok(0);
        }
        let observed = self.authority.read().await?;
        let protected = [
            database::root(&observed)?,
            crate::ownership::root::protected_catalog(
                &self.files,
                &observed.info,
                &self.prefix,
                self.profile,
            )
            .await?,
        ];
        let cursor = Cursor {
            manifests: (manifests.len() == PAGE).then(|| manifests.last().unwrap().clone()),
            indexes: (indexes.len() == PAGE).then(|| indexes.last().unwrap().clone()),
        };
        let mut info = observed.info.clone();
        info.insert(FIELD.into(), serde_json::to_string(&cursor)?);
        crate::publication::budget(&info)?;
        // The common register changes the nonce even if cursors did not change.
        // A losing or uncertain fence grants no deletion permission. Late
        // publishers must rebuild from a fresh comparison, never adopt this one.
        if self.authority.replace(&observed, info).await?.is_none() {
            return Ok(0);
        }
        Directory::new(self.files.clone())
            .collect_unreachable(&protected, &manifests, &indexes, &*self.authority)
            .await
    }
}
