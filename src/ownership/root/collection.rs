//! Upload-free collection of native metadata, covered journal frames and provider history.
//! Uses PublicationSafety's list-before-fence protocol and Directory membership.
use super::{Profile, Registration, catalog, metadata};
use crate::{
    directory::{Directory, witness},
    index::recovery::checkpoint::Header,
    journal::LogId,
    metrics::{Event, REGISTRY},
    publication::{Authority, Bucket},
    storage::Storage,
    upload_budget::database_root,
};
use anyhow::ensure;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const FIELD: &str = "durable-db-gc";
const PAGE: usize = 128;
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    manifests: Option<String>,
    indexes: Option<String>,
    journal: Option<String>,
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
        if let Some(key) = &cursor.journal {
            LogId::parse_key(key)?;
        }
        Ok(cursor)
    }
}
#[derive(Clone)]
pub(crate) struct Collection {
    authority: Arc<dyn Authority>,
    files: Arc<dyn Storage>,
    prefix: String,
    profile: Profile,
}
impl Collection {
    pub(crate) fn new(
        authority: Arc<dyn Authority>,
        files: Arc<dyn Storage>,
        prefix: String,
        profile: Profile,
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
        database_root(&before)?;
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
        let journal = self
            .files
            .list("journal/", cursor.journal.as_deref(), PAGE)
            .await?;
        ensure!(
            manifests.len() <= PAGE && indexes.len() <= PAGE && journal.len() <= PAGE,
            "collection listing exceeds its bound"
        );
        if manifests.is_empty()
            && indexes.is_empty()
            && journal.is_empty()
            && cursor.manifests.is_none()
            && cursor.indexes.is_none()
            && cursor.journal.is_none()
        {
            return Ok(0);
        }
        let observed = self.authority.read().await?;
        let record = metadata::decode(&observed.info, &self.prefix, self.profile, true)?;
        let protected = [
            database_root(&observed)?,
            catalog::load(&self.files, record.and_then(|r| r.catalog)).await?,
        ];
        let cut = if !journal.is_empty() {
            if let Some(record) = record.filter(|r| r.checkpoint.is_some()) {
                let cut = Header::load(self.files.clone(), record.checkpoint.unwrap())
                    .await?
                    .cut;
                ensure!(
                    cut.identity.epoch <= record.identity.epoch
                        && (cut.identity.epoch != record.identity.epoch
                            || cut.identity == record.identity),
                    "checkpoint cut exceeds current ownership"
                );
                Some(cut)
            } else {
                None
            }
        } else {
            None
        };
        let cursor = Cursor {
            manifests: (manifests.len() == PAGE).then(|| manifests.last().unwrap().clone()),
            indexes: (indexes.len() == PAGE).then(|| indexes.last().unwrap().clone()),
            journal: (journal.len() == PAGE).then(|| journal.last().unwrap().clone()),
        };
        let mut info = observed.info.clone();
        info.insert(FIELD.into(), serde_json::to_string(&cursor)?);
        crate::publication::budget(&info)?;
        // The common register changes the nonce even if cursors did not change.
        // A losing or uncertain fence grants no deletion permission. Late
        // directory publishers must rebuild from a fresh comparison, never adopt this one.
        if self.authority.replace(&observed, info).await?.is_none() {
            return Ok(0);
        }
        let mut deleted = Directory::new(self.files.clone())
            .collect_unreachable(&protected, &manifests, &indexes, &*self.authority)
            .await?;
        if let Some(cut) = cut {
            // Keys group frames by incarnation. Retain one validated registration,
            // not one provider GET per frame or an unbounded epoch cache.
            let mut registration: Option<Registration> = None;
            for key in journal {
                let (log, position) = LogId::parse_key(&key)?;
                REGISTRY.event(Event::DataCandidates, 1);
                if log.partition() != cut.prefix.log().partition() {
                    continue;
                }
                if registration.is_none_or(|r| r.identity().incarnation != log.epoch()) {
                    registration = Some(
                        Registration::load(&self.files, log.epoch(), &self.prefix, self.profile)
                            .await?,
                    );
                }
                let identity = registration.unwrap().identity();
                ensure!(
                    identity.incarnation != cut.identity.incarnation || identity == cut.identity,
                    "checkpoint and registration epochs differ"
                );
                if identity.epoch < cut.identity.epoch
                    || (identity == cut.identity && position <= cut.prefix.position())
                {
                    self.authority.delete_versions(&key).await?;
                    REGISTRY.event(Event::DataDeleteAck, 1);
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }
}
