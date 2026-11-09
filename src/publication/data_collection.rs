//! A bounded cluster data-page sweep that performs no immutable PUT.
use super::*;
use crate::engine::{GcResult, MAX_HEAD, protected_pages};
use anyhow::ensure;
use std::collections::BTreeSet;

impl Published {
    /// Format 5 only: the engine's original observation fences staged pages,
    /// and proposals retain that fence through every helper publication.
    pub async fn collect_data(&self) -> anyhow::Result<GcResult> {
        ensure!(
            self.global_collection,
            "upload-free data collection requires format 5"
        );
        let before = self.authority.read().await?;
        let candidates = self
            .files
            .list("blocks/", cursor(&before, DATA_CURSOR), 128)
            .await?;
        let mut databases: BTreeMap<String, Vec<&String>> = BTreeMap::new();
        for key in &candidates {
            databases
                .entry(witness(key, "blocks/")?.to_owned())
                .or_default()
                .push(key);
        }
        if candidates.is_empty() && cursor(&before, DATA_CURSOR).is_none() {
            return Ok(GcResult {
                examined: 0,
                deleted: 0,
            });
        }
        metrics::REGISTRY.event(Event::DataCandidates, candidates.len());
        let observed = self.authority.read().await?;
        let protected = root(&observed.info)?;
        let mut info = observed.info.clone();
        info.insert(
            DATA_CURSOR.into(),
            if candidates.len() == 128 {
                candidates.last().unwrap().clone()
            } else {
                String::new()
            },
        );
        info.insert("durable-db-fence".into(), Uuid::new_v4().to_string());
        budget(&info)?;
        if self.authority.replace(&observed, info).await?.is_none() {
            return Ok(GcResult {
                examined: candidates.len(),
                deleted: 0,
            });
        }
        let mut result = GcResult {
            examined: candidates.len(),
            deleted: 0,
        };
        // One manifest at a time bounds memory independently of database count.
        for (db, names) in databases {
            let live = match self.directory.get(protected.as_ref(), &db).await? {
                None => BTreeSet::new(),
                Some(pointer) => {
                    let cached = match &self.cache {
                        Some(cache) => cache.get(&pointer.key, &pointer.sha256).await,
                        None => None,
                    };
                    let bytes = match cached {
                        Some(bytes) => bytes,
                        None => {
                            let object = self
                                .files
                                .get(&pointer.key, MAX_HEAD)
                                .await?
                                .context("protected collector manifest is unavailable")?;
                            ensure!(
                                hex::encode(Sha256::digest(&object.bytes)) == pointer.sha256,
                                "collector manifest checksum mismatch"
                            );
                            object.bytes
                        }
                    };
                    protected_pages(&db, &bytes)?
                }
            };
            for key in names {
                if !live.contains(key) {
                    self.authority.delete_versions(key).await?;
                    metrics::REGISTRY.event(Event::DataDeleteAck, 1);
                    result.deleted += 1;
                }
            }
        }
        Ok(result)
    }
}
