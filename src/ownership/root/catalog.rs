//! Persisted prefix decisions using the existing bounded-page immutable B+tree.
use super::{Cut, Identity, Record, Registration, Registrations};
use crate::{
    directory::{self, Directory, Pointer},
    journal::{Batch, Prefix, Reader},
    storage::Storage,
};
use anyhow::{Context, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const MAX_HEADER_BYTES: usize = 4096;
pub(super) const SELECT_LIMIT: usize = 8;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: u8,
    id: String,
    index: directory::Root,
}

fn header_key(id: Uuid) -> String {
    format!("catalogs/{}", id.simple())
}
fn entry_key(identity: Identity) -> String {
    format!("{:016x}-{}", identity.epoch, identity.incarnation.simple())
}
pub(super) async fn load(
    files: &Arc<dyn Storage>,
    id: Option<Uuid>,
) -> anyhow::Result<Option<directory::Root>> {
    let Some(id) = id else { return Ok(None) };
    let object = files
        .get(&header_key(id), MAX_HEADER_BYTES)
        .await?
        .context("selected-prefix catalog missing")?;
    ensure!(
        (33..=MAX_HEADER_BYTES).contains(&object.bytes.len()),
        "invalid catalog size"
    );
    let (body, checksum) = object.bytes.split_at(object.bytes.len() - 32);
    ensure!(
        Sha256::digest(body).as_slice() == checksum,
        "catalog checksum mismatch"
    );
    let header: Header = serde_json::from_slice(body)?;
    ensure!(
        header.format == 1 && header.id == id.to_string(),
        "catalog identity or format mismatch"
    );
    header.index.validate()?;
    Ok(Some(header.index))
}
pub(super) async fn selected(
    files: &Arc<dyn Storage>,
    index: Option<&directory::Root>,
    identity: Identity,
) -> anyhow::Result<Option<Prefix>> {
    let Some(pointer) = Directory::new(files.clone())
        .get(index, &entry_key(identity))
        .await?
    else {
        return Ok(None);
    };
    let object = files
        .get(&pointer.key, Prefix::ENCODED_BYTES)
        .await?
        .context("selected prefix missing")?;
    ensure!(
        hex::encode(Sha256::digest(&object.bytes)) == pointer.sha256,
        "selected prefix checksum mismatch"
    );
    let prefix = Prefix::decode(&object.bytes)?;
    ensure!(
        prefix.log() == crate::journal::LogId::root(identity.incarnation),
        "selected prefix names a different journal"
    );
    Ok(Some(prefix))
}

/// Snapshot traversal created only after a confirmed activation and drain.
/// A snapshot may become stale; publication requires the same confirmed catalog
/// and captures its exact provider revision before any immutable upload.
/// Retained scans and recovery cursors need caller-side admission bounds.
pub struct Recovery {
    files: Arc<dyn Storage>,
    record: Record,
    chain: Registrations,
    index: Option<directory::Root>,
    loaded: bool,
    current: Option<Registration>,
    cutoff: Option<Cut>,
    finished: bool,
}
impl Recovery {
    pub(super) fn new(files: Arc<dyn Storage>, record: Record, chain: Registrations) -> Self {
        Self {
            files,
            record,
            chain,
            index: None,
            loaded: false,
            current: None,
            cutoff: None,
            finished: false,
        }
    }

    /// Continue the immutable ancestry only after our own confirmed selection.
    /// A changed identity, checkpoint or collection generation requires a new
    /// cursor. Uncertain publication never enters this path.
    pub(super) async fn advance(&mut self, record: Record) -> anyhow::Result<()> {
        ensure!(
            Record {
                catalog: record.catalog,
                ..self.record
            }
            .same_recovery(record),
            "recovery changed beyond the confirmed catalog selection"
        );
        self.index = load(&self.files, record.catalog).await?;
        self.record = record;
        Ok(())
    }

    /// Returns previous journals, newest first. Existing decisions replay their
    /// exact prefix; only previously unselected journals use discovery.
    /// Calling this does not establish transaction or serving readiness.
    pub async fn next(&mut self) -> anyhow::Result<Option<JournalRecovery>> {
        if self.finished {
            return Ok(None);
        }
        if !self.loaded {
            self.index = load(&self.files, self.record.catalog).await?;
            if let Some(header) = self.record.checkpoint {
                ensure!(
                    header.cut.identity.epoch < self.record.identity.epoch,
                    "checkpoint is not from prior history"
                );
                if let Some(selected) =
                    selected(&self.files, self.index.as_ref(), header.cut.identity).await?
                {
                    Reader::resume(self.files.clone(), header.cut.prefix, selected)?;
                }
                self.cutoff = Some(header.cut);
            }
            self.loaded = true;
        }
        loop {
            if self.current.is_none() {
                self.current = self.chain.next().await?;
            }
            let Some(registration) = self.current else {
                ensure!(self.cutoff.is_none(), "checkpoint ancestry is incomplete");
                self.finished = true;
                return Ok(None);
            };
            if registration.identity() == self.record.identity {
                self.current = None;
                continue;
            }
            ensure!(
                registration.identity().epoch < self.record.identity.epoch,
                "invalid recovery ancestry"
            );
            if let Some(cutoff) = self.cutoff
                && registration.identity().epoch <= cutoff.identity.epoch
            {
                ensure!(
                    registration.identity() == cutoff.identity,
                    "checkpoint ancestry differs from selected history"
                );
                let selected = selected(&self.files, self.index.as_ref(), cutoff.identity).await?;
                self.finished = true;
                if selected.is_some() {
                    return Ok(None);
                }
                return Ok(Some(JournalRecovery {
                    record: self.record,
                    index: self.index.clone(),
                    registration,
                    selected: false,
                    reader: Reader::discover_from(self.files.clone(), cutoff.prefix),
                }));
            }
            let prefix =
                selected(&self.files, self.index.as_ref(), registration.identity()).await?;
            let reader = match prefix {
                Some(prefix) => Reader::replay(self.files.clone(), prefix),
                None => Reader::discover(self.files.clone(), registration.journal()),
            };
            self.current = None;
            return Ok(Some(JournalRecovery {
                record: self.record,
                index: self.index.clone(),
                registration,
                selected: prefix.is_some(),
                reader,
            }));
        }
    }
}

/// Opaque, owner-bound scan. It cannot be built from an arbitrary caller prefix.
pub struct JournalRecovery {
    pub(super) record: Record,
    index: Option<directory::Root>,
    registration: Registration,
    selected: bool,
    reader: Reader,
}
impl JournalRecovery {
    pub fn registration(&self) -> Registration {
        self.registration
    }
    pub fn is_selected(&self) -> bool {
        self.selected
    }
    pub fn finished_prefix(&self) -> Option<Prefix> {
        self.reader.finished_prefix()
    }
    pub async fn read_next(&mut self) -> anyhow::Result<Option<Batch>> {
        self.reader.read_next().await
    }
}

pub(super) struct Publication {
    files: Arc<dyn Storage>,
    owner: Uuid,
    id: Uuid,
    index: Option<directory::Root>,
    prefixes: BTreeMap<String, Prefix>,
}
impl Publication {
    pub(super) fn new(
        files: Arc<dyn Storage>,
        id: Uuid,
        recovered: &[JournalRecovery],
    ) -> anyhow::Result<Self> {
        ensure!(
            (1..=SELECT_LIMIT).contains(&recovered.len()),
            "invalid selection batch size"
        );
        let index = recovered[0].index.clone();
        let owner = recovered[0].record.identity.incarnation;
        let mut prefixes = BTreeMap::new();
        for journal in recovered {
            ensure!(!journal.selected, "journal prefix was already selected");
            let prefix = journal
                .reader
                .finished_prefix()
                .context("journal recovery is incomplete")?;
            ensure!(
                prefixes
                    .insert(entry_key(journal.registration.identity()), prefix)
                    .is_none(),
                "duplicate journal selection"
            );
        }
        Ok(Self {
            files,
            owner,
            id,
            index,
            prefixes,
        })
    }

    /// Fresh immutable objects; no retries. Any failed upload prevents register
    /// publication, even if the object may arrive later. Orphans need future GC.
    pub async fn execute(self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        let prefixes = self.prefixes.len();
        let mut entries = BTreeMap::new();
        for (key, prefix) in self.prefixes {
            let bytes = prefix.encode();
            let pointer = Pointer {
                key: format!("manifests/{key}.{}", Uuid::new_v4()),
                sha256: hex::encode(Sha256::digest(&bytes)),
            };
            self.files.put(&pointer.key, bytes).await?;
            entries.insert(key, pointer);
        }
        let index = Directory::new(self.files.clone())
            .insert_many(self.index.as_ref(), &entries)
            .await?;
        let mut body = serde_json::to_vec(&Header {
            format: 1,
            id: self.id.to_string(),
            index,
        })?;
        let digest = Sha256::digest(&body);
        body.extend_from_slice(&digest);
        ensure!(
            body.len() <= MAX_HEADER_BYTES,
            "catalog exceeds header bound"
        );
        self.files
            .put(&header_key(self.id), Bytes::from(body))
            .await?;
        tracing::info!(owner = %self.owner, prefixes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "native catalog completed");
        Ok(())
    }
}
