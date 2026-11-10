//! Native checkpoints bind values, revisions and retry receipts to an exact
//! selected-journal boundary. All objects are immutable; the root register is
//! the sole publication point. Upload failure returns no publishable handle.
mod tree;
use super::{Limits, Memory, Projection, Recovering, Resident, State, add, memory};
use crate::{
    cache::Cache,
    index::table::{self, Entry, Row, Table},
    journal::{LogId, Prefix},
    ownership::root::{Cut, Identity, Record},
    storage::{Storage, immutable::Blob},
    transactions::record::Database,
};
use anyhow::{Context, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const MAX_HEADER: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const CHUNK_ROWS: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Reference(Blob);
impl Reference {
    pub(crate) fn encode(self) -> String {
        let mut bytes = Vec::new();
        self.0.encode(&mut bytes);
        hex::encode(bytes)
    }
    pub(crate) fn decode(encoded: &str) -> anyhow::Result<Self> {
        ensure!(
            encoded.len() == 112
                && encoded
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid checkpoint reference"
        );
        let blob = Blob::decode(&hex::decode(encoded)?)?;
        ensure!(
            blob.size() <= MAX_HEADER as u64,
            "checkpoint header exceeds bound"
        );
        Ok(Self(blob))
    }
}
/// Can only be produced by staging a private native recovery at a complete
/// frame boundary. It grants neither publication nor serving permission.
pub struct Staged {
    pub(crate) origin: Record,
    pub(crate) reference: Reference,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    format: u8,
    epoch: u64,
    incarnation: String,
    cut: String,
    tree: Option<String>,
    height: u8,
    tables: u64,
    rows: u64,
    databases: u64,
}
pub(crate) struct Header {
    pub cut: Cut,
    tree: Option<(u8, Blob)>,
    tables: u64,
    rows: u64,
    databases: u64,
}
impl Header {
    async fn upload(self, files: Arc<dyn Storage>) -> anyhow::Result<Reference> {
        let tree = self.tree.map(|(_, b)| {
            let mut out = Vec::new();
            b.encode(&mut out);
            hex::encode(out)
        });
        let bytes = serde_json::to_vec(&Wire {
            format: 1,
            epoch: self.cut.identity.epoch,
            incarnation: self.cut.identity.incarnation.to_string(),
            cut: hex::encode(self.cut.prefix.encode()),
            tree,
            height: self.tree.map_or(0, |(h, _)| h),
            tables: self.tables,
            rows: self.rows,
            databases: self.databases,
        })?;
        ensure!(bytes.len() <= MAX_HEADER, "checkpoint header exceeds bound");
        Ok(Reference(Blob::upload(files, bytes.into()).await?))
    }
    pub(crate) async fn load(
        files: Arc<dyn Storage>,
        reference: Reference,
    ) -> anyhow::Result<Self> {
        ensure!(
            reference.0.size() <= MAX_HEADER as u64,
            "checkpoint header exceeds bound"
        );
        let bytes = reference
            .0
            .read_range(files.as_ref(), 0..reference.0.size())
            .await?;
        let wire: Wire = serde_json::from_slice(&bytes)?;
        let incarnation = Uuid::parse_str(&wire.incarnation)?;
        let prefix = Prefix::decode(&hex::decode(&wire.cut)?)?;
        ensure!(
            wire.format == 1
                && wire.epoch > 0
                && !incarnation.is_nil()
                && incarnation.to_string() == wire.incarnation
                && prefix.log() == LogId::root(incarnation),
            "invalid checkpoint cut"
        );
        ensure!(
            hex::encode(prefix.encode()) == wire.cut,
            "noncanonical checkpoint cut"
        );
        let tree = wire
            .tree
            .map(|s| -> anyhow::Result<_> {
                let b = Blob::decode(&hex::decode(&s)?)?;
                let mut out = Vec::new();
                b.encode(&mut out);
                ensure!(hex::encode(out) == s, "noncanonical checkpoint tree");
                Ok((wire.height, b))
            })
            .transpose()?;
        ensure!(
            wire.height <= 8
                && if tree.is_none() {
                    wire.height == 0 && wire.rows == 0 && wire.tables == 0 && wire.databases == 0
                } else {
                    wire.tables > 0
                        && wire.tables <= wire.rows
                        && wire.databases > 0
                        && wire.databases <= wire.rows / 2
                },
            "invalid checkpoint manifest counts"
        );
        Ok(Self {
            cut: Cut {
                identity: Identity {
                    epoch: wire.epoch,
                    incarnation,
                },
                prefix,
            },
            tree,
            tables: wire.tables,
            rows: wire.rows,
            databases: wire.databases,
        })
    }
}
struct Builder {
    files: Arc<dyn Storage>,
    tree: tree::Builder,
    pending: Vec<Entry>,
    bytes: usize,
    rows: u64,
}
impl Builder {
    fn new(files: Arc<dyn Storage>) -> Self {
        Self {
            tree: tree::Builder::new(files.clone()),
            files,
            pending: Vec::new(),
            bytes: 0,
            rows: 0,
        }
    }
    async fn push(
        &mut self,
        prefix: &[u8],
        kind: u8,
        suffix: &[u8],
        revision: u64,
        value: Option<Bytes>,
    ) -> anyhow::Result<()> {
        let length = prefix.len() + 1 + suffix.len() + 14 + value.as_ref().map_or(0, Bytes::len);
        if self.bytes + length > CHUNK_BYTES || self.pending.len() == CHUNK_ROWS {
            self.flush().await?;
        }
        let mut key = Vec::with_capacity(prefix.len() + 1 + suffix.len());
        key.extend_from_slice(prefix);
        key.push(kind);
        key.extend_from_slice(suffix);
        self.pending.push(Entry {
            key: key.into(),
            revision,
            value,
        });
        self.bytes += length;
        self.rows = self
            .rows
            .checked_add(1)
            .context("checkpoint row count overflow")?;
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let reference = table::build(
            self.files.clone(),
            self.pending.iter().map(|r| Row {
                key: &r.key,
                revision: r.revision,
                value: r.value.as_deref(),
            }),
        )
        .await?;
        self.tree
            .push(reference, self.pending.last().unwrap().key.clone())
            .await?;
        self.pending.clear();
        self.bytes = 0;
        Ok(())
    }
}
pub(super) async fn write(recovering: &Recovering) -> anyhow::Result<Staged> {
    let cut = recovering
        .replay
        .cut()
        .context("checkpoint requires a selected journal boundary")?;
    let files = recovering.replay.files();
    let mut builder = Builder::new(files.clone());
    for ((name, generation), resident) in &recovering.projection.databases {
        let state = resident
            .state
            .as_ref()
            .context("checkpoint metadata is incomplete")?;
        state.checkpoint_complete()?;
        let mut prefix = name.as_bytes().to_vec();
        prefix.push(0);
        prefix.extend_from_slice(generation.as_bytes());
        let mut meta = state.revision().to_be_bytes().to_vec();
        meta.extend_from_slice(&state.checkpoint_floor().to_be_bytes());
        builder
            .push(&prefix, 0, b"", state.revision(), Some(meta.into()))
            .await?;
        for (revision, request, fingerprint) in state.checkpoint_receipts() {
            let value = Bytes::copy_from_slice(&fingerprint);
            builder
                .push(&prefix, 1, request.as_bytes(), revision, Some(value))
                .await?;
        }
        for row in resident.memory.scan(b"", None, state.revision())? {
            builder
                .push(&prefix, 2, &row.key, row.revision, row.value)
                .await?;
        }
    }
    builder.flush().await?;
    let tables = builder.tree.tables;
    let tree = builder.tree.finish().await?;
    let reference = Header {
        cut,
        tree,
        tables,
        rows: builder.rows,
        databases: recovering.projection.usage.databases as u64,
    }
    .upload(files)
    .await?;
    Ok(Staged {
        origin: recovering.replay.origin(),
        reference,
    })
}

pub(super) async fn load(
    files: Arc<dyn Storage>,
    header: Header,
    limits: Limits,
) -> anyhow::Result<Projection> {
    ensure!(
        header.databases <= limits.databases as u64
            && header.rows as u128
                <= limits.databases as u128 + limits.versions as u128 + limits.receipts as u128,
        "checkpoint exceeds recovery capacity"
    );
    let mut projection = Projection::new(limits)?;
    let mut manifest = tree::Reader::new(files.clone(), header.tree)?;
    let cache = Arc::new(Cache::new(0, None)?);
    let mut tables = 0u64;
    let mut rows = 0u64;
    let mut previous = None;
    let mut current: Option<(String, Uuid)> = None;
    while let Some(routed) = manifest.next().await? {
        tables = tables
            .checked_add(1)
            .context("checkpoint table count overflow")?;
        ensure!(tables <= header.tables, "extra checkpoint table");
        let table = Table::open(files.clone(), cache.clone(), routed.reference).await?;
        ensure!(
            table.last_key() == routed.last.as_ref(),
            "checkpoint table boundary mismatch"
        );
        let expected = table.row_count();
        ensure!(expected <= header.rows - rows, "extra checkpoint rows");
        let mut scanned = 0;
        let mut cursor = table.scan(b"", None, u64::MAX)?;
        while let Some(row) = cursor.next().await? {
            ensure!(
                previous.as_ref().is_none_or(|p| p < &row.key),
                "checkpoint keys are not strictly ordered"
            );
            previous = Some(row.key.clone());
            load_row(&mut projection, &mut current, row)?;
            scanned += 1;
        }
        ensure!(
            scanned == expected,
            "checkpoint contains hidden row versions"
        );
        rows += scanned;
    }
    finish_database(&mut projection, current.as_ref())?;
    ensure!(
        tables == header.tables
            && rows == header.rows
            && projection.usage.databases as u64 == header.databases,
        "checkpoint is incomplete"
    );
    Ok(projection)
}
fn finish_database(
    projection: &mut Projection,
    current: Option<&(String, Uuid)>,
) -> anyhow::Result<()> {
    if let Some(key) = current {
        projection
            .databases
            .get_mut(key)
            .unwrap()
            .state
            .as_mut()
            .context("checkpoint state missing")?
            .seal_checkpoint()?;
    }
    Ok(())
}
fn load_row(
    projection: &mut Projection,
    current: &mut Option<(String, Uuid)>,
    row: Entry,
) -> anyhow::Result<()> {
    let zero = row
        .key
        .iter()
        .position(|b| *b == 0)
        .context("invalid checkpoint key")?;
    ensure!(row.key.len() >= zero + 18, "truncated checkpoint key");
    let name = std::str::from_utf8(&row.key[..zero])?;
    let generation = Uuid::from_slice(&row.key[zero + 1..zero + 17])?;
    let kind = row.key[zero + 17];
    let suffix = &row.key[zero + 18..];
    let db = Database { name, generation };
    super::validate_database(db)?;
    let key = (name.to_owned(), generation);
    if kind == 0 {
        finish_database(projection, current.as_ref())?;
        ensure!(
            suffix.is_empty() && !projection.databases.contains_key(&key),
            "duplicate checkpoint database"
        );
        ensure!(
            projection.usage.databases < projection.limits.databases,
            "checkpoint database capacity exhausted"
        );
        let bytes = row.value.as_ref().context("checkpoint metadata deleted")?;
        ensure!(bytes.len() == 16, "invalid checkpoint metadata length");
        let revision = u64::from_be_bytes(bytes[..8].try_into()?);
        let floor = u64::from_be_bytes(bytes[8..].try_into()?);
        ensure!(
            row.revision == revision,
            "checkpoint metadata revision mismatch"
        );
        let state = State::from_checkpoint(db, revision, floor)?;
        projection.databases.insert(
            key.clone(),
            Resident {
                memory: Memory::from_checkpoint(
                    revision,
                    memory::Limits {
                        versions: projection.limits.versions,
                        bytes: projection.limits.value_bytes,
                    },
                )?,
                state: Some(state),
                receipts: 0,
                receipt_bytes: 0,
                floor: revision,
            },
        );
        projection.usage.databases += 1;
        *current = Some(key);
        return Ok(());
    }
    ensure!(
        current.as_ref() == Some(&key),
        "checkpoint row has no database metadata"
    );
    let resident = projection.databases.get_mut(&key).unwrap();
    match kind {
        1 => {
            let request = std::str::from_utf8(suffix)?;
            ensure!(
                (1..=128).contains(&request.len()),
                "invalid checkpoint receipt key"
            );
            let bytes = row.value.context("checkpoint receipt deleted")?;
            ensure!(bytes.len() == 32, "invalid checkpoint receipt length");
            let fingerprint = bytes.as_ref();
            let receipts = add(projection.usage.receipts, 1, projection.limits.receipts)?;
            let receipt_bytes = add(
                projection.usage.receipt_bytes,
                request.len() + 40,
                projection.limits.receipt_bytes,
            )?;
            resident.state.as_mut().unwrap().load_receipt(
                row.revision,
                request,
                fingerprint.try_into()?,
            )?;
            resident.receipts += 1;
            resident.receipt_bytes += request.len() + 40;
            projection.usage.receipts = receipts;
            projection.usage.receipt_bytes = receipt_bytes;
        }
        2 => {
            let length = suffix.len() + row.value.as_ref().map_or(0, Bytes::len);
            let versions = add(projection.usage.versions, 1, projection.limits.versions)?;
            let bytes = add(
                projection.usage.value_bytes,
                length,
                projection.limits.value_bytes,
            )?;
            resident.memory.load_row(Entry {
                key: Bytes::copy_from_slice(suffix),
                revision: row.revision,
                value: row.value,
            })?;
            projection.usage.versions = versions;
            projection.usage.value_bytes = bytes;
        }
        _ => anyhow::bail!("invalid checkpoint row kind"),
    }
    Ok(())
}
#[cfg(test)]
mod tests;
