//! Native checkpoints bind values, revisions and retry receipts to an exact
//! selected-journal boundary. The descriptor lives in the root register; its
//! tables and manifest are immutable. Upload failure returns no publishable handle.
mod snapshot;
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
pub use snapshot::{Cursor, DatabaseSnapshot, Snapshot};
use std::{ops::Bound, sync::Arc};
use uuid::Uuid;

const MAX_DESCRIPTOR_BYTES: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const CHUNK_ROWS: usize = 4096;

/// Can only be produced by staging a private native recovery at a complete
/// frame boundary. It grants neither publication nor serving permission.
pub struct Staged {
    pub(crate) origin: Record,
    pub(crate) reference: Reference,
}
impl Staged {
    /// Immutable readback of this staged image. This neither publishes it nor
    /// authorizes serving it as current database state.
    pub fn reference(&self) -> Reference {
        self.reference
    }
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
/// Complete checkpoint descriptor, stored inline in the authority record.
/// It authenticates immutable tables and an exact journal cut; it is not a
/// serving capability or a pin retaining old objects.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Reference {
    pub(crate) cut: Cut,
    tree: Option<(u8, Blob)>,
    tables: u64,
    rows: u64,
    databases: u64,
}
impl Reference {
    pub(crate) fn encode(self) -> anyhow::Result<String> {
        let tree = self.tree.map(|(_, b)| {
            let mut out = Vec::new();
            b.encode(&mut out);
            hex::encode(out)
        });
        let encoded = serde_json::to_string(&Wire {
            format: 3,
            epoch: self.cut.identity.epoch,
            incarnation: self.cut.identity.incarnation.to_string(),
            cut: hex::encode(self.cut.prefix.encode()),
            tree,
            height: self.tree.map_or(0, |(h, _)| h),
            tables: self.tables,
            rows: self.rows,
            databases: self.databases,
        })?;
        ensure!(
            encoded.len() <= MAX_DESCRIPTOR_BYTES,
            "checkpoint descriptor exceeds bound"
        );
        Ok(encoded)
    }
    pub(crate) fn decode(encoded: &str) -> anyhow::Result<Self> {
        ensure!(
            encoded.len() <= MAX_DESCRIPTOR_BYTES,
            "checkpoint descriptor exceeds bound"
        );
        let wire: Wire = serde_json::from_str(encoded)?;
        let incarnation = Uuid::parse_str(&wire.incarnation)?;
        let prefix = Prefix::decode(&hex::decode(&wire.cut)?)?;
        ensure!(
            wire.format == 3
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
    last: Option<Bytes>,
}
impl Builder {
    fn new(files: Arc<dyn Storage>) -> Self {
        Self {
            tree: tree::Builder::new(files.clone()),
            files,
            pending: Vec::new(),
            bytes: 0,
            rows: 0,
            last: None,
        }
    }
    #[cfg(test)]
    async fn push(
        &mut self,
        prefix: &[u8],
        kind: u8,
        suffix: &[u8],
        revision: u64,
        value: Option<Bytes>,
    ) -> anyhow::Result<()> {
        self.push_row(checkpoint_row(prefix, kind, suffix, revision, value))
            .await
    }
    async fn push_row(&mut self, row: Entry) -> anyhow::Result<()> {
        ensure!(
            self.last.as_ref().is_none_or(|last| last < &row.key),
            "checkpoint rows are not strictly ordered"
        );
        let length = row.key.len() + 14 + row.value.as_ref().map_or(0, Bytes::len);
        if self.bytes + length > CHUNK_BYTES || self.pending.len() == CHUNK_ROWS {
            self.flush().await?;
        }
        self.last = Some(row.key.clone());
        self.pending.push(row);
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
    write_projection(
        &recovering.projection,
        recovering.replay.files(),
        recovering.replay.origin(),
        cut,
    )
    .await
}

pub(crate) async fn write_projection(
    projection: &Projection,
    files: Arc<dyn Storage>,
    origin: Record,
    cut: Cut,
) -> anyhow::Result<Staged> {
    let reference = build_projection(projection, files, cut).await?;
    Ok(Staged { origin, reference })
}
async fn build_projection(
    projection: &Projection,
    files: Arc<dyn Storage>,
    cut: Cut,
) -> anyhow::Result<Reference> {
    let mut builder = Builder::new(files.clone());
    for resident in projection.databases.values() {
        resident
            .state
            .as_ref()
            .context("checkpoint metadata is incomplete")?
            .checkpoint_complete()?;
    }
    let mut changes = changed_rows(projection).peekable();
    let mut databases = projection
        .base
        .as_ref()
        .map_or(0, |base| base.header().databases);
    for resident in projection.databases.values() {
        if resident.base.is_none() && resident.state.as_ref().unwrap().revision() > 0 {
            databases = databases
                .checked_add(1)
                .context("checkpoint database count overflow")?;
        }
    }
    if let Some(base) = &projection.base {
        let mut tables = base.seek(Bytes::new())?;
        let mut count = 0u64;
        // Retained references carry their share of this authenticated total.
        // Only replaced tables need an index read and a row-count adjustment.
        builder.rows = base.header().rows;
        let mut boundary: Option<Bytes> = None;
        while let Some(routed) = tables.next().await? {
            builder.tree.reuse_from(&tables);
            let lower = boundary.replace(routed.last.clone());
            count = count
                .checked_add(1)
                .context("checkpoint table count overflow")?;
            ensure!(count <= base.header().tables, "extra checkpoint tables");
            // A changed database replaces its entire receipt window, including
            // expired rows absent from `changes`. The manifest's (lower, last]
            // interval conservatively covers them without opening the table.
            let first = lower
                .as_deref()
                .map(split_key)
                .transpose()?
                .map(|(db, kind, _)| ((db.name.to_owned(), db.generation), kind));
            let (last, _, _) = split_key(&routed.last)?;
            let last = (last.name.to_owned(), last.generation);
            let replaces_metadata = projection
                .databases
                .range((
                    first
                        .as_ref()
                        .map_or(Bound::Unbounded, |(key, _)| Bound::Included(key.clone())),
                    Bound::Included(last),
                ))
                .any(|(key, resident)| {
                    resident.changed()
                        && first
                            .as_ref()
                            .is_none_or(|(first, kind)| key != first || *kind < 2)
                });
            if !replaces_metadata && changes.peek().is_none_or(|row| row.key > routed.last) {
                builder.flush().await?;
                builder.last = Some(routed.last.clone());
                builder.tree.push(routed.reference, routed.last).await?;
                continue;
            }
            let table = base.table(&routed).await?;
            ensure!(
                lower
                    .as_ref()
                    .is_none_or(|last| last.as_ref() < table.first_key()),
                "overlapping checkpoint tables"
            );
            builder.rows = builder
                .rows
                .checked_sub(table.row_count())
                .context("checkpoint row count underflow")?;
            while changes
                .peek()
                .is_some_and(|row| row.key.as_ref() < table.first_key())
            {
                builder.push_row(changes.next().unwrap()).await?;
            }
            let mut source = table.scan(b"", None, u64::MAX)?;
            let mut seen = 0u64;
            while let Some(old) = source.next().await? {
                seen += 1;
                let (db, kind, _) = split_key(&old.key)?;
                if kind < 2
                    && projection
                        .databases
                        .get(&(db.name.to_owned(), db.generation))
                        .is_some_and(Resident::changed)
                {
                    continue;
                }
                while changes.peek().is_some_and(|new| new.key < old.key) {
                    builder.push_row(changes.next().unwrap()).await?;
                }
                let row = if changes.peek().is_some_and(|new| new.key == old.key) {
                    changes.next().unwrap()
                } else {
                    old
                };
                builder.push_row(row).await?;
            }
            ensure!(
                seen == table.row_count(),
                "checkpoint contains hidden row versions"
            );
            while changes.peek().is_some_and(|row| row.key <= routed.last) {
                builder.push_row(changes.next().unwrap()).await?;
            }
        }
        ensure!(
            count == base.header().tables,
            "checkpoint table count mismatch"
        );
    }
    for row in changes {
        builder.push_row(row).await?;
    }
    builder.flush().await?;
    let tables = builder.tree.tables;
    let tree = builder.tree.finish().await?;
    Ok(Reference {
        cut,
        tree,
        tables,
        rows: builder.rows,
        databases,
    })
}

fn checkpoint_row(
    prefix: &[u8],
    kind: u8,
    suffix: &[u8],
    revision: u64,
    value: Option<Bytes>,
) -> Entry {
    let mut key = Vec::with_capacity(prefix.len() + 1 + suffix.len());
    key.extend_from_slice(prefix);
    key.push(kind);
    key.extend_from_slice(suffix);
    Entry {
        key: key.into(),
        revision,
        value,
    }
}
fn changed_rows(projection: &Projection) -> impl Iterator<Item = Entry> + '_ {
    projection
        .databases
        .iter()
        .filter(|(_, resident)| resident.changed())
        .flat_map(|((name, generation), resident)| {
            let state = resident.state.as_ref().expect("checkpoint state validated");
            let mut prefix = name.as_bytes().to_vec();
            prefix.push(0);
            prefix.extend_from_slice(generation.as_bytes());
            let mut meta = state.revision().to_be_bytes().to_vec();
            meta.extend_from_slice(&state.checkpoint_floor().to_be_bytes());
            meta.extend_from_slice(&resident.statistics.keys.to_be_bytes());
            meta.extend_from_slice(&resident.statistics.bytes.to_be_bytes());
            let metadata = std::iter::once(checkpoint_row(
                &prefix,
                0,
                b"",
                state.revision(),
                Some(meta.into()),
            ));
            let receipts_prefix = prefix.clone();
            let receipts =
                state
                    .checkpoint_receipts()
                    .map(move |(revision, request, fingerprint)| {
                        checkpoint_row(
                            &receipts_prefix,
                            1,
                            request.as_bytes(),
                            revision,
                            Some(Bytes::copy_from_slice(&fingerprint)),
                        )
                    });
            let values = resident
                .memory
                .scan(b"", None, state.revision())
                .expect("valid complete range")
                .map(move |row| checkpoint_row(&prefix, 2, &row.key, row.revision, row.value));
            metadata.chain(receipts).chain(values)
        })
}

pub(super) async fn load(
    files: Arc<dyn Storage>,
    header: Reference,
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
        let resident = projection.databases.get_mut(key).unwrap();
        let state = resident
            .state
            .as_mut()
            .context("checkpoint state missing")?;
        state.seal_checkpoint()?;
        let mut actual = super::Statistics::default();
        for row in resident.memory.scan(b"", None, state.revision())? {
            actual.replace(row.key.len(), None, row.value.as_ref().map(Bytes::len))?;
        }
        ensure!(
            actual == resident.statistics,
            "checkpoint live statistics mismatch"
        );
    }
    Ok(())
}
fn load_row(
    projection: &mut Projection,
    current: &mut Option<(String, Uuid)>,
    row: Entry,
) -> anyhow::Result<()> {
    let (db, kind, suffix) = split_key(&row.key)?;
    let Database { name, generation } = db;
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
        let (state, statistics) = metadata(&row, db)?;
        let revision = state.revision();
        projection.databases.insert(
            key.clone(),
            Resident {
                statistics,
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
                base: None,
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
fn split_key(key: &[u8]) -> anyhow::Result<(Database<'_>, u8, &[u8])> {
    let zero = key
        .iter()
        .position(|b| *b == 0)
        .context("invalid checkpoint key")?;
    ensure!(key.len() >= zero + 18, "truncated checkpoint key");
    let database = Database {
        name: std::str::from_utf8(&key[..zero])?,
        generation: Uuid::from_slice(&key[zero + 1..zero + 17])?,
    };
    super::validate_database(database)?;
    let kind = key[zero + 17];
    ensure!(kind <= 2, "invalid checkpoint row kind");
    Ok((database, kind, &key[zero + 18..]))
}
fn metadata(row: &Entry, database: Database<'_>) -> anyhow::Result<(State, super::Statistics)> {
    let bytes = row.value.as_ref().context("checkpoint metadata deleted")?;
    ensure!(bytes.len() == 32, "invalid checkpoint metadata length");
    let revision = u64::from_be_bytes(bytes[..8].try_into()?);
    let floor = u64::from_be_bytes(bytes[8..16].try_into()?);
    ensure!(
        row.revision == revision,
        "checkpoint metadata revision mismatch"
    );
    let statistics = super::Statistics {
        keys: u64::from_be_bytes(bytes[16..24].try_into()?),
        bytes: u64::from_be_bytes(bytes[24..32].try_into()?),
    };
    ensure!(
        statistics.keys <= statistics.bytes && (statistics.keys != 0 || statistics.bytes == 0),
        "invalid checkpoint live statistics"
    );
    Ok((
        State::from_checkpoint(database, revision, floor)?,
        statistics,
    ))
}
#[cfg(test)]
mod tests;
