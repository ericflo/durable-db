//! Lazy reads of one immutable checkpoint. No database registry, values or
//! receipt window is materialized. Callers must separately retain the objects,
//! merge the selected/live suffix, authorize tenants and check serving authority.
use super::{Header, MAX_HEADER, Reference, tree};
use crate::{
    cache::Cache,
    index::table::{self, Entry, Table},
    storage::Storage,
    transactions::{record::Database, recovery::Resolution},
};
use anyhow::{Context, ensure};
use bytes::Bytes;
use std::sync::Arc;

struct Inner {
    files: Arc<dyn Storage>,
    cache: Arc<Cache>,
    header: Header,
}

/// Data at an exact checkpoint, not a current database snapshot. Loading this
/// handle reads only its authenticated header; pages are validated on demand.
#[derive(Clone)]
pub struct Snapshot(Arc<Inner>);
impl Snapshot {
    pub(crate) fn cut(&self) -> crate::ownership::root::Cut {
        self.0.header.cut
    }
    pub(crate) fn rows(&self) -> anyhow::Result<RawCursor> {
        self.scan_raw(Bytes::new(), None)
    }
    /// Explicit complete-image audit. This restores a private projection under
    /// the supplied budgets, unlike normal lazy recovery and readback.
    pub async fn verify(
        &self,
        limits: super::super::Limits,
    ) -> anyhow::Result<super::super::Usage> {
        Ok(super::load(self.0.files.clone(), self.0.header, limits)
            .await?
            .usage())
    }
    fn scan_raw(&self, start: Bytes, end: Option<Bytes>) -> anyhow::Result<RawCursor> {
        Ok(RawCursor {
            snapshot: self.clone(),
            manifest: self.seek(start.clone())?,
            full: start.is_empty() && end.is_none(),
            start,
            end,
            current: None,
            table_end: None,
            previous: None,
            done: false,
            failed: false,
            tables: 0,
            rows: 0,
            declared_rows: 0,
            databases: 0,
        })
    }

    pub async fn open(
        files: Arc<dyn Storage>,
        cache: Arc<Cache>,
        reference: Reference,
    ) -> anyhow::Result<Self> {
        ensure!(
            reference.0.size() <= MAX_HEADER as u64,
            "checkpoint header exceeds bound"
        );
        let bytes = reference.0.read_cached(files.as_ref(), &cache).await?;
        Ok(Self(Arc::new(Inner {
            files,
            cache,
            header: Header::decode(&bytes)?,
        })))
    }

    pub async fn database(
        &self,
        database: Database<'_>,
    ) -> anyhow::Result<Option<DatabaseSnapshot>> {
        super::super::validate_database(database)?;
        let mut prefix = database.name.as_bytes().to_vec();
        prefix.push(0);
        prefix.extend_from_slice(database.generation.as_bytes());
        let key = composite(&prefix, 0, b"");
        let Some(row) = self.lookup(key).await? else {
            return Ok(None);
        };
        let state = super::metadata(&row, database)?;
        Ok(Some(DatabaseSnapshot {
            snapshot: self.clone(),
            prefix: prefix.into(),
            revision: state.revision(),
            receipt_floor: state.checkpoint_floor(),
        }))
    }

    fn seek(&self, key: Bytes) -> anyhow::Result<tree::Reader> {
        tree::Reader::seek(
            self.0.files.clone(),
            self.0.header.tree,
            key,
            self.0.cache.clone(),
        )
    }
    async fn table(&self, routed: tree::RoutedTable) -> anyhow::Result<Table> {
        let table =
            Table::open(self.0.files.clone(), self.0.cache.clone(), routed.reference).await?;
        ensure!(
            table.last_key() == routed.last.as_ref(),
            "checkpoint table boundary mismatch"
        );
        Ok(table)
    }
    async fn lookup(&self, key: Bytes) -> anyhow::Result<Option<Entry>> {
        let mut manifest = self.seek(key.clone())?;
        let Some(routed) = manifest.next().await? else {
            return Ok(None);
        };
        self.table(routed).await?.get(&key, u64::MAX).await
    }
}

/// One generation at its recorded checkpoint revision. Retained receipt
/// resolution and value reads do not consult a mutable root or write storage.
#[derive(Clone)]
pub struct DatabaseSnapshot {
    snapshot: Snapshot,
    prefix: Bytes,
    revision: u64,
    receipt_floor: u64,
}
impl DatabaseSnapshot {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn receipt_floor(&self) -> u64 {
        self.receipt_floor
    }

    /// Includes tombstones, which the suffix merge must preserve. None means
    /// absence from this checkpoint only; a newer journal may have the key.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Entry>> {
        ensure!(
            (1..=1024).contains(&key.len()),
            "invalid checkpoint lookup key"
        );
        self.snapshot
            .lookup(composite(&self.prefix, 2, key.as_bytes()))
            .await?
            .map(|row| self.value(row))
            .transpose()
    }

    /// Resolve only this checkpoint's receipts. A live owner must first check
    /// later selected/committed/pending requests before interpreting absence.
    pub async fn resolve(
        &self,
        request: &str,
        fingerprint: [u8; 32],
        snapshot: u64,
    ) -> anyhow::Result<Resolution> {
        ensure!(
            (1..=128).contains(&request.len()),
            "invalid transaction request identity"
        );
        if let Some(row) = self
            .snapshot
            .lookup(composite(&self.prefix, 1, request.as_bytes()))
            .await?
        {
            let value = row.value.context("checkpoint receipt deleted")?;
            ensure!(
                value.len() == 32
                    && row.revision > self.receipt_floor
                    && row.revision <= self.revision,
                "invalid checkpoint receipt"
            );
            ensure!(
                value.as_ref() == fingerprint,
                "request identity reused for different contents"
            );
            return Ok(Resolution::Committed(row.revision));
        }
        ensure!(
            snapshot <= self.revision,
            "snapshot is ahead of checkpoint history"
        );
        Ok(if snapshot < self.receipt_floor {
            Resolution::Expired
        } else {
            Resolution::Missing
        })
    }

    /// Ordered half-open range, streaming one table index and data block at a
    /// time. Empty start means the first key; no end means this generation's end.
    pub fn scan(&self, start: &str, end: Option<&str>) -> anyhow::Result<Cursor> {
        ensure!(
            start.len() <= 1024 && end.is_none_or(|end| end.len() <= 1024 && start < end),
            "invalid checkpoint scan bounds"
        );
        let start = composite(&self.prefix, 2, start.as_bytes());
        let end = end.map_or_else(
            || composite(&self.prefix, 3, b""),
            |end| composite(&self.prefix, 2, end.as_bytes()),
        );
        Ok(Cursor {
            database: self.clone(),
            raw: self.snapshot.scan_raw(start, Some(end))?,
        })
    }

    pub(crate) async fn restore_state(
        &self,
        receipts: usize,
        bytes: usize,
    ) -> anyhow::Result<crate::transactions::recovery::State> {
        let count = self.revision - self.receipt_floor;
        ensure!(
            count <= receipts as u64 && count as u128 * 41 <= bytes as u128,
            "checkpoint receipt capacity exhausted"
        );
        let name_end = self.prefix.len() - 17;
        let database = Database {
            name: std::str::from_utf8(&self.prefix[..name_end])?,
            generation: uuid::Uuid::from_slice(&self.prefix[name_end + 1..])?,
        };
        let mut state = crate::transactions::recovery::State::from_checkpoint(
            database,
            self.revision,
            self.receipt_floor,
        )?;
        let mut cursor = self.snapshot.scan_raw(
            composite(&self.prefix, 1, b""),
            Some(composite(&self.prefix, 2, b"")),
        )?;
        let mut used = 0usize;
        while let Some(row) = cursor.next().await? {
            let request = std::str::from_utf8(&row.key[self.prefix.len() + 1..])?;
            let value = row.value.context("checkpoint receipt deleted")?;
            ensure!(
                value.len() == 32 && request.len() + 40 <= bytes - used,
                "checkpoint receipt capacity or length invalid"
            );
            used += request.len() + 40;
            state.load_receipt(row.revision, request, value.as_ref().try_into()?)?;
        }
        state.seal_checkpoint()?;
        Ok(state)
    }
    fn value(&self, mut row: Entry) -> anyhow::Result<Entry> {
        ensure!(
            row.key.starts_with(&self.prefix) && row.key.get(self.prefix.len()) == Some(&2),
            "checkpoint value belongs to another generation"
        );
        let key = &row.key[self.prefix.len() + 1..];
        ensure!(
            (1..=1024).contains(&key.len())
                && std::str::from_utf8(key).is_ok()
                && row.revision > 0
                && row.revision <= self.revision
                && row.value.as_ref().is_none_or(|v| v.len() <= 64 * 1024),
            "invalid checkpoint value"
        );
        row.key = Bytes::copy_from_slice(key);
        Ok(row)
    }
}

fn composite(prefix: &[u8], kind: u8, suffix: &[u8]) -> Bytes {
    let mut key = Vec::with_capacity(prefix.len() + 1 + suffix.len());
    key.extend_from_slice(prefix);
    key.push(kind);
    key.extend_from_slice(suffix);
    key.into()
}

/// Restart a scan after any error or canceled `next` call. A retired cursor
/// cannot skip an interrupted manifest node/table and report false absence.
pub struct Cursor {
    database: DatabaseSnapshot,
    raw: RawCursor,
}
impl Cursor {
    pub async fn next(&mut self) -> anyhow::Result<Option<Entry>> {
        let result = self
            .raw
            .next()
            .await?
            .map(|row| self.database.value(row))
            .transpose();
        if result.is_err() {
            self.raw.failed = true;
        }
        result
    }
}

pub(crate) struct RawCursor {
    snapshot: Snapshot,
    manifest: tree::Reader,
    full: bool,
    tables: u64,
    rows: u64,
    declared_rows: u64,
    databases: u64,
    start: Bytes,
    end: Option<Bytes>,
    current: Option<table::Cursor>,
    table_end: Option<Bytes>,
    previous: Option<Bytes>,
    done: bool,
    failed: bool,
}
impl RawCursor {
    pub async fn next(&mut self) -> anyhow::Result<Option<Entry>> {
        ensure!(!self.failed, "checkpoint scan retired");
        self.failed = true;
        let result = self.read_next().await?;
        self.failed = false;
        Ok(result)
    }
    async fn read_next(&mut self) -> anyhow::Result<Option<Entry>> {
        while !self.done {
            if let Some(cursor) = &mut self.current {
                if let Some(row) = cursor.next().await? {
                    ensure!(
                        self.previous.as_ref().is_none_or(|p| p < &row.key),
                        "unordered checkpoint scan"
                    );
                    self.previous = Some(row.key.clone());
                    self.rows = self
                        .rows
                        .checked_add(1)
                        .context("checkpoint row count overflow")?;
                    if self.full {
                        let (_, kind, _) = super::split_key(&row.key)?;
                        self.databases += u64::from(kind == 0);
                        ensure!(
                            self.rows <= self.snapshot.0.header.rows,
                            "extra checkpoint rows"
                        );
                    }
                    return Ok(Some(row));
                }
                self.current = None;
                if self
                    .end
                    .as_ref()
                    .is_some_and(|end| self.table_end.as_ref().is_some_and(|last| last >= end))
                {
                    self.done = true;
                    break;
                }
            }
            let Some(routed) = self.manifest.next().await? else {
                self.done = true;
                break;
            };
            let table = self.snapshot.table(routed).await?;
            self.tables = self
                .tables
                .checked_add(1)
                .context("checkpoint table count overflow")?;
            self.declared_rows = self
                .declared_rows
                .checked_add(table.row_count())
                .context("checkpoint row count overflow")?;
            if self.full {
                ensure!(
                    self.tables <= self.snapshot.0.header.tables
                        && self.declared_rows <= self.snapshot.0.header.rows,
                    "extra checkpoint tables or rows"
                );
            }
            ensure!(
                self.table_end
                    .as_ref()
                    .is_none_or(|last| last.as_ref() < table.first_key()),
                "overlapping checkpoint tables"
            );
            if self
                .end
                .as_ref()
                .is_some_and(|end| table.first_key() >= end.as_ref())
            {
                self.done = true;
                break;
            }
            self.table_end = Some(Bytes::copy_from_slice(table.last_key()));
            self.current = Some(table.scan(&self.start, self.end.as_deref(), u64::MAX)?);
        }
        if self.full {
            let header = &self.snapshot.0.header;
            ensure!(
                self.tables == header.tables
                    && self.rows == header.rows
                    && self.rows == self.declared_rows
                    && self.databases == header.databases,
                "checkpoint image counts mismatch"
            );
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
