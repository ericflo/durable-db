//! Lazy reads of one immutable checkpoint. No database registry, values or
//! receipt window is materialized. Callers must separately retain the objects,
//! merge the selected/live suffix, authorize tenants and check serving authority.
use super::{Reference, tree};
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
    header: Reference,
}

/// Data at an exact checkpoint, not a current database snapshot. Constructing
/// this handle performs no I/O; immutable pages are validated on demand.
#[derive(Clone)]
pub struct Snapshot(Arc<Inner>);
impl Snapshot {
    pub(super) fn header(&self) -> &Reference {
        &self.0.header
    }
    pub(crate) fn cut(&self) -> crate::ownership::root::Cut {
        self.0.header.cut
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
        self.scan_raw_ordered(start, end, false)
    }
    fn scan_raw_ordered(
        &self,
        start: Bytes,
        end: Option<Bytes>,
        reverse: bool,
    ) -> anyhow::Result<RawCursor> {
        Ok(RawCursor {
            snapshot: self.clone(),
            manifest: if reverse {
                tree::Reader::seek_reverse(
                    self.0.files.clone(),
                    self.0.header.tree,
                    end.clone(),
                    self.0.cache.clone(),
                )?
            } else {
                self.seek(start.clone())?
            },
            start,
            end,
            current: None,
            table_boundary: None,
            previous: None,
            done: false,
            failed: false,
            reverse,
        })
    }

    pub fn new(files: Arc<dyn Storage>, cache: Arc<Cache>, reference: Reference) -> Self {
        Self(Arc::new(Inner {
            files,
            cache,
            header: reference,
        }))
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
        let (state, statistics) = super::metadata(&row, database)?;
        Ok(Some(DatabaseSnapshot {
            statistics,
            snapshot: self.clone(),
            prefix: prefix.into(),
            revision: state.revision(),
            receipt_floor: state.checkpoint_floor(),
        }))
    }

    /// Conservative copy contribution, derived only from authenticated manifest
    /// descriptors. A changed metadata/receipt interval and each mutation key
    /// can replace its routed base table, including any neighboring rows.
    pub(super) async fn rewrite_bytes(
        &self,
        database: Database<'_>,
        mutations: &[crate::transactions::record::Mutation<'_>],
    ) -> anyhow::Result<u64> {
        let mut prefix = database.name.as_bytes().to_vec();
        prefix.push(0);
        prefix.extend_from_slice(database.generation.as_bytes());
        let mut ranges = vec![(composite(&prefix, 0, b""), composite(&prefix, 2, b""))];
        ranges.extend(mutations.iter().map(|m| {
            let key = composite(&prefix, 2, m.key.as_bytes());
            (key.clone(), key)
        }));
        ranges.sort_unstable();
        let mut charged_through: Option<Bytes> = None;
        let mut bytes = 0u64;
        for (start, end) in ranges {
            if charged_through.as_ref().is_some_and(|last| last >= &end) {
                continue;
            }
            let mut manifest = self.seek(start)?;
            let mut previous = None;
            while let Some(routed) = manifest.next().await? {
                ensure!(
                    previous.as_ref().is_none_or(|last| last < &routed.last),
                    "unordered checkpoint admission bounds"
                );
                previous = Some(routed.last.clone());
                if charged_through
                    .as_ref()
                    .is_none_or(|last| last < &routed.last)
                {
                    bytes = bytes
                        .checked_add(routed.reference.blobs()[0].size())
                        .context("checkpoint copy admission overflow")?;
                    charged_through = Some(routed.last.clone());
                }
                if routed.last >= end {
                    break;
                }
            }
        }
        Ok(bytes)
    }

    pub(super) fn seek(&self, key: Bytes) -> anyhow::Result<tree::Reader> {
        tree::Reader::seek(
            self.0.files.clone(),
            self.0.header.tree,
            key,
            self.0.cache.clone(),
        )
    }
    pub(super) async fn table(&self, routed: &tree::RoutedTable) -> anyhow::Result<Table> {
        let table = Table::open(
            self.0.files.clone(),
            self.0.cache.clone(),
            routed.reference.clone(),
        )
        .await?;
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
        self.table(&routed).await?.get(&key, u64::MAX).await
    }
}

/// One generation at its recorded checkpoint revision. Retained receipt
/// resolution and value reads do not consult a mutable root or write storage.
#[derive(Clone)]
pub struct DatabaseSnapshot {
    statistics: crate::index::recovery::Statistics,
    snapshot: Snapshot,
    prefix: Bytes,
    revision: u64,
    receipt_floor: u64,
}
impl DatabaseSnapshot {
    pub fn statistics(&self) -> crate::index::recovery::Statistics {
        self.statistics
    }
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
            return Ok(if value.as_ref() == fingerprint {
                Resolution::Committed(row.revision)
            } else {
                Resolution::Reused
            });
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
        self.scan_ordered(start, end, false)
    }
    pub(crate) fn scan_ordered(
        &self,
        start: &str,
        end: Option<&str>,
        reverse: bool,
    ) -> anyhow::Result<Cursor> {
        ensure!(
            start.len() <= 1024 && end.is_none_or(|end| end.len() <= 1025 && start < end),
            "invalid checkpoint scan bounds"
        );
        let start = composite(&self.prefix, 2, start.as_bytes());
        let end = end.map_or_else(
            || composite(&self.prefix, 3, b""),
            |end| composite(&self.prefix, 2, end.as_bytes()),
        );
        Ok(Cursor {
            database: self.clone(),
            raw: self.snapshot.scan_raw_ordered(start, Some(end), reverse)?,
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

struct RawCursor {
    snapshot: Snapshot,
    manifest: tree::Reader,
    start: Bytes,
    end: Option<Bytes>,
    current: Option<table::Cursor>,
    table_boundary: Option<Bytes>,
    previous: Option<Bytes>,
    done: bool,
    failed: bool,
    reverse: bool,
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
                        self.previous.as_ref().is_none_or(|p| if self.reverse {
                            p > &row.key
                        } else {
                            p < &row.key
                        }),
                        "unordered checkpoint scan"
                    );
                    self.previous = Some(row.key.clone());
                    return Ok(Some(row));
                }
                self.current = None;
                if if self.reverse {
                    self.table_boundary
                        .as_ref()
                        .is_some_and(|first| first <= &self.start)
                } else {
                    self.end.as_ref().is_some_and(|end| {
                        self.table_boundary.as_ref().is_some_and(|last| last >= end)
                    })
                } {
                    self.done = true;
                    break;
                }
            }
            let Some(routed) = self.manifest.next().await? else {
                self.done = true;
                break;
            };
            let table = self.snapshot.table(&routed).await?;
            ensure!(
                self.table_boundary
                    .as_ref()
                    .is_none_or(|boundary| if self.reverse {
                        boundary.as_ref() > table.last_key()
                    } else {
                        boundary.as_ref() < table.first_key()
                    }),
                "overlapping checkpoint tables"
            );
            if if self.reverse {
                table.last_key() < self.start.as_ref()
            } else {
                self.end
                    .as_ref()
                    .is_some_and(|end| table.first_key() >= end.as_ref())
            } {
                self.done = true;
                break;
            }
            self.table_boundary = Some(Bytes::copy_from_slice(if self.reverse {
                table.first_key()
            } else {
                table.last_key()
            }));
            self.current = Some(table.scan_ordered(
                &self.start,
                self.end.as_deref(),
                u64::MAX,
                self.reverse,
            )?);
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
