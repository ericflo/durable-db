//! Immutable revisioned sorted tables for the native index. One checksummed
//! index object authenticates bounded data blocks. A table is not a checkpoint:
//! its reference still needs durable publication, accounting and retention.
mod codec;
#[cfg(test)]
mod tests;

use crate::{
    cache::Cache,
    storage::{
        Storage,
        immutable::{Blob, MAX_BLOB_BYTES},
    },
};
use anyhow::{Context, ensure};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, sync::Arc};

const BLOCK_TARGET: usize = 64 * 1024;
const MAX_BLOCK_BYTES: usize = 128 * 1024;
const MAX_BLOCK_ROWS: usize = 4096;
const MAX_BLOCKS: usize = 1024;
const MAX_INDEX_BYTES: usize = 8 * 1024 * 1024;
use super::MAX_KEY_BYTES;

/// Input must be strictly ordered by key ascending, then revision descending.
/// A tombstone is distinct from an absent row and must reach the merge caller.
pub struct Row<'a> {
    pub key: &'a [u8],
    pub revision: u64,
    pub value: Option<&'a [u8]>,
}
pub struct Entry {
    pub key: Bytes,
    pub revision: u64,
    pub value: Option<Bytes>,
}
#[derive(Clone)]
struct Boundary {
    key: Bytes,
    revision: u64,
}
impl Boundary {
    fn compare(&self, key: &[u8], revision: u64) -> Ordering {
        self.key
            .as_ref()
            .cmp(key)
            .then_with(|| revision.cmp(&self.revision))
    }
    fn of(row: &Row<'_>) -> Self {
        Self {
            key: Bytes::copy_from_slice(row.key),
            revision: row.revision,
        }
    }
}
pub(crate) fn encoded_row_len(key_bytes: usize, value_bytes: usize) -> usize {
    14 + key_bytes + value_bytes
}

/// Upper bound for a validated row's data/index encoding: place it alone in
/// one table. A block and index header cost 12 bytes each, the two indexed
/// boundaries cost 10 + key bytes each, and extent/count/digest cost 48.
/// Summing these singleton costs covers any packing of these rows, but not
/// reblocking other retained rows or manifest nodes.
pub(crate) fn row_upload_bound(key_bytes: usize, value_bytes: usize) -> usize {
    encoded_row_len(key_bytes, value_bytes) + 92 + 2 * key_bytes
}

struct Block {
    first: Boundary,
    last: Boundary,
    offset: u64,
    length: u32,
    rows: u32,
    digest: [u8; 32],
}
/// Contains only immutable identities and digests. It authorizes no reads,
/// publication or collection. Decode only from a validated selected manifest.
#[derive(Clone)]
pub struct Reference {
    data: Blob,
    index: Blob,
}
impl Reference {
    pub(crate) fn blobs(&self) -> [Blob; 2] {
        [self.data, self.index]
    }
    pub fn encode(&self) -> Bytes {
        codec::reference(self)
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        codec::decode_reference(bytes)
    }
}

/// Build at most 64 MiB of data plus 8 MiB of index before uploading either.
/// The two fresh PUTs overlap; a reference requires both successful replies.
/// Errors or cancellation can strand either object, so retain both credits.
pub async fn build<'a>(
    storage: Arc<dyn Storage>,
    rows: impl IntoIterator<Item = Row<'a>>,
) -> anyhow::Result<Reference> {
    let mut data = Vec::new();
    let mut blocks = Vec::new();
    let mut pending = Vec::new();
    let mut first = None;
    let mut last: Option<Boundary> = None;
    let mut count = 0usize;
    for row in rows {
        codec::validate_row(&row)?;
        if let Some(last) = &last {
            ensure!(
                last.compare(row.key, row.revision).is_lt(),
                "table rows are not strictly ordered"
            );
        }
        let length = encoded_row_len(row.key.len(), row.value.map_or(0, <[u8]>::len));
        if count > 0 && (pending.len() + length > BLOCK_TARGET || count == MAX_BLOCK_ROWS) {
            codec::finish_block(
                &mut data,
                &mut blocks,
                &mut pending,
                first.take().unwrap(),
                last.clone().unwrap(),
                count,
            )?;
            count = 0;
        }
        if count == 0 {
            first = Some(Boundary::of(&row));
        }
        ensure!(
            data.len() + pending.len() + length + 12 <= MAX_BLOB_BYTES,
            "table data size limit exceeded"
        );
        codec::write_row(&row, &mut pending);
        last = Some(Boundary::of(&row));
        count += 1;
    }
    ensure!(count > 0, "empty table");
    codec::finish_block(
        &mut data,
        &mut blocks,
        &mut pending,
        first.unwrap(),
        last.unwrap(),
        count,
    )?;
    let index = codec::index(&blocks);
    ensure!(
        index.len() <= MAX_INDEX_BYTES,
        "table index size limit exceeded"
    );
    let (data, index) = tokio::try_join!(
        Blob::upload(storage.clone(), data.into()),
        Blob::upload(storage, index),
    )?;
    Ok(Reference { data, index })
}
struct Inner {
    storage: Arc<dyn Storage>,
    cache: Arc<Cache>,
    data: Blob,
    blocks: Vec<Block>,
}
#[derive(Clone)]
pub struct Table(Arc<Inner>);
impl Table {
    pub(crate) fn first_key(&self) -> &[u8] {
        &self
            .0
            .blocks
            .first()
            .expect("nonempty table index")
            .first
            .key
    }
    pub(crate) fn last_key(&self) -> &[u8] {
        &self.0.blocks.last().expect("nonempty table index").last.key
    }
    pub(crate) fn row_count(&self) -> u64 {
        self.0
            .blocks
            .iter()
            .map(|block| u64::from(block.rows))
            .sum()
    }
    pub async fn open(
        storage: Arc<dyn Storage>,
        cache: Arc<Cache>,
        reference: Reference,
    ) -> anyhow::Result<Self> {
        ensure!(
            reference.index.size() <= MAX_INDEX_BYTES as u64,
            "table index size limit exceeded"
        );
        let bytes = reference
            .index
            .read_cached(storage.as_ref(), cache.as_ref())
            .await?;
        let blocks = codec::decode_index(&bytes, reference.data.size())?;
        Ok(Self(Arc::new(Inner {
            storage,
            cache,
            data: reference.data,
            blocks,
        })))
    }
    fn seek(&self, key: &[u8], revision: u64) -> usize {
        self.0
            .blocks
            .partition_point(|block| block.last.compare(key, revision).is_lt())
    }
    async fn block_bytes(&self, number: usize) -> anyhow::Result<Bytes> {
        let block = self
            .0
            .blocks
            .get(number)
            .context("table block out of bounds")?;
        let digest = hex::encode(block.digest);
        let key = format!("native-table/{digest}");
        let bytes = if let Some(bytes) = self.0.cache.get(&key, &digest).await {
            bytes
        } else {
            let bytes = self
                .0
                .data
                .read_range(
                    self.0.storage.as_ref(),
                    block.offset..block.offset + u64::from(block.length),
                )
                .await?;
            ensure!(
                Sha256::digest(&bytes).as_slice() == block.digest,
                "table block digest mismatch"
            );
            self.0.cache.insert(&key, bytes.clone()).await;
            bytes
        };
        Ok(bytes)
    }
    async fn block(&self, number: usize) -> anyhow::Result<Vec<Entry>> {
        let bytes = self.block_bytes(number).await?;
        codec::decode_block(&bytes, &self.0.blocks[number])
    }
    /// Returns the latest row visible at this revision, including deletion.
    /// None means no visible row in THIS table, not global key absence.
    pub async fn get(&self, key: &[u8], revision: u64) -> anyhow::Result<Option<Entry>> {
        super::validate_key(key)?;
        if revision == 0 {
            return Ok(None);
        }
        let number = self.seek(key, revision);
        let Some(block) = self.0.blocks.get(number) else {
            return Ok(None);
        };
        if block.first.key.as_ref() > key {
            return Ok(None);
        }
        let bytes = self.block_bytes(number).await?;
        codec::lookup_block(&bytes, block, key, revision)
    }
    /// One bounded block at a time. Emits tombstones so a table merge cannot
    /// resurrect an older value. Snapshot retention and authority are external.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, revision: u64) -> anyhow::Result<Cursor> {
        self.scan_ordered(start, end, revision, false)
    }
    pub(crate) fn scan_ordered(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
        reverse: bool,
    ) -> anyhow::Result<Cursor> {
        super::validate_range(start, end)?;
        Ok(Cursor {
            table: self.clone(),
            next_block: if reverse {
                self.0
                    .blocks
                    .partition_point(|b| end.is_none_or(|end| b.first.key.as_ref() < end))
            } else {
                self.seek(start, revision)
            },
            rows: Vec::new().into_iter(),
            start: Bytes::copy_from_slice(start),
            end: end.map(Bytes::copy_from_slice),
            revision,
            previous: None,
            done: revision == 0,
            reverse,
            pending: None,
        })
    }
}

pub struct Cursor {
    table: Table,
    next_block: usize,
    rows: std::vec::IntoIter<Entry>,
    start: Bytes,
    end: Option<Bytes>,
    revision: u64,
    previous: Option<Bytes>,
    done: bool,
    reverse: bool,
    pending: Option<Entry>,
}
impl Cursor {
    pub async fn next(&mut self) -> anyhow::Result<Option<Entry>> {
        loop {
            if self.done {
                return Ok(self.pending.take());
            }
            if self.reverse {
                // Reverse physical order encounters a key's oldest revision
                // first. Keep its winner until the entire key group is known,
                // including versions in the preceding block.
                let next_key = self.rows.as_slice().last().map(|r| &r.key).or_else(|| {
                    self.next_block
                        .checked_sub(1)
                        .map(|i| &self.table.0.blocks[i].last.key)
                });
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|p| next_key.is_none_or(|key| key < &p.key))
                {
                    return Ok(self.pending.take());
                }
            }
            let row = if self.reverse {
                self.rows.next_back()
            } else {
                self.rows.next()
            };
            if let Some(row) = row {
                if self.reverse {
                    if row.key < self.start {
                        self.done = true;
                        return Ok(self.pending.take());
                    }
                    if row.revision <= self.revision
                        && self.end.as_ref().is_none_or(|end| row.key < *end)
                    {
                        self.pending = Some(row);
                    }
                    continue;
                }
                if self.end.as_ref().is_some_and(|end| row.key >= *end) {
                    self.done = true;
                    return Ok(None);
                }
                if row.key < self.start
                    || self.previous.as_ref() == Some(&row.key)
                    || row.revision > self.revision
                {
                    continue;
                }
                self.previous = Some(row.key.clone());
                return Ok(Some(row));
            }
            if if self.reverse {
                self.next_block == 0
            } else {
                self.next_block == self.table.0.blocks.len()
            } {
                self.done = true;
                return Ok(self.pending.take());
            }
            // Advance only after a complete validated block. A failed read can
            // be retried and must never become a false end-of-table result.
            let number = if self.reverse {
                self.next_block - 1
            } else {
                self.next_block
            };
            self.rows = self.table.block(number).await?.into_iter();
            self.next_block = if self.reverse { number } else { number + 1 };
        }
    }
}
