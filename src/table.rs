//! Immutable sorted tables.
//!
//! ```text
//! [block]* [index] [footer]
//! block  = lz4( count:u32 | offset:u32 * count | entry* )
//! entry  = varint(key_len) varint(value_len << 1 | tombstone) varint(version) key value
//! index  = postcard(Vec<IndexEntry>)          last key, offset, length and CRC of each block
//! footer = index_offset:u64 index_len:u32 index_crc:u32 entries:u64 magic:[u8; 8]
//! ```
//!
//! A table is written once and never changed, so anything read from it can be
//! cached forever under its key.
use crate::{Error, Result, io::Io};
use bytes::{Buf, BufMut, Bytes};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};

const MAGIC: &[u8; 8] = b"DDBTBL02";
const FOOTER: usize = 8 + 4 + 4 + 8 + 8;
/// Small tables are fetched whole with the footer in one request.
const TAIL: u64 = 256 * 1024;

/// One key at one version. `value: None` records a deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Bytes,
    pub version: u64,
    pub value: Option<Bytes>,
}

impl Entry {
    pub fn weight(&self) -> usize {
        self.key.len() + self.value.as_ref().map_or(0, Bytes::len) + 16
    }
}

/// What the manifest records about a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
    pub key: String,
    pub size: u64,
    pub entries: u64,
    pub first: String,
    pub last: String,
    /// A merge writes its result as several tables of limited size, one
    /// after another in key order. This is the table's place among them;
    /// zero for the first, and for a table written alone.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub part: u32,
}

fn is_zero(part: &u32) -> bool {
    *part == 0
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry {
    last: Vec<u8>,
    offset: u64,
    len: u32,
    crc: u32,
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn get_varint(buf: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf.first()?;
        *buf = &buf[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(value);
        }
    }
    None
}

pub struct TableBuilder {
    out: Vec<u8>,
    block: Vec<u8>,
    offsets: Vec<u32>,
    index: Vec<IndexEntry>,
    entries: u64,
    first: Option<Vec<u8>>,
    last: Vec<u8>,
    block_bytes: usize,
}

impl TableBuilder {
    pub fn new(block_bytes: usize) -> Self {
        Self {
            out: Vec::new(),
            block: Vec::new(),
            offsets: Vec::new(),
            index: Vec::new(),
            entries: 0,
            first: None,
            last: Vec::new(),
            block_bytes,
        }
    }

    /// Keys must arrive in strictly ascending order.
    pub fn add(&mut self, entry: &Entry) {
        debug_assert!(self.first.is_none() || self.last.as_slice() < entry.key.as_ref());
        self.offsets.push(self.block.len() as u32);
        put_varint(&mut self.block, entry.key.len() as u64);
        match &entry.value {
            Some(value) => put_varint(&mut self.block, (value.len() as u64) << 1),
            None => put_varint(&mut self.block, 1),
        }
        put_varint(&mut self.block, entry.version);
        self.block.extend_from_slice(&entry.key);
        if let Some(value) = &entry.value {
            self.block.extend_from_slice(value);
        }
        if self.first.is_none() {
            self.first = Some(entry.key.to_vec());
        }
        self.last.clear();
        self.last.extend_from_slice(&entry.key);
        self.entries += 1;
        if self.block.len() >= self.block_bytes {
            self.finish_block();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn len(&self) -> usize {
        self.out.len() + self.block.len()
    }

    fn finish_block(&mut self) {
        if self.offsets.is_empty() {
            return;
        }
        let mut raw = Vec::with_capacity(4 + 4 * self.offsets.len() + self.block.len());
        raw.put_u32_le(self.offsets.len() as u32);
        for offset in &self.offsets {
            raw.put_u32_le(*offset);
        }
        raw.extend_from_slice(&self.block);
        let packed = lz4_flex::compress_prepend_size(&raw);
        self.index.push(IndexEntry {
            last: self.last.clone(),
            offset: self.out.len() as u64,
            len: packed.len() as u32,
            crc: crc32fast::hash(&packed),
        });
        self.out.extend_from_slice(&packed);
        self.block.clear();
        self.offsets.clear();
    }

    /// The finished object and its manifest record, to be stored under `key`.
    pub fn finish(mut self, key: String) -> (Bytes, TableMeta) {
        self.finish_block();
        let index = postcard::to_stdvec(&self.index).expect("index encodes");
        let index_offset = self.out.len() as u64;
        self.out.extend_from_slice(&index);
        self.out.put_u64_le(index_offset);
        self.out.put_u32_le(index.len() as u32);
        self.out.put_u32_le(crc32fast::hash(&index));
        self.out.put_u64_le(self.entries);
        self.out.extend_from_slice(MAGIC);
        let lossy = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
        let meta = TableMeta {
            key,
            size: self.out.len() as u64,
            entries: self.entries,
            first: lossy(self.first.as_deref().unwrap_or_default()),
            last: lossy(&self.last),
            part: 0,
        };
        (Bytes::from(self.out), meta)
    }
}

/// A decompressed block. Entries are slices of one shared buffer.
pub struct Block {
    data: Bytes,
    count: usize,
}

impl Block {
    fn decode(packed: &[u8], crc: u32) -> Result<Self> {
        if crc32fast::hash(packed) != crc {
            return Err(Error::Corrupt("table block checksum mismatch".into()));
        }
        let raw = lz4_flex::decompress_size_prepended(packed)
            .map_err(|e| Error::Corrupt(format!("table block: {e}")))?;
        let count = raw.get(..4).map(|mut b| b.get_u32_le() as usize);
        match count {
            Some(count) if raw.len() >= 4 + 4 * count => Ok(Self {
                data: Bytes::from(raw),
                count,
            }),
            _ => Err(Error::Corrupt("table block header".into())),
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn weight(&self) -> usize {
        self.data.len() + 64
    }

    fn at(&self, i: usize) -> (usize, usize, Option<usize>, u64) {
        let body = 4 + 4 * self.count;
        let offset = (&self.data[4 + 4 * i..]).get_u32_le() as usize;
        let mut cursor = &self.data[body + offset..];
        let before = cursor.len();
        // The block passed its checksum, so these fields are as written.
        let key_len = get_varint(&mut cursor).expect("valid block") as usize;
        let value = get_varint(&mut cursor).expect("valid block");
        let version = get_varint(&mut cursor).expect("valid block");
        let start = body + offset + (before - cursor.len());
        let value_len = (value & 1 == 0).then_some((value >> 1) as usize);
        (start, key_len, value_len, version)
    }

    pub fn key(&self, i: usize) -> &[u8] {
        let (start, key_len, _, _) = self.at(i);
        &self.data[start..start + key_len]
    }

    pub fn entry(&self, i: usize) -> Entry {
        let (start, key_len, value_len, version) = self.at(i);
        let value_start = start + key_len;
        Entry {
            key: self.data.slice(start..value_start),
            version,
            value: value_len.map(|len| self.data.slice(value_start..value_start + len)),
        }
    }

    /// The index of the first entry whose key is not less than `key`.
    pub fn lower_bound(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// Where each block of a table is, and the last key in it.
pub struct Index {
    blocks: Vec<IndexEntry>,
    weight: usize,
    /// Bytes of all indexes in memory; this one's are taken off when it goes.
    loaded: Arc<AtomicU64>,
    /// When a read last asked the memory cache for it, on the cache's clock.
    used: AtomicU64,
}

impl Index {
    fn new(blocks: Vec<IndexEntry>, loaded: &Arc<AtomicU64>) -> Self {
        let weight = blocks
            .iter()
            .map(|block| block.last.len() + std::mem::size_of::<IndexEntry>())
            .sum::<usize>()
            + 64;
        loaded.fetch_add(weight as u64, Ordering::Relaxed);
        Self {
            blocks,
            weight,
            loaded: loaded.clone(),
            used: AtomicU64::new(0),
        }
    }

    /// An index of this weight, of a table of no blocks.
    #[cfg(test)]
    pub(crate) fn weighing(weight: usize, loaded: &Arc<AtomicU64>) -> Self {
        loaded.fetch_add(weight as u64, Ordering::Relaxed);
        Self {
            blocks: Vec::new(),
            weight,
            loaded: loaded.clone(),
            used: AtomicU64::new(0),
        }
    }

    pub fn weight(&self) -> usize {
        self.weight
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn use_at(&self, now: u64) {
        self.used.store(now, Ordering::Relaxed);
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        self.loaded.fetch_sub(self.weight as u64, Ordering::Relaxed);
    }
}

pub struct Table {
    pub meta: TableMeta,
    key: Arc<str>,
    /// The index, while the cache or a reader holds it. A table does not
    /// keep its own: an index can be as large as the table, and the tables
    /// of every open database would hold theirs at once.
    index: parking_lot::Mutex<Weak<Index>>,
    loading: tokio::sync::Mutex<()>,
}

impl Table {
    pub fn new(meta: TableMeta) -> Arc<Self> {
        Arc::new(Self {
            key: Arc::from(meta.key.as_str()),
            meta,
            index: parking_lot::Mutex::new(Weak::new()),
            loading: tokio::sync::Mutex::new(()),
        })
    }

    /// Whether `key` can be in this table, from the manifest alone.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.meta.entries > 0
            && self.meta.first.as_bytes() <= key
            && key <= self.meta.last.as_bytes()
    }

    async fn index(&self, io: &Arc<Io>, keep: bool) -> Result<Arc<Index>> {
        // Asking the cache is also what tells it the index is in use.
        if let Some(index) = io.cached_index(&self.key) {
            return Ok(index);
        }
        // One reader fetches it; the others wait for that. A merge fetches
        // its own: it waits for a store that fails, and no request is to
        // wait for that.
        let _loading = match keep {
            true => Some(self.loading.lock().await),
            false => None,
        };
        let held = self.index.lock().upgrade();
        if let Some(index) = held {
            // Dropped by the cache while a reader still had it.
            io.cache_index(&self.key, index.clone());
            return Ok(index);
        }
        let index = match self.load_index(io, keep).await {
            // A damaged copy in the disk cache: discard it and read the
            // bucket's.
            Err(Error::Corrupt(_)) if io.forget(&self.key).await => self.load_index(io, keep).await,
            loaded => loaded,
        };
        let index = Arc::new(index?);
        *self.index.lock() = Arc::downgrade(&index);
        io.cache_index(&self.key, index.clone());
        Ok(index)
    }

    async fn load_index(&self, io: &Arc<Io>, keep: bool) -> Result<Index> {
        let size = self.meta.size;
        let tail_start = size.saturating_sub(TAIL);
        let tail = io.range(&self.key, size, tail_start, size, keep).await?;
        let corrupt = || {
            let object = crate::layout::object(&self.key);
            Error::Corrupt(format!("table {object} has a bad footer"))
        };
        if tail.len() < FOOTER || tail.len() as u64 != size - tail_start {
            return Err(corrupt());
        }
        let mut footer = &tail[tail.len() - FOOTER..];
        let index_offset = footer.get_u64_le();
        let index_len = footer.get_u32_le() as u64;
        let index_crc = footer.get_u32_le();
        let _entries = footer.get_u64_le();
        let index_end = index_offset.checked_add(index_len);
        if footer != MAGIC || index_end.and_then(|end| end.checked_add(FOOTER as u64)) != Some(size)
        {
            return Err(corrupt());
        }
        let bytes = if index_offset >= tail_start {
            let start = (index_offset - tail_start) as usize;
            tail.slice(start..start + index_len as usize)
        } else {
            io.range(
                &self.key,
                size,
                index_offset,
                index_offset + index_len,
                keep,
            )
            .await?
        };
        if crc32fast::hash(&bytes) != index_crc {
            return Err(corrupt());
        }
        let blocks: Vec<IndexEntry> = postcard::from_bytes(&bytes).map_err(|_| corrupt())?;
        // Blocks that arrived with the footer need no second request.
        for (i, block) in blocks.iter().enumerate() {
            if block.offset >= tail_start {
                let start = (block.offset - tail_start) as usize;
                let packed = tail
                    .get(start..start + block.len as usize)
                    .ok_or_else(corrupt)?;
                io.cache_block(
                    &self.key,
                    i as u32,
                    Arc::new(Block::decode(packed, block.crc)?),
                );
            }
        }
        Ok(Index::new(blocks, &io.index_bytes))
    }

    async fn block(&self, io: &Arc<Io>, index: &Index, i: usize) -> Result<Arc<Block>> {
        if let Some(block) = io.cached_block(&self.key, i as u32) {
            return Ok(block);
        }
        let entry = &index.blocks[i];
        let (start, end) = (entry.offset, entry.offset + u64::from(entry.len));
        let packed = io
            .range(&self.key, self.meta.size, start, end, true)
            .await?;
        let block = match Block::decode(&packed, entry.crc) {
            Err(Error::Corrupt(_)) if io.forget(&self.key).await => {
                let packed = io
                    .range(&self.key, self.meta.size, start, end, true)
                    .await?;
                Block::decode(&packed, entry.crc)?
            }
            decoded => decoded?,
        };
        let block = Arc::new(block);
        io.cache_block(&self.key, i as u32, block.clone());
        Ok(block)
    }

    /// The entry for `key`, which may be a tombstone.
    pub async fn get(&self, io: &Arc<Io>, key: &[u8]) -> Result<Option<Entry>> {
        if !self.covers(key) {
            return Ok(None);
        }
        let index = self.index(io, true).await?;
        let i = index
            .blocks
            .partition_point(|block| block.last.as_slice() < key);
        if i == index.blocks.len() {
            return Ok(None);
        }
        let block = self.block(io, &index, i).await?;
        let at = block.lower_bound(key);
        Ok((at < block.len() && block.key(at) == key).then(|| block.entry(at)))
    }

    /// Entries with `lo <= key < hi` (an empty `hi` is unbounded), in either
    /// direction. With `keep`, what the scan reads is kept for later
    /// readers: its blocks in memory, and the table in the disk cache when
    /// that has room. A merge reads each of its tables once and keeps none.
    pub fn scan(
        self: &Arc<Self>,
        io: Arc<Io>,
        lo: Bytes,
        hi: Bytes,
        reverse: bool,
        keep: bool,
    ) -> TableScan {
        TableScan {
            table: self.clone(),
            io,
            lo,
            hi,
            reverse,
            keep,
            index: None,
            block: None,
            started: false,
            ahead: None,
            span: 1,
        }
    }
}

/// A scan reads ahead of itself. Its first read of a table is one block,
/// and each read after that asks for twice as many blocks as the last, up
/// to these and to the end of what the scan selects. A scan that ends in
/// its first block has read no more than that; one that goes through a
/// table of 32 MiB asks for it in under forty requests and not a thousand.
///
/// What was read ahead is in memory until the scan has come to it, so a
/// scan holds one of a number of turns meanwhile: `AHEAD_MERGES` for the
/// runs of tables that merges read, and `AHEAD_SCANS` for the scans of
/// requests, which read ahead only in a table that is not in the disk
/// cache, where every read is a request to the bucket. A scan that has no
/// turn reads a block at a time.
const AHEAD_BLOCKS: usize = 64;
const AHEAD_BYTES: u64 = 1 << 20;

/// Blocks that were read with one the scan needed and that it has not come
/// to yet, as they are stored.
struct Ahead {
    /// The number of the first, and how many.
    first: usize,
    count: usize,
    bytes: Bytes,
    /// The turn to read ahead, held for as long as this is in memory.
    _turn: tokio::sync::OwnedSemaphorePermit,
}

impl Ahead {
    fn holds(&self, i: usize) -> bool {
        (self.first..self.first + self.count).contains(&i)
    }
}

pub struct TableScan {
    table: Arc<Table>,
    io: Arc<Io>,
    lo: Bytes,
    hi: Bytes,
    reverse: bool,
    keep: bool,
    /// Held from the first entry to the last.
    index: Option<Arc<Index>>,
    /// The current block, its number, and the next position within it
    /// (for a reverse scan, one past the next entry).
    block: Option<(Arc<Block>, usize, usize)>,
    started: bool,
    ahead: Option<Ahead>,
    /// How many blocks the next read asks for.
    span: usize,
}

impl TableScan {
    /// Give up what was read ahead and the turn held for it, as a merge
    /// does while it waits for another. The scan reads those blocks again
    /// when it comes to them.
    pub fn rest(&mut self) {
        self.ahead = None;
    }

    pub async fn next(&mut self) -> Result<Option<Entry>> {
        if self.table.meta.entries == 0 {
            return Ok(None);
        }
        if self.index.is_none() {
            self.index = Some(self.table.index(&self.io, self.keep).await?);
        }
        let blocks = self.index.as_ref().expect("just loaded").blocks.len();
        if !self.started {
            self.started = true;
            let index = self.index.as_ref().expect("just loaded");
            if self.reverse {
                // The last block that can hold a key below `hi`.
                let i = if self.hi.is_empty() {
                    blocks - 1
                } else {
                    index
                        .blocks
                        .partition_point(|b| b.last.as_slice() < self.hi.as_ref())
                        .min(blocks - 1)
                };
                let block = self.load(i).await?;
                let position = if self.hi.is_empty() {
                    block.len()
                } else {
                    block.lower_bound(&self.hi)
                };
                self.block = Some((block, i, position));
            } else {
                let i = index
                    .blocks
                    .partition_point(|b| b.last.as_slice() < self.lo.as_ref());
                if i == blocks {
                    return Ok(None);
                }
                let block = self.load(i).await?;
                let position = block.lower_bound(&self.lo);
                self.block = Some((block, i, position));
            }
        }
        loop {
            let Some((block, i, position)) = &mut self.block else {
                return Ok(None);
            };
            if self.reverse {
                if *position > 0 {
                    *position -= 1;
                    let entry = block.entry(*position);
                    if entry.key < self.lo {
                        self.block = None;
                        return Ok(None);
                    }
                    return Ok(Some(entry));
                }
                if *i == 0 {
                    self.block = None;
                    return Ok(None);
                }
                let previous = *i - 1;
                // The index says when the block before holds nothing the
                // scan selects: it is not read to find that out.
                let index = self.index.as_ref().expect("loaded before any block");
                if index.blocks[previous].last.as_slice() < self.lo.as_ref() {
                    self.block = None;
                    return Ok(None);
                }
                let block = self.load(previous).await?;
                let position = block.len();
                self.block = Some((block, previous, position));
            } else {
                if *position < block.len() {
                    let entry = block.entry(*position);
                    *position += 1;
                    if !self.hi.is_empty() && entry.key >= self.hi {
                        self.block = None;
                        return Ok(None);
                    }
                    return Ok(Some(entry));
                }
                let next = *i + 1;
                if next == blocks {
                    self.block = None;
                    return Ok(None);
                }
                let block = self.load(next).await?;
                self.block = Some((block, next, 0));
            }
        }
    }

    /// Block `i`: from memory, from what was read ahead, or from a read of
    /// it and of the blocks the scan comes to after it.
    async fn load(&mut self, i: usize) -> Result<Arc<Block>> {
        let block = self.find(i).await;
        // What was read ahead leaves memory, and its turn is another
        // scan's, when the block the scan comes to next is not in it.
        let next = if self.reverse {
            i.checked_sub(1)
        } else {
            Some(i + 1)
        };
        if let Some(ahead) = &self.ahead
            && !next.is_some_and(|next| ahead.holds(next))
        {
            self.ahead = None;
        }
        block
    }

    /// Block `i`, and what was read with it kept for the blocks after it.
    async fn find(&mut self, i: usize) -> Result<Arc<Block>> {
        let table = self.table.clone();
        if let Some(block) = self.io.cached_block(&table.key, i as u32) {
            return Ok(block);
        }
        let index = self.index.clone().expect("loaded before any block");
        let entry = &index.blocks[i];
        let mut again = false;
        loop {
            let read = match &self.ahead {
                Some(ahead) if ahead.holds(i) => (ahead.first, ahead.bytes.clone()),
                _ => {
                    let turn = self.turn();
                    let (first, last) = match turn {
                        Some(_) => self.reach(&index, i),
                        None => (i, i),
                    };
                    let (from, to) = (&index.blocks[first], &index.blocks[last]);
                    let (start, end) = (from.offset, to.offset + u64::from(to.len));
                    let bytes = self
                        .io
                        .range(&table.key, table.meta.size, start, end, self.keep)
                        .await?;
                    if let Some(turn) = turn {
                        self.ahead = Some(Ahead {
                            first,
                            count: last - first + 1,
                            bytes: bytes.clone(),
                            _turn: turn,
                        });
                    }
                    self.span = (self.span * 2).min(AHEAD_BLOCKS);
                    (first, bytes)
                }
            };
            let (first, bytes) = read;
            let at = (entry.offset - index.blocks[first].offset) as usize;
            let block = bytes
                .get(at..at + entry.len as usize)
                .ok_or_else(|| Error::Corrupt("table block is cut short".into()))
                .and_then(|packed| Block::decode(packed, entry.crc));
            match block {
                // A damaged copy in the disk cache: discard it and read
                // the bucket's.
                Err(Error::Corrupt(_)) if !again && self.io.forget(&table.key).await => {
                    self.ahead = None;
                    again = true;
                }
                block => {
                    let block = Arc::new(block?);
                    if self.keep {
                        self.io.cache_block(&table.key, i as u32, block.clone());
                    }
                    return Ok(block);
                }
            }
        }
    }

    /// A turn to read more than one block, if the next read may be of
    /// more. A merge takes one of the merges' turns. A request's scan
    /// takes one of the requests', and only for a table that is not in
    /// the disk cache.
    fn turn(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if self.span < 2 {
            return None;
        }
        let turns = if !self.keep {
            &self.io.merge_ahead
        } else if self.io.local(&self.table.key) {
            return None;
        } else {
            &self.io.ahead
        };
        turns.clone().try_acquire_owned().ok()
    }

    /// The blocks to read with block `i`, which is the first of them for a
    /// scan forward and the last for one in reverse.
    fn reach(&self, index: &Index, i: usize) -> (usize, usize) {
        let blocks = &index.blocks;
        let bytes =
            |a: usize, b: usize| blocks[b].offset + u64::from(blocks[b].len) - blocks[a].offset;
        if self.reverse {
            // The first block that can hold a key the scan selects.
            let stop = blocks
                .partition_point(|b| b.last.as_slice() < self.lo.as_ref())
                .min(i);
            let mut first = i.saturating_sub(self.span - 1).max(stop);
            while bytes(first, i) > AHEAD_BYTES && first < i {
                first += 1;
            }
            (first, i)
        } else {
            // The last block that can.
            let stop = if self.hi.is_empty() {
                blocks.len() - 1
            } else {
                blocks
                    .partition_point(|b| b.last.as_slice() < self.hi.as_ref())
                    .clamp(i, blocks.len() - 1)
            };
            let mut last = (i + self.span - 1).min(stop);
            while bytes(i, last) > AHEAD_BYTES && last > i {
                last -= 1;
            }
            (i, last)
        }
    }
}
