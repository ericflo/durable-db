//! Snapshot reads over overlapping immutable tables. The caller supplies the
//! complete, retained source set; these readers do not establish ownership.
//! Deletions remain rows until the complete read view can suppress them.
use super::{
    memory,
    table::{self, Entry, Table},
};
use anyhow::ensure;
use futures::{StreamExt, TryStreamExt, stream};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
};

/// Per-read fan-in bound, not a bound on databases or stored tables. A future
/// manifest/compactor must keep overlapping runs within the admitted budget.
pub const MAX_SOURCES: usize = 64;
const POINT_CONCURRENCY: usize = 8;

/// Consult every source before returning absence, a value or a deletion. Reads
/// are bounded in flight; an unavailable source always fails the whole lookup.
pub async fn get(tables: &[Table], key: &[u8], revision: u64) -> anyhow::Result<Option<Entry>> {
    lookup(tables, None, key, revision).await
}

/// The memory view and table references must belong to the same database
/// generation and retained snapshot. Source identity is a manifest obligation.
pub async fn get_with_memory(
    tables: &[Table],
    memory: &memory::Memory,
    key: &[u8],
    revision: u64,
) -> anyhow::Result<Option<Entry>> {
    lookup(tables, Some(memory), key, revision).await
}

async fn lookup(
    tables: &[Table],
    memory: Option<&memory::Memory>,
    key: &[u8],
    revision: u64,
) -> anyhow::Result<Option<Entry>> {
    super::validate_key(key)?;
    ensure!(
        tables.len() <= MAX_SOURCES - usize::from(memory.is_some()),
        "index read fan-in exceeded"
    );
    let mut reads = stream::iter(tables.iter().map(|table| table.get(key, revision)))
        .buffer_unordered(POINT_CONCURRENCY);
    let mut winner = memory.map(|m| m.get(key, revision)).transpose()?.flatten();
    let mut conflict = false;
    while let Some(row) = reads.try_next().await? {
        let Some(row) = row else { continue };
        match winner.as_ref().map(|w| row.revision.cmp(&w.revision)) {
            None | Some(Ordering::Greater) => {
                winner = Some(row);
                conflict = false;
            }
            Some(Ordering::Equal) => {
                conflict |= winner.as_ref().unwrap().value != row.value;
            }
            Some(Ordering::Less) => (),
        }
    }
    ensure!(!conflict, "conflicting index rows at the same revision");
    Ok(winner)
}

struct Head {
    source: usize,
    row: Entry,
    reverse: bool,
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap pops the next key in scan order, highest revision first.
        (if self.reverse {
            self.row.key.cmp(&other.row.key)
        } else {
            other.row.key.cmp(&self.row.key)
        })
        .then_with(|| self.row.revision.cmp(&other.row.revision))
        .then_with(|| self.source.cmp(&other.source))
    }
}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Head {}

/// One decoded block and one frontier row per source. Every source uses the
/// same range and snapshot. Table/memory reads can retry interrupted I/O on
/// this cursor; checkpoint-backed scans must restart after interruption.
/// Contradictory visible rows retire any merge permanently.
pub struct Cursor<'a> {
    sources: Vec<Source<'a>>,
    pending: VecDeque<usize>,
    heads: BinaryHeap<Head>,
    failed: bool,
    reverse: bool,
}
enum Source<'a> {
    Table(table::Cursor),
    Memory(memory::Cursor<'a>),
    Checkpoint(Box<super::recovery::checkpoint::Cursor>),
}
impl Source<'_> {
    async fn next(&mut self) -> anyhow::Result<Option<Entry>> {
        match self {
            Self::Table(cursor) => cursor.next().await,
            Self::Memory(cursor) => Ok(cursor.next()),
            Self::Checkpoint(cursor) => cursor.next().await,
        }
    }
}
impl<'a> Cursor<'a> {
    pub(crate) fn with_checkpoint(
        base: Option<&super::recovery::checkpoint::DatabaseSnapshot>,
        memory: Option<&'a memory::Memory>,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
        reverse: bool,
    ) -> anyhow::Result<Self> {
        let mut cursor = Self::create(&[], memory, start, end, revision, reverse)?;
        if let Some(base) = base {
            ensure!(revision >= base.revision(), "snapshot predates checkpoint");
            let source = cursor.sources.len();
            cursor
                .sources
                .push(Source::Checkpoint(Box::new(base.scan_ordered(
                    std::str::from_utf8(start)?,
                    end.map(std::str::from_utf8).transpose()?,
                    reverse,
                )?)));
            cursor.pending.push_back(source);
        }
        Ok(cursor)
    }
    pub fn new(
        tables: &[Table],
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<Self> {
        Self::create(tables, None, start, end, revision, false)
    }

    pub fn with_memory(
        tables: &[Table],
        memory: &'a memory::Memory,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<Self> {
        Self::create(tables, Some(memory), start, end, revision, false)
    }

    fn create(
        tables: &[Table],
        memory: Option<&'a memory::Memory>,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
        reverse: bool,
    ) -> anyhow::Result<Self> {
        super::validate_range(start, end)?;
        ensure!(
            tables.len() <= MAX_SOURCES - usize::from(memory.is_some()),
            "index read fan-in exceeded"
        );
        let count = tables.len() + usize::from(memory.is_some());
        let mut sources = tables
            .iter()
            .map(|t| {
                t.scan_ordered(start, end, revision, reverse)
                    .map(Source::Table)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if let Some(memory) = memory {
            sources.push(Source::Memory(
                memory.scan_ordered(start, end, revision, reverse)?,
            ));
        }
        Ok(Self {
            sources,
            pending: (0..count).collect(),
            heads: BinaryHeap::with_capacity(count),
            failed: false,
            reverse,
        })
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Entry>> {
        ensure!(
            !self.failed,
            "index merge cursor retired after conflicting rows"
        );
        while let Some(&source) = self.pending.front() {
            // Keep both the pending source and previously loaded heads on an
            // error or cancellation. No result is safe with an unknown frontier.
            if let Some(row) = self.sources[source].next().await? {
                self.heads.push(Head {
                    source,
                    row,
                    reverse: self.reverse,
                });
            }
            self.pending.pop_front();
        }
        let Some(winner) = self.heads.pop() else {
            return Ok(None);
        };
        self.pending.push_back(winner.source);
        while self
            .heads
            .peek()
            .is_some_and(|h| h.row.key == winner.row.key)
        {
            let head = self.heads.pop().unwrap();
            self.pending.push_back(head.source);
            if head.row.revision == winner.row.revision && head.row.value != winner.row.value {
                self.failed = true;
                anyhow::bail!("conflicting index rows at the same revision");
            }
        }
        Ok(Some(winner.row))
    }
}

#[cfg(test)]
mod tests;
