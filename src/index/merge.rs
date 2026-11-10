//! Snapshot reads over overlapping immutable tables. The caller supplies the
//! complete, retained source set; these readers do not establish ownership.
//! Deletions remain rows until the complete read view can suppress them.
use super::table::{self, Entry, Table};
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
    super::validate_key(key)?;
    ensure!(tables.len() <= MAX_SOURCES, "index read fan-in exceeded");
    let mut reads = stream::iter(tables.iter().map(|table| table.get(key, revision)))
        .buffer_unordered(POINT_CONCURRENCY);
    let mut winner: Option<Entry> = None;
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
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap pops the greatest: smallest key, highest revision first.
        other
            .row
            .key
            .cmp(&self.row.key)
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
/// same range and snapshot. I/O failures and cancellation can be retried on the
/// same cursor; contradictory visible rows retire it permanently.
pub struct Cursor {
    sources: Vec<table::Cursor>,
    pending: VecDeque<usize>,
    heads: BinaryHeap<Head>,
    failed: bool,
}
impl Cursor {
    pub fn new(
        tables: &[Table],
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<Self> {
        super::validate_range(start, end)?;
        ensure!(tables.len() <= MAX_SOURCES, "index read fan-in exceeded");
        Ok(Self {
            sources: tables
                .iter()
                .map(|t| t.scan(start, end, revision))
                .collect::<anyhow::Result<_>>()?,
            pending: (0..tables.len()).collect(),
            heads: BinaryHeap::with_capacity(tables.len()),
            failed: false,
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
                self.heads.push(Head { source, row });
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
