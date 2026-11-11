//! Range-routed manifest with bounded construction/traversal stacks. Rebuilds
//! retain identical nodes observed by the existing forward base traversal.
use crate::{
    cache::Cache,
    index::table,
    storage::{
        Storage,
        immutable::{Blob, REFERENCE_BYTES},
    },
};
use anyhow::{Context, ensure};
use bytes::{Buf, Bytes};
use std::sync::Arc;
const FANOUT: usize = 64;
const MAX_HEIGHT: usize = 8;
const MAX_NODE: usize = 144 * 1024;
struct Child {
    last: Bytes,
    blob: Blob,
}
pub(super) struct RoutedTable {
    pub last: Bytes,
    pub reference: table::Reference,
}
pub(super) struct Builder {
    files: Arc<dyn Storage>,
    leaf: Vec<RoutedTable>,
    levels: Vec<Vec<Child>>,
    last: Option<Bytes>,
    reuse: [Option<Blob>; MAX_HEIGHT + 1],
    pub tables: u64,
}
impl Builder {
    pub fn new(files: Arc<dyn Storage>) -> Self {
        Self {
            files,
            leaf: Vec::new(),
            levels: Vec::new(),
            last: None,
            reuse: [None; MAX_HEIGHT + 1],
            tables: 0,
        }
    }
    pub fn crosses_leaf(&self, reader: &Reader) -> bool {
        self.reuse[0].is_some() && self.reuse[0] != reader.reuse[0]
    }
    pub async fn align_to(&mut self, reader: &Reader) -> anyhow::Result<()> {
        // Finish each old group before adopting its successor's reuse candidate.
        // Otherwise one inserted/deleted table shifts every later sibling.
        for height in 0..=MAX_HEIGHT {
            if self.reuse[height].is_some() && self.reuse[height] != reader.reuse[height] {
                if height == 0 {
                    self.flush_leaf().await?;
                } else if let Some(children) = self.levels.get_mut(height - 1) {
                    let children = std::mem::take(children);
                    if !children.is_empty() {
                        let child = self.branch(height, &children).await?;
                        self.carry(height, child).await?;
                    }
                }
            }
        }
        self.reuse = reader.reuse;
        Ok(())
    }
    async fn upload(&self, height: usize, bytes: Vec<u8>) -> anyhow::Result<Blob> {
        if let Some(blob) = self.reuse[height].filter(|blob| blob.matches(&bytes)) {
            return Ok(blob);
        }
        Blob::upload(self.files.clone(), bytes.into()).await
    }
    pub async fn push(&mut self, reference: table::Reference, last: Bytes) -> anyhow::Result<()> {
        ensure!(
            !last.is_empty() && last.len() <= 2048 && self.last.as_ref().is_none_or(|p| p < &last),
            "invalid checkpoint table bounds"
        );
        self.tables = self
            .tables
            .checked_add(1)
            .context("checkpoint table count overflow")?;
        self.last = Some(last.clone());
        self.leaf.push(RoutedTable { last, reference });
        if self.leaf.len() > FANOUT || self.leaf.len() == FANOUT && self.reuse[0].is_none() {
            self.flush_leaf().await?;
        }
        Ok(())
    }
    async fn flush_leaf(&mut self) -> anyhow::Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        // Aligned rewrites wait for overflow, then split evenly. A full+one
        // split would let repeated prefix inserts create a new level every 64
        // tables. Fresh bulk construction still packs full nodes.
        let count = if self.leaf.len() > FANOUT {
            self.leaf.len() / 2
        } else {
            self.leaf.len()
        };
        let mut bytes = header(0, count);
        let last = self.leaf[count - 1].last.clone();
        for routed in self.leaf.drain(..count) {
            boundary(&mut bytes, &routed.last);
            bytes.extend_from_slice(&routed.reference.encode());
        }
        let blob = self.upload(0, bytes).await?;
        self.carry(0, Child { last, blob }).await
    }
    async fn carry(&mut self, mut level: usize, mut child: Child) -> anyhow::Result<()> {
        loop {
            ensure!(level <= MAX_HEIGHT, "checkpoint manifest height exhausted");
            if level == self.levels.len() {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(child);
            let len = self.levels[level].len();
            if len < FANOUT || len == FANOUT && self.reuse[0].is_some() {
                return Ok(());
            }
            let count = if len > FANOUT { len / 2 } else { len };
            let children: Vec<_> = self.levels[level].drain(..count).collect();
            child = self.branch(level + 1, &children).await?;
            level += 1;
        }
    }
    pub async fn finish(mut self) -> anyhow::Result<Option<(u8, Blob)>> {
        self.flush_leaf().await?;
        loop {
            let Some(level) = self.levels.iter().position(|v| !v.is_empty()) else {
                return Ok(None);
            };
            let children = std::mem::take(&mut self.levels[level]);
            let higher = self.levels.iter().skip(level + 1).any(|v| !v.is_empty());
            if !higher && children.len() == 1 {
                return Ok(Some((level as u8, children[0].blob)));
            }
            let child = self.branch(level + 1, &children).await?;
            self.carry(level + 1, child).await?;
        }
    }
    async fn branch(&self, height: usize, children: &[Child]) -> anyhow::Result<Child> {
        ensure!(
            height <= MAX_HEIGHT && (1..=FANOUT).contains(&children.len()),
            "invalid checkpoint manifest branch"
        );
        let mut bytes = header(height as u8, children.len());
        for child in children {
            boundary(&mut bytes, &child.last);
            child.blob.encode(&mut bytes);
        }
        Ok(Child {
            last: children.last().unwrap().last.clone(),
            blob: self.upload(height, bytes).await?,
        })
    }
}
fn header(height: u8, count: usize) -> Vec<u8> {
    let mut b = b"DDBCPN01".to_vec();
    b.push(height);
    b.push(count as u8);
    b
}
fn boundary(out: &mut Vec<u8>, key: &[u8]) {
    out.extend_from_slice(&(key.len() as u16).to_be_bytes());
    out.extend_from_slice(key);
}
struct Pending {
    height: u8,
    blob: Blob,
    last: Option<Bytes>,
}
pub(super) struct Reader {
    files: Arc<dyn Storage>,
    cache: Option<Arc<Cache>>,
    start: Bytes,
    stack: Vec<Pending>,
    tables: std::collections::VecDeque<RoutedTable>,
    failed: bool,
    reverse: bool,
    end: Option<Bytes>,
    // Most recently validated node at each height: nine references, no map,
    // value reads or extra provider requests. A missed candidate only costs a PUT.
    reuse: [Option<Blob>; MAX_HEIGHT + 1],
}
impl Reader {
    pub fn new(files: Arc<dyn Storage>, root: Option<(u8, Blob)>) -> anyhow::Result<Self> {
        ensure!(
            root.is_none_or(|(h, _)| h as usize <= MAX_HEIGHT),
            "checkpoint manifest height exceeds bound"
        );
        Ok(Self {
            files,
            cache: None,
            start: Bytes::new(),
            stack: root
                .into_iter()
                .map(|(height, blob)| Pending {
                    height,
                    blob,
                    last: None,
                })
                .collect(),
            tables: std::collections::VecDeque::new(),
            failed: false,
            reverse: false,
            end: None,
            reuse: [None; MAX_HEIGHT + 1],
        })
    }
    pub fn seek(
        files: Arc<dyn Storage>,
        root: Option<(u8, Blob)>,
        start: Bytes,
        cache: Arc<Cache>,
    ) -> anyhow::Result<Self> {
        let mut reader = Self::new(files, root)?;
        reader.start = start;
        reader.cache = Some(cache);
        Ok(reader)
    }
    pub fn seek_reverse(
        files: Arc<dyn Storage>,
        root: Option<(u8, Blob)>,
        end: Option<Bytes>,
        cache: Arc<Cache>,
    ) -> anyhow::Result<Self> {
        let mut reader = Self::new(files, root)?;
        reader.reverse = true;
        reader.end = end;
        reader.cache = Some(cache);
        Ok(reader)
    }
    pub async fn next(&mut self) -> anyhow::Result<Option<RoutedTable>> {
        ensure!(!self.failed, "checkpoint manifest reader retired");
        loop {
            if let Some(table) = self.tables.pop_front() {
                return Ok(Some(table));
            }
            let Some(pending) = self.stack.pop() else {
                return Ok(None);
            };
            // This private recovery cursor is consumed on failure. Retire it
            // even on cancellation, so an interrupted node can never be skipped.
            self.failed = true;
            ensure!(
                pending.blob.size() <= MAX_NODE as u64,
                "checkpoint manifest node exceeds bound"
            );
            let bytes = if let Some(cache) = &self.cache {
                pending.blob.read_cached(self.files.as_ref(), cache).await?
            } else {
                pending
                    .blob
                    .read_range(self.files.as_ref(), 0..pending.blob.size())
                    .await?
            };
            ensure!(
                bytes.len() >= 10 && &bytes[..8] == b"DDBCPN01" && bytes[8] == pending.height,
                "invalid checkpoint manifest node"
            );
            let count = bytes[9] as usize;
            ensure!(
                (1..=FANOUT).contains(&count),
                "invalid checkpoint manifest count"
            );
            let mut input = &bytes[10..];
            let mut previous = None;
            let mut children = Vec::new();
            for _ in 0..count {
                let length = u16::from_be_bytes(take(&mut input, 2)?.try_into()?) as usize;
                ensure!(
                    (1..=2048).contains(&length),
                    "invalid checkpoint manifest key"
                );
                let last = Bytes::copy_from_slice(take(&mut input, length)?);
                ensure!(
                    previous.as_ref().is_none_or(|p| p < &last),
                    "checkpoint manifest bounds are not strictly ordered"
                );
                // A child covers (previous upper bound, its upper bound].
                // Reverse seeking uses its lower boundary, not its maximum:
                // an end inside this child must still visit the child.
                let eligible = if self.reverse {
                    self.end
                        .as_ref()
                        .is_none_or(|end| previous.as_ref().is_none_or(|lower| lower < end))
                } else {
                    last >= self.start
                };
                previous = Some(last.clone());
                if pending.height == 0 {
                    let reference = table::Reference::decode(take(&mut input, 152)?)?;
                    if eligible {
                        let table = RoutedTable { last, reference };
                        if self.reverse {
                            self.tables.push_front(table);
                        } else {
                            self.tables.push_back(table);
                        }
                    }
                } else {
                    let blob = Blob::decode(take(&mut input, REFERENCE_BYTES)?)?;
                    if eligible {
                        children.push(Pending {
                            height: pending.height - 1,
                            blob,
                            last: Some(last),
                        });
                    }
                }
            }
            ensure!(
                input.is_empty()
                    && pending
                        .last
                        .as_ref()
                        .is_none_or(|last| Some(last) == previous.as_ref()),
                "checkpoint manifest boundary or length mismatch"
            );
            self.reuse[pending.height as usize] = Some(pending.blob);
            if self.reverse {
                self.stack.extend(children);
            } else {
                self.stack.extend(children.into_iter().rev());
            }
            self.failed = false;
        }
    }
}
fn take<'a>(input: &mut &'a [u8], length: usize) -> anyhow::Result<&'a [u8]> {
    let out = input
        .get(..length)
        .context("truncated checkpoint manifest")?;
    input.advance(length);
    Ok(out)
}

impl super::Reference {
    /// Remove reachable candidates using the existing bounded manifest walker.
    /// No values/indexes are fetched and no object is deleted on a partial walk.
    pub(crate) async fn protect(
        &self,
        files: Arc<dyn Storage>,
        candidates: &mut std::collections::BTreeSet<String>,
    ) -> anyhow::Result<()> {
        let mut reader = Reader::new(files, self.tree)?;
        let mut tables = 0u64;
        while let Some(table) = reader.next().await? {
            tables += 1;
            ensure!(tables <= self.tables, "checkpoint table count exceeded");
            for blob in reader
                .reuse
                .iter_mut()
                .filter_map(Option::take)
                .chain(table.reference.blobs())
            {
                candidates.remove(&blob.key());
            }
            if candidates.is_empty() {
                return Ok(());
            }
        }
        ensure!(tables == self.tables, "checkpoint table count mismatch");
        Ok(())
    }
}
