//! Bulk-built, range-routed manifest. Nodes contain at most 64 references and
//! their upper key bounds. Both construction and traversal use bounded stacks.
use crate::{
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
    pub tables: u64,
}
impl Builder {
    pub fn new(files: Arc<dyn Storage>) -> Self {
        Self {
            files,
            leaf: Vec::new(),
            levels: Vec::new(),
            last: None,
            tables: 0,
        }
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
        if self.leaf.len() == FANOUT {
            self.flush_leaf().await?;
        }
        Ok(())
    }
    async fn flush_leaf(&mut self) -> anyhow::Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        let mut bytes = header(0, self.leaf.len());
        let last = self.leaf.last().unwrap().last.clone();
        for routed in self.leaf.drain(..) {
            boundary(&mut bytes, &routed.last);
            bytes.extend_from_slice(&routed.reference.encode());
        }
        let blob = Blob::upload(self.files.clone(), bytes.into()).await?;
        self.carry(0, Child { last, blob }).await
    }
    async fn carry(&mut self, mut level: usize, mut child: Child) -> anyhow::Result<()> {
        loop {
            ensure!(level <= MAX_HEIGHT, "checkpoint manifest height exhausted");
            if level == self.levels.len() {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(child);
            if self.levels[level].len() < FANOUT {
                return Ok(());
            }
            let children = std::mem::take(&mut self.levels[level]);
            child = branch(&self.files, level + 1, &children).await?;
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
            let child = branch(&self.files, level + 1, &children).await?;
            self.carry(level + 1, child).await?;
        }
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
async fn branch(
    files: &Arc<dyn Storage>,
    height: usize,
    children: &[Child],
) -> anyhow::Result<Child> {
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
        blob: Blob::upload(files.clone(), bytes.into()).await?,
    })
}
struct Pending {
    height: u8,
    blob: Blob,
    last: Option<Bytes>,
}
pub(super) struct Reader {
    files: Arc<dyn Storage>,
    stack: Vec<Pending>,
    tables: std::collections::VecDeque<RoutedTable>,
    failed: bool,
}
impl Reader {
    pub fn new(files: Arc<dyn Storage>, root: Option<(u8, Blob)>) -> anyhow::Result<Self> {
        ensure!(
            root.is_none_or(|(h, _)| h as usize <= MAX_HEIGHT),
            "checkpoint manifest height exceeds bound"
        );
        Ok(Self {
            files,
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
        })
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
            let bytes = pending
                .blob
                .read_range(self.files.as_ref(), 0..pending.blob.size())
                .await?;
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
                previous = Some(last.clone());
                if pending.height == 0 {
                    self.tables.push_back(RoutedTable {
                        last,
                        reference: table::Reference::decode(take(&mut input, 152)?)?,
                    });
                } else {
                    children.push(Pending {
                        height: pending.height - 1,
                        blob: Blob::decode(take(&mut input, REFERENCE_BYTES)?)?,
                        last: Some(last),
                    });
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
            self.stack.extend(children.into_iter().rev());
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
