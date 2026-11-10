//! Immutable B+tree shared by every cluster provider. Only its root is mutable.
use crate::{cache::Cache, engine::validate_db, storage::Storage};
use anyhow::{Context, bail, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;
mod batch;

const FANOUT: usize = 64;
const MAX_NODE_BYTES: usize = 64 * 1024;
const MAX_HEIGHT: u8 = 12;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Pointer {
    pub key: String,
    pub sha256: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Root {
    pub object: String,
    pub digest: String,
    // Keys include the .json suffix to preserve Storage::list ordering, even
    // between database names such as a, a- and a0.
    first: String,
    last: String,
    pub count: u64,
    pub height: u8,
}
#[derive(Serialize, Deserialize)]
enum Node {
    Leaf(BTreeMap<String, Pointer>),
    Branch(Vec<Root>),
}
#[derive(Debug, thiserror::Error)]
#[error("directory snapshot was collected; restart from the authority")]
pub(crate) struct MissingNode;

pub(crate) struct Directory {
    files: Arc<dyn Storage>,
    cache: Option<Arc<Cache>>,
    cache_writes: bool,
}
impl Directory {
    pub fn new(files: Arc<dyn Storage>) -> Self {
        Self {
            files,
            cache: None,
            cache_writes: true,
        }
    }
    pub fn with_cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = Some(cache);
        self
    }
    async fn load(&self, root: &Root) -> anyhow::Result<Node> {
        root.validate()?;
        let cached = match &self.cache {
            Some(cache) => cache.get(&root.object, &root.digest).await,
            None => None,
        };
        let cache_miss = cached.is_none();
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                let obj = self
                    .files
                    .get(&root.object, MAX_NODE_BYTES)
                    .await?
                    .ok_or(MissingNode)?;
                ensure!(
                    hex::encode(Sha256::digest(&obj.bytes)) == root.digest,
                    "directory checksum mismatch"
                );
                obj.bytes
            }
        };
        let node: Node = serde_json::from_slice(&bytes)?;
        let (first, last, count, height) = node.summary()?;
        ensure!(
            first == root.first
                && last == root.last
                && count == root.count
                && height == root.height,
            "directory reference does not match its node"
        );
        if self.cache_writes
            && cache_miss
            && let Some(cache) = &self.cache
        {
            cache.insert(&root.object, bytes).await;
        }
        Ok(node)
    }
    async fn save(&self, node: Node) -> anyhow::Result<Root> {
        let (first, last, count, height) = node.summary()?;
        let object = format!("indexes/{}.{}", database(&first)?, Uuid::new_v4());
        let bytes = Bytes::from(serde_json::to_vec(&node)?);
        ensure!(
            bytes.len() <= MAX_NODE_BYTES,
            "directory page budget exceeded"
        );
        let root = Root {
            object,
            digest: hex::encode(Sha256::digest(&bytes)),
            first,
            last,
            count,
            height,
        };
        self.files.put(&root.object, bytes.clone()).await?;
        if self.cache_writes
            && let Some(cache) = &self.cache
        {
            cache.insert(&root.object, bytes).await;
        }
        Ok(root)
    }
    pub async fn get(&self, root: Option<&Root>, db: &str) -> anyhow::Result<Option<Pointer>> {
        let key = format!("{db}.json");
        let mut next = root.cloned();
        while let Some(root) = next {
            match self.load(&root).await? {
                Node::Leaf(entries) => return Ok(entries.get(&key).cloned()),
                Node::Branch(children) => next = Some(children[route(&children, &key)].clone()),
            }
        }
        Ok(None)
    }
    pub async fn list(
        &self,
        root: Option<&Root>,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let mut stack: Vec<_> = root.cloned().into_iter().collect();
        let mut result = Vec::new();
        while result.len() < limit {
            let Some(root) = stack.pop() else {
                break;
            };
            if after.is_some_and(|a| root.last.as_str() <= a) {
                continue;
            }
            match self.load(&root).await? {
                Node::Branch(children) => stack.extend(children.into_iter().rev()),
                Node::Leaf(entries) => {
                    for key in entries
                        .into_keys()
                        .filter(|k| after.is_none_or(|a| k.as_str() > a))
                    {
                        result.push(format!("heads/{key}"));
                        if result.len() == limit {
                            break;
                        }
                    }
                }
            }
        }
        Ok(result)
    }
    /// Path copying touches at most two nodes per level, plus a new root on split.
    async fn insert_one(
        &self,
        root: Option<&Root>,
        db: &str,
        pointer: Pointer,
    ) -> anyhow::Result<Root> {
        let key = format!("{db}.json");
        let mut stack = Vec::new();
        let mut next = root.cloned();
        let mut entries = loop {
            let Some(root) = next else {
                break BTreeMap::new();
            };
            match self.load(&root).await? {
                Node::Leaf(entries) => break entries,
                Node::Branch(children) => {
                    let chosen = route(&children, &key);
                    next = Some(children[chosen].clone());
                    stack.push((children, chosen));
                }
            }
        };
        entries.insert(key, pointer);
        let mut replacement = if entries.len() > FANOUT {
            let middle = entries.keys().nth(entries.len() / 2).unwrap().clone();
            let right = entries.split_off(&middle);
            vec![
                self.save(Node::Leaf(entries)).await?,
                self.save(Node::Leaf(right)).await?,
            ]
        } else {
            vec![self.save(Node::Leaf(entries)).await?]
        };
        while let Some((mut children, chosen)) = stack.pop() {
            children.splice(chosen..=chosen, replacement);
            replacement = if children.len() > FANOUT {
                let right = children.split_off(children.len() / 2);
                vec![
                    self.save(Node::Branch(children)).await?,
                    self.save(Node::Branch(right)).await?,
                ]
            } else {
                vec![self.save(Node::Branch(children)).await?]
            };
        }
        if replacement.len() == 1 {
            Ok(replacement.pop().unwrap())
        } else {
            self.save(Node::Branch(replacement)).await
        }
    }
    /// Caller must list these candidates BEFORE a confirmed authority fence.
    /// Protect the union of every directory in that captured authority.
    /// Missing paths are errors, never evidence that a candidate is unreachable.
    pub async fn collect_unreachable(
        &self,
        roots: &[Option<Root>],
        manifests: &[String],
        indexes: &[String],
        authority: &dyn crate::publication::Authority,
    ) -> anyhow::Result<usize> {
        use crate::metrics::{Event, REGISTRY};
        let mut deleted = 0;
        for key in manifests {
            REGISTRY.event(Event::ManifestCandidates, 1);
            let db = witness(key, "manifests/")?;
            let mut live = false;
            for root in roots.iter().flatten() {
                if self
                    .get(Some(root), db)
                    .await?
                    .is_some_and(|p| p.key == *key)
                {
                    live = true;
                    break;
                }
            }
            if !live {
                authority.delete_versions(key).await?;
                REGISTRY.event(Event::ManifestDeleteAck, 1);
                deleted += 1;
            }
        }
        for key in indexes {
            REGISTRY.event(Event::IndexCandidates, 1);
            let mut live = false;
            for root in roots.iter().flatten() {
                if self.reachable(Some(root), key).await? {
                    live = true;
                    break;
                }
            }
            if !live {
                authority.delete_versions(key).await?;
                REGISTRY.event(Event::IndexDeleteAck, 1);
            }
        }
        Ok(deleted)
    }
    /// A reachable immutable node still contains its original first key. Thus
    /// its witness's search path includes it, even after reuse under new parents.
    /// Missing pages are errors, NEVER evidence that the candidate is unreachable.
    pub async fn reachable(&self, root: Option<&Root>, object: &str) -> anyhow::Result<bool> {
        let db = witness(object, "indexes/")?;
        let key = format!("{db}.json");
        let mut next = root.cloned();
        while let Some(root) = next {
            root.validate()?;
            if root.object == object {
                return Ok(true);
            }
            match self.load(&root).await? {
                Node::Leaf(_) => return Ok(false),
                Node::Branch(children) => next = Some(children[route(&children, &key)].clone()),
            }
        }
        Ok(false)
    }
}
fn route(children: &[Root], key: &str) -> usize {
    children
        .partition_point(|c| c.first.as_str() <= key)
        .saturating_sub(1)
}
fn database(key: &str) -> anyhow::Result<&str> {
    let db = key.strip_suffix(".json").context("invalid directory key")?;
    validate_db(db)?;
    Ok(db)
}
pub(crate) fn witness<'a>(key: &'a str, prefix: &str) -> anyhow::Result<&'a str> {
    let (db, id) = key
        .strip_prefix(prefix)
        .and_then(|s| s.rsplit_once('.'))
        .context("invalid directory object name")?;
    validate_db(db)?;
    Uuid::parse_str(id).context("invalid directory object identity")?;
    Ok(db)
}
fn digest_valid(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
impl Root {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            witness(&self.object, "indexes/")? == database(&self.first)?,
            "incorrect directory witness"
        );
        database(&self.last)?;
        ensure!(
            self.first <= self.last
                && self.count > 0
                && self.height > 0
                && self.height <= MAX_HEIGHT
                && digest_valid(&self.digest),
            "invalid directory reference"
        );
        Ok(())
    }
}
impl Node {
    fn summary(&self) -> anyhow::Result<(String, String, u64, u8)> {
        match self {
            Self::Leaf(entries) => {
                ensure!(
                    !entries.is_empty() && entries.len() <= FANOUT,
                    "invalid directory leaf size"
                );
                for (key, p) in entries {
                    ensure!(
                        database(key)? == witness(&p.key, "manifests/")? && digest_valid(&p.sha256),
                        "invalid directory manifest reference"
                    );
                }
                Ok((
                    entries.first_key_value().unwrap().0.clone(),
                    entries.last_key_value().unwrap().0.clone(),
                    entries.len() as u64,
                    1,
                ))
            }
            Self::Branch(children) => {
                ensure!(
                    (2..=FANOUT).contains(&children.len()),
                    "invalid directory branch size"
                );
                let mut count = 0u64;
                for child in children {
                    child.validate()?;
                    ensure!(
                        child.height == children[0].height && child.height < MAX_HEIGHT,
                        "invalid directory height"
                    );
                    count = count
                        .checked_add(child.count)
                        .context("directory count overflow")?;
                }
                if children.windows(2).any(|c| c[0].last >= c[1].first) {
                    bail!("overlapping directory children");
                }
                Ok((
                    children[0].first.clone(),
                    children.last().unwrap().last.clone(),
                    count,
                    children[0].height + 1,
                ))
            }
        }
    }
}
