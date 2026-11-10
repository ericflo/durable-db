//! Native recovery from selected journals and authenticated checkpoints. The runner owns this
//! projection privately and drops it on any error. It never reruns transactions
//! or makes partially recovered rows/receipts available to an external reader.
use super::{
    memory::{self, Memory},
    table,
};
use crate::transactions::{
    record::{Commit, Database},
    recovery::{State, Target},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use std::collections::BTreeMap;
use uuid::Uuid;
pub mod checkpoint;

/// Aggregate bounds for one private recovery target, not bucket/database-size
/// limits. The current checkpoint state and subsequent versions must fit them.
#[derive(Clone, Copy)]
pub struct Limits {
    pub databases: usize,
    pub versions: usize,
    pub value_bytes: usize,
    pub receipts: usize,
    pub receipt_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            databases: 16_384,
            versions: 262_144,
            value_bytes: 64 * 1024 * 1024,
            receipts: 65_536,
            receipt_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    pub databases: usize,
    pub versions: usize,
    pub value_bytes: usize,
    pub receipts: usize,
    pub receipt_bytes: usize,
}
struct Resident {
    memory: Memory,
    state: Option<State>,
    receipts: usize,
    receipt_bytes: usize,
    floor: u64,
}
pub struct Projection {
    databases: BTreeMap<(String, Uuid), Resident>,
    limits: Limits,
    usage: Usage,
}
impl Projection {
    pub fn new(limits: Limits) -> anyhow::Result<Self> {
        ensure!(
            limits.databases > 0
                && limits.versions > 0
                && limits.value_bytes > 0
                && limits.receipts > 0
                && limits.receipt_bytes > 0,
            "invalid native recovery limits"
        );
        Ok(Self {
            databases: BTreeMap::new(),
            limits,
            usage: Usage::default(),
        })
    }
    pub fn usage(&self) -> Usage {
        self.usage
    }
    pub fn view(&self, database: Database<'_>) -> anyhow::Result<Option<View<'_>>> {
        validate_database(database)?;
        let Some(resident) = self
            .databases
            .get(&(database.name.into(), database.generation))
        else {
            return Ok(None);
        };
        Ok(Some(View {
            floor: resident.floor,
            memory: &resident.memory,
            state: resident
                .state
                .as_ref()
                .context("native recovery state is being advanced")?,
        }))
    }
}
pub struct View<'a> {
    memory: &'a Memory,
    state: &'a State,
    floor: u64,
}
impl View<'_> {
    pub fn state(&self) -> &State {
        self.state
    }
    pub fn snapshot_floor(&self) -> u64 {
        self.floor
    }
    pub fn get(&self, key: &[u8], revision: u64) -> anyhow::Result<Option<table::Entry>> {
        ensure!(
            (self.floor..=self.state.revision()).contains(&revision),
            "snapshot is outside recovered history"
        );
        self.memory.get(key, revision)
    }
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<memory::Cursor<'_>> {
        ensure!(
            (self.floor..=self.state.revision()).contains(&revision),
            "snapshot is outside recovered history"
        );
        self.memory.scan(start, end, revision)
    }
    /// Input for a table flush, not a checkpoint: metadata, exact replay cut and
    /// references still require durable publication before history is retired.
    pub fn rows(&self) -> impl Iterator<Item = table::Row<'_>> {
        self.memory.rows()
    }
}
#[async_trait]
impl Target for Projection {
    async fn take_state(&mut self, database: Database<'_>) -> anyhow::Result<State> {
        validate_database(database)?;
        let key = (database.name.to_owned(), database.generation);
        if let Some(resident) = self.databases.get_mut(&key) {
            return resident
                .state
                .take()
                .context("native recovery state already taken");
        }
        ensure!(
            self.usage.databases < self.limits.databases,
            "native recovery database capacity exhausted"
        );
        let state = State::empty(database)?;
        self.databases.insert(
            key,
            Resident {
                memory: Memory::new(memory::Limits {
                    versions: self.limits.versions,
                    bytes: self.limits.value_bytes,
                })?,
                state: None,
                receipts: 0,
                receipt_bytes: 0,
                floor: 0,
            },
        );
        self.usage.databases += 1;
        Ok(state)
    }
    async fn apply(&mut self, commit: &Commit<'_>, state: State) -> anyhow::Result<()> {
        ensure!(
            state.database() == commit.database() && state.revision() == commit.revision(),
            "native recovery metadata mismatch"
        );
        let db = commit.database();
        let resident = self
            .databases
            .get_mut(&(db.name.into(), db.generation))
            .context("native recovery state not taken")?;
        ensure!(resident.state.is_none(), "native recovery state not taken");
        let (receipts, receipt_bytes) = state.receipt_usage();
        let usage = Usage {
            databases: self.usage.databases,
            versions: add(
                self.usage.versions,
                commit.mutations().len(),
                self.limits.versions,
            )?,
            value_bytes: add(
                self.usage.value_bytes,
                commit
                    .mutations()
                    .map(|m| m.key.len() + m.value.map_or(0, <[u8]>::len))
                    .sum(),
                self.limits.value_bytes,
            )?,
            receipts: add(
                self.usage.receipts - resident.receipts,
                receipts,
                self.limits.receipts,
            )?,
            receipt_bytes: add(
                self.usage.receipt_bytes - resident.receipt_bytes,
                receipt_bytes,
                self.limits.receipt_bytes,
            )?,
        };
        resident.memory.append(commit)?;
        resident.state = Some(state);
        resident.receipts = receipts;
        resident.receipt_bytes = receipt_bytes;
        self.usage = usage;
        Ok(())
    }
}
fn add(current: usize, additional: usize, limit: usize) -> anyhow::Result<usize> {
    let value = current
        .checked_add(additional)
        .context("native recovery accounting overflow")?;
    ensure!(value <= limit, "native recovery capacity exhausted");
    Ok(value)
}

fn validate_database(database: Database<'_>) -> anyhow::Result<()> {
    crate::engine::validate_db(database.name)?;
    ensure!(!database.generation.is_nil(), "invalid database generation");
    Ok(())
}

/// A private native recovery at complete frame boundaries. Each advance consumes
/// this value: cancellation or any failure drops the entire partial projection.
/// Checkpoints can be staged between steps, including within a selected journal.
/// Only completed recovery can return a readable projection.
pub struct Recovering {
    replay: crate::ownership::root::Replay,
    projection: Projection,
}
impl Recovering {
    pub async fn begin(
        mut replay: crate::ownership::root::Replay,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        ensure!(
            replay.at_start(),
            "native recovery requires an unread catalog"
        );
        let projection = if let Some(reference) = replay.origin().checkpoint {
            let header = checkpoint::Header::load(replay.files(), reference).await?;
            replay.resume(header.cut).await?;
            checkpoint::load(replay.files(), header, limits).await?
        } else {
            Projection::new(limits)?
        };
        Ok(Self { replay, projection })
    }
    pub async fn advance(mut self, frames: usize) -> anyhow::Result<Self> {
        ensure!((1..=1024).contains(&frames), "invalid recovery step budget");
        for _ in 0..frames {
            let Some(batch) = self.replay.read_next().await? else {
                break;
            };
            for bytes in batch.records() {
                let commit = Commit::decode(bytes)?;
                let state = self
                    .projection
                    .take_state(commit.database())
                    .await?
                    .advance(&commit)?;
                self.projection.apply(&commit, state).await?;
            }
        }
        Ok(self)
    }
    pub fn is_complete(&self) -> bool {
        self.replay.completed()
    }
    pub fn finish(self) -> anyhow::Result<Projection> {
        ensure!(self.is_complete(), "native recovery is incomplete");
        Ok(self.projection)
    }
    pub async fn checkpoint(&self) -> anyhow::Result<checkpoint::Staged> {
        checkpoint::write(self).await
    }
}
