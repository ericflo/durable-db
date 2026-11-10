//! Native recovery from selected journals and authenticated checkpoints. The runner owns this
//! projection privately and drops it on any error. It never reruns transactions
//! or makes partially recovered rows/receipts available to an external reader.
use super::{
    memory::{self, Memory},
    merge, table,
};
use crate::cache::Cache;
use crate::transactions::recovery::Resolution;
use crate::transactions::{
    record::{Commit, Database},
    recovery::{State, Target},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;
pub mod checkpoint;

/// Aggregate bounds for one private recovery target, not bucket/database-size
/// limits. Only databases touched by the suffix and their receipts/versions
/// become resident; checkpoint values remain behind the bounded shared cache.
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
/// Live user keys and their key/value bytes at the current committed revision.
/// Retained versions, receipts and physical storage are accounted separately.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Statistics {
    pub keys: u64,
    pub bytes: u64,
}
impl Statistics {
    pub(crate) fn replace(
        &mut self,
        key: usize,
        old: Option<usize>,
        new: Option<usize>,
    ) -> anyhow::Result<()> {
        if let Some(old) = old {
            self.keys = self
                .keys
                .checked_sub(1)
                .context("live key accounting underflow")?;
            self.bytes = self
                .bytes
                .checked_sub((key + old) as u64)
                .context("live byte accounting underflow")?;
        }
        if let Some(new) = new {
            self.keys = self
                .keys
                .checked_add(1)
                .context("live key accounting overflow")?;
            self.bytes = self
                .bytes
                .checked_add((key + new) as u64)
                .context("live byte accounting overflow")?;
        }
        Ok(())
    }
}
struct Resident {
    statistics: Statistics,
    memory: Memory,
    state: Option<State>,
    receipts: usize,
    receipt_bytes: usize,
    floor: u64,
    base: Option<checkpoint::DatabaseSnapshot>,
}
impl Resident {
    fn changed(&self) -> bool {
        // Only a commit changes revision. Missing state cannot be evicted.
        let base = self.base.as_ref().map_or(0, |base| base.revision());
        self.state
            .as_ref()
            .is_none_or(|state| state.revision() != base)
    }
}
pub struct Projection {
    base: Option<checkpoint::Snapshot>,
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
            base: None,
            databases: BTreeMap::new(),
            limits,
            usage: Usage::default(),
        })
    }
    pub(crate) fn rebase(&mut self, base: checkpoint::Snapshot) {
        self.base = Some(base);
        self.databases.clear();
        self.usage = Usage::default();
    }
    pub(crate) fn discard_warmed(&mut self) -> bool {
        let Some(base) = self.base.clone() else {
            return false;
        };
        if self.usage.versions != 0 || self.databases.values().any(Resident::changed) {
            return false;
        }
        self.rebase(base);
        true
    }
    pub fn usage(&self) -> Usage {
        self.usage
    }
    /// Warm only receipt metadata, retaining a complete committed projection
    /// even if the caller is cancelled during the preceding immutable reads.
    pub(crate) async fn warm(
        &mut self,
        database: Database<'_>,
        reserved_receipts: usize,
        reserved_bytes: usize,
    ) -> anyhow::Result<(u64, u64)> {
        let state = self
            .take_state_reserved(database, reserved_receipts, reserved_bytes)
            .await?;
        let metadata = (state.revision(), state.checkpoint_floor());
        self.databases
            .get_mut(&(database.name.into(), database.generation))
            .expect("warmed database")
            .state = Some(state);
        Ok(metadata)
    }
    async fn take_state_reserved(
        &mut self,
        database: Database<'_>,
        reserved_receipts: usize,
        reserved_bytes: usize,
    ) -> anyhow::Result<State> {
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
        let base = match &self.base {
            Some(base) => base.database(database).await?,
            None => None,
        };
        let state = if let Some(base) = &base {
            base.restore_state(
                self.limits
                    .receipts
                    .saturating_sub(self.usage.receipts + reserved_receipts),
                self.limits
                    .receipt_bytes
                    .saturating_sub(self.usage.receipt_bytes + reserved_bytes),
            )
            .await?
        } else {
            State::empty(database)?
        };
        let (receipts, receipt_bytes) = state.receipt_usage();
        let floor = state.revision();
        self.databases.insert(
            key,
            Resident {
                statistics: base.as_ref().map_or(
                    Statistics::default(),
                    checkpoint::DatabaseSnapshot::statistics,
                ),
                memory: Memory::from_checkpoint(
                    floor,
                    memory::Limits {
                        versions: self.limits.versions,
                        bytes: self.limits.value_bytes,
                    },
                )?,
                state: None,
                receipts,
                receipt_bytes,
                floor,
                base,
            },
        );
        self.usage.databases += 1;
        self.usage.receipts += receipts;
        self.usage.receipt_bytes += receipt_bytes;
        Ok(state)
    }
    pub async fn view(&self, database: Database<'_>) -> anyhow::Result<Option<View<'_>>> {
        validate_database(database)?;
        let resident = self
            .databases
            .get(&(database.name.into(), database.generation));
        let base = if let Some(resident) = resident {
            resident.base.clone()
        } else if let Some(base) = &self.base {
            base.database(database).await?
        } else {
            None
        };
        if resident.is_none() && base.is_none() {
            return Ok(None);
        }
        let state = resident
            .map(|r| {
                r.state
                    .as_ref()
                    .context("native recovery state is being advanced")
            })
            .transpose()?;
        Ok(Some(View {
            statistics: resident
                .map_or_else(|| base.as_ref().unwrap().statistics(), |r| r.statistics),
            floor: resident.map_or_else(|| base.as_ref().unwrap().revision(), |r| r.floor),
            memory: resident.map(|r| &r.memory),
            state,
            base,
        }))
    }
}
/// A coherent immutable checkpoint plus the selected suffix at one generation.
/// The projection is private until recovery completes; authority is external.
pub struct View<'a> {
    statistics: Statistics,
    memory: Option<&'a Memory>,
    state: Option<&'a State>,
    base: Option<checkpoint::DatabaseSnapshot>,
    floor: u64,
}
impl<'a> View<'a> {
    pub fn statistics(&self) -> Statistics {
        self.statistics
    }
    /// Only resident suffix versions, for a future incremental flush. These
    /// rows exclude checkpoint values and are not a complete database image.
    pub fn recent_rows(&self) -> impl Iterator<Item = table::Row<'_>> {
        self.memory.into_iter().flat_map(Memory::rows)
    }
    pub fn revision(&self) -> u64 {
        self.state
            .map_or_else(|| self.base.as_ref().unwrap().revision(), State::revision)
    }
    pub fn snapshot_floor(&self) -> u64 {
        self.floor
    }
    pub async fn resolve(
        &self,
        request: &str,
        fingerprint: [u8; 32],
        snapshot: u64,
    ) -> anyhow::Result<Resolution> {
        if let Some(state) = self.state {
            state.resolve(request, fingerprint, snapshot)
        } else {
            self.base
                .as_ref()
                .unwrap()
                .resolve(request, fingerprint, snapshot)
                .await
        }
    }
    fn check_revision(&self, revision: u64) -> anyhow::Result<()> {
        ensure!(
            (self.floor..=self.revision()).contains(&revision),
            "snapshot is outside recovered history"
        );
        Ok(())
    }
    pub async fn get(&self, key: &[u8], revision: u64) -> anyhow::Result<Option<table::Entry>> {
        self.check_revision(revision)?;
        ensure!(
            (1..=1024).contains(&key.len()),
            "invalid database lookup key"
        );
        let text = std::str::from_utf8(key)?;
        // All memory rows are newer than the checkpoint. A visible suffix
        // deletion is a complete answer and must not fall through to the base.
        if let Some(memory) = self.memory
            && let Some(row) = memory.get(key, revision)?
        {
            return Ok(Some(row));
        }
        match &self.base {
            Some(base) => base.get(text).await,
            None => {
                super::validate_key(key)?;
                Ok(None)
            }
        }
    }
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<merge::Cursor<'a>> {
        self.scan_ordered(start, end, revision, false)
    }
    pub(crate) fn scan_ordered(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
        reverse: bool,
    ) -> anyhow::Result<merge::Cursor<'a>> {
        self.check_revision(revision)?;
        merge::Cursor::with_checkpoint(
            self.base.as_ref(),
            self.memory,
            start,
            end,
            revision,
            reverse,
        )
    }
}
#[async_trait]
impl Target for Projection {
    async fn take_state(&mut self, database: Database<'_>) -> anyhow::Result<State> {
        self.take_state_reserved(database, 0, 0).await
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
        let mut statistics = resident.statistics;
        for mutation in commit.mutations() {
            let old = match resident
                .memory
                .get(mutation.key.as_bytes(), commit.revision() - 1)?
            {
                Some(row) => Some(row),
                None => match &resident.base {
                    Some(base) => base.get(mutation.key).await?,
                    None => None,
                },
            };
            statistics.replace(
                mutation.key.len(),
                old.and_then(|r| r.value).map(|v| v.len()),
                mutation.value.map(<[u8]>::len),
            )?;
        }
        resident.memory.append(commit)?;
        resident.statistics = statistics;
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
    frames: u64,
    replay: crate::ownership::root::Replay,
    projection: Projection,
}
impl Recovering {
    pub async fn begin(
        replay: crate::ownership::root::Replay,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        Self::begin_cached(replay, limits, Arc::new(Cache::new(0, None)?)).await
    }
    /// The controller supplies one bounded cache shared by recovery/read views.
    pub async fn begin_cached(
        mut replay: crate::ownership::root::Replay,
        limits: Limits,
        cache: Arc<Cache>,
    ) -> anyhow::Result<Self> {
        ensure!(
            replay.at_start(),
            "native recovery requires an unread catalog"
        );
        let mut projection = Projection::new(limits)?;
        if let Some(reference) = replay.origin().checkpoint {
            let base = checkpoint::Snapshot::new(replay.files(), cache, reference);
            replay.resume(base.cut()).await?;
            projection.base = Some(base);
        }
        Ok(Self {
            replay,
            projection,
            frames: 0,
        })
    }
    pub async fn advance(mut self, frames: usize) -> anyhow::Result<Self> {
        ensure!((1..=1024).contains(&frames), "invalid recovery step budget");
        for _ in 0..frames {
            let Some(batch) = self.replay.read_next().await? else {
                break;
            };
            self.frames = self
                .frames
                .checked_add(1)
                .context("recovery frame count overflow")?;
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
    pub(crate) fn finish_bound(
        self,
    ) -> anyhow::Result<(crate::ownership::root::Record, Projection, u64)> {
        ensure!(self.is_complete(), "native recovery is incomplete");
        Ok((self.replay.origin(), self.projection, self.frames))
    }
    pub async fn checkpoint(&self) -> anyhow::Result<checkpoint::Staged> {
        checkpoint::write(self).await
    }
}
