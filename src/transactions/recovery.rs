//! Restore evaluated transactions into an unpublished, disposable index.
//!
//! A target must be private until this runner succeeds. Any failure consumes
//! and drops it, including when its final mutation failed after earlier changes.
//! Success is a complete snapshot, not current ownership or serving permission.
use super::record::{Commit, Database};
use crate::ownership::root::Replay;
use anyhow::{bail, ensure};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use uuid::Uuid;

struct Receipt {
    fingerprint: [u8; 32],
    revision: u64,
}

/// Bounded metadata for one database generation. The target must separately
/// bound resident databases, copied values, index pages and concurrent recovery.
pub struct State {
    name: String,
    generation: Uuid,
    revision: u64,
    receipt_floor: u64,
    receipt_bytes: usize,
    receipts: VecDeque<(u64, Arc<str>)>,
    by_request: BTreeMap<Arc<str>, Receipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolution {
    Committed(u64),
    /// No receipt can have expired for a transaction from this snapshot.
    Missing,
    /// Absence cannot establish failure. Never rerun an uncertain request.
    Expired,
}

impl State {
    /// An empty projection for an independently established database generation.
    /// This does not create or authorize a database, or recover its policy.
    pub fn empty(database: Database<'_>) -> anyhow::Result<Self> {
        crate::engine::validate_db(database.name)?;
        ensure!(!database.generation.is_nil(), "invalid database generation");
        Ok(Self {
            name: database.name.into(),
            generation: database.generation,
            revision: 0,
            receipt_floor: 0,
            receipt_bytes: 0,
            receipts: VecDeque::new(),
            by_request: BTreeMap::new(),
        })
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn database(&self) -> Database<'_> {
        Database {
            name: &self.name,
            generation: self.generation,
        }
    }

    /// Retained receipt count and request/fingerprint/revision payload bytes.
    /// Collection nodes and allocator overhead need separate process admission.
    pub(crate) fn receipt_usage(&self) -> (usize, usize) {
        (self.by_request.len(), self.receipt_bytes)
    }

    /// Resolve retained receipts before checking snapshot age or conflicts.
    /// A serving owner must also check requests pending in its upload queue.
    pub fn resolve(
        &self,
        request: &str,
        fingerprint: [u8; 32],
        snapshot: u64,
    ) -> anyhow::Result<Resolution> {
        ensure!(
            (1..=128).contains(&request.len()),
            "invalid transaction request identity"
        );
        if let Some(receipt) = self.by_request.get(request) {
            ensure!(
                receipt.fingerprint == fingerprint,
                "request identity reused for different contents"
            );
            return Ok(Resolution::Committed(receipt.revision));
        }
        ensure!(
            snapshot <= self.revision,
            "snapshot is ahead of recovered history"
        );
        Ok(if snapshot < self.receipt_floor {
            Resolution::Expired
        } else {
            Resolution::Missing
        })
    }

    fn advance(mut self, commit: &Commit<'_>) -> anyhow::Result<Self> {
        ensure!(
            commit.database()
                == Database {
                    name: &self.name,
                    generation: self.generation
                },
            "recovery target returned a different database generation"
        );
        ensure!(
            self.revision.checked_add(1) == Some(commit.revision()),
            "transaction revision gap or repetition"
        );
        match self.resolve(
            commit.request_id(),
            commit.fingerprint(),
            commit.snapshot_revision(),
        )? {
            Resolution::Missing => (),
            Resolution::Committed(_) => bail!("repeated committed request in selected history"),
            Resolution::Expired => bail!("selected transaction used an expired receipt window"),
        }
        ensure!(
            commit.receipt_floor() >= self.receipt_floor,
            "receipt floor regressed"
        );
        while self
            .receipts
            .front()
            .is_some_and(|(revision, _)| *revision <= commit.receipt_floor())
        {
            let (_, request) = self.receipts.pop_front().expect("checked receipt");
            self.by_request.remove(&request);
            self.receipt_bytes -= request.len() + 40;
        }
        self.receipt_floor = commit.receipt_floor();
        let request: Arc<str> = commit.request_id().into();
        self.receipt_bytes += request.len() + 40;
        self.receipts
            .push_back((commit.revision(), request.clone()));
        self.by_request.insert(
            request,
            Receipt {
                fingerprint: commit.fingerprint(),
                revision: commit.revision(),
            },
        );
        self.revision = commit.revision();
        Ok(self)
    }
}

/// Implementations supply the disposable value index, not another durability
/// boundary. They must not expose state through shared handles during recovery.
/// `take_state` removes metadata for the exact name/generation (or supplies its
/// empty state). `apply` installs every evaluated mutation and the supplied
/// revision/receipts. On error the runner discards the entire private target.
/// These are implementation obligations, not properties established for an
/// arbitrary trait implementor. The native index supplies a bounded target in
/// `index::recovery`; it is not yet the HTTP serving engine.
#[async_trait]
pub trait Target: Send + Sized {
    async fn take_state(&mut self, database: Database<'_>) -> anyhow::Result<State>;
    async fn apply(&mut self, commit: &Commit<'_>, state: State) -> anyhow::Result<()>;
}

/// Consumes both cursor and unpublished target, preventing a partial cursor
/// from being resumed against another target after an error. The input target
/// must be empty: checkpoint-based starting positions are not supported yet.
pub async fn restore<T: Target>(mut replay: Replay, mut target: T) -> anyhow::Result<T> {
    ensure!(
        replay.at_start(),
        "transaction recovery requires an unread catalog cursor"
    );
    while let Some(batch) = replay.read_next().await? {
        for bytes in batch.records() {
            let commit = Commit::decode(bytes)?;
            let state = target
                .take_state(commit.database())
                .await?
                .advance(&commit)?;
            target.apply(&commit, state).await?;
        }
    }
    ensure!(replay.completed(), "transaction recovery did not complete");
    Ok(target)
}

#[cfg(test)]
pub(crate) mod tests;
