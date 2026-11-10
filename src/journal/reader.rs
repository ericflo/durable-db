use super::LogId;
use super::budget::Budget;
use super::frame::{Batch, MAX_BATCH_BYTES, Prefix};
use crate::storage::Storage;
use anyhow::{Context, ensure};
use std::sync::Arc;

/// A streaming, bounded-memory scanner. Discovery is provisional; the controller
/// must end old acknowledgement permission and durably select the prefix before
/// serving a replacement. Replay never extends an already selected prefix.
pub struct Reader {
    storage: Arc<dyn Storage>,
    prefix: Prefix,
    selected: Option<Prefix>,
    finished: bool,
    budget: Budget,
}

impl Reader {
    pub fn discover(storage: Arc<dyn Storage>, log: LogId) -> Self {
        Self {
            storage,
            prefix: Prefix::empty(log),
            selected: None,
            finished: false,
            budget: Budget::new(1, MAX_BATCH_BYTES),
        }
    }

    pub fn replay(storage: Arc<dyn Storage>, selected: Prefix) -> Self {
        Self {
            storage,
            prefix: Prefix::empty(selected.log),
            selected: Some(selected),
            finished: false,
            budget: Budget::new(1, MAX_BATCH_BYTES),
        }
    }

    /// Reuse this reader's frame budget across selected journals. A retained
    /// frame (even one whose Rust borrow ended) must not free its allocation slot.
    pub(crate) fn reset(&mut self, selected: Prefix) -> anyhow::Result<()> {
        ensure!(self.finished, "previous journal replay is incomplete");
        let _reservation = self.budget.reserve(MAX_BATCH_BYTES)?;
        self.prefix = Prefix::empty(selected.log);
        self.selected = Some(selected);
        self.finished = false;
        Ok(())
    }

    /// Available only after complete successful discovery/replay, never after an error.
    pub fn finished_prefix(&self) -> Option<Prefix> {
        self.finished.then_some(self.prefix)
    }

    pub async fn read_next(&mut self) -> anyhow::Result<Option<Batch>> {
        if self.finished {
            return Ok(None);
        }
        if self.selected == Some(self.prefix) {
            self.finished = true;
            return Ok(None);
        }
        let position = self
            .prefix
            .position
            .checked_add(1)
            .context("journal epoch exhausted")?;
        // One live replay frame per reader, including frames held by its caller.
        // Reserve before GET so another read cannot allocate beyond that bound.
        let reservation = self.budget.reserve(MAX_BATCH_BYTES)?;
        let object = self
            .storage
            .get(&self.prefix.log.key(position), MAX_BATCH_BYTES)
            .await?;
        let Some(object) = object else {
            ensure!(
                self.selected.is_none(),
                "selected journal prefix is missing a frame"
            );
            self.finished = true;
            return Ok(None);
        };
        let mut batch = Batch::decode(object.bytes, self.prefix.log, position)?;
        ensure!(
            batch.previous == self.prefix.digest,
            "journal hash chain mismatch"
        );
        if let Some(selected) = self.selected {
            ensure!(
                position <= selected.position,
                "journal replay exceeded selected prefix"
            );
            if position == selected.position {
                ensure!(batch.prefix == selected, "selected journal digest mismatch");
            }
        }
        self.prefix = batch.prefix;
        batch.reservation = Some(reservation);
        Ok(Some(batch))
    }
}
