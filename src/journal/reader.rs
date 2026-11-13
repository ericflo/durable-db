use super::LogId;
use super::budget::Budget;
use super::frame::{Batch, MAX_BATCH_BYTES, Prefix};
use crate::{
    cache::Cache,
    storage::{Object, Storage},
};
use anyhow::{Context, ensure};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use std::sync::Arc;

type Read = BoxFuture<'static, (bool, anyhow::Result<Option<Object>>)>;
// Includes the returned frame: at most four 4 MiB bodies per discovery reader.
const READ_WINDOW: usize = 4;

struct ReadAhead<'a> {
    pending: &'a mut FuturesOrdered<Read>,
    preserve: bool,
}
impl Drop for ReadAhead<'_> {
    fn drop(&mut self) {
        // A cancelled cache insertion or failed decode has popped a result but
        // has not advanced the prefix. Discard the suffix before any retry.
        if !self.preserve {
            self.pending.clear();
        }
    }
}

/// A streaming, bounded-memory scanner. Discovery is provisional; the controller
/// must end old acknowledgement permission and durably select the prefix before
/// serving a replacement. Replay never extends an already selected prefix.
pub struct Reader {
    storage: Arc<dyn Storage>,
    prefix: Prefix,
    selected: Option<Prefix>,
    finished: bool,
    budget: Budget,
    cache: Option<Arc<Cache>>,
    pending: FuturesOrdered<Read>,
}

impl Reader {
    /// The published checkpoint establishes this already durable prefix even
    /// when takeover has not selected the old writer's final position yet.
    pub(crate) fn discover_from(storage: Arc<dyn Storage>, cut: Prefix) -> Self {
        let mut reader = Self::discover(storage, cut.log);
        reader.prefix = cut;
        reader
    }
    pub fn discover(storage: Arc<dyn Storage>, log: LogId) -> Self {
        Self {
            storage,
            prefix: Prefix::empty(log),
            selected: None,
            finished: false,
            budget: Budget::new(1, MAX_BATCH_BYTES),
            cache: None,
            pending: FuturesOrdered::new(),
        }
    }

    pub fn replay(storage: Arc<dyn Storage>, selected: Prefix) -> Self {
        Self {
            selected: Some(selected),
            ..Self::discover(storage, selected.log)
        }
    }

    pub(crate) fn with_cache(mut self, cache: Option<Arc<Cache>>) -> Self {
        self.cache = cache;
        self
    }

    /// Resume only from a checkpoint authenticated by the controller. Prefix
    /// equality or the remaining hash chain binds the cut to selected history.
    pub(crate) fn resume(
        storage: Arc<dyn Storage>,
        cut: Prefix,
        selected: Prefix,
    ) -> anyhow::Result<Self> {
        ensure!(
            cut.log == selected.log && cut.position <= selected.position,
            "checkpoint exceeds selected journal"
        );
        ensure!(
            cut.position != selected.position || cut == selected,
            "checkpoint differs from selected journal"
        );
        let mut reader = Self::replay(storage, selected);
        reader.prefix = cut;
        Ok(reader)
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
        let mut reads = ReadAhead {
            pending: &mut self.pending,
            preserve: false,
        };
        let key = self.prefix.log.key(position);
        // Discovery must still observe the provider. Only selected replay may
        // reuse a prior positive read; neither absence nor disk is authority.
        let cached = if self.selected.is_some()
            && let Some(cache) = &self.cache
        {
            cache.journal_frame(&key).await
        } else {
            None
        };
        let fetched = cached.is_none();
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                let object = if self.selected.is_none() {
                    // Probe an empty journal once. After the first frame, keep
                    // a bounded window, consuming results only in chain order.
                    let window = if position == 1 { 1 } else { READ_WINDOW };
                    while reads.pending.len() < window {
                        let Some(next) = position.checked_add(reads.pending.len() as u64) else {
                            break;
                        };
                        let storage = self.storage.clone();
                        let key = self.prefix.log.key(next);
                        let speculative = !reads.pending.is_empty();
                        reads.pending.push_back(
                            async move { (speculative, storage.get(&key, MAX_BATCH_BYTES).await) }
                                .boxed(),
                        );
                    }
                    let (speculative, object) =
                        reads.pending.next().await.expect("discovery read queued");
                    let object = object?;
                    if object.is_none() && speculative {
                        // Lookahead may have observed absence before lower
                        // frames completed. Only a fresh front read ends a scan.
                        self.storage.get(&key, MAX_BATCH_BYTES).await?
                    } else {
                        object
                    }
                } else {
                    self.storage.get(&key, MAX_BATCH_BYTES).await?
                };
                let Some(object) = object else {
                    ensure!(
                        self.selected.is_none(),
                        "selected journal prefix is missing a frame"
                    );
                    self.finished = true;
                    return Ok(None);
                };
                object.bytes
            }
        };
        let mut batch = Batch::decode(bytes.clone(), self.prefix.log, position)?;
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
        if fetched && let Some(cache) = &self.cache {
            cache.remember_journal_frame(&key, bytes).await;
        }
        self.prefix = batch.prefix;
        batch.reservation = Some(reservation);
        reads.preserve = true;
        Ok(Some(batch))
    }
}
