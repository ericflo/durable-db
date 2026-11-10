use super::*;
use crate::{
    index::recovery::checkpoint::{self, Staged},
    storage::Storage,
};

struct Publishing {
    staged: Staged,
    snapshot: checkpoint::Snapshot,
    attempted: bool,
}
pub(super) struct Maintenance {
    files: Arc<dyn Storage>,
    cache: Arc<Cache>,
    publishing: Option<Publishing>,
    covered: u64,
    recovered: u64,
    pub requested: bool,
}
impl Maintenance {
    pub fn new(files: Arc<dyn Storage>, cache: Arc<Cache>, recovered: u64) -> Self {
        Self {
            files,
            cache,
            recovered,
            publishing: None,
            covered: 0,
            requested: false,
        }
    }
}
impl<C: Clock> Owner<C> {
    pub(crate) fn cache(&self) -> Arc<Cache> {
        self.maintenance.cache.clone()
    }

    /// Quiesce admission before resident capacity is exhausted. Outstanding
    /// immutable uploads keep their positions/charges until resolved; writes
    /// cannot indefinitely postpone a checkpoint by keeping the queue nonempty.
    pub fn checkpoint_due(&self) -> bool {
        let used = self.projection.usage();
        let limits = self.limits.index;
        self.maintenance.requested
            || self.maintenance.publishing.is_some()
            || self.maintenance.recovered + self.writer.durable_prefix().position()
                - self.maintenance.covered
                >= self.limits.checkpoint_frames
            || used.versions >= (limits.versions / 2).max(1)
            || used.value_bytes >= (limits.value_bytes / 2).max(1)
            || used.databases >= (limits.databases / 2).max(1)
            || used.receipts >= (limits.receipts / 2).max(1)
            || used.receipt_bytes >= (limits.receipt_bytes / 2).max(1)
            || self.validation.transactions >= (self.limits.validation.transactions / 2).max(1)
    }

    /// Called by admission and completion; a runtime can also drive this after
    /// a failed/cancelled call. Returns false while an upload is still pending.
    /// Checkpoint construction pauses data mutation during I/O. Independent
    /// control renewal must continue; immutable objects are never deleted here.
    pub async fn maintain(&mut self) -> anyhow::Result<bool> {
        self.check()?;
        if !self.checkpoint_due() || self.writer.pending_batches() != 0 {
            return Ok(false);
        }
        if self.maintenance.publishing.is_none() {
            let origin = self.control.lock()?.recovery_record()?;
            let cut = root::Cut {
                identity: self.identity,
                prefix: self.writer.durable_prefix(),
            };
            let staged = checkpoint::write_projection(
                &self.projection,
                self.maintenance.files.clone(),
                origin,
                cut,
            )
            .await?;
            let snapshot = checkpoint::Snapshot::open(
                self.maintenance.files.clone(),
                self.maintenance.cache.clone(),
                staged.reference(),
            )
            .await?;
            self.maintenance.publishing = Some(Publishing {
                staged,
                snapshot,
                attempted: false,
            });
        }
        let pending = self.maintenance.publishing.as_mut().unwrap();
        if !pending.attempted {
            let publication = self.control.lock()?.publish_checkpoint(Staged {
                origin: pending.staged.origin,
                reference: pending.staged.reference,
            })?;
            // A cancelled future may have sent its CAS. Retain this state until
            // a fresh exact validation confirms the new or old reference.
            pending.attempted = true;
            let completed = publication.execute().await;
            let _outcome = self.control.lock()?.finish(completed)?;
        }
        let published = self
            .control
            .lock()?
            .recovery_record()
            .ok()
            .is_some_and(|r| r.checkpoint == Some(pending.staged.reference));
        if !published {
            let _outcome = self.control.renew().await?;
            let record = self.control.lock()?.recovery_record()?;
            if record.checkpoint != Some(pending.staged.reference) {
                ensure!(record == pending.staged.origin, "checkpoint origin changed");
                // Exact validation changed the nonce: the old captured CAS
                // cannot still apply. Reuse the staged bytes on the next call.
                pending.attempted = false;
                anyhow::bail!("checkpoint publication was not confirmed; resume maintenance")
            }
        }
        self.control.check(self.identity)?;
        let pending = self.maintenance.publishing.take().unwrap();
        self.projection.rebase(pending.snapshot);
        self.active.clear();
        self.validation = validation::Usage::default();
        self.maintenance.covered = self.writer.durable_prefix().position();
        self.maintenance.recovered = 0;
        self.maintenance.requested = false;
        Ok(true)
    }
}
