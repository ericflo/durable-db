use super::*;
use crate::{
    index::recovery::checkpoint::{self, Staged},
    storage::Storage,
};
use std::time::Duration;
use tokio::time::Instant;

struct Publishing {
    staged: Staged,
    snapshot: checkpoint::Snapshot,
    attempted: bool,
}
pub(super) struct Maintenance {
    files: Arc<dyn Storage>,
    cache: Arc<Cache>,
    publishing: Option<Publishing>,
    retry: Option<(Instant, Duration)>,
    covered: u64,
    recovered: u64,
    handoff: bool,
    pub requested: bool,
}
impl Maintenance {
    pub fn new(files: Arc<dyn Storage>, cache: Arc<Cache>, recovered: u64, handoff: bool) -> Self {
        Self {
            files,
            cache,
            recovered,
            handoff,
            publishing: None,
            retry: None,
            covered: 0,
            requested: false,
        }
    }
}
/// One frozen journal cut. Staging borrows the owner immutably; publication
/// and the projection swap require its exclusive borrow again.
pub(crate) struct Checkpoint {
    cut: root::Cut,
    started: Instant,
    stage: &'static str,
    staged: Option<Publishing>,
}
impl Owner {
    pub(super) fn reuse_copy_prepayment(&self) -> bool {
        // A failed/cancelled attempt may have spent the previous reserve.
        // After takeover, first establish this owner's funded checkpoint.
        !self.maintenance.handoff && self.maintenance.retry.is_none()
    }

    pub(crate) fn files(&self) -> Arc<dyn Storage> {
        self.maintenance.files.clone()
    }
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

    /// Stage and publish without concurrent access. Serving uses these same
    /// steps with a shared borrow during staging. Returns true only after a
    /// checkpoint is published; cache-only eviction needs no storage work.
    pub async fn maintain(&mut self) -> anyhow::Result<bool> {
        let Some(mut checkpoint) = self.begin_checkpoint()? else {
            return Ok(false);
        };
        let result = async {
            checkpoint.stage(self).await?;
            checkpoint.publish(self).await
        }
        .await;
        checkpoint.finish(result)
    }

    pub(crate) fn begin_checkpoint(&mut self) -> anyhow::Result<Option<Checkpoint>> {
        self.check()?;
        if (!self.checkpoint_due() && !self.maintenance.handoff)
            || self.writer.pending_batches() != 0
        {
            return Ok(None);
        }
        // Even an idle, empty successor needs its own durable cut, otherwise
        // every replacement revisits all earlier owners. This uses the normal
        // checkpoint path and funding. Handoff alone does not mark capacity as
        // exhausted. Staging keeps the projection immutable while allowing reads.
        // Retries/refusals can fill the receipt cache without advancing history.
        // Drop only that disposable state: no new checkpoint or authority CAS.
        if self.maintenance.publishing.is_none()
            && !self.maintenance.handoff
            && self.maintenance.recovered == 0
            && self.writer.durable_prefix().position() == self.maintenance.covered
            && self.projection.discard_warmed()
        {
            self.active.clear();
            self.validation = validation::Usage::default();
            self.maintenance.requested = false;
            return Ok(None);
        }
        if self.maintenance.publishing.is_none() {
            // Set the gate before any await: cancellation and request-driven
            // maintenance must not bypass pacing or multiply charged uploads.
            let now = Instant::now();
            if self.maintenance.retry.is_some_and(|(at, _)| now < at) {
                return Ok(None);
            }
            let delay = self
                .maintenance
                .retry
                .map_or(Duration::from_secs(1), |(_, prior)| {
                    (prior * 2).min(Duration::from_secs(30))
                });
            self.maintenance.retry = Some((now + delay, delay));
        }
        Ok(Some(Checkpoint {
            cut: root::Cut {
                identity: self.identity,
                prefix: self.writer.durable_prefix(),
            },
            started: Instant::now(),
            stage: "prepare",
            staged: None,
        }))
    }

    fn maintenance_budget(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<Arc<crate::upload_budget::Allocator>>> {
        // Funding may consume backing for unused admission rights. It must
        // invalidate the same allocator, not a newly reconstructed grant.
        if let Some(budget) = self.budgets.get(name) {
            return Ok(Some(budget.clone()));
        }
        self.database_budget(name)
    }
}
impl Checkpoint {
    fn check(&self, owner: &Owner) -> anyhow::Result<()> {
        owner.check()?;
        ensure!(
            owner.identity == self.cut.identity
                && owner.writer.durable_prefix() == self.cut.prefix
                && owner.writer.pending_batches() == 0,
            "checkpoint cut changed"
        );
        Ok(())
    }
    fn progress(&mut self, stage: &'static str) {
        self.stage = stage;
        tracing::info!(owner = %self.cut.identity.incarnation, stage,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "native checkpoint progress");
    }
    pub(crate) fn finish(self, result: anyhow::Result<bool>) -> anyhow::Result<bool> {
        if result.is_err() {
            tracing::warn!(owner = %self.cut.identity.incarnation, stage = self.stage,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "native checkpoint deferred");
        } else if matches!(result, Ok(true)) {
            tracing::info!(owner = %self.cut.identity.incarnation, stage = self.stage,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "native checkpoint completed");
        }
        result
    }
    pub(crate) async fn stage(&mut self, owner: &Owner) -> anyhow::Result<()> {
        self.check(owner)?;
        if owner.maintenance.publishing.is_some() {
            return Ok(());
        }
        // Avoid spending maintenance credit for a permanently revoked owner.
        // Publication still requires its exact conditional comparison.
        self.progress("permission");
        owner.validate().await?;
        let origin = {
            let _request = owner.control.request.lock().await;
            // A failed/cancelled refresh clears the cached confirmation,
            // while the live permit can remain valid. Retry the exact
            // comparison instead of getting stuck on the missing origin.
            if owner.control.lock()?.recovery_record().is_err() {
                self.progress("confirmation");
                let _ = owner.control.refresh_locked().await?;
            }
            owner.control.lock()?.recovery_record()?
        };
        let cut = self.cut;
        let mut files = owner.maintenance.files.clone();
        if owner.database_budget(catalog::CATALOG.name)?.is_some() {
            self.progress("allowance");
            let costs = owner.projection.checkpoint_charges().await?;
            let mut total = 0u64;
            let mut tenants = BTreeMap::<String, u64>::new();
            for ((name, generation), bytes) in costs {
                total = total
                    .checked_add(bytes)
                    .context("checkpoint allowance overflow")?;
                if (Database {
                    name: &name,
                    generation,
                }) != catalog::CATALOG
                {
                    let cost = tenants.entry(name).or_default();
                    *cost = cost
                        .checked_add(bytes)
                        .context("checkpoint allowance overflow")?;
                }
            }
            let charges: Vec<_> = tenants
                .into_iter()
                .map(|(name, bytes)| {
                    Ok((
                        owner
                            .maintenance_budget(&name)?
                            .context("missing tenant allocator")?,
                        bytes,
                    ))
                })
                .collect::<anyhow::Result<_>>()?;
            self.progress("funding");
            if let Err(error) =
                crate::upload_budget::Allocator::fund_maintenance(&charges, |before| {
                    owner.control.lock()?.check_funding_origin(before, origin)
                })
                .await
            {
                // Collection/takeover can invalidate the captured origin.
                // Refresh control before the paced retry; do not upload or
                // reconstruct any rights from an uncertain funding reply.
                let _ = owner.control.refresh().await;
                return Err(error);
            }
            files = crate::upload_budget::Attempt::bounded(files, total);
        }
        self.progress("staging");
        let staged = checkpoint::write_projection(&owner.projection, files, origin, cut).await?;
        let snapshot = checkpoint::Snapshot::new(
            owner.maintenance.files.clone(),
            owner.maintenance.cache.clone(),
            staged.reference(),
        );
        self.staged = Some(Publishing {
            staged,
            snapshot,
            attempted: false,
        });
        Ok(())
    }

    pub(crate) async fn publish(&mut self, owner: &mut Owner) -> anyhow::Result<bool> {
        self.check(owner)?;
        if let Some(staged) = self.staged.take() {
            ensure!(
                owner.maintenance.publishing.is_none(),
                "checkpoint staging replaced"
            );
            owner.maintenance.publishing = Some(staged);
        }
        self.progress("publication");
        let _request = owner.control.request.lock().await;
        let pending = owner
            .maintenance
            .publishing
            .as_mut()
            .context("checkpoint not staged")?;
        for attempt in 0..2 {
            if !pending.attempted {
                self.progress("publication");
                let staged = Staged {
                    origin: pending.staged.origin,
                    reference: pending.staged.reference,
                };
                let publication = if attempt == 0 {
                    // Funding and staging can advance the register. Read their
                    // result and let publication validate it in the same CAS.
                    let observed = owner.control.lock()?.observe();
                    let observed = observed.await?;
                    let mut root = owner.control.lock()?;
                    // The request gate excludes live control calls. Only a
                    // cancelled earlier validation can still be pending here.
                    root.abandon();
                    root.publish_checkpoint_observed(observed, staged)
                } else {
                    // The retry below already confirmed a fresh comparison.
                    owner.control.lock()?.publish_checkpoint(staged)
                };
                match publication {
                    Ok(publication) => {
                        // Cancellation may leave a sent CAS. Retain uncertainty
                        // until exact validation confirms the new or old reference.
                        pending.attempted = true;
                        let completed = publication.execute().await;
                        let _outcome = owner.control.lock()?.finish(completed)?;
                    }
                    // Resolve a changed origin through the same confirmation
                    // path, which discards collected staging before rebuilding.
                    Err(root::Error::StaleRecovery) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            let published = owner
                .control
                .lock()?
                .recovery_record()
                .ok()
                .is_some_and(|r| r.checkpoint == Some(pending.staged.reference));
            if !published {
                self.progress("confirmation");
                let _outcome = owner.control.refresh_locked().await?;
                let record = owner.control.lock()?.recovery_record()?;
                if record.checkpoint != Some(pending.staged.reference) {
                    if record.collection != pending.staged.origin.collection {
                        owner.maintenance.publishing = None;
                        return Ok(false);
                    }
                    ensure!(
                        record.same_recovery(pending.staged.origin),
                        "checkpoint origin changed"
                    );
                    // Exact validation changed the nonce: the old captured CAS
                    // cannot still apply. Reuse bytes and retry once in this call,
                    // so routine budget changes need not fail the next admission.
                    pending.attempted = false;
                    ensure!(
                        attempt == 0,
                        "checkpoint publication was not confirmed; resume maintenance"
                    );
                    continue;
                }
            }
            break;
        }
        owner.control.check(owner.identity)?;
        let pending = owner.maintenance.publishing.take().unwrap();
        owner.projection.rebase(pending.snapshot);
        owner.active.clear();
        owner.validation = validation::Usage::default();
        owner.maintenance.covered = owner.writer.durable_prefix().position();
        owner.maintenance.recovered = 0;
        owner.maintenance.handoff = false;
        owner.maintenance.requested = false;
        owner.maintenance.retry = None;
        Ok(true)
    }
}
