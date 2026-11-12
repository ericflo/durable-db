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
impl Owner {
    // Foreground operations only wait for capacity-required maintenance. An
    // unfunded opportunistic handoff checkpoint must not deny ordinary reads.
    pub(super) async fn maintain_required(&mut self) -> anyhow::Result<()> {
        if self.checkpoint_due() {
            self.maintain().await?;
        }
        Ok(())
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

    /// Called by admission and completion; a runtime can also drive this after
    /// a failed/cancelled call. Returns false while an upload is still pending.
    /// Checkpoint construction pauses data mutation during I/O. Independent
    /// control operations remain separate; immutable objects are never deleted here.
    pub async fn maintain(&mut self) -> anyhow::Result<bool> {
        let mut stage = "prepare";
        let started = Instant::now();
        let result = self.maintain_checkpoint(&mut stage, started).await;
        if result.is_err() {
            // Fixed stages only: provider errors may contain credentials or data.
            tracing::warn!(owner = %self.identity.incarnation, stage,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "native checkpoint deferred");
        } else if matches!(result, Ok(true)) && stage != "prepare" {
            // Exclude no-op polls and discarded warm caches. This is the whole
            // checkpoint call's elapsed time, not the last stage's duration.
            tracing::info!(owner = %self.identity.incarnation, stage,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "native checkpoint completed");
        }
        result
    }

    async fn maintain_checkpoint(
        &mut self,
        stage: &mut &'static str,
        started: Instant,
    ) -> anyhow::Result<bool> {
        let identity = self.identity;
        let progress = |current: &mut &'static str, next: &'static str| {
            *current = next;
            tracing::info!(owner = %identity.incarnation, stage = next,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "native checkpoint progress");
        };
        self.check()?;
        if (!self.checkpoint_due() && !self.maintenance.handoff)
            || self.writer.pending_batches() != 0
        {
            return Ok(false);
        }
        // Even an idle, empty successor needs its own durable cut, otherwise
        // every replacement revisits all earlier owners. This uses the normal
        // checkpoint path and funding. Handoff alone does not mark capacity as
        // exhausted, but maintain() holds the serving write lock through I/O.
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
            return Ok(true);
        }
        if self.maintenance.publishing.is_none() {
            // Set the gate before any await: cancellation and request-driven
            // maintenance must not bypass pacing or multiply charged uploads.
            let now = Instant::now();
            if self.maintenance.retry.is_some_and(|(at, _)| now < at) {
                return Ok(false);
            }
            let delay = self
                .maintenance
                .retry
                .map_or(Duration::from_secs(1), |(_, prior)| {
                    (prior * 2).min(Duration::from_secs(30))
                });
            self.maintenance.retry = Some((now + delay, delay));
            // Avoid spending maintenance credit for a permanently revoked owner.
            // Publication still requires its exact conditional comparison.
            progress(stage, "permission");
            self.validate().await?;
            let origin = {
                let _request = self.control.request.lock().await;
                // A failed/cancelled refresh clears the cached confirmation,
                // while the live permit can remain valid. Retry the exact
                // comparison instead of getting stuck on the missing origin.
                if self.control.lock()?.recovery_record().is_err() {
                    progress(stage, "confirmation");
                    let _ = self.control.refresh_locked().await?;
                }
                self.control.lock()?.recovery_record()?
            };
            let cut = root::Cut {
                identity: self.identity,
                prefix: self.writer.durable_prefix(),
            };
            let mut files = self.maintenance.files.clone();
            if self.database_budget(catalog::CATALOG.name)?.is_some() {
                progress(stage, "allowance");
                let costs = self.projection.checkpoint_charges().await?;
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
                            self.cached_budget(&name)?
                                .context("missing tenant allocator")?,
                            bytes,
                        ))
                    })
                    .collect::<anyhow::Result<_>>()?;
                progress(stage, "funding");
                if let Err(error) =
                    crate::upload_budget::Allocator::fund_maintenance(&charges, |before| {
                        self.control.lock()?.check_funding_origin(before, origin)
                    })
                    .await
                {
                    // Collection/takeover can invalidate the captured origin.
                    // Refresh control before the paced retry; do not upload or
                    // reconstruct any rights from an uncertain funding reply.
                    let _ = self.control.refresh().await;
                    return Err(error);
                }
                files = crate::upload_budget::Attempt::bounded(files, total);
            }
            progress(stage, "staging");
            let staged = checkpoint::write_projection(&self.projection, files, origin, cut).await?;
            let snapshot = checkpoint::Snapshot::new(
                self.maintenance.files.clone(),
                self.maintenance.cache.clone(),
                staged.reference(),
            );
            self.maintenance.publishing = Some(Publishing {
                staged,
                snapshot,
                attempted: false,
            });
        }
        progress(stage, "publication");
        let _request = self.control.request.lock().await;
        let pending = self.maintenance.publishing.as_mut().unwrap();
        for attempt in 0..2 {
            if !pending.attempted {
                progress(stage, "publication");
                let staged = Staged {
                    origin: pending.staged.origin,
                    reference: pending.staged.reference,
                };
                let publication = if attempt == 0 {
                    // Funding and staging can advance the register. Read their
                    // result and let publication validate it in the same CAS.
                    let observed = self.control.lock()?.observe();
                    let observed = observed.await?;
                    let mut root = self.control.lock()?;
                    // The request gate excludes live control calls. Only a
                    // cancelled earlier validation can still be pending here.
                    root.abandon();
                    root.publish_checkpoint_observed(observed, staged)
                } else {
                    // The retry below already confirmed a fresh comparison.
                    self.control.lock()?.publish_checkpoint(staged)
                };
                match publication {
                    Ok(publication) => {
                        // Cancellation may leave a sent CAS. Retain uncertainty
                        // until exact validation confirms the new or old reference.
                        pending.attempted = true;
                        let completed = publication.execute().await;
                        let _outcome = self.control.lock()?.finish(completed)?;
                    }
                    // Resolve a changed origin through the same confirmation
                    // path, which discards collected staging before rebuilding.
                    Err(root::Error::StaleRecovery) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            let published = self
                .control
                .lock()?
                .recovery_record()
                .ok()
                .is_some_and(|r| r.checkpoint == Some(pending.staged.reference));
            if !published {
                progress(stage, "confirmation");
                let _outcome = self.control.refresh_locked().await?;
                let record = self.control.lock()?.recovery_record()?;
                if record.checkpoint != Some(pending.staged.reference) {
                    if record.collection != pending.staged.origin.collection {
                        self.maintenance.publishing = None;
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
        self.control.check(self.identity)?;
        let pending = self.maintenance.publishing.take().unwrap();
        self.projection.rebase(pending.snapshot);
        self.active.clear();
        self.validation = validation::Usage::default();
        self.maintenance.covered = self.writer.durable_prefix().position();
        self.maintenance.recovered = 0;
        self.maintenance.handoff = false;
        self.maintenance.requested = false;
        self.maintenance.retry = None;
        Ok(true)
    }
}
