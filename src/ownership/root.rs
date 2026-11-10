//! Root ownership control for the candidate engine, under an injected clock contract.
//!
//! A successful comparison earns an interval anchored to its request. A first
//! confirmation begins a conservative drain; activation never extends the interval.
//! This is not a serving engine: journal selection/recovery and data-view checks
//! must still precede every permission check. There is no system clock adapter.
mod catalog;
mod metadata;
mod registration;
mod replay;

use super::timing::{self, Bounds, Clock, Deadline, Drain, Timeline};
use crate::providers::control;
use crate::{
    journal::{QueueLimits, Writer},
    storage::Storage,
};
use metadata::Phase;
pub(crate) use metadata::Record;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub use catalog::{JournalRecovery, Recovery};
pub use control::Outcome;
pub use registration::{Registration, Registrations};
pub(crate) use replay::Cut;
pub use replay::{Replay, ReplayBatch};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid or incompatible ownership metadata, prefix or timing profile")]
    Incompatible,
    #[error("this incarnation is already bound; a new claim needs a fresh controller")]
    Bound,
    #[error("observation does not name this incarnation and its bound epoch")]
    NotOwner,
    #[error("ownership epoch exhausted")]
    Exhausted,
    #[error("a control request is already pending")]
    Busy,
    #[error("activation requires a confirmed fence, completed drain and live interval")]
    NotReady,
    #[error("controller retired after an invalid confirmation")]
    Retired,
    #[error("this incarnation's journal writer has already been opened")]
    JournalOpened,
    #[error("the confirmed recovery catalog changed; restart from a fresh snapshot")]
    StaleRecovery,
    #[error("control provider observation failed")]
    Provider(#[from] anyhow::Error),
    #[error(transparent)]
    Timing(#[from] timing::Error),
    #[error(transparent)]
    Control(#[from] control::Error),
}

/// Every participant must use the same durably recorded profile. Constructing
/// this value checks arithmetic, not physical clock behavior or deployment safety.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Profile {
    bounds: Bounds,
    error: Duration,
}

impl Profile {
    pub fn new(lifetime: Duration, error: Duration) -> Result<Self, Error> {
        Ok(Self {
            bounds: Bounds::new(lifetime, error)?,
            error,
        })
    }
}

/// A journal/placement identity, not a transferable serving capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub epoch: u64,
    pub incarnation: Uuid,
}

/// A raw provider observation. It never establishes permission.
pub struct Observation {
    raw: control::Observation,
    record: Option<Record>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Claim,
    Validate,
    Activate,
    Select,
    Checkpoint,
}

struct Pending {
    id: Arc<()>,
    kind: Kind,
    record: Record,
    deadline: Deadline,
}

/// One non-reusable candidate incarnation. Only a fresh controller can target
/// another predecessor. Retained observations/requests need caller-side bounds.
pub struct Root<C> {
    files: Arc<dyn Storage>,
    session: control::Session,
    timeline: Timeline<C>,
    profile: Profile,
    prefix: String,
    incarnation: Uuid,
    epoch: Option<u64>,
    pending: Option<Pending>,
    deadline: Option<Deadline>,
    drain: Option<Drain>,
    active: bool,
    retired: bool,
    journal_opened: bool,
}

impl<C: Clock> Root<C> {
    pub(crate) fn new(
        files: Arc<dyn Storage>,
        session: control::Session,
        prefix: &str,
        clock: C,
        profile: Profile,
    ) -> Result<Self, Error> {
        if prefix.is_empty()
            || prefix.len() > 128
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            return Err(Error::Incompatible);
        }
        Ok(Self {
            files,
            session,
            timeline: Timeline::new(clock, profile.bounds)?,
            profile,
            prefix: prefix.into(),
            incarnation: Uuid::new_v4(),
            epoch: None,
            pending: None,
            deadline: None,
            drain: None,
            active: false,
            retired: false,
            journal_opened: false,
        })
    }

    /// The returned future owns its request state. Await it outside the root's
    /// mutex so an observation cannot stall local permission checks or renewals.
    pub fn observe(&self) -> impl Future<Output = Result<Observation, Error>> + Send + use<C> {
        let available = self.available();
        let observation = self.session.observe();
        let prefix = self.prefix.clone();
        let profile = self.profile;
        async move {
            available?;
            let raw = observation.await?;
            let record = metadata::decode(raw.metadata(), &prefix, profile)?;
            Ok(Observation { raw, record })
        }
    }

    /// Capture a single predecessor and fresh incarnation before sending. Even
    /// timeout or conflict cannot retarget this identity to another generation.
    pub fn claim(&mut self, observed: Observation) -> Result<Prepared, Error> {
        self.available()?;
        if self.epoch.is_some() {
            return Err(Error::Bound);
        }
        let epoch = observed
            .record
            .map_or(0, |r| r.identity.epoch)
            .checked_add(1)
            .ok_or(Error::Exhausted)?;
        let record = Record {
            identity: Identity {
                epoch,
                incarnation: self.incarnation,
            },
            phase: Phase::Fenced,
            catalog: observed.record.and_then(|r| r.catalog),
            checkpoint: observed.record.and_then(|r| r.checkpoint),
        };
        let registration = Registration::new(record.identity, observed.record.map(|r| r.identity));
        let mut prepared = self.begin(observed.raw, record, Kind::Claim)?;
        prepared.registration =
            Some(registration.prepare(self.files.clone(), &self.prefix, self.profile));
        self.epoch = Some(epoch);
        Ok(prepared)
    }

    /// Validate exactly the captured incarnation/epoch, including after an
    /// uncertain claim or activation. Merely observing one's own record is insufficient.
    pub fn validate(&mut self, observed: Observation) -> Result<Prepared, Error> {
        self.available()?;
        let record = observed.record.ok_or(Error::NotOwner)?;
        if Some(record.identity.epoch) != self.epoch
            || record.identity.incarnation != self.incarnation
        {
            return Err(Error::NotOwner);
        }
        self.begin(observed.raw, record, Kind::Validate)
    }

    pub fn activate(&mut self) -> Result<Prepared, Error> {
        self.available()?;
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if self.active || !self.drained()? || !self.live_interval()? {
            return Err(Error::NotReady);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        let mut record =
            metadata::decode(raw.metadata(), &self.prefix, self.profile)?.ok_or(Error::NotReady)?;
        if record.phase != Phase::Fenced
            || Some(record.identity.epoch) != self.epoch
            || record.identity.incarnation != self.incarnation
        {
            return Err(Error::NotReady);
        }
        record.phase = Phase::Open;
        self.begin(raw, record, Kind::Activate)
    }

    fn begin(
        &mut self,
        observed: control::Observation,
        record: Record,
        kind: Kind,
    ) -> Result<Prepared, Error> {
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let metadata = metadata::encode(observed.metadata(), &self.prefix, self.profile, record)?;
        let start = self.timeline.begin()?;
        let deadline = self.timeline.root_deadline(&start)?;
        let raw = self.session.begin(observed, metadata)?;
        let id = Arc::new(());
        self.pending = Some(Pending {
            id: id.clone(),
            kind,
            record,
            deadline,
        });
        Ok(Prepared {
            id,
            raw,
            registration: None,
            catalog: None,
        })
    }

    /// Abandon local interest; the remote operation may still apply. A previously
    /// earned active interval survives, without any extension.
    pub fn abandon(&mut self) {
        self.pending = None;
        self.session.abandon();
    }

    pub fn finish(&mut self, completed: Completed) -> Result<Outcome, Error> {
        self.available()?;
        if self
            .pending
            .as_ref()
            .is_none_or(|p| !Arc::ptr_eq(&p.id, &completed.id))
        {
            return Ok(Outcome::Ignored);
        }
        let outcome = match completed.raw {
            Ok(raw) => self.session.finish(raw),
            Err(error) => {
                self.session.abandon();
                Outcome::Uncertain(error)
            }
        };
        if matches!(outcome, Outcome::Ignored) {
            return Ok(outcome);
        }
        let pending = self.pending.take().expect("matched current invocation");
        if matches!(outcome, Outcome::Confirmed)
            && let Err(error) = self.confirm(pending)
        {
            self.retired = true;
            self.active = false;
            self.session.abandon();
            return Err(error);
        }
        Ok(outcome)
    }

    fn confirm(&mut self, pending: Pending) -> Result<(), Error> {
        let raw = self.session.confirmed().ok_or(Error::Incompatible)?;
        let record = metadata::decode(raw.metadata(), &self.prefix, self.profile)?;
        if record != Some(pending.record) {
            return Err(Error::Incompatible);
        }
        if self.drain.is_none() {
            if pending.record.phase != Phase::Fenced {
                return Err(Error::NotReady);
            }
            self.drain = Some(self.timeline.begin_drain()?);
        }
        if pending.record.phase == Phase::Open {
            if !self.drained()? {
                return Err(Error::NotReady);
            }
            self.active = true;
        }
        if pending.kind != Kind::Activate {
            self.deadline = Some(pending.deadline);
        }
        Ok(())
    }

    /// Check AFTER preparing the consistent data view or durable acknowledgement.
    /// A returned identity is a fact about this check, not future permission.
    /// Journal recovery and assignment checks are separate caller obligations.
    pub fn permission(&mut self) -> Result<Option<Identity>, Error> {
        self.available()?;
        if !self.active || !self.live_interval()? {
            return Ok(None);
        }
        Ok(Some(Identity {
            epoch: self.epoch.expect("active incarnation has a bound epoch"),
            incarnation: self.incarnation,
        }))
    }

    /// A child must anchor this duration to its own request start and bind the
    /// reply to a current assignment and invocation. Zero permits no service.
    pub fn child_duration(&mut self) -> Result<Duration, Error> {
        self.available()?;
        if !self.active {
            return Ok(Duration::ZERO);
        }
        match &self.deadline {
            Some(deadline) => Ok(self.timeline.child_lifetime(deadline)?),
            None => Ok(Duration::ZERO),
        }
    }

    /// Open this incarnation's writer exactly once, under a live interval.
    /// Recovery and selected-prefix publication remain separate requirements
    /// before accepting or acknowledging data through the returned journal.
    pub fn take_journal(&mut self, limits: QueueLimits) -> Result<Writer, Error> {
        if self.journal_opened {
            return Err(Error::JournalOpened);
        }
        self.permission()?.ok_or(Error::NotReady)?;
        self.journal_opened = true;
        Ok(Writer::for_root(
            self.files.clone(),
            self.incarnation,
            limits,
        ))
    }

    /// Follow the durable registration chain for this candidate. Only a winning,
    /// recovered controller with current permission may use it for serving.
    pub fn registrations(&self) -> Result<Registrations, Error> {
        self.available()?;
        let epoch = self.epoch.ok_or(Error::NotOwner)?;
        Ok(Registrations::new(
            self.files.clone(),
            self.prefix.clone(),
            self.profile,
            Identity {
                epoch,
                incarnation: self.incarnation,
            },
        ))
    }

    /// Begin old-journal discovery only after activation has completed the drain.
    /// The returned cursor owns its snapshot and can be awaited outside the lock.
    pub fn recovery(&mut self) -> Result<Recovery, Error> {
        let record = self.recovery_record()?;
        Ok(Recovery::new(
            self.files.clone(),
            record,
            self.registrations()?,
        ))
    }

    /// Stream every prior selected journal in transaction order. The cursor
    /// checks catalog completeness before returning frames and holds no root
    /// lock across I/O. Completion still requires a final permission check.
    pub fn replay(&mut self) -> Result<Replay, Error> {
        let record = self.recovery_record()?;
        Ok(Replay::new(self.files.clone(), record))
    }

    /// Capture the confirmed checkpoint identity for immutable readback. The
    /// returned image does not include newer journal entries or grant a lease.
    pub fn checkpoint_reference(
        &mut self,
    ) -> Result<Option<crate::index::recovery::checkpoint::Reference>, Error> {
        Ok(self.recovery_record()?.checkpoint)
    }

    /// Atomically publish a fully uploaded native checkpoint built from this
    /// exact recovery snapshot. Unknown results require exact revalidation; a
    /// late request cannot overwrite a successor's claim or newer checkpoint.
    pub fn publish_checkpoint(
        &mut self,
        staged: crate::index::recovery::checkpoint::Staged,
    ) -> Result<Prepared, Error> {
        let record = self.recovery_record()?;
        if record.identity != staged.origin.identity {
            return Err(Error::NotOwner);
        }
        if record != staged.origin {
            return Err(Error::StaleRecovery);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        self.begin(
            raw,
            Record {
                checkpoint: Some(staged.reference),
                ..record
            },
            Kind::Checkpoint,
        )
    }

    fn recovery_record(&mut self) -> Result<Record, Error> {
        let identity = self.permission()?.ok_or(Error::NotReady)?;
        let raw = self.session.confirmed().ok_or(Error::NotReady)?;
        let record =
            metadata::decode(raw.metadata(), &self.prefix, self.profile)?.ok_or(Error::NotReady)?;
        if record.identity != identity || record.phase != Phase::Open {
            return Err(Error::NotReady);
        }
        Ok(record)
    }

    /// Persist a complete discovery against an unchanged confirmed catalog.
    /// Renewal can refresh the comparison while a scan is running. A changed
    /// catalog requires a new snapshot; an already selected prefix never extends.
    pub fn select_prefix(&mut self, recovered: JournalRecovery) -> Result<Prepared, Error> {
        let identity = self.permission()?.ok_or(Error::NotReady)?;
        if recovered.record.identity != identity {
            return Err(Error::NotOwner);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        let confirmed =
            metadata::decode(raw.metadata(), &self.prefix, self.profile)?.ok_or(Error::NotReady)?;
        if confirmed != recovered.record {
            return Err(Error::StaleRecovery);
        }
        let id = Uuid::new_v4();
        let catalog = recovered.publication(self.files.clone(), id)?;
        let mut record = recovered.record;
        record.catalog = Some(id);
        let mut prepared = self.begin(raw, record, Kind::Select)?;
        prepared.catalog = Some(catalog);
        Ok(prepared)
    }

    fn drained(&mut self) -> Result<bool, Error> {
        match &self.drain {
            Some(drain) => Ok(self.timeline.drain_complete(drain)?),
            None => Ok(false),
        }
    }

    fn live_interval(&mut self) -> Result<bool, Error> {
        match &self.deadline {
            Some(deadline) => Ok(!self.timeline.remaining(deadline)?.is_zero()),
            None => Ok(false),
        }
    }

    fn available(&self) -> Result<(), Error> {
        if self.retired {
            Err(Error::Retired)
        } else {
            Ok(())
        }
    }
}

/// Exactly one provider invocation; neither this nor its completion is cloneable.
#[must_use = "execute or abandon this root request"]
pub struct Prepared {
    id: Arc<()>,
    raw: control::Prepared,
    registration: Option<registration::Prepared>,
    catalog: Option<catalog::Publication>,
}

impl Prepared {
    pub async fn execute(self) -> Completed {
        let raw = async {
            if let Some(registration) = self.registration {
                registration.execute().await?;
            }
            if let Some(catalog) = self.catalog {
                catalog.execute().await?;
            }
            Ok(self.raw.execute().await)
        }
        .await;
        Completed { id: self.id, raw }
    }
}

#[must_use = "finish or abandon this root request"]
pub struct Completed {
    id: Arc<()>,
    raw: anyhow::Result<control::Completed>,
}

#[cfg(test)]
mod tests;
