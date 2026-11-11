//! Exact root acquisition, permanent predecessor revocation and fresh response
//! validation. Remote serving has no clock bound. Local directories retain an
//! exclusive OS guard. Journal recovery must precede service in either mode.
mod catalog;
mod collection;
mod metadata;
mod permit;
mod registration;
mod replay;

use crate::providers::control;
use crate::{
    journal::{QueueLimits, Writer},
    storage::Storage,
};
pub(crate) use collection::Collection;
use metadata::Phase;
pub(crate) use metadata::Record;
pub(crate) use metadata::{VERSION, budget as metadata_budget};
pub(crate) use permit::validate_key as permit_key;
use std::{net::SocketAddr, sync::Arc};
use uuid::Uuid;

pub use catalog::{JournalRecovery, Recovery};
pub use control::Outcome;
pub use registration::{Registration, Registrations};
pub(crate) use replay::Cut;
pub use replay::{Replay, ReplayBatch};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid or incompatible ownership metadata, prefix or storage format")]
    Incompatible,
    #[error("this incarnation is already bound; a new claim needs a fresh controller")]
    Bound,
    #[error("observation does not name this incarnation and its bound epoch")]
    NotOwner,
    #[error("ownership epoch exhausted")]
    Exhausted,
    #[error("a control request is already pending")]
    Busy,
    #[error("activation requires this incarnation's confirmed fence")]
    NotReady,
    #[error("controller retired after an invalid confirmation")]
    Retired,
    #[error("this incarnation's journal writer has already been opened")]
    JournalOpened,
    #[error("the confirmed recovery catalog changed; restart from a fresh snapshot")]
    StaleRecovery,
    #[error("the namespace already retains a different checkpoint; release it explicitly first")]
    RetentionOccupied,
    #[error("control provider observation failed")]
    Provider(#[from] anyhow::Error),
    #[error(transparent)]
    Control(#[from] control::Error),
}

// Local directories retain their released encoding. Remote namespaces use a
// new format so an older timed writer can never join a permit-based controller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Encoding {
    pub local: bool,
}
impl Encoding {
    pub(crate) const REMOTE: Self = Self { local: false };
    pub(crate) const LOCAL: Self = Self { local: true };
    fn legacy_lifetime(self) -> u64 {
        u64::from(self.local)
    }
}

/// A journal/placement identity, not a transferable serving capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub epoch: u64,
    pub incarnation: Uuid,
}

/// A routing hint, never a serving capability. The destination must check the
/// incarnation, authorization and a fresh permit for every response group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    pub identity: Identity,
    pub address: SocketAddr,
}
pub(crate) fn valid_address(address: SocketAddr) -> Result<(), Error> {
    if address.port() == 0
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address, SocketAddr::V6(a) if a.scope_id() != 0 || a.flowinfo() != 0)
        || address.ip() == std::net::Ipv4Addr::BROADCAST
    {
        return Err(Error::Incompatible);
    }
    Ok(())
}

/// A namespace backup reference protected from collection until explicitly
/// released. It includes only the exact checkpoint cut, not subsequent writes.
/// This is an administrator-owned descriptor; never expose it to a tenant.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RetainedCheckpoint {
    id: Uuid,
    reference: crate::index::recovery::checkpoint::Reference,
}
impl RetainedCheckpoint {
    pub fn id(self) -> Uuid {
        self.id
    }
    pub fn reference(self) -> crate::index::recovery::checkpoint::Reference {
        self.reference
    }
}

/// A raw provider observation. It never establishes permission.
pub struct Observation {
    raw: control::Observation,
    record: Option<Record>,
}

impl Observation {
    /// May be stale, or precede journal recovery. Only the named native backend
    /// can serve it, after its ordinary authorization and permission checks.
    pub fn route(&self) -> Option<Route> {
        let record = self.record?;
        (record.phase == Phase::Open).then_some(Route {
            identity: record.identity,
            address: record.address?,
        })
    }
}

struct Pending {
    id: Arc<()>,
    record: Record,
}

/// One non-reusable candidate incarnation. Only a fresh controller can target
/// another predecessor. Retained observations/requests need caller-side bounds.
pub struct Root {
    files: Arc<dyn Storage>,
    session: control::Session,
    encoding: Encoding,
    prefix: String,
    incarnation: Uuid,
    address: Option<SocketAddr>,
    epoch: Option<u64>,
    pending: Option<Pending>,
    local_guard: Option<Arc<crate::storage::Local>>,
    pid: u32,
    active: bool,
    retired: bool,
    journal_opened: bool,
    budgeted: bool,
}

impl Root {
    pub(crate) fn check_funding_origin(
        &self,
        bucket: &crate::publication::Bucket,
        origin: Record,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            metadata::decode(&bucket.info, &self.prefix, self.encoding, self.budgeted)?
                .is_some_and(|record| record.same_recovery(origin)),
            "maintenance funding origin changed"
        );
        Ok(())
    }
    pub(crate) fn collection(&self) -> Collection {
        Collection::new(
            self.session.authority(),
            self.files.clone(),
            self.prefix.clone(),
            self.encoding,
            self.budgeted,
        )
    }

    pub(crate) fn database_budget(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<Arc<crate::upload_budget::Allocator>>> {
        self.budgeted
            .then(|| {
                crate::upload_budget::Allocator::database(
                    self.session.authority(),
                    self.files.clone(),
                    name,
                )
                .map(Arc::new)
            })
            .transpose()
    }

    pub(crate) fn new(
        files: Arc<dyn Storage>,
        session: control::Session,
        prefix: &str,
        encoding: Encoding,
        budgeted: bool,
        local_guard: Option<Arc<crate::storage::Local>>,
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
            encoding,
            prefix: prefix.into(),
            incarnation: Uuid::new_v4(),
            address: None,
            epoch: None,
            pending: None,
            local_guard,
            pid: std::process::id(),
            active: false,
            retired: false,
            journal_opened: false,
            budgeted,
        })
    }

    /// Advertise a peer address atomically with this fresh claim. Configure it
    /// before claiming; a successor never inherits a predecessor's address.
    pub fn advertise(mut self, address: SocketAddr) -> Result<Self, Error> {
        self.available()?;
        valid_address(address)?;
        if self.epoch.is_some() {
            return Err(Error::Bound);
        }
        if self.encoding.local {
            return Err(Error::Incompatible);
        }
        self.address = Some(address);
        Ok(self)
    }

    /// The returned future owns its request state. Await it outside the root's
    /// mutex so an observation cannot stall response validation.
    pub fn observe(&self) -> impl Future<Output = Result<Observation, Error>> + Send + use<> {
        let available = self.available();
        let observation = self.session.observe();
        let prefix = self.prefix.clone();
        let encoding = self.encoding;
        let budgeted = self.budgeted;
        async move {
            available?;
            let raw = observation.await?;
            let record = metadata::decode(raw.metadata(), &prefix, encoding, budgeted)?;
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
            collection: collection::generation(observed.raw.metadata())
                .map_err(|_| Error::Incompatible)?,
            identity: Identity {
                epoch,
                incarnation: self.incarnation,
            },
            phase: Phase::Fenced,
            address: self.address,
            catalog: observed.record.and_then(|r| r.catalog),
            checkpoint: observed.record.and_then(|r| r.checkpoint),
            retained: observed.record.and_then(|r| r.retained),
        };
        let registration = Registration::new(record.identity, observed.record.map(|r| r.identity));
        let mut prepared = self.begin(observed.raw, record)?;
        prepared.registration =
            Some(registration.prepare(self.files.clone(), &self.prefix, self.encoding));
        if !self.encoding.local {
            prepared.permit = Some(permit::Prepared::new(self.files.clone(), self.incarnation));
        }
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
        self.begin(observed.raw, record)
    }

    pub fn activate(&mut self) -> Result<Prepared, Error> {
        self.available()?;
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if self.active {
            return Err(Error::NotReady);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        let mut record =
            metadata::decode(raw.metadata(), &self.prefix, self.encoding, self.budgeted)?
                .ok_or(Error::NotReady)?;
        if record.phase != Phase::Fenced
            || Some(record.identity.epoch) != self.epoch
            || record.identity.incarnation != self.incarnation
        {
            return Err(Error::NotReady);
        }
        record.phase = Phase::Open;
        let mut prepared = self.begin(raw, record)?;
        if !self.encoding.local {
            prepared.revoke = Some(Revocation {
                registrations: self.registrations()?,
                authority: self.session.authority(),
                files: self.files.clone(),
                cut: record.checkpoint.map(|reference| reference.cut.identity),
            });
        }
        Ok(prepared)
    }

    fn begin(&mut self, observed: control::Observation, record: Record) -> Result<Prepared, Error> {
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let metadata = metadata::encode(
            observed.metadata(),
            &self.prefix,
            self.encoding,
            record,
            self.budgeted,
        )?;
        let raw = self.session.begin(observed, metadata)?;
        let id = Arc::new(());
        self.pending = Some(Pending {
            id: id.clone(),
            record,
        });
        Ok(Prepared {
            id,
            raw,
            registration: None,
            permit: None,
            revoke: None,
            catalog: None,
        })
    }

    /// Discover and select ancestral journals after activation. An unconfirmed
    /// publication is an error with a potentially committed outcome: revalidate
    /// this exact incarnation before continuing recovery.
    pub async fn select_journals(&mut self) -> Result<(), Error> {
        loop {
            let mut recovery = self.recovery()?;
            let mut pending = None;
            while let Some(mut journal) = recovery.next().await? {
                if !journal.is_selected() {
                    while journal.read_next().await?.is_some() {}
                    pending = Some(journal);
                    break;
                }
            }
            let Some(journal) = pending else {
                return Ok(());
            };
            let select = self.select_prefix(journal)?;
            if !matches!(self.finish(select.execute().await)?, Outcome::Confirmed) {
                return Err(Error::Provider(anyhow::anyhow!(
                    "journal selection unconfirmed; revalidate before recovery"
                )));
            }
        }
    }

    /// Abandon local interest; the remote operation may still apply. This grants
    /// no permission; every response still requires fresh validation.
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
        let record = metadata::decode(raw.metadata(), &self.prefix, self.encoding, self.budgeted)?;
        if record != Some(pending.record) {
            return Err(Error::Incompatible);
        }
        self.active = pending.record.phase == Phase::Open;
        Ok(())
    }

    /// Structural admission check only. Never use this as serving permission.
    pub(crate) fn identity(&self) -> Result<Option<Identity>, Error> {
        self.available()?;
        Ok(self.active.then(|| Identity {
            epoch: self.epoch.expect("active root has an epoch"),
            incarnation: self.incarnation,
        }))
    }

    /// Issue only AFTER sealing a response and completing its durable prefix.
    /// This future owns its state; no root mutex or reusable permission token
    /// spans I/O. A late success applies only to that already sealed response.
    pub fn permission(
        &self,
    ) -> impl Future<Output = Result<Option<Identity>, Error>> + Send + use<> {
        let identity = self.identity();
        let files = self.files.clone();
        let local = self.local_guard.clone();
        let pid = self.pid;
        async move {
            if pid != std::process::id() {
                return Err(Error::Retired);
            }
            let Some(identity) = identity? else {
                return Ok(None);
            };
            let permitted = local.is_some() || permit::live(&files, identity.incarnation).await?;
            if pid != std::process::id() {
                return Err(Error::Retired);
            }
            if permitted {
                Ok(Some(identity))
            } else {
                Ok(None)
            }
        }
    }

    /// Open this incarnation's writer exactly once, after activation.
    /// Recovery and selected-prefix publication remain separate requirements
    /// before accepting or acknowledging data through the returned journal.
    pub fn take_journal(&mut self, limits: QueueLimits) -> Result<Writer, Error> {
        if self.journal_opened {
            return Err(Error::JournalOpened);
        }
        self.identity()?.ok_or(Error::NotReady)?;
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
            self.encoding,
            Identity {
                epoch,
                incarnation: self.incarnation,
            },
        ))
    }

    /// Begin old-journal discovery only after activation has revoked predecessors.
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

    /// Read the last confirmed backup slot. After a lost reply or a restart,
    /// first validate current ownership. The descriptor is not a read lease:
    /// administrators must coordinate release with their backup readers.
    pub fn retained_checkpoint(&mut self) -> Result<Option<RetainedCheckpoint>, Error> {
        Ok(self.recovery_record()?.retained)
    }

    /// Retain the currently published checkpoint in the one namespace backup
    /// slot. The returned identity is tentative until finish confirms the CAS.
    /// A conflict requires a fresh operation; an uncertain reply requires
    /// validation and checking the retained identity, never blind recreation.
    pub fn retain_checkpoint(&mut self) -> Result<(RetainedCheckpoint, Prepared), Error> {
        let mut record = self.recovery_record()?;
        if record.retained.is_some() {
            return Err(Error::RetentionOccupied);
        }
        let retained = RetainedCheckpoint {
            id: Uuid::new_v4(),
            reference: record.checkpoint.ok_or(Error::NotReady)?,
        };
        record.retained = Some(retained);
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        Ok((retained, self.begin(raw, record)?))
    }

    /// Explicit, identity-checked release. An empty slot is an idempotent
    /// success only after the comparison confirms; another identity is refused.
    /// This does not release the current checkpoint's own collection protection.
    pub fn release_checkpoint(&mut self, id: Uuid) -> Result<Prepared, Error> {
        let mut record = self.recovery_record()?;
        if record.retained.is_some_and(|r| r.id != id) {
            return Err(Error::RetentionOccupied);
        }
        record.retained = None;
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        self.begin(raw, record)
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
        if !record.same_recovery(staged.origin) {
            return Err(Error::StaleRecovery);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        self.begin(
            raw,
            Record {
                checkpoint: Some(staged.reference),
                ..record
            },
        )
    }

    pub(crate) fn recovery_record(&mut self) -> Result<Record, Error> {
        let identity = self.identity()?.ok_or(Error::NotReady)?;
        let raw = self.session.confirmed().ok_or(Error::NotReady)?;
        let record = metadata::decode(raw.metadata(), &self.prefix, self.encoding, self.budgeted)?
            .ok_or(Error::NotReady)?;
        if record.identity != identity || record.phase != Phase::Open {
            return Err(Error::NotReady);
        }
        Ok(record)
    }

    /// Persist a complete discovery against an unchanged confirmed catalog.
    /// Validation can refresh the comparison while a scan is running. A changed
    /// catalog requires a new snapshot; an already selected prefix never extends.
    pub fn select_prefix(&mut self, recovered: JournalRecovery) -> Result<Prepared, Error> {
        let identity = self.identity()?.ok_or(Error::NotReady)?;
        if recovered.record.identity != identity {
            return Err(Error::NotOwner);
        }
        let raw = self.session.confirmed().cloned().ok_or(Error::NotReady)?;
        let confirmed =
            metadata::decode(raw.metadata(), &self.prefix, self.encoding, self.budgeted)?
                .ok_or(Error::NotReady)?;
        if !confirmed.same_recovery(recovered.record) {
            return Err(Error::StaleRecovery);
        }
        let id = Uuid::new_v4();
        let catalog = recovered.publication(self.files.clone(), id)?;
        let mut record = confirmed;
        record.catalog = Some(id);
        let mut prepared = self.begin(raw, record)?;
        prepared.catalog = Some(catalog);
        Ok(prepared)
    }

    pub(crate) fn retire(&mut self) {
        self.retired = true;
        self.active = false;
    }

    fn available(&self) -> Result<(), Error> {
        if self.retired || self.pid != std::process::id() {
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
    permit: Option<permit::Prepared>,
    revoke: Option<Revocation>,
    catalog: Option<catalog::Publication>,
}

struct Revocation {
    registrations: Registrations,
    authority: Arc<dyn crate::publication::Authority>,
    files: Arc<dyn Storage>,
    cut: Option<Identity>,
}

impl Prepared {
    pub async fn execute(self) -> Completed {
        let raw = async {
            if let Some(registration) = self.registration {
                registration.execute().await?;
            }
            if let Some(permit) = self.permit {
                permit.execute().await?;
            }
            if let Some(Revocation {
                mut registrations,
                authority,
                files,
                cut,
            }) = self.revoke
            {
                // Keep registrations: deleting permission must not erase ancestry.
                let own = registrations.next().await?.ok_or(Error::NotReady)?;
                let mut reached = cut.is_none();
                while let Some(old) = registrations.next().await? {
                    permit::revoke(&files, &authority, old.identity().incarnation).await?;
                    // A published checkpoint's owner completed revocation before
                    // it could serve. Revoke that owner too; earlier registrations
                    // need not survive collection. Never stop at an unfinished claim.
                    if Some(old.identity()) == cut {
                        reached = true;
                        break;
                    }
                }
                anyhow::ensure!(reached, "checkpoint owner missing from ancestry");
                anyhow::ensure!(
                    permit::live(&files, own.identity().incarnation).await?,
                    "activation permit revoked"
                );
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
