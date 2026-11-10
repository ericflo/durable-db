//! Native serving under an injected ownership contract. The owner is exclusively
//! borrowed for admission/publication; immutable reads can run concurrently.
//! Uploads and control renewal own their I/O and do not hold that borrow.
//! The HTTP adapter uses catalog-authorized methods. Low-level generation APIs
//! require caller authorization. Physical timing/placement remains external.
mod catalog;
mod control;
pub use control::{Control, Renewal};
mod checkpoint;
pub use catalog::{AuthorizedBatch, AuthorizedTicket, CatalogChange, CatalogResult};
mod commit;
mod read;
use super::{
    record::{Commit, Database, Mutation},
    recovery::{Resolution, Target},
    validation::{self, Read, Validator},
};
use crate::{
    cache::Cache,
    index::{
        recovery::{self, Projection, Recovering},
        table::Entry,
    },
    journal,
    ownership::{
        root::{self, Root},
        timing::Clock,
    },
};
use anyhow::{Context, ensure};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, ops::Bound, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct Limits {
    pub index: recovery::Limits,
    /// Aggregate across active databases, as well as per-database bounds.
    pub validation: validation::Limits,
    pub journal: journal::QueueLimits,
    pub receipt_window: u64,
    pub checkpoint_frames: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            index: recovery::Limits::default(),
            validation: validation::Limits::default(),
            journal: journal::QueueLimits::default(),
            receipt_window: 1024,
            checkpoint_frames: 256,
        }
    }
}

#[derive(Clone)]
pub struct Snapshot {
    name: Arc<str>,
    generation: Uuid,
    token: validation::Snapshot,
}
impl Snapshot {
    pub fn revision(&self) -> u64 {
        self.token.revision()
    }
    fn database(&self) -> Database<'_> {
        Database {
            name: &self.name,
            generation: self.generation,
        }
    }
    fn key(&self) -> (Arc<str>, Uuid) {
        (self.name.clone(), self.generation)
    }
}
pub struct Request<'a> {
    pub snapshot: &'a Snapshot,
    pub id: &'a str,
    pub reads: &'a [Read<'a>],
    /// Evaluated, sorted, unique mutations. Reads must include every dependency
    /// of the application's evaluation, including absent keys and scan ranges.
    pub mutations: &'a [Mutation<'a>],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Validation(validation::Error),
    ReceiptExpired,
    IdentityReused,
    Capacity,
    Quota,
    PhysicalBudget,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pending,
    Committed(u64),
    Rejected(Rejection),
}
/// An opaque result fenced by the journal position on which it depends.
/// This is not an authenticated client token and grants no serving permission.
#[derive(Clone, Copy)]
pub struct Ticket {
    owner: root::Identity,
    position: u64,
    result: Outcome,
    replayed: bool,
}
pub struct Prepared {
    pub tickets: Vec<Ticket>,
    pub upload: Option<journal::PreparedUpload>,
}

/// Recovery is private. Keep `control()` running while storage I/O is pending.
/// Prior journal selection must be complete before constructing this opening.
pub struct Opening<C> {
    control: Control<C>,
    replay: root::Replay,
    limits: Limits,
    cache: Arc<Cache>,
}
impl<C: Clock> Opening<C> {
    pub fn new(mut root: Root<C>, limits: Limits, cache: Arc<Cache>) -> anyhow::Result<Self> {
        Validator::new(0, limits.validation)?;
        ensure!(limits.checkpoint_frames > 0, "invalid checkpoint interval");
        ensure!(
            (1..=super::record::MAX_RECEIPTS).contains(&limits.receipt_window)
                && limits.receipt_window < limits.index.receipts as u64,
            "invalid live receipt window"
        );
        let replay = root.replay()?;
        Ok(Self {
            control: Control::new(root),
            replay,
            limits,
            cache,
        })
    }
    pub fn control(&self) -> Control<C> {
        self.control.clone()
    }
    pub async fn recover(self) -> anyhow::Result<Owner<C>> {
        let files = self.replay.files();
        let mut recovered =
            Recovering::begin_cached(self.replay, self.limits.index, self.cache.clone()).await?;
        while !recovered.is_complete() {
            recovered = recovered.advance(64).await?;
        }
        let (origin, projection, frames) = recovered.finish_bound()?;
        let writer = {
            let _request = self.control.request.lock().await;
            let mut root = self.control.lock()?;
            ensure!(
                root.recovery_record()? == origin,
                "recovery snapshot changed"
            );
            root.take_journal(self.limits.journal)?
        };
        Ok(Owner {
            control: self.control,
            identity: origin.identity,
            projection,
            writer,
            limits: self.limits,
            active: BTreeMap::new(),
            budgets: BTreeMap::new(),
            pending: BTreeMap::new(),
            catalog_pending: BTreeMap::new(),
            reserved: recovery::Usage::default(),
            validation: validation::Usage::default(),
            failed: false,
            maintenance: checkpoint::Maintenance::new(files, self.cache, frames),
        })
    }
}
struct Active {
    validator: Validator,
    floor: u64,
}
struct Pending {
    fingerprint: [u8; 32],
    ticket: Ticket,
}
type RequestKey = (Arc<str>, Uuid, Arc<str>);
pub struct Owner<C> {
    control: Control<C>,
    identity: root::Identity,
    projection: Projection,
    writer: journal::Writer,
    limits: Limits,
    active: BTreeMap<(Arc<str>, Uuid), Active>,
    // Local rights survive checkpoint eviction but never owner replacement.
    budgets: BTreeMap<Arc<str>, Arc<crate::upload_budget::Allocator>>,
    pending: BTreeMap<RequestKey, Pending>,
    catalog_pending: BTreeMap<Arc<str>, catalog::PendingChange>,
    reserved: recovery::Usage,
    validation: validation::Usage,
    failed: bool,
    maintenance: checkpoint::Maintenance,
}
impl<C: Clock> Owner<C> {
    pub fn control(&self) -> Control<C> {
        self.control.clone()
    }
    pub(crate) fn collection(&self) -> anyhow::Result<Option<crate::ownership::root::Collection>> {
        Ok(self.control.lock()?.collection())
    }
    pub(crate) fn database_budget(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<Arc<crate::upload_budget::Allocator>>> {
        self.control.lock()?.database_budget(name)
    }
    async fn charge_database(
        &mut self,
        database: Database<'_>,
        bytes: usize,
    ) -> anyhow::Result<bool> {
        if database.name == catalog::CATALOG.name
            && database.generation == catalog::CATALOG.generation
        {
            return Ok(true);
        }
        if !self.budgets.contains_key(database.name) {
            let Some(budget) = self.database_budget(database.name)? else {
                return Ok(true);
            };
            if self.budgets.len() >= self.limits.index.databases {
                self.budgets.pop_first();
            }
            self.budgets.insert(Arc::from(database.name), budget);
        }
        match self.budgets[database.name].charge(bytes as u64).await {
            Ok(()) => Ok(true),
            Err(e)
                if e.downcast_ref::<crate::upload_budget::Exhausted>()
                    .is_some_and(|e| e.database) =>
            {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
    pub fn usage(&self) -> recovery::Usage {
        self.projection.usage()
    }
    pub(crate) fn check(&self) -> anyhow::Result<()> {
        ensure!(!self.failed, "owner retired after incomplete publication");
        self.control.check(self.identity)
    }
    async fn warm(&mut self, database: Database<'_>) -> anyhow::Result<()> {
        let key = (Arc::from(database.name), database.generation);
        if !self.active.contains_key(&key) {
            let (revision, floor) = self
                .projection
                .warm(
                    database,
                    self.reserved.receipts,
                    self.reserved.receipt_bytes,
                )
                .await?;
            self.active.insert(
                key,
                Active {
                    validator: Validator::new(revision, self.limits.validation)?,
                    floor,
                },
            );
        }
        Ok(())
    }
    pub async fn snapshot(&mut self, database: Database<'_>) -> anyhow::Result<Snapshot> {
        self.check()?;
        self.maintain().await?;
        self.warm(database).await?;
        let name: Arc<str> = database.name.into();
        let token = self.active[&(name.clone(), database.generation)]
            .validator
            .snapshot();
        let snapshot = Snapshot {
            name,
            generation: database.generation,
            token,
        };
        self.check()?;
        Ok(snapshot)
    }
    fn check_snapshot(&self, snapshot: &Snapshot) -> anyhow::Result<()> {
        self.check()?;
        ensure!(
            self.active.get(&snapshot.key()).is_some_and(
                |a| a.validator.snapshot().incarnation() == snapshot.token.incarnation()
            ),
            "snapshot expired"
        );
        Ok(())
    }
    pub async fn get(&self, snapshot: &Snapshot, key: &str) -> anyhow::Result<Option<Entry>> {
        self.check_snapshot(snapshot)?;
        let view = self
            .projection
            .view(snapshot.database())
            .await?
            .context("missing live view")?;
        let result = view.get(key.as_bytes(), snapshot.revision()).await?;
        self.check()?;
        Ok(result)
    }
    /// Group up to 128 transactions into one immutable PUT. No await occurs
    /// between the first accepted revision and creation of its upload handle.
    pub async fn prepare(&mut self, requests: &[Request<'_>]) -> anyhow::Result<Prepared> {
        self.check()?;
        self.maintain().await?;
        ensure!(
            !self.checkpoint_due(),
            "checkpoint waiting for pending uploads"
        );
        self.prepare_hashed(requests, None).await
    }
    async fn prepare_hashed(
        &mut self,
        requests: &[Request<'_>],
        fingerprints: Option<&[[u8; 32]]>,
    ) -> anyhow::Result<Prepared> {
        self.check()?;
        ensure!(
            (1..=128).contains(&requests.len()),
            "invalid transaction batch size"
        );
        let mut length = 116usize; // journal envelope plus per-record lengths
        for request in requests {
            length += 4 + Commit::encoded_len(
                request.snapshot.database(),
                request.id,
                request.mutations,
            )?;
            ensure!(
                length <= journal::MAX_BATCH_BYTES,
                "transaction batch too large"
            );
            validation::validate_inputs(
                request.reads,
                &request.mutations.iter().map(|m| m.key).collect::<Vec<_>>(),
            )?;
        }
        let fingerprints = match fingerprints {
            Some(fingerprints) => fingerprints.to_vec(),
            None => requests.iter().map(fingerprint).collect(),
        };
        // Only committed metadata is warmed here. Cancellation or storage errors
        // leave no accepted transaction, taken metadata or partial value view.
        let mut resolved = Vec::with_capacity(requests.len());
        for (request, fingerprint) in requests.iter().zip(&fingerprints) {
            self.warm(request.snapshot.database()).await?;
            resolved.push(
                self.projection
                    .view(request.snapshot.database())
                    .await?
                    .unwrap()
                    .resolve(request.id, *fingerprint, request.snapshot.revision())
                    .await?,
            );
        }
        // Charge the stable name before reserving shared journal order. A
        // refusal must not create a hole that prevents other tenants committing.
        // Identical retries in this batch reuse admission; later validation
        // failures may strand credit but can never create new rights.
        let mut admissions = BTreeMap::new();
        let mut admitted = Vec::with_capacity(requests.len());
        for ((request, fingerprint), resolution) in
            requests.iter().zip(&fingerprints).zip(&resolved)
        {
            let key = (
                request.snapshot.name.clone(),
                request.snapshot.generation,
                Arc::from(request.id),
            );
            let allowed = if self.pending.contains_key(&key) || *resolution != Resolution::Missing {
                true
            } else if let Some(allowed) = admissions.get(&(key.clone(), *fingerprint)) {
                *allowed
            } else {
                let bytes = 120
                    + Commit::encoded_len(
                        request.snapshot.database(),
                        request.id,
                        request.mutations,
                    )?;
                let allowed = self
                    .charge_database(request.snapshot.database(), bytes)
                    .await?;
                admissions.insert((key, *fingerprint), allowed);
                allowed
            };
            admitted.push(allowed);
        }
        self.check()?;
        let mut fence = self.writer.planned_position();
        let reservation = self.writer.reserve(length)?;
        let position = reservation.position();
        self.failed = true; // any unexpected error after admission retires the view
        let mut records = Vec::new();
        let mut tickets = Vec::with_capacity(requests.len());
        for (((request, fingerprint), resolved), admitted) in requests
            .iter()
            .zip(fingerprints.iter().copied())
            .zip(resolved)
            .zip(admitted)
        {
            let key = (
                request.snapshot.name.clone(),
                request.snapshot.generation,
                Arc::from(request.id),
            );
            let ticket = if !admitted {
                Ticket {
                    owner: self.identity,
                    replayed: false,
                    position: 0,
                    result: Outcome::Rejected(Rejection::PhysicalBudget),
                }
            } else if let Some(pending) = self.pending.get(&key) {
                if pending.fingerprint == fingerprint {
                    Ticket {
                        replayed: true,
                        ..pending.ticket
                    }
                } else {
                    Ticket {
                        owner: self.identity,
                        replayed: false,
                        position: fence,
                        result: Outcome::Rejected(Rejection::IdentityReused),
                    }
                }
            } else if let Resolution::Committed(revision) = resolved {
                Ticket {
                    owner: self.identity,
                    replayed: true,
                    position: 0,
                    result: Outcome::Committed(revision),
                }
            } else {
                let active = self.active.get_mut(&request.snapshot.key()).unwrap();
                let writes: Vec<_> = request.mutations.iter().map(|m| m.key).collect();
                let plan = if resolved == Resolution::Reused {
                    Err(Rejection::IdentityReused)
                } else if resolved == Resolution::Expired
                    || request.snapshot.revision() < active.floor
                {
                    Err(Rejection::ReceiptExpired)
                } else {
                    active
                        .validator
                        .plan(request.snapshot.token, request.reads, &writes)
                        .map_err(Rejection::Validation)
                };
                let value_bytes: usize = request
                    .mutations
                    .iter()
                    .map(|m| m.key.len() + m.value.map_or(0, <[u8]>::len))
                    .sum();
                let used = self.projection.usage();
                let receipt_bytes = request.id.len() + 40;
                let capacity = used.versions + self.reserved.versions + writes.len()
                    <= self.limits.index.versions
                    && used.value_bytes + self.reserved.value_bytes + value_bytes
                        <= self.limits.index.value_bytes
                    && used.receipts + self.reserved.receipts < self.limits.index.receipts
                    && used.receipt_bytes + self.reserved.receipt_bytes + receipt_bytes
                        <= self.limits.index.receipt_bytes;
                let old = active.validator.usage();
                let plan = plan.and_then(|plan| {
                    let new = plan.usage();
                    if capacity
                        && self.validation.transactions - old.transactions + new.transactions
                            <= self.limits.validation.transactions
                        && self.validation.keys - old.keys + new.keys
                            <= self.limits.validation.key_references
                        && self.validation.bytes - old.bytes + new.bytes
                            <= self.limits.validation.key_bytes
                    {
                        Ok(plan)
                    } else {
                        Err(Rejection::Capacity)
                    }
                });
                match plan {
                    Err(reason) => {
                        if reason == Rejection::Capacity {
                            self.maintenance.requested = true;
                        }
                        Ticket {
                            owner: self.identity,
                            replayed: false,
                            position: fence,
                            result: Outcome::Rejected(reason),
                        }
                    }
                    Ok(plan) => {
                        let revision = plan.revision();
                        let floor = active
                            .floor
                            .max(revision.saturating_sub(self.limits.receipt_window));
                        let record = Commit::encode(
                            request.snapshot.database(),
                            revision,
                            request.snapshot.revision(),
                            floor,
                            request.id,
                            fingerprint,
                            request.mutations,
                        )?;
                        let new = plan.usage();
                        self.validation.transactions =
                            self.validation.transactions - old.transactions + new.transactions;
                        self.validation.keys = self.validation.keys - old.keys + new.keys;
                        self.validation.bytes = self.validation.bytes - old.bytes + new.bytes;
                        active.validator.install(plan);
                        active.floor = floor;
                        self.reserved.versions += writes.len();
                        self.reserved.value_bytes += value_bytes;
                        self.reserved.receipts += 1;
                        self.reserved.receipt_bytes += receipt_bytes;
                        let ticket = Ticket {
                            owner: self.identity,
                            replayed: false,
                            position,
                            result: Outcome::Committed(revision),
                        };
                        self.pending.insert(
                            key,
                            Pending {
                                fingerprint,
                                ticket,
                            },
                        );
                        records.push(record);
                        fence = position;
                        ticket
                    }
                }
            };
            tickets.push(ticket);
        }
        let upload = if records.is_empty() {
            None
        } else {
            Some(reservation.finish(&records)?)
        };
        self.failed = false;
        Ok(Prepared { tickets, upload })
    }
    pub async fn complete(&mut self, completion: journal::Completion) -> anyhow::Result<()> {
        self.check()?;
        self.writer.complete(completion)?;
        self.publish().await?;
        self.maintain().await?;
        Ok(())
    }
    pub async fn resolve_upload(&mut self, position: u64) -> anyhow::Result<journal::Resolution> {
        self.check()?;
        let result = self.writer.resolve(position).await?;
        self.publish().await?;
        self.maintain().await?;
        Ok(result)
    }
    async fn publish(&mut self) -> anyhow::Result<()> {
        self.failed = true;
        while let Some(batch) = self.writer.pop_durable()? {
            for bytes in batch.records() {
                let commit = Commit::decode(bytes)?;
                let database = commit.database();
                let state = self
                    .projection
                    .take_state(database)
                    .await?
                    .advance(&commit)?;
                self.projection.apply(&commit, state).await?;
                self.active
                    .get_mut(&(Arc::from(database.name), database.generation))
                    .context("missing live validator")?
                    .validator
                    .publish_through(commit.revision())?;
                self.pending.remove(&(
                    Arc::from(database.name),
                    database.generation,
                    Arc::from(commit.request_id()),
                ));
                self.reserved.versions -= commit.mutations().len();
                self.reserved.value_bytes -= commit
                    .mutations()
                    .map(|m| m.key.len() + m.value.map_or(0, <[u8]>::len))
                    .sum::<usize>();
                self.reserved.receipts -= 1;
                self.reserved.receipt_bytes -= commit.request_id().len() + 40;
            }
        }
        let durable = self.writer.durable_prefix().position();
        self.catalog_pending
            .retain(|_, change| change.ticket.position > durable);
        self.failed = false;
        Ok(())
    }
    pub fn outcome(&self, ticket: Ticket) -> anyhow::Result<Outcome> {
        ensure!(
            ticket.owner == self.identity,
            "ticket belongs to another owner; resolve its original request"
        );
        let result = if ticket.position <= self.writer.durable_prefix().position() {
            ticket.result
        } else {
            Outcome::Pending
        };
        self.check()?;
        Ok(result)
    }
}

fn fingerprint(request: &Request<'_>) -> [u8; 32] {
    fn field(hash: &mut Sha256, bytes: &[u8]) {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    fn bound(hash: &mut Sha256, bound: Bound<&str>) {
        match bound {
            Bound::Unbounded => hash.update([0]),
            Bound::Included(s) => {
                hash.update([1]);
                field(hash, s.as_bytes());
            }
            Bound::Excluded(s) => {
                hash.update([2]);
                field(hash, s.as_bytes());
            }
        }
    }
    let mut hash = Sha256::new();
    hash.update(b"durable-db/live-request/1");
    field(&mut hash, request.snapshot.name.as_bytes());
    hash.update(request.snapshot.generation.as_bytes());
    hash.update(request.snapshot.token.incarnation().as_bytes());
    hash.update(request.snapshot.revision().to_be_bytes());
    field(&mut hash, request.id.as_bytes());
    hash.update((request.reads.len() as u64).to_be_bytes());
    for read in request.reads {
        match *read {
            Read::Key(key) => {
                hash.update([0]);
                field(&mut hash, key.as_bytes());
            }
            Read::Range { start, end } => {
                hash.update([1]);
                bound(&mut hash, start);
                bound(&mut hash, end);
            }
            Read::Database => hash.update([2]),
        }
    }
    hash.update((request.mutations.len() as u64).to_be_bytes());
    for mutation in request.mutations {
        field(&mut hash, mutation.key.as_bytes());
        match mutation.value {
            None => hash.update([0]),
            Some(value) => {
                hash.update([1]);
                field(&mut hash, value);
            }
        }
    }
    hash.finalize().into()
}
