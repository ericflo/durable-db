//! Server-owned admission and uploads. Disconnecting an HTTP caller only drops
//! its reply; it never cancels accepted journal work or rolls back a revision.
use super::Backend;
use crate::{
    Error, Result,
    auth::Access,
    cache::Cache,
    engine::{
        self, CommitRequest, CommitResult, CreateRequest, CreateResult, Info, PolicyRequest,
        PolicyResult, ReadRequest, ReadResult,
    },
    journal::{self, Resolution},
    metrics::{self, Operation, Outcome},
    transactions::live::{AuthorizedTicket, CatalogChange, CatalogResult, Owner},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, RwLock, RwLockWriteGuard, Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

/// Native HTTP backend for a recovered owner. Remote responses validate a fresh
/// write-once permit after sealing their result. Local directories hold an OS
/// guard. Election and routing are the caller's responsibility.
/// Dropping it retires the worker; accepted writes can have unknown outcomes.
pub struct Native {
    incarnation: uuid::Uuid,
    owner: Arc<Resident>,
    cache: Arc<Cache>,
    queue: mpsc::Sender<Command>,
    permits: Arc<Semaphore>,
    running: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}
// The slot outlives routing retirement and follows every use of this state.
struct Resident {
    state: RwLock<Owner>,
    reservation: Option<Arc<OwnedSemaphorePermit>>,
}
impl std::ops::Deref for Resident {
    type Target = RwLock<Owner>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
enum Write {
    Create(String, CreateRequest),
    Policy(String, PolicyRequest),
    Commit(String, CommitRequest, Access, usize),
}
impl Write {
    fn batch_bytes(&self) -> Option<usize> {
        match self {
            Self::Commit(_, _, _, bytes) => Some(*bytes),
            _ => None,
        }
    }
}
enum Reply {
    Catalog(CatalogResult),
    Commit(CommitResult),
}
struct Response {
    send: oneshot::Sender<Result<Reply>>,
    _permit: OwnedSemaphorePermit,
}
struct Command {
    write: Write,
    response: Response,
}
enum Ticket {
    Catalog(CatalogChange),
    Commit(AuthorizedTicket, Access),
}
struct Pending {
    ticket: Ticket,
    response: Response,
}
struct Running(Arc<AtomicBool>, Option<Arc<Notify>>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
        if let Some(retired) = &self.1 {
            retired.notify_one();
        }
    }
}
fn unavailable() -> Error {
    Error::Storage(anyhow::anyhow!("native worker retired"))
}

impl Native {
    /// Bound queued plus executing plus pending replies, including duplicates.
    /// Requests also obey the public wire bounds; journal/cache limits remain
    /// independent. Capacity is 1..128; saturation refuses before admission.
    pub fn new(owner: Owner, capacity: usize) -> Result<Self> {
        Self::start(owner, capacity, Duration::from_secs(60))
    }
    pub(super) fn start(owner: Owner, capacity: usize, maintenance: Duration) -> Result<Self> {
        Self::start_reserved(owner, capacity, maintenance, None, None)
    }
    pub(super) fn start_reserved(
        owner: Owner,
        capacity: usize,
        maintenance: Duration,
        reservation: Option<OwnedSemaphorePermit>,
        retired: Option<Arc<Notify>>,
    ) -> Result<Self> {
        Self::validate(capacity, maintenance)?;
        owner.check()?;
        let incarnation = owner.identity().incarnation;
        let cache = owner.cache();
        let collection = owner.collection()?;
        let owner = Arc::new(Resident {
            state: RwLock::new(owner),
            reservation: reservation.map(Arc::new),
        });
        let (queue, receive) = mpsc::channel(capacity);
        let running = Arc::new(AtomicBool::new(true));
        let worker = tokio::spawn(run(
            owner.clone(),
            receive,
            Running(running.clone(), retired),
            collection,
            maintenance,
        ));
        Ok(Self {
            incarnation,
            owner,
            cache,
            queue,
            permits: Arc::new(Semaphore::new(capacity)),
            running,
            worker,
        })
    }
    pub(super) fn validate(capacity: usize, maintenance: Duration) -> Result<()> {
        if !(Duration::from_secs(1)..=Duration::from_secs(86400)).contains(&maintenance) {
            return Err(Error::Invalid(
                "maintenance interval must be 1 second..1 day".into(),
            ));
        }
        if !(1..=128).contains(&capacity) {
            return Err(Error::Invalid(
                "native request capacity must be 1..128".into(),
            ));
        }
        Ok(())
    }
    fn check(&self) -> Result<()> {
        if self.running.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    async fn submit(&self, write: Write) -> Result<Reply> {
        self.check()?;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let (send, receive) = oneshot::channel();
        self.queue
            .try_send(Command {
                write,
                response: Response {
                    send,
                    _permit: permit,
                },
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => unavailable(),
            })?;
        receive.await.map_err(|_| unavailable())?
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[async_trait]
impl Backend for Native {
    fn owner_incarnation(&self) -> Option<uuid::Uuid> {
        Some(self.incarnation)
    }
    async fn database_upload_budget(&self, db: &str) -> Result<crate::upload_budget::UploadStatus> {
        let budget = self
            .owner
            .read()
            .await
            .database_budget(db)?
            .ok_or(Error::Unsupported("database upload budgeting is disabled"))?;
        Ok(budget.status().await?)
    }
    async fn replenish_database_upload_budget(
        &self,
        db: &str,
        request: &crate::upload_budget::UploadRequest,
    ) -> Result<crate::upload_budget::UploadResult> {
        let budget = self
            .owner
            .read()
            .await
            .database_budget(db)?
            .ok_or(Error::Unsupported("database upload budgeting is disabled"))?;
        budget.replenish(request).await
    }
    async fn upload_budget(&self) -> Result<Option<crate::upload_budget::UploadStatus>> {
        let files = self.owner.read().await.files();
        Ok(files.upload_budget().await?)
    }
    async fn replenish_upload_budget(
        &self,
        request: &crate::upload_budget::UploadRequest,
    ) -> Result<crate::upload_budget::UploadResult> {
        let files = self.owner.read().await.files();
        files.replenish_upload_budget(request).await
    }
    async fn ready(&self) -> bool {
        // Health schedules recovery, never authorizes responses. A transient
        // provider error must not discard an idle owner. Nor may a checkpoint
        // holding this lock stall reconciliation of unrelated partitions.
        self.check().is_ok()
            && self
                .owner
                .try_read()
                .map_or(true, |owner| owner.check().is_ok())
    }
    async fn memory_bytes(&self) -> usize {
        self.cache.memory_bytes().await
    }
    async fn info(&self, db: &str, access: &Access) -> Result<Info> {
        self.check()?;
        self.owner.read().await.info_authorized(db, access).await
    }
    async fn read(&self, db: &str, request: &ReadRequest, access: &Access) -> Result<ReadResult> {
        self.check()?;
        let timer = metrics::REGISTRY.start(Operation::NativeReadLock);
        let owner = self.owner.read().await;
        timer.finish(Outcome::Ok);
        metrics::measure(
            Operation::NativeRead,
            owner.read_authorized(db, request, access),
        )
        .await
    }
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult> {
        engine::validate_db(&db)?;
        // JSON overestimates decoded keys/values and mutation framing. Add the
        // record envelope and database name; coalescing adds one frame envelope.
        let bytes = request.decode()?.bytes + 120 + db.len();
        match self
            .submit(Write::Commit(db, request, access, bytes))
            .await?
        {
            Reply::Commit(result) => Ok(result),
            _ => unreachable!("typed worker reply"),
        }
    }
    async fn create_protected(&self, db: String, request: CreateRequest) -> Result<CreateResult> {
        engine::validate_db(&db)?;
        request.validate()?;
        match self.submit(Write::Create(db, request)).await? {
            Reply::Catalog(CatalogResult::Created(result)) => Ok(result),
            _ => unreachable!("typed worker reply"),
        }
    }
    async fn change_policy(&self, db: String, request: PolicyRequest) -> Result<PolicyResult> {
        engine::validate_db(&db)?;
        request.digest()?;
        match self.submit(Write::Policy(db, request)).await? {
            Reply::Catalog(CatalogResult::Policy(result)) => Ok(result),
            _ => unreachable!("typed worker reply"),
        }
    }
}

async fn prepare(
    owner: &mut Owner,
    write: Write,
) -> Result<(Ticket, Option<journal::PreparedUpload>)> {
    let mut change = match write {
        Write::Create(db, request) => {
            owner
                .prepare_creation(&db, &request, &Access::Administrator)
                .await?
        }
        Write::Policy(db, request) => {
            owner
                .prepare_policy(&db, &request, &Access::Administrator)
                .await?
        }
        Write::Commit(db, request, access, _) => {
            let mut batch = owner
                .prepare_authorized(&[(&db, &request, &access)])
                .await?;
            return Ok((
                Ticket::Commit(batch.tickets.remove(0), access),
                batch.upload,
            ));
        }
    };
    let upload = change.upload.take();
    Ok((Ticket::Catalog(change), upload))
}

type Uploads = JoinSet<(u64, journal::Completion, Option<Arc<OwnedSemaphorePermit>>)>;

fn send_upload(owner: &Resident, uploads: &mut Uploads, upload: Option<journal::PreparedUpload>) {
    if let Some(upload) = upload {
        // Accepted work belongs to the server before the next await.
        let reservation = owner.reservation.clone();
        uploads.spawn(async move { (upload.position(), upload.send().await, reservation) });
    }
}

async fn admit(
    owner: &Resident,
    uploads: &mut Uploads,
    pending: &mut Vec<Pending>,
    mut commands: Vec<Command>,
) {
    commands.retain(|command| !command.response.send.is_closed());
    if commands.len() > 1 {
        let requests: Vec<_> = commands
            .iter()
            .map(|command| match &command.write {
                Write::Commit(db, request, access, _) => (db.as_str(), request, access),
                _ => unreachable!("catalog changes delimit HTTP groups"),
            })
            .collect();
        let result = metrics::measure(Operation::NativePrepare, async {
            owner.write().await.prepare_authorized(&requests).await
        })
        .await;
        match result {
            Ok(batch) => {
                send_upload(owner, uploads, batch.upload);
                for (command, ticket) in commands.into_iter().zip(batch.tickets) {
                    let Write::Commit(_, _, access, _) = command.write else {
                        unreachable!()
                    };
                    pending.push(Pending {
                        ticket: Ticket::Commit(ticket, access),
                        response: command.response,
                    });
                }
                return;
            }
            Err(Error::Storage(_)) => {
                // Funding/observation may have failed partway through the group.
                // Do not repeat it automatically or report a definite rejection.
                let _ = validate_response(&*owner.read().await).await;
                for command in commands {
                    let _ = command.response.send.send(Err(unavailable()));
                }
                return;
            }
            Err(_) => (), // Isolate request-local failures using the same single-request path.
        }
    }
    for command in commands {
        if command.response.send.is_closed() {
            continue;
        }
        let result = metrics::measure(Operation::NativePrepare, async {
            prepare(&mut *owner.write().await, command.write).await
        })
        .await;
        match result {
            Ok((ticket, upload)) => {
                send_upload(owner, uploads, upload);
                pending.push(Pending {
                    ticket,
                    response: command.response,
                });
            }
            Err(error) => {
                let result = validate_response(&*owner.read().await)
                    .await
                    .and(Err(error));
                let _ = command.response.send.send(result);
            }
        }
    }
}

// A failed observation suppresses this response, not the worker. Only a
// confirmed missing permit or an already failed local view requires retirement.
async fn validate_response(owner: &Owner) -> Result<()> {
    owner
        .validate()
        .await
        .inspect_err(|_| {
            if owner.check().is_ok() {
                crate::metrics::REGISTRY
                    .event(crate::metrics::Event::ResponseValidationUnavailable, 1);
            }
        })
        .map_err(Error::Storage)
}

// The worker is the sole mutator. Downgrade during immutable staging so reads
// keep their existing view; reacquire exclusive access before publication can
// let collection retire old tables. No second projection or worker is needed.
async fn maintain(
    owner: &Resident,
    queue: &mut mpsc::Receiver<Command>,
    may_yield: bool,
) -> anyhow::Result<Option<Command>> {
    let mut state = owner.write().await;
    let required = state.checkpoint_due();
    let Some(mut checkpoint) = state.begin_checkpoint()? else {
        return Ok(None);
    };
    let state = RwLockWriteGuard::downgrade(state);
    let staged = if required || !may_yield {
        checkpoint.stage(&state).await
    } else {
        // Handoff-only work may yield before publication. Immutable uploads
        // can finish late; their charges and retry pacing remain in force.
        // Capacity checkpoints and uncertain publications never take this path.
        tokio::select! {
            biased;
            command = queue.recv() => {
                tracing::info!(owner = %state.identity().incarnation,
                    "native handoff checkpoint yielded to queued work");
                return Ok(command);
            }
            result = checkpoint.stage(&state) => result,
        }
    };
    drop(state);
    let result = match staged {
        Ok(()) => checkpoint.publish(&mut *owner.write().await).await,
        Err(error) => Err(error),
    };
    checkpoint.finish(result).map(|_| None)
}

async fn run(
    owner: Arc<Resident>,
    mut queue: mpsc::Receiver<Command>,
    _running: Running,
    collection: crate::ownership::root::Collection,
    maintenance: Duration,
) {
    let (control, identity) = {
        let state = owner.read().await;
        (state.control(), state.identity())
    };
    let mut probes = JoinSet::new();
    let mut health = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(3),
        Duration::from_secs(3),
    );
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut uploads = JoinSet::new();
    // At most one bounded sweep per serving worker, outside every owner lock.
    // Dropping the JoinSet on retirement cancels it; no unique collector lease
    // or in-memory cursor is needed for safety or restart.
    let mut collections = JoinSet::new();
    let mut sweep =
        tokio::time::interval_at(tokio::time::Instant::now() + maintenance, maintenance);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending: Vec<Pending> = Vec::new();
    let mut queued = None;
    let mut may_yield = true;
    let mut unknown = BTreeSet::new();
    let mut unsent = BTreeMap::new();
    let mut admission = tokio::time::interval(Duration::from_secs(1));
    admission.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retry = tokio::time::interval(Duration::from_millis(100));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = health.tick(), if probes.is_empty() => {
                // A revoked idle owner may receive no requests after rejoin.
                // Observe independently of the owner lock and reconciliation;
                // only confirmed absence retires it. Errors grant no authority.
                let control = control.clone();
                probes.spawn(async move { control.validate(identity).await });
            }
            _ = probes.join_next(), if !probes.is_empty() => (),
            _ = sweep.tick(), if collections.is_empty() => {
                let collection = collection.clone();
                let reservation = owner.reservation.clone();
                collections.spawn(async move {
                    (tokio::time::timeout(Duration::from_secs(30), collection.collect()).await, reservation)
                });
            }
            result = collections.join_next(), if !collections.is_empty() => {
                if !matches!(result, Some(Ok((Ok(Ok(_)), _)))) { tracing::warn!("native storage collection deferred"); }
            }
            command = async {
                match queued.take() {
                    Some(command) => Some(command),
                    None => queue.recv().await,
                }
            } => {
                let Some(command) = command else { return };
                if !command.response.send.is_closed() {
                    let mut bytes = 120 + command.write.batch_bytes().unwrap_or(0);
                    let commits = command.write.batch_bytes().is_some();
                    let mut commands = vec![command];
                    if commits {
                        // One scheduler yield gathers already concurrent callers;
                        // no timer or minimum group size delays a lone request.
                        tokio::task::yield_now().await;
                        let limit = owner.read().await.available_journal_bytes();
                        for _ in 1..128 {
                            let Ok(next) = queue.try_recv() else { break };
                            if next.response.send.is_closed() { continue; }
                            match next.write.batch_bytes() {
                                Some(size) if bytes + size <= limit => {
                                    bytes += size;
                                    commands.push(next);
                                }
                                _ => { queued = Some(next); break; }
                            }
                        }
                    }
                    admit(&owner, &mut uploads, &mut pending, commands).await;
                }
            }
            completed = uploads.join_next(), if !uploads.is_empty() => {
                let Some(Ok((position, mut completion, _reservation))) = completed else {
                    tracing::warn!(owner = %identity.incarnation, reason = "upload_task", "native worker retired");
                    return;
                };
                if let Some(upload) = completion.take_unsent() {
                    unsent.insert(position, upload);
                } else if completion.resolution() == Resolution::Unknown { unknown.insert(position); }
                if owner.write().await.complete(completion).await.is_err() {
                    tracing::warn!(owner = %identity.incarnation, position, "native completion deferred; checking authority before retry");
                }
            }
            _ = admission.tick() => {
                // At most one pre-send retry per second, earliest gap first.
                // No resolving GET or repeated data PUT for an unsent frame.
                if let Some((position, upload)) = unsent.pop_first() {
                    let reservation = owner.reservation.clone();
                    uploads.spawn(async move { (position, upload.send().await, reservation) });
                }
            }
            _ = retry.tick() => {
                if let Some(position) = unknown.first().copied() {
                    let mut owner = owner.write().await;
                    match owner.resolve_upload(position).await {
                        Ok(Resolution::Durable) => { unknown.remove(&position); }
                        Ok(Resolution::Unknown) => {
                            // Absence cannot prove an abort, but waiting cannot
                            // repair a failed PUT either. Reacquisition revokes
                            // this owner and selects its durable journal prefix.
                            tracing::warn!(owner = %identity.incarnation, reason = "unresolved_upload", position, "native worker retired");
                            return;
                        }
                        Err(_) => (),
                    }
                }
            }
        }
        {
            let owner = owner.read().await;
            if let Err(error) = owner.check() {
                let reason = if error.to_string() == "owner retired after incomplete publication" {
                    "incomplete_publication"
                } else {
                    "permission"
                };
                tracing::warn!(owner = %identity.incarnation, reason, "native worker retired");
                return;
            }
            let mut replies = Vec::new();
            let mut i = 0;
            while i < pending.len() {
                let response = &pending[i].response;
                if response.send.is_closed() {
                    pending.swap_remove(i);
                    continue;
                }
                let result = match &pending[i].ticket {
                    Ticket::Catalog(change) => owner
                        .catalog_result_unvalidated(change)
                        .map(|r| r.map(Reply::Catalog)),
                    Ticket::Commit(ticket, access) => owner
                        .authorized_result_unvalidated(ticket, access)
                        .await
                        .map(|r| r.map(Reply::Commit)),
                };
                let result = match result {
                    Ok(None) => {
                        i += 1;
                        continue;
                    }
                    Ok(Some(reply)) => Ok(reply),
                    Err(error) => Err(error),
                };
                replies.push((pending.swap_remove(i).response, result));
            }
            if !replies.is_empty() {
                let validated = validate_response(&owner).await.is_ok();
                for (response, result) in replies {
                    let result = if validated {
                        result
                    } else {
                        Err(Error::Storage(anyhow::anyhow!(
                            "serving permission unavailable"
                        )))
                    };
                    let _ = response.send.send(result);
                }
            }
        }
        // Reply before maintenance starts, then quiesce new admissions until
        // its cut is published. The retry tick also drives idle/paced work.
        if queued.is_none() {
            queued = maintain(&owner, &mut queue, may_yield)
                .await
                .unwrap_or_default();
            // One yield per owner: a busy queue cannot starve the next attempt.
            may_yield &= queued.is_none();
        }
    }
}
