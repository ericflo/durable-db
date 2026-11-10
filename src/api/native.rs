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
    ownership::timing::Clock,
    transactions::live::{AuthorizedTicket, CatalogChange, CatalogResult, Owner, Renewal},
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
    sync::{OwnedSemaphorePermit, RwLock, Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

/// Native HTTP backend for an already recovered owner. The caller must supply
/// a qualified ownership contract. Use `renewing` to own its renewal task, or
/// `new` with externally driven renewal. Neither selects a physical clock,
/// elects an owner, or enables the CLI cloud runtime.
/// Dropping it retires the worker; accepted writes can have unknown outcomes.
pub struct Native<C> {
    owner: Arc<RwLock<Owner<C>>>,
    cache: Arc<Cache>,
    queue: mpsc::Sender<Command>,
    permits: Arc<Semaphore>,
    running: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}
enum Write {
    Create(String, CreateRequest),
    Policy(String, PolicyRequest),
    Commit(String, CommitRequest, Access),
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
struct Running(Arc<AtomicBool>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
fn unavailable() -> Error {
    Error::Storage(anyhow::anyhow!("native worker retired"))
}

impl<C: Clock + Send + 'static> Native<C> {
    /// Bound queued plus executing plus pending replies, including duplicates.
    /// Requests also obey the public wire bounds; journal/cache limits remain
    /// independent. Capacity is 1..128; saturation refuses before admission.
    pub fn new(owner: Owner<C>, capacity: usize) -> Result<Self> {
        Self::start(owner, capacity, None)
    }
    /// Own bounded renewal scheduling alongside serving. The injected clock and
    /// profile must already be qualified. Worker failure/drop also stops renewal.
    pub fn renewing(
        owner: Owner<C>,
        capacity: usize,
        period: Duration,
        timeout: Duration,
    ) -> Result<Self> {
        let renewal = owner.control().keep_alive(period, timeout)?;
        Self::start(owner, capacity, Some(renewal))
    }
    fn start(owner: Owner<C>, capacity: usize, renewal: Option<Renewal>) -> Result<Self> {
        if !(1..=128).contains(&capacity) {
            return Err(Error::Invalid(
                "native request capacity must be 1..128".into(),
            ));
        }
        owner.check()?;
        let cache = owner.cache();
        let owner = Arc::new(RwLock::new(owner));
        let (queue, receive) = mpsc::channel(capacity);
        let running = Arc::new(AtomicBool::new(true));
        let worker = tokio::spawn(run(
            owner.clone(),
            receive,
            Running(running.clone()),
            renewal,
        ));
        Ok(Self {
            owner,
            cache,
            queue,
            permits: Arc::new(Semaphore::new(capacity)),
            running,
            worker,
        })
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
impl<C> Drop for Native<C> {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[async_trait]
impl<C: Clock + Send + 'static> Backend for Native<C> {
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
        self.check().is_ok() && self.owner.read().await.check().is_ok()
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
        self.owner
            .read()
            .await
            .read_authorized(db, request, access)
            .await
    }
    async fn commit(
        &self,
        db: String,
        request: CommitRequest,
        access: Access,
    ) -> Result<CommitResult> {
        engine::validate_db(&db)?;
        request.decode()?;
        match self.submit(Write::Commit(db, request, access)).await? {
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

async fn prepare<C: Clock>(
    owner: &mut Owner<C>,
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
        Write::Commit(db, request, access) => {
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

async fn run<C: Clock + Send>(
    owner: Arc<RwLock<Owner<C>>>,
    mut queue: mpsc::Receiver<Command>,
    _running: Running,
    _renewal: Option<Renewal>,
) {
    let mut uploads = JoinSet::new();
    let mut pending: Vec<Pending> = Vec::new();
    let mut unknown = BTreeSet::new();
    let mut unsent = BTreeMap::new();
    let mut admission = tokio::time::interval(Duration::from_secs(1));
    admission.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retry = tokio::time::interval(Duration::from_millis(100));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            command = queue.recv() => {
                let Some(command) = command else { return };
                if !command.response.send.is_closed() {
                    let prepared = prepare(&mut *owner.write().await, command.write).await;
                    match prepared {
                        Ok((ticket, upload)) => {
                            if let Some(upload) = upload {
                                // No await between acceptance and handing the upload to
                                // the server. The owner lock never covers its PUT.
                                uploads.spawn(async move {
                                    let position = upload.position();
                                    (position, upload.send().await)
                                });
                            }
                            pending.push(Pending { ticket, response: command.response });
                        }
                        Err(error) => { let _ = command.response.send.send(Err(error)); }
                    }
                }
            }
            completed = uploads.join_next(), if !uploads.is_empty() => {
                let Some(Ok((position, mut completion))) = completed else { return };
                if let Some(upload) = completion.take_unsent() {
                    unsent.insert(position, upload);
                } else if completion.resolution() == Resolution::Unknown { unknown.insert(position); }
                if owner.write().await.complete(completion).await.is_err() {
                    tracing::warn!("native completion deferred; checking authority before retry");
                }
            }
            _ = admission.tick() => {
                // At most one admission retry per second, earliest gap first.
                // No resolving GET or repeated data PUT for an unsent frame.
                if let Some((position, upload)) = unsent.pop_first() {
                    uploads.spawn(async move { (position, upload.send().await) });
                }
            }
            _ = retry.tick() => {
                let mut owner = owner.write().await;
                if let Some(position) = unknown.first().copied() {
                    if matches!(owner.resolve_upload(position).await, Ok(Resolution::Durable)) {
                        unknown.remove(&position);
                    }
                } else if owner.maintain().await.is_err() {
                    tracing::warn!("native checkpoint deferred; checking authority before retry");
                }
            }
        }
        let owner = owner.read().await;
        if owner.check().is_err() {
            return;
        }
        let mut i = 0;
        while i < pending.len() {
            let response = &pending[i].response;
            if response.send.is_closed() {
                pending.swap_remove(i);
                continue;
            }
            let result = match &pending[i].ticket {
                Ticket::Catalog(change) => {
                    owner.catalog_result(change).map(|r| r.map(Reply::Catalog))
                }
                Ticket::Commit(ticket, access) => owner
                    .authorized_result(ticket, access)
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
            let _ = pending.swap_remove(i).response.send.send(result);
        }
    }
}
