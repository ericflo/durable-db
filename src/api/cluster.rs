//! Membership schedules work; only the object-store ownership protocol can
//! authorize service. No lease, heartbeat expiry or bounded clock is involved.
use super::{Backend, Forward, Native};
use crate::{
    Result,
    auth::Auth,
    cache::Cache,
    ownership::root::Outcome,
    providers::{
        Connection,
        partitions::{Layout, Partitions},
    },
    transactions::live::{Limits, Opening},
    upload_budget::{UploadLimits, UploadRequest, UploadResult, UploadStatus},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{RwLock, Semaphore, watch},
    task::JoinHandle,
};

pub(super) const MAX_MEMBERS: usize = 256;

/// Index/validation and journal byte allowances are aggregate across residents.
/// The byte cache is shared; frame/window/checkpoint thresholds stay per partition.
/// Membership and this process's advertised socket addresses must be canonical.
pub struct ClusterConfig {
    pub layout: Layout,
    pub address: SocketAddr,
    pub limits: Limits,
    pub upload_limits: UploadLimits,
    pub capacity: usize,
    pub maintenance: Duration,
}
impl ClusterConfig {
    /// Validate resource/placement settings before connecting to storage.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.layout.validate()?;
        self.limits
            .share(self.layout.partitions() as usize)
            .context("resident allowance is too small for the partition layout")?;
        self.upload_limits.validate()?;
        crate::ownership::root::valid_address(self.address)?;
        Native::validate(self.capacity, self.maintenance)?;
        Ok(())
    }
}
struct Partition {
    connection: Connection,
    forward: Arc<Forward>,
    local: RwLock<Option<Arc<Native>>>,
}
/// A bounded native controller. The caller supplies membership snapshots; an
/// update or a three-second retry tick reconciles placement. Closing membership
/// retires local backends. Losing the last controller handle aborts its task.
/// This interface does not discover Kubernetes membership or migrate layouts.
pub struct Cluster {
    directory: Partitions,
    config: ClusterConfig,
    cache: Arc<Cache>,
    partitions: Vec<Partition>,
    limits: Limits,
    residents: Arc<Semaphore>,
    ready: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
}
impl Cluster {
    pub async fn open(
        directory: Partitions,
        config: ClusterConfig,
        cache: Arc<Cache>,
        auth: Arc<Auth>,
        mut members: watch::Receiver<Vec<SocketAddr>>,
    ) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let limits = config.limits.share(config.layout.partitions() as usize)?;
        membership(&members.borrow())?;
        directory.configure(config.layout).await?;
        let mut partitions = Vec::new();
        for id in 0..config.layout.partitions() {
            let connection = directory.lazy_partition(id)?;
            let forward = Arc::new(Forward::new(&connection, auth.clone(), config.capacity)?);
            partitions.push(Partition {
                connection,
                forward,
                local: RwLock::new(None),
            });
        }
        let residents = Arc::new(Semaphore::new(config.layout.partitions() as usize));
        let cluster = Arc::new(Self {
            limits,
            residents,
            directory,
            config,
            cache,
            partitions,
            ready: AtomicBool::new(false),
            workers: Mutex::new(Vec::new()),
        });
        let mut maintenance_members = members.clone();
        let directory = cluster.directory.clone();
        let address = cluster.config.address;
        let interval = cluster.config.maintenance;
        let maintenance = tokio::spawn(async move {
            let mut sweep =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = sweep.tick() => (),
                    update = maintenance_members.changed() => {
                        if update.is_err() { break; }
                        continue;
                    }
                }
                // Membership schedules cost only. Disagreement is safe under
                // the collector's existing list-before-fence protocol.
                if preferred(0, &maintenance_members.borrow()) == Some(address)
                    && !matches!(
                        tokio::time::timeout(Duration::from_secs(30), directory.maintenance())
                            .await,
                        Ok(Ok(()))
                    )
                {
                    tracing::warn!("shared directory collection deferred");
                }
            }
        });
        let weak = Arc::downgrade(&cluster);
        let worker = tokio::spawn(async move {
            let mut retry = tokio::time::interval(Duration::from_secs(3));
            retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut cursor = 0;
            loop {
                tokio::select! {
                    _ = retry.tick() => (),
                    update = members.changed() => if update.is_err() { break; },
                }
                let peers = members.borrow_and_update().clone();
                let Some(cluster) = weak.upgrade() else {
                    return;
                };
                // Individual provider calls are bounded. A whole recovery may
                // legitimately take longer; restarting it on a fixed timer can
                // prevent progress forever. Changed placement still cancels it.
                let mut work = Box::pin(cluster.reconcile(&peers, &mut cursor));
                let result = loop {
                    tokio::select! {
                        result = &mut work => break Some(result),
                        update = members.changed() => {
                            let same_placement = membership(&members.borrow()).is_ok_and(|next| {
                                next.contains(&cluster.config.address) == peers.contains(&cluster.config.address)
                                    && (0..cluster.partitions.len()).all(|id| {
                                        let ours = |peers: &[SocketAddr]| {
                                            preferred(id as u32, peers) == Some(cluster.config.address)
                                        };
                                        ours(&next) == ours(&peers)
                                    })
                            });
                            if update.is_err() || !same_placement {
                                break None;
                            }
                        }
                    }
                };
                match result {
                    Some(Ok(true)) => retry.reset_immediately(),
                    Some(Ok(false)) => (),
                    Some(Err(_)) => {
                        cluster.ready.store(false, Ordering::Release);
                        tracing::warn!("native placement incomplete; retrying");
                    }
                    None => {
                        cluster.ready.store(false, Ordering::Release);
                        retry.reset_immediately();
                        if members.has_changed().is_err() {
                            break;
                        }
                    }
                }
            }
            if let Some(cluster) = weak.upgrade() {
                cluster.retire().await;
            }
        });
        *cluster.workers.lock().expect("cluster workers lock") = vec![worker, maintenance];
        Ok(cluster)
    }
    async fn retire(&self) {
        self.ready.store(false, Ordering::Release);
        for partition in &self.partitions {
            partition.local.write().await.take();
        }
    }
    async fn reconcile(&self, peers: &[SocketAddr], cursor: &mut usize) -> anyhow::Result<bool> {
        let peers = membership(peers)?;
        let present = peers.contains(&self.config.address);
        if !present {
            self.ready.store(false, Ordering::Release);
        }
        let desired: Vec<_> = (0..self.partitions.len())
            .map(|id| preferred(id as u32, &peers) == Some(self.config.address))
            .collect();
        // Retire removed placements before I/O. A routine health sweep keeps
        // readiness until an actual loss; it must not flap on every retry tick.
        for (partition, wanted) in self.partitions.iter().zip(&desired) {
            if !wanted {
                partition.local.write().await.take();
            }
        }
        let mut checked = 0;
        let mut acquisitions = FuturesUnordered::new();
        for _ in 0..self.partitions.len() {
            let id = *cursor;
            // Advance before I/O, so failure or cancellation cannot starve the
            // next partition. Limit work even when the provider is healthy.
            *cursor = (id + 1) % self.partitions.len();
            if !desired[id] {
                continue;
            }
            let partition = &self.partitions[id];
            let local = partition.local.read().await.clone();
            let healthy = match local {
                Some(local) => local.ready().await,
                None => false,
            };
            if !healthy {
                self.ready.store(false, Ordering::Release);
                partition.local.write().await.take();
                acquisitions.push(async move {
                    let local = self.acquire(id as u32, &partition.connection).await?;
                    *partition.local.write().await = Some(Arc::new(local));
                    Ok::<_, anyhow::Error>(())
                });
                if acquisitions.len() == 4 {
                    break;
                }
                continue;
            }
            checked += 1;
            if checked == 8 {
                break;
            }
        }
        let acquired = !acquisitions.is_empty();
        let mut failure = None;
        // Finish independent acquisitions even if another fails or stalls.
        // Reservations bound all recovery/serving state through cancellation.
        while let Some(result) = acquisitions.next().await {
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let mut complete = present;
        for (partition, wanted) in self.partitions.iter().zip(desired) {
            if wanted && partition.local.read().await.is_none() {
                complete = false;
            }
        }
        self.ready.store(complete, Ordering::Release);
        Ok(acquired)
    }
    async fn acquire(&self, id: u32, connection: &Connection) -> anyhow::Result<Native> {
        let started = std::time::Instant::now();
        let progress = |stage: &'static str| {
            tracing::info!(
                partition = id,
                stage,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "native acquisition progress"
            );
        };
        let mut stage = "resident";
        progress(stage);
        let result = async {
            let reservation = self
                .residents
                .clone()
                .try_acquire_owned()
                .context("retired owners still occupy the resident budget")?;
            stage = "budget";
            progress(stage);
            connection
                .initialize_upload_budget(self.config.upload_limits)
                .await?;
            // Definite claim conflicts use a fresh candidate. Resolve unknown
            // outcomes on the same identity, without replaying its claim PUTs.
            let mut validations = 4;
            for _ in 0..4 {
                let mut root = connection.root()?.advertise(self.config.address)?;
                stage = "observe";
                progress(stage);
                let observed = root.observe().await?;
                stage = "claim";
                progress(stage);
                let claim = root.claim(observed)?;
                let outcome = root.finish(claim.execute().await)?;
                if matches!(outcome, Outcome::Conflict) {
                    continue;
                }
                root.confirm_outcome(outcome, &mut validations)
                    .await
                    .context("ownership claim unconfirmed")?;
                stage = "activate";
                progress(stage);
                root.activate_confirmed(&mut validations).await?;
                stage = "select_journals";
                progress(stage);
                root.select_journals().await?;
                stage = "recover";
                progress(stage);
                let owner = Opening::new(root, self.limits, self.cache.clone())?
                    .recover()
                    .await?;
                stage = "serve";
                progress(stage);
                return Native::start_reserved(
                    owner,
                    self.config.capacity,
                    self.config.maintenance,
                    Some(reservation),
                )
                .map_err(anyhow::Error::from);
            }
            anyhow::bail!("ownership claim remained contended")
        }
        .await;
        if let Err(error) = &result {
            // Only known static diagnostics may leave the error chain. Provider
            // messages can contain credentials, object names or request bodies.
            let reason = [
                "submitted publication changed or was fenced; outcome is uncertain",
                "publication helping budget exhausted; outcome is uncertain",
                "partition changed after publication; outcome uncertain",
                "published manifest changed repeatedly or is unavailable",
                "activation permit revoked",
            ]
            .into_iter()
            .find(|message| error.chain().any(|cause| cause.to_string() == *message))
            .unwrap_or("provider_or_validation");
            tracing::warn!(
                partition = id,
                stage,
                reason,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "native acquisition failed"
            );
        } else {
            progress("ready");
        }
        result
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for worker in self
            .workers
            .get_mut()
            .expect("cluster workers lock")
            .drain(..)
        {
            worker.abort();
        }
    }
}
#[async_trait]
impl Backend for Cluster {
    async fn resolve(&self, db: &str) -> Result<Option<Arc<dyn Backend>>> {
        let id = self.config.layout.partition(db)?;
        let partition = &self.partitions[id as usize];
        let backend: Arc<dyn Backend> = match partition.local.read().await.clone() {
            Some(local) => local,
            None => partition.forward.clone(),
        };
        Ok(Some(backend))
    }
    async fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    async fn memory_bytes(&self) -> usize {
        self.cache.memory_bytes().await
    }
    async fn upload_budget(&self) -> Result<Option<UploadStatus>> {
        Ok(Some(self.directory.upload_budget().await?))
    }
    async fn replenish_upload_budget(&self, request: &UploadRequest) -> Result<UploadResult> {
        self.directory.replenish_upload_budget(request).await
    }
}
fn membership(peers: &[SocketAddr]) -> anyhow::Result<Vec<SocketAddr>> {
    ensure!(peers.len() <= MAX_MEMBERS, "membership exceeds 256 peers");
    for peer in peers {
        crate::ownership::root::valid_address(*peer)?;
    }
    let mut sorted = peers.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    ensure!(sorted.len() == peers.len(), "duplicate peer address");
    Ok(sorted)
}
/// Rendezvous selection moves only partitions whose preferred node changes.
/// Ordering/duplicates are handled by membership validation, never as authority.
fn preferred(partition: u32, peers: &[SocketAddr]) -> Option<SocketAddr> {
    peers.iter().copied().max_by_key(|peer| {
        let mut hash = Sha256::new();
        hash.update(b"durable-db.placement.v1\0");
        hash.update(partition.to_be_bytes());
        hash.update(peer.to_string().as_bytes());
        (hash.finalize(), *peer)
    })
}
