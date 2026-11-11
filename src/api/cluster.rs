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

/// Index/validation and journal byte allowances are aggregate across residents.
/// The byte cache is shared; frame/window/checkpoint thresholds stay per partition.
/// Membership and this process's advertised socket addresses must be canonical.
pub struct ClusterConfig {
    pub layout: Layout,
    pub address: SocketAddr,
    pub limits: Limits,
    pub upload_limits: UploadLimits,
    pub capacity: usize,
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
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl Cluster {
    pub async fn open(
        directory: Partitions,
        config: ClusterConfig,
        cache: Arc<Cache>,
        auth: Arc<Auth>,
        mut members: watch::Receiver<Vec<SocketAddr>>,
    ) -> anyhow::Result<Arc<Self>> {
        config.layout.validate()?;
        let limits = config.limits.share(config.layout.partitions() as usize)?;
        config.upload_limits.validate()?;
        crate::ownership::root::valid_address(config.address)?;
        ensure!(
            (1..=128).contains(&config.capacity),
            "cluster capacity must be 1..128"
        );
        membership(&members.borrow())?;
        directory.configure(config.layout).await?;
        let mut partitions = Vec::new();
        for id in 0..config.layout.partitions() {
            let connection = directory.partition(id, config.upload_limits).await?;
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
            worker: Mutex::new(None),
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
                // The deadline changes availability only. A canceled claim may
                // still publish; its persisted registration remains discoverable.
                let result = tokio::time::timeout(
                    Duration::from_secs(30),
                    cluster.reconcile(&peers, &mut cursor),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    cluster.ready.store(false, Ordering::Release);
                    tracing::warn!("native placement incomplete; retrying");
                }
            }
            if let Some(cluster) = weak.upgrade() {
                cluster.retire().await;
            }
        });
        *cluster.worker.lock().expect("cluster worker lock") = Some(worker);
        Ok(cluster)
    }
    async fn retire(&self) {
        self.ready.store(false, Ordering::Release);
        for partition in &self.partitions {
            partition.local.write().await.take();
        }
    }
    async fn reconcile(&self, peers: &[SocketAddr], cursor: &mut usize) -> anyhow::Result<()> {
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
                let local = self.acquire(&partition.connection).await?;
                *partition.local.write().await = Some(Arc::new(local));
            }
            checked += 1;
            if checked == 8 {
                break;
            }
        }
        let mut complete = present;
        for (partition, wanted) in self.partitions.iter().zip(desired) {
            if wanted && partition.local.read().await.is_none() {
                complete = false;
            }
        }
        self.ready.store(complete, Ordering::Release);
        Ok(())
    }
    async fn acquire(&self, connection: &Connection) -> anyhow::Result<Native> {
        let reservation = self
            .residents
            .clone()
            .try_acquire_owned()
            .context("retired owners still occupy the resident budget")?;
        // A bootstrap upload grant can invalidate a captured comparison. Only
        // definite conflicts get a fresh attempt here; uncertainty returns.
        for _ in 0..4 {
            let mut root = connection.root()?.advertise(self.config.address)?;
            let observed = root.observe().await?;
            let claim = root.claim(observed)?;
            match root.finish(claim.execute().await)? {
                Outcome::Confirmed => (),
                Outcome::Conflict => continue,
                _ => anyhow::bail!("ownership claim unconfirmed"),
            }
            let activate = root.activate()?;
            ensure!(
                matches!(root.finish(activate.execute().await)?, Outcome::Confirmed),
                "ownership activation unconfirmed"
            );
            root.select_journals().await?;
            let owner = Opening::new(root, self.limits, self.cache.clone())?
                .recover()
                .await?;
            return Native::start_reserved(
                owner,
                self.config.capacity,
                Duration::from_secs(60),
                Some(reservation),
            )
            .map_err(anyhow::Error::from);
        }
        anyhow::bail!("ownership claim remained contended")
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().expect("cluster worker lock").take() {
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
    ensure!(peers.len() <= 256, "membership exceeds 256 peers");
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
