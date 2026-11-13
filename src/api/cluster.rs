//! Membership schedules work; only the object-store ownership protocol can
//! authorize service. No lease, heartbeat expiry or bounded clock is involved.
use super::{Backend, Forward, Member, Native};
use crate::{
    Result,
    auth::Auth,
    cache::Cache,
    ownership::root::{Identity, Outcome},
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
    task::{JoinHandle, JoinSet},
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
    local: RwLock<Option<Local>>,
    // Advisory wait state survives acquisition retries without retaining a slot.
    challenge: Mutex<Option<Challenge>>,
}
#[derive(Clone, Copy)]
struct Challenge {
    incumbent: Identity,
    nonce: uuid::Uuid,
    ready_at: tokio::time::Instant,
}
// Advisory replies stop when placement retires this slot, even if in-flight
// requests retain the old backend. Its permit remains the response authority.
struct Local {
    backend: Arc<Native>,
    member: uuid::Uuid,
    _challenges: JoinSet<()>,
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
        mut members: watch::Receiver<Vec<Member>>,
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
                challenge: Mutex::new(None),
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
        let count = cluster.config.layout.partitions() as usize;
        let weak = Arc::downgrade(&cluster);
        let worker = tokio::spawn(async move {
            let mut retry = tokio::time::interval(Duration::from_secs(3));
            retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending = vec![false; count];
            let mut failed = vec![false; count];
            let mut acquisitions = FuturesUnordered::new();
            let mut cursor = 0;
            loop {
                tokio::select! {
                    _ = retry.tick() => failed.fill(false),
                    update = members.changed() => {
                        if update.is_err() { break; }
                        failed.fill(false);
                    },
                    Some((id, result)) = acquisitions.next(), if !acquisitions.is_empty() => {
                        let result: anyhow::Result<()> = result;
                        pending[id] = false;
                        failed[id] = result.is_err();
                    }
                }
                let peers = membership(&members.borrow_and_update()).unwrap_or_default();
                let Some(cluster) = weak.upgrade() else {
                    return;
                };
                // Readiness keeps routing/administration reachable; membership
                // schedules work but cannot authorize a data response.
                cluster
                    .ready
                    .store(contains(&peers, cluster.config.address), Ordering::Release);
                for partition in &cluster.partitions {
                    let mut local = partition.local.write().await;
                    // A preference change schedules the successor; it does not
                    // revoke this incumbent. Keep serving until actual takeover
                    // or removal, retaining the resident reservation throughout.
                    // Native health checks are local and never wait for I/O.
                    if !contains(&peers, cluster.config.address)
                        || matches!(&*local, Some(local) if !local.backend.ready().await)
                    {
                        local.take();
                    }
                }
                for _ in 0..count {
                    if acquisitions.len() == 4 {
                        break;
                    }
                    let id = cursor;
                    cursor = (id + 1) % count;
                    if pending[id]
                        || failed[id]
                        || preferred(id as u32, &peers) != Some(cluster.config.address)
                    {
                        continue;
                    }
                    let partition = &cluster.partitions[id];
                    if partition.local.read().await.is_some() {
                        continue;
                    }
                    pending[id] = true;
                    let cluster = cluster.clone();
                    let members = members.clone();
                    acquisitions
                        .push(async move { (id, cluster.acquire_current(id, members).await) });
                }
            }
            drop(acquisitions);
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
    async fn acquire_current(
        &self,
        id: usize,
        mut members: watch::Receiver<Vec<Member>>,
    ) -> anyhow::Result<()> {
        let partition = &self.partitions[id];
        let claimed = AtomicBool::new(false);
        let wanted = |peers: &[Member]| {
            membership(peers).is_ok_and(|peers| {
                contains(&peers, self.config.address)
                    && (claimed.load(Ordering::Acquire)
                        || preferred(id as u32, &peers) == Some(self.config.address))
            })
        };
        let acquisition = self.acquire(id as u32, partition, members.clone(), &claimed);
        tokio::pin!(acquisition);
        let local = loop {
            if !wanted(&members.borrow_and_update()) {
                return Ok(());
            }
            tokio::select! {
                result = &mut acquisition => break result?,
                update = members.changed() => if update.is_err() { return Ok(()); },
            }
        };
        // A confirmed claimant is already the incumbent. Finish recovery while
        // still a member, even if preference changes; takeover can fence it at
        // any point. Unclaimed candidates still yield to their new destination.
        if wanted(&members.borrow()) {
            *partition.local.write().await = Some(local);
        }
        Ok(())
    }
    async fn acquire(
        &self,
        id: u32,
        partition: &Partition,
        members: watch::Receiver<Vec<Member>>,
        claimed: &AtomicBool,
    ) -> anyhow::Result<Local> {
        let connection = &partition.connection;
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
                let mut root = connection
                    .root()?
                    .with_cache(self.cache.clone())
                    .advertise(self.config.address)?;
                stage = "observe";
                progress(stage);
                let observed = root.observe().await?;
                let member = member_hint(&members.borrow(), self.config.address)?;
                let incumbent = observed
                    .incumbent()
                    .filter(|incumbent| {
                        incumbent.address != self.config.address
                            && !contains(&members.borrow(), incumbent.address)
                    })
                    .map(|incumbent| incumbent.identity)
                    .or_else(|| {
                        observed
                            .withdrawn_member()
                            .filter(|(_, target)| *target != member)
                            .map(|(identity, _)| identity)
                    });
                if let Some(incumbent) = incumbent {
                    stage = "challenge";
                    progress(stage);
                    let challenge = *partition
                        .challenge
                        .lock()
                        .expect("partition challenge lock");
                    if let Some(challenge) = challenge.filter(|c| {
                        c.incumbent == incumbent && observed.challenge() == Some(c.nonce)
                    }) {
                        ensure!(
                            tokio::time::Instant::now() >= challenge.ready_at,
                            "owner challenge pending"
                        );
                    } else {
                        let nonce = root
                            .challenge(observed)
                            .await?
                            .context("owner challenge unconfirmed")?;
                        *partition
                            .challenge
                            .lock()
                            .expect("partition challenge lock") = Some(Challenge {
                            incumbent,
                            nonce,
                            ready_at: tokio::time::Instant::now() + Duration::from_secs(15),
                        });
                        anyhow::bail!("owner challenge pending");
                    }
                }
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
                claimed.store(true, Ordering::Release);
                let mut challenges = JoinSet::new();
                let answering_members = members.clone();
                let address = self.config.address;
                challenges.spawn(root.answer_challenges(move || {
                    preferred(id, &answering_members.borrow()) == Some(address)
                }));
                stage = "activate";
                progress(stage);
                root.activate_confirmed(&mut validations).await?;
                if let Ok(Some(identity)) = root.identity() {
                    tracing::info!(partition = id, owner = %identity.incarnation,
                        "native acquisition owner");
                }
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
                let native = Native::start_reserved(
                    owner,
                    self.config.capacity,
                    self.config.maintenance,
                    Some(reservation),
                )
                .map_err(anyhow::Error::from)?;
                return Ok(Local {
                    backend: Arc::new(native),
                    member,
                    _challenges: challenges,
                });
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
                "published manifest changed repeatedly or is unavailable",
                "activation permit revoked",
                "owner challenge pending",
                "owner challenge unconfirmed",
            ]
            .into_iter()
            .find(|message| error.chain().any(|cause| cause.to_string() == *message))
            .unwrap_or("provider_or_validation");
            if reason == "owner challenge pending" {
                tracing::info!(
                    partition = id,
                    stage,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "native acquisition deferred"
                );
            } else {
                tracing::warn!(
                    partition = id,
                    stage,
                    reason,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "native acquisition failed"
                );
            }
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
    async fn drain(&self) {
        // Stop acquisition/maintenance before taking the final resident set.
        // Abort and join outside the mutex; cancellation needs no storage hint.
        let workers = std::mem::take(&mut *self.workers.lock().expect("cluster workers lock"));
        for worker in &workers {
            worker.abort();
        }
        for worker in workers {
            let _ = worker.await;
        }
        self.ready.store(false, Ordering::Release);
        // Retire every resident before storage I/O can consume the shutdown
        // budget. In-flight requests may still hold their permitted backend.
        let mut owned = Vec::new();
        for (id, partition) in self.partitions.iter().enumerate() {
            if let Some(local) = partition.local.write().await.take() {
                owned.push((
                    id,
                    local
                        .backend
                        .owner_incarnation()
                        .expect("native owner identity"),
                    local.member,
                ));
            }
        }
        futures::stream::iter(owned)
            .map(|(id, incarnation, member)| async move {
                let partition = &self.partitions[id];
                let withdraw = async {
                    let mut root = partition.connection.root()?;
                    let observed = root.observe().await?;
                    root.withdraw_route(observed, incarnation, member).await
                };
                if !matches!(withdraw.await, Ok(true)) {
                    tracing::warn!(partition = id, "native routing withdrawal deferred");
                }
            })
            .buffer_unordered(4)
            .for_each(|_| async {})
            .await;
    }

    async fn resolve(&self, db: &str) -> Result<Option<Arc<dyn Backend>>> {
        let id = self.config.layout.partition(db)?;
        let partition = &self.partitions[id as usize];
        let backend: Arc<dyn Backend> = match partition
            .local
            .read()
            .await
            .as_ref()
            .map(|local| local.backend.clone())
        {
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
fn contains(peers: &[Member], address: SocketAddr) -> bool {
    peers.iter().any(|peer| peer.address == address)
}
fn member_hint(peers: &[Member], address: SocketAddr) -> anyhow::Result<uuid::Uuid> {
    let member = peers
        .iter()
        .find(|peer| peer.address == address)
        .context("local member is absent")?;
    let mut hash = Sha256::new();
    hash.update(b"durable-db.member.v1\0");
    hash.update(member.id.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    // A bounded UUIDv8 fingerprint, not an authentication credential.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(uuid::Uuid::from_bytes(bytes))
}
fn membership(peers: &[Member]) -> anyhow::Result<Vec<Member>> {
    ensure!(peers.len() <= MAX_MEMBERS, "membership exceeds 256 peers");
    for peer in peers {
        peer.validate()?;
    }
    let mut sorted = peers.to_vec();
    sorted.sort_unstable();
    ensure!(
        sorted.windows(2).all(|pair| pair[0].id != pair[1].id),
        "duplicate member ID"
    );
    let addresses: std::collections::BTreeSet<_> = peers.iter().map(|peer| peer.address).collect();
    ensure!(addresses.len() == peers.len(), "duplicate peer address");
    Ok(sorted)
}
/// Rendezvous selection moves only partitions whose preferred node changes.
/// Ordering/duplicates are handled by membership validation, never as authority.
fn preferred(partition: u32, peers: &[Member]) -> Option<SocketAddr> {
    peers
        .iter()
        .max_by_key(|peer| {
            let mut hash = Sha256::new();
            hash.update(b"durable-db.placement.v1\0");
            hash.update(partition.to_be_bytes());
            hash.update(peer.id.as_bytes());
            (hash.finalize(), &peer.id)
        })
        .map(|peer| peer.address)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacing_endpoints_preserves_logical_placement() {
        let mut peers: Vec<_> = (0..6)
            .map(|n| Member {
                id: format!("durable-db-{n}"),
                address: SocketAddr::from(([127, 0, 0, 1], 8000 + n)),
            })
            .collect();
        let winners: Vec<_> = (0..1024)
            .map(|p| {
                peers
                    .iter()
                    .find(|m| Some(m.address) == preferred(p, &peers))
                    .unwrap()
                    .id
                    .clone()
            })
            .collect();
        let hints: std::collections::BTreeMap<_, _> = peers
            .iter()
            .map(|peer| (peer.id.clone(), member_hint(&peers, peer.address).unwrap()))
            .collect();
        peers.reverse();
        for index in 0..peers.len() {
            peers[index].address.set_port(9000 + index as u16);
            let validated = membership(&peers).unwrap();
            for peer in &validated {
                assert_eq!(
                    member_hint(&validated, peer.address).unwrap(),
                    hints[&peer.id]
                );
            }
            for (p, expected) in winners.iter().enumerate() {
                let winner = preferred(p as u32, &validated).unwrap();
                assert_eq!(
                    &validated.iter().find(|m| m.address == winner).unwrap().id,
                    expected
                );
            }
        }
        let mut duplicate = peers[0].clone();
        duplicate.address.set_port(12345);
        peers.push(duplicate);
        assert!(membership(&peers).is_err());
    }
}
