use super::*;
mod budget;
mod catalog;
mod checkpoint;
mod http;
mod lifecycle;
mod live;
mod registration;
mod replay;
mod transactions;
use crate::{
    providers::Connection,
    publication::{Authority, Bucket},
    storage::Remote,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[derive(Clone, Default)]
struct ManualClock {
    now: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
}
impl ManualClock {
    fn set(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn read(&self) -> Option<Duration> {
        (!self.failed.load(Ordering::SeqCst))
            .then(|| Duration::from_nanos(self.now.load(Ordering::SeqCst)))
    }
}

struct Provider {
    files: Arc<Remote>,
    history: Mutex<Vec<Bucket>>,
    stale: Mutex<Option<usize>>,
    calls: AtomicU64,
    reads: AtomicU64,
    maintenance: AtomicU64,
    lose_reply: AtomicBool,
    corrupt_reply: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Provider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            files: Arc::new(
                Remote::new(Arc::new(object_store::memory::InMemory::new()), "fixture").unwrap(),
            ),
            history: Mutex::new(vec![Bucket {
                revision: 0,
                version: None,
                info: BTreeMap::new(),
            }]),
            stale: Mutex::new(None),
            calls: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            maintenance: AtomicU64::new(0),
            lose_reply: AtomicBool::new(false),
            corrupt_reply: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: Default::default(),
            release: Default::default(),
        })
    }
    fn connection(self: &Arc<Self>, prefix: &str) -> Connection {
        Connection::new(self.files.clone(), self.clone(), prefix)
    }
    fn root(self: &Arc<Self>, clock: &ManualClock) -> Root<ManualClock> {
        self.connection("test")
            .root(clock.clone(), profile())
            .unwrap()
    }
    fn record(&self) -> Record {
        let history = self.history.lock().unwrap();
        let info = &history.last().unwrap().info;
        metadata::decode(
            info,
            "test",
            profile(),
            info.contains_key(crate::upload_budget::FIELD),
        )
        .unwrap()
        .unwrap()
    }
}
#[async_trait]
impl Authority for Provider {
    async fn read(&self) -> anyhow::Result<Bucket> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let history = self.history.lock().unwrap();
        Ok(history[self.stale.lock().unwrap().unwrap_or(history.len() - 1)].clone())
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mut history = self.history.lock().unwrap();
        let current = history.last().unwrap();
        // Keep B2's no-op shortcut; the shared nonce wrapper must prevent it.
        if current.info == info {
            return Ok(Some(current.clone()));
        }
        if current.revision != old.revision {
            return Ok(None);
        }
        let mut bucket = Bucket {
            revision: current.revision + 1,
            version: None,
            info,
        };
        history.push(bucket.clone());
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost response");
        }
        if self.corrupt_reply.swap(false, Ordering::SeqCst) {
            bucket
                .info
                .insert("durable-db-prefix".into(), "unexpected".into());
        }
        Ok(Some(bucket))
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        self.maintenance.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.files.delete(key).await
    }
}

fn profile() -> Profile {
    Profile::new(Duration::from_nanos(10), Duration::from_nanos(1)).unwrap()
}
async fn claim(root: &mut Root<ManualClock>) {
    let observed = root.observe().await.unwrap();
    let request = root.claim(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}
async fn validate(root: &mut Root<ManualClock>) {
    let observed = root.observe().await.unwrap();
    let request = root.validate(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}
async fn activate(root: &mut Root<ManualClock>) {
    let request = root.activate().unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}
async fn active(provider: &Arc<Provider>, clock: &ManualClock) -> Root<ManualClock> {
    let mut root = provider.root(clock);
    claim(&mut root).await;
    clock.set(23);
    validate(&mut root).await;
    activate(&mut root).await;
    assert!(root.permission().unwrap().is_some());
    root
}

#[tokio::test]
async fn bootstrap_drains_and_activation_never_extends_the_earned_interval() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = provider.root(&clock);
    assert!(root.permission().unwrap().is_none());
    assert_eq!(root.child_duration().unwrap(), Duration::ZERO);
    claim(&mut root).await;
    assert!(root.permission().unwrap().is_none());
    assert!(matches!(root.activate(), Err(Error::NotReady)));
    clock.set(22);
    validate(&mut root).await;
    assert!(matches!(root.activate(), Err(Error::NotReady)));
    clock.set(23);
    activate(&mut root).await;
    assert_eq!(root.permission().unwrap().unwrap().epoch, 1);
    assert_eq!(root.child_duration().unwrap(), Duration::from_nanos(3));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    clock.set(31);
    assert!(root.permission().unwrap().is_some());
    assert_eq!(root.child_duration().unwrap(), Duration::ZERO);
    clock.set(32); // Renewal began at 22; activation at 23 did not move expiry.
    assert!(root.permission().unwrap().is_none());
}

#[tokio::test]
async fn competing_candidates_bind_once_and_only_the_exact_winner_can_revalidate() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut a = provider.root(&clock);
    let mut b = provider.root(&clock);
    let a_observed = a.observe().await.unwrap();
    let b_observed = b.observe().await.unwrap();
    let a_request = a.claim(a_observed).unwrap();
    let b_request = b.claim(b_observed).unwrap();
    assert!(matches!(
        b.finish(b_request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    assert!(matches!(
        a.finish(a_request.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert_eq!(provider.record().identity.incarnation, b.incarnation);
    let observed = a.observe().await.unwrap();
    assert!(matches!(a.claim(observed), Err(Error::Bound)));
    let observed = a.observe().await.unwrap();
    assert!(matches!(a.validate(observed), Err(Error::NotOwner)));
    let mut c = provider.root(&clock);
    claim(&mut c).await;
    assert_eq!(provider.record().identity.epoch, 2);
    assert_eq!(provider.record().identity.incarnation, c.incarnation);
    // An old empty-register observation cannot reset epoch two to epoch one.
    *provider.stale.lock().unwrap() = Some(0);
    let mut stale = provider.root(&clock);
    let observed = stale.observe().await.unwrap();
    let request = stale.claim(observed).unwrap();
    assert!(matches!(
        stale.finish(request.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert_eq!(provider.record().identity.epoch, 2);
}

#[tokio::test]
async fn claim_applied_after_timeout_needs_validation_and_a_new_drain() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = provider.root(&clock);
    let observed = root.observe().await.unwrap();
    let delayed = root.claim(observed).unwrap();
    root.abandon();
    clock.set(3);
    assert!(matches!(
        root.finish(delayed.execute().await).unwrap(),
        Outcome::Ignored
    ));
    assert!(root.permission().unwrap().is_none());
    assert!(root.drain.is_none());
    clock.set(5);
    validate(&mut root).await;
    clock.set(27);
    validate(&mut root).await;
    assert!(matches!(root.activate(), Err(Error::NotReady)));
    clock.set(28);
    activate(&mut root).await;
    assert!(root.permission().unwrap().is_some());
}

#[tokio::test]
async fn lost_claim_and_activation_replies_recover_without_inventing_permission() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = provider.root(&clock);
    let observed = root.observe().await.unwrap();
    let request = root.claim(observed).unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    assert!(root.drain.is_none());
    assert!(root.permission().unwrap().is_none());
    clock.set(5);
    validate(&mut root).await;
    clock.set(28);
    validate(&mut root).await;
    let request = root.activate().unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    assert!(root.permission().unwrap().is_none());
    // Observing the open record does not authorize reads. Matching validation
    // recovers the earlier, already-drained activation without restarting its drain.
    let observed = root.observe().await.unwrap();
    assert!(root.permission().unwrap().is_none());
    clock.set(29);
    let request = root.validate(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    assert!(root.permission().unwrap().is_some());
    clock.set(39);
    assert!(root.permission().unwrap().is_none());
}

#[tokio::test]
async fn late_old_writes_and_replies_cannot_overlap_a_successors_interval() {
    for apply_before_fence in [false, true] {
        let provider = Provider::new();
        let clock = ManualClock::default();
        let mut old = active(&provider, &clock).await;
        clock.set(24);
        let observation = old.observe();
        // Network work owns its request state; local permission checks remain
        // available while an observation future is outstanding.
        assert!(old.permission().unwrap().is_some());
        let observed = observation.await.unwrap();
        let request = old.validate(observed).unwrap();
        // Starting or abandoning renewal preserves the old interval, ending at 33.
        assert!(old.permission().unwrap().is_some());
        old.abandon();
        let mut deferred = Some(request);
        let early_reply = if apply_before_fence {
            Some(deferred.take().unwrap().execute().await)
        } else {
            None
        };
        clock.set(25);
        let mut successor = provider.root(&clock);
        claim(&mut successor).await;
        assert!(old.permission().unwrap().is_some());
        let reply = match early_reply {
            Some(reply) => reply,
            None => deferred.take().unwrap().execute().await,
        };
        clock.set(48);
        validate(&mut successor).await;
        activate(&mut successor).await;
        assert!(matches!(old.finish(reply).unwrap(), Outcome::Ignored));
        assert!(old.permission().unwrap().is_none());
        assert_eq!(successor.permission().unwrap().unwrap().epoch, 2);
        // A stale observation of the old owner's metadata cannot renew it.
        *provider.stale.lock().unwrap() = Some(3);
        let observed = old.observe().await.unwrap();
        let request = old.validate(observed).unwrap();
        assert!(matches!(
            old.finish(request.execute().await).unwrap(),
            Outcome::Conflict
        ));
        assert!(old.permission().unwrap().is_none());
    }
}

#[tokio::test]
async fn delayed_replies_consume_their_original_interval() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = provider.root(&clock);
    let observed = root.observe().await.unwrap();
    let request = root.claim(observed).unwrap();
    let reply = request.execute().await;
    clock.set(20);
    assert!(matches!(root.finish(reply).unwrap(), Outcome::Confirmed));
    let observed = root.observe().await.unwrap();
    let request = root.validate(observed).unwrap();
    let reply = request.execute().await;
    clock.set(45);
    assert!(matches!(root.finish(reply).unwrap(), Outcome::Confirmed));
    assert!(root.drained().unwrap());
    assert!(matches!(root.activate(), Err(Error::NotReady)));
    validate(&mut root).await;
    activate(&mut root).await;
    assert!(root.permission().unwrap().is_some());
}

#[tokio::test]
async fn foreign_observations_and_completions_do_not_consume_the_current_request() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut a = provider.root(&clock);
    let mut b = provider.root(&clock);
    let foreign = a.observe().await.unwrap();
    assert!(matches!(
        b.claim(foreign),
        Err(Error::Control(control::Error::ForeignObservation))
    ));
    assert!(b.epoch.is_none());
    let observed = a.observe().await.unwrap();
    let a_request = a.claim(observed).unwrap();
    let observed = b.observe().await.unwrap();
    let b_request = b.claim(observed).unwrap();
    assert!(matches!(
        b.finish(a_request.execute().await).unwrap(),
        Outcome::Ignored
    ));
    assert!(matches!(
        b.finish(b_request.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert!(b.pending.is_none());
}

#[tokio::test]
async fn failed_clocks_and_corrupt_confirmations_cannot_be_revived() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = active(&provider, &clock).await;
    let old_identity = root.permission().unwrap().unwrap();
    clock.failed.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.permission(),
        Err(Error::Timing(timing::Error::Retired))
    ));
    clock.failed.store(false, Ordering::SeqCst);
    assert!(root.permission().is_err());
    let mut replacement = provider.root(&clock);
    assert_ne!(replacement.incarnation, old_identity.incarnation);
    claim(&mut replacement).await;
    clock.set(46);
    validate(&mut replacement).await;
    activate(&mut replacement).await;
    let observed = replacement.observe().await.unwrap();
    let request = replacement.validate(observed).unwrap();
    provider.corrupt_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        replacement.finish(request.execute().await),
        Err(Error::Incompatible)
    ));
    assert!(matches!(replacement.permission(), Err(Error::Retired)));
    assert!(matches!(replacement.observe().await, Err(Error::Retired)));
}

#[tokio::test]
async fn incompatible_namespaces_profiles_and_metadata_fail_before_writes() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let connection = provider.connection("test");
    for format in [
        "3",
        "4",
        "5",
        "owned-candidate-1",
        "owned-candidate-2",
        "owned-candidate-8",
        "owned-candidate-9",
        "future",
    ] {
        provider.history.lock().unwrap()[0].info =
            BTreeMap::from([("durable-db-format".into(), format.into())]);
        let root = connection.root(clock.clone(), profile()).unwrap();
        assert!(matches!(root.observe().await, Err(Error::Incompatible)));
    }
    provider.history.lock().unwrap()[0].info.clear();
    let mut root = provider.root(&clock);
    claim(&mut root).await;
    let calls = provider.calls.load(Ordering::SeqCst);
    let different = Profile::new(Duration::from_nanos(11), Duration::from_nanos(1)).unwrap();
    let mismatch = connection.root(clock.clone(), different).unwrap();
    assert!(matches!(mismatch.observe().await, Err(Error::Incompatible)));
    let wrong_prefix = provider
        .connection("other")
        .root(clock.clone(), profile())
        .unwrap();
    assert!(matches!(
        wrong_prefix.observe().await,
        Err(Error::Incompatible)
    ));
    assert!(
        provider
            .connection(&"x".repeat(129))
            .root(clock.clone(), profile())
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    let valid = provider
        .history
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .info
        .clone();
    for value in ["{}".to_owned(), "x".repeat(1025)] {
        let mut invalid = valid.clone();
        invalid.insert("durable-db-owned-root".into(), value);
        assert!(metadata::decode(&invalid, "test", profile(), false).is_err());
    }
    for field in [
        "epoch",
        "incarnation",
        "catalog",
        "lifetime_ns",
        "error_ns",
        "extra",
    ] {
        let mut invalid = valid.clone();
        let mut wire: serde_json::Value =
            serde_json::from_str(&invalid["durable-db-owned-root"]).unwrap();
        wire[field] = if field == "incarnation" || field == "catalog" {
            serde_json::json!(Uuid::nil().to_string())
        } else {
            serde_json::json!(0)
        };
        invalid.insert("durable-db-owned-root".into(), wire.to_string());
        assert!(
            metadata::decode(&invalid, "test", profile(), false).is_err(),
            "{field}"
        );
    }
}

#[tokio::test]
async fn metadata_limits_and_epoch_exhaustion_do_not_send_or_bind_a_claim() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    provider.history.lock().unwrap()[0].info = (0..7)
        .map(|n| (format!("custom-{n}"), "keep".into()))
        .collect();
    let mut root = provider.root(&clock);
    let observed = root.observe().await.unwrap();
    assert!(matches!(root.claim(observed), Err(Error::Incompatible)));
    assert!(root.epoch.is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    provider.history.lock().unwrap()[0].info.remove("custom-6");
    claim(&mut root).await;
    assert_eq!(
        provider.history.lock().unwrap().last().unwrap().info.len(),
        10
    );
    assert_eq!(
        provider.history.lock().unwrap().last().unwrap().info["custom-0"],
        "keep"
    );
    {
        let mut history = provider.history.lock().unwrap();
        let info = &mut history.last_mut().unwrap().info;
        let mut wire: serde_json::Value =
            serde_json::from_str(&info["durable-db-owned-root"]).unwrap();
        wire["epoch"] = serde_json::json!(u64::MAX);
        info.insert("durable-db-owned-root".into(), wire.to_string());
    }
    let mut fresh = provider.root(&clock);
    let observed = fresh.observe().await.unwrap();
    assert!(matches!(fresh.claim(observed), Err(Error::Exhausted)));
    assert!(fresh.epoch.is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}
