//! DNS and bounded HTTP observations supply hints, never serving permission.
use anyhow::ensure;
use futures::{StreamExt, stream};
use std::{collections::BTreeSet, future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

/// Keep this handle alive while serving. With no service, membership contains
/// only this address. DNS service peers use its port and address family.
pub struct Membership {
    receiver: watch::Receiver<Vec<SocketAddr>>,
    workers: Vec<JoinHandle<()>>,
}
impl Membership {
    pub fn start(address: SocketAddr, service: Option<String>) -> anyhow::Result<Self> {
        crate::ownership::root::valid_address(address)?;
        if let Some(service) = &service {
            validate_service(service)?;
        }
        let Some(service) = service else {
            let (send, receiver) = watch::channel(vec![address]);
            return Ok(Self {
                receiver,
                workers: vec![tokio::spawn(async move {
                    send.closed().await;
                })],
            });
        };
        let (send, candidates) = watch::channel(vec![]);
        let mut membership = Self::observe(candidates)?;
        let service: Arc<str> = service.into();
        membership.workers.push(tokio::spawn(follow(send, move || {
            let service = service.clone();
            async move {
                let answers = tokio::net::lookup_host((service.as_ref(), address.port())).await?;
                normalize(answers, address.is_ipv4())
            }
        })));
        Ok(membership)
    }
    pub(crate) fn observe(candidates: watch::Receiver<Vec<SocketAddr>>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(1))
            .pool_max_idle_per_host(0)
            .build()?;
        let (send, receiver) = watch::channel(vec![]);
        let worker = tokio::spawn(filter(send, candidates, move |address| {
            let client = client.clone();
            async move {
                client
                    .get(format!("http://{address}/health/live"))
                    .send()
                    .await
                    .is_ok_and(|reply| reply.status() == reqwest::StatusCode::OK)
            }
        }));
        Ok(Self {
            receiver,
            workers: vec![worker],
        })
    }
    pub fn subscribe(&self) -> watch::Receiver<Vec<SocketAddr>> {
        self.receiver.clone()
    }
}
impl Drop for Membership {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.abort();
        }
    }
}
fn validate_service(service: &str) -> anyhow::Result<()> {
    let name = service.strip_suffix('.').unwrap_or(service);
    ensure!(
        !name.is_empty()
            && name.len() <= 253
            && name.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }),
        "discovery service must be a DNS name"
    );
    Ok(())
}
fn normalize(
    answers: impl IntoIterator<Item = SocketAddr>,
    ipv4: bool,
) -> anyhow::Result<Vec<SocketAddr>> {
    let mut peers = BTreeSet::new();
    for address in answers {
        if address.is_ipv4() != ipv4 {
            continue;
        }
        crate::ownership::root::valid_address(address)?;
        peers.insert(address);
        ensure!(
            peers.len() <= super::cluster::MAX_MEMBERS,
            "discovery exceeds peer bound"
        );
    }
    Ok(peers.into_iter().collect())
}
async fn follow<F, Fut>(send: watch::Sender<Vec<SocketAddr>>, mut resolve: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<SocketAddr>>>,
{
    let mut poll = tokio::time::interval(Duration::from_secs(3));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = send.closed() => return,
            _ = poll.tick() => (),
        }
        // Do not start another lookup on timeout: system DNS can use a blocking
        // resolver that survives cancellation. At most one may be outstanding.
        let result = tokio::select! {
            _ = send.closed() => return,
            result = resolve() => result,
        };
        match result {
            Ok(peers) => {
                send.send_if_modified(|current| {
                    if *current == peers {
                        return false;
                    }
                    *current = peers;
                    true
                });
            }
            Err(_) => tracing::warn!("membership lookup unavailable; retaining last snapshot"),
        }
    }
}

// Resolve and probe independently: a stuck system lookup cannot pin a failed
// peer in placement. New peers must answer; an eligible peer survives one miss.
async fn filter<F, Fut>(
    send: watch::Sender<Vec<SocketAddr>>,
    mut candidates: watch::Receiver<Vec<SocketAddr>>,
    probe: F,
) where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = bool>,
{
    let mut poll = tokio::time::interval(Duration::from_secs(3));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut missed = BTreeSet::new();
    loop {
        tokio::select! {
            _ = send.closed() => return,
            _ = poll.tick() => (),
            update = candidates.changed() => if update.is_err() { return; },
        }
        let peers = candidates.borrow_and_update().clone();
        let round = stream::iter(peers)
            .map(|peer| {
                let observation = probe(peer);
                async move { (peer, observation.await) }
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>();
        let observations = tokio::select! {
            _ = send.closed() => return,
            update = candidates.changed() => {
                if update.is_err() { return; }
                // Canceled, idempotent probes do not count as failures. Refresh
                // immediately without overlapping the superseded round.
                poll.reset_immediately();
                continue;
            }
            observations = round => observations,
        };
        let previous = send.borrow().clone();
        let mut eligible = Vec::new();
        let mut failures = BTreeSet::new();
        for (peer, alive) in observations {
            if alive || (previous.contains(&peer) && !missed.contains(&peer)) {
                eligible.push(peer);
            }
            if !alive {
                failures.insert(peer);
            }
        }
        missed = failures;
        eligible.sort_unstable();
        send.send_if_modified(|current| {
            if *current == eligible {
                return false;
            }
            *current = eligible;
            true
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn canonical_discovery_is_bounded_and_uses_one_address_family() {
        let a = "127.0.0.1:7654".parse().unwrap();
        let b = "127.0.0.2:7654".parse().unwrap();
        let v6 = "[::1]:7654".parse().unwrap();
        assert_eq!(normalize([b, a, a, v6], true).unwrap(), [a, b]);
        assert_eq!(normalize([a, v6], false).unwrap(), [v6]);
        assert!(normalize(["0.0.0.0:7654".parse().unwrap()], true).is_err());
        assert!(normalize((1..=257).map(|port| SocketAddr::new(a.ip(), port)), true).is_err());
        for name in ["durable-db-peers", "peers.namespace.svc.cluster.local."] {
            validate_service(name).unwrap();
        }
        for name in [
            "",
            ".",
            "a..b",
            "-a",
            "a-",
            "https://peers",
            "peers:7654",
            "a/b",
        ] {
            assert!(validate_service(name).is_err());
        }
        assert!(validate_service(&"a".repeat(64)).is_err());
        assert!(validate_service(&("a.".repeat(127) + "a")).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn failures_keep_membership_and_slow_lookups_cannot_overlap() {
        let a = "127.0.0.1:7654".parse().unwrap();
        let b = "127.0.0.2:7654".parse().unwrap();
        let (send, mut receive) = watch::channel(vec![]);
        let (answers, pending) = tokio::sync::mpsc::unbounded_channel();
        let pending = Arc::new(tokio::sync::Mutex::new(pending));
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let worker = tokio::spawn(follow(send, move || {
            let pending = pending.clone();
            count.fetch_add(1, Ordering::SeqCst);
            async move { pending.lock().await.recv().await.unwrap() }
        }));
        answers.send(Ok(vec![a])).unwrap();
        receive.changed().await.unwrap();
        assert_eq!(*receive.borrow_and_update(), [a]);
        answers.send(Err(anyhow::anyhow!("DNS failure"))).unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        assert!(!receive.has_changed().unwrap());
        assert_eq!(*receive.borrow(), [a]);
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        let stalled = calls.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            stalled,
            "no overlapping blocking resolutions"
        );
        answers.send(Ok(vec![b])).unwrap();
        receive.changed().await.unwrap();
        assert_eq!(*receive.borrow_and_update(), [b]);
        answers.send(Ok(vec![])).unwrap();
        receive.changed().await.unwrap();
        assert!(receive.borrow().is_empty());
        drop(receive);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn lifetime_handle_controls_static_and_dns_membership() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let static_members = Membership::start(address, None).unwrap();
        let mut receiver = static_members.subscribe();
        assert_eq!(*receiver.borrow(), [address]);
        drop(static_members);
        assert!(receiver.changed().await.is_err());
        let server = tokio::spawn(async move {
            let app = axum::Router::new()
                .route("/health/live", axum::routing::get(|| async { "ok" }))
                .route(
                    "/health/ready",
                    axum::routing::get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
                );
            axum::serve(listener, app).await.unwrap();
        });
        // Exercises the actual system resolver without depending on public DNS.
        let dns = Membership::start(address, Some("localhost".into())).unwrap();
        let mut receiver = dns.subscribe();
        tokio::time::timeout(Duration::from_secs(3), receiver.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(receiver.borrow().contains(&address));
        drop(dns);
        assert!(receiver.changed().await.is_err());
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_candidates_are_rechecked_with_one_miss_of_hysteresis() {
        let a = "127.0.0.1:7654".parse().unwrap();
        let b = "127.0.0.2:7654".parse().unwrap();
        let (dns, candidates) = watch::channel(vec![a, b]);
        let (send, mut receive) = watch::channel(vec![]);
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let up = alive.clone();
        let worker = tokio::spawn(filter(send, candidates, move |peer| {
            let up = up.clone();
            async move { peer == a || up.load(Ordering::SeqCst) }
        }));
        receive.changed().await.unwrap();
        assert_eq!(*receive.borrow_and_update(), [a, b]);
        alive.store(false, Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(3)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(
            !receive.has_changed().unwrap(),
            "one failure must not churn placement"
        );
        tokio::time::advance(Duration::from_secs(3)).await;
        receive.changed().await.unwrap();
        assert_eq!(*receive.borrow_and_update(), [a]);
        assert_eq!(*dns.borrow(), [a, b]);
        alive.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(3)).await;
        receive.changed().await.unwrap();
        assert_eq!(*receive.borrow_and_update(), [a, b]);
        dns.send_replace(vec![]);
        receive.changed().await.unwrap();
        assert!(receive.borrow().is_empty());
        drop(receive);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn probe_rounds_are_bounded_and_cancel_on_new_candidates_or_shutdown() {
        async fn reaches(active: &AtomicUsize, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                while active.load(Ordering::SeqCst) != expected {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }
        struct Active(Arc<AtomicUsize>);
        impl Drop for Active {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let peers = (1..=super::super::cluster::MAX_MEMBERS)
            .map(|port| SocketAddr::from(([127, 0, 0, 1], port as u16)))
            .collect();
        let (dns, candidates) = watch::channel(peers);
        let (send, receive) = watch::channel(vec![]);
        let active = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (count, called) = (active.clone(), calls.clone());
        let worker = tokio::spawn(filter(send, candidates, move |_| {
            let (count, called) = (count.clone(), called.clone());
            async move {
                let _guard = Active(count.clone());
                assert!(count.fetch_add(1, Ordering::SeqCst) < 16);
                called.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<bool>().await
            }
        }));
        reaches(&active, 16).await;
        assert_eq!(calls.load(Ordering::SeqCst), 16);
        dns.send_replace(vec![]);
        reaches(&active, 0).await;
        dns.send_replace(vec!["127.0.0.1:7654".parse().unwrap()]);
        reaches(&active, 1).await;
        drop(receive);
        worker.await.unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}
