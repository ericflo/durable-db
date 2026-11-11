//! DNS supplies placement hints; it never grants serving permission.
use anyhow::ensure;
use std::{collections::BTreeSet, future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

/// Keep this handle alive while serving. With no service, membership contains
/// only this address. DNS service peers use its port and address family.
pub struct Membership {
    receiver: watch::Receiver<Vec<SocketAddr>>,
    worker: JoinHandle<()>,
}
impl Membership {
    pub fn start(address: SocketAddr, service: Option<String>) -> anyhow::Result<Self> {
        crate::ownership::root::valid_address(address)?;
        if let Some(service) = &service {
            validate_service(service)?;
        }
        let initial = if service.is_none() {
            vec![address]
        } else {
            vec![]
        };
        let (send, receiver) = watch::channel(initial);
        let worker = tokio::spawn(async move {
            let Some(service) = service else {
                send.closed().await;
                return;
            };
            let service: Arc<str> = service.into();
            follow(send, move || {
                let service = service.clone();
                async move {
                    let answers =
                        tokio::net::lookup_host((service.as_ref(), address.port())).await?;
                    normalize(answers, address.is_ipv4())
                }
            })
            .await;
        });
        Ok(Self { receiver, worker })
    }
    pub fn subscribe(&self) -> watch::Receiver<Vec<SocketAddr>> {
        self.receiver.clone()
    }
}
impl Drop for Membership {
    fn drop(&mut self) {
        self.worker.abort();
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
        let address = "127.0.0.1:7654".parse().unwrap();
        let static_members = Membership::start(address, None).unwrap();
        let mut receiver = static_members.subscribe();
        assert_eq!(*receiver.borrow(), [address]);
        drop(static_members);
        assert!(receiver.changed().await.is_err());
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
    }
}
