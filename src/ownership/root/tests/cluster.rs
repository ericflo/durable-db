use super::routing::{Server, app, auth};
use super::*;
use crate::{
    api::{Backend, Cluster, ClusterConfig, Membership},
    auth::Access,
    cache::Cache,
    engine::Info,
    providers::partitions::{Layout, Partitions},
    transactions::live::Limits,
    upload_budget::UploadLimits,
};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use reqwest::StatusCode;
use serde_json::{Value, json};
use std::net::SocketAddr;
use tokio::{net::TcpListener, sync::watch};
use tower::ServiceExt;

fn budget() -> UploadLimits {
    UploadLimits {
        bytes: 256 * 1024 * 1024,
        attempts: 100_000,
    }
}
async fn directory(provider: &Arc<Provider>) -> Partitions {
    provider
        .connection("test")
        .into_partitions(budget(), Arc::new(Cache::new(1024 * 1024, None).unwrap()))
        .await
        .unwrap()
}
struct Node {
    server: Server,
    cluster: Arc<Cluster>,
    address: SocketAddr,
    membership: watch::Sender<Vec<SocketAddr>>,
}
impl Node {
    async fn start(directory: Partitions, listener: TcpListener, peers: Vec<SocketAddr>) -> Self {
        Self::with_layout(directory, listener, peers, Layout::new(2).unwrap()).await
    }
    async fn with_layout(
        directory: Partitions,
        listener: TcpListener,
        peers: Vec<SocketAddr>,
        layout: Layout,
    ) -> Self {
        let (membership, receiver) = watch::channel(peers);
        Self::with_membership(directory, listener, layout, membership, receiver).await
    }
    async fn monitored(
        directory: Partitions,
        listener: TcpListener,
        membership: watch::Sender<Vec<SocketAddr>>,
    ) -> (Self, Membership) {
        let monitor = Membership::observe(membership.subscribe()).unwrap();
        let node = Self::with_membership(
            directory,
            listener,
            Layout::new(2).unwrap(),
            membership,
            monitor.subscribe(),
        )
        .await;
        (node, monitor)
    }
    async fn with_membership(
        directory: Partitions,
        listener: TcpListener,
        layout: Layout,
        membership: watch::Sender<Vec<SocketAddr>>,
        receiver: watch::Receiver<Vec<SocketAddr>>,
    ) -> Self {
        let address = listener.local_addr().unwrap();
        let cluster = Cluster::open(
            directory,
            ClusterConfig {
                layout,
                address,
                limits: Limits::default(),
                upload_limits: budget(),
                capacity: 16,
                maintenance: Duration::from_secs(60),
            },
            Arc::new(Cache::new(1024 * 1024, None).unwrap()),
            Arc::new(auth()),
            receiver,
        )
        .await
        .unwrap();
        let server = Server::serve(listener, app(cluster.clone()));
        Self {
            server,
            cluster,
            address,
            membership,
        }
    }
    fn members(&self, peers: &[SocketAddr]) {
        self.membership.send_replace(peers.to_vec());
    }
}
// Select representative ephemeral addresses with one partition on each node.
// The assertions below observe actual owner identities and data, not this score.
fn preferred(id: u32, peers: &[SocketAddr]) -> SocketAddr {
    use sha2::{Digest, Sha256};
    *peers
        .iter()
        .max_by_key(|peer| {
            let mut h = Sha256::new();
            h.update(b"durable-db.placement.v1\0");
            h.update(id.to_be_bytes());
            h.update(peer.to_string().as_bytes());
            (h.finalize(), **peer)
        })
        .unwrap()
}
fn names() -> [String; 2] {
    std::array::from_fn(|id| {
        (0..100)
            .map(|n| format!("app-{n}"))
            .find(|name| Layout::new(2).unwrap().partition(name).unwrap() == id as u32)
            .unwrap()
    })
}
async fn settled(nodes: &[&Node], peers: &[SocketAddr]) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut done = true;
            for node in nodes {
                done &= node.cluster.ready().await == peers.contains(&node.address);
                for (id, db) in names().iter().enumerate() {
                    let local = node
                        .cluster
                        .resolve(db)
                        .await
                        .unwrap()
                        .unwrap()
                        .owner_incarnation();
                    done &= local.is_some()
                        == (!peers.is_empty() && preferred(id as u32, peers) == node.address);
                }
            }
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("placement converges");
}
fn commit(id: usize) -> Value {
    json!({"request_id":"first", "expected_revision":0,
        "writes":[{"key":"x", "value": if id==0 {"b25l"} else {"dHdv"}}]})
}

#[tokio::test]
async fn shared_partition_checkpoints_keep_both_writers_progressing() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peers = vec![listener.local_addr().unwrap()];
    let layout = Layout::new(16).unwrap();
    let directory = directory(&provider).await;
    let node = Node::with_layout(directory.clone(), listener, peers, layout).await;
    let collection = directory
        .partition(0, budget())
        .await
        .unwrap()
        .root()
        .unwrap()
        .collection();
    let token = auth().administrator_token(600).unwrap();
    let names: Vec<_> = (0..1000)
        .map(|n| format!("tenant-{n}"))
        .filter(|name| layout.partition(name).unwrap() == 0)
        .take(2)
        .collect();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node
                .cluster
                .resolve(&names[0])
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for name in &names {
        node.server
            .ok(
                "PUT",
                &format!("/v1/databases/{name}"),
                &token,
                json!({"request_id":"create", "limits":{"max_keys":1000,"max_bytes":1048576}}),
            )
            .await;
    }
    let write = async |name: &str| {
        for revision in 0..200 {
            let request = json!({"expected_revision":revision, "request_id":format!("write-{revision}"),
                "writes":[{"key":format!("key-{revision}"),"value":"eA=="}]});
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let (status, value) = node
                        .server
                        .call(
                            "POST",
                            &format!("/v1/databases/{name}/commit"),
                            &token,
                            request.clone(),
                        )
                        .await;
                    if status == StatusCode::OK {
                        break value;
                    }
                    assert!(
                        matches!(
                            status,
                            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                        ),
                        "{status}: {value}"
                    );
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("both tenants resume after concurrent collection");
            assert_eq!(result["revision"], revision + 1);
        }
    };
    tokio::select! {
        _ = async { tokio::join!(write(&names[0]), write(&names[1])); } => (),
        _ = async {
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = collection.collect().await;
                let _ = directory.maintenance().await;
            }
            std::future::pending::<()>().await;
        } => unreachable!(),
    }
}

#[tokio::test(start_paused = true)]
async fn storage_challenges_preserve_split_owners_then_recover_without_dns_changes() {
    let provider = Provider::new();
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = first.local_addr().unwrap();
    let second = loop {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = [address, listener.local_addr().unwrap()];
        if preferred(0, &peers) != preferred(1, &peers) {
            break listener;
        }
    };
    let peers = [address, second.local_addr().unwrap()];
    let directory = directory(&provider).await;
    let a = Node::start(directory.clone(), first, peers.to_vec()).await;
    let b = Node::start(directory, second, peers.to_vec()).await;
    settled(&[&a, &b], &peers).await;
    let mut before = Vec::new();
    for (id, name) in names().iter().enumerate() {
        let node = if preferred(id as u32, &peers) == a.address {
            &a
        } else {
            &b
        };
        before.push(
            node.cluster
                .resolve(name)
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .unwrap(),
        );
    }
    a.members(&[a.address]);
    b.members(&[b.address]);
    // Two disjoint views both want every partition. More than three challenge
    // deadlines pass, but actual owner identities must not churn.
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for (id, name) in names().iter().enumerate() {
            let node = if preferred(id as u32, &peers) == a.address {
                &a
            } else {
                &b
            };
            assert_eq!(
                node.cluster
                    .resolve(name)
                    .await
                    .unwrap()
                    .unwrap()
                    .owner_incarnation(),
                Some(before[id])
            );
        }
    }
    // Retain the old data handle to exercise permit fencing separately from
    // responder lifetime. No membership change tells A that B has stopped.
    let id = (0..2)
        .find(|id| preferred(*id, &peers) == b.address)
        .unwrap() as usize;
    let old = b.cluster.resolve(&names()[id]).await.unwrap().unwrap();
    drop(b);
    settled(&[&a], &[a.address]).await;
    assert_ne!(
        a.cluster
            .resolve(&names()[id])
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation(),
        Some(before[id])
    );
    assert!(matches!(
        old.info(&names()[id], &Access::Administrator).await,
        Err(crate::Error::Storage(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn uncertain_claim_retry_reuses_only_the_completed_owner_challenge() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = first.local_addr().unwrap();
    let first = Node::with_layout(
        directory.clone(),
        first,
        vec![address],
        Layout::new(1).unwrap(),
    )
    .await;
    settled(&[&first], &[address]).await;
    drop(first);
    tokio::task::yield_now().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let successor = Node::with_layout(
        directory.clone(),
        listener,
        vec![address],
        Layout::new(1).unwrap(),
    )
    .await;
    let observer = directory
        .partition(0, budget())
        .await
        .unwrap()
        .root()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while observer.observe().await.unwrap().challenge().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Reject the claim after its wait, then restore the provider. The worker's
    // next acquisition must not pay another deadline for this same probe.
    tokio::time::sleep(Duration::from_secs(14)).await;
    provider.fail_replacements.store(100, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(provider.fail_replacements.swap(0, Ordering::SeqCst) < 100);
    tokio::time::timeout(Duration::from_secs(8), settled(&[&successor], &[address]))
        .await
        .expect("a provider error must not restart the unchanged challenge wait");
}

#[tokio::test(start_paused = true)]
async fn one_replacement_waits_for_all_partition_challenges_without_serial_waves() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let layout = Layout::new(16).unwrap();
    let names: Vec<_> = (0..16)
        .map(|id| {
            (0..1000)
                .map(|n| format!("app-{n}"))
                .find(|name| layout.partition(name).unwrap() == id)
                .unwrap()
        })
        .collect();
    async fn all_owned(node: &Node, names: &[String]) {
        for name in names {
            while node
                .cluster
                .resolve(name)
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .is_none()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let old = Node::with_layout(directory.clone(), listener, vec![address], layout).await;
    tokio::time::timeout(Duration::from_secs(10), all_owned(&old, &names))
        .await
        .unwrap();
    drop(old);
    tokio::task::yield_now().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let replacement = Node::with_layout(directory, listener, vec![address], layout).await;
    // Four active acquisitions remain the bound. Sixteen idle waits must not
    // serialize into four fifteen-second waves before recovery can start.
    tokio::time::timeout(Duration::from_secs(25), all_owned(&replacement, &names))
        .await
        .expect("waiting for a challenge must release its acquisition slot");
}

#[tokio::test]
async fn unreachable_peer_in_unchanged_dns_is_replaced_and_its_old_owner_is_fenced() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let second = loop {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = [address, listener.local_addr().unwrap()];
        if preferred(0, &peers) != preferred(1, &peers) {
            break listener;
        }
    };
    let peers = vec![address, second.local_addr().unwrap()];
    let (dns, _) = watch::channel(peers.clone());
    let (a, _a_monitor) = Node::monitored(directory(&provider).await, listener, dns.clone()).await;
    let (b, b_monitor) = Node::monitored(directory(&provider).await, second, dns.clone()).await;
    settled(&[&a, &b], &peers).await;
    let id = (0..2)
        .find(|id| preferred(*id, &peers) == b.address)
        .unwrap() as usize;
    let name = &names()[id];
    let path = format!("/v1/databases/{name}");
    let token = auth().administrator_token(600).unwrap();
    a.server
        .ok(
            "PUT",
            &path,
            &token,
            json!({"request_id":"create","limits":{"max_keys":20,"max_bytes":10000}}),
        )
        .await;
    let request = commit(id);
    let mut receipt = a
        .server
        .ok("POST", &format!("{path}/commit"), &token, request.clone())
        .await;
    assert_eq!(receipt["replayed"], false);
    receipt["replayed"] = json!(true);
    let old = b.cluster.resolve(name).await.unwrap().unwrap();
    let address = b.address;
    drop(b);
    drop(b_monitor);
    tokio::task::yield_now().await;
    // TCP accepts, but HTTP never responds: a connect-only probe is insufficient.
    let stalled = Server::serve(
        TcpListener::bind(address).await.unwrap(),
        axum::Router::new().route(
            "/health/live",
            axum::routing::get(|| async { std::future::pending::<&'static str>().await }),
        ),
    );
    settled(&[&a], &[a.address]).await;
    assert_eq!(
        *dns.borrow(),
        peers,
        "discovery never withdrew the failed address"
    );
    assert_ne!(
        a.cluster
            .resolve(name)
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation(),
        old.owner_incarnation()
    );
    assert!(
        matches!(
            old.info(name, &Access::Administrator).await,
            Err(crate::Error::Storage(_))
        ),
        "a still-retained old owner must fail durable permit validation"
    );
    assert_eq!(
        a.server
            .ok("POST", &format!("{path}/commit"), &token, request.clone())
            .await,
        receipt
    );
    drop(stalled);
    tokio::task::yield_now().await;
    let (b, _b_monitor) = Node::monitored(
        directory(&provider).await,
        TcpListener::bind(address).await.unwrap(),
        dns.clone(),
    )
    .await;
    settled(&[&a, &b], &peers).await;
    // The router still remembers the pre-failure incarnation. Refuse it once;
    // only the client's identical retry may use the freshly discovered owner.
    let (status, _) = a
        .server
        .call("POST", &format!("{path}/commit"), &token, request.clone())
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        a.server
            .ok("POST", &format!("{path}/commit"), &token, request)
            .await,
        receipt
    );
}

#[tokio::test]
async fn membership_scales_one_two_one_then_recovers_without_compute() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let a = Node::start(directory(&provider).await, listener, vec![address]).await;
    settled(&[&a], &[address]).await;
    let second = loop {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = [address, listener.local_addr().unwrap()];
        if preferred(0, &peers) != preferred(1, &peers) {
            break listener;
        }
    };
    let b = Node::start(directory(&provider).await, second, vec![address]).await;
    settled(&[&a, &b], &[address]).await;
    let token = auth().administrator_token(600).unwrap();
    let mut old = Vec::new();
    let mut before = Vec::new();
    for (id, name) in names().iter().enumerate() {
        let path = format!("/v1/databases/{name}");
        b.server
            .ok(
                "PUT",
                &path,
                &token,
                json!({"request_id":"create","limits":{"max_keys":20,"max_bytes":10000}}),
            )
            .await;
        a.server
            .ok("POST", &format!("{path}/commit"), &token, commit(id))
            .await;
        before.push(
            b.server
                .ok(
                    "POST",
                    &format!("{path}/read"),
                    &token,
                    json!({"keys":["x"]}),
                )
                .await,
        );
        old.push(a.cluster.resolve(name).await.unwrap().unwrap());
    }
    let peers = [a.address, b.address];
    a.members(&peers);
    b.members(&peers);
    settled(&[&a, &b], &peers).await;
    for (id, name) in names().iter().enumerate() {
        let path = format!("/v1/databases/{name}");
        let target = if preferred(id as u32, &peers) == a.address {
            &a
        } else {
            &b
        };
        let current = target.cluster.resolve(name).await.unwrap().unwrap();
        if target.address == b.address {
            assert_ne!(current.owner_incarnation(), old[id].owner_incarnation());
            assert!(matches!(
                old[id].info(name, &Access::Administrator).await,
                Err(crate::Error::Storage(_))
            ));
        } else {
            assert_eq!(current.owner_incarnation(), old[id].owner_incarnation());
        }
        assert_eq!(
            target
                .server
                .ok(
                    "POST",
                    &format!("{path}/read"),
                    &token,
                    json!({"keys":["x"]})
                )
                .await,
            before[id]
        );
        assert_eq!(
            target
                .server
                .ok("POST", &format!("{path}/commit"), &token, commit(id))
                .await["replayed"],
            true
        );
    }
    drop(old);
    // Remove a still-live node. All partitions move to b without moving keys.
    a.members(&[b.address]);
    b.members(&[b.address]);
    settled(&[&a, &b], &[b.address]).await;
    for (id, name) in names().iter().enumerate() {
        assert_eq!(
            b.server
                .ok(
                    "POST",
                    &format!("/v1/databases/{name}/read"),
                    &token,
                    json!({"keys":["x"]})
                )
                .await,
            before[id]
        );
    }
    let weak_a = Arc::downgrade(&a.cluster);
    let weak_b = Arc::downgrade(&b.cluster);
    drop(a);
    drop(b);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak_a.upgrade().is_some() || weak_b.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("controllers stop without leaking background owners");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cold = Node::start(directory(&provider).await, listener, vec![address]).await;
    settled(&[&cold], &[address]).await;
    for (id, name) in names().iter().enumerate() {
        let path = format!("/v1/databases/{name}");
        assert_eq!(
            cold.server
                .ok(
                    "POST",
                    &format!("{path}/read"),
                    &token,
                    json!({"keys":["x"]})
                )
                .await,
            before[id]
        );
        assert_eq!(
            cold.server
                .ok("POST", &format!("{path}/commit"), &token, commit(id))
                .await["replayed"],
            true
        );
    }
    drop(cold.membership);
    tokio::time::timeout(Duration::from_secs(3), async {
        while cold.cluster.ready().await
            || cold
                .cluster
                .resolve(&names()[0])
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closed membership retires owners");
}

#[tokio::test]
async fn unconfirmed_acquisition_stays_unserved_and_recovers_without_a_new_claim() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let directory = directory(&provider).await;
    let node = Node::start(directory.clone(), listener, vec![]).await;
    provider.pause_publication.store(true, Ordering::SeqCst);
    provider
        .lose_publication_reply
        .store(true, Ordering::SeqCst);
    node.members(&[node.address]);
    provider.entered.notified().await;
    assert!(
        node.cluster.ready().await,
        "routing stays reachable during recovery"
    );
    assert_eq!(
        reqwest::get(format!("{}/health/ready", node.server.url))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let token = auth().administrator_token(600).unwrap();
    assert_eq!(
        node.server
            .call("GET", "/v1/databases/app", &token, Value::Null)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE,
        "HTTP readiness must not authorize an unconfirmed owner"
    );
    for db in names() {
        assert!(
            node.cluster
                .resolve(&db)
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .is_none()
        );
    }
    provider.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider.lose_publication_reply.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    node.members(&[node.address]);
    settled(&[&node], &[node.address]).await;
    for (id, name) in names().iter().enumerate() {
        let record = directory
            .lazy_partition(id as u32)
            .unwrap()
            .root()
            .unwrap()
            .observe()
            .await
            .unwrap()
            .record
            .unwrap();
        assert_eq!(record.identity.epoch, 1, "lost reply added another owner");
        assert_eq!(
            node.cluster
                .resolve(name)
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation(),
            Some(record.identity.incarnation)
        );
    }
    node.server
        .ok(
            "PUT",
            "/v1/databases/app",
            &token,
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
}

struct Leaf(Uuid, u64);
#[async_trait]
impl Backend for Leaf {
    fn owner_incarnation(&self) -> Option<Uuid> {
        Some(self.0)
    }
    async fn memory_bytes(&self) -> usize {
        0
    }
    async fn info(&self, _: &str, _: &Access) -> crate::Result<Info> {
        Ok(Info {
            revision: self.1,
            bytes: 0,
            keys: 0,
            limits: Default::default(),
            policy: None,
        })
    }
}
struct Switching {
    first: Arc<Leaf>,
    calls: AtomicU64,
}
#[async_trait]
impl Backend for Switching {
    async fn memory_bytes(&self) -> usize {
        0
    }
    async fn resolve(&self, _: &str) -> crate::Result<Option<Arc<dyn Backend>>> {
        Ok(Some(if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first.clone()
        } else {
            Arc::new(Leaf(Uuid::new_v4(), 999))
        }))
    }
}
#[tokio::test]
async fn admission_pins_the_checked_backend_and_refuses_cross_scope_before_selection() {
    let leaf = Arc::new(Leaf(Uuid::new_v4(), 7));
    let selector = Arc::new(Switching {
        first: leaf.clone(),
        calls: AtomicU64::new(0),
    });
    let router = app(selector.clone());
    let tenant = auth().issue("other".into(), false, 600).unwrap();
    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/databases/app")
                .header("authorization", format!("Bearer {tenant}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(selector.calls.load(Ordering::SeqCst), 0);
    let reply = router
        .oneshot(
            Request::builder()
                .uri("/v1/databases/%61pp")
                .header(
                    "authorization",
                    format!("Bearer {}", auth().administrator_token(600).unwrap()),
                )
                .header("x-durable-db-owner", leaf.0.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), StatusCode::OK);
    let value: Value =
        serde_json::from_slice(&reply.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["revision"], 7);
    assert_eq!(selector.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_cluster_configuration_cannot_acquire_or_change_the_layout() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let address = "127.0.0.1:7654".parse().unwrap();
    let cache = Arc::new(Cache::new(0, None).unwrap());
    let before = provider.calls.load(Ordering::SeqCst);
    for fault in 0..6 {
        let mut config = ClusterConfig {
            layout: Layout::new(2).unwrap(),
            address,
            limits: Limits::default(),
            upload_limits: budget(),
            capacity: 16,
            maintenance: Duration::from_secs(60),
        };
        let mut peers = vec![address];
        match fault {
            0 => config.capacity = 0,
            1 => config.limits.index.databases = 0,
            2 => config.limits.receipt_window = 0,
            3 => peers.push(address),
            4 => peers = vec![address; 257],
            _ => config.limits.index.receipts = 2048, // Valid alone, too small when split.
        }
        let (_send, receive) = watch::channel(peers);
        assert!(
            Cluster::open(
                directory.clone(),
                config,
                cache.clone(),
                Arc::new(auth()),
                receive
            )
            .await
            .is_err()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn a_failed_partition_does_not_starve_later_acquisitions() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = Node::start(directory.clone(), listener, vec![]).await;
    let first = directory.partition(0, budget()).await.unwrap();
    let original = first.authority().read().await.unwrap();
    let mut damaged = original.info.clone();
    damaged.insert("durable-db-format".into(), "unsupported".into());
    first
        .authority()
        .replace(&original, damaged)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while node
            .cluster
            .resolve(&names()[1])
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation()
            .is_none()
        {
            node.members(&[node.address]);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("another partition's failure cannot pin the retry cursor");
    assert!(
        node.cluster
            .resolve(&names()[0])
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation()
            .is_none()
    );
    let token = auth().administrator_token(600).unwrap();
    node.server
        .ok(
            "PUT",
            &format!("/v1/databases/{}", names()[1]),
            &token,
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
    let damaged = first.authority().read().await.unwrap();
    first
        .authority()
        .replace(&damaged, original.info)
        .await
        .unwrap()
        .unwrap();
    node.members(&[node.address]);
    settled(&[&node], &[node.address]).await;
}

struct PausedPermit {
    inner: Arc<Remote>,
    pause: AtomicBool,
    blocked: tokio::sync::Mutex<()>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl crate::storage::Storage for PausedPermit {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<crate::storage::Object>> {
        if key.contains("/permits/") && self.pause.swap(false, Ordering::SeqCst) {
            let _blocked = self.blocked.lock().await;
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.get(key, max).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        size: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        self.inner.get_range(key, range, size).await
    }
    async fn put(&self, key: &str, bytes: bytes::Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }
    async fn cas(
        &self,
        key: &str,
        old: Option<&crate::storage::Version>,
        bytes: bytes::Bytes,
    ) -> anyhow::Result<bool> {
        self.inner.cas(key, old, bytes).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
}
#[tokio::test]
async fn retired_backend_references_keep_the_process_budget_until_released() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::start(directory(&provider).await, listener, vec![address]).await;
    settled(&[&node], &[address]).await;
    let mut held = Vec::new();
    for name in names() {
        held.push(node.cluster.resolve(&name).await.unwrap().unwrap());
    }
    node.members(&[]);
    settled(&[&node], &[]).await;
    let calls = provider.calls.load(Ordering::SeqCst);
    node.members(&[address]);
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        node.cluster.ready().await,
        "full residency does not hide routing or administration"
    );
    for name in names() {
        assert!(
            node.cluster
                .resolve(&name)
                .await
                .unwrap()
                .unwrap()
                .owner_incarnation()
                .is_none()
        );
    }
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "full process budget refuses before ownership I/O"
    );
    drop(held.pop());
    node.members(&[address]);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut count = 0;
            for name in names() {
                count += usize::from(
                    node.cluster
                        .resolve(&name)
                        .await
                        .unwrap()
                        .unwrap()
                        .owner_incarnation()
                        .is_some(),
                );
            }
            if count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        node.cluster.ready().await,
        "routing remains available while one old reference occupies a slot"
    );
    drop(held);
    node.members(&[address]);
    settled(&[&node], &[address]).await;
}

#[tokio::test]
async fn empty_unfunded_cluster_exposes_admin_refill_before_acquisition() {
    let provider = Provider::new();
    let directory = provider
        .connection("test")
        .into_partitions(
            UploadLimits::default(),
            Arc::new(Cache::new(0, None).unwrap()),
        )
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::start(directory, listener, vec![address]).await;
    let token = auth().administrator_token(600).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !node.cluster.ready().await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reqwest::get(format!("{}/health/ready", node.server.url))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "Kubernetes must expose the administrator refill endpoint with zero credit"
    );
    let status = node
        .server
        .ok("GET", "/v1/storage/upload-budget", &token, Value::Null)
        .await;
    assert_eq!(status["limits"]["bytes"], 0);
    assert_eq!(status["allocated"]["bytes"], 0);
    let refill = json!({"request_id":"bootstrap", "expected_configuration_revision":0,
        "limits":{"bytes":268435456,"attempts":100000}});
    assert_eq!(
        node.server
            .ok("POST", "/v1/storage/upload-budget", &token, refill.clone())
            .await["replayed"],
        false
    );
    assert_eq!(
        node.server
            .ok("POST", "/v1/storage/upload-budget", &token, refill)
            .await["replayed"],
        true
    );
    node.members(&[address]);
    settled(&[&node], &[address]).await;
    node.server
        .ok(
            "PUT",
            "/v1/databases/app",
            &token,
            json!({"request_id":"create", "limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
    assert_eq!(
        node.server
            .ok("POST", "/v1/databases/app/commit", &token, commit(0))
            .await["revision"],
        1
    );
}

#[tokio::test(start_paused = true)]
async fn paused_acquisition_does_not_block_queued_partitions() {
    let provider = Provider::new();
    let files = Arc::new(PausedPermit {
        inner: provider.files.clone(),
        pause: AtomicBool::new(true),
        blocked: Default::default(),
        entered: Default::default(),
        release: Default::default(),
    });
    let directory = Connection::new(files.clone(), provider, "test")
        .into_partitions(budget(), Arc::new(Cache::new(0, None).unwrap()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let layout = Layout::new(8).unwrap();
    let node = Node::with_layout(directory, listener, vec![], layout).await;
    let names: Vec<_> = (0..8)
        .map(|id| {
            (0..1000)
                .map(|n| format!("app-{n}"))
                .find(|name| layout.partition(name).unwrap() == id)
                .unwrap()
        })
        .collect();
    node.members(&[node.address]);
    files.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut serving = 0;
            for name in &names {
                serving += usize::from(
                    node.cluster
                        .resolve(name)
                        .await
                        .unwrap()
                        .unwrap()
                        .owner_incarnation()
                        .is_some(),
                );
            }
            if serving == 7 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("free acquisition slots must admit queued partitions");
    assert!(
        files.blocked.try_lock().is_err(),
        "slow recovery remains in flight"
    );
    let token = auth().administrator_token(600).unwrap();
    node.server
        .ok(
            "PUT",
            &format!("/v1/databases/{}", names[7]),
            &token,
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
    files.release.notify_one();
}

#[tokio::test(start_paused = true)]
async fn slow_acquisition_survives_elapsed_time_but_cancels_on_placement_change() {
    let provider = Provider::new();
    let files = Arc::new(PausedPermit {
        inner: provider.files.clone(),
        pause: AtomicBool::new(true),
        blocked: Default::default(),
        entered: Default::default(),
        release: Default::default(),
    });
    let directory = Connection::new(files.clone(), provider.clone(), "test")
        .into_partitions(budget(), Arc::new(Cache::new(0, None).unwrap()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = Node::start(directory, listener, vec![]).await;
    node.members(&[node.address]);
    files.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for db in names() {
                let backend = node.cluster.resolve(&db).await.unwrap().unwrap();
                if backend.owner_incarnation().is_some() && backend.ready().await {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a paused partition cannot block another acquisition");
    assert!(
        node.cluster.ready().await,
        "one paused partition cannot hide the HTTP router"
    );
    let calls = provider.calls.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(40)).await;
    // Repeated identical membership must not abort useful acquisition either.
    node.members(&[node.address]);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    // A new peer that owns neither local partition must not restart recovery.
    let peer = (1..=u16::MAX)
        .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
        .find(|peer| {
            *peer != node.address
                && (0..2).all(|id| preferred(id, &[node.address, *peer]) == node.address)
        })
        .unwrap();
    node.members(&[peer, node.address]);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(files.blocked.try_lock().is_err(), "recovery was canceled");
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "elapsed time or unchanged local placement restarted the paused operation"
    );
    let mut paused = None;
    for (id, name) in names().iter().enumerate() {
        if node
            .cluster
            .resolve(name)
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation()
            .is_none()
        {
            paused = Some(id as u32);
        }
    }
    let paused = paused.unwrap();
    let peer = (1..=u16::MAX)
        .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
        .find(|peer| {
            *peer != node.address
                && preferred(paused, &[node.address, *peer]) == node.address
                && preferred(1 - paused, &[node.address, *peer]) == *peer
        })
        .unwrap();
    node.members(&[peer, node.address]);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(
        files.blocked.try_lock().is_err(),
        "another partition's move canceled recovery"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    assert!(
        node.cluster
            .resolve(&names()[(1 - paused) as usize])
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation()
            .is_none(),
        "moved owner must retire without waiting for recovery"
    );
    node.members(&[]);
    // Let the controller observe removal before restoring the same address;
    // watch snapshots intentionally coalesce unobserved intermediate changes.
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    settled(&[&node], &[]).await;
    // The old provider future remains blocked. Only cancellation can release
    // the controller to acquire for this new placement.
    node.members(&[node.address]);
    settled(&[&node], &[node.address]).await;
}

#[tokio::test(start_paused = true)]
async fn shared_maintenance_runs_once_and_moves_with_membership() {
    let provider = Provider::new();
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peers = [first.local_addr().unwrap(), second.local_addr().unwrap()];
    let a = Node::start(directory(&provider).await, first, peers.to_vec()).await;
    let b = Node::start(directory(&provider).await, second, peers.to_vec()).await;
    settled(&[&a, &b], &peers).await;
    assert_eq!(provider.maintenance.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(provider.maintenance.load(Ordering::SeqCst), 1);
    let survivor = if preferred(0, &peers) == a.address {
        &b
    } else {
        &a
    };
    a.members(&[survivor.address]);
    b.members(&[survivor.address]);
    settled(&[&a, &b], &[survivor.address]).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(provider.maintenance.load(Ordering::SeqCst), 2);
    a.members(&[]);
    b.members(&[]);
    settled(&[&a, &b], &[]).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(provider.maintenance.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn unavailable_permit_reads_preserve_idle_owners_but_refuse_data() {
    let provider = Provider::new();
    let faults = Arc::new(super::http::Faults::default());
    let files = Arc::new(super::http::Files {
        inner: provider.files.clone(),
        faults: faults.clone(),
    });
    let directory = Connection::new(files, provider.clone(), "test")
        .into_partitions(budget(), Arc::new(Cache::new(0, None).unwrap()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::start(directory, listener, vec![address]).await;
    settled(&[&node], &[address]).await;
    let old = node.cluster.resolve("app").await.unwrap().unwrap();
    let token = auth().administrator_token(600).unwrap();
    let calls = provider.calls.load(Ordering::SeqCst);
    faults.fail_permit.store(true, Ordering::SeqCst);
    assert_eq!(
        node.server
            .call("GET", "/v1/databases/app", &token, json!(null))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE,
    );
    // Reconcile unchanged placement while the provider remains unavailable.
    node.members(&[address]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(old.ready().await, "a failed observation is not revocation");
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    faults.fail_permit.store(false, Ordering::SeqCst);
    assert_eq!(
        node.cluster
            .resolve("app")
            .await
            .unwrap()
            .unwrap()
            .owner_incarnation(),
        old.owner_incarnation(),
    );
    assert_eq!(
        node.server
            .call("GET", "/v1/databases/app", &token, json!(null))
            .await
            .0,
        StatusCode::NOT_FOUND,
    );
}

#[tokio::test]
async fn absent_uncertain_journal_reacquires_and_excludes_a_late_old_upload() {
    use crate::storage::Storage;
    let provider = Provider::new();
    let faults = Arc::new(super::http::Faults::default());
    let files = Arc::new(super::http::Files {
        inner: provider.files.clone(),
        faults: faults.clone(),
    });
    let directory = Connection::new(files, provider.clone(), "test")
        .into_partitions(budget(), Arc::new(Cache::new(1024 * 1024, None).unwrap()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::start(directory, listener, vec![address]).await;
    settled(&[&node], &[address]).await;
    let token = auth().administrator_token(600).unwrap();
    node.server
        .ok(
            "PUT",
            "/v1/databases/app",
            &token,
            json!({"request_id":"create", "limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
    let old = node.cluster.resolve("app").await.unwrap().unwrap();
    faults.fail_journal.store(true, Ordering::SeqCst);
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        node.server
            .call("POST", "/v1/databases/app/commit", &token, commit(0)),
    )
    .await
    .unwrap();
    assert_eq!(response.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!old.ready().await);
    drop(old); // Release its resident reservation, like a completed HTTP request.
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let backend = node.cluster.resolve("app").await.unwrap().unwrap();
            if backend.owner_incarnation().is_some() && backend.ready().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        node.server
            .ok("POST", "/v1/databases/app/commit", &token, commit(0))
            .await["revision"],
        1
    );
    // The first provider PUT arrives only after the successor acknowledged.
    let (key, bytes) = faults.late_upload.lock().unwrap().take().unwrap();
    provider.files.put(&key, bytes).await.unwrap();
    node.members(&[]);
    settled(&[&node], &[]).await;
    node.members(&[address]);
    settled(&[&node], &[address]).await;
    let retry = node
        .server
        .ok("POST", "/v1/databases/app/commit", &token, commit(0))
        .await;
    assert_eq!(retry["revision"], 1);
    assert_eq!(retry["replayed"], true);
    assert_eq!(
        node.server
            .ok(
                "POST",
                "/v1/databases/app/read",
                &token,
                json!({"keys":["x"]})
            )
            .await["values"][0],
        "b25l"
    );
}
