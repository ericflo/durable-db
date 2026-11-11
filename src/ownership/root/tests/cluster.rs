use super::routing::{Server, app, auth};
use super::*;
use crate::{
    api::{Backend, Cluster, ClusterConfig},
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
        let address = listener.local_addr().unwrap();
        let (membership, receiver) = watch::channel(peers);
        let cluster = Cluster::open(
            directory,
            ClusterConfig {
                layout: Layout::new(2).unwrap(),
                address,
                limits: Limits::default(),
                upload_limits: budget(),
                capacity: 16,
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
    tokio::time::timeout(Duration::from_secs(8), async {
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
async fn unconfirmed_acquisition_never_enters_the_serving_map_and_retry_recovers() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = Node::start(directory(&provider).await, listener, vec![]).await;
    provider.pause_publication.store(true, Ordering::SeqCst);
    provider
        .lose_publication_reply
        .store(true, Ordering::SeqCst);
    node.members(&[node.address]);
    provider.entered.notified().await;
    assert!(!node.cluster.ready().await);
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
    let token = auth().administrator_token(600).unwrap();
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
    for fault in 0..5 {
        let mut config = ClusterConfig {
            layout: Layout::new(2).unwrap(),
            address,
            limits: Limits::default(),
            upload_limits: budget(),
            capacity: 16,
        };
        let mut peers = vec![address];
        match fault {
            0 => config.capacity = 0,
            1 => config.limits.index.databases = 0,
            2 => config.limits.receipt_window = 0,
            3 => peers.push(address),
            _ => peers = vec![address; 257],
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
