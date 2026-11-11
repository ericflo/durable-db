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
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl crate::storage::Storage for PausedPermit {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<crate::storage::Object>> {
        if key.contains("/permits/") && self.pause.swap(false, Ordering::SeqCst) {
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
async fn routine_health_validation_does_not_clear_readiness() {
    let provider = Provider::new();
    let files = Arc::new(PausedPermit {
        inner: provider.files.clone(),
        pause: AtomicBool::new(false),
        entered: Default::default(),
        release: Default::default(),
    });
    let directory = Connection::new(files.clone(), provider, "test")
        .into_partitions(budget(), Arc::new(Cache::new(0, None).unwrap()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::start(directory, listener, vec![address]).await;
    settled(&[&node], &[address]).await;
    files.pause.store(true, Ordering::SeqCst);
    node.members(&[address]);
    tokio::time::timeout(Duration::from_secs(2), files.entered.notified())
        .await
        .unwrap();
    assert!(
        node.cluster.ready().await,
        "a routine permit probe is not a placement loss"
    );
    files.release.notify_one();
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
    assert!(!node.cluster.ready().await);
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
        !node.cluster.ready().await,
        "one old reference still occupies a slot"
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
    assert!(!node.cluster.ready().await);
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
async fn slow_acquisition_survives_elapsed_time_but_cancels_on_membership_change() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = Node::start(directory, listener, vec![]).await;
    provider.pause_publication.store(true, Ordering::SeqCst);
    node.members(&[node.address]);
    provider.entered.notified().await;
    let calls = provider.calls.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(40)).await;
    // Repeated identical membership must not abort useful acquisition either.
    node.members(&[node.address]);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "elapsed time restarted the paused operation"
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
