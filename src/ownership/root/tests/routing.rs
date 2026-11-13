use super::*;
use crate::{
    api::{self, Backend, Forward, Native},
    auth::Auth,
};
use axum::{
    Router,
    extract::{Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use std::net::SocketAddr;
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

pub(super) fn auth() -> Auth {
    Auth::new(&"91".repeat(32)).unwrap()
}
#[derive(Default)]
struct Faults {
    commits: AtomicU64,
    lose: AtomicBool,
}
async fn lose_reply(State(faults): State<Arc<Faults>>, request: Request, next: Next) -> Response {
    let commit = request.uri().path().ends_with("/commit");
    if commit {
        faults.commits.fetch_add(1, Ordering::SeqCst);
    }
    // Forwarding an administrator must never put the legacy signing-key bearer
    // on the wire. Tenant claims remain scoped and are tested after revocation.
    if let Some(token) = request.headers().get("authorization") {
        assert!(!token.to_str().unwrap().contains("ddb_admin_"));
    }
    let result = next.run(request).await;
    if commit && faults.lose.swap(false, Ordering::SeqCst) {
        crate::Error::Storage(anyhow::anyhow!("injected lost reply")).into_response()
    } else {
        result
    }
}
pub(super) struct Server {
    pub(super) url: String,
    task: JoinHandle<()>,
    client: reqwest::Client,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(super) fn serve(listener: TcpListener, router: Router) -> Self {
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            url,
            task,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
        }
    }
    pub(super) async fn call(
        &self,
        method: &str,
        path: &str,
        token: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let reply = self
            .client
            .request(method.parse().unwrap(), format!("{}{path}", self.url))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = reply.status();
        (status, reply.json().await.unwrap())
    }
    pub(super) async fn ok(&self, method: &str, path: &str, token: &str, body: Value) -> Value {
        let (status, value) = self.call(method, path, token, body).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value
    }
}
pub(super) fn app(backend: Arc<dyn Backend>) -> Router {
    api::router(Arc::new(api::App::with_rate_limits(
        backend,
        auth(),
        32,
        api::RateLimits::new(20_000, 20_000).unwrap(),
    )))
}
async fn advertised(provider: &Arc<Provider>, address: SocketAddr) -> Root {
    let mut root = provider.root().advertise(address).unwrap();
    claim(&mut root).await;
    activate(&mut root).await;
    root.select_journals().await.unwrap();
    root
}
async fn owner(provider: &Arc<Provider>, faults: Arc<Faults>) -> (Server, Uuid) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = advertised(provider, listener.local_addr().unwrap()).await;
    let identity = root.identity().unwrap().unwrap().incarnation;
    let owner = live::open(root, crate::transactions::live::Limits::default()).await;
    let router = app(Arc::new(Native::new(owner, 16).unwrap()))
        .layer(middleware::from_fn_with_state(faults, lose_reply));
    (Server::serve(listener, router), identity)
}

#[tokio::test]
async fn selection_retry_preserves_http_receipts_and_refuses_the_live_predecessor() {
    let provider = Provider::new();
    let (old, _) = owner(&provider, Arc::new(Faults::default())).await;
    let admin = auth().administrator_token(60).unwrap();
    old.ok(
        "PUT",
        "/v1/databases/app",
        &admin,
        json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
    )
    .await;
    let commit = json!({"request_id":"write","expected_revision":0,
        "writes":[{"key":"x","value":"eA=="}]});
    let acknowledged = old
        .ok("POST", "/v1/databases/app/commit", &admin, commit.clone())
        .await;
    let mut root = active(&provider).await;
    let identity = root.identity().unwrap();
    provider.fail_replacements.store(1, Ordering::SeqCst);
    let calls = provider.calls.load(Ordering::SeqCst);
    root.select_journals().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst) - calls, 2);
    assert_eq!(root.identity().unwrap(), identity);
    let recovered = live::open(root, crate::transactions::live::Limits::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let next = Server::serve(listener, app(Arc::new(Native::new(recovered, 16).unwrap())));
    let retry = next
        .ok("POST", "/v1/databases/app/commit", &admin, commit)
        .await;
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["revision"], acknowledged["revision"]);
    let read = json!({"keys":["x"]});
    let values = next
        .ok("POST", "/v1/databases/app/read", &admin, read.clone())
        .await;
    assert_eq!(values["values"][0], "eA==");
    assert_eq!(
        old.call("POST", "/v1/databases/app/read", &admin, read)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn advertised_route_is_bound_to_claim_and_never_inherited() {
    let provider = Provider::new();
    for address in [
        "0.0.0.0:7654",
        "127.0.0.1:0",
        "224.0.0.1:7654",
        "255.255.255.255:1",
    ] {
        assert!(provider.root().advertise(address.parse().unwrap()).is_err());
    }
    let address = "[::1]:7654".parse().unwrap();
    let mut root = provider.root().advertise(address).unwrap();
    claim(&mut root).await;
    assert!(
        root.observe().await.unwrap().route().is_none(),
        "fence is not a route"
    );
    activate(&mut root).await;
    let expected = Route {
        identity: root.identity().unwrap().unwrap(),
        address,
    };
    assert_eq!(root.observe().await.unwrap().route(), Some(expected));
    assert!(matches!(root.advertise(address), Err(Error::Bound)));
    let successor = active(&provider).await;
    assert!(successor.observe().await.unwrap().route().is_none());
    assert!(!provider.read().await.unwrap().info["durable-db-owned-root"].contains("address"));
}

#[tokio::test]
async fn forwarding_preserves_auth_receipts_and_moves_after_live_takeover() {
    let provider = Provider::new();
    let faults = Arc::new(Faults::default());
    let (old, identity) = owner(&provider, faults.clone()).await;
    let forward = Arc::new(Forward::new(&provider.connection("test"), auth(), 8).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = Server::serve(listener, app(forward));
    let admin = auth().administrator_token(60).unwrap();
    let db = "/v1/databases/app";
    front
        .ok(
            "PUT",
            db,
            &admin,
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1048576}}),
        )
        .await;
    let credential = front
        .ok(
            "POST",
            "/v1/tokens",
            &admin,
            json!({"database":"app","write":true,"ttl_seconds":60}),
        )
        .await;
    let token = credential["token"].as_str().unwrap();
    let write =
        json!({"request_id":"write","expected_revision":0,"writes":[{"key":"x","value":"eA=="}]});
    faults.lose.store(true, Ordering::SeqCst);
    assert_eq!(
        front
            .call("POST", &format!("{db}/commit"), token, write.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        faults.commits.load(Ordering::SeqCst),
        1,
        "forwarder replayed an uncertain request"
    );
    assert_eq!(
        front
            .ok("POST", &format!("{db}/commit"), token, write.clone())
            .await["replayed"],
        true
    );
    let read = json!({"keys":["x"]});
    let before = front
        .ok("POST", &format!("{db}/read"), token, read.clone())
        .await;
    let controls = provider.reads.load(Ordering::SeqCst);
    assert_eq!(
        front
            .ok("POST", &format!("{db}/read"), token, read.clone())
            .await,
        before
    );
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        controls,
        "warm routing read the control authority"
    );
    assert_eq!(
        front
            .call("GET", "/v1/databases/other", token, json!(null))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for target in [&old, &front] {
        let response = target
            .client
            .get(format!("{}{db}", target.url))
            .bearer_auth(&admin)
            .header("x-durable-db-owner", Uuid::new_v4().to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    assert_eq!(
        old.client
            .get(format!("{}{db}", old.url))
            .bearer_auth(&admin)
            .header("x-durable-db-owner", identity.to_string())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let (_new, _) = owner(&provider, Arc::new(Faults::default())).await;
    assert_eq!(
        front
            .call("POST", &format!("{db}/read"), token, read.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        front.ok("POST", &format!("{db}/read"), token, read).await,
        before
    );
    assert_eq!(
        front
            .ok("POST", &format!("{db}/commit"), token, write)
            .await["replayed"],
        true
    );
    front.ok("POST", &format!("{db}/access"), &admin, json!({"request_id":"revoke","expected_policy_revision":1,"change":{"action":"revoke","token_id":credential["token_id"]}})).await;
    assert_eq!(
        front.call("GET", db, token, json!(null)).await.0,
        StatusCode::UNAUTHORIZED,
        "forwarding replaced the revoked token identity"
    );
}

#[tokio::test]
async fn forwarding_refuses_redirects_and_bounds_inflight_work() {
    let provider = Provider::new();
    let trap_calls = Arc::new(AtomicU64::new(0));
    let count = trap_calls.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let trap = Server::serve(
        listener,
        Router::new().fallback(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _root = advertised(&provider, listener.local_addr().unwrap()).await;
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let (a, b, url) = (entered.clone(), resume.clone(), trap.url.clone());
    let _source = Server::serve(
        listener,
        Router::new().fallback(move || {
            let (a, b, url) = (a.clone(), b.clone(), url.clone());
            async move {
                a.notify_one();
                b.notified().await;
                (StatusCode::TEMPORARY_REDIRECT, [("location", url)])
            }
        }),
    );
    let forward = Arc::new(Forward::new(&provider.connection("test"), auth(), 1).unwrap());
    let next = forward.clone();
    let pending =
        tokio::spawn(async move { next.info("app", &crate::auth::Access::Administrator).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        forward
            .info("app", &crate::auth::Access::Administrator)
            .await,
        Err(crate::Error::Busy)
    ));
    resume.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(crate::Error::Storage(_))
    ));
    assert_eq!(trap_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn delayed_failure_or_cancellation_does_not_clear_a_refreshed_route() {
    for cancel in [false, true] {
        let provider = Provider::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _root = advertised(&provider, listener.local_addr().unwrap()).await;
        let calls = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (count, a, b) = (calls.clone(), entered.clone(), resume.clone());
        let _server = Server::serve(
            listener,
            Router::new().fallback(move || {
                let (count, a, b) = (count.clone(), a.clone(), b.clone());
                async move {
                    match count.fetch_add(1, Ordering::SeqCst) {
                        0 => {
                            a.notify_one();
                            b.notified().await;
                            crate::Error::Busy.into_response()
                        }
                        1 => crate::Error::Storage(anyhow::anyhow!("immediate failure"))
                            .into_response(),
                        4 => (StatusCode::OK, vec![b'x'; 1024 * 1024 + 1]).into_response(),
                        _ => crate::Error::NotFound.into_response(),
                    }
                }
            }),
        );
        let forward = Arc::new(Forward::new(&provider.connection("test"), auth(), 4).unwrap());
        let first = forward.clone();
        let pending =
            tokio::spawn(
                async move { first.info("app", &crate::auth::Access::Administrator).await },
            );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(matches!(
            forward
                .info("app", &crate::auth::Access::Administrator)
                .await,
            Err(crate::Error::Storage(_))
        ));
        assert!(matches!(
            forward
                .info("app", &crate::auth::Access::Administrator)
                .await,
            Err(crate::Error::NotFound)
        ));
        let reads = provider.reads.load(Ordering::SeqCst);
        if cancel {
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
        } else {
            resume.notify_one();
            assert!(matches!(pending.await.unwrap(), Err(crate::Error::Busy)));
        }
        assert!(matches!(
            forward
                .info("app", &crate::auth::Access::Administrator)
                .await,
            Err(crate::Error::NotFound)
        ));
        assert_eq!(
            provider.reads.load(Ordering::SeqCst),
            reads,
            "late failure erased the new cache instance"
        );
        let error = forward
            .info("app", &crate::auth::Access::Administrator)
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::Error::Storage(e) if e.to_string() == "owner response exceeds bound")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }
}

#[tokio::test]
async fn excluded_cached_peer_refreshes_before_sending_without_refusing_a_live_hint() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _old = advertised(&provider, address).await;
    let stalled = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicU64::new(0));
    let (blocked, count) = (stalled.clone(), calls.clone());
    let _server = Server::serve(
        listener,
        Router::new().fallback(move || {
            let (blocked, count) = (blocked.clone(), count.clone());
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                if blocked.load(Ordering::SeqCst) {
                    std::future::pending::<()>().await;
                }
                crate::Error::NotFound.into_response()
            }
        }),
    );
    let (members, view) = tokio::sync::watch::channel(vec![api::Member::from(address)]);
    let mut forward = Forward::new(&provider.connection("test"), auth(), 1).unwrap();
    forward.members = Some(view);
    let access = crate::auth::Access::Administrator;
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::NotFound)
    ));
    let reads = provider.reads.load(Ordering::SeqCst);
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::NotFound)
    ));
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        reads,
        "healthy routes stay cached"
    );

    // A false suspicion only expires the hint. The fresh root may still name
    // this reachable owner, so exclusion alone must not refuse its response.
    members.send_replace(Vec::new());
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::NotFound)
    ));
    stalled.store(true, Ordering::SeqCst);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _next = advertised(&provider, listener.local_addr().unwrap()).await;
    let _replacement = Server::serve(
        listener,
        Router::new().fallback(|| async { crate::Error::NotFound.into_response() }),
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), forward.info("app", &access))
            .await
            .expect("an excluded cached peer must not consume another response deadline"),
        Err(crate::Error::NotFound)
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "no stale send or hidden replay"
    );
}

#[tokio::test]
async fn cancelled_stalled_forward_cannot_pin_the_old_route() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _old = advertised(&provider, listener.local_addr().unwrap()).await;
    let calls = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(Notify::new());
    let (count, seen) = (calls.clone(), entered.clone());
    let _stalled = Server::serve(
        listener,
        Router::new().fallback(move || {
            count.fetch_add(1, Ordering::SeqCst);
            seen.notify_one();
            std::future::pending::<StatusCode>()
        }),
    );
    let forward = Arc::new(Forward::new(&provider.connection("test"), auth(), 1).unwrap());
    let first = forward.clone();
    let pending =
        tokio::spawn(async move { first.info("app", &crate::auth::Access::Administrator).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    pending.abort(); // Upstream cancellation precedes the 25-second peer timeout.
    assert!(pending.await.unwrap_err().is_cancelled());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _new = advertised(&provider, listener.local_addr().unwrap()).await;
    let _replacement = Server::serve(
        listener,
        Router::new().fallback(|| async { crate::Error::NotFound.into_response() }),
    );
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            forward.info("app", &crate::auth::Access::Administrator)
        )
        .await
        .expect("explicit retry must refresh the cancelled route"),
        Err(crate::Error::NotFound)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no stale replay");
}

#[tokio::test]
async fn admission_rejection_cannot_pin_a_stale_owner_route() {
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _old = advertised(&provider, listener.local_addr().unwrap()).await;
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let _server = Server::serve(
        listener,
        Router::new().fallback(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { crate::Error::Busy.into_response() }
        }),
    );
    let forward = Forward::new(&provider.connection("test"), auth(), 1).unwrap();
    let access = crate::auth::Access::Administrator;
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::Busy)
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _new = advertised(&provider, listener.local_addr().unwrap()).await;
    let _replacement = Server::serve(
        listener,
        Router::new().fallback(|| async { crate::Error::NotFound.into_response() }),
    );
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::NotFound)
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no internal replay or repeated stale route"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires nft and an isolated network namespace; see docs/verification.md"]
async fn unacknowledged_pooled_forward_releases_capacity_and_refreshes_route() {
    use std::process::Command;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn nft(command: &str) {
        assert!(
            Command::new("nft")
                .args(command.split_ascii_whitespace())
                .status()
                .unwrap()
                .success()
        );
    }
    struct Firewall(String);
    impl Drop for Firewall {
        fn drop(&mut self) {
            nft(&format!("delete table inet {}", self.0));
        }
    }
    // Only loopback may exist: run this opt-in fixture under unshare -Urn.
    let interfaces = std::fs::read_to_string("/proc/net/dev").unwrap();
    assert!(
        interfaces
            .lines()
            .skip(2)
            .all(|line| line.split(':').next().unwrap().trim() == "lo")
    );
    let provider = Provider::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _old = advertised(&provider, address).await;
    let forward = Forward::new(&provider.connection("test"), auth(), 1).unwrap();
    let access = crate::auth::Access::Administrator;
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        // Consume the complete GET (including its JSON null body), then keep
        // this socket alive. No second connection is accepted by the fixture.
        while !bytes.ends_with(b"\r\n\r\nnull") {
            bytes.push(socket.read_u8().await.unwrap());
            assert!(bytes.len() < 4096);
        }
        let body = br#"{"error":"not_found","message":"missing"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(body).await.unwrap();
        // A dropped request must not reach the old peer or be replayed here.
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::NotFound)
    ));
    let reads = provider.reads.load(Ordering::SeqCst);
    let table = format!("durable_db_forward_{}", std::process::id());
    nft(&format!("add table inet {table}"));
    let firewall = Firewall(table);
    nft(&format!(
        "add chain inet {} output {{ type filter hook output priority 0; policy accept; }}",
        firewall.0
    ));
    nft(&format!(
        "add rule inet {} output ip daddr 127.0.0.1 tcp dport {} counter drop",
        firewall.0,
        address.port()
    ));
    let started = std::time::Instant::now();
    let pending = forward.info("app", &access);
    tokio::pin!(pending);
    tokio::select! {
        _ = &mut pending => panic!("the retained connection must stall"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => (),
    }
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        reads,
        "must reuse the cached route"
    );
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::Busy)
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _new = advertised(&provider, listener.local_addr().unwrap()).await;
    let _replacement = Server::serve(
        listener,
        Router::new().fallback(|| async { crate::Error::NotFound.into_response() }),
    );
    let error = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("unacknowledged traffic must fail before the 25-second response deadline")
        .unwrap_err();
    let crate::Error::Storage(error) = error else {
        panic!("expected a transport error");
    };
    let error = error.downcast_ref::<reqwest::Error>().unwrap();
    assert!(
        !error.is_connect(),
        "must fail on the existing connection, not a new handshake"
    );
    assert!(started.elapsed() >= Duration::from_secs(1));
    let counters = Command::new("nft")
        .args(["list", "table", "inet", &firewall.0])
        .output()
        .unwrap();
    let counters = String::from_utf8(counters.stdout).unwrap();
    assert!(counters.contains("counter packets ") && !counters.contains("counter packets 0 "));
    assert!(
        matches!(
            forward.info("app", &access).await,
            Err(crate::Error::NotFound)
        ),
        "explicit retry must release capacity and rediscover the replacement"
    );
    println!(
        "Retained-connection packet loss returned in {:.3}s",
        started.elapsed().as_secs_f64()
    );
    assert!(
        tasks.try_join_next().is_none(),
        "the old connection must remain open without receiving a replay"
    );
    drop(firewall);
    // JoinSet aborts the retained server task on exit, including assertion failure.
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn stalled_connect_releases_capacity_and_discovers_the_replacement() {
    use tokio::net::{TcpSocket, TcpStream};

    // Saturating a local listen queue stalls a TCP handshake without firewall
    // changes, an external address, or a delayed provider/HTTP handler.
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addr().unwrap();
    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), TcpStream::connect(address))
            .await
            .is_err(),
        "the fixture must stall the handshake"
    );
    let provider = Provider::new();
    let _old = advertised(&provider, address).await;
    let forward = Forward::new(&provider.connection("test"), auth(), 1).unwrap();
    let access = crate::auth::Access::Administrator;
    let reads = provider.reads.load(Ordering::SeqCst);
    let pending = forward.info("app", &access);
    tokio::pin!(pending);
    tokio::select! {
        _ = &mut pending => panic!("the old endpoint must stall"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => (),
    }
    assert!(provider.reads.load(Ordering::SeqCst) > reads);
    assert!(matches!(
        forward.info("app", &access).await,
        Err(crate::Error::Busy)
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let _new = advertised(&provider, listener.local_addr().unwrap()).await;
    let _replacement = Server::serve(
        listener,
        Router::new().fallback(|| async {
            // A connected request retains its full response deadline.
            tokio::time::sleep(Duration::from_millis(2100)).await;
            crate::Error::NotFound.into_response()
        }),
    );
    let error = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("connect failure must not consume the whole request deadline")
        .unwrap_err();
    let crate::Error::Storage(error) = error else {
        panic!("expected a connection error");
    };
    let transport = error.downcast_ref::<reqwest::Error>().unwrap();
    assert!(transport.is_connect() && transport.is_timeout());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), forward.info("app", &access))
            .await
            .expect("retry must use the new owner and released capacity"),
        Err(crate::Error::NotFound)
    ));
}
