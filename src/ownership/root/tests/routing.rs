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
async fn delayed_failure_does_not_clear_a_refreshed_route() {
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
                        crate::Error::Storage(anyhow::anyhow!("late failure")).into_response()
                    }
                    1 => {
                        crate::Error::Storage(anyhow::anyhow!("immediate failure")).into_response()
                    }
                    4 => (StatusCode::OK, vec![b'x'; 1024 * 1024 + 1]).into_response(),
                    _ => crate::Error::NotFound.into_response(),
                }
            }
        }),
    );
    let forward = Arc::new(Forward::new(&provider.connection("test"), auth(), 4).unwrap());
    let first = forward.clone();
    let pending =
        tokio::spawn(async move { first.info("app", &crate::auth::Access::Administrator).await });
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
    resume.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(crate::Error::Storage(_))
    ));
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
