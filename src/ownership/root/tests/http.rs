use super::*;
use crate::{
    api::{self, App, Native, RateLimits},
    auth::Auth,
    cache::Cache,
    storage::{Object, Storage, Version},
    transactions::live::{self, Opening},
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tower::ServiceExt;

#[derive(Default)]
struct Faults {
    pause: AtomicBool,
    pause_read: AtomicBool,
    fail_checkpoint: AtomicBool,
    entered: Notify,
    release: Notify,
    puts: AtomicU64,
    range_reads: AtomicU64,
}
struct Files {
    inner: Arc<Remote>,
    faults: Arc<Faults>,
}
#[async_trait]
impl Storage for Files {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.inner.get(key, max).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        size: u64,
    ) -> anyhow::Result<Bytes> {
        self.faults.range_reads.fetch_add(1, Ordering::SeqCst);
        if self.faults.pause_read.swap(false, Ordering::SeqCst) {
            self.faults.entered.notify_one();
            self.faults.release.notified().await;
        }
        self.inner.get_range(key, range, size).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        panic!("native data path cannot use object CAS")
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.faults.puts.fetch_add(1, Ordering::SeqCst);
        if key.starts_with("tables/") && self.faults.fail_checkpoint.load(Ordering::SeqCst) {
            anyhow::bail!("checkpoint uploads unavailable");
        }
        if self.faults.pause.swap(false, Ordering::SeqCst) {
            self.faults.entered.notify_one();
            self.faults.release.notified().await;
            self.inner.put(key, bytes).await?;
            anyhow::bail!("applied PUT lost its reply");
        }
        self.inner.put(key, bytes).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after, limit).await
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        panic!("serving never deletes physical history")
    }
}
type Fixture = (
    axum::Router,
    Arc<Provider>,
    ManualClock,
    Arc<Faults>,
    String,
);
async fn fixture(capacity: usize, checkpoints: u64) -> Fixture {
    fixture_budget(
        capacity,
        checkpoints,
        crate::upload_budget::UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 10_000,
        },
    )
    .await
}
async fn fixture_budget(
    capacity: usize,
    checkpoints: u64,
    limits: crate::upload_budget::UploadLimits,
) -> Fixture {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let faults = Arc::new(Faults::default());
    let files = Arc::new(Files {
        inner: provider.files.clone(),
        faults: faults.clone(),
    });
    let connection = Connection::new(files, provider.clone(), "test")
        .with_upload_budget(limits)
        .await
        .unwrap();
    // The first credit grant changes the captured comparison. Its registration
    // remains charged; a fresh incarnation uses the already confirmed credit.
    let mut root = connection.root(clock.clone(), profile()).unwrap();
    let observed = root.observe().await.unwrap();
    let claim = root.claim(observed).unwrap();
    assert!(matches!(
        root.finish(claim.execute().await).unwrap(),
        Outcome::Conflict
    ));
    let mut root = connection.root(clock.clone(), profile()).unwrap();
    super::claim(&mut root).await;
    clock.set(23);
    validate(&mut root).await;
    activate(&mut root).await;
    let owner = Opening::new(
        root,
        live::Limits {
            checkpoint_frames: checkpoints,
            receipt_window: 128,
            ..Default::default()
        },
        Arc::new(Cache::new(0, None).unwrap()),
    )
    .unwrap()
    .recover()
    .await
    .unwrap();
    let auth = Auth::new(&"31".repeat(32)).unwrap();
    let token = auth.admin_token();
    // Keep contract tests independent of the production default refill delay.
    // Dedicated middleware tests cover the low-rate and eviction boundaries.
    let app = App::with_rate_limits(
        Arc::new(Native::new(owner, capacity).unwrap()),
        auth,
        64,
        RateLimits::new(20_000, 20_000).unwrap(),
    );
    (api::router(Arc::new(app)), provider, clock, faults, token)
}
fn request(method: &str, path: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}
async fn call(
    router: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        router.clone().oneshot(request(method, path, token, body)),
    )
    .await
    .unwrap()
    .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
fn commit() -> Value {
    json!({"expected_revision":0,"request_id":"write","writes":[{"key":"x","value":"eA=="}]})
}
async fn create(router: &axum::Router, token: &str, db: &str) {
    assert_eq!(
        call(
            router,
            "PUT",
            &format!("/v1/databases/{db}"),
            token,
            json!({"request_id":"create","limits":{"max_keys":1000,"max_bytes":1048576}})
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn native_http_serves_auth_lifecycle_quotas_and_checkpoint_reads_over_tcp() {
    let (router, provider, clock, _, token) = fixture(8, 2).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let url = format!("{base}/v1/databases/app");
    let creation = json!({"request_id":"create","limits":{"max_keys":1,"max_bytes":1048576}});
    let response = client
        .put(&url)
        .bearer_auth(&token)
        .json(&creation)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let first: Value = response.json().await.unwrap();
    let retry: Value = client
        .put(&url)
        .bearer_auth(&token)
        .json(&creation)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retry["info"], first["info"]);
    assert_eq!(retry["replayed"], true);
    let mut credentials = Vec::new();
    for write in [false, true] {
        let result: Value = client
            .post(format!("{base}/v1/tokens"))
            .bearer_auth(&token)
            .json(&json!({"database":"app","write":write,"ttl_seconds":3600}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        credentials.push(result);
    }
    let reader = credentials[0]["token"].as_str().unwrap();
    let writer = credentials[1]["token"].as_str().unwrap();
    assert_eq!(
        client
            .post(format!("{url}/commit"))
            .bearer_auth(reader)
            .json(&commit())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let written: Value = client
        .post(format!("{url}/commit"))
        .bearer_auth(writer)
        .json(&commit())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(written["revision"], 1);
    // A creation plus commit checkpoint is now durable; reads use range I/O
    // with zero cache and do not mutate the ownership register.
    let controls = provider.calls.load(Ordering::SeqCst);
    for _ in 0..2 {
        let read: Value = client
            .post(format!("{url}/read"))
            .bearer_auth(reader)
            .json(&json!({"keys":["x"],"scans":[{"prefix":"","reverse":true,"limit":1}]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(read["values"][0], "eA==");
        assert_eq!(read["revision"], 1);
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), controls);
    let info: Value = client
        .get(&url)
        .bearer_auth(reader)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (info["keys"].as_u64(), info["bytes"].as_u64()),
        (Some(1), Some(2))
    );
    assert!(info.get("pages").is_none());
    let mut over = commit();
    over["expected_revision"] = json!(1);
    over["request_id"] = json!("over");
    over["writes"][0]["key"] = json!("y");
    assert_eq!(
        client
            .post(format!("{url}/commit"))
            .bearer_auth(writer)
            .json(&over)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/databases/other"))
            .bearer_auth(writer)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let revoked = client.post(format!("{url}/access")).bearer_auth(&token).json(&json!({
        "request_id":"revoke","expected_policy_revision":1,"change":{"action":"revoke","token_id":credentials[1]["token_id"]}
    })).send().await.unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    assert_eq!(
        client
            .post(format!("{url}/commit"))
            .bearer_auth(writer)
            .json(&commit())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!("{url}/gc"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    let budget_url = format!("{base}/v1/storage/upload-budget");
    assert_eq!(
        client
            .get(&budget_url)
            .bearer_auth(reader)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let status: crate::upload_budget::UploadStatus = client
        .get(&budget_url)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status.allocated.attempts >= 2);
    assert_eq!(status.limits.attempts, 10_000);
    let funding = json!({"request_id":"fund-native", "expected_configuration_revision":status.configuration_revision,
        "limits":{"bytes":status.limits.bytes + 1, "attempts":status.limits.attempts}});
    for replayed in [false, true] {
        let result: crate::upload_budget::UploadResult = client
            .post(&budget_url)
            .bearer_auth(&token)
            .json(&funding)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(result.replayed, replayed);
        assert_eq!(result.status.allocated, status.allocated);
    }
    clock.set(100);
    assert_eq!(
        client
            .get(format!("{base}/health/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(reader)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        client
            .get(format!("{base}/health/live"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn native_http_disconnect_keeps_accepted_work_bounded_and_reads_run_during_uploads() {
    let (router, provider, _, faults, token) = fixture(2, 5).await;
    create(&router, &token, "a").await;
    create(&router, &token, "b").await;
    // Allocate local tenant grants before pausing a journal PUT. Checkpoint
    // rebasing must preserve these grants, keeping warm admission control-free.
    for db in ["a", "b"] {
        assert_eq!(
            call(
                &router,
                "POST",
                &format!("/v1/databases/{db}/commit"),
                &token,
                json!({"request_id":"prime","expected_revision":0,"writes":[{"key":"prime","value":"cA=="}]})
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    create(&router, &token, "checkpoint").await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider.record().checkpoint.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut write = commit();
    write["expected_revision"] = json!(1);
    let calls = provider.calls.load(Ordering::SeqCst);
    let puts = faults.puts.load(Ordering::SeqCst);
    faults.pause.store(true, Ordering::SeqCst);
    let a = tokio::spawn(router.clone().oneshot(request(
        "POST",
        "/v1/databases/a/commit",
        &token,
        write.clone(),
    )));
    tokio::time::timeout(Duration::from_secs(2), faults.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/a/read",
            &token,
            json!({"keys":["x"]})
        )
        .await
        .1["revision"],
        1
    );
    let b = tokio::spawn(router.clone().oneshot(request(
        "POST",
        "/v1/databases/b/commit",
        &token,
        write.clone(),
    )));
    tokio::time::timeout(Duration::from_secs(2), async {
        while faults.puts.load(Ordering::SeqCst) != puts + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Both pending tickets hold capacity, including an identical retry. The
    // second upload can finish but cannot expose a value across the first gap.
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/a/commit",
            &token,
            write.clone()
        )
        .await
        .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/b/read",
            &token,
            json!({"keys":["x"]})
        )
        .await
        .1["revision"],
        1
    );
    let ranges = faults.range_reads.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        faults.range_reads.load(Ordering::SeqCst),
        ranges,
        "pending reply polling must not reread cold catalog tables"
    );
    a.abort();
    let _ = a.await;
    faults.release.notify_one();
    let reply = tokio::time::timeout(Duration::from_secs(2), b)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reply.status(), StatusCode::OK);
    let retried = call(
        &router,
        "POST",
        "/v1/databases/a/commit",
        &token,
        write.clone(),
    )
    .await;
    assert_eq!(retried.0, StatusCode::OK);
    assert_eq!(retried.1["replayed"], true);
    assert_eq!(retried.1["revision"], 2);
    assert_eq!(
        faults.puts.load(Ordering::SeqCst),
        puts + 2,
        "lost replies never repeat PUT"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "ordinary reads and writes never renew control"
    );
}

#[tokio::test]
async fn native_http_retries_idle_checkpoint_and_survives_disconnect_during_cold_admission() {
    let (router, provider, _, faults, token) = fixture(2, 2).await;
    create(&router, &token, "app").await;
    faults.fail_checkpoint.store(true, Ordering::SeqCst);
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/app/commit",
            &token,
            commit()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(provider.record().checkpoint.is_none());
    // No next request is needed to recover maintenance after the provider heals.
    faults.fail_checkpoint.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider.record().checkpoint.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // No byte cache: pause the worker's cold lookup, before admission finishes.
    // Cancelling the HTTP handler must not cancel that exclusive operation.
    faults.pause_read.store(true, Ordering::SeqCst);
    let mut next = commit();
    next["expected_revision"] = json!(1);
    next["request_id"] = json!("next");
    let attempt = tokio::spawn(router.clone().oneshot(request(
        "POST",
        "/v1/databases/app/commit",
        &token,
        next.clone(),
    )));
    tokio::time::timeout(Duration::from_secs(2), faults.entered.notified())
        .await
        .unwrap();
    attempt.abort();
    let _ = attempt.await;
    faults.release.notify_one();
    // The identical request shares the accepted result even if it reaches the
    // queue before the original upload finishes.
    let retry = call(&router, "POST", "/v1/databases/app/commit", &token, next).await;
    assert_eq!(retry.0, StatusCode::OK);
    assert_eq!(retry.1["replayed"], true);
    assert_eq!(retry.1["revision"], 2);
}

#[tokio::test]
async fn native_http_expired_owner_cannot_acknowledge_a_pending_upload() {
    let (router, _, clock, faults, token) = fixture(2, 256).await;
    create(&router, &token, "app").await;
    faults.pause.store(true, Ordering::SeqCst);
    let attempt = tokio::spawn(router.clone().oneshot(request(
        "POST",
        "/v1/databases/app/commit",
        &token,
        commit(),
    )));
    tokio::time::timeout(Duration::from_secs(2), faults.entered.notified())
        .await
        .unwrap();
    clock.set(100);
    faults.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(2), attempt)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        call(&router, "GET", "/health/ready", &token, Value::Null)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/app/read",
            &token,
            json!({"keys":["x"]})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
#[ignore = "requires built clients/javascript and installed Node dependencies; run by Woodpecker client checks"]
async fn native_http_javascript_contract() {
    use std::io::Write;
    let (router, _, _, _, token) = fixture(32, 64).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut credential = tempfile::NamedTempFile::new().unwrap();
    writeln!(credential, "{token}").unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(90),
        tokio::process::Command::new("node")
            .current_dir("clients/javascript")
            .args([
                "node_modules/vitest/vitest.mjs",
                "run",
                "--pool=threads",
                "--maxWorkers=1",
                "--no-file-parallelism",
                "--testTimeout=30000",
            ])
            .env("DURABLE_DB_TEST_URL", url)
            .env("DURABLE_DB_TEST_TOKEN_FILE", credential.path())
            .kill_on_drop(true)
            .output(),
    )
    .await;
    server.abort();
    let _ = server.await;
    let output = output
        .expect("native client suite deadline")
        .expect("run Node client suite");
    assert!(
        output.status.success(),
        "native client contract failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

#[tokio::test(start_paused = true)]
async fn native_http_stops_renewal_when_the_serving_worker_retires() {
    use crate::api::Backend;
    let provider = Provider::new();
    let clock = ManualClock::default();
    let owner = super::live::open(active(&provider, &clock).await, live::Limits::default()).await;
    let before = provider.calls.load(Ordering::SeqCst);
    let backend = Native::renewing(
        owner,
        2,
        Duration::from_millis(10),
        Duration::from_millis(5),
    )
    .unwrap();
    tokio::task::yield_now().await;
    assert!(provider.calls.load(Ordering::SeqCst) > before);
    assert!(backend.ready().await);
    clock.failed.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;
    assert!(!backend.ready().await);
    let reads = provider.reads.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        reads,
        "a retired serving worker must not keep its renewal task running"
    );
}

#[tokio::test]
async fn native_http_refill_resumes_unsent_journal_without_replacing_the_owner() {
    let (router, _, _, faults, token) = fixture_budget(
        8,
        64,
        crate::upload_budget::UploadLimits {
            bytes: 8192,
            attempts: 4,
        },
    )
    .await;
    let create = request(
        "PUT",
        "/v1/databases/app",
        &token,
        json!({"request_id":"create", "limits":{"max_keys":10,"max_bytes":1000}}),
    );
    let service = router.clone();
    let waiting = tokio::spawn(async move { service.oneshot(create).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    assert_eq!(
        faults.puts.load(Ordering::SeqCst),
        4,
        "only registrations and database ledger initialization reached storage"
    );
    let (status, body) = call(
        &router,
        "POST",
        "/v1/storage/upload-budget",
        &token,
        json!({"request_id":"fund", "expected_configuration_revision":0,
            "limits":{"bytes":16384,"attempts":10}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let response = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        faults.puts.load(Ordering::SeqCst),
        5,
        "the admitted journal frame is uploaded once"
    );
    let (status, _) = call(&router, "GET", "/v1/databases/app", &token, json!({})).await;
    assert_eq!(status, StatusCode::OK);
}

// Keep transport assertions compact while exercising the real TCP stack.
async fn tcp_call(
    client: &reqwest::Client,
    method: &str,
    url: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = client
        .request(method.parse().unwrap(), url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

#[tokio::test]
async fn native_http_database_budget_refuses_before_journal_and_preserves_neighbors() {
    let (router, _, _, faults, token) = fixture(8, 2).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let budget = format!("{base}/v1/databases/a/upload-budget");
    let fund = json!({"request_id":"fund","expected_configuration_revision":0,"limits":{"bytes":4096,"attempts":1}});
    for replayed in [false, true] {
        let (status, result) = tcp_call(&client, "POST", &budget, &token, fund.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["replayed"], replayed);
    }
    for name in ["a", "b"] {
        let (status, created) = tcp_call(
            &client,
            "PUT",
            &format!("{base}/v1/databases/{name}"),
            &token,
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(created["info"]["revision"], 0);
    }
    let (_, credentials) = tcp_call(
        &client,
        "POST",
        &format!("{base}/v1/tokens"),
        &token,
        json!({"database":"a","write":true,"ttl_seconds":3600}),
    )
    .await;
    let writer = credentials["token"].as_str().unwrap();
    for method in ["GET", "POST"] {
        assert_eq!(
            tcp_call(&client, method, &budget, writer, fund.clone())
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }
    let a = format!("{base}/v1/databases/a/commit");
    assert_eq!(
        tcp_call(&client, "POST", &a, writer, commit()).await.0,
        StatusCode::OK
    );
    let (status, before) = tcp_call(&client, "GET", &budget, &token, json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        before["limits"]["attempts"], 1,
        "creation preserved the administrative ceiling"
    );
    let puts = faults.puts.load(Ordering::SeqCst);
    let (status, retry) = tcp_call(&client, "POST", &a, writer, commit()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retry["replayed"], true);
    assert_eq!(faults.puts.load(Ordering::SeqCst), puts);
    let mut next = commit();
    next["request_id"] = json!("next");
    next["expected_revision"] = json!(1);
    assert_eq!(
        tcp_call(&client, "POST", &a, writer, next.clone()).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        faults.puts.load(Ordering::SeqCst),
        puts,
        "refusal must not upload a journal frame"
    );
    assert_eq!(
        tcp_call(
            &client,
            "POST",
            &format!("{base}/v1/databases/b/commit"),
            &token,
            commit()
        )
        .await
        .0,
        StatusCode::OK,
        "an exhausted neighbor must not leave a shared journal gap"
    );
    let (_, after) = tcp_call(&client, "GET", &budget, &token, json!({})).await;
    assert_eq!(
        before, after,
        "checkpointing and neighboring admission preserve the ledger"
    );
    let refill = json!({"request_id":"more","expected_configuration_revision":1,"limits":{"bytes":8192,"attempts":2}});
    assert_eq!(
        tcp_call(&client, "POST", &budget, &token, refill).await.0,
        StatusCode::OK
    );
    // Ledger/status comparisons invalidate checkpoint observations. Maintenance
    // revalidates and retries once without imposing a routine 503 on admission.
    assert_eq!(
        tcp_call(&client, "POST", &a, writer, next).await.0,
        StatusCode::OK
    );
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn native_http_automatically_collects_only_obsolete_budget_metadata() {
    let (router, provider, _, _, token) = fixture(8, 64).await;
    create(&router, &token, "app").await;
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/app/commit",
            &token,
            commit()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        provider
            .files
            .list("manifests/", None, 128)
            .await
            .unwrap()
            .len()
            > 1
    );
    let journals = provider.files.list("journal/", None, 128).await.unwrap();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider
            .files
            .list("manifests/", None, 128)
            .await
            .unwrap()
            .len()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        provider
            .files
            .list("indexes/", None, 128)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        provider.files.list("journal/", None, 128).await.unwrap(),
        journals
    );
    let (status, budget) = call(
        &router,
        "GET",
        "/v1/databases/app/upload-budget",
        &token,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(budget["allocated"]["attempts"], 32);
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/databases/app/read",
            &token,
            json!({"keys":["x"]})
        )
        .await
        .1["values"][0],
        "eA=="
    );
}
