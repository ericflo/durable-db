//! Live provider qualification through the native HTTP backend. The fixture
//! supplies a logical clock and STOPS the old server before takeover.
use super::*;
use durable_db::{
    api::{self, App, RateLimits},
    auth::Auth,
    transactions::live::{self, Opening},
    upload_budget::UploadLimits,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{sync::oneshot, task::JoinHandle};

struct Server {
    url: String,
    token: String,
    client: reqwest::Client,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(connection: &Connection) -> Self {
        let clock = TestClock::default();
        let mut root = activate(connection, &clock).await;
        select(&mut root).await;
        let owner = Opening::new(
            root,
            live::Limits {
                checkpoint_frames: 2,
                ..Default::default()
            },
            Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap()),
        )
        .unwrap()
        .recover()
        .await
        .unwrap_or_else(|_| panic!("HTTP owner recovery failed"));
        let auth = Auth::new(&"42".repeat(32)).unwrap();
        let token = auth.admin_token();
        let app = App::with_rate_limits(
            Arc::new(api::Native::new(owner, 16).unwrap()),
            auth,
            32,
            RateLimits::new(1000, 1000).unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stop) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, api::router(Arc::new(app)))
                .with_graceful_shutdown(async {
                    let _ = stop.await;
                })
                .await
                .unwrap();
        });
        Self {
            url,
            token,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(40))
                .build()
                .unwrap(),
            shutdown,
            task,
        }
    }
    async fn call(&self, method: &str, path: &str, body: &Value) -> (StatusCode, Value) {
        let response = self
            .client
            .request(method.parse().unwrap(), format!("{}{path}", self.url))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .unwrap_or_else(|_| panic!("native HTTP request failed"));
        let status = response.status();
        (status, response.json().await.expect("native JSON response"))
    }
    async fn ok(&self, method: &str, path: &str, body: &Value) -> Value {
        let (status, value) = self.call(method, path, body).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "native HTTP status; no implicit fixture retries"
        );
        value
    }
    async fn stop(self) {
        drop(self.client);
        self.shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap();
        tokio::task::yield_now().await;
    }
}
fn write(revision: u64) -> Value {
    json!({"request_id":format!("write-{revision}"),"expected_revision":revision,
        "writes":[{"key":"x","value":if revision == 0 {"b25l"} else {"dHdv"}}]})
}

#[tokio::test]
#[ignore = "requires a fresh disposable B2/S3 bucket and a native HTTP report path"]
async fn native_http_budgets_cleanup_and_cold_recovery() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").unwrap();
    let report = std::env::var("DURABLE_DB_NATIVE_HTTP_REPORT").unwrap();
    let namespace = format!("native-http-{}", Uuid::new_v4());
    let started = Instant::now();
    let connection = connect(&provider, &bucket, &namespace)
        .await
        .with_upload_budget(UploadLimits {
            // Three fresh connections each consume a nonrefundable 8 MiB grant.
            bytes: 32 * 1024 * 1024,
            attempts: 256,
        })
        .await
        .unwrap_or_else(|_| panic!("native budget initialization failed"));
    let server = Server::start(&connection).await;
    let budget = "/v1/databases/a/upload-budget";
    let fund = json!({"request_id":"fund","expected_configuration_revision":0,"limits":{"bytes":4096,"attempts":1}});
    for replayed in [false, true] {
        assert_eq!(server.ok("POST", budget, &fund).await["replayed"], replayed);
    }
    for db in ["a", "b"] {
        server
            .ok(
                "PUT",
                &format!("/v1/databases/{db}"),
                &json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":10000}}),
            )
            .await;
    }
    let commit = "/v1/databases/a/commit";
    assert_eq!(server.ok("POST", commit, &write(0)).await["revision"], 1);
    let puts = calls("object_put");
    assert_eq!(server.ok("POST", commit, &write(0)).await["replayed"], true);
    assert_eq!(
        server.call("POST", commit, &write(1)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        calls("object_put"),
        puts,
        "retry/refusal must not send another immutable object"
    );
    assert_eq!(
        server.ok("POST", "/v1/databases/b/commit", &write(0)).await["revision"],
        1
    );
    server
        .ok(
            "POST",
            budget,
            &json!({"request_id":"more","expected_configuration_revision":1,
        "limits":{"bytes":8192,"attempts":2}}),
        )
        .await;
    assert_eq!(server.ok("POST", commit, &write(1)).await["revision"], 2);
    let balance = server.ok("GET", budget, &json!({})).await;
    assert_eq!(balance["allocated"]["attempts"], 2);
    let files = connection.objects();
    let initial = files
        .list("manifests/", None, 128)
        .await
        .expect("list tenant metadata")
        .len();
    assert!(
        initial > 2,
        "fixture must leave obsolete ledgers to collect"
    );
    let journals_before = files.list("journal/", None, 128).await.unwrap().len();
    assert_eq!(journals_before, 5);
    let checkpoint_objects = files.list("tables/", None, 128).await.unwrap().len();
    assert_eq!(
        checkpoint_objects, 6,
        "two checkpoints each have data, index and manifest, with no header object"
    );
    let cleanup = Instant::now();
    let mut remaining = [0; 3];
    tokio::time::timeout(Duration::from_secs(150), async {
        loop {
            for (i, prefix) in ["manifests/", "indexes/", "journal/"].iter().enumerate() {
                remaining[i] = files.list(prefix, None, 128).await.unwrap().len();
            }
            if remaining == [2, 1, 1] {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("automatic native collection timed out: manifests/indexes/journals {remaining:?}")
    });
    let cleanup_ms = cleanup.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(server.ok("GET", budget, &json!({})).await, balance);
    server.stop().await;
    drop((connection, files));

    let recovery = Instant::now();
    let connection = connect(&provider, &bucket, &namespace)
        .await
        .with_upload_budget(UploadLimits::default())
        .await
        .unwrap_or_else(|_| panic!("native reopen failed"));
    let server = Server::start(&connection).await;
    let recovery_ms = recovery.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(server.ok("GET", budget, &json!({})).await, balance);
    assert_eq!(server.ok("POST", commit, &write(1)).await["replayed"], true);
    assert_eq!(
        server.ok("POST", "/v1/databases/b/commit", &write(0)).await["replayed"],
        true
    );
    assert_eq!(
        server.call("POST", commit, &write(2)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let read = json!({"keys":["x"],"scans":[{"prefix":"","limit":10}]});
    let mut readers = [
        ("/v1/databases/a/read", 2, "dHdv"),
        ("/v1/databases/b/read", 1, "b25l"),
    ];
    // Warm both the recovered suffix and checkpoint-backed value paths.
    for (path, _, value) in readers {
        assert_eq!(
            server.ok("POST", path, &read).await["values"],
            json!([value])
        );
    }
    let before = controls();
    let gets = calls("object_get");
    let mut elapsed = Vec::new();
    for i in 0..100 {
        let (path, revision, value) = readers[i % readers.len()];
        let start = Instant::now();
        let response = server.ok("POST", path, &read).await;
        assert_eq!(response["revision"], revision);
        assert_eq!(response["values"], json!([value]));
        assert_eq!(
            response["scans"][0]["items"],
            json!([{"key":"x","value":value}])
        );
        elapsed.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    assert_eq!(
        before,
        controls(),
        "warm reads do not mutate or fetch authority"
    );
    assert_eq!(
        gets,
        calls("object_get"),
        "warm reads use the bounded cache and suffix"
    );
    elapsed.sort_by(f64::total_cmp);

    // A new checkpoint covers both the prior epoch and this owner's frame.
    assert_eq!(
        server.ok("POST", "/v1/databases/b/commit", &write(1)).await["revision"],
        2
    );
    readers[1] = ("/v1/databases/b/read", 2, "dHdv");
    // Wait for both metadata loops and all covered journals after takeover.
    // The catalog shares prefixes with budgets and must survive this sweep.
    let files = connection.objects();
    let markers = ["manifests", "indexes"]
        .map(|prefix| format!("{prefix}/zz-collection-check.{}", Uuid::new_v4()));
    for marker in &markers {
        files.put(marker, Bytes::new()).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(150), async {
        loop {
            if files.get(&markers[0], 1).await.unwrap().is_none()
                && files.get(&markers[1], 1).await.unwrap().is_none()
                && files.list("journal/", None, 128).await.unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("replacement collection made no progress");
    server.stop().await;
    drop((connection, files));
    let connection = connect(&provider, &bucket, &namespace)
        .await
        .with_upload_budget(UploadLimits::default())
        .await
        .unwrap();
    let server = Server::start(&connection).await;
    let namespace_budget = connection.objects().upload_budget().await.unwrap().unwrap();
    assert_eq!(namespace_budget.limits.bytes, 32 * 1024 * 1024);
    assert_eq!(namespace_budget.allocated.bytes, 24 * 1024 * 1024);
    assert_eq!(server.ok("GET", budget, &json!({})).await, balance);
    for (path, revision, value) in readers {
        let response = server.ok("POST", path, &read).await;
        assert_eq!(response["revision"], revision);
        assert_eq!(response["values"], json!([value]));
    }
    assert_eq!(server.ok("POST", commit, &write(1)).await["replayed"], true);
    assert_eq!(
        server.ok("POST", "/v1/databases/b/commit", &write(0)).await["replayed"],
        true
    );
    assert_eq!(
        server.ok("POST", "/v1/databases/b/commit", &write(1)).await["replayed"],
        true
    );
    server.stop().await;
    let evidence = json!({"provider":provider,"namespace":namespace,
        "tenant_budget":balance,"namespace_budget":namespace_budget,"exhausted_tenant_did_not_stall_neighbor":true,
        "refusal_and_retry_immutable_puts":0,"automatic_collection":true,
        "ledger_objects_before":initial,"ledger_objects_after":2,"directory_objects_after":1,
        "cleanup_wait_ms":cleanup_ms,"cold_recovery_ms":recovery_ms,
        "cold_receipts_and_values_verified":true,"warm_http_reads":100,"warm_read_control_calls":0,"warm_read_object_gets":0,"warm_read_paths":["checkpoint","recovered_suffix"],
        "catalog_survived_collection_after_takeover":true,"complete_reopens":2,
        "inline_checkpoint_descriptors":true,"checkpoint_objects_before_first_sweep":checkpoint_objects,
        "journal_objects_before":journals_before,"journal_objects_after_first_sweep":1,
        "journal_objects_after_takeover_sweep":0,"closed_journal_epochs_reclaimed":true,
        "warm_read_ms":{"p50":elapsed[49],"p95":elapsed[94],"p99":elapsed[98]},
        "total_seconds":started.elapsed().as_secs_f64(),
        "scope":"Debug native HTTP composition on local loopback; 4 MiB cache; logical clock; old server stopped before takeover. Not physical timing, AWS or performance-SLO qualification."});
    std::fs::write(report, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
