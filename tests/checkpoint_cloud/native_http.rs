//! Live provider qualification through the native HTTP backend. The fixture
//! supplies a logical clock and STOPS the old server before takeover.
use super::*;
use durable_db::{
    api::{self, App, RateLimits},
    auth::Auth,
    backup,
    transactions::live::{self, Opening},
    upload_budget::UploadLimits,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::{sync::oneshot, task::JoinHandle};

struct Server {
    control: live::Control<TestClock>,
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
        let control = owner.control();
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
            control,
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
        if status != StatusCode::OK {
            let metrics = REGISTRY.render();
            let failures: Vec<_> = metrics
                .lines()
                .filter(|line| {
                    line.starts_with("durable_db_operations_total{")
                        && !line.ends_with(" 0")
                        && !line.contains("outcome=\"ok\"")
                })
                .collect();
            panic!("native {method} {path}: {status}; no implicit retries; {failures:?}");
        }
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

// Mark metadata and table prefixes after setup: slow provider calls may let the
// ordinary 60-second worker run before the fixture finishes writing its data.
async fn collected(connection: &Connection, expected: [usize; 3]) -> (f64, usize) {
    let files = connection.objects();
    let markers = [
        format!("manifests/zz-collection-check.{}", Uuid::new_v4()),
        format!("indexes/zz-collection-check.{}", Uuid::new_v4()),
        format!("tables/{}", Uuid::new_v4().simple()),
    ];
    for marker in &markers {
        files.put(marker, Bytes::new()).await.unwrap();
    }
    let start = Instant::now();
    let mut remaining = [0; 4];
    tokio::time::timeout(Duration::from_secs(150), async {
        loop {
            for (i, prefix) in ["manifests/", "indexes/", "journal/", "tables/"]
                .iter()
                .enumerate()
            {
                remaining[i] = files.list(prefix, None, 128).await.unwrap().len();
            }
            // This small fixture has one manifest node, but table reuse may
            // preserve more than one data/index pair. Read the published count
            // instead of hard-coding the writer's current packing decisions.
            let authority = connection.authority().read().await.unwrap();
            let owner: Value =
                serde_json::from_str(&authority.info["durable-db-owned-root"]).unwrap();
            let checkpoint: Value =
                serde_json::from_str(owner["checkpoint"].as_str().unwrap()).unwrap();
            assert_eq!(checkpoint["height"], 0);
            let tables = checkpoint["tables"].as_u64().unwrap() as usize;
            assert!(tables > 0);
            let table_count = if authority
                .info
                .contains_key("durable-db-retained-checkpoint")
            {
                remaining[3] > 2 * tables
            } else {
                remaining[3] == 2 * tables + 1
            };
            if remaining[..3] == expected
                && table_count
                && files.get(&markers[0], 1).await.unwrap().is_none()
                && files.get(&markers[1], 1).await.unwrap().is_none()
                && files.get(&markers[2], 1).await.unwrap().is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("automatic collection timed out: manifests/indexes/journals/tables {remaining:?}")
    });
    (start.elapsed().as_secs_f64() * 1000.0, remaining[3])
}

async fn checkpoint_copy_allowances(
    files: &dyn durable_db::storage::Storage,
) -> std::collections::BTreeMap<String, u64> {
    let mut data = std::collections::BTreeMap::new();
    for key in files.list("tables/", None, 128).await.unwrap() {
        let object = files.get(&key, 1024 * 1024).await.unwrap().unwrap();
        if object.bytes.starts_with(b"DDBBLK01")
            || object.bytes.starts_with(b"DDBIDX01")
            || object.bytes.starts_with(b"DDBCPN01")
        {
            data.insert(
                key,
                if object.bytes.starts_with(b"DDBBLK01") {
                    let bytes = object.bytes.len() as u64;
                    5 * bytes
                        + 38_280
                        + (bytes * 38_280).div_ceil(4_126_706)
                        + (bytes / 33 * 38_280).div_ceil(4096)
                } else if object.bytes.starts_with(b"DDBIDX01") {
                    0
                } else {
                    object.bytes.len() as u64
                },
            );
        }
    }
    data
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
    let fund = json!({"request_id":"fund","expected_configuration_revision":0,"limits":{"bytes":833,"attempts":2}});
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
    let initial_tables = checkpoint_copy_allowances(connection.objects().as_ref()).await;
    assert_eq!(initial_tables.len(), 3, "one data/index pair and manifest");
    let initial_copies = initial_tables.values().sum::<u64>();
    let first_admission = 2 * (6285 + initial_copies);
    // 254 journal + 580 data/index + 5,451 manifest growth/split allowance bytes.
    // Admission must precede every journal PUT.
    let puts = calls("object_put");
    assert_eq!(
        server.call("POST", commit, &write(0)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(calls("object_put"), puts);
    let refused = server.ok("GET", budget, &json!({})).await;
    assert_eq!(refused["allocated"]["bytes"], 0);
    server
        .ok(
            "POST",
            budget,
            &json!({"request_id":"rows","expected_configuration_revision":1,
        "limits":{"bytes":first_admission,"attempts":2}}),
        )
        .await;
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
    let current_tables = checkpoint_copy_allowances(connection.objects().as_ref()).await;
    let replaced: Vec<_> = current_tables
        .iter()
        .filter(|(key, _)| !initial_tables.contains_key(*key))
        .collect();
    assert_eq!(replaced.len(), 3, "one data/index pair and manifest");
    let copies = replaced.iter().map(|(_, size)| **size).sum::<u64>();
    let second_admission = 2 * (6285 + copies);
    server
        .ok(
            "POST",
            budget,
            &json!({"request_id":"more","expected_configuration_revision":2,
        "limits":{"bytes":first_admission + 2 * 6285,"attempts":2}}),
        )
        .await;
    let puts = calls("object_put");
    assert_eq!(
        server.call("POST", commit, &write(1)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(calls("object_put"), puts);
    assert_eq!(
        server.ok("GET", budget, &json!({})).await["allocated"]["bytes"],
        first_admission
    );
    server
        .ok(
            "POST",
            budget,
            &json!({"request_id":"copies","expected_configuration_revision":3,
        "limits":{"bytes":first_admission + second_admission,"attempts":2}}),
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
    assert!(initial >= 2, "both live tenant ledgers must remain");
    let journals_before = files.list("journal/", None, 128).await.unwrap().len();
    assert!(
        (1..=5).contains(&journals_before),
        "setup may overlap automatic collection, but the suffix must remain"
    );
    let checkpoint_objects = files.list("tables/", None, 128).await.unwrap().len();
    assert!(
        checkpoint_objects >= 3,
        "the current checkpoint retains data, an index and a manifest"
    );
    let setup_seconds = started.elapsed().as_secs_f64();
    let puts = calls("object_put");
    let (retained, outcome) = server.control.retain_checkpoint().await.unwrap();
    assert!(matches!(outcome, Outcome::Confirmed));
    assert_eq!(calls("object_put"), puts, "retention uploads no objects");
    let (cleanup_ms, retained_tables) = collected(&connection, [2, 1, 1]).await;
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
    assert!(server.control.retained_checkpoint().await.unwrap() == Some(retained));
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
    let (_, protected_tables) = collected(&connection, [3, 2, 0]).await;
    let recovered_balance = server.ok("GET", budget, &json!({})).await;
    assert_eq!(recovered_balance["allocated"], balance["allocated"]);
    assert_eq!(
        recovered_balance["maintenance"]["funded_bytes"],
        balance["maintenance"]["funded_bytes"]
    );
    assert_eq!(
        recovered_balance["maintenance"]["allocated"]["attempts"],
        balance["maintenance"]["allocated"]["attempts"]
            .as_u64()
            .unwrap()
            + 1
    );
    assert!(
        recovered_balance["maintenance"]["allocated"]["bytes"]
            .as_u64()
            .unwrap()
            > balance["maintenance"]["allocated"]["bytes"]
                .as_u64()
                .unwrap()
    );
    let retained_image = Snapshot::new(
        connection.objects(),
        Arc::new(Cache::new(0, None).unwrap()),
        retained.reference(),
    );
    retained_image.verify(Limits::default()).await.unwrap();
    let archives = tempfile::tempdir().unwrap();
    let export_puts = calls("object_put");
    for name in ["a", "b"] {
        let info = server
            .ok("GET", &format!("/v1/databases/{name}"), &json!({}))
            .await;
        let database = Database {
            name,
            generation: Uuid::parse_str(info["policy"]["incarnation"].as_str().unwrap()).unwrap(),
        };
        let old = retained_image.database(database).await.unwrap().unwrap();
        assert_eq!(old.revision(), 1);
        assert_eq!(
            old.get("x").await.unwrap().unwrap().value.as_deref(),
            Some(b"one".as_slice())
        );
        let exported = backup::export_checkpoint(
            &retained_image,
            name,
            &archives.path().join(format!("{name}.jsonl")),
        )
        .await
        .unwrap();
        assert_eq!(exported.snapshot.revision, 1);
        assert_eq!(exported.snapshot.keys, 1);
        assert_eq!(exported.snapshot.bytes, 4);
        assert_eq!(
            exported.snapshot.incarnation.as_deref(),
            info["policy"]["incarnation"].as_str()
        );
    }
    assert_eq!(
        calls("object_put"),
        export_puts,
        "export makes no immutable PUT"
    );
    assert!(matches!(
        server
            .control
            .release_checkpoint(retained.id())
            .await
            .unwrap(),
        Outcome::Confirmed
    ));
    let (_, takeover_tables) = collected(&connection, [3, 2, 0]).await;
    assert!(
        protected_tables > takeover_tables,
        "released checkpoint objects are reclaimed"
    );
    assert!(
        Snapshot::new(
            connection.objects(),
            Arc::new(Cache::new(0, None).unwrap()),
            retained.reference()
        )
        .verify(Limits::default())
        .await
        .is_err()
    );
    server.stop().await;
    drop((connection, files));
    let connection = connect(&provider, &bucket, &namespace)
        .await
        .with_upload_budget(UploadLimits::default())
        .await
        .unwrap();
    let server = Server::start(&connection).await;
    assert!(
        server
            .control
            .retained_checkpoint()
            .await
            .unwrap()
            .is_none()
    );
    let namespace_budget = connection.objects().upload_budget().await.unwrap().unwrap();
    assert_eq!(namespace_budget.limits.bytes, 32 * 1024 * 1024);
    assert_eq!(namespace_budget.allocated.bytes, 24 * 1024 * 1024);
    assert_eq!(
        server.ok("GET", budget, &json!({})).await,
        recovered_balance
    );
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
    // The retained source has been collected and all cloud servers stopped.
    // The unchanged restore path accepts its archives into an independent laptop directory.
    let local = api::open_local(
        archives.path().join("restored"),
        Arc::new(Cache::new(0, None).unwrap()),
        8,
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let auth = Auth::new(&"47".repeat(32)).unwrap();
    let token_file = archives.path().join("admin-token");
    std::fs::write(&token_file, auth.admin_token()).unwrap();
    let app = api::router(Arc::new(App::with_rate_limits(
        Arc::new(local),
        auth,
        16,
        RateLimits::new(1000, 1000).unwrap(),
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for name in ["a", "b"] {
        let path = archives.path().join(format!("{name}.jsonl"));
        for _ in 0..2 {
            let restored = backup::restore(&url, &token_file, name, &path)
                .await
                .unwrap();
            assert_eq!(restored.source_revision, 1);
            assert_eq!(restored.verified_keys, 1);
            assert_eq!(restored.destination.bytes, 4);
        }
    }
    task.abort();
    let _ = task.await;
    let mut evidence = json!({"provider":provider,"namespace":namespace,
        "tenant_budget":recovered_balance,"namespace_budget":namespace_budget,"exhausted_tenant_did_not_stall_neighbor":true,
        "journal_only_funding_refused":true,"journal_bytes_per_write":254,"checkpoint_row_bytes_per_write":174,
        "admission_bytes_per_write":[first_admission,second_admission],"base_table_and_manifest_allowance_per_write":[initial_copies,copies],
        "new_rows_only_funding_refused":true,"refusal_and_retry_immutable_puts":0,"automatic_collection":true,
        "ledger_objects_before":initial,"ledger_objects_after":2,"directory_objects_after":1,
        "setup_seconds":setup_seconds,"cleanup_wait_ms":cleanup_ms,"cold_recovery_ms":recovery_ms,
        "cold_receipts_and_values_verified":true,"warm_http_reads":100,"warm_read_control_calls":0,"warm_read_object_gets":0,"warm_read_paths":["checkpoint","recovered_suffix"],
        "catalog_survived_collection_after_takeover":true,"complete_reopens":2,
        "retention":{"cold_reopen_and_collection":true,"exact_cut_values":true,"release_reclaimed_old_checkpoint":true,"immutable_puts":0},
        "automatic_table_and_manifest_collection":true,"checkpoint_objects_after_first_sweep":retained_tables,"checkpoint_objects_after_takeover_sweep":takeover_tables,
        "inline_checkpoint_descriptors":true,"checkpoint_objects_before_first_sweep":checkpoint_objects,
        "journal_objects_before":journals_before,"journal_objects_after_first_sweep":1,
        "journal_objects_after_takeover_sweep":0,"closed_journal_epochs_reclaimed":true,
        "warm_read_ms":{"p50":elapsed[49],"p95":elapsed[94],"p99":elapsed[98]},
        "total_seconds":started.elapsed().as_secs_f64(),
        "scope":"Debug native HTTP composition on local loopback; 4 MiB cache; logical clock; old server stopped before takeover. Not physical timing, AWS or performance-SLO qualification."});
    evidence["maintenance_funding"] = json!({"cold_recovery_claimed_preserved_reserve":true,
        "no_new_admission_allocation_for_recovery":true,"before_recovery":balance["maintenance"],
        "after_recovery":recovered_balance["maintenance"],"second_cold_reopen_preserved_counters":true});
    evidence["retained_export"] = json!({"databases":2,"source_revision":1,"immutable_puts":0,
        "restored_after_source_image_collection":true,"independent_local_destination":true,"identical_restore_repeated":true});
    std::fs::write(report, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
