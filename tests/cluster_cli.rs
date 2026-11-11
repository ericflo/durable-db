//! Native cloud CLI qualification on explicitly disposable providers.
use durable_db::{auth::Auth, providers::partitions::Layout};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_durable-db")
}

#[test]
fn cluster_cli_configuration_is_validated_before_provider_access() {
    let temp = tempfile::tempdir().unwrap();
    let key = temp.path().join("admin.key");
    std::fs::write(&key, "2a".repeat(32)).unwrap();
    let base = [
        "serve",
        "--b2-bucket",
        "not-a-real-bucket",
        "--upload-budget-bytes",
        "1048576",
        "--upload-budget-attempts",
        "4096",
        "--admin-key-file",
        key.to_str().unwrap(),
    ];
    for (args, expected) in [
        // Valid default resource settings must reach the missing-credential boundary.
        (vec!["--listen", "127.0.0.1:0"], "initialize B2 transport"),
        (vec!["--listen", "0.0.0.0:0"], "require --advertise-ip"),
        (
            vec!["--listen", "127.0.0.1:0", "--advertise-ip", "127.0.0.2"],
            "must match the listen",
        ),
        (
            vec!["--listen", "127.0.0.1:0", "--discovery-service", "peers"],
            "fixed listen port",
        ),
        (
            vec![
                "--listen",
                "127.0.0.1:0",
                "--partitions",
                "1024",
                "--resident-memory-mib",
                "16",
            ],
            "invalid validation limits",
        ),
        (
            vec!["--maintenance-seconds", "86401"],
            "invalid resource configuration",
        ),
    ] {
        let result = Command::new(binary())
            .args(base)
            .args(args)
            .env_remove("B2_APPLICATION_KEY_ID")
            .env_remove("B2_APPLICATION_KEY")
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(
            String::from_utf8_lossy(&result.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    for args in [
        vec!["serve", "--b2-bucket", "not-a-real-bucket"],
        vec!["serve", "--discovery-service", "peers"],
    ] {
        let result = Command::new(binary()).args(args).output().unwrap();
        assert!(!result.status.success());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            error.contains("requires --upload-budget") || error.contains("require B2/S3"),
            "{error}"
        );
    }
}

struct Server {
    child: Child,
    url: String,
    log: PathBuf,
    client: Client,
    token: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Server {
    async fn start(
        dir: &Path,
        provider: &str,
        bucket: &str,
        prefix: &str,
        partitions: u32,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let key = dir.join("admin.key");
        let log = dir.join(format!("{}.log", uuid::Uuid::new_v4()));
        let output = std::fs::File::create(&log).unwrap();
        let child = Command::new(binary())
            .args([
                "serve",
                if provider == "b2" {
                    "--b2-bucket"
                } else {
                    "--bucket"
                },
                bucket,
                "--prefix",
                prefix,
                "--listen",
                &address.to_string(),
                "--admin-key-file",
                key.to_str().unwrap(),
                "--partitions",
                &partitions.to_string(),
                "--memory-cache-mib",
                "4",
                "--resident-memory-mib",
                "128",
                "--concurrency",
                "16",
                "--request-units-per-second",
                "1000",
                "--request-burst-units",
                "1000",
                "--upload-budget-bytes",
                "0",
                "--upload-budget-attempts",
                "4096",
            ])
            .env_remove("DURABLE_DB_ADVERTISE_IP")
            .env_remove("DURABLE_DB_DISCOVERY_SERVICE")
            .stdout(output.try_clone().unwrap())
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        Self {
            child,
            url: format!("http://{address}"),
            log,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(35))
                .build()
                .unwrap(),
            token: Auth::new(&std::fs::read_to_string(key).unwrap())
                .unwrap()
                .administrator_token(3600)
                .unwrap(),
        }
    }
    async fn health(&mut self, path: &str) {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    panic!(
                        "server exited {status}: {}",
                        std::fs::read_to_string(&self.log).unwrap()
                    );
                }
                if self
                    .client
                    .get(format!("{}{path}", self.url))
                    .send()
                    .await
                    .is_ok_and(|response| response.status() == StatusCode::OK)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("native CLI health deadline");
    }
    async fn call(&self, method: &str, path: &str, body: Value) -> Value {
        // Caller retries retain the identical ID and body, including uncertain writes.
        for attempt in 0..12 {
            let response = self
                .client
                .request(method.parse().unwrap(), format!("{}{path}", self.url))
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let result: Value = response.json().await.unwrap();
            if status.is_success() {
                return result;
            }
            assert!(
                matches!(
                    status,
                    StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                ) && attempt < 11,
                "{status}: {result}"
            );
            tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
        }
        unreachable!()
    }
}
fn commit(id: usize) -> Value {
    json!({"request_id":"write", "expected_revision":0,
        "writes":[{"key":"value", "value":if id == 0 {"b25l"} else {"dHdv"}}]})
}

#[tokio::test]
#[ignore = "requires explicitly disposable B2/S3 bucket and report path"]
async fn native_cloud_cli_recovers_exhausted_admission_and_preserves_data() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").unwrap();
    assert!(matches!(provider.as_str(), "b2" | "s3"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("durable-db-test-"));
    let report = std::env::var("DURABLE_DB_NATIVE_HTTP_REPORT").unwrap();
    let prefix = format!("cli-{}", uuid::Uuid::new_v4());
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("admin.key"), "2a".repeat(32)).unwrap();
    let names: [String; 2] = std::array::from_fn(|id| {
        (0..100)
            .map(|i| format!("app-{i}"))
            .find(|name| Layout::new(2).unwrap().partition(name).unwrap() == id as u32)
            .unwrap()
    });
    let started = Instant::now();
    let mut first = Server::start(dir.path(), &provider, &bucket, &prefix, 2).await;
    first.health("/health/live").await;
    let empty = first
        .call("GET", "/v1/storage/upload-budget", Value::Null)
        .await;
    assert_eq!(empty["limits"]["bytes"], 0);
    assert_eq!(empty["allocated"]["bytes"], 0);
    first
        .call(
            "POST",
            "/v1/storage/upload-budget",
            json!({
                "request_id":"bootstrap-credit", "expected_configuration_revision":0,
                "limits":{"bytes":1048576,"attempts":4096}
            }),
        )
        .await;
    first.health("/health/ready").await;
    for (id, name) in names.iter().enumerate() {
        first
            .call(
                "PUT",
                &format!("/v1/databases/{name}"),
                json!({"request_id":"create", "limits":{"max_keys":100,"max_bytes":1048576}}),
            )
            .await;
        assert_eq!(
            first
                .call("POST", &format!("/v1/databases/{name}/commit"), commit(id))
                .await["revision"],
            1
        );
    }
    let before = first
        .call("GET", "/v1/storage/upload-budget", Value::Null)
        .await;
    assert_eq!(
        before["allocated"]["bytes"], before["limits"]["bytes"],
        "first grant reserves the small ceiling"
    );
    drop(first); // SIGKILL strands all unspent process-local namespace credit.
    let mut second = Server::start(dir.path(), &provider, &bucket, &prefix, 2).await;
    second.health("/health/live").await;
    assert_eq!(
        second
            .client
            .get(format!("{}/health/ready", second.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        second
            .call("GET", "/v1/storage/upload-budget", Value::Null)
            .await,
        before
    );
    let refill = json!({"request_id":"recover-credit", "expected_configuration_revision":1,
        "limits":{"bytes":16777216,"attempts":8192}});
    let funded = second
        .call("POST", "/v1/storage/upload-budget", refill.clone())
        .await;
    assert_eq!(funded["replayed"], false);
    assert_eq!(
        second
            .call("POST", "/v1/storage/upload-budget", refill)
            .await["replayed"],
        true
    );
    second.health("/health/ready").await;
    for (id, name) in names.iter().enumerate() {
        let read = second
            .call(
                "POST",
                &format!("/v1/databases/{name}/read"),
                json!({"keys":["value"]}),
            )
            .await;
        assert_eq!(read["values"][0], if id == 0 { "b25l" } else { "dHdv" });
        assert_eq!(
            second
                .call("POST", &format!("/v1/databases/{name}/commit"), commit(id))
                .await["revision"],
            1
        );
    }
    let mut incompatible = Server::start(dir.path(), &provider, &bucket, &prefix, 3).await;
    tokio::time::timeout(Duration::from_secs(60), async {
        while incompatible.child.try_wait().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        std::fs::read_to_string(&incompatible.log)
            .unwrap()
            .contains("layout")
    );
    drop(incompatible);
    drop(second);
    let mut third = Server::start(dir.path(), &provider, &bucket, &prefix, 2).await;
    third.health("/health/ready").await;
    assert_eq!(
        third
            .call("GET", "/v1/storage/upload-budget", Value::Null)
            .await["limits"],
        funded["status"]["limits"]
    );
    for (id, name) in names.iter().enumerate() {
        assert_eq!(
            third
                .call("POST", &format!("/v1/databases/{name}/commit"), commit(id))
                .await["revision"],
            1
        );
    }
    drop(third);
    use sha2::{Digest, Sha256};
    std::fs::write(report, serde_json::to_vec_pretty(&json!({
        "seconds":started.elapsed().as_secs_f64(), "partitions":2, "cold_processes":3,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(binary()).unwrap())),
        "native_cloud_cli":true,"admin_bootstrap_from_zero_credit":true,"admin_refill_available_before_owner_readiness":true,
        "exhausted_credit_preserved_on_restart":true,"refill_idempotent":true,
        "replenished_limits_preserved_on_restart":true,"values_and_exact_receipts_recovered":true,
        "incompatible_layout_refused":true,
        "scope":"Debug executable with single-process membership over real B2/versioned MinIO. Not DNS/Kubernetes, AWS, a sustained workload or an RSS/SLO bound."
    })).unwrap()).unwrap();
}
