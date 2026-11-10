#![cfg(target_os = "linux")]
use durable_db::{auth::Auth, cache::Cache, engine::Engine, storage::Local};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

struct Server {
    child: Child,
    url: String,
    token: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Server {
    async fn start(data: &Path) -> Self {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);
        let log = data.join("server.log");
        let mut child = Command::new(env!("CARGO_BIN_EXE_durable-db"))
            .args(["serve", "--data"])
            .arg(data)
            .args([
                "--listen",
                &address.to_string(),
                "--maintenance-seconds",
                "1",
                "--request-units-per-second",
                "1000000",
                "--request-burst-units",
                "1000000",
            ])
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let url = format!("http://{address}");
        let client = reqwest::Client::new();
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "server exited before readiness: {}",
                    std::fs::read_to_string(&log).unwrap()
                );
                if client
                    .get(format!("{url}/health/ready"))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if ready.is_err() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("local readiness timed out");
        }
        let token = Auth::new(&std::fs::read_to_string(data.join("admin.key")).unwrap())
            .unwrap()
            .admin_token();
        Self { child, url, token }
    }
    async fn call(&self, method: &str, path: &str, body: Value) -> Value {
        let response = reqwest::Client::new()
            .request(method.parse().unwrap(), format!("{}{path}", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(status.is_success(), "{status}: {body}");
        body
    }
}
fn files(path: PathBuf) -> usize {
    std::fs::read_dir(path).unwrap().count()
}
fn write(revision: u64) -> Value {
    json!({"request_id":format!("r-{revision}"),"expected_revision":revision,"writes":[{"key":"x","value":"eA=="}]})
}

#[tokio::test]
async fn native_local_cli_pause_kill_recovery_and_automatic_collection() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path();
    let first = Server::start(data).await;
    first
        .call(
            "PUT",
            "/v1/databases/app",
            json!({"request_id":"create","limits":{"max_keys":10,"max_bytes":1000}}),
        )
        .await;
    for revision in 0..260 {
        assert_eq!(
            first
                .call("POST", "/v1/databases/app/commit", write(revision))
                .await["revision"],
            revision + 1
        );
    }
    assert_eq!(
        files(data.join("store/heads")),
        0,
        "CLI uses native journals/tables"
    );
    assert!(files(data.join("store/tables")) >= 3);
    tokio::time::timeout(Duration::from_secs(5), async {
        while files(data.join("store/journal")) > 5 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("automatic unbudgeted checkpoint collection");
    assert!(
        Command::new("kill")
            .args(["-STOP", &first.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !std::fs::read_to_string(format!("/proc/{}/status", first.child.id()))
            .unwrap()
            .contains("State:\tT")
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = Command::new(env!("CARGO_BIN_EXE_durable-db"))
        .args(["serve", "--data"])
        .arg(data)
        .args(["--listen", "127.0.0.1:0"])
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("already owned"));
    assert!(
        Command::new("kill")
            .args(["-CONT", &first.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(
        first
            .call("POST", "/v1/databases/app/commit", write(259))
            .await["replayed"],
        true
    );
    drop(first); // SIGKILL, not graceful shutdown.
    let recovered = Server::start(data).await;
    let read = recovered
        .call("POST", "/v1/databases/app/read", json!({"keys":["x"]}))
        .await;
    assert_eq!(read["revision"], 260);
    assert_eq!(read["values"], json!(["eA=="]));
    assert_eq!(
        recovered
            .call("POST", "/v1/databases/app/commit", write(259))
            .await["replayed"],
        true
    );
    assert_eq!(
        recovered
            .call("POST", "/v1/databases/app/commit", write(260))
            .await["revision"],
        261
    );
    drop(recovered);
    let again = Server::start(data).await;
    assert_eq!(
        again
            .call("POST", "/v1/databases/app/commit", write(260))
            .await["replayed"],
        true
    );
    drop(again);
    let store = data.join("store");
    let authority = std::fs::read(store.join("authority.json")).unwrap();
    std::fs::remove_file(store.join("authority.json")).unwrap();
    assert!(
        durable_db::api::open_local(
            store.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            8,
            Duration::from_secs(1)
        )
        .await
        .is_err()
    );
    assert!(
        !store.join("authority.json").exists(),
        "missing authority must not initialize an empty database"
    );
    std::fs::write(store.join("authority.json"), authority).unwrap();
}

#[tokio::test]
async fn native_local_refuses_legacy_data_without_replacing_it() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("store");
    let cache = Arc::new(Cache::new(0, None).unwrap());
    let engine = Engine::new(Arc::new(Local::open(path.clone()).unwrap()), cache.clone());
    engine.create("app", Default::default()).await.unwrap();
    drop(engine);
    let before = std::fs::read(path.join("heads/app.json")).unwrap();
    let result = durable_db::api::open_local(path.clone(), cache, 8, Duration::from_secs(1)).await;
    assert!(result.is_err());
    assert_eq!(std::fs::read(path.join("heads/app.json")).unwrap(), before);
    assert!(!path.join("authority.json").exists());
    assert!(
        Local::open(path).is_ok(),
        "failed startup released ownership"
    );
}
