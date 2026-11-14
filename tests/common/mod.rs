//! Real server processes for the HTTP and cluster tests.
#![allow(dead_code)]
use durable_db::auth::{Grant, Keys};
use serde_json::Value;
use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub struct Server {
    pub url: String,
    pub address: String,
    child: Child,
}

impl Server {
    /// Start `durable-db serve` on `data`, optionally as a member of `peers`.
    pub async fn start(data: &Path, port: u16, peers: Option<&str>, extra: &[&str]) -> Self {
        let address = format!("127.0.0.1:{port}");
        let mut command = Command::new(env!("CARGO_BIN_EXE_durable-db"));
        command
            .args(["serve", "--no-fsync", "--listen", &address, "--data"])
            .arg(data)
            .args(extra)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if let Some(peers) = peers {
            command.args([
                "--peers",
                peers,
                "--advertise",
                &address,
                "--drain-seconds",
                "0",
            ]);
        }
        let child = command.spawn().expect("the server starts");
        let server = Self {
            url: format!("http://{address}"),
            address,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if reqwest::get(format!("{}/readyz", server.url))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return server;
            }
            assert!(Instant::now() < deadline, "the server did not become ready");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// SIGKILL: the process gets no chance to release anything.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// SIGTERM, then wait for the graceful exit.
    pub fn terminate(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

pub fn key_file(data: &Path) -> PathBuf {
    data.join("admin.key")
}

pub fn admin_token(data: &Path) -> String {
    Keys::load(&key_file(data)).unwrap().mint(&Grant::admin())
}

/// A JSON request; returns the status and the parsed body (or its text).
pub async fn call(method: &str, url: &str, token: &str, body: Option<Value>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut request = client.request(method.parse().unwrap(), url);
    if !token.is_empty() {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await.expect("the request is sent");
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}
