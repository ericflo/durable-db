//! Real server processes for the HTTP and cluster tests.
#![allow(dead_code)]
use durable_db::auth::{Grant, Keys};
use serde_json::Value;
use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU16, Ordering},
    time::{Duration, Instant},
};

/// A port for a server process to listen on. Not one the system picks
/// (`bind` to port 0): between this function's return and the server's own
/// `bind` the system may give the same port to the next caller or to a
/// client's connection, and then one server fails to start and its test
/// talks to another test's server. These are below the range the system
/// picks from, no two calls in one process return the same, and each was
/// free when it was returned.
pub fn free_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        // Processes start at different places, so that two test runs on one
        // machine seldom try the same port.
        let nth = NEXT.fetch_add(1, Ordering::Relaxed) as u32;
        let port = 20_000 + (std::process::id() * 64 + nth) % 12_000;
        if TcpListener::bind(("127.0.0.1", port as u16)).is_ok() {
            return port as u16;
        }
    }
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
        let mut server = Self {
            url: format!("http://{address}"),
            address,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!("the server on {} stopped: {status}", server.address);
            }
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
