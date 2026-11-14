//! `durable-db bench`: a closed-loop load generator for a running server.
//!
//! Each client sends one request at a time and waits for the answer, so the
//! reported latency is what a caller observes, including queueing in the
//! server. Throughput is completed requests over wall time.
use crate::auth::{Grant, Keys};
use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use rand::Rng;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Workload {
    /// One-key transactions.
    Write,
    /// Linearizable point reads.
    Read,
    /// Point reads at session consistency.
    SessionRead,
    /// Linearizable 20-item range scans.
    Scan,
    /// Half writes, half linearizable reads.
    Mixed,
}

#[derive(Args)]
pub struct Bench {
    /// Servers to load, comma-separated. Clients are spread across them.
    #[arg(long, env = "DURABLE_DB_URL", default_value = "http://127.0.0.1:7654")]
    url: String,
    /// File holding the token signing key, to create and delete the test databases.
    #[arg(long, env = "DURABLE_DB_KEY_FILE", default_value = "data/admin.key")]
    key_file: PathBuf,
    #[arg(long, value_enum, default_value = "write")]
    workload: Workload,
    /// Concurrent clients.
    #[arg(long, default_value_t = 64)]
    clients: usize,
    /// Databases the clients are spread over.
    #[arg(long, default_value_t = 1)]
    databases: usize,
    /// Seconds to measure.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    /// Bytes of JSON per value.
    #[arg(long, default_value_t = 256)]
    value_bytes: usize,
    /// Distinct keys per database.
    #[arg(long, default_value_t = 10_000)]
    keys: usize,
    /// Send an idempotency key with every write, as the SDK does.
    #[arg(long)]
    idempotent: bool,
    /// Write the results as JSON to this file.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Leave the test databases in place.
    #[arg(long)]
    keep: bool,
}

struct Shared {
    http: reqwest::Client,
    urls: Vec<String>,
    token: String,
    names: Vec<String>,
    value: String,
    keys: usize,
    idempotent: bool,
    stop: AtomicBool,
    errors: AtomicU64,
}

impl Shared {
    async fn post(
        &self,
        client: usize,
        db: &str,
        action: &str,
        body: String,
    ) -> anyhow::Result<reqwest::StatusCode> {
        let url = format!(
            "{}/v1/databases/{db}/{action}",
            self.urls[client % self.urls.len()]
        );
        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        response.bytes().await?;
        Ok(status)
    }

    fn write_body(&self, key: usize, unique: u64) -> String {
        let idempotency = if self.idempotent {
            format!(",\"idempotency_key\":\"bench-{unique:016x}\"")
        } else {
            String::new()
        };
        format!(
            "{{\"writes\":[{{\"key\":\"key/{key:08}\",\"value\":{}}}]{idempotency}}}",
            self.value
        )
    }
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    f64::from(sorted[index]) / 1000.0
}

pub async fn run(args: Bench) -> anyhow::Result<()> {
    if args.clients == 0 || args.databases == 0 || args.keys == 0 {
        bail!("clients, databases and keys must be at least 1");
    }
    let keys = Keys::load(&args.key_file)?;
    let run_id: u32 = rand::random();
    let padding = args.value_bytes.saturating_sub(12);
    let shared = Arc::new(Shared {
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(args.clients + 8)
            .timeout(Duration::from_secs(60))
            .build()?,
        urls: args
            .url
            .split(',')
            .map(|url| url.trim().trim_end_matches('/').to_owned())
            .collect(),
        token: keys.mint(&Grant::admin()),
        names: (0..args.databases)
            .map(|i| format!("bench-{run_id:08x}-{i}"))
            .collect(),
        value: json!({"pad": "x".repeat(padding)}).to_string(),
        keys: args.keys,
        idempotent: args.idempotent,
        stop: AtomicBool::new(false),
        errors: AtomicU64::new(0),
    });

    for name in &shared.names {
        let response = shared
            .http
            .put(format!("{}/v1/databases/{name}", shared.urls[0]))
            .bearer_auth(&shared.token)
            .send()
            .await
            .with_context(|| format!("cannot reach {}", shared.urls[0]))?;
        if !response.status().is_success() {
            bail!(
                "creating {name}: {} {}",
                response.status(),
                response.text().await?
            );
        }
    }
    if args.workload != Workload::Write {
        // Reads need something to find. Load in batches of 500 keys.
        eprintln!(
            "loading {} keys into each of {} databases",
            args.keys, args.databases
        );
        for name in &shared.names {
            for start in (0..args.keys).step_by(500) {
                let writes: Vec<String> = (start..(start + 500).min(args.keys))
                    .map(|key| format!("{{\"key\":\"key/{key:08}\",\"value\":{}}}", shared.value))
                    .collect();
                let status = shared
                    .post(
                        0,
                        name,
                        "commit",
                        format!("{{\"writes\":[{}]}}", writes.join(",")),
                    )
                    .await?;
                if !status.is_success() {
                    bail!("loading {name}: {status}");
                }
            }
        }
    }

    eprintln!(
        "measuring for {}s: {} clients, {} database(s), {} server(s)",
        args.seconds,
        args.clients,
        args.databases,
        shared.urls.len()
    );
    let started = Instant::now();
    let mut clients = Vec::new();
    for client in 0..args.clients {
        let shared = shared.clone();
        let workload = args.workload;
        clients.push(tokio::spawn(async move {
            let name = shared.names[client % shared.names.len()].clone();
            let mut latencies: Vec<u32> = Vec::with_capacity(1 << 16);
            while !shared.stop.load(Ordering::Relaxed) {
                let (key, unique, coin): (usize, u64, bool) = {
                    let mut rng = rand::rng();
                    (rng.random_range(0..shared.keys), rng.random(), rng.random())
                };
                let read = |consistency: &str| {
                    format!("{{\"keys\":[\"key/{key:08}\"],\"consistency\":\"{consistency}\"}}")
                };
                let (action, body) = match workload {
                    Workload::Write => ("commit", shared.write_body(key, unique)),
                    Workload::Mixed if coin => ("commit", shared.write_body(key, unique)),
                    Workload::Read | Workload::Mixed => ("read", read("strong")),
                    Workload::SessionRead => ("read", read("session")),
                    Workload::Scan => (
                        "read",
                        format!("{{\"scans\":[{{\"start\":\"key/{key:08}\",\"limit\":20}}]}}"),
                    ),
                };
                let began = Instant::now();
                match shared.post(client, &name, action, body).await {
                    Ok(status) if status.is_success() => {
                        latencies
                            .push(began.elapsed().as_micros().min(u128::from(u32::MAX)) as u32);
                    }
                    _ => {
                        shared.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies
        }));
    }
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    shared.stop.store(true, Ordering::Relaxed);
    let mut latencies: Vec<u32> = Vec::new();
    for client in clients {
        latencies.extend(client.await?);
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();

    if !args.keep {
        for name in &shared.names {
            let _ = shared
                .http
                .delete(format!("{}/v1/databases/{name}", shared.urls[0]))
                .bearer_auth(&shared.token)
                .send()
                .await;
        }
    }

    let workload = args
        .workload
        .to_possible_value()
        .map(|v| v.get_name().to_owned())
        .unwrap_or_default();
    let result = json!({
        "workload": workload,
        "clients": args.clients,
        "databases": args.databases,
        "servers": shared.urls.len(),
        "value_bytes": args.value_bytes,
        "idempotent": args.idempotent,
        "seconds": (elapsed * 100.0).round() / 100.0,
        "requests": latencies.len(),
        "errors": shared.errors.load(Ordering::Relaxed),
        "requests_per_second": (latencies.len() as f64 / elapsed).round(),
        "latency_ms": {
            "p50": percentile(&latencies, 0.50),
            "p90": percentile(&latencies, 0.90),
            "p99": percentile(&latencies, 0.99),
            "p999": percentile(&latencies, 0.999),
            "max": percentile(&latencies, 1.0),
        },
    });
    println!(
        "{workload}: {} requests/s   p50 {:.2} ms   p99 {:.2} ms   p99.9 {:.2} ms   max {:.2} ms   errors {}",
        result["requests_per_second"],
        result["latency_ms"]["p50"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["p99"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["p999"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["max"].as_f64().unwrap_or(0.0),
        result["errors"],
    );
    if let Some(path) = args.output {
        std::fs::write(&path, serde_json::to_string_pretty(&result)? + "\n")?;
    }
    Ok(())
}
