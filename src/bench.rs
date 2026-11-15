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
    /// Every key written once, in an order that is spread over the keys,
    /// by one-key transactions; it ends when all are written. For a
    /// database of a size: `--keys` times `--value-bytes`.
    Fill,
    /// Linearizable point reads.
    Read,
    /// Point reads at session consistency.
    SessionRead,
    /// Linearizable 20-item range scans.
    Scan,
    /// Half writes, half linearizable reads.
    Mixed,
    /// Transfers between accounts, each also appending a log entry, verified
    /// afterwards: balances must sum to the starting total and the log must
    /// hold exactly the acknowledged transfers. Run it while servers are
    /// added, removed or killed to check that nothing is lost or applied twice.
    Ledger,
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
    /// Seconds to measure. A fill takes as long as it takes.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    /// Bytes of JSON per value.
    #[arg(long, default_value_t = 256)]
    value_bytes: usize,
    /// Distinct keys per database.
    #[arg(long, default_value_t = 10_000)]
    keys: usize,
    /// Give every write a different value that does not compress, so that
    /// what is stored is as large as what is written.
    #[arg(long)]
    random_values: bool,
    /// Send an idempotency key with every write, as the SDK does.
    #[arg(long)]
    idempotent: bool,
    /// Write the results as JSON to this file.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Leave the test databases in place.
    #[arg(long)]
    keep: bool,
    /// Quota in bytes of the databases created; the server's default without it.
    #[arg(long)]
    max_bytes: Option<u64>,
    /// Use the databases of this name, with -0, -1 and so on after it, as
    /// they are: they may exist already, nothing is loaded into them before
    /// they are read, and they are left in place.
    #[arg(long)]
    database: Option<String>,
}

struct Shared {
    http: reqwest::Client,
    urls: Vec<String>,
    token: String,
    names: Vec<String>,
    value: String,
    /// Length of the text in a value that is made up for each write.
    random: Option<usize>,
    keys: usize,
    /// For a fill: how many of each database's keys have been taken, and
    /// how far apart one is from the next. The stride shares no factor with
    /// the number of keys, so every key is reached once.
    filled: Vec<AtomicU64>,
    stride: u64,
    idempotent: bool,
    stop: AtomicBool,
    errors: AtomicU64,
    /// Answers of 503: the server asked the client to come back.
    refused: AtomicU64,
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

    /// The next key of a fill, or `None` when the database has them all.
    fn next_to_fill(&self, db: usize) -> Option<usize> {
        let taken = self.filled[db].fetch_add(1, Ordering::Relaxed);
        let keys = self.keys as u64;
        (taken < keys)
            .then(|| (u128::from(taken) * u128::from(self.stride) % u128::from(keys)) as usize)
    }

    fn write_body(&self, key: usize, unique: u64) -> String {
        let idempotency = if self.idempotent {
            format!(",\"idempotency_key\":\"bench-{unique:016x}\"")
        } else {
            String::new()
        };
        format!(
            "{{\"writes\":[{{\"key\":\"key/{key:08}\",\"value\":{}}}]{idempotency}}}",
            self.value(unique)
        )
    }

    /// The value of one write: the same for all, or text made from `seed`.
    fn value(&self, seed: u64) -> std::borrow::Cow<'_, str> {
        let Some(len) = self.random else {
            return std::borrow::Cow::Borrowed(&self.value);
        };
        let mut pad = String::with_capacity(len + 16);
        let mut state = seed | 1;
        while pad.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            pad.push_str(&format!("{state:016x}"));
        }
        pad.truncate(len);
        std::borrow::Cow::Owned(format!("{{\"pad\":\"{pad}\"}}"))
    }
}

/// A step through `keys` keys that reaches each of them once and puts
/// neighbours in the order far apart among the keys.
fn stride(keys: u64) -> u64 {
    let gcd = |mut a: u64, mut b: u64| {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    };
    let mut stride = ((keys as f64 * 0.618_033_988_75) as u64).max(1);
    while gcd(stride, keys) != 1 {
        stride += 1;
    }
    stride
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
    let prefix = match &args.database {
        Some(name) => name.clone(),
        None => format!("bench-{:08x}", rand::random::<u32>()),
    };
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
            .map(|i| format!("{prefix}-{i}"))
            .collect(),
        value: json!({"pad": "x".repeat(padding)}).to_string(),
        random: args.random_values.then_some(padding),
        keys: args.keys,
        filled: (0..args.databases).map(|_| AtomicU64::new(0)).collect(),
        stride: stride(args.keys as u64),
        idempotent: args.idempotent,
        stop: AtomicBool::new(false),
        errors: AtomicU64::new(0),
        refused: AtomicU64::new(0),
    });

    for name in &shared.names {
        // Creation is idempotent; repeat it through transient failures.
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut request = shared
                .http
                .put(format!("{}/v1/databases/{name}", shared.urls[0]))
                .bearer_auth(&shared.token);
            if let Some(max_bytes) = args.max_bytes {
                request = request
                    .header("content-type", "application/json")
                    .body(json!({ "max_bytes": max_bytes }).to_string());
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("cannot reach {}", shared.urls[0]))?;
            // One that is there with other limits is used as it is.
            if response.status().is_success()
                || (response.status() == 409 && args.database.is_some())
            {
                break;
            }
            if response.status() != 503 || attempt >= 20 {
                bail!(
                    "creating {name}: {} {}",
                    response.status(),
                    response.text().await?
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    if args.workload == Workload::Ledger {
        return ledger(&args, shared).await;
    }
    let writes_only = matches!(args.workload, Workload::Write | Workload::Fill);
    if !writes_only && args.database.is_none() {
        // Reads need something to find. Load in batches of 500 keys.
        eprintln!(
            "loading {} keys into each of {} databases",
            args.keys, args.databases
        );
        for name in &shared.names {
            for start in (0..args.keys).step_by(500) {
                let writes: Vec<String> = (start..(start + 500).min(args.keys))
                    .map(|key| {
                        let value = shared.value(rand::random());
                        format!("{{\"key\":\"key/{key:08}\",\"value\":{value}}}")
                    })
                    .collect();
                let body = format!("{{\"writes\":[{}]}}", writes.join(","));
                // A database that is flushing says so; wait as a client would.
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    let status = shared.post(0, name, "commit", body.clone()).await?;
                    if status.is_success() {
                        break;
                    }
                    if status != 503 || attempt >= 400 {
                        bail!("loading {name}: {status}");
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }

    if args.workload == Workload::Fill {
        eprintln!(
            "writing {} keys to each of {} database(s): {} clients, {} server(s)",
            args.keys,
            args.databases,
            args.clients,
            shared.urls.len()
        );
    } else {
        eprintln!(
            "measuring for {}s: {} clients, {} database(s), {} server(s)",
            args.seconds,
            args.clients,
            args.databases,
            shared.urls.len()
        );
    }
    let started = Instant::now();
    let mut clients = Vec::new();
    for client in 0..args.clients {
        let shared = shared.clone();
        let workload = args.workload;
        clients.push(tokio::spawn(async move {
            let db = client % shared.names.len();
            let name = shared.names[db].clone();
            let mut latencies: Vec<u32> = Vec::with_capacity(1 << 16);
            // For a fill: a key the server asked to wait with.
            let mut again: Option<usize> = None;
            while !shared.stop.load(Ordering::Relaxed) {
                let (mut key, unique, coin): (usize, u64, bool) = {
                    let mut rng = rand::rng();
                    (rng.random_range(0..shared.keys), rng.random(), rng.random())
                };
                if workload == Workload::Fill {
                    match again.take().or_else(|| shared.next_to_fill(db)) {
                        Some(next) => key = next,
                        None => break,
                    }
                }
                let read = |consistency: &str| {
                    format!("{{\"keys\":[\"key/{key:08}\"],\"consistency\":\"{consistency}\"}}")
                };
                let (action, body) = match workload {
                    Workload::Write | Workload::Fill => ("commit", shared.write_body(key, unique)),
                    Workload::Mixed if coin => ("commit", shared.write_body(key, unique)),
                    Workload::Read | Workload::Mixed => ("read", read("strong")),
                    Workload::SessionRead => ("read", read("session")),
                    Workload::Ledger => unreachable!("handled separately"),
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
                    Ok(status) if status == 503 => {
                        shared.refused.fetch_add(1, Ordering::Relaxed);
                        if workload == Workload::Fill {
                            again = Some(key);
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    }
                    _ => {
                        shared.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies
        }));
    }
    // A fill ends when its clients have written every key.
    if args.workload != Workload::Fill {
        tokio::time::sleep(Duration::from_secs(args.seconds)).await;
        shared.stop.store(true, Ordering::Relaxed);
    }
    let mut latencies: Vec<u32> = Vec::new();
    for client in clients {
        latencies.extend(client.await?);
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();

    if !args.keep && args.database.is_none() {
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
        "refused": shared.refused.load(Ordering::Relaxed),
        "requests_per_second": (latencies.len() as f64 / elapsed).round(),
        "latency_ms": {
            "p50": percentile(&latencies, 0.50),
            "p90": percentile(&latencies, 0.90),
            "p99": percentile(&latencies, 0.99),
            "p999": percentile(&latencies, 0.999),
            "max": percentile(&latencies, 1.0),
        },
    });
    // A 503 is the server asking the client to wait, not a failure.
    println!(
        "{workload}: {} requests/s   p50 {:.2} ms   p99 {:.2} ms   p99.9 {:.2} ms   max {:.2} ms   errors {}   told to wait {}",
        result["requests_per_second"],
        result["latency_ms"]["p50"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["p99"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["p999"].as_f64().unwrap_or(0.0),
        result["latency_ms"]["max"].as_f64().unwrap_or(0.0),
        result["errors"],
        result["refused"],
    );
    if let Some(path) = args.output {
        std::fs::write(&path, serde_json::to_string_pretty(&result)? + "\n")?;
    }
    Ok(())
}

const OPENING_BALANCE: u64 = 1000;

/// A request repeated until it has a definite answer.
async fn definite(
    shared: &Shared,
    client: usize,
    db: &str,
    action: &str,
    body: &str,
) -> anyhow::Result<(u16, serde_json::Value)> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let url = format!(
            "{}/v1/databases/{db}/{action}",
            shared.urls[(client + attempts) % shared.urls.len()]
        );
        let sent = shared
            .http
            .post(url)
            .bearer_auth(&shared.token)
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await;
        if let Ok(response) = sent {
            let status = response.status().as_u16();
            if let Ok(text) = response.text().await
                && status != 503
                && status != 429
            {
                return Ok((
                    status,
                    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
                ));
            }
        }
        shared.errors.fetch_add(1, Ordering::Relaxed);
        if attempts > 2400 {
            bail!("no definite answer from {db} after two minutes");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn ledger(args: &Bench, shared: Arc<Shared>) -> anyhow::Result<()> {
    let accounts = (args.clients * 2).max(8);
    for name in &shared.names {
        let writes: Vec<String> = (0..accounts)
            .map(|a| format!("{{\"key\":\"acct/{a:06}\",\"value\":{OPENING_BALANCE}}}"))
            .collect();
        let (status, _) = definite(
            &shared,
            0,
            name,
            "commit",
            &format!("{{\"writes\":[{}]}}", writes.join(",")),
        )
        .await?;
        if status != 200 {
            bail!("opening the ledger in {name}: {status}");
        }
    }
    eprintln!(
        "ledger for {}s: {} clients, {} accounts in each of {} database(s), {} server(s)",
        args.seconds,
        args.clients,
        accounts,
        shared.names.len(),
        shared.urls.len()
    );
    let started = Instant::now();
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut clients = Vec::new();
    for client in 0..args.clients {
        let (shared, conflicts) = (shared.clone(), conflicts.clone());
        clients.push(tokio::spawn(async move {
            let name = shared.names[client % shared.names.len()].clone();
            let mut transfers = 0u64;
            let mut latencies: Vec<u32> = Vec::new();
            while !shared.stop.load(Ordering::Relaxed) {
                let (from, to, amount, unique): (usize, usize, u64, u64) = {
                    let mut rng = rand::rng();
                    let from = rng.random_range(0..accounts);
                    (from, (from + rng.random_range(1..accounts)) % accounts, rng.random_range(1..=10), rng.random())
                };
                let began = Instant::now();
                let read = format!("{{\"keys\":[\"acct/{from:06}\",\"acct/{to:06}\"]}}");
                let (status, seen) = definite(&shared, client, &name, "read", &read).await?;
                let balance = |i: usize| (seen["values"][i]["value"].as_u64(), seen["values"][i]["version"].as_u64());
                let ((Some(a), Some(a_version)), (Some(b), Some(b_version))) = (balance(0), balance(1)) else {
                    bail!("reading accounts in {name}: {status} {seen}");
                };
                if a < amount {
                    continue;
                }
                // Both balances and the log entry change together or not at all.
                let commit = format!(
                    "{{\"checks\":[{{\"key\":\"acct/{from:06}\",\"version\":{a_version}}},{{\"key\":\"acct/{to:06}\",\"version\":{b_version}}}],\
                     \"writes\":[{{\"key\":\"acct/{from:06}\",\"value\":{}}},{{\"key\":\"acct/{to:06}\",\"value\":{}}},\
                     {{\"key\":\"log/{client:05}/{transfers:09}\",\"value\":{{\"from\":{from},\"to\":{to},\"amount\":{amount}}}}}],\
                     \"idempotency_key\":\"ledger-{unique:016x}\"}}",
                    a - amount,
                    b + amount
                );
                match definite(&shared, client, &name, "commit", &commit).await? {
                    (200, _) => {
                        transfers += 1;
                        latencies.push(began.elapsed().as_micros().min(u128::from(u32::MAX)) as u32);
                    }
                    (409, _) => {
                        conflicts.fetch_add(1, Ordering::Relaxed);
                    }
                    (status, body) => bail!("transfer in {name}: {status} {body}"),
                }
            }
            Ok((name, transfers, latencies))
        }));
    }
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    shared.stop.store(true, Ordering::Relaxed);
    let mut acknowledged = std::collections::HashMap::new();
    let mut latencies: Vec<u32> = Vec::new();
    for client in clients {
        let (name, transfers, mut observed) = client.await??;
        *acknowledged.entry(name).or_insert(0u64) += transfers;
        latencies.append(&mut observed);
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();

    // Verify from scratch with linearizable scans.
    let mut total = 0u64;
    for name in &shared.names {
        let (mut sum, mut logged) = (0u64, 0u64);
        for (prefix, is_log) in [("acct/", false), ("log/", true)] {
            let mut after: Option<String> = None;
            loop {
                let cursor = after
                    .as_ref()
                    .map(|key| format!(",\"after\":\"{key}\""))
                    .unwrap_or_default();
                let body =
                    format!("{{\"scans\":[{{\"prefix\":\"{prefix}\",\"limit\":5000{cursor}}}]}}");
                let (status, page) = definite(&shared, 0, name, "read", &body).await?;
                let Some(items) = page["scans"][0]["items"].as_array() else {
                    bail!("verifying {name}: {status} {page}");
                };
                if is_log {
                    logged += items.len() as u64;
                } else {
                    sum += items
                        .iter()
                        .filter_map(|item| item["value"].as_u64())
                        .sum::<u64>();
                }
                match page["scans"][0]["next"].as_str() {
                    Some(next) => after = Some(next.to_owned()),
                    None => break,
                }
            }
        }
        let expected = acknowledged.get(name).copied().unwrap_or(0);
        if sum != accounts as u64 * OPENING_BALANCE {
            bail!(
                "LEDGER VIOLATION in {name}: balances sum to {sum}, not {}",
                accounts as u64 * OPENING_BALANCE
            );
        }
        if logged != expected {
            bail!(
                "LEDGER VIOLATION in {name}: {logged} transfers are stored but {expected} were acknowledged"
            );
        }
        total += expected;
    }
    if !args.keep && args.database.is_none() {
        for name in &shared.names {
            let _ = shared
                .http
                .delete(format!("{}/v1/databases/{name}", shared.urls[0]))
                .bearer_auth(&shared.token)
                .send()
                .await;
        }
    }
    println!(
        "ledger verified: {total} transfers ({:.0}/s), every balance and log entry accounted for; {} conflicts retried, {} requests repeated; p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
        total as f64 / elapsed,
        conflicts.load(Ordering::Relaxed),
        shared.errors.load(Ordering::Relaxed),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.99),
        percentile(&latencies, 1.0),
    );
    Ok(())
}
