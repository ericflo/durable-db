use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use durable_db::{
    api::{App, Cluster, ClusterConfig, Member, Membership, RateLimits, router},
    auth::Auth,
    cache::Cache,
    providers::b2::Native,
};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[derive(Parser)]
#[command(
    version,
    about = "Transactional ordered storage. One binary, disposable compute."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Export, inspect, or restore a consistent logical database archive.
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// Start a local development database or a stateless B2/S3-backed server.
    Serve(Box<Serve>),
    /// Mint a scoped token offline. Keep the signing key on the service side.
    Token {
        #[arg(long, default_value = "./data/admin.key")]
        key_file: PathBuf,
        #[arg(long)]
        database: Option<String>,
        #[arg(long)]
        write: bool,
        #[arg(long, default_value_t = 3600)]
        ttl_seconds: u64,
        #[arg(long, conflicts_with = "database")]
        admin: bool,
        /// Mint a credential that can only scrape GET /metrics.
        #[arg(long, conflicts_with_all = ["database", "admin", "write"])]
        metrics: bool,
        /// Current database policy incarnation (obtain from administrator info).
        #[arg(long, requires_all = ["database", "generation"], conflicts_with_all = ["admin", "metrics"])]
        incarnation: Option<String>,
        #[arg(long, requires = "incarnation")]
        generation: Option<u64>,
    },
    /// Generate a signing key file with restrictive permissions.
    Keygen {
        #[arg(long, default_value = "admin.key")]
        output: PathBuf,
    },
    /// Assemble a bounded rotation keyring from raw hex key files.
    Keyring {
        #[arg(long)]
        active_key_file: PathBuf,
        #[arg(long, action = clap::ArgAction::Append)]
        verify_key_file: Vec<PathBuf>,
        /// Accept the old administrator envelope, which exposes a signing key.
        #[arg(long)]
        allow_legacy_admin: bool,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(clap::Args)]
struct Serve {
    #[arg(long, default_value = "127.0.0.1:7654", env = "DURABLE_DB_LISTEN")]
    listen: SocketAddr,
    #[arg(long, default_value = "./data", env = "DURABLE_DB_DATA")]
    data: PathBuf,
    #[arg(long, env = "DURABLE_DB_BUCKET")]
    bucket: Option<String>,
    /// Use B2 native bucket revision publication with B2_APPLICATION_KEY_ID / B2_APPLICATION_KEY.
    #[arg(long, env = "DURABLE_DB_B2_BUCKET", conflicts_with = "bucket")]
    b2_bucket: Option<String>,
    #[arg(long, default_value = "durable-db", env = "DURABLE_DB_PREFIX")]
    prefix: String,
    /// Reachable cloud peer IP; defaults to a specific listen address.
    #[arg(long, env = "DURABLE_DB_ADVERTISE_IP")]
    advertise_ip: Option<IpAddr>,
    /// Headless DNS service; omit only for a single cloud process.
    #[arg(long, env = "DURABLE_DB_DISCOVERY_SERVICE")]
    discovery_service: Option<String>,
    /// Stable, unique cloud placement identity; defaults to the advertised address.
    #[arg(long, env = "DURABLE_DB_MEMBER_ID")]
    member_id: Option<String>,
    /// Fixed cloud partition count (default 16); cannot change on reopen.
    #[arg(long, env = "DURABLE_DB_PARTITIONS", value_parser = clap::value_parser!(u32).range(1..=1024))]
    partitions: Option<u32>,
    /// Aggregate native resident payload allowance, separate from cache/RSS (default 128 MiB).
    #[arg(long, env = "DURABLE_DB_RESIDENT_MEMORY_MIB", value_parser = clap::value_parser!(u32).range(16..=8192))]
    resident_memory_mib: Option<u32>,
    /// Initialize a fresh cloud namespace with this cumulative immutable-upload byte ceiling.
    #[arg(
        long,
        env = "DURABLE_DB_UPLOAD_BUDGET_BYTES",
        requires = "upload_budget_attempts"
    )]
    upload_budget_bytes: Option<u64>,
    /// Cumulative immutable PUT attempt ceiling; authoritative once initialized.
    #[arg(
        long,
        env = "DURABLE_DB_UPLOAD_BUDGET_ATTEMPTS",
        requires = "upload_budget_bytes"
    )]
    upload_budget_attempts: Option<u64>,
    #[arg(long, env = "DURABLE_DB_ADMIN_KEY_FILE")]
    admin_key_file: Option<PathBuf>,
    #[arg(long, default_value_t = 64, env = "DURABLE_DB_MEMORY_CACHE_MIB")]
    memory_cache_mib: usize,
    #[arg(long, env = "DURABLE_DB_DISK_CACHE")]
    disk_cache: Option<PathBuf>,
    #[arg(long, default_value_t = 1024, env = "DURABLE_DB_DISK_CACHE_MIB")]
    disk_cache_mib: usize,
    /// Tenant HTTP slots per pod, plus two reserved administrator/metrics slots.
    #[arg(long, default_value_t = 8, env = "DURABLE_DB_CONCURRENCY")]
    concurrency: usize,
    /// Per-principal, per-pod budget refill. A commit costs 5 units; other requests cost 1.
    #[arg(long, default_value_t = RateLimits::DEFAULT_UNITS_PER_SECOND, env = "DURABLE_DB_REQUEST_UNITS_PER_SECOND")]
    request_units_per_second: u32,
    /// Maximum accumulated request units per principal, including the initial burst.
    #[arg(long, default_value_t = RateLimits::DEFAULT_BURST_UNITS, env = "DURABLE_DB_REQUEST_BURST_UNITS")]
    request_burst_units: u32,
    #[arg(long, default_value_t = 60, env = "DURABLE_DB_MAINTENANCE_SECONDS")]
    maintenance_seconds: u64,
}

#[derive(Subcommand)]
enum BackupCommand {
    /// Export one revision without overwriting an existing file. Source writes may cause a conflict.
    Export {
        #[arg(long)]
        url: String,
        #[arg(long)]
        token_file: PathBuf,
        #[arg(long)]
        database: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Check every record, count, and checksum without contacting a server.
    Inspect {
        #[arg(long)]
        input: PathBuf,
    },
    /// Restore into a new protected destination, or resume the identical archive and destination.
    Restore {
        #[arg(long)]
        url: String,
        #[arg(long)]
        token_file: PathBuf,
        #[arg(long)]
        database: String,
        #[arg(long)]
        input: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "durable_db=info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Backup { command } => {
            use durable_db::backup;
            let result = match command {
                BackupCommand::Export {
                    url,
                    token_file,
                    database,
                    output,
                } => serde_json::to_value(
                    backup::export(&url, &token_file, &database, &output).await?,
                )?,
                BackupCommand::Inspect { input } => serde_json::to_value(backup::inspect(&input)?)?,
                BackupCommand::Restore {
                    url,
                    token_file,
                    database,
                    input,
                } => serde_json::to_value(
                    backup::restore(&url, &token_file, &database, &input).await?,
                )?,
            };
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Keygen { output } => {
            keygen(&output)?;
            println!("Created {}", output.display());
        }
        Command::Keyring {
            active_key_file,
            verify_key_file,
            allow_legacy_admin,
            output,
        } => {
            anyhow::ensure!(
                verify_key_file.len() <= 3,
                "at most three verification key files are allowed"
            );
            let active = read_key_file(&active_key_file)?;
            let verify = verify_key_file
                .iter()
                .map(|p| read_key_file(p))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let document = Auth::keyring_document(&active, verify, allow_legacy_admin)?;
            write_secret(&output, document.as_bytes())?;
            println!("Created {}", output.display());
        }
        Command::Token {
            key_file,
            database,
            write,
            ttl_seconds,
            admin,
            metrics,
            incarnation,
            generation,
        } => {
            let auth = Auth::new(&read_key_file(&key_file)?)?;
            if admin {
                println!("{}", auth.administrator_token(ttl_seconds)?);
            } else if metrics {
                println!("{}", auth.metrics_token(ttl_seconds)?);
            } else if let Some(incarnation) = incarnation {
                println!(
                    "{}",
                    auth.issue_for_policy(
                        database.context("--database required")?,
                        write,
                        ttl_seconds,
                        incarnation,
                        generation.context("--generation required")?
                    )?
                    .0
                );
            } else {
                println!(
                    "{}",
                    auth.issue(
                        database.context("--database, --admin, or --metrics required")?,
                        write,
                        ttl_seconds
                    )?
                );
            }
        }
        Command::Serve(args) => {
            let Serve {
                listen,
                data,
                bucket,
                b2_bucket,
                prefix,
                advertise_ip,
                discovery_service,
                member_id,
                partitions,
                resident_memory_mib,
                upload_budget_bytes,
                upload_budget_attempts,
                admin_key_file,
                memory_cache_mib,
                disk_cache,
                disk_cache_mib,
                concurrency,
                request_units_per_second,
                request_burst_units,
                maintenance_seconds,
            } = *args;
            if concurrency == 0
                || concurrency > 128
                || memory_cache_mib > 8192
                || disk_cache_mib > 1024 * 1024
                || !(1..=86400).contains(&maintenance_seconds)
            {
                bail!("invalid resource configuration");
            }
            let rate_limits = RateLimits::new(request_units_per_second, request_burst_units)?;
            let cluster = bucket.is_some() || b2_bucket.is_some();
            if !cluster
                && (advertise_ip.is_some()
                    || discovery_service.is_some()
                    || member_id.is_some()
                    || partitions.is_some()
                    || resident_memory_mib.is_some())
            {
                bail!("native cluster settings require B2/S3 storage");
            }
            if discovery_service.is_some() && listen.port() == 0 {
                bail!("DNS discovery requires a fixed listen port");
            }
            let upload_limits = match (upload_budget_bytes, upload_budget_attempts) {
                (Some(bytes), Some(attempts)) if cluster => {
                    let limits = durable_db::upload_budget::UploadLimits { bytes, attempts };
                    limits.validate()?;
                    Some(limits)
                }
                (None, None) if !cluster => None,
                (None, None) => bail!(
                    "cloud serving requires --upload-budget-bytes and --upload-budget-attempts"
                ),
                _ => bail!("upload budgets require both ceilings and B2/S3 storage"),
            };
            let key_file = match admin_key_file {
                Some(p) => p,
                None if cluster => bail!(
                    "cluster mode requires --admin-key-file; mount the same secret into every pod"
                ),
                None => {
                    std::fs::create_dir_all(&data)?;
                    let p = data.join("admin.key");
                    if !p.exists() {
                        keygen(&p)?;
                    }
                    p
                }
            };
            let auth = Arc::new(Auth::new(&read_key_file(&key_file)?)?);
            let cache = Arc::new(Cache::new(
                memory_cache_mib * 1024 * 1024,
                disk_cache.map(|p| (p, disk_cache_mib * 1024 * 1024)),
            )?);
            let listener = tokio::net::TcpListener::bind(listen).await?;
            let listen = listener.local_addr()?;
            let mut member = None;
            let (backend, membership): (Arc<dyn durable_db::api::Backend>, Option<Membership>) =
                if cluster {
                    let ip = advertise_ip.unwrap_or(listen.ip());
                    anyhow::ensure!(
                        !ip.is_unspecified(),
                        "cloud wildcard listeners require --advertise-ip"
                    );
                    anyhow::ensure!(
                        ip.is_ipv4() == listen.is_ipv4()
                            && (listen.ip().is_unspecified() || listen.ip() == ip),
                        "advertised IP must match the listen address/family"
                    );
                    let address = SocketAddr::new(ip, listen.port());
                    let mut limits = durable_db::transactions::live::Limits::resident(
                        resident_memory_mib.unwrap_or(128) as usize * 1024 * 1024,
                    )?;
                    limits.receipt_window = 128;
                    let config = ClusterConfig {
                        layout: durable_db::providers::partitions::Layout::new(
                            partitions.unwrap_or(16),
                        )?,
                        address,
                        limits,
                        capacity: concurrency,
                        maintenance: Duration::from_secs(maintenance_seconds),
                        // Shared namespace admission is the effective operator ceiling.
                        // Internal partition ledgers must not become unrefillable lower ceilings.
                        upload_limits: durable_db::upload_budget::UploadLimits {
                            bytes: (1 << 53) - 1,
                            attempts: (1 << 53) - 1,
                        },
                    };
                    config.validate()?;
                    let identity = Member {
                        id: member_id.unwrap_or_else(|| address.to_string()),
                        address,
                    };
                    let membership = Membership::start(identity.clone(), discovery_service)?;
                    member = Some(identity);
                    let connection = if let Some(bucket) = b2_bucket {
                        Native::connect_storage(&bucket, &prefix)
                            .await
                            .context("initialize B2 transport")?
                    } else {
                        durable_db::providers::s3::S3::connect_storage(
                            &bucket.expect("cloud provider selected"),
                            &prefix,
                        )
                        .await?
                    };
                    let directory = connection
                        .into_partitions(
                            upload_limits.expect("cloud ceilings validated"),
                            cache.clone(),
                        )
                        .await?;
                    let backend = Cluster::open(
                        directory,
                        config,
                        cache,
                        auth.clone(),
                        membership.subscribe(),
                    )
                    .await?;
                    (backend, Some(membership))
                } else {
                    (
                        Arc::new(
                            durable_db::api::open_local(
                                data.join("store"),
                                cache,
                                concurrency,
                                Duration::from_secs(maintenance_seconds),
                            )
                            .await?,
                        ),
                        None,
                    )
                };
            let mut app = App::with_rate_limits(backend, auth, concurrency, rate_limits);
            if let Some(member) = member {
                app = app.with_member(&member)?;
            }
            let app = Arc::new(app);
            tracing::info!(address = %listen, cluster, "durable-db HTTP listener started");
            serve(listener, app, shutdown(), membership).await?;
        }
    }
    Ok(())
}
async fn serve(
    listener: tokio::net::TcpListener,
    app: Arc<App>,
    signal: impl std::future::Future<Output = ()> + Send + 'static,
    membership: Option<Membership>,
) -> std::io::Result<()> {
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let http = axum::serve(listener, router(app.clone())).with_graceful_shutdown(async {
        let _ = stopped.await;
    });
    let withdrawal = async move {
        signal.await;
        app.ready.store(false, Ordering::Relaxed);
        // Stop acceptance and idle keep-alives before provider I/O can stall.
        // Admitted requests and best-effort route withdrawal drain together.
        let _ = stop.send(());
        tracing::info!("draining requests");
        if tokio::time::timeout(Duration::from_secs(5), app.backend.drain())
            .await
            .is_err()
        {
            tracing::warn!("routing withdrawal deadline reached");
        }
        drop(membership);
        Ok(())
    };
    // Keep withdrawal alive even when HTTP has no remaining requests, and
    // admitted requests alive if withdrawal completes or reaches its bound.
    tokio::try_join!(async { http.await }, withdrawal)?;
    Ok(())
}

fn keygen(path: &Path) -> anyhow::Result<()> {
    let key: [u8; 32] = rand::random();
    write_secret(path, format!("{}\n", hex::encode(key)).as_bytes())
}
fn read_key_file(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;
    let file = std::fs::File::open(path).context("open signing-key file")?;
    let mut contents = String::new();
    file.take(16385)
        .read_to_string(&mut contents)
        .context("read signing-key file")?;
    anyhow::ensure!(contents.len() <= 16384, "signing-key file exceeds 16 KiB");
    Ok(contents)
}
fn write_secret(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_db::{
        api::Backend,
        auth::Access,
        engine::{ReadRequest, ReadResult},
    };
    use std::sync::atomic::AtomicBool;
    use tokio::{net::TcpStream, sync::Notify};

    #[derive(Default)]
    struct HeldBackend {
        read_entered: Notify,
        read_release: Notify,
        drain_entered: Notify,
        drain_release: Notify,
        drain_stopped: AtomicBool,
        drain_finished: AtomicBool,
    }
    struct Stopped<'a>(&'a AtomicBool);
    impl Drop for Stopped<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    #[async_trait::async_trait]
    impl Backend for HeldBackend {
        async fn memory_bytes(&self) -> usize {
            0
        }
        async fn read(
            &self,
            _: &str,
            _: &ReadRequest,
            _: &Access,
        ) -> durable_db::Result<ReadResult> {
            self.read_entered.notify_one();
            self.read_release.notified().await;
            Err(durable_db::Error::NotFound)
        }
        async fn drain(&self) {
            let _stopped = Stopped(&self.drain_stopped);
            self.drain_entered.notify_one();
            self.drain_release.notified().await;
            self.drain_finished.store(true, Ordering::Relaxed);
        }
    }

    async fn shutdown_case(expire_withdrawal: bool) {
        let backend = Arc::new(HeldBackend::default());
        let auth = Arc::new(Auth::new(&"ab".repeat(32)).unwrap());
        let token = auth.issue("app".into(), false, 3600).unwrap();
        let app = Arc::new(App::new(backend.clone(), auth, 4));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let origin = format!("http://{address}");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            app.clone(),
            async { stopped.await.unwrap() },
            None,
        ));
        let client = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        // Keep an idle pooled connection as well as an admitted request alive.
        assert_eq!(
            client
                .get(format!("{origin}/health/live"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
        let request = client
            .post(format!("{origin}/v1/databases/app/read"))
            .bearer_auth(&token)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({"keys":[]}));
        let pending = tokio::spawn(async move { request.send().await });
        tokio::time::timeout(Duration::from_secs(1), backend.read_entered.notified())
            .await
            .unwrap();
        // Warm a second connection while the first request is held.
        client
            .get(format!("{origin}/health/live"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        stop.send(()).unwrap();
        backend.drain_entered.notified().await;
        assert!(!app.ready.load(Ordering::Relaxed));
        tokio::time::timeout(Duration::from_millis(250), async {
            while TcpStream::connect(address).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("listener must close while route withdrawal is still held");
        assert!(!backend.drain_stopped.load(Ordering::Relaxed));
        assert!(!server.is_finished());
        // The existing idle connection must also close, not keep returning 429.
        assert!(
            client
                .post(format!("{origin}/v1/databases/app/read"))
                .bearer_auth(&token)
                .json(&serde_json::json!({"keys":[]}))
                .send()
                .await
                .is_err()
        );
        if expire_withdrawal {
            // Advance only after real socket shutdown, then resume before I/O.
            // This must cancel withdrawal, not the still-admitted request.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(5)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(1), async {
                while !backend.drain_stopped.load(Ordering::Relaxed) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!backend.drain_finished.load(Ordering::Relaxed));
            assert!(!server.is_finished());
        }
        backend.read_release.notify_one();
        assert_eq!(
            pending.await.unwrap().unwrap().status(),
            reqwest::StatusCode::NOT_FOUND
        );
        if !expire_withdrawal {
            assert!(
                !server.is_finished(),
                "HTTP completion must not drop withdrawal"
            );
            backend.drain_release.notify_one();
        }
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(backend.drain_stopped.load(Ordering::Relaxed));
        assert_eq!(
            backend.drain_finished.load(Ordering::Relaxed),
            !expire_withdrawal
        );
    }

    #[tokio::test]
    async fn shutdown_closes_http_before_withdrawal_and_waits_for_both_drains() {
        shutdown_case(false).await;
    }

    #[tokio::test]
    async fn shutdown_bounds_withdrawal_without_cancelling_admitted_requests() {
        shutdown_case(true).await;
    }
}
