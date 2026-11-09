use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use durable_db::{
    api::{App, RateLimits, router},
    auth::Auth,
    cache::Cache,
    engine::Engine,
    providers::b2::Native,
    storage::{Local, Storage},
};
use std::{
    net::SocketAddr,
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
    Serve {
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
    },
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
        Command::Serve {
            listen,
            data,
            bucket,
            b2_bucket,
            prefix,
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
        } => {
            if concurrency == 0
                || concurrency > 128
                || memory_cache_mib > 8192
                || disk_cache_mib > 1024 * 1024
                || maintenance_seconds == 0
            {
                bail!("invalid resource configuration");
            }
            let rate_limits = RateLimits::new(request_units_per_second, request_burst_units)?;
            let cluster = bucket.is_some() || b2_bucket.is_some();
            let upload_limits = match (upload_budget_bytes, upload_budget_attempts) {
                (Some(bytes), Some(attempts)) if cluster => {
                    let limits = durable_db::upload_budget::UploadLimits { bytes, attempts };
                    limits.validate()?;
                    Some(limits)
                }
                (None, None) => None,
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
            let auth = Auth::new(&read_key_file(&key_file)?)?;
            let cache = Arc::new(Cache::new(
                memory_cache_mib * 1024 * 1024,
                disk_cache.map(|p| (p, disk_cache_mib * 1024 * 1024)),
            )?);
            let storage: Arc<dyn Storage> = if let Some(bucket) = b2_bucket {
                Arc::new(
                    Native::connect_with_budget(&bucket, &prefix, upload_limits)
                        .await
                        .context("initialize B2 authority")?
                        .with_cache(cache.clone()),
                )
            } else if let Some(bucket) = bucket {
                Arc::new(
                    durable_db::providers::s3::S3::connect_with_budget(
                        &bucket,
                        &prefix,
                        upload_limits,
                    )
                    .await?
                    .with_cache(cache.clone()),
                )
            } else {
                let local = Local::open(data.join("store"))?;
                local.cleanup_temps()?;
                Arc::new(local)
            };
            let engine = Arc::new(Engine::new(storage, cache));
            let app = Arc::new(App::with_rate_limits(
                engine,
                auth,
                concurrency,
                rate_limits,
            ));
            let listener = tokio::net::TcpListener::bind(listen).await?;
            tracing::info!(address = %listener.local_addr()?, cluster, "durable-db ready");
            let maintenance = tokio::spawn(maintain(app.clone(), maintenance_seconds));
            let shutdown_app = app.clone();
            axum::serve(listener, router(app))
                .with_graceful_shutdown(async move {
                    shutdown().await;
                    shutdown_app.ready.store(false, Ordering::Relaxed);
                    tracing::info!("draining requests");
                })
                .await?;
            maintenance.abort();
        }
    }
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
async fn maintain(app: Arc<App>, seconds: u64) {
    let mut after = None;
    loop {
        // Jitter prevents every replica from fencing the same database simultaneously.
        tokio::time::sleep(Duration::from_millis(
            seconds.saturating_mul(1000) + rand::random::<u64>() % 1000,
        ))
        .await;
        if !app.ready.load(Ordering::Relaxed) {
            return;
        }
        let Ok(_permit) = app.permits.clone().try_acquire_owned() else {
            continue;
        };
        if durable_db::metrics::measure(
            durable_db::metrics::Operation::StorageMaintenance,
            app.engine.maintain_storage(),
        )
        .await
        .is_err()
        {
            tracing::warn!("provider maintenance deferred; will retry on a later sweep");
        }
        match app.engine.databases(after.as_deref()).await {
            Ok(dbs) => {
                after = if dbs.len() == 64 {
                    dbs.last().map(|d| format!("heads/{d}.json"))
                } else {
                    None
                };
                for db in dbs {
                    if durable_db::metrics::measure(
                        durable_db::metrics::Operation::DatabaseMaintenance,
                        app.engine.maintain_database(&db),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!("maintenance deferred; will retry on a later sweep");
                    }
                }
            }
            Err(_) => tracing::warn!("maintenance listing unavailable"),
        }
    }
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
