use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use durable_db::{
    api::{App, router},
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
    },
    /// Generate a signing key file with restrictive permissions.
    Keygen {
        #[arg(long, default_value = "admin.key")]
        output: PathBuf,
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
        Command::Keygen { output } => {
            keygen(&output)?;
            println!("Created {}", output.display());
        }
        Command::Token {
            key_file,
            database,
            write,
            ttl_seconds,
            admin,
        } => {
            let auth = Auth::new(&std::fs::read_to_string(key_file)?)?;
            if admin {
                println!("{}", auth.admin_token());
            } else {
                println!(
                    "{}",
                    auth.issue(
                        database.context("--database or --admin required")?,
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
            admin_key_file,
            memory_cache_mib,
            disk_cache,
            disk_cache_mib,
            concurrency,
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
            let cluster = bucket.is_some() || b2_bucket.is_some();
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
            let auth = Auth::new(&std::fs::read_to_string(&key_file).context("read signing key")?)?;
            let cache = Arc::new(Cache::new(
                memory_cache_mib * 1024 * 1024,
                disk_cache.map(|p| (p, disk_cache_mib * 1024 * 1024)),
            )?);
            let storage: Arc<dyn Storage> = if let Some(bucket) = b2_bucket {
                Arc::new(
                    Native::connect(&bucket, &prefix)
                        .await
                        .context("initialize B2 authority")?
                        .with_cache(cache.clone()),
                )
            } else if let Some(bucket) = bucket {
                Arc::new(
                    durable_db::providers::s3::S3::connect(&bucket, &prefix)
                        .await?
                        .with_cache(cache.clone()),
                )
            } else {
                let local = Local::open(data.join("store"))?;
                local.cleanup_temps()?;
                Arc::new(local)
            };
            let engine = Arc::new(Engine::new(storage, cache));
            let app = Arc::new(App::new(engine, auth, concurrency));
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
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    let key: [u8; 32] = rand::random();
    writeln!(file, "{}", hex::encode(key))?;
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
        if app.engine.maintain_storage().await.is_err() {
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
                    if app.engine.compact(&db).await.is_err() {
                        tracing::warn!("compaction deferred; will retry on a later sweep");
                    }
                    if app.engine.gc(&db).await.is_err() {
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
