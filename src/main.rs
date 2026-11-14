use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use durable_db::{
    auth::{Access, Grant, Keys, expiry},
    bench,
    cluster::Cluster,
    db::{Ctx, Options},
    http::{ApiLimits, App, router},
    io::{DiskCache, Io},
    metrics::{Metered, Metrics},
    node::{Limits, Node},
    store::{
        ObjectStore,
        fs::FsStore,
        s3::{S3Config, S3Store},
    },
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

/// A transactional ordered database whose source of truth is a bucket.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a server.
    Serve(Box<Serve>),
    /// Mint a bearer token from the signing key.
    Token(Token),
    /// Create a database and print a write token for it.
    Create(Create),
    /// Delete a database for good.
    Delete(Remote),
    /// List databases.
    List(Client),
    /// Measure a running server.
    Bench(bench::Bench),
}

#[derive(Args)]
struct Serve {
    /// Address to listen on.
    #[arg(long, env = "DURABLE_DB_LISTEN", default_value = "127.0.0.1:7654")]
    listen: SocketAddr,
    /// Store data in this directory. The default when no bucket is given.
    #[arg(long, env = "DURABLE_DB_DATA", conflicts_with = "bucket")]
    data: Option<PathBuf>,
    /// Store data in this S3-compatible bucket.
    #[arg(long, env = "DURABLE_DB_BUCKET", requires = "endpoint")]
    bucket: Option<String>,
    /// The bucket's endpoint, such as https://s3.us-west-002.backblazeb2.com.
    #[arg(long, env = "DURABLE_DB_ENDPOINT")]
    endpoint: Option<String>,
    /// The bucket's region. Derived from a Backblaze or AWS endpoint when omitted.
    #[arg(long, env = "DURABLE_DB_REGION")]
    region: Option<String>,
    /// Key prefix inside the bucket, so one bucket can hold several deployments.
    #[arg(long, env = "DURABLE_DB_PREFIX", default_value = "")]
    prefix: String,
    /// File holding the token signing key. Created if missing in directory mode.
    #[arg(long, env = "DURABLE_DB_KEY_FILE")]
    key_file: Option<PathBuf>,
    /// Every server of the deployment: a DNS name that resolves to all of them (a Kubernetes
    /// headless Service), or a comma-separated list of host:port addresses.
    #[arg(long, env = "DURABLE_DB_PEERS", requires = "advertise")]
    peers: Option<String>,
    /// This server's address as its peers reach it, such as $(POD_IP):7654.
    #[arg(long, env = "DURABLE_DB_ADVERTISE")]
    advertise: Option<String>,
    /// Memory for cached table blocks, in bytes.
    #[arg(long, env = "DURABLE_DB_CACHE_BYTES", default_value_t = 256 << 20)]
    cache_bytes: u64,
    /// Directory for a disk cache of table objects. Disposable; useful with a bucket.
    #[arg(long, env = "DURABLE_DB_CACHE_DIR")]
    cache_dir: Option<PathBuf>,
    /// Bytes the disk cache may hold.
    #[arg(long, env = "DURABLE_DB_CACHE_DIR_BYTES", default_value_t = 4 << 30)]
    cache_dir_bytes: u64,
    /// Memory for writes not yet flushed to tables, across all databases.
    #[arg(long, env = "DURABLE_DB_MEMTABLE_BYTES", default_value_t = 256 << 20)]
    memtable_bytes: usize,
    /// Databases kept open at once.
    #[arg(long, env = "DURABLE_DB_MAX_OPEN", default_value_t = 4096)]
    max_open: usize,
    /// Seconds a database stays open without requests.
    #[arg(long, env = "DURABLE_DB_IDLE_SECONDS", default_value_t = 300)]
    idle_seconds: u64,
    /// Requests per second allowed per database, unless it sets its own. 0 is unlimited.
    #[arg(long, env = "DURABLE_DB_REQUESTS_PER_SECOND", default_value_t = 0)]
    requests_per_second: u32,
    /// Largest value accepted, in bytes.
    #[arg(long, env = "DURABLE_DB_MAX_VALUE_BYTES", default_value_t = 4 << 20)]
    max_value_bytes: usize,
    /// Do not flush directory writes to disk. Faster; a power loss can lose acknowledged writes.
    #[arg(long, env = "DURABLE_DB_NO_FSYNC")]
    no_fsync: bool,
    /// Use S3 PutObject even on Backblaze, instead of B2's faster upload call.
    #[arg(long, env = "DURABLE_DB_S3_UPLOADS_ONLY")]
    s3_uploads_only: bool,
    /// Identical uploads that may race for one object in a bucket; the first to finish wins.
    #[arg(long, env = "DURABLE_DB_UPLOAD_ATTEMPTS", default_value_t = 2)]
    upload_attempts: usize,
    /// Milliseconds before each further racing upload starts; 0 starts them together.
    /// Default: 0 on Backblaze, where uploads are free and long-tailed, otherwise 150.
    #[arg(long, env = "DURABLE_DB_HEDGE_MS")]
    hedge_ms: Option<u64>,
}

#[derive(Args)]
struct KeyFile {
    /// File holding the token signing key.
    #[arg(long, env = "DURABLE_DB_KEY_FILE", default_value = "data/admin.key")]
    key_file: PathBuf,
}

#[derive(Clone, Copy, ValueEnum)]
enum TokenAccess {
    Read,
    Write,
}

#[derive(Args)]
struct Token {
    #[command(flatten)]
    key: KeyFile,
    /// Mint an administrator token.
    #[arg(long, conflicts_with = "database")]
    admin: bool,
    /// Mint a token for this database.
    #[arg(long, required_unless_present = "admin")]
    database: Option<String>,
    #[arg(long, value_enum, default_value = "write")]
    access: TokenAccess,
    /// Seconds until the token expires. 0 never expires.
    #[arg(long, default_value_t = 0)]
    ttl: u64,
}

#[derive(Args)]
struct Client {
    #[command(flatten)]
    key: KeyFile,
    /// The server to talk to.
    #[arg(long, env = "DURABLE_DB_URL", default_value = "http://127.0.0.1:7654")]
    url: String,
}

#[derive(Args)]
struct Remote {
    name: String,
    #[command(flatten)]
    client: Client,
}

#[derive(Args)]
struct Create {
    name: String,
    #[command(flatten)]
    client: Client,
    /// Storage quota in bytes.
    #[arg(long)]
    max_bytes: Option<u64>,
}

fn region_of(endpoint: &str) -> Option<String> {
    // s3.<region>.backblazeb2.com and s3.<region>.amazonaws.com
    let host = endpoint.split_once("://")?.1;
    let mut parts = host.split('.');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("s3"), Some(region), Some("backblazeb2" | "amazonaws")) => Some(region.to_owned()),
        _ => None,
    }
}

fn credential(names: &[&str]) -> anyhow::Result<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .with_context(|| format!("set {}", names.join(" or ")))
}

async fn serve(args: Serve) -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::default());
    let mut backblaze = false;
    let (store, key_file, described): (Arc<dyn ObjectStore>, PathBuf, String) = match &args.bucket {
        Some(bucket) => {
            let endpoint = args
                .endpoint
                .clone()
                .expect("clap requires an endpoint with a bucket");
            let region = args
                .region
                .clone()
                .or_else(|| region_of(&endpoint))
                .context("pass --region: it cannot be derived from this endpoint")?;
            let store = S3Store::new(S3Config {
                endpoint: endpoint.clone(),
                region,
                bucket: bucket.clone(),
                access_key_id: credential(&["AWS_ACCESS_KEY_ID", "B2_APPLICATION_KEY_ID"])?,
                secret_access_key: credential(&["AWS_SECRET_ACCESS_KEY", "B2_APPLICATION_KEY"])?,
            })?;
            backblaze = store.is_backblaze();
            let store = if backblaze && !args.s3_uploads_only {
                store.with_b2_uploads()
            } else {
                store
            };
            let key_file = args.key_file.clone().context(
                "pass --key-file: every server of a deployment shares one signing key (create it with `openssl rand -hex 32`)",
            )?;
            (
                Arc::new(store),
                key_file,
                format!("bucket {bucket} at {endpoint}"),
            )
        }
        None => {
            let dir = args.data.clone().unwrap_or_else(|| PathBuf::from("data"));
            let store = FsStore::open(&dir, !args.no_fsync)?;
            let key_file = args
                .key_file
                .clone()
                .unwrap_or_else(|| dir.join("admin.key"));
            (
                Arc::new(store),
                key_file,
                format!("directory {}", dir.display()),
            )
        }
    };
    let keys = if args.bucket.is_some() {
        Keys::load(&key_file)?
    } else {
        Keys::load_or_create(&key_file)?
    };
    let store: Arc<dyn ObjectStore> = Arc::new(Metered {
        inner: store,
        metrics: metrics.clone(),
    });

    // Refuse to start on storage that does not behave as the protocol needs.
    probe(&*store, &args.prefix)
        .await
        .context("the storage check failed")?;

    let disk = match &args.cache_dir {
        Some(dir) => Some(
            DiskCache::open(dir, args.cache_dir_bytes)
                .with_context(|| format!("cannot use the cache directory {}", dir.display()))?,
        ),
        None => None,
    };
    let mut prefix = args.prefix.trim_matches('/').to_owned();
    if !prefix.is_empty() {
        prefix.push('/');
    }
    let ctx = Arc::new(Ctx {
        io: Io::new(store, args.cache_bytes, disk),
        opts: Options {
            node: args.advertise.clone().unwrap_or_default(),
            upload_attempts: if args.bucket.is_some() {
                args.upload_attempts
            } else {
                1
            },
            hedge_after: Duration::from_millis(args.hedge_ms.unwrap_or(if backblaze {
                0
            } else {
                150
            })),
            ..Options::default()
        },
        root: prefix,
    });
    let limits = Limits {
        max_open: args.max_open,
        idle_close: Duration::from_secs(args.idle_seconds),
        mem_bytes: args.memtable_bytes,
        requests_per_second: args.requests_per_second,
    };
    let node = Node::new(ctx, limits, metrics);
    let cluster = Arc::new(Cluster::new(args.advertise.clone()));
    let api = ApiLimits {
        max_value: args.max_value_bytes,
        ..ApiLimits::default()
    };
    let app = App::new(node.clone(), keys, cluster.clone(), api);

    // Housekeeping: release idle databases, bound memory, follow membership.
    let housekeeping = {
        let (node, cluster, peers, port) = (
            node.clone(),
            cluster.clone(),
            args.peers.clone(),
            args.listen.port(),
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                if let Some(peers) = &peers {
                    // A list of addresses is used as given; a single name is
                    // resolved each tick, which is how a Kubernetes headless
                    // Service reports the pods that are currently ready.
                    if peers.contains(',') || peers.parse::<SocketAddr>().is_ok() {
                        cluster.set_peers(
                            peers
                                .split(',')
                                .map(|peer| peer.trim().to_owned())
                                .collect(),
                        );
                    } else {
                        cluster.resolve(peers, port).await;
                    }
                    // Hand over databases that now belong to another member,
                    // a few at a time, so a membership change is not a stampede.
                    let mut handed = 0;
                    for name in node.open_names() {
                        if handed == 16 {
                            break;
                        }
                        if let Some(peer) = cluster.owner(&name)
                            && cluster.alive(&peer).await
                        {
                            node.release(&name).await;
                            handed += 1;
                        }
                    }
                }
                node.tend().await;
            }
        })
    };

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("cannot listen on {}", args.listen))?;
    tracing::info!(listen = %args.listen, storage = %described, "durable-db {} is serving", env!("CARGO_PKG_VERSION"));
    axum::serve(listener, router(app))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Hand every database back so the next owner opens without recovery.
    housekeeping.abort();
    tracing::info!("releasing databases");
    node.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("signal handler installs");
        tokio::select! {
            _ = interrupt => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = interrupt.await;
}

/// Exercise each storage operation once and check read-after-write.
async fn probe(store: &dyn ObjectStore, prefix: &str) -> anyhow::Result<()> {
    let key = format!(
        "{}probe/{:016x}",
        prefix.trim_matches('/').to_owned() + if prefix.is_empty() { "" } else { "/" },
        rand::random::<u64>()
    );
    let body = bytes::Bytes::from_static(b"durable-db storage probe");
    store.put(&key, body.clone()).await?;
    if store.get(&key).await?.as_ref() != Some(&body) {
        bail!("an object just written could not be read back");
    }
    if store.get_range(&key, 8, 10).await?.as_deref() != Some(&b"db"[..]) {
        bail!("ranged reads return the wrong bytes");
    }
    let listed = store.list(&key, None, 10).await?;
    if listed.len() != 1 || listed[0].key != key {
        bail!("an object just written is missing from the listing");
    }
    store.delete(&key).await?;
    if store.exists(&key).await? || !store.list(&key, None, 10).await?.is_empty() {
        bail!("an object just deleted is still visible");
    }
    store
        .purge(&key, None, &|candidate| candidate == key)
        .await?;
    Ok(())
}

async fn admin_request(
    client: &Client,
    method: reqwest::Method,
    path: &str,
    body: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let token = Keys::load(&client.key.key_file)?.mint(&Grant::admin());
    let response = reqwest::Client::new()
        .request(
            method,
            format!("{}{path}", client.url.trim_end_matches('/')),
        )
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .with_context(|| format!("cannot reach {}", client.url))?;
    let status = response.status();
    let text = response.text().await?;
    let value: serde_json::Value =
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    if !status.is_success() {
        bail!(
            "{status}: {}",
            value["error"]["message"]
                .as_str()
                .unwrap_or(&value.to_string())
        );
    }
    Ok(value)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Serve(args) => serve(*args).await,
        Command::Token(args) => {
            let keys = Keys::load(&args.key.key_file)?;
            let grant = match args.database {
                None => Grant {
                    exp: expiry(args.ttl),
                    ..Grant::admin()
                },
                Some(db) => Grant {
                    db,
                    access: match args.access {
                        TokenAccess::Read => Access::Read,
                        TokenAccess::Write => Access::Write,
                    },
                    exp: expiry(args.ttl),
                    generation: 0,
                },
            };
            println!("{}", keys.mint(&grant));
            Ok(())
        }
        Command::Create(args) => {
            let mut body = serde_json::Map::new();
            if let Some(max_bytes) = args.max_bytes {
                body.insert("max_bytes".into(), max_bytes.into());
            }
            let path = format!("/v1/databases/{}", args.name);
            admin_request(&args.client, reqwest::Method::PUT, &path, body.into()).await?;
            let token = admin_request(
                &args.client,
                reqwest::Method::POST,
                &format!("{path}/tokens"),
                serde_json::json!({"access": "write"}),
            )
            .await?;
            println!(
                "{}",
                token["token"]
                    .as_str()
                    .context("the server returned no token")?
            );
            Ok(())
        }
        Command::Delete(args) => {
            let path = format!("/v1/databases/{}", args.name);
            admin_request(
                &args.client,
                reqwest::Method::DELETE,
                &path,
                serde_json::Value::Null,
            )
            .await?;
            Ok(())
        }
        Command::List(client) => {
            let mut after: Option<String> = None;
            loop {
                let path = match &after {
                    Some(after) => format!("/v1/databases?limit=1000&after={after}"),
                    None => "/v1/databases?limit=1000".to_owned(),
                };
                let page = admin_request(
                    &client,
                    reqwest::Method::GET,
                    &path,
                    serde_json::Value::Null,
                )
                .await?;
                for name in page["databases"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|n| n.as_str())
                {
                    println!("{name}");
                }
                match page["next"].as_str() {
                    Some(next) => after = Some(next.to_owned()),
                    None => return Ok(()),
                }
            }
        }
        Command::Bench(args) => bench::run(args).await,
    }
}
