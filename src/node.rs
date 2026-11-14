//! One server process: the databases it currently owns and the limits that
//! keep any one of them from exhausting it.
//!
//! Nothing here is authoritative. A node that forgets everything, or is
//! replaced mid-request, loses only cached bytes and time.
use crate::{
    Error, Result,
    db::{Committed, Ctx, Db, Info, ReadReq, ReadResp, Txn},
    layout::{DbConfig, Layout, valid_name},
    metrics::Metrics,
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Limits {
    /// Databases kept open; the least recently used beyond this are released.
    pub max_open: usize,
    /// Release a database nobody has used for this long.
    pub idle_close: Duration,
    /// Unflushed bytes across all open databases before the largest are flushed.
    pub mem_bytes: usize,
    /// Requests per second per database unless its configuration says otherwise. Zero is unlimited.
    pub requests_per_second: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_open: 4096,
            idle_close: Duration::from_secs(300),
            mem_bytes: 256 << 20,
            requests_per_second: 0,
        }
    }
}

struct Bucket {
    tokens: f64,
    at: Instant,
}

pub struct Node {
    pub ctx: Arc<Ctx>,
    pub limits: Limits,
    pub metrics: Arc<Metrics>,
    open: RwLock<HashMap<String, Arc<Db>>>,
    opening: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl Node {
    pub fn new(ctx: Arc<Ctx>, limits: Limits, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            limits,
            metrics,
            open: RwLock::default(),
            opening: Mutex::default(),
            buckets: Mutex::default(),
        })
    }

    /// Whether this node is serving `name` right now.
    pub fn serving(&self, name: &str) -> bool {
        self.open.read().get(name).is_some_and(|db| !db.is_closed())
    }

    pub fn open_count(&self) -> usize {
        self.open.read().len()
    }

    fn current(&self, name: &str) -> Option<Arc<Db>> {
        self.open
            .read()
            .get(name)
            .filter(|db| !db.is_closed())
            .cloned()
    }

    /// The open database, taking ownership of it first if necessary.
    pub async fn db(&self, name: &str) -> Result<Arc<Db>> {
        self.acquire(name, None).await.map(|(db, _)| db)
    }

    async fn acquire(&self, name: &str, create: Option<DbConfig>) -> Result<(Arc<Db>, bool)> {
        if let Some(db) = self.current(name) {
            return Ok((db, false));
        }
        if !valid_name(name) {
            return Err(Error::Invalid(
                "a database name is 1-128 characters of letters, digits, '.', '_' and '-'".into(),
            ));
        }
        // One activation per database at a time on this node.
        let gate = self
            .opening
            .lock()
            .entry(name.to_owned())
            .or_default()
            .clone();
        let _turn = gate.lock().await;
        if let Some(db) = self.current(name) {
            return Ok((db, false));
        }
        let started = Instant::now();
        let mut attempt = 0;
        let opened = loop {
            match Db::open(self.ctx.clone(), name, create.clone()).await {
                // Another node is claiming at this moment. Whoever holds the
                // higher claim proceeds, so a short wait settles it.
                Err(Error::Busy(_)) if attempt < 4 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(15 << attempt)).await;
                }
                other => break other,
            }
        };
        self.opening.lock().remove(name);
        let (db, created) = opened?;
        self.metrics.opens.observe(started.elapsed());
        self.open.write().insert(name.to_owned(), db.clone());
        Ok((db, created))
    }

    /// Create a database, or confirm that it exists with this configuration.
    pub async fn create(&self, name: &str, config: DbConfig) -> Result<(Info, bool)> {
        let (db, created) = self.acquire(name, Some(config.clone())).await?;
        if !created {
            // Existence is a fact about the bucket, not about this node.
            db.check().await?;
            let existing = db.config();
            if existing.max_bytes != config.max_bytes
                || existing.max_requests_per_second != config.max_requests_per_second
            {
                return Err(Error::Exists);
            }
        }
        Ok((db.info(), created))
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let db = self.db(name).await?;
        let result = db.destroy().await;
        self.open.write().remove(name);
        self.buckets.lock().remove(name);
        result
    }

    /// Names of existing databases after `after`, ascending.
    pub async fn list(&self, after: Option<&str>, limit: usize) -> Result<Vec<String>> {
        let store = &*self.ctx.io.store;
        let prefix = Layout::databases(&self.ctx.root);
        let mut names = Vec::new();
        let mut after = after.map(str::to_owned);
        while names.len() < limit {
            let page =
                crate::io::retry(|| store.list_dirs(&prefix, after.as_deref(), limit)).await?;
            let Some(last) = page.last() else { break };
            after = Some(last.clone());
            // A deleted database leaves only its tombstone behind.
            let alive = futures::future::try_join_all(page.iter().map(|name| {
                let tombstone = Layout::new(&self.ctx.root, name).tombstone();
                async move {
                    crate::io::retry(|| store.exists(&tombstone))
                        .await
                        .map(|gone| !gone)
                }
            }))
            .await?;
            let complete = page.len() < limit;
            names.extend(
                page.into_iter()
                    .zip(alive)
                    .filter(|(_, alive)| *alive)
                    .map(|(name, _)| name),
            );
            if complete {
                break;
            }
        }
        names.truncate(limit);
        Ok(names)
    }

    /// Spend one request from the database's budget.
    pub fn admit(&self, name: &str, config: &DbConfig) -> Result<()> {
        let rate = match config.max_requests_per_second {
            0 => self.limits.requests_per_second,
            rate => rate,
        };
        if rate == 0 {
            return Ok(());
        }
        let rate = f64::from(rate);
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        let bucket = buckets.entry(name.to_owned()).or_insert(Bucket {
            tokens: rate,
            at: now,
        });
        bucket.tokens =
            (bucket.tokens + now.duration_since(bucket.at).as_secs_f64() * rate).min(rate);
        bucket.at = now;
        if bucket.tokens < 1.0 {
            return Err(Error::RateLimited);
        }
        bucket.tokens -= 1.0;
        Ok(())
    }

    /// Run `op` against the database. If ownership moved underneath it and
    /// the operation is safe to repeat, take the database back and repeat.
    async fn with_db<T, F, Fut>(&self, name: &str, repeatable: bool, op: F) -> Result<T>
    where
        F: Fn(Arc<Db>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        loop {
            let db = self.db(name).await?;
            match op(db).await {
                Err(Error::Lost(_) | Error::Busy(_)) if repeatable && attempt < 3 => attempt += 1,
                other => return other,
            }
        }
    }

    pub async fn read(&self, name: &str, request: ReadReq) -> Result<ReadResp> {
        self.with_db(name, true, |db| {
            let request = request.clone();
            async move { db.read(request).await }
        })
        .await
    }

    pub async fn commit(&self, name: &str, txns: Vec<Txn>) -> Result<Vec<Result<Committed>>> {
        // Repeating a commit whose outcome is unknown is safe only when
        // every transaction can be recognized as already applied.
        let repeatable = txns.iter().all(|txn| txn.idempotency_key.is_some());
        self.with_db(name, repeatable, |db| {
            let txns = txns.clone();
            async move { db.commit(txns).await }
        })
        .await
    }

    pub async fn info(&self, name: &str) -> Result<Info> {
        self.with_db(name, true, |db| async move {
            db.check().await?;
            Ok(db.info())
        })
        .await
    }

    pub async fn configure(&self, name: &str, change: impl Fn(&mut DbConfig)) -> Result<Info> {
        let db = self.db(name).await?;
        let mut config = db.config();
        change(&mut config);
        db.set_config(config).await?;
        Ok(db.info())
    }

    /// Release idle databases and keep unflushed memory within its budget.
    pub async fn tend(&self) {
        let mut open: Vec<Arc<Db>> = self.open.read().values().cloned().collect();
        self.open.write().retain(|_, db| !db.is_closed());
        open.retain(|db| !db.is_closed());
        open.sort_by_key(|db| std::cmp::Reverse(db.idle_for()));
        let surplus = open.len().saturating_sub(self.limits.max_open);
        let mut released = Vec::new();
        for (i, db) in open.iter().enumerate() {
            if i < surplus || db.idle_for() >= self.limits.idle_close {
                self.open.write().remove(&db.name);
                released.push(db.clone());
            }
        }
        futures::future::join_all(released.iter().map(|db| db.close())).await;
        let mut sizes: Vec<(usize, &Arc<Db>)> =
            open.iter().map(|db| (db.mem_bytes(), db)).collect();
        let mut total: usize = sizes.iter().map(|(bytes, _)| bytes).sum();
        sizes.sort_by_key(|(bytes, _)| std::cmp::Reverse(*bytes));
        for (bytes, db) in sizes {
            if total <= self.limits.mem_bytes {
                break;
            }
            db.request_flush();
            total -= bytes;
        }
    }

    /// Hand every database back so successors open without recovery.
    pub async fn shutdown(&self) {
        let open: Vec<Arc<Db>> = self.open.write().drain().map(|(_, db)| db).collect();
        futures::future::join_all(open.iter().map(|db| db.close())).await;
    }

    /// Release one database, for a planned move to another node.
    pub async fn release(&self, name: &str) {
        let db = self.open.write().remove(name);
        if let Some(db) = db {
            db.close().await;
        }
    }

    pub fn open_names(&self) -> Vec<String> {
        self.open.read().keys().cloned().collect()
    }
}
