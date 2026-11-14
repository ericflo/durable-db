//! Storage access shared by every open database: the object store, bounded
//! retries, and two caches of immutable table bytes.
//!
//! Memory holds decoded blocks. A directory, when configured, holds whole
//! table objects, so a restart or a cold block costs a local read instead of
//! a request to the bucket. Both are bounded and both are disposable: every
//! block carries a checksum, a damaged cache file is deleted and fetched
//! again, and nothing here is ever the only copy of anything.
use crate::{
    Error, Result,
    store::{ObjectStore, StoreError, StoreResult},
    table::Block,
};
use bytes::Bytes;
use parking_lot::Mutex;
use quick_cache::{Weighter, sync::Cache};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

#[derive(Clone)]
struct BlockWeight;

impl Weighter<(Arc<str>, u32), Arc<Block>> for BlockWeight {
    fn weight(&self, key: &(Arc<str>, u32), block: &Arc<Block>) -> u64 {
        (key.0.len() + block.weight()) as u64
    }
}

/// Tables larger than this are read from the bucket by range only.
const DISK_OBJECT_BYTES: u64 = 64 << 20;

pub struct DiskCache {
    dir: PathBuf,
    budget: u64,
    used: AtomicU64,
    counter: AtomicU64,
    /// Objects being downloaded in the background.
    filling: Mutex<HashSet<String>>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl DiskCache {
    pub fn open(dir: impl Into<PathBuf>, budget: u64) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut used = 0;
        for entry in fs::read_dir(&dir)?.flatten() {
            // Half-written files from an earlier run.
            if entry.file_name().to_string_lossy().starts_with("tmp-") {
                let _ = fs::remove_file(entry.path());
            } else if let Ok(meta) = entry.metadata() {
                used += meta.len();
            }
        }
        Ok(Self {
            dir,
            budget,
            used: AtomicU64::new(used),
            counter: AtomicU64::new(0),
            filling: Mutex::default(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir
            .join(hex::encode(&Sha256::digest(key.as_bytes())[..16]))
    }

    fn read(&self, key: &str, start: u64, end: u64) -> Option<Bytes> {
        let mut file = fs::File::open(self.path(key)).ok()?;
        let meta = file.metadata().ok()?;
        let end = end.min(meta.len());
        let start = start.min(end);
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut body = vec![0; (end - start) as usize];
        file.read_exact(&mut body).ok()?;
        // Eviction goes by modification time; refresh it now and then so
        // that objects in use outlive objects that are not.
        let stale = SystemTime::now() - Duration::from_secs(600);
        if meta.modified().is_ok_and(|at| at < stale) {
            let _ = file.set_modified(SystemTime::now());
        }
        Some(Bytes::from(body))
    }

    fn write(&self, key: &str, body: &[u8]) {
        let size = body.len() as u64;
        if size > self.budget.min(DISK_OBJECT_BYTES) {
            return;
        }
        let tmp = self.dir.join(format!(
            "tmp-{}-{}",
            std::process::id(),
            self.counter.fetch_add(1, Ordering::Relaxed)
        ));
        let stored = fs::File::create(&tmp)
            .and_then(|mut file| file.write_all(body))
            .and_then(|()| fs::rename(&tmp, self.path(key)));
        match stored {
            Ok(()) => {
                if self.used.fetch_add(size, Ordering::Relaxed) + size > self.budget {
                    self.trim();
                }
            }
            Err(_) => {
                let _ = fs::remove_file(&tmp);
            }
        }
    }

    fn remove(&self, key: &str) -> bool {
        let path = self.path(key);
        let size = fs::metadata(&path).map_or(0, |meta| meta.len());
        let removed = fs::remove_file(path).is_ok();
        if removed {
            self.used
                .fetch_sub(size.min(self.used()), Ordering::Relaxed);
        }
        removed
    }

    /// Delete the least recently used objects until the budget has headroom.
    fn trim(&self) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        let mut files: Vec<(SystemTime, u64, PathBuf)> = entries
            .flatten()
            .filter_map(|entry| {
                let meta = entry.metadata().ok()?;
                Some((meta.modified().ok()?, meta.len(), entry.path()))
            })
            .collect();
        files.sort();
        let mut used: u64 = files.iter().map(|(_, size, _)| size).sum();
        for (_, size, path) in files {
            if used <= self.budget / 10 * 8 {
                break;
            }
            if fs::remove_file(path).is_ok() {
                used -= size;
            }
        }
        self.used.store(used, Ordering::Relaxed);
    }
}

pub struct Io {
    pub store: Arc<dyn ObjectStore>,
    blocks: Cache<(Arc<str>, u32), Arc<Block>, BlockWeight>,
    pub disk: Option<Arc<DiskCache>>,
    pub block_hits: AtomicU64,
    pub block_misses: AtomicU64,
}

impl Io {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cache_bytes: u64,
        disk: Option<DiskCache>,
    ) -> Arc<Self> {
        let cache_bytes = cache_bytes.max(1 << 20);
        Arc::new(Self {
            store,
            blocks: Cache::with_weighter(
                (cache_bytes / 16_384) as usize + 16,
                cache_bytes,
                BlockWeight,
            ),
            disk: disk.map(Arc::new),
            block_hits: AtomicU64::new(0),
            block_misses: AtomicU64::new(0),
        })
    }

    pub fn cached_block(&self, table: &Arc<str>, block: u32) -> Option<Arc<Block>> {
        let found = self.blocks.get(&(table.clone(), block));
        let counter = if found.is_some() {
            &self.block_hits
        } else {
            &self.block_misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    pub fn cache_block(&self, table: &Arc<str>, block: u32, data: Arc<Block>) {
        self.blocks.insert((table.clone(), block), data);
    }

    pub fn cache_bytes(&self) -> u64 {
        self.blocks.weight()
    }

    /// Bytes `start..end` of an immutable table of `size` bytes that the
    /// manifest says exists: from the disk cache when it is there, otherwise
    /// from the bucket, filling the disk cache on the way.
    pub async fn range(
        self: &Arc<Self>,
        key: &str,
        size: u64,
        start: u64,
        end: u64,
    ) -> Result<Bytes> {
        if let Some(disk) = &self.disk {
            let (cache, owned) = (disk.clone(), key.to_owned());
            let hit = tokio::task::spawn_blocking(move || cache.read(&owned, start, end))
                .await
                .ok()
                .flatten();
            if let Some(bytes) = hit {
                disk.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(bytes);
            }
            disk.misses.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = retry(|| self.store.get_range(key, start, end))
            .await?
            .ok_or_else(|| Error::Lost(format!("{key} was collected by a newer owner")))?;
        if let Some(disk) = &self.disk {
            if start == 0 && end >= size {
                self.remember(key, bytes.clone());
            } else if size <= DISK_OBJECT_BYTES && disk.filling.lock().insert(key.to_owned()) {
                // Fetch the whole object once, off the request's path.
                let (io, key) = (self.clone(), key.to_owned());
                tokio::spawn(async move {
                    if let Ok(Some(body)) = retry(|| io.store.get(&key)).await {
                        io.remember(&key, body);
                    }
                    if let Some(disk) = &io.disk {
                        disk.filling.lock().remove(&key);
                    }
                });
            }
        }
        Ok(bytes)
    }

    /// Put a whole table into the disk cache, such as one just written.
    pub fn remember(&self, key: &str, body: Bytes) {
        if let Some(disk) = &self.disk {
            let (disk, key) = (disk.clone(), key.to_owned());
            tokio::task::spawn_blocking(move || disk.write(&key, &body));
        }
    }

    /// Discard a cached table that failed its checksum. Returns whether there
    /// was one, in which case reading again goes to the bucket.
    pub async fn forget(&self, key: &str) -> bool {
        let Some(disk) = &self.disk else { return false };
        let (disk, key) = (disk.clone(), key.to_owned());
        tokio::task::spawn_blocking(move || disk.remove(&key))
            .await
            .unwrap_or(false)
    }
}

/// Retry an idempotent storage call through transient failures.
pub async fn retry<T, F, Fut>(mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = StoreResult<T>>,
{
    let mut delay = Duration::from_millis(20);
    for attempt in 0.. {
        match call().await {
            Ok(value) => return Ok(value),
            Err(StoreError::Unavailable(reason)) if attempt < 6 => {
                tracing::debug!(%reason, attempt, "storage call failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!()
}
