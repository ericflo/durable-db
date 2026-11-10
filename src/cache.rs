//! Caches hold immutable bytes; mutable authority and local heads are never cached.
use bytes::Bytes;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::Mutex;

// Cache manifests, pages and native table blocks, never complete large data
// objects. Readers enforce their own smaller limits after cache lookup.
const MAX_OBJECT_BYTES: usize = 512 * 1024;

struct Entry<T> {
    value: T,
    weight: usize,
    touched: u64,
}
struct Lru<T> {
    values: HashMap<String, Entry<T>>,
    bytes: usize,
    limit: usize,
    clock: u64,
}
impl<T> Lru<T> {
    fn new(limit: usize) -> Self {
        Self {
            values: HashMap::new(),
            bytes: 0,
            limit,
            clock: 0,
        }
    }
    fn remove(&mut self, key: &str) {
        if let Some(e) = self.values.remove(key) {
            self.bytes -= e.weight;
        }
    }
    fn insert(&mut self, key: String, value: T, size: usize) -> Vec<String> {
        self.remove(&key);
        let weight = size.saturating_add(key.len()).saturating_add(256);
        if weight > self.limit {
            return Vec::new();
        }
        let mut removed = Vec::new();
        while self.bytes + weight > self.limit || self.values.len() >= 4096 {
            let Some(old) = self
                .values
                .iter()
                .min_by_key(|(_, e)| e.touched)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.remove(&old);
            removed.push(old);
        }
        self.clock += 1;
        self.bytes += weight;
        self.values.insert(
            key,
            Entry {
                value,
                weight,
                touched: self.clock,
            },
        );
        removed
    }
    fn get(&mut self, key: &str) -> Option<&T> {
        self.clock += 1;
        let entry = self.values.get_mut(key)?;
        entry.touched = self.clock;
        Some(&entry.value)
    }
}

struct Disk {
    root: PathBuf,
    index: Lru<()>,
    _lock: std::fs::File,
}
#[derive(Clone)]
struct MemoryValue {
    bytes: Bytes,
    digest: String,
}
pub struct Cache {
    memory: Mutex<Lru<MemoryValue>>,
    disk: Option<Mutex<Disk>>,
}
impl Cache {
    pub fn new(memory_bytes: usize, disk: Option<(PathBuf, usize)>) -> anyhow::Result<Self> {
        let disk = if let Some((root, limit)) = disk {
            std::fs::create_dir_all(&root)?;
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join("LOCK"))?;
            lock.try_lock_exclusive()?;
            let mut index = Lru::new(limit);
            for item in std::fs::read_dir(&root)? {
                let item = item?;
                let name = item.file_name().to_string_lossy().into_owned();
                if name.len() != 64
                    || !name.bytes().all(|b| b.is_ascii_hexdigit())
                    || !item.file_type()?.is_file()
                {
                    continue;
                }
                let size = item.metadata()?.len() as usize;
                if size > MAX_OBJECT_BYTES || size.saturating_add(320) > limit {
                    std::fs::remove_file(item.path())?;
                    continue;
                }
                for old in index.insert(name, (), size) {
                    std::fs::remove_file(root.join(old))?;
                }
            }
            Some(Mutex::new(Disk {
                root,
                index,
                _lock: lock,
            }))
        } else {
            None
        };
        Ok(Self {
            memory: Mutex::new(Lru::new(memory_bytes)),
            disk,
        })
    }
    pub async fn get(&self, key: &str, digest: &str) -> Option<Bytes> {
        let result = self.load(key, digest).await;
        crate::metrics::REGISTRY.event(
            if result.is_some() {
                crate::metrics::Event::CacheHit
            } else {
                crate::metrics::Event::CacheMiss
            },
            1,
        );
        result
    }
    async fn load(&self, key: &str, digest: &str) -> Option<Bytes> {
        if let Some(value) = self.memory.lock().await.get(key).cloned()
            && value.digest == digest
        {
            return Some(value.bytes);
        }
        if let Some(disk) = &self.disk {
            let mut disk = disk.lock().await;
            let file = hex::encode(Sha256::digest(key.as_bytes()));
            disk.index.get(&file)?;
            let path = disk.root.join(&file);
            // Never trust a cache file's size, including after local corruption.
            use tokio::io::AsyncReadExt;
            let handle = tokio::fs::File::open(&path).await.ok()?;
            if handle.metadata().await.ok()?.len() > MAX_OBJECT_BYTES as u64 {
                return None;
            }
            let mut bytes = Vec::new();
            handle
                .take(MAX_OBJECT_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .await
                .ok()?;
            if bytes.len() > MAX_OBJECT_BYTES {
                return None;
            }
            if hex::encode(Sha256::digest(&bytes)) != digest {
                disk.index.remove(&file);
                let _ = tokio::fs::remove_file(path).await;
                return None;
            }
            let bytes = Bytes::from(bytes);
            self.memory.lock().await.insert(
                key.to_string(),
                MemoryValue {
                    bytes: bytes.clone(),
                    digest: digest.into(),
                },
                bytes.len(),
            );
            return Some(bytes);
        }
        None
    }
    pub async fn insert(&self, key: &str, bytes: Bytes) {
        if bytes.len() > MAX_OBJECT_BYTES {
            return;
        }
        self.memory.lock().await.insert(
            key.to_string(),
            MemoryValue {
                digest: hex::encode(Sha256::digest(&bytes)),
                bytes: bytes.clone(),
            },
            bytes.len(),
        );
        if let Some(disk) = &self.disk {
            let mut disk = disk.lock().await;
            if bytes.len().saturating_add(320) > disk.index.limit {
                return;
            }
            let file = hex::encode(Sha256::digest(key.as_bytes()));
            for old in disk.index.insert(file.clone(), (), bytes.len()) {
                let _ = tokio::fs::remove_file(disk.root.join(old)).await;
            }
            // A crash can tear a cache write. Checksums make it a miss on the next read.
            if tokio::fs::write(disk.root.join(&file), bytes)
                .await
                .is_err()
            {
                disk.index.remove(&file);
            }
        }
    }
    pub async fn memory_bytes(&self) -> usize {
        self.memory.lock().await.bytes
    }
}
