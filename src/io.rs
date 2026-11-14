//! Storage access shared by every open database: the object store, bounded
//! retries, and the block cache. Everything cached here is disposable.
use crate::{
    Error, Result,
    store::{ObjectStore, StoreError, StoreResult},
    table::Block,
};
use bytes::Bytes;
use quick_cache::{Weighter, sync::Cache};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone)]
struct BlockWeight;

impl Weighter<(Arc<str>, u32), Arc<Block>> for BlockWeight {
    fn weight(&self, key: &(Arc<str>, u32), block: &Arc<Block>) -> u64 {
        (key.0.len() + block.weight()) as u64
    }
}

pub struct Io {
    pub store: Arc<dyn ObjectStore>,
    blocks: Cache<(Arc<str>, u32), Arc<Block>, BlockWeight>,
    pub block_hits: AtomicU64,
    pub block_misses: AtomicU64,
}

impl Io {
    pub fn new(store: Arc<dyn ObjectStore>, cache_bytes: u64) -> Arc<Self> {
        let cache_bytes = cache_bytes.max(1 << 20);
        Arc::new(Self {
            store,
            blocks: Cache::with_weighter(
                (cache_bytes / 16_384) as usize + 16,
                cache_bytes,
                BlockWeight,
            ),
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

    /// Part of an immutable object that the manifest says exists.
    pub async fn range(&self, key: &str, start: u64, end: u64) -> Result<Bytes> {
        retry(|| self.store.get_range(key, start, end))
            .await?
            .ok_or_else(|| Error::Lost(format!("{key} was collected by a newer owner")))
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
