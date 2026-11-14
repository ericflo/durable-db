//! An in-memory store for tests, with injectable faults.
//!
//! Faults model what a real network does to a request: it can be delayed,
//! lost before it reaches storage, or applied with its acknowledgement lost.
use super::{Listed, ObjectStore, StoreError, StoreResult};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Debug, Clone, Default)]
pub struct Faults {
    /// Probability that a request fails without reaching storage.
    pub drop_request: f64,
    /// Probability that a write is applied but reported as failed.
    pub drop_response: f64,
    /// Each request sleeps a uniform duration up to this long, before and after.
    pub max_delay: Duration,
}

#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<BTreeMap<String, Bytes>>,
    faults: Mutex<Faults>,
    rng: Mutex<Option<StdRng>>,
    pub puts: AtomicU64,
    pub gets: AtomicU64,
    pub lists: AtomicU64,
    pub deletes: AtomicU64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deterministic faults: the same seed yields the same decisions.
    pub fn with_faults(seed: u64, faults: Faults) -> Self {
        let store = Self::default();
        *store.faults.lock() = faults;
        *store.rng.lock() = Some(StdRng::seed_from_u64(seed));
        store
    }

    pub fn set_faults(&self, faults: Faults) {
        *self.faults.lock() = faults;
    }

    pub fn keys(&self) -> Vec<String> {
        self.objects.lock().keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn roll(&self, p: f64) -> bool {
        p > 0.0
            && self
                .rng
                .lock()
                .as_mut()
                .is_some_and(|rng| rng.random_bool(p))
    }

    async fn delay(&self) {
        let max = self.faults.lock().max_delay;
        if max.is_zero() {
            return;
        }
        let nanos = self
            .rng
            .lock()
            .as_mut()
            .map(|rng| rng.random_range(0..=max.as_nanos() as u64));
        if let Some(nanos) = nanos {
            tokio::time::sleep(Duration::from_nanos(nanos)).await;
        }
    }

    /// Returns whether the response is to be lost.
    async fn enter(&self) -> StoreResult<bool> {
        let faults = self.faults.lock().clone();
        self.delay().await;
        if self.roll(faults.drop_request) {
            return Err(StoreError::Unavailable("injected: request lost".into()));
        }
        Ok(self.roll(faults.drop_response))
    }

    async fn leave(&self, lost: bool) -> StoreResult<()> {
        self.delay().await;
        if lost {
            return Err(StoreError::Unavailable("injected: response lost".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()> {
        let lost = self.enter().await?;
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.objects.lock().insert(key.to_owned(), body);
        self.leave(lost).await
    }

    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        self.enter().await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        let found = self.objects.lock().get(key).cloned();
        self.leave(false).await?;
        Ok(found)
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
        Ok(self.get(key).await?.map(|body| {
            let end = (end as usize).min(body.len());
            body.slice((start as usize).min(end)..end)
        }))
    }

    async fn exists(&self, key: &str) -> StoreResult<bool> {
        self.enter().await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        let found = self.objects.lock().contains_key(key);
        self.leave(false).await?;
        Ok(found)
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>> {
        self.enter().await?;
        self.lists.fetch_add(1, Ordering::Relaxed);
        let start = match after {
            Some(after) if after >= prefix => after.to_owned(),
            _ => prefix.to_owned(),
        };
        let out = self
            .objects
            .lock()
            .range(start..)
            .filter(|(key, _)| Some(key.as_str()) != after)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, body)| Listed {
                key: key.clone(),
                size: body.len() as u64,
            })
            .collect();
        self.leave(false).await?;
        Ok(out)
    }

    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        self.enter().await?;
        self.lists.fetch_add(1, Ordering::Relaxed);
        let mut out: Vec<String> = Vec::new();
        for key in self
            .objects
            .lock()
            .range(prefix.to_owned()..)
            .map(|(k, _)| k)
        {
            let Some(rest) = key.strip_prefix(prefix) else {
                break;
            };
            let Some((dir, _)) = rest.split_once('/') else {
                continue;
            };
            if after.is_some_and(|after| dir <= after) || out.last().is_some_and(|l| l == dir) {
                continue;
            }
            if out.len() >= limit {
                break;
            }
            out.push(dir.to_owned());
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        let lost = self.enter().await?;
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.objects.lock().remove(key);
        self.leave(lost).await
    }

    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        self.enter().await?;
        self.lists.fetch_add(1, Ordering::Relaxed);
        let mut objects = self.objects.lock();
        let keys: Vec<String> = objects
            .range(prefix.to_owned()..)
            .map(|(key, _)| key)
            .take_while(|key| key.starts_with(prefix) && end.is_none_or(|end| key.as_str() < end))
            .filter(|key| doomed(key))
            .cloned()
            .collect();
        for key in &keys {
            objects.remove(key);
        }
        self.deletes.fetch_add(keys.len() as u64, Ordering::Relaxed);
        Ok(keys.len())
    }
}
