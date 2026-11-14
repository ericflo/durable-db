//! Counters and latency histograms, rendered in the Prometheus text format.
//! Labels are fixed strings; no key, value, token or database name is recorded.
use crate::store::{Listed, ObjectStore, StoreResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    fmt::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const BOUNDS_MS: [f64; 15] = [
    0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10000.0,
];

#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; 16],
    micros: AtomicU64,
    failed: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, elapsed: Duration) {
        let ms = elapsed.as_secs_f64() * 1000.0;
        let slot = BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(BOUNDS_MS.len());
        self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        self.micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn fail(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    fn render(&self, out: &mut String, name: &str, label: &str) {
        let mut total = 0;
        for (i, bucket) in self.buckets.iter().enumerate() {
            total += bucket.load(Ordering::Relaxed);
            let bound = BOUNDS_MS
                .get(i)
                .map_or("+Inf".to_owned(), |ms| format!("{}", ms / 1000.0));
            let _ = writeln!(
                out,
                "{name}_seconds_bucket{{{label},le=\"{bound}\"}} {total}"
            );
        }
        let seconds = self.micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_seconds_sum{{{label}}} {seconds}");
        let _ = writeln!(out, "{name}_seconds_count{{{label}}} {total}");
        let _ = writeln!(
            out,
            "{name}_failures_total{{{label}}} {}",
            self.failed.load(Ordering::Relaxed)
        );
    }
}

#[derive(Default)]
pub struct Metrics {
    pub reads: Histogram,
    pub commits: Histogram,
    pub admin: Histogram,
    pub opens: Histogram,
    pub forwarded: AtomicU64,
    pub rejected: AtomicU64,
    pub store: [Histogram; 6],
    pub store_bytes_out: AtomicU64,
    pub store_bytes_in: AtomicU64,
}

const STORE_OPS: [&str; 6] = ["put", "get", "exists", "list", "delete", "purge"];

impl Metrics {
    pub fn render(
        &self,
        open_databases: usize,
        cache_bytes: u64,
        block_hits: u64,
        block_misses: u64,
    ) -> String {
        let mut out = String::new();
        out.push_str("# TYPE durable_db_request_seconds histogram\n");
        self.reads
            .render(&mut out, "durable_db_request", "kind=\"read\"");
        self.commits
            .render(&mut out, "durable_db_request", "kind=\"commit\"");
        self.admin
            .render(&mut out, "durable_db_request", "kind=\"admin\"");
        self.opens
            .render(&mut out, "durable_db_request", "kind=\"open\"");
        out.push_str("# TYPE durable_db_storage_seconds histogram\n");
        for (op, histogram) in STORE_OPS.iter().zip(&self.store) {
            histogram.render(&mut out, "durable_db_storage", &format!("op=\"{op}\""));
        }
        let gauge = |out: &mut String, name: &str, value: u64| {
            let _ = writeln!(out, "durable_db_{name} {value}");
        };
        gauge(
            &mut out,
            "storage_sent_bytes_total",
            self.store_bytes_out.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "storage_received_bytes_total",
            self.store_bytes_in.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "forwarded_requests_total",
            self.forwarded.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "rejected_requests_total",
            self.rejected.load(Ordering::Relaxed),
        );
        gauge(&mut out, "open_databases", open_databases as u64);
        gauge(&mut out, "block_cache_bytes", cache_bytes);
        gauge(&mut out, "block_cache_hits_total", block_hits);
        gauge(&mut out, "block_cache_misses_total", block_misses);
        out
    }
}

/// Times every storage call.
pub struct Metered {
    pub inner: Arc<dyn ObjectStore>,
    pub metrics: Arc<Metrics>,
}

impl Metered {
    async fn timed<T>(
        &self,
        op: usize,
        call: impl Future<Output = StoreResult<T>>,
    ) -> StoreResult<T> {
        let started = Instant::now();
        let result = call.await;
        self.metrics.store[op].observe(started.elapsed());
        if result.is_err() {
            self.metrics.store[op].fail();
        }
        result
    }
}

#[async_trait]
impl ObjectStore for Metered {
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.metrics
            .store_bytes_out
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        self.timed(0, self.inner.put(key, body)).await
    }
    async fn put_once(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.metrics
            .store_bytes_out
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        self.timed(0, self.inner.put_once(key, body)).await
    }
    async fn put_spare(&self, key: &str, body: Bytes) -> StoreResult<bool> {
        let (bytes, started) = (body.len() as u64, Instant::now());
        let result = self.inner.put_spare(key, body).await;
        // One that was not sent is not an upload.
        if !matches!(result, Ok(false)) {
            self.metrics
                .store_bytes_out
                .fetch_add(bytes, Ordering::Relaxed);
            self.metrics.store[0].observe(started.elapsed());
            if result.is_err() {
                self.metrics.store[0].fail();
            }
        }
        result
    }
    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        let found = self.timed(1, self.inner.get(key)).await?;
        let bytes = found.as_ref().map_or(0, Bytes::len) as u64;
        self.metrics
            .store_bytes_in
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(found)
    }
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>> {
        let found = self.timed(1, self.inner.get_range(key, start, end)).await?;
        let bytes = found.as_ref().map_or(0, Bytes::len) as u64;
        self.metrics
            .store_bytes_in
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(found)
    }
    async fn exists(&self, key: &str) -> StoreResult<bool> {
        self.timed(2, self.inner.exists(key)).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>> {
        self.timed(3, self.inner.list(prefix, after, limit)).await
    }
    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        self.timed(3, self.inner.list_dirs(prefix, after, limit))
            .await
    }
    async fn delete(&self, key: &str) -> StoreResult<()> {
        self.timed(4, self.inner.delete(key)).await
    }
    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize> {
        self.timed(5, self.inner.purge(prefix, end, doomed)).await
    }
}
