//! The only thing durable-db asks of storage.
//!
//! Six operations, each on one key or one prefix. No conditional writes, no
//! transactions, no clocks. The protocol in `spec/DurableDb.tla` needs exactly
//! two guarantees from an implementation:
//!
//! 1. **Read-after-write.** Once `put` or `delete` has returned `Ok`, every
//!    `get`, `exists` and `list` that starts afterwards observes it.
//! 2. **Atomic objects.** A reader sees a whole object or none of it.
//!
//! Amazon S3, Backblaze B2, Cloudflare R2, Google Cloud Storage, MinIO and a
//! local directory all provide both.
use async_trait::async_trait;
use bytes::Bytes;

pub mod fs;
pub mod memory;
pub mod s3;

#[derive(Debug, Clone, thiserror::Error)]
pub enum StoreError {
    /// The request may or may not have taken effect. Safe to retry for
    /// everything except a claim, which is abandoned instead.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// Retrying cannot help: bad credentials, missing bucket, corrupt data.
    #[error("storage error: {0}")]
    Fatal(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub size: u64,
}

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Store an object under `key`. One attempt; the caller owns retries,
    /// and may run several attempts for the same immutable bytes at once.
    async fn put(&self, key: &str, body: Bytes) -> StoreResult<()>;

    /// Store an object that is uploaded exactly once and never retried: a
    /// claim. Same contract as `put`; an implementation may choose its most
    /// dependable path, since no second attempt will cover for a slow one.
    async fn put_once(&self, key: &str, body: Bytes) -> StoreResult<()> {
        self.put(key, body).await
    }

    async fn get(&self, key: &str) -> StoreResult<Option<Bytes>>;

    /// Bytes `start..end` of the object, clamped to its length.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Option<Bytes>>;

    async fn exists(&self, key: &str) -> StoreResult<bool>;

    /// Keys beginning with `prefix` and greater than `after`, ascending.
    /// Returns at most `limit` entries; fewer means the listing is complete.
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<Listed>>;

    /// The distinct next path segments below `prefix` that follow `after`:
    /// `list_dirs("db/")` yields `"a"`, `"b"` for `db/a/x`, `db/b/y`. They
    /// come in the order of the keys below them, which is the order of the
    /// names with `/` appended (see `dir_order`): `a-b` precedes `a`.
    async fn list_dirs(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>>;

    /// Make `key` absent to every later read. Idempotent.
    async fn delete(&self, key: &str) -> StoreResult<()>;

    /// Permanently remove every object below `prefix` whose key is less than
    /// `end` (when given) and for which `doomed` returns true, together with
    /// any versions or delete markers the provider retained. Returns how many
    /// keys were removed. Never makes a previously deleted key visible again.
    async fn purge(
        &self,
        prefix: &str,
        end: Option<&str>,
        doomed: &(dyn for<'a> Fn(&'a str) -> bool + Sync),
    ) -> StoreResult<usize>;
}

/// The order in which `list_dirs` yields names: that of the keys below them,
/// so each name is compared with its `/` appended.
pub fn dir_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.bytes().chain([b'/']).cmp(b.bytes().chain([b'/']))
}

/// Every key below `prefix`, in order.
pub async fn list_all(store: &dyn ObjectStore, prefix: &str) -> StoreResult<Vec<Listed>> {
    list_from(store, prefix, None).await
}

pub async fn list_from(
    store: &dyn ObjectStore,
    prefix: &str,
    after: Option<&str>,
) -> StoreResult<Vec<Listed>> {
    const PAGE: usize = 1000;
    let mut out = Vec::new();
    let mut after = after.map(str::to_owned);
    loop {
        let page = store.list(prefix, after.as_deref(), PAGE).await?;
        let full = page.len() >= PAGE;
        if let Some(last) = page.last() {
            after = Some(last.key.clone());
        }
        out.extend(page);
        if !full {
            return Ok(out);
        }
    }
}
