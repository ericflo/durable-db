//! The only authority boundary: fresh reads, immutable uploads, and conditional replacement.
pub mod immutable;
mod ranges;
pub use ranges::MAX_RANGE_BYTES;

use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

/// No object request was sent. Admission may already have spent credit; never
/// refund it. A connection failure proves this only without retries/redirects.
#[derive(Debug, thiserror::Error)]
#[error("object upload failed before sending its request")]
pub(crate) struct NotSent;

// Inspect typed causes only. Their messages may contain signed URLs, keys or
// response bodies; neither diagnostics nor retry decisions parse those strings.
pub(crate) fn upload_error_kind(error: &anyhow::Error) -> &'static str {
    use object_store::client::{HttpError, HttpErrorKind};
    match error.chain().find_map(|e| e.downcast_ref::<HttpError>()) {
        Some(e) => match e.kind() {
            HttpErrorKind::Connect => "connect",
            HttpErrorKind::Timeout => "timeout",
            HttpErrorKind::Interrupted => "interrupted",
            HttpErrorKind::Request => "request",
            HttpErrorKind::Decode => "decode",
            _ => "other",
        },
        None => "other",
    }
}

#[derive(Clone, Debug)]
pub struct Version(pub UpdateVersion);
pub struct Object {
    pub bytes: Bytes,
    pub version: Version,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn upload_budget(&self) -> anyhow::Result<Option<crate::upload_budget::UploadStatus>> {
        Ok(None)
    }
    async fn replenish_upload_budget(
        &self,
        _: &crate::upload_budget::UploadRequest,
    ) -> crate::Result<crate::upload_budget::UploadResult> {
        Err(crate::Error::Invalid(
            "this storage namespace has no upload budget".into(),
        ))
    }
    async fn maintenance(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>>;
    /// Read exactly a bounded nonempty range of an existing immutable object.
    /// The durable reference supplies the full object size. Missing/truncated
    /// bytes, size changes and ignored Range headers are errors. There is no
    /// fallback to downloading the whole object. Identity immutability and
    /// payload integrity remain the caller/provider's contract.
    async fn get_range(
        &self,
        _key: &str,
        _range: std::ops::Range<u64>,
        _object_size: u64,
    ) -> anyhow::Result<Bytes> {
        bail!("bounded range reads are unsupported by this storage binding")
    }
    /// None means create-if-absent. False is a definite conflict, Err is possibly ambiguous.
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool>;
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()>;
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>>;
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
}

pub struct Remote {
    inner: Arc<dyn ObjectStore>,
    uploads: Option<Arc<dyn ObjectStore>>,
    prefix: String,
}
impl Remote {
    pub fn new(inner: Arc<dyn ObjectStore>, prefix: &str) -> anyhow::Result<Self> {
        if prefix.is_empty()
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            bail!("object prefix must be a nonempty ASCII identifier");
        }
        Ok(Self {
            uploads: None,
            inner,
            prefix: format!("{prefix}/"),
        })
    }
    /// Trusted binding: SDK retries, transport retries and redirects MUST all be
    /// disabled. Only then can its final connect error prove no request was sent.
    /// Arbitrary stores supplied to `new` never provide that witness.
    pub(crate) fn with_single_attempt_uploads(mut self, uploads: Arc<dyn ObjectStore>) -> Self {
        self.uploads = Some(uploads);
        self
    }
    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }
    /// Fail closed if a provider ignores create/update preconditions. This is a compatibility
    /// check, not a substitute for the provider's linearizability contract.
    pub async fn probe(&self) -> anyhow::Result<()> {
        let key = format!("probes/{}", Uuid::new_v4());
        let result = async {
            if !self.cas(&key, None, Bytes::from_static(b"one")).await? {
                bail!("probe create failed");
            }
            let one = self.get(&key, 16).await?.context("probe read missing")?;
            if self.cas(&key, None, Bytes::from_static(b"wrong")).await? {
                bail!("provider ignores create preconditions");
            }
            if !self
                .cas(&key, Some(&one.version), Bytes::from_static(b"two"))
                .await?
            {
                bail!("probe update failed");
            }
            if self
                .cas(&key, Some(&one.version), Bytes::from_static(b"wrong"))
                .await?
            {
                bail!("provider ignores stale ETag preconditions");
            }
            if self
                .get(&key, 16)
                .await?
                .context("probe read missing")?
                .bytes
                != b"two"[..]
            {
                bail!("provider is not read-after-write consistent");
            }
            Ok(())
        }
        .await;
        let _ = self.delete(&key).await;
        result
    }
}
// Bound the whole observation, including SDK retries and streamed bodies.
// Dropping a read cannot publish anything; an expired read is never absence.
async fn observe<T>(read: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .context("object observation deadline exceeded")?
}
#[async_trait]
impl Storage for Remote {
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        observe(self.read_range(key, range, object_size)).await
    }

    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        observe(async {
            let result = match self.inner.get(&self.path(key)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            if result.meta.size > max_bytes as u64 {
                bail!("object exceeds read budget");
            }
            let version = Version(UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            });
            let mut stream = result.into_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await? {
                if bytes.len().saturating_add(chunk.len()) > max_bytes {
                    bail!("object stream exceeds read budget");
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some(Object {
                bytes: bytes.into(),
                version,
            }))
        })
        .await
    }

    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        let mode = old.map_or(PutMode::Create, |v| PutMode::Update(v.0.clone()));
        match self
            .uploads
            .as_ref()
            .unwrap_or(&self.inner)
            .put_opts(&self.path(key), bytes.into(), mode.into())
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.uploads
            .as_ref()
            .unwrap_or(&self.inner)
            .put(&self.path(key), bytes.into())
            .await
            .map(|_| ())
            .map_err(|error| {
                let error = anyhow::Error::new(error);
                let unsent = self.uploads.is_some() && upload_error_kind(&error) == "connect";
                if unsent {
                    error.context(NotSent)
                } else {
                    error
                }
            })
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        observe(async {
            // ObjectStore lists directory prefixes, not arbitrary string prefixes. Use the
            // enclosing directory and an offset, then stop at the end of our literal range.
            let directory = prefix.rsplit_once('/').context("invalid listing prefix")?.0;
            let directory = self.path(directory);
            let start = self.path(after.unwrap_or(prefix));
            let mut stream = self.inner.list_with_offset(Some(&directory), &start);
            let mut keys = Vec::new();
            while keys.len() < limit {
                let Some(meta) = stream.try_next().await? else {
                    break;
                };
                let key = meta
                    .location
                    .as_ref()
                    .strip_prefix(&self.prefix)
                    .context("invalid object prefix")?
                    .to_string();
                if key.starts_with(prefix) {
                    keys.push(key);
                } else if key.as_str() > prefix {
                    break;
                }
            }
            keys.sort();
            Ok(keys)
        })
        .await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self.inner.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// A single-process local authority. The lifetime OS lock explicitly rejects two servers.
/// Atomic rename + fsync(file) + fsync(directory) is the local linearization/durability boundary.
pub struct Local {
    root: PathBuf,
    serial: Arc<std::sync::Mutex<()>>,
    _lock: Arc<std::fs::File>,
}
impl Local {
    pub fn open(root: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&root)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("LOCK"))?;
        lock.try_lock_exclusive()
            .context("local directory already owned; multi-node mode requires object storage")?;
        // Fixed directory structure: no per-tenant mkdir durability ambiguity during writes.
        for dir in [
            "heads",
            "blocks",
            "journal",
            "tables",
            "ownership",
            "catalogs",
            "manifests",
            "indexes",
        ] {
            std::fs::create_dir_all(root.join(dir))?;
        }
        std::fs::File::open(&root)?.sync_all()?;
        Ok(Self {
            root,
            serial: Arc::new(std::sync::Mutex::new(())),
            _lock: Arc::new(lock),
        })
    }
    fn path(&self, key: &str) -> anyhow::Result<PathBuf> {
        if key.split('/').any(|s| {
            s.is_empty()
                || s == "."
                || s == ".."
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        }) {
            bail!("invalid internal path");
        }
        Ok(self.root.join(key))
    }
    async fn read(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        let path = self.path(key)?;
        let mut file = match tokio::fs::File::open(path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        use tokio::io::AsyncReadExt;
        if file.metadata().await?.len() > max as u64 {
            bail!("object exceeds read budget");
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(max as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > max {
            bail!("object exceeds read budget");
        }
        let tag = hex::encode(Sha256::digest(&bytes));
        Ok(Some(Object {
            bytes: bytes.into(),
            version: Version(UpdateVersion {
                e_tag: Some(tag),
                version: None,
            }),
        }))
    }
    async fn write(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let path = self.path(key)?;
        // Temp names stay outside the authoritative object namespace; crash leftovers are
        // removed on open while the lifetime lock is held (see cleanup_temps).
        let root = self.root.clone();
        let lock = self._lock.clone();
        tokio::task::spawn_blocking(move || {
            let _keep_owner = lock;
            write_local(&root, &path, &bytes)
        })
        .await?
    }
    pub fn cleanup_temps(&self) -> anyhow::Result<()> {
        for item in std::fs::read_dir(&self.root)? {
            let item = item?;
            if item.file_name().to_string_lossy().starts_with("tmp-") && item.file_type()?.is_file()
            {
                std::fs::remove_file(item.path())?;
            }
        }
        Ok(())
    }
}
#[async_trait]
impl Storage for Local {
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        self.read_range(key, range, object_size).await
    }

    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.read(key, max).await
    }
    async fn cas(&self, key: &str, old: Option<&Version>, bytes: Bytes) -> anyhow::Result<bool> {
        // The lock must live INSIDE the blocking task. Cancelling the async waiter must
        // not release exclusion while a filesystem rename is still in flight.
        let serial = self.serial.clone();
        let lock = self._lock.clone();
        let path = self.path(key)?;
        let root = self.root.clone();
        let expected = old.and_then(|v| v.0.e_tag.clone());
        tokio::task::spawn_blocking(move || {
            let _keep_owner = lock;
            let _guard = serial
                .lock()
                .map_err(|_| anyhow::anyhow!("local lock poisoned"))?;
            let current = match std::fs::read(&path) {
                Ok(b) => Some(hex::encode(Sha256::digest(b))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(e.into()),
            };
            if current != expected {
                return Ok(false);
            }
            write_local(&root, &path, &bytes)?;
            Ok(true)
        })
        .await?
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.write(key, bytes).await
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        // All object keys have exactly one directory separator. Bounded memory even for
        // large directories; remote S3 uses server-side pagination instead.
        let (dir, name) = prefix.split_once('/').context("invalid listing prefix")?;
        let mut entries = tokio::fs::read_dir(self.path(dir)?).await?;
        let mut found = std::collections::BTreeSet::new();
        while let Some(entry) = entries.next_entry().await? {
            let file = entry.file_name().to_string_lossy().into_owned();
            let key = format!("{dir}/{file}");
            if file.starts_with(name) && after.is_none_or(|a| key.as_str() > a) {
                found.insert(key);
                if found.len() > limit {
                    found.pop_last();
                }
            }
        }
        Ok(found.into_iter().collect())
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.path(key)?;
        let lock = self._lock.clone();
        tokio::task::spawn_blocking(move || {
            let _keep_owner = lock;
            match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
        .await?
    }
}

fn write_local(root: &std::path::Path, path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let parent = path.parent().context("missing parent")?;
    let temp = root.join(format!("tmp-{}", Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        // rename crosses the tmp directory and the object's directory.
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(temp);
    result
}

#[cfg(test)]
mod local_tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_local_cas_retains_exclusion_until_blocking_write_finishes() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(Local::open(temp.path().into()).unwrap());
        let guard = Arc::downgrade(&local._lock);
        let serial = local.serial.clone();
        let (entered, blocked) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let _lock = serial.lock().unwrap();
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        blocked.recv().unwrap();
        let writer = local.clone();
        let request = tokio::spawn(async move {
            writer
                .cas("authority.json", None, Bytes::from_static(b"committed"))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while guard.strong_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        request.abort();
        let _ = request.await;
        drop(local);
        assert!(
            Local::open(temp.path().into()).is_err(),
            "cancelled waiter released a live filesystem mutation"
        );
        release.send(()).unwrap();
        blocker.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while guard.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let next = Local::open(temp.path().into()).unwrap();
        assert_eq!(
            next.get("authority.json", 32).await.unwrap().unwrap().bytes,
            b"committed"[..]
        );
    }
}
