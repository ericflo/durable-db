//! Narrow integration adapter. Inline files only; no production serving or GC.
use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    journal,
    owned_files::{self, Change, Condition, Namespace},
};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{path::Path, *};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, Notify};

#[derive(Default)]
pub struct Budget {
    puts: AtomicUsize,
    bytes: AtomicUsize,
    gets: AtomicUsize,
    lists: AtomicUsize,
    lost_replies: AtomicUsize,
    external_uploads: AtomicUsize,
    external_reads: AtomicUsize,
}
impl Budget {
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({"external_uploads":self.external_uploads.load(Ordering::SeqCst),"external_reads":self.external_reads.load(Ordering::SeqCst),"mutation_attempts":self.puts.load(Ordering::SeqCst),"injected_lost_replies":self.lost_replies.load(Ordering::SeqCst),"put_payload_bytes":self.bytes.load(Ordering::SeqCst),"get_calls":self.gets.load(Ordering::SeqCst),"list_calls":self.lists.load(Ordering::SeqCst)})
    }
}
struct State {
    objects: Arc<dyn durable_db::storage::Storage>,
    files: Mutex<Namespace>,
    changed: Notify,
    failed: AtomicBool,
    closed: AtomicBool,
    budget: Arc<Budget>,
}
#[derive(Clone)]
pub struct Store(Arc<State>);
impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OwnedIndexFixture")
    }
}
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("owned journal integration fixture")
    }
}
fn error(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Generic {
        store: "OwnedIndexFixture",
        source: message.into(),
    }
}
fn unsupported() -> Error {
    Error::NotSupported {
        source: "unsupported by the bounded index integration fixture".into(),
    }
}
fn meta(path: &Path, file: &owned_files::File) -> ObjectMeta {
    ObjectMeta {
        location: path.clone(),
        size: file.size(),
        e_tag: Some(file.version().to_string()),
        version: None,
        // No time-based GC is enabled in this fixture. A production adapter
        // needs persisted metadata and reference-aware reclamation.
        last_modified: chrono::DateTime::UNIX_EPOCH,
    }
}
impl Store {
    pub fn new(
        files: Namespace,
        budget: Arc<Budget>,
        objects: Arc<dyn durable_db::storage::Storage>,
    ) -> Self {
        Self(Arc::new(State {
            files: Mutex::new(files),
            objects,
            changed: Notify::new(),
            failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            budget,
        }))
    }
    fn ready(&self) -> Result<()> {
        if self.0.closed.load(Ordering::SeqCst) || self.0.failed.load(Ordering::SeqCst) {
            return Err(error("fixture retired"));
        }
        Ok(())
    }
    pub async fn usage(&self) -> anyhow::Result<serde_json::Value> {
        let files = self.0.files.lock().await;
        let view = files.view()?;
        let external_bytes: u64 = view
            .list("")
            .filter_map(|(_, file)| file.blob())
            .map(|blob| blob.size())
            .sum();
        let external_files = view
            .list("")
            .filter(|(_, file)| file.blob().is_some())
            .count();
        Ok(
            serde_json::json!({"resident_name_payload_bytes":view.resident_bytes(),"logical_files":view.len(),"external_files":external_files,"external_bytes":external_bytes}),
        )
    }
    pub async fn retire(&self) -> anyhow::Result<journal::Prefix> {
        let files = self.0.files.lock().await;
        anyhow::ensure!(
            files.pending_batches() == 0,
            "cannot retire with unresolved index writes"
        );
        self.0.closed.store(true, Ordering::SeqCst);
        Ok(files.durable_prefix())
    }
    pub async fn prepare_late_manifest(&self) -> anyhow::Result<owned_files::Prepared> {
        let mut files = self.0.files.lock().await;
        anyhow::ensure!(
            files.pending_batches() == 0,
            "unresolved writes before late-upload fixture"
        );
        let name = files
            .view()?
            .list("index/manifest/")
            .last()
            .map(|(name, _)| name.to_owned())
            .ok_or_else(|| anyhow::anyhow!("manifest missing"))?;
        let upload = files.prepare(&[Change {
            name: &name,
            value: Some(b"invalid late manifest"),
            condition: Condition::Any,
        }])?;
        self.0.closed.store(true, Ordering::SeqCst);
        Ok(upload)
    }

    async fn wait(&self, through: u64) -> Result<()> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.ready()?;
            let files = self.0.files.lock().await;
            files
                .view()
                .map_err(|_| error("logical namespace unavailable"))?;
            if files.durable_prefix().position() >= through {
                return Ok(());
            }
            drop(files);
            changed.await;
        }
    }
    async fn mutate(&self, path: &Path, bytes: Option<Bytes>, mode: PutMode) -> Result<PutResult> {
        self.ready()?;
        let attempt = self.0.budget.puts.fetch_add(1, Ordering::SeqCst);
        let size = bytes.as_ref().map_or(0, Bytes::len);
        let charged = self.0.budget.bytes.fetch_add(size, Ordering::SeqCst);
        if attempt >= 512 || charged.saturating_add(size) > 32 * 1024 * 1024 {
            return Err(error("fixture upload budget exhausted"));
        }
        let lose_reply = attempt % 5 == 4;
        let create = matches!(mode, PutMode::Create);
        let condition = match mode {
            PutMode::Overwrite => Condition::Any,
            PutMode::Create => Condition::Absent,
            PutMode::Update(v) => {
                if v.version.is_some() {
                    return Err(unsupported());
                }
                Condition::Matches(v.e_tag.ok_or_else(unsupported)?.parse().map_err(|_| {
                    Error::Precondition {
                        path: path.to_string(),
                        source: "invalid logical version".into(),
                    }
                })?)
            }
        };
        let external = if size > 256 * 1024 {
            self.0
                .budget
                .external_uploads
                .fetch_add(1, Ordering::SeqCst);
            Some(
                owned_files::Blob::upload(
                    self.0.objects.clone(),
                    bytes.clone().expect("nonempty payload"),
                )
                .await
                .map_err(|_| error("external file upload failed; outcome may be unknown"))?,
            )
        } else {
            None
        };
        let prepared = {
            let mut files = self.0.files.lock().await;
            self.ready()?;
            if let Some(blob) = &external {
                files.prepare_reference(path.as_ref(), blob, condition)
            } else {
                files.prepare(&[Change {
                    name: path.as_ref(),
                    value: bytes.as_deref(),
                    condition,
                }])
            }
        };
        let prepared = match prepared {
            Ok(p) => p,
            Err(e) => {
                if let Some(owned_files::Error::Precondition { through }) =
                    e.downcast_ref::<owned_files::Error>()
                {
                    self.wait(*through).await?;
                    return Err(if create {
                        Error::AlreadyExists {
                            path: path.to_string(),
                            source: "logical file exists".into(),
                        }
                    } else {
                        Error::Precondition {
                            path: path.to_string(),
                            source: "logical version changed".into(),
                        }
                    });
                }
                return Err(error("logical file admission failed"));
            }
        };
        let position = prepared.position();
        let version = prepared.versions()[0];
        let state = self.0.clone();
        // A cancelled object-store waiter does not cancel the accepted upload.
        // The namespace queue bounds the number and size of these tasks.
        tokio::spawn(async move {
            let completion = prepared.send().await;
            let unknown = completion.resolution() == journal::Resolution::Unknown;
            let mut files = state.files.lock().await;
            let mut success = if lose_reply {
                state.budget.lost_replies.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                files.complete(completion).is_ok()
            };
            if success && (unknown || lose_reply) {
                success = matches!(
                    files.resolve(position).await,
                    Ok(journal::Resolution::Durable)
                );
            }
            if !success {
                state.failed.store(true, Ordering::SeqCst);
            }
            drop(files);
            state.changed.notify_waiters();
        });
        self.wait(position).await?;
        Ok(PutResult {
            e_tag: Some(version.to_string()),
            version: None,
            extensions: Default::default(),
        })
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if !options.attributes.is_empty() {
            return Err(unsupported());
        }
        if payload.content_length() > owned_files::MAX_BLOB_BYTES {
            return Err(error("index file exceeds fixture bound"));
        }
        let payload: Bytes = payload.into();
        self.mutate(path, Some(payload), options.mode).await
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(unsupported())
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.ready()?;
        self.0.budget.gets.fetch_add(1, Ordering::SeqCst);
        if options.version.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return Err(unsupported());
        }
        let files = self.0.files.lock().await;
        self.ready()?;
        let view = files
            .view()
            .map_err(|_| error("logical namespace unavailable"))?;
        let file = view.get(path.as_ref()).ok_or_else(|| Error::NotFound {
            path: path.to_string(),
            source: "logical file absent".into(),
        })?;
        let meta = meta(path, file);
        options.check_preconditions(&meta)?;
        let range = options
            .range
            .map(|r| r.as_range(meta.size))
            .transpose()
            .map_err(|_| error("invalid logical file range"))?
            .unwrap_or(0..meta.size);
        let external = file.blob().cloned();
        let inline = if options.head {
            Some(Bytes::new())
        } else {
            file.bytes().map(|bytes| {
                Bytes::copy_from_slice(&bytes[range.start as usize..range.end as usize])
            })
        };
        drop(files);
        let bytes = if let Some(bytes) = inline {
            bytes
        } else {
            self.0.budget.external_reads.fetch_add(1, Ordering::SeqCst);
            external
                .expect("file content")
                .read_range(self.0.objects.as_ref(), range.clone())
                .await
                .map_err(|_| error("external file range read failed"))?
        };
        Ok(GetResult {
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
        })
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        paths
            .and_then(move |path| {
                let store = store.clone();
                async move {
                    store.mutate(&path, None, PutMode::Overwrite).await?;
                    Ok(path)
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            store.ready()?;
            store.0.budget.lists.fetch_add(1, Ordering::SeqCst);
            let files = store.0.files.lock().await;
            store.ready()?;
            let view = files
                .view()
                .map_err(|_| error("logical namespace unavailable"))?;
            let names = view
                .list(prefix.as_ref().map_or("", Path::as_ref))
                .filter_map(|(name, file)| {
                    let path = Path::from(name);
                    let matches = prefix.as_ref().is_none_or(|p| {
                        path.prefix_match(p)
                            .is_some_and(|mut parts| parts.next().is_some())
                    });
                    matches.then(|| Ok(meta(&path, file)))
                })
                .collect::<Vec<_>>();
            Ok::<_, Error>(stream::iter(names))
        })
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(unsupported())
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(unsupported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_db::{
        journal::{QueueLimits, Writer},
        owned_files::{Limits, View},
        storage::{Object, Remote, Storage, Version},
    };
    use std::time::Duration;

    struct Paused {
        inner: Remote,
        entered: Notify,
        release: Notify,
    }
    #[async_trait]
    impl Storage for Paused {
        async fn get_range(
            &self,
            name: &str,
            range: std::ops::Range<u64>,
            size: u64,
        ) -> anyhow::Result<Bytes> {
            self.inner.get_range(name, range, size).await
        }

        async fn get(&self, name: &str, max: usize) -> anyhow::Result<Option<Object>> {
            self.inner.get(name, max).await
        }
        async fn put(&self, name: &str, value: Bytes) -> anyhow::Result<()> {
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.put(name, value).await
        }
        async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
            panic!("physical CAS forbidden")
        }
        async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
            panic!("physical LIST forbidden")
        }
        async fn delete(&self, _: &str) -> anyhow::Result<()> {
            panic!("physical deletion forbidden")
        }
    }
    #[tokio::test]
    async fn cancelled_waiter_keeps_upload_alive_and_conflicting_create_waits_for_durability() {
        let backend = Arc::new(Paused {
            inner: Remote::new(Arc::new(memory::InMemory::new()), "fixture").unwrap(),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let namespace = Namespace::new(
            View::empty(Limits::default()),
            Writer::new(
                backend.clone(),
                uuid::Uuid::new_v4(),
                QueueLimits::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let store = Store::new(namespace, Arc::new(Budget::default()), backend.clone());
        let first_store = store.clone();
        let first = tokio::spawn(async move {
            first_store
                .put_opts(
                    &Path::from("file"),
                    Bytes::from_static(b"selected").into(),
                    PutMode::Create.into(),
                )
                .await
        });
        backend.entered.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let path = Path::from("file");
        let conflict = store.put_opts(
            &path,
            Bytes::from_static(b"other").into(),
            PutMode::Create.into(),
        );
        tokio::pin!(conflict);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut conflict)
                .await
                .is_err()
        );
        assert!(matches!(
            store.get(&path).await,
            Err(Error::NotFound { .. })
        ));
        backend.release.notify_one();
        assert!(matches!(conflict.await, Err(Error::AlreadyExists { .. })));
        let result = store.get(&path).await.unwrap();
        let version = result.meta.e_tag.clone().unwrap();
        assert_eq!(result.bytes().await.unwrap(), b"selected"[..]);
        assert!(matches!(
            store
                .get_opts(
                    &path,
                    GetOptions {
                        if_match: Some("different".into()),
                        ..Default::default()
                    }
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        assert_eq!(
            store
                .get_opts(
                    &path,
                    GetOptions {
                        if_match: Some(version),
                        range: Some(GetRange::Bounded(1..4)),
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"ele"[..]
        );
        store.retire().await.unwrap();
        assert!(store.get(&path).await.is_err());
    }
}
