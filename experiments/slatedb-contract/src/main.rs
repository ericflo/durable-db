use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use slatedb::{
    Db,
    config::{FlushOptions, FlushType, Settings},
};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Default)]
struct Trace {
    inner: memory::InMemory,
    counts: Mutex<BTreeMap<String, usize>>,
}
impl Trace {
    fn count(&self, op: &str) {
        *self.counts.lock().unwrap().entry(op.into()).or_default() += 1;
    }
    fn stats(&self) -> BTreeMap<String, usize> {
        self.counts.lock().unwrap().clone()
    }
}
impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("index contract fixture")
    }
}
#[async_trait]
impl ObjectStore for Trace {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let kind = if path.as_ref().contains("/wal/") {
            "wal"
        } else if path.as_ref().contains("/manifest/") {
            "manifest"
        } else {
            "other"
        };
        let mode = match &options.mode {
            PutMode::Create => "create",
            PutMode::Update(_) => "update",
            PutMode::Overwrite => "overwrite",
        };
        self.count(&format!("put.{kind}.{mode}"));
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.count("multipart");
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.count("get");
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.count("delete_stream");
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.count("list");
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.count("list_delimiter");
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.count("copy");
        self.inner.copy_opts(from, to, options).await
    }
}

async fn probe() -> anyhow::Result<()> {
    let store = Arc::new(Trace::default());
    let settings = Settings {
        flush_interval: None,
        compactor_options: None,
        l0_sst_size_bytes: 1024 * 1024,
        max_unflushed_bytes: 4 * 1024 * 1024,
        manifest_poll_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let db = Db::builder("probe", store.clone())
        .with_settings(settings.clone())
        .build()
        .await?;
    let before = db.snapshot()?;
    let first = db.put(b"key", b"one").await?;
    let first_view = db.snapshot()?;
    let second = db.put(b"key", b"two").await?;
    let second_view = db.snapshot()?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), first.await_durable())
            .await
            .is_err()
    );
    assert_eq!(before.get(b"key").await?, None);
    assert_eq!(
        first_view.get(b"key").await?.as_deref(),
        Some(b"one".as_slice())
    );
    assert_eq!(
        second_view.get(b"key").await?.as_deref(),
        Some(b"two".as_slice())
    );
    db.flush().await?;
    first.await_durable().await?;
    second.await_durable().await?;
    let io_before = store.stats();
    for _ in 0..1000 {
        assert_eq!(
            first_view.get(b"key").await?.as_deref(),
            Some(b"one".as_slice())
        );
        assert_eq!(
            second_view.get(b"key").await?.as_deref(),
            Some(b"two".as_slice())
        );
    }
    assert_eq!(io_before, store.stats());
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    assert_eq!(
        first_view.get(b"key").await?.as_deref(),
        Some(b"one".as_slice())
    );
    drop((before, first_view, second_view));
    db.close().await?;
    drop(db);
    let reopened = Db::builder("probe", store.clone())
        .with_settings(settings)
        .build()
        .await?;
    assert_eq!(
        reopened.get(b"key").await?.as_deref(),
        Some(b"two".as_slice())
    );
    reopened.close().await?;
    println!(
        "{}",
        serde_json::json!({"slatedb":"0.18.1","rust":env!("PROBE_RUSTC"),
        "scope":"InMemory object store; dependency/snapshot contract probe only",
        "stable_captured_snapshots":true,"pending_handle_waited_for_wal":true,
        "warm_reads_without_object_calls":2000,"memtable_flush_and_reopen":true,
        "object_operations":store.stats(),"b2_compatibility_established":false})
    );
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), probe()).await??;
    Ok(())
}
