use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::*,
    publication::{Authority, Bucket, Published},
    storage::{Remote, Storage},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, Notify};

struct Metadata {
    state: Mutex<Bucket>,
    files: Arc<dyn Storage>,
    pause: AtomicBool,
    lost_ack: AtomicBool,
    paused: Notify,
    resume: Notify,
}
#[async_trait]
impl Authority for Metadata {
    async fn read(&self) -> anyhow::Result<Bucket> {
        Ok(self.state.lock().await.clone())
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let is_publication =
            info.get("durable-db-read-fence") == old.info.get("durable-db-read-fence");
        if is_publication && self.pause.swap(false, Ordering::SeqCst) {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        let mut state = self.state.lock().await;
        if state.revision != old.revision {
            return Ok(None);
        }
        let publication =
            info.get("durable-db-read-fence") == old.info.get("durable-db-read-fence");
        state.revision += 1;
        state.info = info;
        if publication && self.lost_ack.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost response after bucket publication");
        }
        Ok(Some(state.clone()))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        self.files.delete(key).await
    }
}
async fn setup() -> (Arc<Published>, Arc<Metadata>) {
    let files =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap());
    let metadata = Arc::new(Metadata {
        state: Mutex::new(Bucket {
            version: None,
            revision: 0,
            info: BTreeMap::from([("unrelated".into(), "preserved".into())]),
        }),
        files: files.clone(),
        pause: AtomicBool::new(false),
        lost_ack: AtomicBool::new(false),
        paused: Notify::new(),
        resume: Notify::new(),
    });
    (
        Arc::new(
            Published::open(files, metadata.clone(), "test")
                .await
                .unwrap(),
        ),
        metadata,
    )
}
fn engine(b2: Arc<Published>) -> Engine {
    Engine::new(b2, Arc::new(Cache::new(0, None).unwrap()))
}
fn request(revision: u64) -> CommitRequest {
    CommitRequest {
        expected_revision: revision,
        request_id: format!("r{revision}"),
        checks: vec![],
        writes: vec![Write {
            key: "a".into(),
            value: Some("aGVsbG8=".into()),
        }],
    }
}
#[tokio::test]
async fn native_authority_preserves_metadata_and_compares_database_identity() {
    let (b2, metadata) = setup().await;
    assert!(
        b2.cas("heads/a.json", None, Bytes::from_static(b"one"))
            .await
            .unwrap()
    );
    let old = b2.get("heads/a.json", 100).await.unwrap().unwrap();
    assert!(
        b2.cas("heads/b.json", None, Bytes::from_static(b"other"))
            .await
            .unwrap()
    );
    assert!(
        b2.cas(
            "heads/a.json",
            Some(&old.version),
            Bytes::from_static(b"two")
        )
        .await
        .unwrap()
    );
    assert_eq!(
        b2.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"two"[..]
    );
    assert_eq!(
        metadata.read().await.unwrap().info["unrelated"],
        "preserved"
    );
    assert!(
        Published::open(metadata.files.clone(), metadata.clone(), "wrong-prefix")
            .await
            .is_err()
    );
}
#[tokio::test]
async fn manifest_collector_fences_a_paused_publisher_and_reclaims_its_orphan() {
    let (b2, metadata) = setup().await;
    b2.cas("heads/a.json", None, Bytes::from_static(b"one"))
        .await
        .unwrap();
    let old = b2.get("heads/a.json", 100).await.unwrap().unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let b2 = b2.clone();
        async move {
            b2.cas(
                "heads/a.json",
                Some(&old.version),
                Bytes::from_static(b"two"),
            )
            .await
        }
    });
    metadata.paused.notified().await;
    assert_eq!(b2.collect_manifests().await.unwrap(), 1);
    metadata.resume.notify_one();
    assert!(!task.await.unwrap().unwrap());
    assert_eq!(
        b2.get("heads/a.json", 100).await.unwrap().unwrap().bytes,
        b"one"[..]
    );
    assert_eq!(
        metadata
            .files
            .list("manifests/", None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn lost_bucket_ack_recovers_receipt_after_both_collectors_and_empty_cache() {
    let (b2, metadata) = setup().await;
    let e = engine(b2.clone());
    e.create("test", Limits::default()).await.unwrap();
    metadata.lost_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        e.commit("test", &request(0)).await,
        Err(Error::Storage(_))
    ));
    e.gc("test").await.unwrap();
    b2.collect_manifests().await.unwrap();
    let reopened = engine(b2.clone());
    assert!(reopened.commit("test", &request(0)).await.unwrap().replayed);
    assert_eq!(
        reopened
            .read(
                "test",
                &ReadRequest {
                    keys: vec!["a".into()],
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .values[0]
            .as_deref(),
        Some("aGVsbG8=")
    );
    assert_eq!(
        metadata
            .files
            .list("manifests/", None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn bounded_slots_reject_before_manifest_upload() {
    let (b2, metadata) = setup().await;
    for n in 0..32 {
        assert!(
            b2.cas(&format!("heads/d{n}.json"), None, Bytes::from_static(b"x"))
                .await
                .unwrap()
        );
    }
    let before = metadata.files.list("manifests/", None, 100).await.unwrap();
    assert!(
        b2.cas("heads/excess.json", None, Bytes::from_static(b"x"))
            .await
            .is_err()
    );
    assert_eq!(
        before,
        metadata.files.list("manifests/", None, 100).await.unwrap()
    );
}
#[tokio::test]
async fn multiple_stateless_replicas_survive_both_collectors_and_reopen() {
    let (b2, _) = setup().await;
    engine(b2.clone())
        .create("test", Limits::default())
        .await
        .unwrap();
    let mut tasks = vec![];
    for worker in 0..3 {
        let b2 = b2.clone();
        tasks.push(tokio::spawn(async move {
            let e = engine(b2);
            for op in 0..12 {
                loop {
                    let r = e.info("test").await.unwrap().revision;
                    let mut req = request(r);
                    req.request_id = format!("worker-{worker}-op-{op}");
                    match e.commit("test", &req).await {
                        Ok(_) => break,
                        Err(Error::Conflict) => {}
                        Err(e) => panic!("{e}"),
                    }
                }
            }
        }));
    }
    let gc = tokio::spawn({
        let b2 = b2.clone();
        async move {
            let e = engine(b2.clone());
            for _ in 0..20 {
                let _ = e.gc("test").await;
                b2.collect_manifests().await.unwrap();
                tokio::task::yield_now().await;
            }
        }
    });
    for task in tasks {
        task.await.unwrap();
    }
    gc.await.unwrap();
    let e = engine(b2);
    assert_eq!(e.info("test").await.unwrap().revision, 36);
    assert_eq!(
        e.read(
            "test",
            &ReadRequest {
                keys: vec!["a".into()],
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .values[0]
            .as_deref(),
        Some("aGVsbG8=")
    );
}
