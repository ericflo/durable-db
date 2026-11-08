use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::*,
    storage::{Local, Object, Remote, Storage, Version},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Notify;

fn cache() -> Arc<Cache> {
    Arc::new(Cache::new(1024 * 1024, None).unwrap())
}
fn remote() -> Arc<Remote> {
    Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "test").unwrap())
}
fn request(revision: u64, id: &str, key: &str, value: Option<&[u8]>) -> CommitRequest {
    CommitRequest {
        expected_revision: revision,
        request_id: id.into(),
        checks: vec![],
        writes: vec![Write {
            key: key.into(),
            value: value.map(|v| STANDARD.encode(v)),
        }],
    }
}
async fn value(engine: &Engine, key: &str) -> Option<Vec<u8>> {
    let read = engine
        .read(
            "test",
            &ReadRequest {
                keys: vec![key.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    read.values[0].as_ref().map(|v| STANDARD.decode(v).unwrap())
}

#[tokio::test]
async fn atomicity_receipts_revisions_checks_and_isolation() {
    let storage = remote();
    storage.probe().await.unwrap();
    let engine = Engine::new(storage, cache());
    engine.create("test", Limits::default()).await.unwrap();
    engine
        .create("test-other", Limits::default())
        .await
        .unwrap();
    let mut req = request(0, "first", "a", Some(b"hello"));
    req.writes.push(Write {
        key: "b".into(),
        value: Some(STANDARD.encode(b"world")),
    });
    assert_eq!(engine.commit("test", &req).await.unwrap().revision, 1);
    assert!(engine.commit("test", &req).await.unwrap().replayed);
    assert_eq!(engine.info("test").await.unwrap().keys, 2);
    assert_eq!(engine.info("test-other").await.unwrap().keys, 0);
    assert!(matches!(
        engine.commit("test", &request(0, "stale", "a", None)).await,
        Err(Error::Conflict)
    ));
    req.expected_revision = 1;
    assert!(matches!(
        engine.commit("test", &req).await,
        Err(Error::Invalid(_))
    ));
    let mut req = request(1, "check", "a", None);
    req.checks.push(Check {
        key: "b".into(),
        value: None,
    });
    assert!(matches!(
        engine.commit("test", &req).await,
        Err(Error::Conflict)
    ));
    assert_eq!(value(&engine, "a").await.unwrap(), b"hello");
    req.checks[0].value = Some(STANDARD.encode(b"world"));
    engine.commit("test", &req).await.unwrap();
    assert!(value(&engine, "a").await.is_none());
}

#[tokio::test]
async fn quotas_refuse_before_upload_and_keys_cannot_escape_namespace() {
    let storage = remote();
    let engine = Engine::new(storage.clone(), cache());
    engine
        .create(
            "test",
            Limits {
                max_bytes: 10,
                max_keys: 1,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        engine
            .commit("test", &request(0, "big", "a", Some(b"0123456789")))
            .await,
        Err(Error::Limit(_))
    ));
    assert!(
        storage
            .list("blocks/test.", None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(engine.info("test").await.unwrap().revision, 0);
    assert!(engine.create("../escape", Limits::default()).await.is_err());
    assert!(
        engine
            .commit("test", &request(0, "nul", "\0hidden", Some(b"x")))
            .await
            .is_err()
    );
    // User key syntax has no filesystem interpretation.
    engine
        .commit("test", &request(0, "path", "../x", Some(b"yes")))
        .await
        .unwrap();
    assert_eq!(value(&engine, "../x").await.unwrap(), b"yes");
}

#[tokio::test]
async fn ordered_scans_across_split_pages_have_stable_exclusive_cursors() {
    let engine = Engine::new(remote(), cache());
    engine.create("test", Limits::default()).await.unwrap();
    let mut req = request(0, "many", "unused", None);
    req.writes = (0..100)
        .map(|n| Write {
            key: format!("entries/{n:04}"),
            value: Some(STANDARD.encode(vec![n as u8; 4096])),
        })
        .collect();
    engine.commit("test", &req).await.unwrap();
    assert!(engine.info("test").await.unwrap().pages > 1);
    for reverse in [false, true] {
        let mut after = None;
        let mut keys = Vec::new();
        loop {
            let result = engine
                .read(
                    "test",
                    &ReadRequest {
                        expected_revision: Some(1),
                        keys: vec![],
                        scans: vec![Scan {
                            prefix: "entries/".into(),
                            start: Some("entries/0010".into()),
                            end: Some("entries/0080".into()),
                            after,
                            reverse,
                            limit: 9,
                        }],
                    },
                )
                .await
                .unwrap();
            let scan = &result.scans[0];
            keys.extend(scan.items.iter().map(|p| p.key.clone()));
            after = scan.next.clone();
            if after.is_none() {
                break;
            }
        }
        let mut expected: Vec<_> = (10..80).map(|n| format!("entries/{n:04}")).collect();
        if reverse {
            expected.reverse();
        }
        assert_eq!(keys, expected);
    }
    engine
        .commit("test", &request(1, "earliest", "a", Some(b"new")))
        .await
        .unwrap();
    assert_eq!(value(&engine, "a").await.unwrap(), b"new");
    assert!(matches!(
        engine
            .read(
                "test",
                &ReadRequest {
                    expected_revision: Some(1),
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Conflict)
    ));
}

struct Faults {
    inner: Arc<dyn Storage>,
    pause: AtomicBool,
    ambiguous: AtomicBool,
    reached: Notify,
    resume: Notify,
}
impl Faults {
    fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            pause: AtomicBool::new(false),
            ambiguous: AtomicBool::new(false),
            reached: Notify::new(),
            resume: Notify::new(),
        }
    }
}
#[async_trait]
impl Storage for Faults {
    async fn get(&self, k: &str, m: usize) -> anyhow::Result<Option<Object>> {
        self.inner.get(k, m).await
    }
    async fn cas(&self, k: &str, v: Option<&Version>, b: Bytes) -> anyhow::Result<bool> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.cas(k, v, b).await?;
        if self.ambiguous.swap(false, Ordering::SeqCst) {
            anyhow::bail!("connection lost after committed CAS")
        }
        Ok(result)
    }
    async fn put(&self, k: &str, b: Bytes) -> anyhow::Result<()> {
        self.inner.put(k, b).await
    }
    async fn list(&self, p: &str, a: Option<&str>, l: usize) -> anyhow::Result<Vec<String>> {
        self.inner.list(p, a, l).await
    }
    async fn delete(&self, k: &str) -> anyhow::Result<()> {
        self.inner.delete(k).await
    }
}

#[tokio::test]
async fn collector_fences_a_paused_writer_before_deleting_its_upload() {
    let store = Arc::new(Faults::new(remote()));
    let a = Arc::new(Engine::new(store.clone(), cache()));
    let b = Engine::new(store.clone(), cache());
    a.create("test", Limits::default()).await.unwrap();
    store.pause.store(true, Ordering::SeqCst);
    let writer = tokio::spawn({
        let a = a.clone();
        async move {
            a.commit("test", &request(0, "paused", "x", Some(b"value")))
                .await
        }
    });
    store.reached.notified().await;
    assert_eq!(b.gc("test").await.unwrap().deleted, 1);
    store.resume.notify_one();
    assert!(matches!(writer.await.unwrap(), Err(Error::Conflict)));
    assert_eq!(b.info("test").await.unwrap().revision, 0);
    a.commit("test", &request(0, "retry", "x", Some(b"value")))
        .await
        .unwrap();
    assert_eq!(value(&b, "x").await.unwrap(), b"value");
}

#[tokio::test]
async fn lost_acknowledgement_is_resolved_without_reapplying_or_deleting_live_data() {
    let store = Arc::new(Faults::new(remote()));
    let a = Engine::new(store.clone(), cache());
    a.create("test", Limits::default()).await.unwrap();
    store.ambiguous.store(true, Ordering::SeqCst);
    let req = request(0, "lost", "x", Some(b"value"));
    assert!(matches!(
        a.commit("test", &req).await,
        Err(Error::Storage(_))
    ));
    let b = Engine::new(store.clone(), cache());
    b.gc("test").await.unwrap();
    assert!(b.commit("test", &req).await.unwrap().replayed);
    assert_eq!(b.info("test").await.unwrap().revision, 1);
    assert_eq!(value(&b, "x").await.unwrap(), b"value");
}

#[tokio::test]
async fn replicas_compete_with_collection_and_survive_complete_cache_loss() {
    let storage = remote();
    let bootstrap = Engine::new(storage.clone(), cache());
    bootstrap.create("test", Limits::default()).await.unwrap();
    let mut tasks = Vec::new();
    for node in 0..3 {
        let storage = storage.clone();
        tasks.push(tokio::spawn(async move {
            let engine = Engine::new(storage, cache());
            for operation in 0..20 {
                loop {
                    let revision = engine.info("test").await.unwrap().revision;
                    let req = request(
                        revision,
                        &format!("{node}-{operation}-{revision}"),
                        &format!("{node}/{operation}"),
                        Some(b"v"),
                    );
                    match engine.commit("test", &req).await {
                        Ok(_) => break,
                        Err(Error::Conflict) => tokio::task::yield_now().await,
                        Err(e) => panic!("unexpected {e:?}"),
                    }
                }
            }
        }));
    }
    for _ in 0..30 {
        let _ = bootstrap.gc("test").await;
        tokio::task::yield_now().await;
    }
    for task in tasks {
        task.await.unwrap();
    }
    bootstrap.gc("test").await.unwrap();
    let recovered = Engine::new(storage, Arc::new(Cache::new(0, None).unwrap()));
    let info = recovered.info("test").await.unwrap();
    assert_eq!(info.revision, 60);
    assert_eq!(info.keys, 60);
    for node in 0..3 {
        for op in 0..20 {
            assert_eq!(
                value(&recovered, &format!("{node}/{op}")).await.unwrap(),
                b"v"
            );
        }
    }
}

#[tokio::test]
async fn local_reopen_and_exclusive_owner() {
    let dir = tempfile::tempdir().unwrap();
    {
        let storage = Arc::new(Local::open(dir.path().into()).unwrap());
        assert!(Local::open(dir.path().into()).is_err());
        let engine = Engine::new(storage, cache());
        engine.create("test", Limits::default()).await.unwrap();
        engine
            .commit("test", &request(0, "local", "key", Some(b"durable")))
            .await
            .unwrap();
    }
    let storage = Arc::new(Local::open(dir.path().into()).unwrap());
    let engine = Engine::new(storage, cache());
    assert_eq!(value(&engine, "key").await.unwrap(), b"durable");
    engine.gc("test").await.unwrap();
}

#[tokio::test]
async fn cache_budgets_and_corrupt_disk_cache_are_not_authority() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::new(1024, Some((dir.path().into(), 2048))).unwrap();
    use sha2::{Digest, Sha256};
    for i in 0..100 {
        cache
            .insert(&format!("k{i}"), Bytes::from(vec![7; 512]))
            .await;
    }
    assert!(cache.memory_bytes().await <= 1024);
    let bytes = Bytes::from(vec![9; 512]);
    cache.insert("target", bytes.clone()).await;
    drop(cache);
    let name = hex::encode(Sha256::digest(b"target"));
    std::fs::write(dir.path().join(name), b"corrupted").unwrap();
    let cache = Cache::new(0, Some((dir.path().into(), 2048))).unwrap();
    assert!(
        cache
            .get("target", &hex::encode(Sha256::digest(&bytes)))
            .await
            .is_none()
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    #[test]
    fn random_updates_match_a_reference_map(ops in proptest::collection::vec(("[a-z]{1,5}",proptest::option::of(proptest::collection::vec(proptest::num::u8::ANY,0..100))),1..80)) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let engine=Engine::new(remote(),cache());
            engine.create("test",Limits::default()).await.unwrap();
            let mut expected=BTreeMap::new();
            for (i,(key,val)) in ops.iter().enumerate() {
                engine.commit("test",&request(i as u64,&format!("op{i}"),key,val.as_deref())).await.unwrap();
                if let Some(v)=val { expected.insert(key.clone(),v.clone()); } else { expected.remove(key); }
                if i%7==0 { engine.gc("test").await.unwrap(); }
            }
            let result=engine.read("test",&ReadRequest { scans:vec![Scan { prefix:"".into(),start:None,end:None,after:None,reverse:false,limit:1000 }], ..Default::default() }).await.unwrap();
            let actual:BTreeMap<_,_>=result.scans[0].items.iter().map(|p|(p.key.clone(),STANDARD.decode(&p.value).unwrap())).collect();
            assert_eq!(actual,expected);
        });
    }
}

#[tokio::test]
async fn compaction_preserves_logical_revision_and_snapshot_contents() {
    let engine = Engine::new(remote(), cache());
    engine.create("test", Limits::default()).await.unwrap();
    let writes = (0..80)
        .map(|n| Write {
            key: format!("k{n:03}"),
            value: Some(STANDARD.encode(vec![7; 4096])),
        })
        .collect();
    engine
        .commit(
            "test",
            &CommitRequest {
                expected_revision: 0,
                request_id: "seed".into(),
                checks: vec![],
                writes,
            },
        )
        .await
        .unwrap();
    let writes = (0..80)
        .filter(|n| n % 12 != 0)
        .map(|n| Write {
            key: format!("k{n:03}"),
            value: None,
        })
        .collect();
    engine
        .commit(
            "test",
            &CommitRequest {
                expected_revision: 1,
                request_id: "shrink".into(),
                checks: vec![],
                writes,
            },
        )
        .await
        .unwrap();
    let before = engine.info("test").await.unwrap();
    assert!(before.pages > 1);
    let req = ReadRequest {
        scans: vec![Scan {
            limit: 100,
            prefix: String::new(),
            start: None,
            end: None,
            after: None,
            reverse: false,
        }],
        ..Default::default()
    };
    let snapshot = serde_json::to_value(engine.read("test", &req).await.unwrap()).unwrap();
    for _ in 0..8 {
        engine.compact("test").await.unwrap();
    }
    engine.gc("test").await.unwrap();
    let after = engine.info("test").await.unwrap();
    assert_eq!(before.revision, after.revision);
    assert_eq!(before.bytes, after.bytes);
    assert_eq!(after.pages, 1);
    assert_eq!(
        snapshot,
        serde_json::to_value(engine.read("test", &req).await.unwrap()).unwrap()
    );
}

#[tokio::test]
async fn response_budget_includes_json_key_escaping() {
    let engine = Engine::new(remote(), cache());
    engine.create("test", Limits::default()).await.unwrap();
    for batch in 0..2 {
        let writes = (0..50)
            .map(|n| Write {
                key: format!("{:03}{}", batch * 50 + n, "\u{0001}".repeat(900)),
                value: Some(STANDARD.encode(vec![1; 5000])),
            })
            .collect();
        engine
            .commit(
                "test",
                &CommitRequest {
                    expected_revision: batch,
                    request_id: format!("batch{batch}"),
                    checks: vec![],
                    writes,
                },
            )
            .await
            .unwrap();
    }
    let result = engine
        .read(
            "test",
            &ReadRequest {
                scans: vec![Scan {
                    prefix: String::new(),
                    start: None,
                    end: None,
                    after: None,
                    reverse: false,
                    limit: 100,
                }],
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(Error::Limit(_))));
}
