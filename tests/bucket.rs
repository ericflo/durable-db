//! The storage contract and the engine against a real S3-compatible bucket.
//!
//! Skipped unless these are set (see `docs/development.md`):
//! `DDB_S3_ENDPOINT`, `DDB_S3_BUCKET`, `DDB_S3_ACCESS_KEY_ID`,
//! `DDB_S3_SECRET_ACCESS_KEY`, and `DDB_S3_REGION` unless the endpoint names it.
//! Every test works under its own random prefix and removes what it wrote.
use bytes::Bytes;
use durable_db::{
    Error,
    db::{Ctx, Db, Options, ReadReq, Scan, Txn},
    io::Io,
    layout::DbConfig,
    store::{
        ObjectStore, list_all,
        s3::{S3Config, S3Store},
    },
};
use std::sync::Arc;

fn bucket() -> Option<(Arc<dyn ObjectStore>, String)> {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let (Some(endpoint), Some(bucket)) = (var("DDB_S3_ENDPOINT"), var("DDB_S3_BUCKET")) else {
        eprintln!("skipped: no bucket configured");
        return None;
    };
    let region = var("DDB_S3_REGION").unwrap_or_else(|| "us-east-1".into());
    let store = S3Store::new(S3Config {
        endpoint,
        region,
        bucket,
        access_key_id: var("DDB_S3_ACCESS_KEY_ID").expect("DDB_S3_ACCESS_KEY_ID"),
        secret_access_key: var("DDB_S3_SECRET_ACCESS_KEY").expect("DDB_S3_SECRET_ACCESS_KEY"),
    })
    .unwrap();
    let store = if store.is_backblaze() && var("DDB_S3_UPLOADS_ONLY").is_none() {
        store.with_b2_uploads()
    } else {
        store
    };
    Some((
        Arc::new(store),
        format!("test-{:016x}/", rand::random::<u64>()),
    ))
}

async fn keys(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    list_all(store, prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|item| item.key)
        .collect()
}

#[tokio::test]
async fn the_bucket_honors_the_storage_contract() {
    let Some((store, root)) = bucket() else {
        return;
    };
    let key = |name: &str| format!("{root}{name}");
    let all = |_: &str| true;
    assert_eq!(store.get(&key("a/1")).await.unwrap(), None);
    assert!(!store.exists(&key("a/1")).await.unwrap());

    store
        .put(&key("a/1"), Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    store
        .put(&key("a/2"), Bytes::from_static(b"two"))
        .await
        .unwrap();
    store
        .put(&key("b/~"), Bytes::from_static(b"tilde"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&key("a/1")).await.unwrap().as_deref(),
        Some(&b"0123456789"[..])
    );
    assert_eq!(
        store.get_range(&key("a/1"), 2, 5).await.unwrap().as_deref(),
        Some(&b"234"[..])
    );
    assert_eq!(
        store
            .get_range(&key("a/1"), 8, 99)
            .await
            .unwrap()
            .as_deref(),
        Some(&b"89"[..])
    );
    assert_eq!(
        store
            .get_range(&key("a/1"), 50, 60)
            .await
            .unwrap()
            .as_deref(),
        Some(&b""[..])
    );
    assert_eq!(store.get_range(&key("missing"), 0, 5).await.unwrap(), None);
    assert!(store.exists(&key("b/~")).await.unwrap());

    assert_eq!(
        keys(&*store, &root).await,
        [key("a/1"), key("a/2"), key("b/~")]
    );
    let after = store.list(&key("a/"), Some(&key("a/1")), 10).await.unwrap();
    assert_eq!(
        after
            .iter()
            .map(|i| (i.key.clone(), i.size))
            .collect::<Vec<_>>(),
        [(key("a/2"), 3)]
    );
    assert_eq!(store.list(&root, None, 2).await.unwrap().len(), 2);
    assert_eq!(store.list_dirs(&root, None, 10).await.unwrap(), ["a", "b"]);
    assert_eq!(store.list_dirs(&root, Some("a"), 10).await.unwrap(), ["b"]);

    // Overwriting leaves versions behind on a versioned bucket; a delete
    // hides the key at once, and a purge removes every trace.
    store
        .put(&key("a/1"), Bytes::from_static(b"again"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&key("a/1")).await.unwrap().as_deref(),
        Some(&b"again"[..])
    );
    store.delete(&key("a/1")).await.unwrap();
    store.delete(&key("a/1")).await.unwrap();
    assert!(!store.exists(&key("a/1")).await.unwrap());
    assert_eq!(keys(&*store, &key("a/")).await, [key("a/2")]);

    let end = key("b/");
    assert!(store.purge(&root, Some(&end), &all).await.unwrap() >= 1);
    assert_eq!(keys(&*store, &root).await, [key("b/~")]);
    assert!(
        !store.exists(&key("a/1")).await.unwrap(),
        "purging must not resurrect a deleted key"
    );
    store.purge(&root, None, &all).await.unwrap();
    assert!(keys(&*store, &root).await.is_empty());
    assert_eq!(store.purge(&root, None, &all).await.unwrap(), 0);
}

/// The one assumption the protocol makes about storage: a write or delete
/// that has returned is seen by every read that starts afterwards.
#[tokio::test]
async fn reads_observe_completed_writes_and_deletes() {
    let Some((store, root)) = bucket() else {
        return;
    };
    let rounds: usize = std::env::var("DDB_CONSISTENCY_ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(12);
    let mut workers = Vec::new();
    for worker in 0..8 {
        let (store, root) = (store.clone(), root.clone());
        workers.push(tokio::spawn(async move {
            for round in 0..rounds {
                let prefix = format!("{root}w{worker}/r{round}/");
                let key = format!("{prefix}claim");
                // A single upload to a Backblaze pod can fail; the engine
                // retries, and so does this.
                let body = Bytes::from(format!("{worker}-{round}"));
                let mut stored = store.put(&key, body.clone()).await;
                for _ in 0..5 {
                    if stored.is_ok() {
                        break;
                    }
                    stored = store.put(&key, body.clone()).await;
                }
                stored.unwrap();
                // Checked the way the protocol uses them: HEAD, GET and LIST.
                let (exists, body, listed) = tokio::join!(
                    store.exists(&key),
                    store.get(&key),
                    store.list(&prefix, None, 10)
                );
                assert!(exists.unwrap(), "{key}: HEAD missed a completed PUT");
                assert_eq!(
                    body.unwrap().as_deref(),
                    Some(format!("{worker}-{round}").as_bytes()),
                    "{key}: GET"
                );
                assert_eq!(
                    listed.unwrap().len(),
                    1,
                    "{key}: LIST missed a completed PUT"
                );
                store.delete(&key).await.unwrap();
                let (exists, body, listed) = tokio::join!(
                    store.exists(&key),
                    store.get(&key),
                    store.list(&prefix, None, 10)
                );
                assert!(!exists.unwrap(), "{key}: HEAD saw a deleted object");
                assert_eq!(body.unwrap(), None, "{key}: GET saw a deleted object");
                assert!(
                    listed.unwrap().is_empty(),
                    "{key}: LIST saw a deleted object"
                );
            }
        }));
    }
    for worker in workers {
        worker.await.unwrap();
    }
    store.purge(&root, None, &|_| true).await.unwrap();
    assert!(keys(&*store, &root).await.is_empty());
}

#[tokio::test]
async fn the_engine_runs_on_the_bucket() {
    let Some((store, root)) = bucket() else {
        return;
    };
    let ctx = |store: &Arc<dyn ObjectStore>| {
        Arc::new(Ctx {
            io: Io::new(store.clone(), 8 << 20, None),
            opts: Options {
                flush_frames: 6,
                upload_attempts: 2,
                ..Options::default()
            },
            root: root.clone(),
        })
    };
    let b = |text: &str| Bytes::copy_from_slice(text.as_bytes());
    let put = |key: &str, value: &str| Txn {
        writes: vec![(b(key), Some(b(value)))],
        ..Txn::default()
    };
    let everything = ReadReq {
        scans: vec![Scan {
            lo: Bytes::from_static(&[1]),
            hi: Bytes::new(),
            reverse: false,
            limit: 1000,
        }],
        strong: true,
        ..ReadReq::default()
    };

    let first = ctx(&store);
    let (db, created) = Db::open(first.clone(), "app", Some(DbConfig::default()))
        .await
        .unwrap();
    assert!(created);
    for i in 0..20 {
        let results = db
            .commit(vec![put(&format!("k{i:02}"), &format!("{{\"n\":{i}}}"))])
            .await
            .unwrap();
        assert_eq!(results[0].as_ref().unwrap().revision, i + 1);
    }
    // Another node takes over without the first one's cooperation.
    let (successor, _) = Db::open(ctx(&store), "app", None).await.unwrap();
    assert!(matches!(
        db.commit(vec![put("zombie", "1")]).await,
        Err(Error::Lost(_))
    ));
    let read = successor.read(everything.clone()).await.unwrap();
    assert_eq!((read.revision, read.scans[0].items.len()), (20, 20));
    successor.commit(vec![put("k20", "{}")]).await.unwrap();
    successor.close().await;
    // Released: no claim remains. The revoked owner may have left a frame
    // or a manifest behind after the successor swept; the next owner
    // collects those.
    let left = keys(&*store, &root).await;
    assert!(left.iter().all(|key| !key.contains("/c/")), "{left:#?}");

    let (reopened, _) = Db::open(ctx(&store), "app", None).await.unwrap();
    assert_eq!(
        reopened.read(everything.clone()).await.unwrap().scans[0]
            .items
            .len(),
        21
    );
    reopened.close().await;
    // Now the bucket holds one manifest and the tables it names.
    let left = keys(&*store, &root).await;
    assert!(
        left.iter()
            .all(|key| key.contains("/m/") || key.contains("/t/")),
        "{left:#?}"
    );
    assert_eq!(
        left.iter().filter(|key| key.contains("/m/")).count(),
        1,
        "{left:#?}"
    );

    let (reopened, _) = Db::open(ctx(&store), "app", None).await.unwrap();
    assert_eq!(
        reopened.read(everything).await.unwrap().scans[0]
            .items
            .len(),
        21
    );
    reopened.destroy().await.unwrap();
    assert_eq!(keys(&*store, &root).await, [format!("{root}db/app/m/~")]);
    store.purge(&root, None, &|_| true).await.unwrap();
}
