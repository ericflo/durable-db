//! Explicit opt-in qualification on a fresh disposable cloud namespace.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use durable_db::{
    Error,
    cache::Cache,
    engine::{CommitRequest, Engine, Limits, ReadRequest, Write},
    providers::{b2::Native, s3::S3},
    publication::Published,
    storage::Storage,
    upload_budget::{UploadLimits, UploadRequest},
};
use std::sync::Arc;

async fn connect(bucket: &str, prefix: &str, limits: Option<UploadLimits>) -> Published {
    if std::env::var("DURABLE_DB_TEST_PROVIDER").as_deref() == Ok("b2") {
        Native::connect_with_budget(bucket, prefix, limits)
            .await
            .unwrap()
    } else {
        S3::connect_with_budget(bucket, prefix, limits)
            .await
            .unwrap()
    }
}
fn engine(storage: Arc<Published>) -> Engine {
    Engine::new(storage, Arc::new(Cache::new(0, None).unwrap()))
}
#[tokio::test]
#[ignore = "requires a fresh disposable bucket and provider credentials"]
async fn cloud_budget_survives_exhaustion_reopen_collection_and_refill() {
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    let prefix = format!("budget-{}", uuid::Uuid::new_v4());
    let limits = UploadLimits {
        bytes: 1024 * 1024,
        attempts: 8,
    };
    let storage = Arc::new(connect(&bucket, &prefix, Some(limits)).await);
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    let commit = CommitRequest {
        expected_revision: 0,
        request_id: "first".into(),
        checks: vec![],
        writes: vec![Write {
            key: "key".into(),
            value: Some(STANDARD.encode(b"persisted")),
        }],
    };
    db.commit("sample", &commit).await.unwrap();
    let orphan = format!("manifests/sample.{}", uuid::Uuid::new_v4());
    storage.put(&orphan, Bytes::new()).await.unwrap();
    let status = storage.upload_budget().await.unwrap().unwrap();
    assert_eq!(status.allocated, limits);
    drop(db);
    drop(storage);

    let storage = Arc::new(connect(&bucket, &prefix, None).await);
    assert_eq!(storage.upload_budget().await.unwrap().unwrap(), status);
    let db = engine(storage.clone());
    let read = db
        .read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(read.revision, 1);
    assert_eq!(read.values, [Some(STANDARD.encode(b"persisted"))]);
    let next = CommitRequest {
        expected_revision: 1,
        request_id: "second".into(),
        ..commit
    };
    assert!(matches!(
        db.commit("sample", &next).await,
        Err(Error::Storage(_))
    ));
    assert!(storage.collect_manifests().await.unwrap() >= 1);
    storage.maintenance().await.unwrap();
    assert!(storage.get(&orphan, 10).await.unwrap().is_none());

    let request = UploadRequest {
        request_id: "fund-next".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 2 * 1024 * 1024,
            attempts: 64,
        },
    };
    let funded = storage.replenish_upload_budget(&request).await.unwrap();
    assert_eq!(funded.status.allocated, status.allocated);
    assert!(!funded.replayed);
    assert!(
        storage
            .replenish_upload_budget(&request)
            .await
            .unwrap()
            .replayed
    );
    db.commit("sample", &next).await.unwrap();
    drop(db);
    drop(storage);
    let storage = Arc::new(connect(&bucket, &prefix, None).await);
    let status = storage.upload_budget().await.unwrap().unwrap();
    assert_eq!(status.configuration_revision, 1);
    assert_eq!(status.limits, request.limits);
    assert_eq!(engine(storage).info("sample").await.unwrap().revision, 2);
    eprintln!("Upload admission qualification passed. Disposable prefix: {prefix}");
}

#[tokio::test]
#[ignore = "requires a fresh disposable bucket and provider credentials"]
async fn cloud_data_collection_progresses_after_exhaustion_and_complete_reopen() {
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    let prefix = format!("budget-{}", uuid::Uuid::new_v4());
    let limits = UploadLimits {
        bytes: 1024 * 1024,
        attempts: 142,
    };
    let storage = Arc::new(connect(&bucket, &prefix, Some(limits)).await);
    let db = engine(storage.clone());
    db.create("sample", Limits::default()).await.unwrap();
    for revision in 0..3 {
        db.commit(
            "sample",
            &CommitRequest {
                expected_revision: revision,
                request_id: format!("write-{revision}"),
                checks: vec![],
                writes: vec![Write {
                    key: "key".into(),
                    value: Some(STANDARD.encode(format!("value-{revision}"))),
                }],
            },
        )
        .await
        .unwrap();
    }
    for _ in 0..127 {
        storage
            .put(
                &format!("blocks/uncreated.{}", uuid::Uuid::new_v4()),
                Bytes::new(),
            )
            .await
            .unwrap();
    }
    let before = storage.upload_budget().await.unwrap().unwrap();
    assert_eq!(before.allocated, limits);
    drop(db);
    drop(storage);
    let storage = Arc::new(connect(&bucket, &prefix, None).await);
    assert!(storage.put("blocks/refused", Bytes::new()).await.is_err());
    let swept = storage.collect_data().await.unwrap();
    assert_eq!((swept.examined, swept.deleted), (128, 127));
    drop(storage);
    let storage = Arc::new(connect(&bucket, &prefix, None).await);
    // The regular cluster maintenance path resumes the durable cursor and
    // removes the remaining two orphans, without obtaining upload credit.
    storage.maintenance().await.unwrap();
    assert_eq!(storage.upload_budget().await.unwrap().unwrap(), before);
    let remaining = storage.list("blocks/", None, 128).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].starts_with("blocks/sample."));
    let read = engine(storage.clone())
        .read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(read.revision, 3);
    assert_eq!(read.values, [Some(STANDARD.encode(b"value-2"))]);
    assert_eq!(storage.collect_data().await.unwrap().deleted, 0);
    eprintln!(
        "Upload-free data collection passed: 142 allocated attempts, 129 orphan pages reclaimed, revision 3 intact. Disposable prefix: {prefix}"
    );
}
