//! Run explicitly against a disposable S3 bucket; never touches another prefix.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use durable_db::{cache::Cache, engine::*, providers::s3::S3, storage::Storage};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires DURABLE_DB_TEST_BUCKET and AWS credentials for a disposable bucket"]
async fn real_s3_conditional_commits_gc_and_reopen() {
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    let prefix = format!("test-{}", uuid::Uuid::new_v4());
    let backend = Arc::new(S3::connect(&bucket, &prefix).await.unwrap());
    let a = Engine::new(backend.clone(), Arc::new(Cache::new(0, None).unwrap()));
    let b = Engine::new(
        Arc::new(S3::connect(&bucket, &prefix).await.unwrap()),
        Arc::new(Cache::new(0, None).unwrap()),
    );
    a.create("sample", Limits::default()).await.unwrap();
    let req = CommitRequest {
        expected_revision: 0,
        request_id: "one".into(),
        checks: vec![],
        writes: vec![Write {
            key: "key".into(),
            value: Some(STANDARD.encode(b"persisted")),
        }],
    };
    a.commit("sample", &req).await.unwrap();
    assert!(b.commit("sample", &req).await.unwrap().replayed);
    for n in 1..5 {
        a.commit(
            "sample",
            &CommitRequest {
                expected_revision: n,
                request_id: format!("r{n}"),
                ..req.clone()
            },
        )
        .await
        .unwrap();
    }
    assert!(b.gc("sample").await.unwrap().deleted >= 4);
    assert!(backend.collect_manifests().await.unwrap() >= 5);
    backend.maintenance().await.unwrap();
    let result = b
        .read(
            "sample",
            &ReadRequest {
                keys: vec!["key".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.revision, 5);
    assert_eq!(result.values[0], Some(STANDARD.encode(b"persisted")));
    for n in 0..70 {
        a.create(&format!("site-{n:03}"), Limits::default())
            .await
            .unwrap();
    }
    for _ in 0..3 {
        backend.maintenance().await.unwrap();
    }
    let reopened = S3::connect(&bucket, &prefix).await.unwrap();
    let mut names = Vec::new();
    loop {
        let page = reopened
            .list("heads/", names.last().map(String::as_str), 32)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        names.extend(page);
    }
    assert_eq!(names.len(), 71);
    for n in [0, 31, 32, 63, 69] {
        assert!(
            reopened
                .get(&format!("heads/site-{n:03}.json"), 512 * 1024)
                .await
                .unwrap()
                .is_some()
        );
    }
    eprintln!("Shared publication protocol passed against S3. Disposable prefix: {prefix}");
}
