//! Run explicitly against a disposable S3 bucket; never touches another prefix.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use durable_db::{
    cache::Cache,
    engine::*,
    storage::{Remote, Storage},
};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires DURABLE_DB_TEST_BUCKET and AWS credentials for a disposable bucket"]
async fn real_s3_conditional_commits_gc_and_reopen() {
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    let prefix = format!("test-{}", uuid::Uuid::new_v4());
    let backend = Arc::new(
        Remote::new(
            Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .unwrap(),
            ),
            &prefix,
        )
        .unwrap(),
    );
    backend.probe().await.unwrap();
    let a = Engine::new(backend.clone(), Arc::new(Cache::new(0, None).unwrap()));
    let b = Engine::new(backend.clone(), Arc::new(Cache::new(0, None).unwrap()));
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
    for key in backend.list("blocks/", None, 1000).await.unwrap() {
        backend.delete(&key).await.unwrap();
    }
    backend.delete("heads/sample.json").await.unwrap();
}
