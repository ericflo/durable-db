//! Explicit opt-in: requires a DEDICATED disposable B2 bucket, never a production bucket.
use durable_db::{Error, cache::Cache, engine::*, providers::b2::Native};
use std::sync::Arc;
#[tokio::test]
#[ignore = "requires DURABLE_DB_TEST_B2_BUCKET and B2_APPLICATION_KEY_ID/B2_APPLICATION_KEY"]
async fn real_b2_conditional_authority_versions_and_cache_free_recovery() {
    let bucket =
        std::env::var("DURABLE_DB_TEST_B2_BUCKET").expect("dedicated disposable test bucket");
    let db = format!("cloud-{}", uuid::Uuid::new_v4());
    let storage = Arc::new(
        Native::connect(&bucket, "durable-db-cloud-test")
            .await
            .unwrap(),
    );
    let engine = Engine::new(storage.clone(), Arc::new(Cache::new(0, None).unwrap()));
    engine.create(&db, Limits::default()).await.unwrap();
    for n in 0..5 {
        let req = CommitRequest {
            expected_revision: n,
            request_id: format!("r{n}"),
            checks: vec![],
            writes: vec![Write {
                key: "a".into(),
                value: Some("aGVsbG8=".into()),
            }],
        };
        let mut completed = false;
        for _ in 0..12 {
            match engine.commit(&db, &req).await {
                Ok(r) => {
                    assert_eq!(r.revision, n + 1);
                    completed = true;
                    break;
                }
                Err(Error::Conflict) => {}
                Err(e) => panic!("B2 write failed: {e}"),
            }
        }
        assert!(completed, "B2 contention did not settle");
        assert!(engine.commit(&db, &req).await.unwrap().replayed);
    }
    assert!(engine.gc(&db).await.unwrap().deleted >= 4);
    assert!(storage.collect_manifests().await.unwrap() >= 5);
    let reopened = Engine::new(
        Arc::new(
            Native::connect(&bucket, "durable-db-cloud-test")
                .await
                .unwrap(),
        ),
        Arc::new(Cache::new(0, None).unwrap()),
    );
    let result = reopened
        .read(
            &db,
            &ReadRequest {
                keys: vec!["a".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.revision, 5);
    assert_eq!(result.values[0].as_deref(), Some("aGVsbG8="));
    eprintln!("B2 cloud test passed. Disposable database retained for inspection: {db}");
}
