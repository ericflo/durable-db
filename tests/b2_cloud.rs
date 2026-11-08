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

#[tokio::test]
#[ignore = "requires a disposable real B2 bucket and scoped workload credentials"]
async fn real_b2_directory_split_and_cache_free_reopen() {
    use durable_db::storage::Storage;
    let bucket = std::env::var("DURABLE_DB_TEST_B2_BUCKET").unwrap();
    let storage = Arc::new(
        Native::connect(&bucket, "durable-db-cloud-test")
            .await
            .unwrap()
            .with_cache(Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap())),
    );
    let engine = Engine::new(storage.clone(), Arc::new(Cache::new(0, None).unwrap()));
    let group = format!("directory-{}", uuid::Uuid::new_v4());
    for n in 0..70 {
        let name = format!("{group}-{n:03}");
        engine.create(&name, Limits::default()).await.unwrap();
    }
    for _ in 0..3 {
        storage.collect_manifests().await.unwrap();
    }
    let reopened = Native::connect(&bucket, "durable-db-cloud-test")
        .await
        .unwrap();
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
    assert_eq!(names.iter().filter(|s| s.contains(&group)).count(), 70);
    for n in [0, 31, 32, 63, 69] {
        assert!(
            reopened
                .get(&format!("heads/{group}-{n:03}.json"), 512 * 1024)
                .await
                .unwrap()
                .is_some()
        );
    }
    eprintln!(
        "Real B2 directory split, collection, pagination and empty-cache reopen passed with 70 databases."
    );
}
