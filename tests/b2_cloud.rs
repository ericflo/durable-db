//! Explicit opt-in: requires a DEDICATED disposable B2 bucket, never a production bucket.
use durable_db::{Error, cache::Cache, engine::*, providers::b2::Native, storage::Storage};
use std::sync::Arc;

// Both opt-in tests share a bucket and run concurrently. A collector in one
// test can legitimately fence a submitted proposal in the other. These tests
// exclusively own fresh database names, so observing that name can resolve an
// uncertain create. Production create idempotency remains a separate API gate.
fn proposal_race(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("outcome is uncertain")
        || message.starts_with("directory snapshot was collected")
}
async fn create(engine: &Engine, db: &str) {
    for attempt in 0..12 {
        match engine.create(db, Limits::default()).await {
            Ok(_) => return,
            Err(Error::Conflict) => {
                if let Ok(info) = engine.info(db).await {
                    assert_eq!(info.revision, 0);
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25 * (attempt + 1))).await;
            }
            Err(Error::Storage(error)) if proposal_race(&error) => {
                eprintln!(
                    "B2 create proposal raced collection; recovering the uniquely owned name"
                );
                match engine.info(db).await {
                    Ok(info) => {
                        assert_eq!(info.revision, 0);
                        return;
                    }
                    Err(Error::NotFound) => {}
                    Err(error) => panic!("B2 create recovery failed: {error}"),
                }
                tokio::time::sleep(std::time::Duration::from_millis(25 * (attempt + 1))).await;
            }
            Err(error) => panic!("B2 create failed: {error}"),
        }
    }
    panic!("B2 create contention did not settle within 12 attempts");
}
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
    create(&engine, &db).await;
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
                Err(Error::Storage(error)) if proposal_race(&error) => {
                    eprintln!(
                        "B2 commit proposal raced collection; retrying the identical request"
                    );
                }
                Err(e) => panic!("B2 write failed: {e}"),
            }
        }
        assert!(completed, "B2 contention did not settle");
        assert!(engine.commit(&db, &req).await.unwrap().replayed);
    }
    assert!(engine.gc(&db).await.unwrap().deleted >= 4);
    // Another collector may already have reclaimed our old manifests, and the
    // shared bounded cursor can start anywhere. Check the reclamation result,
    // not which concurrent collector received credit for doing the work.
    let prefix = format!("manifests/{db}.");
    let mut reclaimed = false;
    for _ in 0..32 {
        storage.collect_manifests().await.unwrap();
        if storage.list(&prefix, None, 8).await.unwrap().len() == 1 {
            reclaimed = true;
            break;
        }
    }
    assert!(
        reclaimed,
        "obsolete manifests survived 32 bounded collection passes"
    );
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
        create(&engine, &name).await;
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
