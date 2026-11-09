//! Opt-in common conditional-register contract against a disposable provider.
use durable_db::providers::{Connection, b2::Native, s3::S3};
use serde_json::json;
use std::collections::BTreeMap;

async fn connect(provider: &str, bucket: &str, prefix: &str) -> Connection {
    let result = match provider {
        "b2" => Native::connect_storage(bucket, prefix).await,
        "s3" => S3::connect_storage(bucket, prefix).await,
        _ => panic!("provider must be b2 or s3"),
    };
    result.unwrap_or_else(|_| panic!("provider connection failed"))
}

fn logical(mut info: BTreeMap<String, String>) -> BTreeMap<String, String> {
    info.remove("durable-db-probe");
    info
}

#[tokio::test]
#[ignore = "requires an explicitly disposable provider bucket and a report path"]
async fn unchanged_metadata_still_requires_an_exact_comparison() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("dedicated bucket");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("b2 or s3");
    let context = std::env::var("DURABLE_DB_TEST_CONTEXT").expect("provider placement");
    let output = std::env::var("DURABLE_DB_REGISTER_REPORT").expect("report path");
    let prefix = format!("register-{}", uuid::Uuid::new_v4());
    let register = connect(&provider, &bucket, &prefix).await.authority();
    let before = register.read().await.unwrap();
    let first = register
        .replace(&before, before.info.clone())
        .await
        .unwrap()
        .expect("first comparison");
    assert_eq!(logical(first.info.clone()), logical(before.info.clone()));
    assert!(
        register
            .replace(&before, first.info.clone())
            .await
            .unwrap()
            .is_none()
    );
    // Two replicas propose the same logical metadata at the same revision.
    let peer = connect(&provider, &bucket, &prefix).await.authority();
    let (left, right) = tokio::join!(
        register.replace(&first, first.info.clone()),
        peer.replace(&first, first.info.clone())
    );
    let (left, right) = (left.unwrap(), right.unwrap());
    assert_ne!(
        left.is_some(),
        right.is_some(),
        "exactly one comparison wins"
    );
    let winner = left.or(right).unwrap();
    assert_eq!(logical(winner.info.clone()), logical(before.info.clone()));
    assert_ne!(
        winner.info["durable-db-probe"],
        first.info["durable-db-probe"]
    );
    drop(register);
    drop(peer);

    let reopened = connect(&provider, &bucket, &prefix).await.authority();
    let fresh = reopened.read_fresh().await.unwrap();
    assert!(fresh.revision > winner.revision);
    assert_eq!(logical(fresh.info.clone()).get("durable-db-format"), None);
    assert!(
        reopened
            .replace(&winner, fresh.info.clone())
            .await
            .unwrap()
            .is_none()
    );
    let report = json!({
        "schema": 1, "complete": true, "provider": provider, "context": context,
        "logical_noop_advanced_revision": true,
        "stale_identical_metadata_rejected": true,
        "concurrent_identical_metadata_winners": 1,
        "fresh_barrier_after_reconnect": true,
        "publication_format_initialized": false,
        "limitations": [
            "Conditional-register contract, not a serving grant or a database workload.",
            "No live provider fault injection; uncertain replies are covered by local HTTP fixtures.",
            "MinIO is S3-compatible; this does not qualify AWS."
        ]
    });
    std::fs::write(output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
}
