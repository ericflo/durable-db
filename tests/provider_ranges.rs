//! Opt-in immutable range transport qualification; no serving-engine claim.
use bytes::Bytes;
use durable_db::{
    metrics::REGISTRY,
    providers::{b2::Native, s3::S3},
    storage::MAX_RANGE_BYTES,
};
use serde_json::json;
use std::{path::PathBuf, time::Instant};

fn controls() -> Vec<u64> {
    let rendered = REGISTRY.render();
    ["authority_read", "authority_replace", "read_barrier"]
        .iter()
        .map(|op| {
            let prefix =
                format!("durable_db_operation_duration_seconds_count{{operation=\"{op}\"}} ");
            rendered
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires an explicitly disposable B2 or S3 bucket and a report path"]
async fn bounded_ranges_of_a_large_immutable_object() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("test bucket required");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("provider required");
    let report =
        PathBuf::from(std::env::var("DURABLE_DB_RANGE_REPORT").expect("report path required"));
    let namespace = format!("ranges-{}", uuid::Uuid::new_v4());
    let connection = match provider.as_str() {
        "b2" => Native::connect_storage(&bucket, &namespace).await,
        "s3" => S3::connect_storage(&bucket, &namespace).await,
        _ => panic!("provider must be b2 or s3"),
    }
    .unwrap_or_else(|_| panic!("provider connection failed"));
    let files = connection.objects();
    let size = 16 * 1024 * 1024usize;
    let pattern = |offset: usize| (offset % 251) as u8;
    let payload = Bytes::from((0..size).map(pattern).collect::<Vec<_>>());
    let before = controls();
    let key = "journal/immutable-range-fixture";
    assert!(
        files.put(key, payload).await.is_ok(),
        "immutable upload failed"
    );
    let mut samples = Vec::new();
    for range in [
        0..1,
        128..65664,
        size as u64 / 2 - 33..size as u64 / 2 - 33 + 65536,
        size as u64 - 1..size as u64,
        0..MAX_RANGE_BYTES as u64,
    ] {
        let start = Instant::now();
        let bytes = files
            .get_range(key, range.clone(), size as u64)
            .await
            .unwrap_or_else(|_| panic!("immutable range read failed"));
        assert_eq!(bytes.len(), (range.end - range.start) as usize);
        assert!(
            bytes
                .iter()
                .enumerate()
                .all(|(i, b)| *b == pattern(range.start as usize + i)),
            "range contents changed"
        );
        samples.push(json!({"start":range.start,"end":range.end,"elapsed_ms":start.elapsed().as_secs_f64()*1000.0}));
    }
    assert!(
        files
            .get_range(key, 128..192, size as u64 + 1)
            .await
            .is_err()
    );
    assert!(
        files
            .get_range("journal/absent", 128..192, size as u64)
            .await
            .is_err()
    );
    assert!(
        files
            .get_range(key, 0..MAX_RANGE_BYTES as u64 + 1, size as u64)
            .await
            .is_err()
    );
    assert_eq!(before, controls());
    std::fs::write(report,serde_json::to_vec_pretty(&json!({"schema":1,"provider":provider,"namespace":namespace,"object_bytes":size,"maximum_range_bytes":MAX_RANGE_BYTES,"successful_ranges":samples,"wrong_size_rejected":true,"missing_reference_rejected":true,"oversized_request_rejected":true,"shared_register_calls":0,"scope":"Direct immutable range transport; debug build; no index, ownership or AWS qualification"})).unwrap()).unwrap();
}
