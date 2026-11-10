//! Opt-in native table qualification, independent of serving or ownership.
use durable_db::{
    cache::Cache,
    index::table::{self, Reference, Row, Table},
    metrics::REGISTRY,
    providers::{b2::Native, s3::S3},
};
use std::{path::PathBuf, sync::Arc, time::Instant};

fn calls(operation: &str) -> u64 {
    let prefix =
        format!("durable_db_operation_duration_seconds_count{{operation=\"{operation}\"}} ");
    REGISTRY
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap()
        .parse()
        .unwrap()
}
fn controls() -> Vec<u64> {
    ["authority_read", "authority_replace", "read_barrier"]
        .iter()
        .map(|op| calls(op))
        .collect()
}
#[tokio::test]
#[ignore = "requires an explicitly disposable B2 or S3 bucket and a report path"]
async fn native_tables_use_bounded_blocks_and_recover_revisioned_ordered_rows() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("test bucket required");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("provider required");
    let report =
        PathBuf::from(std::env::var("DURABLE_DB_TABLE_REPORT").expect("report path required"));
    let namespace = format!("native-table-{}", uuid::Uuid::new_v4());
    let connection = match provider.as_str() {
        "b2" => Native::connect_storage(&bucket, &namespace).await,
        "s3" => S3::connect_storage(&bucket, &namespace).await,
        _ => panic!("provider must be b2 or s3"),
    }
    .unwrap_or_else(|_| panic!("provider connection failed"));
    let files = connection.objects();
    let before = controls();
    let keys: Vec<_> = (0..1024).map(|n| format!("db/0001/item/{n:04}")).collect();
    let new = vec![42; 1024];
    let old = vec![41; 1024];
    let initial = vec![40; 512];
    let rows = keys.iter().enumerate().flat_map(|(n, key)| {
        [
            Row {
                key: key.as_bytes(),
                revision: 3,
                value: Some(new.as_slice()),
            },
            Row {
                key: key.as_bytes(),
                revision: 2,
                value: if n % 5 == 0 {
                    None
                } else {
                    Some(old.as_slice())
                },
            },
            Row {
                key: key.as_bytes(),
                revision: 1,
                value: Some(initial.as_slice()),
            },
        ]
    });
    let start = Instant::now();
    let reference = table::build(files.clone(), rows)
        .await
        .unwrap_or_else(|_| panic!("native table upload failed; outcome may be unknown"));
    let upload_ms = start.elapsed().as_secs_f64() * 1000.0;
    let reference = reference.encode();
    let cache = Arc::new(Cache::new(256 * 1024, None).unwrap());
    let table = Table::open(
        files.clone(),
        cache.clone(),
        Reference::decode(&reference).unwrap(),
    )
    .await
    .unwrap_or_else(|_| panic!("native table open failed"));
    let gets = calls("object_get");
    let start = Instant::now();
    let row = table
        .get(keys[512].as_bytes(), 2)
        .await
        .unwrap_or_else(|_| panic!("native point read failed"))
        .unwrap();
    assert!(row.value.as_deref() == Some(old.as_slice()));
    let cold_point_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(calls("object_get"), gets + 1);
    let gets = calls("object_get");
    let start = Instant::now();
    for _ in 0..2000 {
        let row = table
            .get(keys[512].as_bytes(), 2)
            .await
            .unwrap_or_else(|_| panic!("native cached read failed"))
            .unwrap();
        assert!(row.revision == 2 && row.value.as_deref() == Some(old.as_slice()));
    }
    let warm_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(calls("object_get"), gets);
    let mut cursor = table.scan(b"", None, 2).unwrap();
    let mut rows = 0;
    let mut tombstones = 0;
    while let Some(row) = cursor
        .next()
        .await
        .unwrap_or_else(|_| panic!("native scan failed"))
    {
        assert!(row.key.as_ref() == keys[rows].as_bytes() && row.revision == 2);
        if rows % 5 == 0 {
            assert!(row.value.is_none());
            tombstones += 1;
        } else {
            assert!(row.value.as_deref() == Some(old.as_slice()));
        }
        rows += 1;
    }
    assert_eq!(rows, 1024);
    assert_eq!(tombstones, 205);
    assert!(cache.memory_bytes().await <= 256 * 1024);
    drop((cursor, table, cache));
    let reopened = Table::open(
        files,
        Arc::new(Cache::new(0, None).unwrap()),
        Reference::decode(&reference).unwrap(),
    )
    .await
    .unwrap_or_else(|_| panic!("native cold reopen failed"));
    for revision in 1..=3 {
        let row = reopened
            .get(keys[0].as_bytes(), revision)
            .await
            .unwrap_or_else(|_| panic!("native reopened point read failed"))
            .unwrap();
        assert_eq!(row.revision, revision);
        assert_eq!(row.value.is_none(), revision == 2);
    }
    assert_eq!(before, controls());
    std::fs::write(report,serde_json::to_vec_pretty(&serde_json::json!({
        "schema":1,"provider":provider,"namespace":namespace,"scope":"Native immutable table; debug build; no serving authority, transactions, compaction or AWS qualification",
        "keys":1024,"stored_versions":3072,"snapshot_revision":2,"scan_rows":rows,"tombstones":tombstones,
        "maximum_block_bytes":131072,"memory_cache_bytes":262144,"reference_bytes":reference.len(),
        "upload_ms":upload_ms,"cold_point_ms":cold_point_ms,"cold_point_range_reads":1,
        "warm_point_reads":2000,"warm_point_total_ms":warm_ms,"warm_point_additional_object_calls":0,
        "cache_free_reopen":true,"shared_register_calls":0
    })).unwrap()).unwrap();
}
