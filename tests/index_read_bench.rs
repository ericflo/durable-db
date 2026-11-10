//! Opt-in release CPU measurement, not a serving or provider benchmark.
use durable_db::{
    cache::Cache,
    index::table::{self, Row, Table},
    storage::Remote,
};
use std::{sync::Arc, time::Instant};

#[tokio::test]
#[ignore = "requires a release build and DURABLE_DB_INDEX_READ_REPORT"]
async fn cached_native_point_read_cost() {
    assert!(!cfg!(debug_assertions), "use --release");
    let report = std::env::var("DURABLE_DB_INDEX_READ_REPORT").unwrap();
    let keys: Vec<_> = (0..4096).map(|n| format!("key/{n:05}")).collect();
    let mut results = Vec::new();
    for size in [0, 32, 4096] {
        let storage = Arc::new(
            Remote::new(
                Arc::new(object_store::memory::InMemory::new()),
                "read-bench",
            )
            .unwrap(),
        );
        let value = vec![42; size];
        let reference = table::build(
            storage.clone(),
            keys.iter().enumerate().map(|(n, key)| Row {
                key: key.as_bytes(),
                revision: 1,
                value: if n % 7 == 0 {
                    None
                } else {
                    Some(value.as_slice())
                },
            }),
        )
        .await
        .unwrap();
        let cache = Arc::new(Cache::new(32 * 1024 * 1024, None).unwrap());
        let table = Table::open(storage, cache.clone(), reference)
            .await
            .unwrap();
        let mut warm = table.scan(b"", None, 1).unwrap();
        while warm.next().await.unwrap().is_some() {}
        let before = object_gets();
        let mut samples = Vec::with_capacity(20_000);
        for n in 0..20_000 {
            let index = (n * 1597) % keys.len();
            let start = Instant::now();
            let row = table.get(keys[index].as_bytes(), 1).await.unwrap().unwrap();
            samples.push(start.elapsed().as_nanos() as u64);
            assert_eq!(row.key, keys[index].as_bytes());
            assert_eq!(
                row.value.as_deref(),
                if index % 7 == 0 {
                    None
                } else {
                    Some(value.as_slice())
                }
            );
            std::hint::black_box(row);
        }
        assert_eq!(before, object_gets());
        let total: u64 = samples.iter().sum();
        samples.sort_unstable();
        results.push(
            serde_json::json!({"keys":4096,"value_bytes":size,"reads":samples.len(),
            "mean_ns":total as f64/samples.len() as f64,"p50_ns":samples[samples.len()/2],
            "p95_ns":samples[samples.len()*95/100],"p99_ns":samples[samples.len()*99/100],
            "provider_gets":0,"cache_accounted_bytes":cache.memory_bytes().await}),
        );
    }
    std::fs::write(report,serde_json::to_vec_pretty(&serde_json::json!({"schema":1,
        "scope":"Release CPU microbenchmark: one task, warm 32 MiB byte cache, in-memory authoritative test store, no HTTP/ownership/provider latency or concurrent workload", "results":results})).unwrap()).unwrap();
}
fn object_gets() -> u64 {
    let prefix = "durable_db_operation_duration_seconds_count{operation=\"object_get\"} ";
    durable_db::metrics::REGISTRY
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap()
        .parse()
        .unwrap()
}
