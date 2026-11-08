//! Reproducible local microbenchmark, deliberately not a claim about object-store latency.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use durable_db::{cache::Cache, engine::*, storage::Local};
use std::{sync::Arc, time::Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = Engine::new(
        Arc::new(Local::open(dir.path().into())?),
        Arc::new(Cache::new(64 * 1024 * 1024, None)?),
    );
    engine.create("bench", Limits::default()).await?;
    let mut writes = Vec::new();
    for n in 0..200 {
        let start = Instant::now();
        engine
            .commit(
                "bench",
                &CommitRequest {
                    expected_revision: n,
                    request_id: format!("r{n}"),
                    checks: vec![],
                    writes: vec![Write {
                        key: format!("key/{n:08}"),
                        value: Some(STANDARD.encode([42; 1024])),
                    }],
                },
            )
            .await?;
        writes.push(start.elapsed().as_micros() as u64);
    }
    let mut reads = Vec::new();
    for n in 0..2000 {
        let start = Instant::now();
        engine
            .read(
                "bench",
                &ReadRequest {
                    keys: vec![format!("key/{:08}", n % 200)],
                    ..Default::default()
                },
            )
            .await?;
        reads.push(start.elapsed().as_micros() as u64);
    }
    writes.sort();
    reads.sort();
    println!(
        "{}",
        serde_json::json!({"backend":"local-fsync","write_samples":writes.len(),"read_samples":reads.len(),"value_bytes":1024,"write_us":{"p50":writes[100],"p95":writes[190],"p99":writes[198]},"read_us":{"p50":reads[1000],"p95":reads[1900],"p99":reads[1980]}})
    );
    Ok(())
}
