mod store;
use durable_db::{
    journal::QueueLimits,
    owned_files::{self, Limits, Namespace},
    ownership::{
        root::{Outcome, Profile, Root},
        timing::Clock,
    },
    providers::{Connection, b2::Native, s3::S3},
};
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::{
    Db, WriteBatch,
    config::{FlushOptions, FlushType, Settings},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use store::{Budget, Store};

/// Deterministic controller fixture, not a production physical clock. Every
/// previous index/writer is explicitly stopped before the next owner starts.
#[derive(Clone, Default)]
struct FixtureClock(Arc<AtomicU64>);
impl Clock for FixtureClock {
    fn read(&self) -> Option<Duration> {
        Some(Duration::from_secs(self.0.load(Ordering::SeqCst)))
    }
}
async fn activate(
    connection: &Connection,
    clock: &FixtureClock,
) -> anyhow::Result<Root<FixtureClock>> {
    let mut root = connection.root(
        clock.clone(),
        Profile::new(Duration::from_secs(10), Duration::from_secs(1))?,
    )?;
    let observation = root.observe().await?;
    let request = root.claim(observation)?;
    anyhow::ensure!(
        matches!(root.finish(request.execute().await)?, Outcome::Confirmed),
        "claim not confirmed"
    );
    clock.0.fetch_add(23, Ordering::SeqCst);
    let observation = root.observe().await?;
    let request = root.validate(observation)?;
    anyhow::ensure!(
        matches!(root.finish(request.execute().await)?, Outcome::Confirmed),
        "validation not confirmed"
    );
    let request = root.activate()?;
    anyhow::ensure!(
        matches!(root.finish(request.execute().await)?, Outcome::Confirmed),
        "activation not confirmed"
    );
    loop {
        let mut history = root.recovery()?;
        let mut unselected = None;
        while let Some(mut journal) = history.next().await? {
            if !journal.is_selected() {
                while journal.read_next().await?.is_some() {}
                unselected = Some(journal);
                break;
            }
        }
        let Some(journal) = unselected else { break };
        let request = root.select_prefix(journal)?;
        anyhow::ensure!(
            matches!(root.finish(request.execute().await)?, Outcome::Confirmed),
            "selection not confirmed"
        );
    }
    Ok(root)
}
async fn index(
    root: &mut Root<FixtureClock>,
    budget: Arc<Budget>,
    objects: Arc<dyn durable_db::storage::Storage>,
) -> anyhow::Result<(Db, Arc<Store>)> {
    let view = owned_files::restore(root.replay()?, Limits::default()).await?;
    let files = Namespace::new(view, root.take_journal(QueueLimits::default())?)?;
    let store = Arc::new(Store::new(files, budget, objects));
    let settings = Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        l0_sst_size_bytes: 4 * 1024 * 1024,
        max_unflushed_bytes: 8 * 1024 * 1024,
        manifest_poll_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let cache = Arc::new(slatedb::db_cache::moka::MokaCache::new_with_opts(
        slatedb::db_cache::moka::MokaCacheOptions {
            max_capacity: 8 * 1024 * 1024,
            ..Default::default()
        },
    ));
    let db = Db::builder("index", store.clone())
        .with_db_cache(cache, 0)
        .with_settings(settings)
        .build()
        .await?;
    Ok((db, store))
}
fn control_counts() -> Vec<u64> {
    let metrics = durable_db::metrics::REGISTRY.render();
    ["authority_read", "authority_replace", "read_barrier"]
        .iter()
        .map(|op| {
            let prefix =
                format!("durable_db_operation_duration_seconds_count{{operation=\"{op}\"}} ");
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}
fn key(n: usize) -> String {
    format!("db/{:04}/item/{:04}", n % 16, n / 16)
}
async fn run() -> anyhow::Result<()> {
    anyhow::ensure!(
        std::env::var("DURABLE_DB_DISPOSABLE").as_deref() == Ok("1"),
        "explicit disposable bucket required"
    );
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET")?;
    anyhow::ensure!(
        bucket.starts_with("durable-db-test-"),
        "test bucket prefix required"
    );
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER")?;
    let namespace = format!("owned-index-{}", uuid::Uuid::new_v4());
    let connection = match provider.as_str() {
        "b2" => Native::connect_storage(&bucket, &namespace).await?,
        "s3" => S3::connect_storage(&bucket, &namespace).await?,
        _ => anyhow::bail!("provider must be b2 or s3"),
    };
    eprintln!("phase: provider connected");
    let clock = FixtureClock::default();
    let budget = Arc::new(Budget::default());
    let mut root = activate(&connection, &clock).await?;
    let (db, store) = index(&mut root, budget.clone(), connection.objects()).await?;
    eprintln!("phase: initial index open");
    let control_before = control_counts();
    let empty = db.snapshot()?;
    let mut samples = Vec::new();
    for group in 0..8 {
        let mut batch = WriteBatch::new();
        for n in group * 128..(group + 1) * 128 {
            batch.put(key(n), [42; 1024]);
        }
        let start = Instant::now();
        let handle = db.write(batch).await?;
        let snapshot = db.snapshot()?;
        db.flush().await?;
        handle.await_durable().await?;
        anyhow::ensure!(root.permission()?.is_some(), "fixture permission expired");
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(
            snapshot.get(key(group * 128)).await?.as_deref(),
            Some([42; 1024].as_slice())
        );
    }
    assert_eq!(empty.get(key(0)).await?, None);
    let snapshot = db.snapshot()?;
    let before = budget.stats();
    let start = Instant::now();
    for n in 0..2000 {
        assert_eq!(
            snapshot.get(key(n % 1024)).await?.as_deref(),
            Some([42; 1024].as_slice())
        );
    }
    let read_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(before, budget.stats());
    let mut scan = snapshot.scan::<std::ops::RangeFull>(..).await?;
    let mut rows = 0;
    let mut previous = bytes::Bytes::new();
    while let Some(row) = scan.next().await? {
        assert!(row.key > previous);
        previous = row.key;
        rows += 1;
    }
    assert_eq!(rows, 1024);
    let grouped_before = budget.stats()["mutation_attempts"].as_u64().unwrap();
    let grouped_start = Instant::now();
    let handles = stream::iter(0..32)
        .map(|n| {
            let db = &db;
            async move { db.put(key(n), [43; 1024]).await }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await?;
    db.flush().await?;
    for handle in handles {
        handle.await_durable().await?;
    }
    anyhow::ensure!(root.permission()?.is_some(), "fixture permission expired");
    let grouped_ms = grouped_start.elapsed().as_secs_f64() * 1000.0;
    let grouped_puts = budget.stats()["mutation_attempts"].as_u64().unwrap() - grouped_before;
    assert_eq!(grouped_puts, 1);
    for n in 0..32 {
        assert_eq!(
            snapshot.get(key(n)).await?.as_deref(),
            Some([42; 1024].as_slice())
        );
        assert_eq!(
            db.get(key(n)).await?.as_deref(),
            Some([43; 1024].as_slice())
        );
    }
    drop((empty, snapshot, scan));
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;
    drop(db);
    eprintln!("phase: initial data and table flush complete");
    assert_eq!(control_before, control_counts());
    let first_usage = store.usage().await?;
    assert!(first_usage["external_bytes"].as_u64().unwrap() > 512 * 1024);
    let late = store.prepare_late_manifest().await?;
    let first_frames = late.position() - 1;
    drop(store);
    drop(root);
    let mut root = activate(&connection, &clock).await?;
    let _late_completion = late.send().await;
    let (db, store) = index(&mut root, budget.clone(), connection.objects()).await?;
    eprintln!("phase: first cold reopen complete");
    for n in 0..1024 {
        assert_eq!(
            db.get(key(n)).await?.as_deref(),
            Some([if n < 32 { 43 } else { 42 }; 1024].as_slice())
        );
    }
    let table_warm_before = budget.stats();
    assert!(table_warm_before["external_reads"].as_u64().unwrap() > 0);
    let table_warm_start = Instant::now();
    for n in 0..2000 {
        let n = n % 1024;
        assert_eq!(
            db.get(key(n)).await?.as_deref(),
            Some([if n < 32 { 43 } else { 42 }; 1024].as_slice())
        );
    }
    let table_warm_ms = table_warm_start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(table_warm_before, budget.stats());
    let mut batch = WriteBatch::new();
    batch.put(key(0), b"replacement");
    batch.delete(key(1));
    let handle = db.write(batch).await?;
    db.flush().await?;
    handle.await_durable().await?;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;
    drop(db);
    store.retire().await?;
    drop(store);
    drop(root);
    let mut root = activate(&connection, &clock).await?;
    let (db, store) = index(&mut root, budget.clone(), connection.objects()).await?;
    eprintln!("phase: second cold reopen complete");
    assert_eq!(
        db.get(key(0)).await?.as_deref(),
        Some(b"replacement".as_slice())
    );
    assert_eq!(db.get(key(1)).await?, None);
    for n in 2..1024 {
        assert_eq!(
            db.get(key(n)).await?.as_deref(),
            Some([if n < 32 { 43 } else { 42 }; 1024].as_slice())
        );
    }
    db.close().await?;
    store.retire().await?;
    println!(
        "{}",
        serde_json::json!({"provider":provider,"namespace":namespace,"rust":env!("PROBE_RUSTC"),"build":"debug","slatedb":"0.18.1","keys":1024,"key_prefixes":16,"batch_records":128,"durable_batch_latency_ms_samples":samples,"warm_reads":2000,"warm_read_total_ms":read_ms,"warm_reads_additional_index_store_calls":0,"ordered_scan_rows":rows,"cold_reopens":2,"overwrite_and_delete_recovered":true,"late_manifest_upload_excluded":true,"application_phase_control_calls":0,"grouped_writes":32,"grouped_admission_concurrency":16,"grouped_durable_ms":grouped_ms,"grouped_logical_puts":grouped_puts,"first_journal_frames":first_frames,"logical_store_operations":budget.stats(),"block_cache_capacity_bytes":8388608,"external_file_threshold_bytes":262144,"value_bytes":1024,"first_namespace_usage":first_usage,"warm_table_reads":2000,"warm_table_read_total_ms":table_warm_ms,"warm_table_reads_additional_store_calls":0,"scope":"External-file index integration with actual provider bytes and selected root catalogs; stopped writers and deterministic clocks, no concurrent ownership or serving qualification"})
    );
    Ok(())
}
#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        if let Some(location) = info.location() {
            eprintln!(
                "Index fixture assertion failed at {}:{}",
                location.file(),
                location.line()
            );
        }
    }));
    // Suppress provider errors that can contain signed URLs or credentials.
    match tokio::time::timeout(Duration::from_secs(300), run()).await {
        Ok(Ok(())) => (),
        _ => {
            eprintln!("Owned index integration failed; no production qualification claimed.");
            std::process::exit(1);
        }
    }
}
