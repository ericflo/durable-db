//! Opt-in checkpoint composition on disposable providers. Writers are stopped
//! before takeover; injected clocks do not qualify physical lease safety.
use bytes::Bytes;
use durable_db::{
    cache::Cache,
    index::recovery::{Limits, Recovering, checkpoint::Snapshot},
    journal::{QueueLimits, Writer},
    metrics::REGISTRY,
    ownership::{
        root::{Outcome, Profile, Root},
        timing::Clock,
    },
    providers::{Connection, b2::Native, s3::S3},
    transactions::{
        record::{Commit, Database, Mutation},
        recovery::Resolution,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
#[derive(Clone, Default)]
struct TestClock(Arc<AtomicU64>);
impl Clock for TestClock {
    fn read(&self) -> Option<Duration> {
        Some(Duration::from_nanos(self.0.load(Ordering::SeqCst)))
    }
}
async fn connect(provider: &str, bucket: &str, namespace: &str) -> Connection {
    match provider {
        "b2" => Native::connect_storage(bucket, namespace).await,
        "s3" => S3::connect_storage(bucket, namespace).await,
        _ => panic!("invalid provider"),
    }
    .unwrap_or_else(|_| panic!("checkpoint provider connection failed"))
}
async fn activate(connection: &Connection, clock: &TestClock) -> Root<TestClock> {
    let profile = Profile::new(Duration::from_nanos(10), Duration::from_nanos(1)).unwrap();
    let mut root = connection.root(clock.clone(), profile).unwrap();
    let observed = root
        .observe()
        .await
        .unwrap_or_else(|_| panic!("root observation failed"));
    let request = root.claim(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    clock.0.fetch_add(23, Ordering::SeqCst);
    validate(&mut root).await;
    let request = root.activate().unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    root
}
async fn validate(root: &mut Root<TestClock>) {
    let observed = root
        .observe()
        .await
        .unwrap_or_else(|_| panic!("root validation observation failed"));
    let request = root.validate(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}
async fn select(root: &mut Root<TestClock>) {
    loop {
        let mut recovery = root.recovery().unwrap();
        let mut chosen = None;
        while let Some(mut journal) = recovery
            .next()
            .await
            .unwrap_or_else(|_| panic!("recovery ancestry failed"))
        {
            if journal.is_selected() {
                continue;
            }
            while journal
                .read_next()
                .await
                .unwrap_or_else(|_| panic!("journal discovery failed"))
                .is_some()
            {}
            chosen = Some(journal);
            break;
        }
        let Some(chosen) = chosen else { break };
        let request = root.select_prefix(chosen).unwrap();
        assert!(matches!(
            root.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
    }
}
fn records(revision: u64, start: u128, end: u128, late: bool) -> Vec<Bytes> {
    (start..end)
        .map(|n| {
            let name = format!("app-{n:04}");
            let value = [revision as u8; 64];
            let request = if late {
                "late".into()
            } else {
                format!("r-{revision}")
            };
            Commit::encode(
                Database {
                    name: &name,
                    generation: Uuid::from_u128(n + 1),
                },
                revision,
                revision - 1,
                0,
                &request,
                [revision as u8; 32],
                &[Mutation {
                    key: "x",
                    value: match revision {
                        3 | 6 => None,
                        4 => Some(b""),
                        _ => Some(&value),
                    },
                }],
            )
            .unwrap()
        })
        .collect()
}
async fn append(writer: &mut Writer, records: &[Bytes]) {
    let upload = writer.prepare(records).unwrap();
    writer.complete(upload.send().await).unwrap();
    assert!(writer.pop_durable().unwrap().is_some());
}
fn calls(operation: &str) -> u64 {
    let prefix =
        format!("durable_db_operation_duration_seconds_count{{operation=\"{operation}\"}} ");
    REGISTRY
        .render()
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap()
        .parse()
        .unwrap()
}
fn controls() -> [u64; 3] {
    [
        calls("authority_read"),
        calls("authority_replace"),
        calls("read_barrier"),
    ]
}
async fn lazy_reads(connection: &Connection, root: &mut Root<TestClock>) -> serde_json::Value {
    let before = controls();
    let gets = calls("object_get");
    let cache = Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap());
    let reference = root.checkpoint_reference().unwrap().unwrap();
    let start = Instant::now();
    let snapshot = Snapshot::open(connection.objects(), cache.clone(), reference)
        .await
        .unwrap_or_else(|_| panic!("lazy checkpoint open failed"));
    let view = snapshot
        .database(Database {
            name: "app-0512",
            generation: Uuid::from_u128(513),
        })
        .await
        .unwrap_or_else(|_| panic!("lazy checkpoint metadata failed"))
        .unwrap();
    assert_eq!(view.revision(), 4);
    assert_eq!(
        view.get("x").await.unwrap().unwrap().value,
        Some(Bytes::new())
    );
    assert_eq!(
        view.resolve("r-1", [1; 32], 0).await.unwrap(),
        Resolution::Committed(1)
    );
    let cold_ms = start.elapsed().as_secs_f64() * 1000.0;
    let cold_gets = calls("object_get") - gets;
    assert!(cold_gets <= 8, "one database must not load the checkpoint");
    let gets = calls("object_get");
    let start = Instant::now();
    for _ in 0..1000 {
        assert_eq!(
            view.get("x").await.unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert_eq!(
            view.resolve("r-1", [1; 32], 0).await.unwrap(),
            Resolution::Committed(1)
        );
        let mut scan = view.scan("", None).unwrap();
        assert_eq!(scan.next().await.unwrap().unwrap().key, b"x"[..]);
        assert!(scan.next().await.unwrap().is_none());
    }
    let warm_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(gets, calls("object_get"));
    for n in 0..1024 {
        let name = format!("app-{n:04}");
        let view = snapshot
            .database(Database {
                name: &name,
                generation: Uuid::from_u128(n + 1),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.revision(), 4);
        assert_eq!(
            view.get("x").await.unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert_eq!(
            view.resolve("r-4", [4; 32], 3).await.unwrap(),
            Resolution::Committed(4)
        );
        assert_eq!(
            view.resolve("r-6", [6; 32], 4).await.unwrap(),
            Resolution::Missing
        );
    }
    assert_eq!(before, controls());
    let memory_bytes = cache.memory_bytes().await;
    assert!(memory_bytes <= 4 * 1024 * 1024);
    serde_json::json!({"cold_point_and_receipt_ms":cold_ms,"cold_object_gets":cold_gets,
        "warm_iterations":1000,"warm_operations_per_iteration":3,"warm_ms":warm_ms,
        "warm_object_gets":0,"control_calls":0,"verified_databases":1024,
        "cache_capacity_bytes":4*1024*1024,"cache_accounted_bytes":memory_bytes,
        "scope":"Immutable checkpoint at revision 4 only; excludes later selected/live journal and serving authority; debug integration sample"})
}
#[tokio::test]
#[ignore = "requires an explicitly disposable B2 or S3 bucket and a report path"]
async fn native_checkpoints_recover_selected_suffix_across_complete_reopen() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").unwrap();
    let report = std::env::var("DURABLE_DB_CHECKPOINT_REPORT").unwrap();
    let namespace = format!("checkpoint-{}", Uuid::new_v4());
    let connection = connect(&provider, &bucket, &namespace).await;
    let clock = TestClock::default();
    let mut first = activate(&connection, &clock).await;
    let mut writer = first.take_journal(QueueLimits::default()).unwrap();
    for revision in 1..=6 {
        for batch in 0..4 {
            append(
                &mut writer,
                &records(revision, batch * 256, (batch + 1) * 256, false),
            )
            .await;
        }
    }
    let late = writer.prepare(&records(7, 0, 256, true)).unwrap();
    let files = connection.objects();
    let covered = files
        .list("journal/", None, 64)
        .await
        .unwrap_or_else(|_| panic!("fixture journal listing failed"));
    assert_eq!(covered.len(), 24);
    let mut second = activate(&connection, &clock).await;
    select(&mut second).await;
    let recovery = Recovering::begin(second.replay().unwrap(), Limits::default())
        .await
        .unwrap_or_else(|_| panic!("native recovery start failed"))
        .advance(16)
        .await
        .unwrap_or_else(|_| panic!("native partial recovery failed"));
    assert!(!recovery.is_complete());
    let start = Instant::now();
    let staged = recovery
        .checkpoint()
        .await
        .unwrap_or_else(|_| panic!("checkpoint staging failed; uploads may be uncertain"));
    let checkpoint_ms = start.elapsed().as_secs_f64() * 1000.0;
    let request = second.publish_checkpoint(staged).unwrap();
    second.abandon();
    assert!(matches!(
        second.finish(request.execute().await).unwrap(),
        Outcome::Ignored
    ));
    validate(&mut second).await;
    for key in &covered[..16] {
        files
            .delete(key)
            .await
            .unwrap_or_else(|_| panic!("covered fixture journal removal failed"));
    }
    let _late = late.send().await;
    let mut current = second.take_journal(QueueLimits::default()).unwrap();
    append(&mut current, &records(7, 0, 256, false)).await;
    drop((
        first, writer, second, current, recovery, connection, files, clock,
    ));
    let connection = connect(&provider, &bucket, &namespace).await;
    let clock = TestClock::default();
    let mut root = activate(&connection, &clock).await;
    select(&mut root).await;
    let lazy = lazy_reads(&connection, &mut root).await;
    let before = controls();
    let gets = calls("object_get");
    let start = Instant::now();
    let mut recovery = Recovering::begin_cached(
        root.replay().unwrap(),
        Limits::default(),
        Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap()),
    )
    .await
    .unwrap_or_else(|_| panic!("checkpoint reconstruction failed"));
    let mut frames = 0;
    while !recovery.is_complete() {
        recovery = recovery
            .advance(1)
            .await
            .unwrap_or_else(|_| panic!("checkpoint suffix recovery failed"));
        if !recovery.is_complete() {
            frames += 1;
        }
    }
    let recovery_ms = start.elapsed().as_secs_f64() * 1000.0;
    let recovery_gets = calls("object_get") - gets;
    let rewrite_started = Instant::now();
    let rewritten = recovery.checkpoint().await.unwrap_or_else(|error| {
        panic!(
            "overlay checkpoint rewrite failed: {}",
            error
                .downcast_ref::<&str>()
                .unwrap_or(&"provider or encoding failure (details redacted)")
        )
    });
    let rewrite_ms = rewrite_started.elapsed().as_secs_f64() * 1000.0;
    let projection = recovery.finish().unwrap();
    assert_eq!(projection.usage().versions, 2304);
    assert_eq!(frames, 9);
    assert_eq!(projection.usage().databases, 1024);
    assert_eq!(before, controls());
    let gets = calls("object_get");
    let start = Instant::now();
    for n in 0..1024 {
        let name = format!("app-{n:04}");
        let view = projection
            .view(Database {
                name: &name,
                generation: Uuid::from_u128(n + 1),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.snapshot_floor(), 4);
        assert!(view.get(b"x", 3).await.is_err());
        assert_eq!(
            view.get(b"x", 4).await.unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert!(view.get(b"x", 6).await.unwrap().unwrap().value.is_none());
        let latest = if n < 256 { 7 } else { 6 };
        assert_eq!(view.revision(), latest);
        let row = view.get(b"x", latest).await.unwrap().unwrap();
        assert_eq!(row.value.is_none(), latest == 6);
        assert_eq!(
            view.resolve("r-1", [1; 32], 0).await.unwrap(),
            Resolution::Committed(1)
        );
        assert_eq!(
            view.resolve(&format!("r-{latest}"), [latest as u8; 32], latest - 1)
                .await
                .unwrap(),
            Resolution::Committed(latest)
        );
        assert_eq!(
            view.resolve("late", [7; 32], 6).await.unwrap(),
            Resolution::Missing
        );
    }
    let reads_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(gets, calls("object_get"));
    assert_eq!(before, controls());
    let request = root.publish_checkpoint(rewritten).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let before = controls();
    let gets = calls("object_get");
    let zero = Recovering::begin_cached(
        root.replay().unwrap(),
        Limits {
            databases: 1,
            versions: 1,
            value_bytes: 1,
            receipts: 1,
            receipt_bytes: 1,
        },
        Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap()),
    )
    .await
    .unwrap()
    .advance(1)
    .await
    .unwrap()
    .finish()
    .unwrap();
    let zero_gets = calls("object_get") - gets;
    assert_eq!(zero.usage(), durable_db::index::recovery::Usage::default());
    for n in 0..1024 {
        let name = format!("app-{n:04}");
        let view = zero
            .view(Database {
                name: &name,
                generation: Uuid::from_u128(n + 1),
            })
            .await
            .unwrap()
            .unwrap();
        let revision = if n < 256 { 7 } else { 6 };
        assert_eq!(view.revision(), revision);
        assert_eq!(view.snapshot_floor(), revision);
        assert_eq!(
            view.get(b"x", revision)
                .await
                .unwrap()
                .unwrap()
                .value
                .is_none(),
            revision == 6
        );
        assert_eq!(
            view.resolve("r-1", [1; 32], 0).await.unwrap(),
            Resolution::Committed(1)
        );
        assert_eq!(
            view.resolve("late", [7; 32], revision).await.unwrap(),
            Resolution::Missing
        );
    }
    assert_eq!(before, controls());
    std::fs::write(report,serde_json::to_vec_pretty(&serde_json::json!({
        "schema":1,"provider":provider,"namespace":namespace,"scope":"Native checkpoint composition; injected clocks, stopped writers, debug build; no serving API, live stale-owner fencing, automatic GC or AWS qualification", "lazy_reads":lazy,
        "databases":1024,"original_selected_frames":24,"checkpoint_cut_frame":16,"covered_frames_removed":16,"suffix_frames_replayed":frames,"checkpoint_publication_abandoned_then_validated":true,"late_old_suffix_excluded":true,"fresh_connection_and_controller_reconstruction":true,
        "checkpoint_stage_ms":checkpoint_ms,"recovery_ms":recovery_ms,"recovery_object_gets":recovery_gets,"recovery_control_calls":0,"resident_versions_after_suffix":projection.usage().versions,"receipts_after_suffix":projection.usage().receipts,
        "verified_databases":1024,"verification_ms":reads_ms,"verification_object_calls":0,
        "rewrite_ms":rewrite_ms,"rewritten_checkpoint_verified_databases":1024,"rewritten_recovery_resident_databases":0,"rewritten_recovery_resident_versions":0,"rewritten_recovery_resident_receipts":0,"rewritten_recovery_object_gets":zero_gets
    })).unwrap()).unwrap();
}
