//! Opt-in checkpoint composition on disposable providers. Writers are stopped
//! before takeover; injected clocks do not qualify physical lease safety.
use bytes::Bytes;
use durable_db::{
    index::recovery::{Limits, Recovering},
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
    let before = controls();
    let gets = calls("object_get");
    let start = Instant::now();
    let mut recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
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
    let projection = recovery.finish().unwrap();
    let recovery_ms = start.elapsed().as_secs_f64() * 1000.0;
    let recovery_gets = calls("object_get") - gets;
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
            .unwrap()
            .unwrap();
        assert_eq!(view.snapshot_floor(), 4);
        assert!(view.get(b"x", 3).is_err());
        assert_eq!(
            view.get(b"x", 4).unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert!(view.get(b"x", 6).unwrap().unwrap().value.is_none());
        let latest = if n < 256 { 7 } else { 6 };
        assert_eq!(view.state().revision(), latest);
        let row = view.get(b"x", latest).unwrap().unwrap();
        assert_eq!(row.value.is_none(), latest == 6);
        assert_eq!(
            view.state().resolve("r-1", [1; 32], 0).unwrap(),
            Resolution::Committed(1)
        );
        assert_eq!(
            view.state()
                .resolve(&format!("r-{latest}"), [latest as u8; 32], latest - 1)
                .unwrap(),
            Resolution::Committed(latest)
        );
        assert_eq!(
            view.state().resolve("late", [7; 32], 6).unwrap(),
            Resolution::Missing
        );
    }
    let reads_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(gets, calls("object_get"));
    assert_eq!(before, controls());
    std::fs::write(report,serde_json::to_vec_pretty(&serde_json::json!({
        "schema":1,"provider":provider,"namespace":namespace,"scope":"Native checkpoint composition; injected clocks, stopped writers, debug build; no serving API, live stale-owner fencing, automatic GC or AWS qualification",
        "databases":1024,"original_selected_frames":24,"checkpoint_cut_frame":16,"covered_frames_removed":16,"suffix_frames_replayed":frames,"checkpoint_publication_abandoned_then_validated":true,"late_old_suffix_excluded":true,"fresh_connection_and_controller_reconstruction":true,
        "checkpoint_stage_ms":checkpoint_ms,"recovery_ms":recovery_ms,"recovery_object_gets":recovery_gets,"recovery_control_calls":0,"resident_versions_after_suffix":projection.usage().versions,"receipts_after_suffix":projection.usage().receipts,
        "verified_databases":1024,"verification_ms":reads_ms,"verification_object_calls":0
    })).unwrap()).unwrap();
}
