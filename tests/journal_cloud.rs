//! Opt-in journal component qualification, not an owned database benchmark.
use async_trait::async_trait;
use bytes::Bytes;
use durable_db::{
    journal::{Prefix, QueueLimits, Reader, Resolution, Writer},
    metrics::REGISTRY,
    providers::{Connection, b2::Native, s3::S3},
    storage::{Object, Storage, Version},
};
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct LostReplies {
    store: Arc<dyn Storage>,
    attempts: AtomicUsize,
}

#[async_trait]
impl Storage for LostReplies {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.store.get(key, max).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        anyhow::ensure!(
            attempt <= 17 && bytes.len() <= 33 * 1024,
            "fixture upload budget exceeded"
        );
        self.store.put(key, bytes).await?;
        anyhow::ensure!(
            !attempt.is_multiple_of(5),
            "injected lost journal upload response"
        );
        Ok(())
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        anyhow::bail!("data path must not issue a conditional update")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        anyhow::bail!("journal recovery must use point reads")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("fixture cleanup belongs to the disposable-bucket owner")
    }
}

async fn connect(provider: &str, bucket: &str, namespace: &str) -> Connection {
    let result = match provider {
        "b2" => Native::connect_storage(bucket, namespace).await,
        "s3" => S3::connect_storage(bucket, namespace).await,
        _ => panic!("provider must be b2 or s3"),
    };
    result.unwrap_or_else(|_| panic!("provider connection failed"))
}

fn save(path: &Path, report: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(report).unwrap()).unwrap();
}

fn control_counts() -> Vec<u64> {
    let metrics = REGISTRY.render();
    ["authority_read", "authority_replace", "read_barrier"]
        .into_iter()
        .map(|operation| {
            let prefix = format!(
                "durable_db_operation_duration_seconds_count{{operation=\"{operation}\"}} "
            );
            metrics
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly disposable provider bucket and a report path"]
async fn journal_reconnect_unknown_responses_and_late_uploads() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("dedicated test bucket");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("b2 or s3");
    let context = std::env::var("DURABLE_DB_TEST_CONTEXT").expect("declare provider placement");
    let output = std::env::var("DURABLE_DB_JOURNAL_REPORT").expect("report path");
    let output = Path::new(&output);
    let namespace = format!("journal-{}", uuid::Uuid::new_v4());
    let mut report = json!({
        "schema": 1, "complete": false, "provider": provider, "context": context,
        "namespace": namespace, "journal_put_attempt_bound": 17,
        "journal_payload_bound_bytes": 17 * 33 * 1024,
        "durable_batches": 0, "resolved_unknown_responses": 0,
        "limitations": [
            "Journal component qualification, not transaction throughput or a serving prototype.",
            "Raw transports carry the journal; existing publication initializes only to store the fixture selection.",
            "No distributed ownership, permission expiry, concurrent handoff or tenant admission is implemented here.",
            "Initialization, conditional selection, verification and bucket cleanup are additional operations.",
            "MinIO is an S3-compatible fixture, not AWS qualification."
        ]
    });
    save(output, &report);
    let connection = connect(&provider, &bucket, &namespace).await;
    let faults = Arc::new(LostReplies {
        store: connection.objects(),
        attempts: AtomicUsize::new(0),
    });
    let mut writer =
        Writer::new(faults.clone(), uuid::Uuid::new_v4(), QueueLimits::default()).unwrap();
    let records = vec![Bytes::from(vec![42; 1024]); 32];
    let mut uploads = Vec::new();
    for _ in 0..16 {
        let upload = writer.prepare(&records).unwrap();
        uploads.push((upload.position(), upload));
    }
    let before = control_counts();
    let mut responses = stream::iter(uploads)
        .map(|(position, upload)| async move { (position, upload.send().await) })
        .buffer_unordered(4);
    let mut resolved = 0;
    let mut durable = 0;
    while let Some((position, completion)) = responses.next().await {
        let unknown = completion.resolution() == Resolution::Unknown;
        writer.complete(completion).unwrap();
        if unknown {
            assert_eq!(
                writer
                    .resolve(position)
                    .await
                    .unwrap_or_else(|_| panic!("journal resolution failed")),
                Resolution::Durable
            );
            resolved += 1;
        }
        while let Some(batch) = writer.pop_durable().unwrap() {
            assert!(batch.records().eq(records.iter().map(Bytes::as_ref)));
            durable += 1;
        }
        report["durable_batches"] = json!(durable);
        report["resolved_unknown_responses"] = json!(resolved);
        save(output, &report);
    }
    assert_eq!((durable, resolved), (16, 3));
    let after = control_counts();
    report["control_calls_during_batch_path"] = json!(
        after
            .iter()
            .zip(before)
            .map(|(a, b)| a - b)
            .collect::<Vec<_>>()
    );
    assert_eq!(report["control_calls_during_batch_path"], json!([0, 0, 0]));
    assert_eq!(writer.reserved_bytes(), 0);

    let late = writer.prepare(&records).unwrap();
    let expected = writer.durable_prefix();
    drop(writer); // No writer remains to acknowledge the late batch.
    let mut discovery = Reader::discover(faults.clone(), expected.log());
    while discovery
        .read_next()
        .await
        .unwrap_or_else(|_| panic!("journal discovery failed"))
        .is_some()
    {}
    assert_eq!(discovery.finished_prefix(), Some(expected));
    // Explicit fixture control, after the raw journal has been written and recovered.
    // The publication engine is not a dependency of that data path.
    let store = connection
        .into_published(None)
        .await
        .unwrap_or_else(|_| panic!("fixture publication initialization failed"));
    assert!(
        store
            .cas("heads/journal-selection.json", None, expected.encode())
            .await
            .unwrap_or_else(|_| panic!("fixture prefix selection failed"))
    );
    assert_eq!(late.send().await.resolution(), Resolution::Durable);
    assert!(discovery.read_next().await.unwrap().is_none());
    report["journal_put_attempts"] = json!(faults.attempts.load(Ordering::SeqCst));
    drop(discovery);
    drop(faults);
    drop(store);

    let connection = connect(&provider, &bucket, &namespace).await;
    let objects = connection.objects();
    let reopened = connection
        .into_published(None)
        .await
        .unwrap_or_else(|_| panic!("fixture publication reconnect failed"));
    let selected = reopened
        .get("heads/journal-selection.json", 112)
        .await
        .unwrap_or_else(|_| panic!("fixture prefix read failed"))
        .expect("selected prefix exists");
    let selected = Prefix::decode(&selected.bytes).unwrap();
    assert_eq!(selected, expected);
    drop(reopened);
    let mut replay = Reader::replay(objects, selected);
    let mut verified = 0;
    while let Some(batch) = replay
        .read_next()
        .await
        .unwrap_or_else(|_| panic!("selected prefix replay failed"))
    {
        assert!(batch.records().eq(records.iter().map(Bytes::as_ref)));
        verified += batch.records().len();
    }
    assert_eq!(verified, 512);
    assert_eq!(replay.finished_prefix(), Some(expected));
    report["records_verified_after_reconnect"] = json!(verified);
    report["late_suffix_excluded"] = json!(true);
    report["complete"] = json!(true);
    save(output, &report);
}
