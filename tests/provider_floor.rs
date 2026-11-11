//! Opt-in measurement of the existing provider bindings, without engine publication.
//! This is a raw object benchmark, not transaction throughput or a serving prototype.
use bytes::Bytes;
use durable_db::{
    journal::{QueueLimits, Reader, Resolution, Writer},
    metrics::REGISTRY,
    providers::{Connection, b2::Native, s3::S3},
    storage::Storage,
};
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Instant};

const OPERATIONS: usize = 32;

fn save(path: &Path, report: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(report).unwrap()).unwrap();
}

fn control_counts() -> Vec<u64> {
    let rendered = REGISTRY.render();
    ["authority_read", "authority_replace", "read_barrier"]
        .into_iter()
        .map(|name| {
            let prefix =
                format!("durable_db_operation_duration_seconds_count{{operation=\"{name}\"}} ");
            rendered
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}

async fn connect(provider: &str, bucket: &str, prefix: &str) -> Connection {
    let result = match provider {
        "b2" => Native::connect_storage(bucket, prefix).await,
        "s3" => S3::connect_storage(bucket, prefix).await,
        _ => panic!("provider must be b2 or s3"),
    };
    // Never put a provider error body or credential-bearing request in the report/log.
    result.unwrap_or_else(|_| panic!("provider connection failed"))
}

async fn phase(
    storage: Arc<dyn Storage>,
    items: &[(String, Bytes)],
    concurrency: usize,
    write: bool,
) -> Value {
    let before = control_counts();
    let started = Instant::now();
    let outcomes = stream::iter(items.iter().cloned())
        .map(|(key, bytes)| {
            let storage = storage.clone();
            async move {
                let began = Instant::now();
                let ok = if write {
                    storage.put(&key, bytes).await.is_ok()
                } else {
                    matches!(storage.get(&key, bytes.len()).await,
                        Ok(Some(object)) if object.bytes == bytes)
                };
                (began.elapsed().as_secs_f64() * 1000.0, ok)
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    let elapsed = started.elapsed().as_secs_f64();
    let after = control_counts();
    let mut latencies = outcomes.iter().map(|x| x.0).collect::<Vec<_>>();
    latencies.sort_by(f64::total_cmp);
    let percentile = |p: usize| latencies[(latencies.len() * p).div_ceil(100) - 1];
    let succeeded = outcomes.iter().filter(|x| x.1).count();
    json!({
        "operation": if write { "immutable_put" } else { "immutable_get_and_verify" },
        "concurrency": concurrency, "payload_bytes": items[0].1.len(),
        "planned": items.len(), "succeeded": succeeded,
        "failed": items.len() - succeeded, "elapsed_seconds": elapsed,
        "successful_objects_per_second": succeeded as f64 / elapsed,
        "all_attempt_latency_ms": { "p50": percentile(50), "p95": percentile(95),
            "p99": percentile(99), "max": latencies.last() },
        "samples_ms_and_success": outcomes,
        "authority_calls_during_phase": after.iter().zip(before).map(|(a,b)| a-b).collect::<Vec<_>>()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly disposable provider bucket and a report path"]
async fn bounded_raw_provider_floor() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("dedicated test bucket");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("b2 or s3");
    let context =
        std::env::var("DURABLE_DB_TEST_CONTEXT").expect("declare client/provider placement");
    let output = std::env::var("DURABLE_DB_PROVIDER_FLOOR_REPORT").expect("report path");
    let output = Path::new(&output);
    let prefix = format!("floor-{}", uuid::Uuid::new_v4());
    let mut report = json!({
        "schema": 1, "complete": false, "provider": provider, "context": context,
        "namespace": prefix, "operations_per_phase": OPERATIONS, "warmup_objects": 2,
        "maximum_immutable_put_attempts": 194, "maximum_immutable_payload_bytes": 6391808,
        "phases": [], "verified_objects_after_reconnect": 0,
        "limitations": [
            "Raw immutable-object operations, not database commits, transactions or local reads.",
            "32 samples per phase do not establish tail-latency SLOs or sustained capacity.",
            "One process; provider connection pools are reused after two warmup objects.",
            "PUTs use the existing single-attempt binding; GETs may use bounded SDK retries.",
            "Authority-call counts cover the three named logical operations, not every provider HTTP request.",
            "No writer failover, multi-process loss or cleanup race is modeled by this benchmark."
        ]
    });
    save(output, &report);
    let start = Instant::now();
    let storage = connect(&provider, &bucket, &prefix).await.objects();
    report["initialization_seconds"] = json!(start.elapsed().as_secs_f64());
    let mut objects = Vec::new();
    for _ in 0..2 {
        let key = format!("blocks/{}", uuid::Uuid::new_v4());
        let bytes = Bytes::from(vec![42; 1024]);
        assert!(
            storage.put(&key, bytes.clone()).await.is_ok(),
            "warmup PUT failed"
        );
        assert!(matches!(storage.get(&key, 1024).await, Ok(Some(object)) if object.bytes == bytes));
        objects.push((key, bytes));
    }
    for size in [1024, 65536] {
        let bytes = Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        for concurrency in [1, 4, 16] {
            let items = (0..OPERATIONS)
                .map(|_| (format!("blocks/{}", uuid::Uuid::new_v4()), bytes.clone()))
                .collect::<Vec<_>>();
            for write in [true, false] {
                let measured = phase(storage.clone(), &items, concurrency, write).await;
                report["phases"].as_array_mut().unwrap().push(measured);
                save(output, &report);
            }
            objects.extend(items);
        }
    }
    drop(storage);
    let fresh = connect(&provider, &bucket, &prefix).await.objects();
    let mut listed = BTreeSet::new();
    let mut after = None;
    loop {
        let page = fresh
            .list("blocks/", after.as_deref(), 128)
            .await
            .unwrap_or_else(|_| panic!("fresh listing failed"));
        if page.is_empty() {
            break;
        }
        assert!(page.windows(2).all(|p| p[0] < p[1]), "listing not ordered");
        assert!(
            after.as_ref().is_none_or(|a| page[0] > *a),
            "listing did not advance"
        );
        after = page.last().cloned();
        listed.extend(page);
        assert!(
            listed.len() <= 194,
            "unexpected objects in the dedicated namespace"
        );
    }
    report["ordered_listing_matches"] =
        json!(listed == objects.iter().map(|x| x.0.clone()).collect());
    let verified = stream::iter(objects.iter().cloned()).map(|(key, bytes)| {
        let fresh = fresh.clone();
        async move { matches!(fresh.get(&key, bytes.len()).await, Ok(Some(object)) if object.bytes == bytes) }
    }).buffer_unordered(16).collect::<Vec<_>>().await;
    report["verified_objects_after_reconnect"] = json!(verified.iter().filter(|ok| **ok).count());
    report["complete"] = json!(
        verified.iter().all(|ok| *ok)
            && report["ordered_listing_matches"] == true
            && report["phases"]
                .as_array()
                .unwrap()
                .iter()
                .all(|phase| phase["failed"] == 0
                    && phase["authority_calls_during_phase"] == json!([0, 0, 0]))
    );
    save(output, &report);
    // The caller removes the dedicated bucket (including uncertain uploads and metadata).
    assert_eq!(
        report["complete"], true,
        "provider floor run has failures; inspect report"
    );
}

/// One shared transport experiment; these proposal keys are disposable fixture
/// data, not an adopted permit format or a production ownership controller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly disposable provider bucket and a report path"]
async fn revocable_permit_floor() {
    assert_eq!(std::env::var("DURABLE_DB_DISPOSABLE").as_deref(), Ok("1"));
    let bucket = std::env::var("DURABLE_DB_TEST_BUCKET").expect("dedicated bucket");
    assert!(bucket.starts_with("durable-db-test-"));
    let provider = std::env::var("DURABLE_DB_TEST_PROVIDER").expect("b2 or s3");
    let context = std::env::var("DURABLE_DB_TEST_CONTEXT").expect("provider placement");
    let output = std::env::var("DURABLE_DB_PROVIDER_FLOOR_REPORT").expect("report path");
    let output = Path::new(&output);
    let namespace = format!("permit-{}", uuid::Uuid::new_v4());
    let mut report = json!({
        "schema": 1, "complete": false, "provider": provider, "context": context,
        "namespace": namespace, "phases": [], "permits": OPERATIONS,
        "maximum_put_attempts": OPERATIONS + 2,
        "limitations": [
            "Provider primitive, not a serving controller, placement protocol or database benchmark.",
            "Finite deletion observations do not prove provider linearizability.",
            "32 samples per phase, one client host, no SLO or sustained-capacity claim.",
            "The positive reply is retained locally across revocation, not delayed inside the provider.",
            "No concurrent collector or competing root acquisition; dedicated fixture owns deletion.",
            "GETs may use bounded SDK retries; PUTs are single attempt.",
            "S3-compatible MinIO does not qualify AWS S3."
        ]
    });
    save(output, &report);
    let connection = connect(&provider, &bucket, &namespace).await;
    let storage = connection.objects();
    let permits = (0..OPERATIONS)
        .map(|_| {
            let id = uuid::Uuid::new_v4();
            (
                format!("proposals/permit-{id}"),
                Bytes::from(id.to_string()),
            )
        })
        .collect::<Vec<_>>();
    // Each unique permit receives exactly one physical PUT in this fixture.
    let initialized = phase(storage.clone(), &permits, 4, true).await;
    report["initialization"] = initialized;
    save(output, &report);
    assert_eq!(report["initialization"]["failed"], 0);
    for concurrency in [1, 4, 16] {
        let measured = phase(storage.clone(), &permits, concurrency, false).await;
        assert_eq!(measured["failed"], 0);
        assert_eq!(measured["authority_calls_during_phase"], json!([0, 0, 0]));
        report["phases"].as_array_mut().unwrap().push(measured);
        save(output, &report);
    }

    let before = control_counts();
    let mut writer = Writer::new(
        storage.clone(),
        uuid::Uuid::new_v4(),
        QueueLimits::default(),
    )
    .unwrap();
    let upload = writer.prepare(&[Bytes::from(vec![42; 1024])]).unwrap();
    let completion = upload.send().await;
    assert_eq!(completion.resolution(), Resolution::Durable);
    writer.complete(completion).unwrap();
    drop(
        writer
            .pop_durable()
            .unwrap()
            .expect("durable before validation"),
    );
    let expected = writer.durable_prefix();
    // This sealed result's successful validation may be consumed after takeover.
    let (key, bytes) = &permits[0];
    let held_reply = storage
        .get(key, bytes.len())
        .await
        .unwrap_or_else(|_| panic!("positive permit GET failed"))
        .expect("live permit");
    assert_eq!(held_reply.bytes, *bytes);
    let late = writer
        .prepare(&[Bytes::from_static(b"late unacknowledged suffix")])
        .unwrap();
    let peer = connect(&provider, &bucket, &namespace).await;
    let started = Instant::now();
    let revocations = stream::iter(permits.iter())
        .map(|(key, _)| {
            let authority = peer.authority();
            async move { authority.delete_versions(key).await.is_ok() }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    report["revoke_all_seconds"] = json!(started.elapsed().as_secs_f64());
    assert!(
        revocations.iter().all(|ok| *ok),
        "physical permit revocation failed"
    );
    // Check absence before scanning, through old and independently opened pools.
    for reader in [storage.clone(), peer.objects()] {
        for (key, bytes) in &permits {
            assert!(matches!(reader.get(key, bytes.len()).await, Ok(None)));
        }
    }
    let mut recovery = Reader::discover(peer.objects(), expected.log());
    while recovery
        .read_next()
        .await
        .unwrap_or_else(|_| panic!("journal recovery failed"))
        .is_some()
    {}
    assert_eq!(recovery.finished_prefix(), Some(expected));
    // A delayed successful response can acknowledge only the already recovered cut.
    assert_eq!(held_reply.bytes, permits[0].1);
    assert_eq!(late.send().await.resolution(), Resolution::Durable);
    assert!(matches!(
        storage.get(&permits[0].0, permits[0].1.len()).await,
        Ok(None)
    ));
    assert!(recovery.read_next().await.unwrap().is_none());
    drop(writer);
    drop(storage);
    drop(peer);
    drop(connection);
    let reopened = connect(&provider, &bucket, &namespace).await.objects();
    for (key, bytes) in &permits {
        assert!(matches!(reopened.get(key, bytes.len()).await, Ok(None)));
    }
    let mut replay = Reader::replay(reopened, expected);
    let recovered = replay.read_next().await.unwrap().expect("selected frame");
    assert_eq!(
        recovered.records().collect::<Vec<_>>(),
        vec![vec![42; 1024].as_slice()]
    );
    drop(recovered);
    assert!(replay.read_next().await.unwrap().is_none());
    let after = control_counts();
    let counts = after
        .iter()
        .zip(before)
        .map(|(a, b)| a - b)
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![0, 0, 0]);
    report["control_calls_during_handoff_fixture"] = json!(counts);
    report["post_revocation_negative_gets"] = json!(OPERATIONS * 3 + 1);
    report["pre_revocation_prefix_recovered"] = json!(true);
    report["late_suffix_excluded"] = json!(true);
    report["complete"] = json!(true);
    save(output, &report);
}
