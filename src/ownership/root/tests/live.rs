use super::*;
use crate::{
    cache::Cache,
    transactions::{
        live::{self, Opening, Owner, Rejection, Request},
        record::{Database, Mutation},
        validation::{self, Read},
    },
};
use catalog::{scan, seal, successor};
use live::Outcome as Result;

fn database(name: &str) -> Database<'_> {
    Database {
        name,
        generation: Uuid::from_u128(10),
    }
}
pub(super) async fn open(root: Root<ManualClock>, limits: live::Limits) -> Owner<ManualClock> {
    Opening::new(
        root,
        limits,
        Arc::new(Cache::new(1024 * 1024, None).unwrap()),
    )
    .unwrap()
    .recover()
    .await
    .unwrap()
}
fn request<'a>(
    snapshot: &'a live::Snapshot,
    id: &'a str,
    reads: &'a [Read<'a>],
    mutations: &'a [Mutation<'a>],
) -> Request<'a> {
    Request {
        snapshot,
        id,
        reads,
        mutations,
    }
}

#[tokio::test]
async fn live_group_commit_hides_pending_values_and_fences_conflicts_and_retries() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(active(&provider, &clock).await, live::Limits::default()).await;
    let calls = provider.calls.load(Ordering::SeqCst);
    let a = owner.snapshot(database("a")).await.unwrap();
    let b = owner.snapshot(database("b")).await.unwrap();
    let writes = [Mutation {
        key: "x",
        value: Some(b"first"),
    }];
    let first = owner
        .prepare(&[
            request(&a, "first", &[Read::Key("x")], &writes),
            request(&a, "first", &[Read::Key("x")], &writes),
            request(&a, "conflict", &[Read::Key("x")], &[]),
            request(&b, "independent", &[], &writes),
        ])
        .await
        .unwrap();
    for ticket in &first.tickets {
        assert_eq!(owner.outcome(*ticket).unwrap(), Result::Pending);
    }
    assert!(owner.get(&a, "x").await.unwrap().is_none());
    let next = owner
        .prepare(&[request(
            &a,
            "next",
            &[],
            &[Mutation {
                key: "x",
                value: None,
            }],
        )])
        .await
        .unwrap();
    owner
        .complete(next.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(owner.outcome(next.tickets[0]).unwrap(), Result::Pending);
    assert!(owner.get(&a, "x").await.unwrap().is_none());
    owner
        .complete(first.upload.unwrap().send().await)
        .await
        .unwrap();
    for ticket in &first.tickets[..2] {
        assert_eq!(owner.outcome(*ticket).unwrap(), Result::Committed(1));
    }
    assert_eq!(
        owner.outcome(first.tickets[2]).unwrap(),
        Result::Rejected(Rejection::Validation(validation::Error::Conflict))
    );
    assert_eq!(
        owner.outcome(next.tickets[0]).unwrap(),
        Result::Committed(2)
    );
    let current = owner.snapshot(database("a")).await.unwrap();
    assert_eq!(current.revision(), 2);
    assert!(
        owner
            .get(&current, "x")
            .await
            .unwrap()
            .unwrap()
            .value
            .is_none()
    );
    let page = owner
        .read_request(
            database("a"),
            &serde_json::from_value(serde_json::json!({"scans":[{"limit":10}]})).unwrap(),
        )
        .await
        .unwrap();
    assert!(page.scans[0].items.is_empty());
    let retry = owner
        .prepare(&[request(&a, "first", &[Read::Key("x")], &writes)])
        .await
        .unwrap();
    assert!(retry.upload.is_none());
    assert_eq!(
        owner.outcome(retry.tickets[0]).unwrap(),
        Result::Committed(1)
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "no control updates for data operations"
    );
    clock.set(33);
    assert!(owner.get(&current, "x").await.is_err());
    assert!(owner.outcome(next.tickets[0]).is_err());
}

#[tokio::test]
async fn live_reopen_preserves_values_receipts_generations_and_excludes_a_late_suffix() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(active(&provider, &clock).await, live::Limits::default()).await;
    let old = owner.snapshot(database("a")).await.unwrap();
    let writes = [Mutation {
        key: "x",
        value: Some(b"value"),
    }];
    let first = owner
        .prepare(&[request(&old, "first", &[], &writes)])
        .await
        .unwrap();
    owner
        .complete(first.upload.unwrap().send().await)
        .await
        .unwrap();
    let late = owner
        .prepare(&[request(
            &old,
            "late",
            &[],
            &[Mutation {
                key: "x",
                value: None,
            }],
        )])
        .await
        .unwrap();
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let _late = late.upload.unwrap().send().await;
    assert!(owner.outcome(first.tickets[0]).is_err());
    drop(owner);
    let mut owner = open(root, live::Limits::default()).await;
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    assert_eq!(snapshot.revision(), 1);
    assert_eq!(
        owner
            .get(&snapshot, "x")
            .await
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"value".as_slice())
    );
    let retry = owner
        .prepare(&[request(&old, "first", &[], &writes)])
        .await
        .unwrap();
    assert!(retry.upload.is_none());
    assert_eq!(
        owner.outcome(retry.tickets[0]).unwrap(),
        Result::Committed(1)
    );
    assert!(owner.get(&old, "x").await.is_err());
    let uncommitted = owner
        .prepare(&[request(&old, "late", &[], &[])])
        .await
        .unwrap();
    assert!(uncommitted.upload.is_none());
    assert_eq!(
        owner.outcome(uncommitted.tickets[0]).unwrap(),
        Result::Rejected(Rejection::Validation(validation::Error::SnapshotExpired))
    );
    let other = owner
        .snapshot(Database {
            name: "a",
            generation: Uuid::from_u128(11),
        })
        .await
        .unwrap();
    assert_eq!(other.revision(), 0);
    assert!(owner.get(&other, "x").await.unwrap().is_none());
}

#[tokio::test]
async fn live_admission_reserves_index_and_queue_before_acceptance() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let limits = live::Limits {
        index: crate::index::recovery::Limits {
            versions: 1,
            ..Default::default()
        },
        journal: QueueLimits::new(1, 4096).unwrap(),
        ..Default::default()
    };
    let mut owner = open(active(&provider, &clock).await, limits).await;
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    let writes = [Mutation {
        key: "x",
        value: Some(b"v"),
    }];
    let first = owner
        .prepare(&[
            request(&snapshot, "first", &[], &writes),
            request(&snapshot, "capacity", &[], &writes),
        ])
        .await
        .unwrap();
    assert!(
        owner
            .prepare(&[request(&snapshot, "queue", &[], &[])])
            .await
            .is_err()
    );
    owner
        .complete(first.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner.outcome(first.tickets[1]).unwrap(),
        Result::Rejected(Rejection::Capacity)
    );
    // Capacity pressure now checkpoints the completed frame and expires its
    // old validator tokens, while retaining the receipt and current value.
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    let second = owner
        .prepare(&[request(&snapshot, "queue", &[], &[])])
        .await
        .unwrap();
    assert_eq!(second.upload.as_ref().unwrap().position(), 2);
    owner
        .complete(second.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner.outcome(second.tickets[0]).unwrap(),
        Result::Committed(2)
    );
    assert_eq!(owner.usage().versions, 0);
}

#[tokio::test]
async fn live_receipt_expiry_is_durable_and_pending_request_ids_cannot_be_reused() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            receipt_window: 1,
            ..Default::default()
        },
    )
    .await;
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    let first = owner
        .prepare(&[request(&snapshot, "first", &[], &[])])
        .await
        .unwrap();
    let changed = owner
        .prepare(&[request(&snapshot, "first", &[Read::Database], &[])])
        .await
        .unwrap();
    assert!(changed.upload.is_none());
    assert_eq!(owner.outcome(changed.tickets[0]).unwrap(), Result::Pending);
    owner
        .complete(first.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner.outcome(changed.tickets[0]).unwrap(),
        Result::Rejected(Rejection::IdentityReused)
    );
    let fresh = owner.snapshot(database("a")).await.unwrap();
    let second = owner
        .prepare(&[request(&fresh, "second", &[], &[])])
        .await
        .unwrap();
    owner
        .complete(second.upload.unwrap().send().await)
        .await
        .unwrap();
    let expired = owner
        .prepare(&[request(&snapshot, "first", &[], &[])])
        .await
        .unwrap();
    assert!(expired.upload.is_none());
    assert_eq!(
        owner.outcome(expired.tickets[0]).unwrap(),
        Result::Rejected(Rejection::ReceiptExpired)
    );
    assert_eq!(owner.usage().receipts, 1);
}

#[tokio::test]
async fn live_checkpoints_schedule_continuously_and_recover_only_the_uncovered_suffix() {
    use crate::storage::Storage;
    let provider = Provider::new();
    let clock = ManualClock::default();
    let limits = live::Limits {
        checkpoint_frames: 2,
        receipt_window: 4,
        ..Default::default()
    };
    let mut owner = open(active(&provider, &clock).await, limits).await;
    // Merely reading an empty generation must not encode revision-zero rows.
    let _empty = owner.snapshot(database("empty")).await.unwrap();
    for n in 1..=7 {
        let snapshot = owner.snapshot(database("a")).await.unwrap();
        let id = format!("r{n}");
        let mutation = [Mutation {
            key: "x",
            value: Some(&[n]),
        }];
        let prepared = owner
            .prepare(&[request(&snapshot, &id, &[], &mutation)])
            .await
            .unwrap();
        if n == 4 {
            provider.lose_reply.store(true, Ordering::SeqCst);
        }
        owner
            .complete(prepared.upload.unwrap().send().await)
            .await
            .unwrap();
        assert_eq!(
            owner.outcome(prepared.tickets[0]).unwrap(),
            Result::Committed(n as u64)
        );
        assert_eq!(owner.usage().versions, n as usize % 2);
        assert!(!owner.checkpoint_due());
    }
    // Fixture deletion proves both discovery and replay skip covered frames.
    // This is not permission for a runtime collector to delete them.
    let keys = provider.files.list("journal/", None, 20).await.unwrap();
    for key in &keys[..6] {
        provider.files.delete(key).await.unwrap();
    }
    let mut root = successor(&provider, &clock).await;
    let mut recovery = root.recovery().unwrap();
    let mut journal = recovery.next().await.unwrap().unwrap();
    assert!(!journal.is_selected());
    assert_eq!(
        journal
            .read_next()
            .await
            .unwrap()
            .unwrap()
            .prefix()
            .position(),
        7
    );
    assert!(journal.read_next().await.unwrap().is_none());
    assert!(recovery.next().await.unwrap().is_none());
    seal(&mut root, journal).await;
    drop(owner);
    let mut owner = open(root, limits).await;
    assert_eq!(owner.usage().versions, 1);
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    assert_eq!(snapshot.revision(), 7);
    assert_eq!(
        owner
            .get(&snapshot, "x")
            .await
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some([7].as_slice())
    );
    // Recovered suffix work counts toward the same schedule across takeovers.
    let prepared = owner
        .prepare(&[request(
            &snapshot,
            "r8",
            &[],
            &[Mutation {
                key: "x",
                value: None,
            }],
        )])
        .await
        .unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(owner.usage().versions, 0);
    let current = owner.snapshot(database("a")).await.unwrap();
    assert!(
        owner
            .get(&current, "x")
            .await
            .unwrap()
            .unwrap()
            .value
            .is_none()
    );
    assert_eq!(current.revision(), 8);
}

#[tokio::test]
async fn live_checkpoint_drains_pending_uploads_before_accepting_more_work() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            checkpoint_frames: 1,
            ..Default::default()
        },
    )
    .await;
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    let first = owner
        .prepare(&[request(&snapshot, "first", &[], &[])])
        .await
        .unwrap();
    let next = owner
        .prepare(&[request(&snapshot, "next", &[], &[])])
        .await
        .unwrap();
    owner
        .complete(first.upload.unwrap().send().await)
        .await
        .unwrap();
    assert!(owner.checkpoint_due());
    assert!(!owner.maintain().await.unwrap());
    assert!(
        owner
            .prepare(&[request(&snapshot, "third", &[], &[])])
            .await
            .is_err()
    );
    owner
        .complete(next.upload.unwrap().send().await)
        .await
        .unwrap();
    assert!(!owner.checkpoint_due());
    let retry = owner
        .prepare(&[request(&snapshot, "first", &[], &[])])
        .await
        .unwrap();
    assert!(retry.upload.is_none());
    assert_eq!(
        owner.outcome(retry.tickets[0]).unwrap(),
        Result::Committed(1)
    );
    let fresh = owner.snapshot(database("a")).await.unwrap();
    let third = owner
        .prepare(&[request(&fresh, "third", &[], &[])])
        .await
        .unwrap();
    assert_eq!(third.upload.as_ref().unwrap().position(), 3);
    owner
        .complete(third.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner.outcome(third.tickets[0]).unwrap(),
        Result::Committed(3)
    );
}

#[tokio::test]
async fn live_checkpoint_cancellation_resolves_publication_and_reads_recheck_authority_after_io() {
    use crate::storage::{Object, Storage, Version};
    use bytes::Bytes;
    struct Paused {
        provider: Arc<Provider>,
        clock: ManualClock,
        pause: AtomicBool,
        expire: AtomicBool,
        entered: tokio::sync::Notify,
    }
    #[async_trait]
    impl Authority for Paused {
        async fn read(&self) -> anyhow::Result<Bucket> {
            self.provider.read().await
        }
        async fn replace(
            &self,
            old: &Bucket,
            info: BTreeMap<String, String>,
        ) -> anyhow::Result<Option<Bucket>> {
            let result = self.provider.replace(old, info).await?;
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(result)
        }
        async fn delete_versions(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }
    #[async_trait]
    impl Storage for Paused {
        async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
            self.provider.files.get(key, max).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
            size: u64,
        ) -> anyhow::Result<Bytes> {
            let bytes = self.provider.files.get_range(key, range, size).await?;
            if self.expire.swap(false, Ordering::SeqCst) {
                self.clock.set(33);
            }
            Ok(bytes)
        }
        async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
            self.provider.files.put(key, bytes).await
        }
        async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn list(
            &self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> anyhow::Result<Vec<String>> {
            self.provider.files.list(prefix, after, limit).await
        }
        async fn delete(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }
    let provider = Provider::new();
    let clock = ManualClock::default();
    let paused = Arc::new(Paused {
        provider: provider.clone(),
        clock: clock.clone(),
        pause: AtomicBool::new(false),
        expire: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
    });
    let mut root = Connection::new(paused.clone(), paused.clone(), "test")
        .root(clock.clone(), profile())
        .unwrap();
    claim(&mut root).await;
    clock.set(23);
    validate(&mut root).await;
    activate(&mut root).await;
    let mut owner = Opening::new(
        root,
        live::Limits {
            checkpoint_frames: 1,
            ..Default::default()
        },
        Arc::new(Cache::new(0, None).unwrap()),
    )
    .unwrap()
    .recover()
    .await
    .unwrap();
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    let prepared = owner
        .prepare(&[request(
            &snapshot,
            "first",
            &[],
            &[Mutation {
                key: "x",
                value: Some(b"value"),
            }],
        )])
        .await
        .unwrap();
    let completion = prepared.upload.unwrap().send().await;
    paused.pause.store(true, Ordering::SeqCst);
    tokio::select! {
        result = owner.complete(completion) => panic!("publication was not paused: {result:?}"),
        _ = paused.entered.notified() => (),
    }
    let objects = provider.files.list("tables/", None, 100).await.unwrap();
    assert!(owner.checkpoint_due());
    assert!(owner.maintain().await.unwrap());
    assert_eq!(
        provider.files.list("tables/", None, 100).await.unwrap(),
        objects,
        "resolve the exact staged image without uploading another copy"
    );
    assert_eq!(
        owner.outcome(prepared.tickets[0]).unwrap(),
        Result::Committed(1)
    );
    let snapshot = owner.snapshot(database("a")).await.unwrap();
    paused.expire.store(true, Ordering::SeqCst);
    assert!(
        owner
            .read_request(
                database("a"),
                &crate::engine::ReadRequest {
                    expected_revision: Some(snapshot.revision()),
                    keys: vec!["x".into()],
                    scans: vec![],
                }
            )
            .await
            .is_err(),
        "authority expired during immutable value I/O"
    );
    assert!(!paused.expire.load(Ordering::SeqCst));
}

#[tokio::test]
async fn live_public_reads_match_the_existing_wire_contract_without_warming_receipts() {
    use crate::engine::{CommitRequest, Engine, ReadRequest, Write};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            checkpoint_frames: 2,
            ..Default::default()
        },
    )
    .await;
    let old = Engine::new(
        Arc::new(
            Remote::new(
                Arc::new(object_store::memory::InMemory::new()),
                "read-contract",
            )
            .unwrap(),
        ),
        Arc::new(Cache::new(0, None).unwrap()),
    );
    old.create("a", Default::default()).await.unwrap();
    let long = format!("{}\u{7f}", "x".repeat(1023));
    let mut mutations = vec![
        Mutation {
            key: "note/a",
            value: Some(b"one"),
        },
        Mutation {
            key: "note/aa",
            value: Some(b""),
        },
        Mutation {
            key: "note/ab",
            value: None,
        },
        Mutation {
            key: "note/b",
            value: Some(b"two"),
        },
        Mutation {
            key: "note/é",
            value: Some(b"unicode"),
        },
        Mutation {
            key: "other",
            value: Some(b"neighbor"),
        },
    ];
    for key in [
        &long,
        "utf/\u{7f}",
        "utf/\u{80}",
        "utf/\u{d7ff}",
        "utf/\u{e000}",
        "utf/\u{10ffff}",
        "\u{10ffff}",
    ] {
        mutations.push(Mutation {
            key,
            value: Some(b"boundary"),
        });
    }
    for stage in 0..3 {
        mutations.sort_by_key(|m| m.key);
        let snapshot = owner.snapshot(database("a")).await.unwrap();
        let prepared = owner
            .prepare(&[request(
                &snapshot,
                &format!("write-{stage}"),
                &[],
                &mutations,
            )])
            .await
            .unwrap();
        owner
            .complete(prepared.upload.unwrap().send().await)
            .await
            .unwrap();
        old.commit(
            "a",
            &CommitRequest {
                expected_revision: stage,
                request_id: format!("write-{stage}"),
                checks: vec![],
                writes: mutations
                    .iter()
                    .map(|m| Write {
                        key: m.key.into(),
                        value: m.value.map(|v| STANDARD.encode(v)),
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
        let controls = provider.calls.load(Ordering::SeqCst);
        let bodies = [
            r#"{}"#,
            r#"{"keys":["note/a","note/aa","note/ab","absent","note/a"]}"#,
            r#"{"expected_revision":1,"keys":["other"],"scans":[{"prefix":"note/","limit":2},{"prefix":"note/","after":"note/aa","limit":2}]}"#,
            r#"{"scans":[{"prefix":"note/","start":"note/aa","end":"note/é","limit":1000},{"prefix":"note/é","limit":1}]}"#,
            r#"{"scans":[{"prefix":"note/","after":"zzz","limit":1},{"start":"z","end":"a","limit":1}]}"#,
            r#"{"scans":[{"prefix":"note/","reverse":true,"limit":2},{"prefix":"note/","reverse":true,"after":"note/b","limit":2}]}"#,
            r#"{"scans":[{"prefix":"note/","reverse":true,"start":"note/aa","end":"note/é","limit":1000},{"prefix":"note/é","reverse":true,"limit":1}]}"#,
            r#"{"scans":[{"reverse":true,"limit":2},{"prefix":"note/","reverse":true,"after":"aaa","limit":2}]}"#,
        ];
        for body in bodies {
            let mut request: ReadRequest = serde_json::from_str(body).unwrap();
            if request.expected_revision.is_some() {
                request.expected_revision = Some(stage + 1);
            }
            let actual = owner.read_request(database("a"), &request).await.unwrap();
            let expected = old.read("a", &request).await.unwrap();
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        for prefix in [
            &long,
            "utf/\u{7f}",
            "utf/\u{d7ff}",
            "utf/\u{10ffff}",
            "\u{10ffff}",
        ] {
            for reverse in [false, true] {
                let request = serde_json::from_value(
                    serde_json::json!({ "scans": [{"prefix":prefix,"reverse":reverse,"limit":2}] }),
                )
                .unwrap();
                let actual = owner.read_request(database("a"), &request).await.unwrap();
                let expected = old.read("a", &request).await.unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
        }
        if stage == 1 {
            assert_eq!(
                owner.usage(),
                crate::index::recovery::Usage::default(),
                "cold reads must not materialize receipt windows or validator state"
            );
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), controls);
        for body in [
            r#"{"expected_revision":0}"#,
            r#"{"keys":[""]}"#,
            r#"{"scans":[{"prefix":"\u0000","limit":1}]}"#,
            r#"{"scans":[{"limit":1001}]}"#,
        ] {
            let request = serde_json::from_str(body).unwrap();
            assert!(owner.read_request(database("a"), &request).await.is_err());
            assert!(old.read("a", &request).await.is_err());
        }
        mutations = vec![
            Mutation {
                key: "note/b",
                value: if stage == 0 { None } else { Some(b"restored") },
            },
            Mutation {
                key: "note/aa",
                value: None,
            },
            Mutation {
                key: "note/c",
                value: Some(b"new"),
            },
        ];
    }
}

#[tokio::test]
async fn live_wire_commits_keep_identity_across_pending_checkpoint_takeover_and_expiry() {
    use crate::{Error, engine::CommitRequest};
    let provider = Provider::new();
    let clock = ManualClock::default();
    let limits = live::Limits {
        checkpoint_frames: 2,
        receipt_window: 2,
        ..Default::default()
    };
    let mut owner = open(active(&provider, &clock).await, limits).await;
    let first: CommitRequest = serde_json::from_str(r#"{"expected_revision":0,"request_id":"first","checks":[{"key":"a","value":null}],"writes":[{"key":"b","value":""},{"key":"a","value":""},{"key":"a","value":"b25l"}]}"#).unwrap();
    let prepared = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(owner.commit_result(prepared.tickets[0]).unwrap().is_none());
    let duplicate = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(duplicate.upload.is_none());
    let mut changed = first.clone();
    changed.checks[0].value = Some("".into());
    let reused = owner
        .prepare_request(database("a"), &changed)
        .await
        .unwrap();
    assert!(reused.upload.is_none());
    let mut stale = first.clone();
    stale.request_id = "stale".into();
    let conflict = owner.prepare_request(database("a"), &stale).await.unwrap();
    assert!(conflict.upload.is_none());
    for ticket in [duplicate.tickets[0], reused.tickets[0], conflict.tickets[0]] {
        assert!(owner.commit_result(ticket).unwrap().is_none());
    }
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    let result = owner.commit_result(prepared.tickets[0]).unwrap().unwrap();
    assert_eq!(result.revision, 1);
    assert!(!result.replayed);
    assert!(
        owner
            .commit_result(duplicate.tickets[0])
            .unwrap()
            .unwrap()
            .replayed
    );
    assert!(matches!(
        owner.commit_result(reused.tickets[0]),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        owner.commit_result(conflict.tickets[0]),
        Err(Error::Conflict)
    ));
    // A committed retry resolves before its now-false value/revision checks.
    let retry = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(retry.upload.is_none());
    assert!(
        owner
            .commit_result(retry.tickets[0])
            .unwrap()
            .unwrap()
            .replayed
    );
    for revision in [1, u64::MAX] {
        stale.expected_revision = revision;
        let conflict = owner.prepare_request(database("a"), &stale).await.unwrap();
        assert!(conflict.upload.is_none());
        assert!(matches!(
            owner.commit_result(conflict.tickets[0]),
            Err(Error::Conflict)
        ));
    }
    let second: CommitRequest = serde_json::from_str(r#"{"expected_revision":1,"request_id":"second","checks":[{"key":"a","value":"b25l"},{"key":"b","value":""},{"key":"missing","value":null}],"writes":[{"key":"a","value":""},{"key":"b","value":null}]}"#).unwrap();
    let prepared = owner.prepare_request(database("a"), &second).await.unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(owner.usage().versions, 0, "second frame checkpoints");
    let retry = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(retry.upload.is_none());
    assert_eq!(
        owner
            .commit_result(retry.tickets[0])
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    assert!(owner.commit_result(retry.tickets[0]).is_err());
    drop(owner);
    let mut owner = open(root, limits).await;
    let retry = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(retry.upload.is_none());
    assert!(
        owner
            .commit_result(retry.tickets[0])
            .unwrap()
            .unwrap()
            .replayed
    );
    let reused = owner
        .prepare_request(database("a"), &changed)
        .await
        .unwrap();
    assert!(matches!(
        owner.commit_result(reused.tickets[0]),
        Err(Error::Invalid(_))
    ));
    // Same ID and request in another generation cannot see this receipt.
    let other = Database {
        name: "a",
        generation: Uuid::from_u128(11),
    };
    let separate = owner.prepare_request(other, &first).await.unwrap();
    owner
        .complete(separate.upload.unwrap().send().await)
        .await
        .unwrap();
    assert!(
        !owner
            .commit_result(separate.tickets[0])
            .unwrap()
            .unwrap()
            .replayed
    );
    let third: CommitRequest = serde_json::from_str(r#"{"expected_revision":2,"request_id":"third","checks":[{"key":"a","value":""},{"key":"b","value":null}],"writes":[{"key":"a","value":null}]}"#).unwrap();
    let prepared = owner.prepare_request(database("a"), &third).await.unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    let expired = owner.prepare_request(database("a"), &first).await.unwrap();
    assert!(expired.upload.is_none());
    assert!(matches!(
        owner.commit_result(expired.tickets[0]),
        Err(Error::ReceiptExpired)
    ));
    clock.failed.store(true, Ordering::SeqCst);
    assert!(owner.commit_result(prepared.tickets[0]).is_err());
    // Validation precedes even the permission check, which currently fails.
    for (key, value) in [("", Some("eA==")), ("a", Some("%%%")), ("a\0b", None)] {
        let mut invalid = third.clone();
        invalid.writes[0].key = key.into();
        invalid.writes[0].value = value.map(str::to_owned);
        assert!(matches!(
            owner.prepare_request(database("a"), &invalid).await,
            Err(Error::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn live_wire_batches_group_independent_databases_and_validate_before_admission() {
    use crate::{Error, engine::CommitRequest};
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(active(&provider, &clock).await, live::Limits::default()).await;
    let write: CommitRequest = serde_json::from_str(
        r#"{"expected_revision":0,"request_id":"write","writes":[{"key":"x","value":""}]}"#,
    )
    .unwrap();
    let mut invalid = write.clone();
    invalid.writes[0].value = Some("invalid!".into());
    assert!(matches!(
        owner
            .prepare_requests(&[(database("a"), &write), (database("b"), &invalid)])
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(owner.usage(), crate::index::recovery::Usage::default());
    let mut changed = write.clone();
    changed.writes[0].value = None;
    assert!(matches!(
        owner
            .prepare_requests(&[(database("a"), &write), (database("a"), &changed)])
            .await,
        Err(Error::Invalid(_))
    ));
    let mut conflict = write.clone();
    conflict.request_id = "conflict".into();
    let prepared = owner
        .prepare_requests(&[
            (database("a"), &write),
            (database("a"), &write),
            (database("a"), &conflict),
            (database("b"), &write),
        ])
        .await
        .unwrap();
    let upload = prepared.upload.unwrap();
    assert_eq!(
        upload.position(),
        1,
        "rejected batches consumed no position"
    );
    assert_eq!(prepared.tickets.len(), 4);
    for ticket in &prepared.tickets {
        assert!(owner.commit_result(*ticket).unwrap().is_none());
    }
    owner.complete(upload.send().await).await.unwrap();
    for (i, replayed) in [(0, false), (1, true), (3, false)] {
        let result = owner.commit_result(prepared.tickets[i]).unwrap().unwrap();
        assert_eq!((result.revision, result.replayed), (1, replayed));
    }
    assert!(matches!(
        owner.commit_result(prepared.tickets[2]),
        Err(Error::Conflict)
    ));
    let mixed = owner
        .prepare_requests(&[
            (database("a"), &write),
            (database("a"), &conflict),
            (database("c"), &write),
        ])
        .await
        .unwrap();
    assert!(
        owner
            .commit_result(mixed.tickets[0])
            .unwrap()
            .unwrap()
            .replayed
    );
    assert!(matches!(
        owner.commit_result(mixed.tickets[1]),
        Err(Error::Conflict)
    ));
    assert!(owner.commit_result(mixed.tickets[2]).unwrap().is_none());
    let upload = mixed.upload.unwrap();
    assert_eq!(upload.position(), 2);
    owner.complete(upload.send().await).await.unwrap();
    assert!(
        !owner
            .commit_result(mixed.tickets[2])
            .unwrap()
            .unwrap()
            .replayed
    );
    // Request/aggregate byte bounds are applied before any database is warmed.
    assert!(
        owner
            .prepare_requests(&vec![(database("a"), &write); 129])
            .await
            .is_err()
    );
    let mut large = write.clone();
    large.writes[0].value = Some("eA==".repeat(20_000)); // malformed base64
    assert!(
        owner
            .prepare_requests(&[(database("a"), &large)])
            .await
            .is_err()
    );
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    large.writes[0].value = Some(STANDARD.encode(vec![0; 64 * 1024]));
    let before = owner.usage();
    assert!(matches!(
        owner
            .prepare_requests(&vec![(database("new"), &large); 49])
            .await,
        Err(Error::Limit(_))
    ));
    assert_eq!(owner.usage(), before);
}

#[tokio::test]
async fn live_wire_admission_does_not_checkpoint_away_its_just_evaluated_snapshot() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut limits = live::Limits::default();
    limits.index.databases = 2; // warming one generation reaches the soft threshold
    let mut owner = open(active(&provider, &clock).await, limits).await;
    let write = serde_json::from_str(
        r#"{"expected_revision":0,"request_id":"write","writes":[{"key":"x","value":""}]}"#,
    )
    .unwrap();
    let prepared = owner.prepare_request(database("a"), &write).await.unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner
            .commit_result(prepared.tickets[0])
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        owner.usage().databases,
        0,
        "checkpoint runs after publication"
    );
}
