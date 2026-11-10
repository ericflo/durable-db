use super::{
    catalog::{scan, seal, successor},
    *,
};
use crate::{
    cache::Cache,
    index::{
        recovery::{Limits as IndexLimits, Projection},
        table,
    },
    storage::Storage,
    transactions::{
        record::{Database, Mutation},
        recovery::{
            self, Resolution, Target,
            tests::{Memory, record},
        },
    },
};
use bytes::Bytes;

fn database(name: &str) -> Database<'_> {
    Database {
        name,
        generation: Uuid::from_u128(if name == "a" { 10 } else { 20 }),
    }
}
async fn write(root: &mut Root<ManualClock>, frames: &[Vec<Bytes>]) {
    let mut writer = root.take_journal(QueueLimits::default()).unwrap();
    for records in frames {
        let upload = writer.prepare(records).unwrap();
        writer.complete(upload.send().await).unwrap();
        assert!(writer.pop_durable().unwrap().is_some());
    }
}

#[tokio::test]
async fn selected_history_restores_values_and_receipts_across_owners_without_reexecution() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut first = active(&provider, &clock).await;
    let mut writer = first.take_journal(QueueLimits::default()).unwrap();
    let upload = writer
        .prepare(&[
            record(
                database("a"),
                1,
                "same-id",
                &[Mutation {
                    key: "x",
                    value: Some(b"before"),
                }],
            ),
            record(
                database("b"),
                1,
                "same-id",
                &[Mutation {
                    key: "x",
                    value: Some(b"other"),
                }],
            ),
        ])
        .unwrap();
    writer.complete(upload.send().await).unwrap();
    assert!(writer.pop_durable().unwrap().is_some());
    let late = writer
        .prepare(&[record(
            database("a"),
            2,
            "late",
            &[Mutation {
                key: "x",
                value: Some(b"discard"),
            }],
        )])
        .unwrap();
    let mut middle = successor(&provider, &clock).await;
    let journal = scan(&mut middle).await;
    seal(&mut middle, journal).await;
    write(
        &mut middle,
        &[vec![
            record(
                database("a"),
                2,
                "next",
                &[
                    Mutation {
                        key: "x",
                        value: None,
                    },
                    Mutation {
                        key: "y",
                        value: Some(b"after"),
                    },
                ],
            ),
            record(
                database("b"),
                2,
                "next",
                &[Mutation {
                    key: "x",
                    value: Some(b""),
                }],
            ),
        ]],
    )
    .await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let _late = late.send().await;
    drop(first);
    drop(middle);
    drop(writer);
    let replay = root.replay().unwrap();
    clock.set(clock.now.load(Ordering::SeqCst) + 10);
    let view = recovery::restore(replay, Projection::new(IndexLimits::default()).unwrap())
        .await
        .unwrap();
    assert_eq!(view.usage().databases, 2);
    assert_eq!(view.usage().versions, 5);
    assert_eq!(view.usage().receipts, 4);
    let a = view.view(database("a")).unwrap().unwrap();
    let b = view.view(database("b")).unwrap().unwrap();
    assert!(a.get(b"x", 2).unwrap().unwrap().value.is_none());
    assert_eq!(
        a.get(b"x", 1).unwrap().unwrap().value.as_deref(),
        Some(b"before".as_slice())
    );
    assert_eq!(
        a.get(b"y", 2).unwrap().unwrap().value.as_deref(),
        Some(b"after".as_slice())
    );
    assert_eq!(
        b.get(b"x", 2).unwrap().unwrap().value.as_deref(),
        Some(b"".as_slice())
    );
    assert!(a.get(b"x", 3).is_err());
    assert!(a.scan(b"", None, 3).is_err());
    for name in ["a", "b"] {
        let database = view.view(database(name)).unwrap().unwrap();
        let state = database.state();
        assert_eq!(state.revision(), 2);
        assert_eq!(
            state.resolve("same-id", [1; 32], 0).unwrap(),
            Resolution::Committed(1)
        );
        assert_eq!(
            state.resolve("next", [2; 32], 1).unwrap(),
            Resolution::Committed(2)
        );
        assert_eq!(
            state.resolve("late", [2; 32], 1).unwrap(),
            Resolution::Missing
        );
    }
    // Flush the actual selected-journal projection through the native table
    // codec and read it after dropping the recovered memory view. This is a
    // value flush, not checkpoint publication or permission to retire history.
    let reference = table::build(provider.files.clone(), a.rows())
        .await
        .unwrap();
    drop(view);
    let reopened = table::Table::open(
        provider.files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .get(b"x", 1)
            .await
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"before".as_slice())
    );
    assert!(
        reopened
            .get(b"x", 2)
            .await
            .unwrap()
            .unwrap()
            .value
            .is_none()
    );
    let mut scan = reopened.scan(b"", None, 2).unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().key.as_ref(), b"x");
    assert_eq!(scan.next().await.unwrap().unwrap().key.as_ref(), b"y");
    assert!(scan.next().await.unwrap().is_none());
    assert!(root.permission().unwrap().is_none());
}

#[tokio::test]
async fn failed_or_partial_projection_is_discarded_and_never_resumed() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    write(
        &mut old,
        &[
            vec![record(
                database("a"),
                1,
                "first",
                &[Mutation {
                    key: "x",
                    value: Some(b"first"),
                }],
            )],
            vec![record(
                database("a"),
                2,
                "second",
                &[Mutation {
                    key: "x",
                    value: Some(b"second"),
                }],
            )],
        ],
    )
    .await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    for fail_at in [1, 2] {
        let target = Memory {
            states: BTreeMap::new(),
            values: BTreeMap::new(),
            calls: 0,
            fail_at: Some(fail_at),
            dropped: Arc::new(AtomicBool::new(false)),
        };
        let dropped = target.dropped.clone();
        assert!(
            recovery::restore(root.replay().unwrap(), target)
                .await
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }
    let mut partial = root.replay().unwrap();
    assert!(partial.read_next().await.unwrap().is_some());
    let target = Memory::default();
    let dropped = target.dropped.clone();
    assert!(recovery::restore(partial, target).await.is_err());
    assert!(dropped.load(Ordering::SeqCst));

    let keys = provider.files.list("journal/", None, 10).await.unwrap();
    let last = &keys[1];
    let bytes = provider
        .files
        .get(last, crate::journal::MAX_BATCH_BYTES)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    provider.files.delete(last).await.unwrap();
    let target = Memory::default();
    let dropped = target.dropped.clone();
    assert!(
        recovery::restore(root.replay().unwrap(), target)
            .await
            .is_err()
    );
    assert!(dropped.load(Ordering::SeqCst));
    provider.files.put(last, bytes).await.unwrap();
    let recovered = recovery::restore(root.replay().unwrap(), Memory::default())
        .await
        .unwrap();
    assert_eq!(recovered.calls, 2);
}

#[tokio::test]
async fn a_selected_but_invalid_transaction_cannot_return_an_earlier_partial_view() {
    for invalid in [
        Bytes::from_static(b"unknown record format"),
        record(database("a"), 3, "gap", &[]),
        record(database("a"), 2, "first", &[]),
    ] {
        let provider = Provider::new();
        let clock = ManualClock::default();
        let mut old = active(&provider, &clock).await;
        write(
            &mut old,
            &[vec![
                record(
                    database("a"),
                    1,
                    "first",
                    &[Mutation {
                        key: "x",
                        value: Some(b"first"),
                    }],
                ),
                invalid,
            ]],
        )
        .await;
        let mut root = successor(&provider, &clock).await;
        let journal = scan(&mut root).await;
        seal(&mut root, journal).await;
        let target = Memory::default();
        let dropped = target.dropped.clone();
        assert!(
            recovery::restore(root.replay().unwrap(), target)
                .await
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn cancellation_drops_the_private_target_after_partial_application() {
    struct Paused {
        memory: Memory,
        entered: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl Target for Paused {
        async fn take_state(&mut self, db: Database<'_>) -> anyhow::Result<recovery::State> {
            self.memory.take_state(db).await
        }
        async fn apply(
            &mut self,
            commit: &crate::transactions::record::Commit<'_>,
            state: recovery::State,
        ) -> anyhow::Result<()> {
            self.memory.apply(commit, state).await?;
            self.entered.notify_one();
            std::future::pending().await
        }
    }
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    write(
        &mut old,
        &[vec![record(
            database("a"),
            1,
            "first",
            &[Mutation {
                key: "x",
                value: Some(b"first"),
            }],
        )]],
    )
    .await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let memory = Memory::default();
    let dropped = memory.dropped.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let task = tokio::spawn(recovery::restore(
        root.replay().unwrap(),
        Paused {
            memory,
            entered: entered.clone(),
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn native_recovery_bounds_are_aggregate_and_do_not_alias_database_generations() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let original = database("a");
    let recreated = Database {
        name: "a",
        generation: Uuid::from_u128(11),
    };
    write(
        &mut old,
        &[vec![
            record(
                original,
                1,
                "first",
                &[Mutation {
                    key: "x",
                    value: Some(b"old!"),
                }],
            ),
            record(
                recreated,
                1,
                "first",
                &[Mutation {
                    key: "x",
                    value: Some(b"new!"),
                }],
            ),
        ]],
    )
    .await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let defaults = IndexLimits::default();
    for limits in [
        IndexLimits {
            databases: 1,
            ..defaults
        },
        IndexLimits {
            versions: 1,
            ..defaults
        },
        IndexLimits {
            value_bytes: 9,
            ..defaults
        },
        IndexLimits {
            receipts: 1,
            ..defaults
        },
        IndexLimits {
            receipt_bytes: 89,
            ..defaults
        },
    ] {
        assert!(
            recovery::restore(root.replay().unwrap(), Projection::new(limits).unwrap())
                .await
                .is_err()
        );
    }
    let recovered = recovery::restore(root.replay().unwrap(), Projection::new(defaults).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.usage().databases, 2);
    assert_eq!(recovered.usage().versions, 2);
    assert_eq!(recovered.usage().value_bytes, 10);
    assert_eq!(recovered.usage().receipts, 2);
    assert_eq!(recovered.usage().receipt_bytes, 90);
    for (db, expected) in [(original, b"old!"), (recreated, b"new!")] {
        let view = recovered.view(db).unwrap().unwrap();
        assert_eq!(
            view.get(b"x", 1).unwrap().unwrap().value.as_deref(),
            Some(expected.as_slice())
        );
        assert_eq!(
            view.state().resolve("first", [1; 32], 0).unwrap(),
            Resolution::Committed(1)
        );
    }
    assert!(recovered.view(database("missing")).unwrap().is_none());
}

#[tokio::test]
async fn native_recovery_receipt_eviction_releases_only_receipt_capacity() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let frames: Vec<_> = (1..=3)
        .map(|revision| {
            vec![
                crate::transactions::record::Commit::encode(
                    database("a"),
                    revision,
                    revision - 1,
                    revision - 1,
                    &format!("r-{revision}"),
                    [revision as u8; 32],
                    &[Mutation {
                        key: "x",
                        value: Some(b"v"),
                    }],
                )
                .unwrap(),
            ]
        })
        .collect();
    write(&mut old, &frames).await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let limits = IndexLimits {
        receipts: 1,
        receipt_bytes: 43,
        ..IndexLimits::default()
    };
    let recovered = recovery::restore(root.replay().unwrap(), Projection::new(limits).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.usage().versions, 3);
    assert_eq!(recovered.usage().value_bytes, 6);
    assert_eq!(recovered.usage().receipts, 1);
    assert_eq!(recovered.usage().receipt_bytes, 43);
    let view = recovered.view(database("a")).unwrap().unwrap();
    assert_eq!(
        view.state().resolve("r-1", [1; 32], 0).unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.state().resolve("r-3", [3; 32], 0).unwrap(),
        Resolution::Committed(3)
    );
    assert_eq!(view.get(b"x", 1).unwrap().unwrap().revision, 1);
}

#[tokio::test]
async fn native_selected_recovery_handles_ten_thousand_database_generations() {
    const DATABASES: usize = 10_005;
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let records: Vec<_> = (0..DATABASES)
        .map(|n| {
            record(
                Database {
                    name: &format!("app-{n:05}"),
                    generation: Uuid::from_u128(n as u128 + 1),
                },
                1,
                "first",
                &[Mutation {
                    key: "x",
                    value: Some(b"v"),
                }],
            )
        })
        .collect();
    let frames: Vec<_> = records
        .chunks(crate::journal::MAX_RECORDS)
        .map(<[_]>::to_vec)
        .collect();
    write(&mut old, &frames).await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let recovered = recovery::restore(
        root.replay().unwrap(),
        Projection::new(IndexLimits::default()).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.usage().databases, DATABASES);
    assert_eq!(recovered.usage().versions, DATABASES);
    assert_eq!(recovered.usage().receipts, DATABASES);
    for n in 0..DATABASES {
        let view = recovered
            .view(Database {
                name: &format!("app-{n:05}"),
                generation: Uuid::from_u128(n as u128 + 1),
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            view.state().resolve("first", [1; 32], 0).unwrap(),
            Resolution::Committed(1)
        );
        let row = view.get(b"x", 1).unwrap().unwrap();
        assert_eq!(row.value.as_deref(), Some(b"v".as_slice()));
        assert!(view.get(b"x", 0).unwrap().is_none());
    }
}
