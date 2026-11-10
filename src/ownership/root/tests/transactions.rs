use super::{
    catalog::{scan, seal, successor},
    *,
};
use crate::{
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
    let view = recovery::restore(replay, Memory::default()).await.unwrap();
    assert_eq!(view.calls, 4);
    assert_eq!(view.values.len(), 2);
    assert_eq!(
        view.values[&("a".into(), database("a").generation, "y".into())],
        b"after"
    );
    assert_eq!(
        view.values[&("b".into(), database("b").generation, "x".into())],
        b""
    );
    for name in ["a", "b"] {
        let state = &view.states[&(name.into(), database(name).generation)];
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
    assert!(!view.dropped.load(Ordering::SeqCst));
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
