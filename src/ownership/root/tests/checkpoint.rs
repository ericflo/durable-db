use super::{
    catalog::{scan, seal, successor},
    *,
};
use crate::{
    index::recovery::{Limits, Recovering},
    storage::Storage,
    transactions::{
        record::{Database, Mutation},
        recovery::{Resolution, tests::record},
    },
};
use bytes::Bytes;
fn db() -> Database<'static> {
    Database {
        name: "app",
        generation: Uuid::from_u128(77),
    }
}
async fn append(writer: &mut Writer, revision: u64, value: Option<&[u8]>) {
    let upload = writer
        .prepare(&[record(
            db(),
            revision,
            &format!("r-{revision}"),
            &[Mutation { key: "x", value }],
        )])
        .unwrap();
    writer.complete(upload.send().await).unwrap();
    assert!(writer.pop_durable().unwrap().is_some());
}
async fn selected(provider: &Arc<Provider>, clock: &ManualClock) -> Root<ManualClock> {
    let mut root = successor(provider, clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    root
}
async fn finish(mut recovery: Recovering) -> crate::index::recovery::Projection {
    while !recovery.is_complete() {
        recovery = recovery.advance(1).await.unwrap();
    }
    recovery.finish().unwrap()
}

#[tokio::test]
async fn lazy_overlay_and_checkpoint_rewrite_preserve_untouched_values_generations_and_receipts() {
    use crate::{cache::Cache, transactions::record::Commit};
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut first = active(&provider, &clock).await;
    let mut writer = first.take_journal(QueueLimits::default()).unwrap();
    let neighbor = Database {
        name: "app",
        generation: Uuid::from_u128(78),
    };
    let new = Database {
        name: "later",
        generation: Uuid::from_u128(79),
    };
    let keys: Vec<_> = (0..64).map(|n| format!("bulk/{n:03}")).collect();
    let large = vec![42; 4096];
    let mut mutations: Vec<_> = keys
        .iter()
        .map(|key| Mutation {
            key,
            value: Some(large.as_slice()),
        })
        .collect();
    mutations.extend([
        Mutation {
            key: "a",
            value: Some(b"old-a"),
        },
        Mutation {
            key: "b",
            value: Some(b"old-b"),
        },
    ]);
    mutations.sort_by_key(|m| m.key);
    let frames = [
        vec![
            record(db(), 1, "r-1", &mutations),
            record(
                neighbor,
                1,
                "r-1",
                &[Mutation {
                    key: "a",
                    value: Some(b"neighbor"),
                }],
            ),
        ],
        vec![record(
            db(),
            2,
            "r-2",
            &[
                Mutation {
                    key: "a",
                    value: None,
                },
                Mutation {
                    key: "new",
                    value: Some(b"fresh"),
                },
            ],
        )],
        vec![
            Commit::encode(
                db(),
                3,
                2,
                1,
                "r-3",
                [3; 32],
                &[Mutation {
                    key: "b",
                    value: Some(b""),
                }],
            )
            .unwrap(),
        ],
        vec![record(
            new,
            1,
            "r-1",
            &[Mutation {
                key: "z",
                value: Some(b"first"),
            }],
        )],
    ];
    for frame in &frames {
        let upload = writer.prepare(frame).unwrap();
        writer.complete(upload.send().await).unwrap();
        writer.pop_durable().unwrap().unwrap();
    }
    let mut root = selected(&provider, &clock).await;
    let initial = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(1)
        .await
        .unwrap();
    let staged = initial.checkpoint().await.unwrap();
    let request = root.publish_checkpoint(staged).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let limits = Limits {
        databases: 2,
        versions: 4,
        value_bytes: 32,
        receipts: 3,
        receipt_bytes: 256,
    };
    for limited in [
        Limits {
            versions: 3,
            ..limits
        },
        Limits {
            value_bytes: 8,
            ..limits
        },
        Limits {
            receipts: 2,
            ..limits
        },
        Limits {
            databases: 1,
            ..limits
        },
    ] {
        assert!(
            Recovering::begin(root.replay().unwrap(), limited)
                .await
                .unwrap()
                .advance(4)
                .await
                .is_err()
        );
    }
    let recovered = Recovering::begin_cached(
        root.replay().unwrap(),
        limits,
        Arc::new(Cache::new(1024 * 1024, None).unwrap()),
    )
    .await
    .unwrap()
    .advance(4)
    .await
    .unwrap();
    assert!(recovered.is_complete());
    // Rewrite must stream the original large values despite only 32 resident
    // value bytes being admitted, including the completely untouched generation.
    let rewritten = recovered.checkpoint().await.unwrap();
    let original = recovered.finish().unwrap();
    assert_eq!(original.usage().databases, 2);
    assert_eq!(original.usage().versions, 4);
    assert_eq!(original.usage().receipts, 3);
    assert!(original.usage().value_bytes <= 32);
    let view = original.view(db()).await.unwrap().unwrap();
    for revision in 1..=3 {
        let mut expected = std::collections::BTreeMap::new();
        expected.insert(
            "a".to_owned(),
            if revision == 1 {
                Some(Bytes::from_static(b"old-a"))
            } else {
                None
            },
        );
        expected.insert(
            "b".to_owned(),
            Some(if revision == 3 {
                Bytes::new()
            } else {
                Bytes::from_static(b"old-b")
            }),
        );
        for key in &keys {
            expected.insert(key.clone(), Some(Bytes::copy_from_slice(&large)));
        }
        if revision >= 2 {
            expected.insert("new".to_owned(), Some(Bytes::from_static(b"fresh")));
        }
        let mut scan = view.scan(b"", None, revision).unwrap();
        for (key, value) in expected {
            assert_eq!(
                view.get(key.as_bytes(), revision)
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                value
            );
            let row = scan.next().await.unwrap().unwrap();
            assert_eq!(row.key, key.as_bytes());
            assert_eq!(row.value, value);
        }
        assert!(scan.next().await.unwrap().is_none());
    }
    assert_eq!(
        view.resolve("r-1", [1; 32], 0).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.resolve("r-2", [2; 32], 1).await.unwrap(),
        Resolution::Committed(2)
    );
    let request = root.publish_checkpoint(rewritten).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let replacement = finish(
        Recovering::begin_cached(
            root.replay().unwrap(),
            Limits {
                databases: 1,
                versions: 1,
                value_bytes: 1,
                receipts: 1,
                receipt_bytes: 1,
            },
            Arc::new(Cache::new(1024 * 1024, None).unwrap()),
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(
        replacement.usage(),
        crate::index::recovery::Usage::default()
    );
    let latest = replacement.view(db()).await.unwrap().unwrap();
    assert_eq!(latest.snapshot_floor(), 3);
    assert!(latest.get(b"a", 2).await.is_err());
    assert!(latest.get(b"a", 3).await.unwrap().unwrap().value.is_none());
    assert_eq!(
        latest.get(b"b", 3).await.unwrap().unwrap().value,
        Some(Bytes::new())
    );
    for key in &keys {
        assert_eq!(
            latest
                .get(key.as_bytes(), 3)
                .await
                .unwrap()
                .unwrap()
                .value
                .as_deref(),
            Some(large.as_slice())
        );
    }
    assert_eq!(
        latest.resolve("r-1", [1; 32], 0).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        latest.resolve("r-3", [3; 32], 2).await.unwrap(),
        Resolution::Committed(3)
    );
    assert_eq!(
        replacement
            .view(neighbor)
            .await
            .unwrap()
            .unwrap()
            .get(b"a", 1)
            .await
            .unwrap()
            .unwrap()
            .value,
        Some(Bytes::from_static(b"neighbor"))
    );
    assert_eq!(
        replacement
            .view(new)
            .await
            .unwrap()
            .unwrap()
            .get(b"z", 1)
            .await
            .unwrap()
            .unwrap()
            .value,
        Some(Bytes::from_static(b"first"))
    );
    // Existing immutable views remain coherent; publishing is not collection.
    assert_eq!(
        view.get(b"a", 1).await.unwrap().unwrap().value,
        Some(Bytes::from_static(b"old-a"))
    );
}

#[tokio::test]
async fn checkpoint_inside_a_selected_journal_recovers_suffix_without_covered_frames() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let mut writer = old.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 1, Some(b"one")).await;
    append(&mut writer, 2, None).await;
    append(&mut writer, 3, Some(b"")).await;
    let mut root = selected(&provider, &clock).await;
    let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(2)
        .await
        .unwrap();
    assert!(!recovery.is_complete());
    let request = root
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    validate(&mut root).await;
    // Deliberately remove covered frames to establish they are not downloaded.
    // Production collection is still disabled and requires a retention protocol.
    let keys = provider.files.list("journal/", None, 10).await.unwrap();
    assert_eq!(keys.len(), 3);
    for key in &keys[..2] {
        provider.files.delete(key).await.unwrap();
    }
    assert!(
        crate::transactions::recovery::restore(
            root.replay().unwrap(),
            crate::transactions::recovery::tests::Memory::default()
        )
        .await
        .is_err()
    );
    let mut current = root.take_journal(QueueLimits::default()).unwrap();
    append(&mut current, 4, Some(b"next-owner")).await;
    let mut successor = selected(&provider, &clock).await;
    let restored = Recovering::begin(successor.replay().unwrap(), Limits::default())
        .await
        .unwrap();
    let projection = finish(restored).await;
    let view = projection.view(db()).await.unwrap().unwrap();
    assert_eq!(view.snapshot_floor(), 2);
    assert!(view.get(b"x", 1).await.is_err());
    assert!(view.get(b"x", 2).await.unwrap().unwrap().value.is_none());
    assert_eq!(
        view.get(b"x", 3).await.unwrap().unwrap().value,
        Some(Bytes::new())
    );
    assert_eq!(
        view.get(b"x", 4).await.unwrap().unwrap().value.as_deref(),
        Some(b"next-owner".as_slice())
    );
    for revision in 1..=4 {
        assert_eq!(
            view.resolve(&format!("r-{revision}"), [revision as u8; 32], revision - 1)
                .await
                .unwrap(),
            Resolution::Committed(revision)
        );
    }
    assert_eq!(projection.usage().versions, 2); // only the selected suffix is resident
}

#[tokio::test]
async fn checkpoint_publication_and_successor_claim_are_serialized_in_both_orders() {
    for checkpoint_first in [true, false] {
        let provider = Provider::new();
        let clock = ManualClock::default();
        let mut old = active(&provider, &clock).await;
        let mut writer = old.take_journal(QueueLimits::default()).unwrap();
        append(&mut writer, 1, Some(b"one")).await;
        let mut root = selected(&provider, &clock).await;
        let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
            .await
            .unwrap()
            .advance(1)
            .await
            .unwrap();
        let staged = recovery.checkpoint().await.unwrap();
        let publication = root.publish_checkpoint(staged).unwrap();
        root.abandon();
        let mut next = provider.root(&clock);
        let observed = next.observe().await.unwrap();
        let claim_request = next.claim(observed).unwrap();
        if checkpoint_first {
            assert!(matches!(
                root.finish(publication.execute().await).unwrap(),
                Outcome::Ignored
            ));
            assert!(matches!(
                next.finish(claim_request.execute().await).unwrap(),
                Outcome::Conflict
            ));
            next = provider.root(&clock);
            claim(&mut next).await;
        } else {
            assert!(matches!(
                next.finish(claim_request.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            assert!(matches!(
                root.finish(publication.execute().await).unwrap(),
                Outcome::Ignored
            ));
        }
        assert_eq!(provider.record().checkpoint.is_some(), checkpoint_first);
        clock.set(clock.now.load(Ordering::SeqCst) + 23);
        validate(&mut next).await;
        activate(&mut next).await;
        let journal = scan(&mut next).await;
        seal(&mut next, journal).await;
        let projection = finish(
            Recovering::begin(next.replay().unwrap(), Limits::default())
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(projection.view(db()).await.unwrap().unwrap().revision(), 1);
    }
}

#[tokio::test]
async fn stale_checkpoint_cannot_replace_a_newer_cut_and_snapshot_floor_advances() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let mut writer = old.take_journal(QueueLimits::default()).unwrap();
    for revision in 1..=3 {
        append(&mut writer, revision, Some(b"v")).await;
    }
    let mut root = selected(&provider, &clock).await;
    let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(1)
        .await
        .unwrap();
    let stale = recovery.checkpoint().await.unwrap();
    let recovery = recovery.advance(1).await.unwrap();
    let request = root
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!(matches!(
        root.publish_checkpoint(stale),
        Err(Error::StaleRecovery)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(1)
        .await
        .unwrap();
    let request = root
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let projection = finish(
        Recovering::begin(root.replay().unwrap(), Limits::default())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(projection.usage().versions, 0);
    let view = projection.view(db()).await.unwrap().unwrap();
    assert_eq!(view.snapshot_floor(), 3);
    assert!(view.get(b"x", 2).await.is_err());
}

#[tokio::test]
async fn repeated_checkpoints_bound_recovery_versions_while_preserving_receipts() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let mut writer = old.take_journal(QueueLimits::default()).unwrap();
    for revision in 1..=140 {
        append(&mut writer, revision, Some(b"v")).await;
    }
    let mut root = selected(&provider, &clock).await;
    let limits = Limits {
        versions: 2,
        ..Limits::default()
    };
    assert!(
        crate::transactions::recovery::restore(
            root.replay().unwrap(),
            crate::index::recovery::Projection::new(limits).unwrap()
        )
        .await
        .is_err()
    );
    let mut checkpoints = 0;
    let projection = loop {
        let recovery = Recovering::begin(root.replay().unwrap(), limits)
            .await
            .unwrap()
            .advance(1)
            .await
            .unwrap();
        if recovery.is_complete() {
            break recovery.finish().unwrap();
        }
        let request = root
            .publish_checkpoint(recovery.checkpoint().await.unwrap())
            .unwrap();
        assert!(matches!(
            root.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        checkpoints += 1;
    };
    assert_eq!(checkpoints, 140);
    assert_eq!(projection.usage(), crate::index::recovery::Usage::default());
    let view = projection.view(db()).await.unwrap().unwrap();
    assert_eq!(view.revision(), 140);
    assert_eq!(
        view.resolve("r-1", [1; 32], 0).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.resolve("r-13", [13; 32], 12).await.unwrap(),
        Resolution::Committed(13)
    );
    assert_eq!(
        view.resolve("r-140", [140; 32], 139).await.unwrap(),
        Resolution::Committed(140)
    );
}

#[tokio::test]
async fn checkpoints_page_ten_thousand_generations_in_shared_tables() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let mut writer = old.take_journal(QueueLimits::default()).unwrap();
    for batch in 0..20 {
        let records: Vec<_> = (batch * 512..((batch + 1) * 512).min(10005))
            .map(|n| {
                record(
                    Database {
                        name: &format!("app-{n:05}"),
                        generation: Uuid::from_u128(n + 1),
                    },
                    1,
                    "same-request",
                    &[Mutation {
                        key: "x",
                        value: Some(b""),
                    }],
                )
            })
            .collect();
        let upload = writer.prepare(&records).unwrap();
        writer.complete(upload.send().await).unwrap();
        assert!(writer.pop_durable().unwrap().is_some());
    }
    let mut root = selected(&provider, &clock).await;
    let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(32)
        .await
        .unwrap();
    assert!(recovery.is_complete());
    let request = root
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let objects = provider.files.list("tables/", None, 100).await.unwrap();
    assert_eq!(objects.len(), 18); // eight shared tables, one manifest node, one header
    for key in provider.files.list("journal/", None, 32).await.unwrap() {
        provider.files.delete(&key).await.unwrap();
    }
    let projection = finish(
        Recovering::begin_cached(
            root.replay().unwrap(),
            Limits {
                databases: 1,
                versions: 1,
                value_bytes: 1,
                receipts: 1,
                receipt_bytes: 1,
            },
            Arc::new(crate::cache::Cache::new(1024 * 1024, None).unwrap()),
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(projection.usage(), crate::index::recovery::Usage::default());
    for n in 0..10005 {
        let view = projection
            .view(Database {
                name: &format!("app-{n:05}"),
                generation: Uuid::from_u128(n + 1),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            view.get(b"x", 1).await.unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert_eq!(
            view.resolve("same-request", [1; 32], 0).await.unwrap(),
            Resolution::Committed(1)
        );
    }
}

#[tokio::test]
async fn recovery_ancestry_stops_at_the_checkpoint_instead_of_traversing_covered_owners() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut first = active(&provider, &clock).await;
    let mut writer = first.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 1, Some(b"before")).await;
    let mut second = selected(&provider, &clock).await;
    let mut writer = second.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 2, Some(b"after")).await;
    let mut third = selected(&provider, &clock).await;
    let recovery = Recovering::begin(third.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(10)
        .await
        .unwrap();
    assert!(recovery.is_complete());
    let request = third
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    assert!(matches!(
        third.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    provider
        .files
        .delete(&format!("ownership/{}", first.incarnation.simple()))
        .await
        .unwrap();
    for key in provider.files.list("journal/", None, 10).await.unwrap() {
        provider.files.delete(&key).await.unwrap();
    }
    let mut fourth = selected(&provider, &clock).await;
    let mut ancestry = fourth.recovery().unwrap();
    let mut last = ancestry.next().await.unwrap().unwrap();
    assert_eq!(last.registration().identity().epoch, 3);
    assert!(last.read_next().await.unwrap().is_none());
    assert!(ancestry.next().await.unwrap().is_none());
    let projection = finish(
        Recovering::begin(fourth.replay().unwrap(), Limits::default())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        projection
            .view(db())
            .await
            .unwrap()
            .unwrap()
            .get(b"x", 2)
            .await
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"after".as_slice())
    );
}
