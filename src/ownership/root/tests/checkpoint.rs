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
    let view = projection.view(db()).unwrap().unwrap();
    assert_eq!(view.snapshot_floor(), 2);
    assert!(view.get(b"x", 1).is_err());
    assert!(view.get(b"x", 2).unwrap().unwrap().value.is_none());
    assert_eq!(
        view.get(b"x", 3).unwrap().unwrap().value,
        Some(Bytes::new())
    );
    assert_eq!(
        view.get(b"x", 4).unwrap().unwrap().value.as_deref(),
        Some(b"next-owner".as_slice())
    );
    for revision in 1..=4 {
        assert_eq!(
            view.state()
                .resolve(&format!("r-{revision}"), [revision as u8; 32], revision - 1)
                .unwrap(),
            Resolution::Committed(revision)
        );
    }
    assert_eq!(projection.usage().versions, 3); // one checkpoint row + two suffix versions
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
        assert_eq!(
            projection.view(db()).unwrap().unwrap().state().revision(),
            1
        );
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
    assert_eq!(projection.usage().versions, 1);
    let view = projection.view(db()).unwrap().unwrap();
    assert_eq!(view.snapshot_floor(), 3);
    assert!(view.get(b"x", 2).is_err());
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
    assert_eq!(projection.usage().versions, 1);
    assert_eq!(projection.usage().receipts, 128);
    let view = projection.view(db()).unwrap().unwrap();
    assert_eq!(view.state().revision(), 140);
    assert_eq!(
        view.state().resolve("r-1", [1; 32], 0).unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.state().resolve("r-13", [13; 32], 12).unwrap(),
        Resolution::Committed(13)
    );
    assert_eq!(
        view.state().resolve("r-140", [140; 32], 139).unwrap(),
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
        Recovering::begin(root.replay().unwrap(), Limits::default())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(projection.usage().databases, 10005);
    assert_eq!(projection.usage().versions, 10005);
    assert_eq!(projection.usage().receipts, 10005);
    for n in 0..10005 {
        let view = projection
            .view(Database {
                name: &format!("app-{n:05}"),
                generation: Uuid::from_u128(n + 1),
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            view.get(b"x", 1).unwrap().unwrap().value,
            Some(Bytes::new())
        );
        assert_eq!(
            view.state().resolve("same-request", [1; 32], 0).unwrap(),
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
            .unwrap()
            .unwrap()
            .get(b"x", 2)
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"after".as_slice())
    );
}
