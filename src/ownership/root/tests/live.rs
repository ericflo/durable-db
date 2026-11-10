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
async fn open(root: Root<ManualClock>, limits: live::Limits) -> Owner<ManualClock> {
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
    assert!(owner.scan(&current, "", None, 10).await.unwrap().is_empty());
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
    assert_eq!(owner.usage().versions, 1);
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
