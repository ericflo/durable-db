use super::{
    checkpoint::{append, db, selected},
    *,
};
use crate::{
    cache::Cache,
    index::recovery::{Limits, Recovering, checkpoint::Snapshot},
    storage::Storage,
};

async fn publish(root: &mut Root<ManualClock>) {
    let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
        .await
        .unwrap()
        .advance(10)
        .await
        .unwrap();
    assert!(recovery.is_complete());
    let prepared = root
        .publish_checkpoint(recovery.checkpoint().await.unwrap())
        .unwrap();
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}
fn snapshot(provider: &Arc<Provider>, retained: RetainedCheckpoint) -> Snapshot {
    Snapshot::new(
        provider.files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        retained.reference(),
    )
}
async fn initial(provider: &Arc<Provider>, clock: &ManualClock) -> Root<ManualClock> {
    let mut first = active(provider, clock).await;
    assert!(matches!(first.retain_checkpoint(), Err(Error::NotReady)));
    let mut writer = first.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 1, Some(b"before")).await;
    let mut root = selected(provider, clock).await;
    publish(&mut root).await;
    root
}

#[tokio::test]
async fn retained_checkpoint_survives_lost_reply_takeover_collection_and_identity_checked_release()
{
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = initial(&provider, &clock).await;
    let (retained, prepared) = root.retain_checkpoint().unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    assert!(root.retained_checkpoint().is_err());
    validate(&mut root).await;
    assert!(root.retained_checkpoint().unwrap() == Some(retained));
    assert!(matches!(
        root.retain_checkpoint(),
        Err(Error::RetentionOccupied)
    ));
    let mut writer = root.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 2, None).await;
    append(&mut writer, 3, Some(b"after")).await;
    drop(root);
    let mut root = selected(&provider, &clock).await;
    publish(&mut root).await;
    assert!(root.retained_checkpoint().unwrap() == Some(retained));
    let collector = root.collection();
    collector.collect().await.unwrap();
    // No journal or warm cache is needed for the retained values and receipts.
    assert!(
        provider
            .files
            .list("journal/", None, 128)
            .await
            .unwrap()
            .is_empty()
    );
    let old = snapshot(&provider, retained);
    old.verify(Limits::default()).await.unwrap();
    let database = old.database(db()).await.unwrap().unwrap();
    assert_eq!(
        database.get("x").await.unwrap().unwrap().value.as_deref(),
        Some(b"before".as_slice())
    );
    assert_eq!(
        database.resolve("r-1", [1; 32], 0).await.unwrap(),
        crate::transactions::recovery::Resolution::Committed(1)
    );
    validate(&mut root).await;
    assert!(matches!(
        root.release_checkpoint(Uuid::new_v4()),
        Err(Error::RetentionOccupied)
    ));
    let prepared = root.release_checkpoint(retained.id()).unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    validate(&mut root).await;
    assert!(root.retained_checkpoint().unwrap().is_none());
    // An empty-slot release still compares: its delayed completion cannot
    // remove a newly retained image, even after local cancellation.
    let late = root.release_checkpoint(retained.id()).unwrap();
    root.abandon();
    validate(&mut root).await;
    let (next, prepared) = root.retain_checkpoint().unwrap();
    assert_ne!(next.id(), retained.id());
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    assert!(matches!(
        root.finish(late.execute().await).unwrap(),
        Outcome::Ignored
    ));
    validate(&mut root).await;
    assert!(root.retained_checkpoint().unwrap() == Some(next));
    assert!(matches!(
        root.release_checkpoint(retained.id()),
        Err(Error::RetentionOccupied)
    ));
    collector.collect().await.unwrap();
    assert!(
        snapshot(&provider, retained)
            .verify(Limits::default())
            .await
            .is_err()
    );
    snapshot(&provider, next)
        .verify(Limits::default())
        .await
        .unwrap();
    validate(&mut root).await;
    for _ in 0..2 {
        let prepared = root.release_checkpoint(next.id()).unwrap();
        assert!(matches!(
            root.finish(prepared.execute().await).unwrap(),
            Outcome::Confirmed
        ));
    }
    collector.collect().await.unwrap();
    // Releasing a backup never deletes the current serving checkpoint.
    snapshot(&provider, next)
        .verify(Limits::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn late_retention_cannot_resurrect_an_image_collected_after_a_new_checkpoint() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = initial(&provider, &clock).await;
    let mut writer = root.take_journal(QueueLimits::default()).unwrap();
    append(&mut writer, 2, Some(b"after")).await;
    let (retained, prepared) = root.retain_checkpoint().unwrap();
    let mut successor = selected(&provider, &clock).await;
    publish(&mut successor).await;
    assert!(successor.collection().collect().await.unwrap() > 0);
    assert!(
        snapshot(&provider, retained)
            .verify(Limits::default())
            .await
            .is_err()
    );
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert!(provider.record().retained.is_none());
}

#[tokio::test]
async fn invalid_retention_metadata_stops_collection_before_deletion() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = initial(&provider, &clock).await;
    let (_, prepared) = root.retain_checkpoint().unwrap();
    assert!(matches!(
        root.finish(prepared.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let info = provider
        .history
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .info
        .clone();
    let tables = provider.files.list("tables/", None, 128).await.unwrap();
    let valid: serde_json::Value =
        serde_json::from_str(&info["durable-db-retained-checkpoint"]).unwrap();
    let mut nil = valid.clone();
    nil["id"] = serde_json::json!(Uuid::nil().to_string());
    let mut corrupt = valid.clone();
    corrupt["checkpoint"] = serde_json::json!("invalid");
    for invalid in [
        "not-json".to_string(),
        " ".repeat(1025),
        nil.to_string(),
        corrupt.to_string(),
    ] {
        provider
            .history
            .lock()
            .unwrap()
            .last_mut()
            .unwrap()
            .info
            .insert("durable-db-retained-checkpoint".into(), invalid);
        assert!(root.observe().await.is_err());
        assert!(root.collection().collect().await.is_err());
        assert_eq!(
            provider.files.list("tables/", None, 128).await.unwrap(),
            tables
        );
    }
    provider.history.lock().unwrap().last_mut().unwrap().info = info;
    root.collection().collect().await.unwrap();
}

#[tokio::test]
async fn backup_slot_changes_rebase_staged_checkpoints_without_losing_retention() {
    for keep in [false, true] {
        let provider = Provider::new();
        let clock = ManualClock::default();
        let mut root = initial(&provider, &clock).await;
        let (retained, prepared) = root.retain_checkpoint().unwrap();
        assert!(matches!(
            root.finish(prepared.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        let mut writer = root.take_journal(QueueLimits::default()).unwrap();
        append(&mut writer, 2, Some(b"after")).await;
        let mut root = selected(&provider, &clock).await;
        let recovery = Recovering::begin(root.replay().unwrap(), Limits::default())
            .await
            .unwrap()
            .advance(10)
            .await
            .unwrap();
        let staged = recovery.checkpoint().await.unwrap();
        let prepared = root.release_checkpoint(retained.id()).unwrap();
        assert!(matches!(
            root.finish(prepared.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        let expected = if keep {
            let (retained, prepared) = root.retain_checkpoint().unwrap();
            assert!(matches!(
                root.finish(prepared.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            Some(retained)
        } else {
            None
        };
        let prepared = root.publish_checkpoint(staged).unwrap();
        assert!(matches!(
            root.finish(prepared.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        assert!(root.retained_checkpoint().unwrap() == expected);
        root.collection().collect().await.unwrap();
        assert_eq!(
            snapshot(&provider, retained)
                .verify(Limits::default())
                .await
                .is_ok(),
            keep
        );
    }
}
