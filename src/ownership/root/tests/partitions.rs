use super::*;
use crate::{
    cache::Cache, providers::partitions::Partitions, storage::Storage, upload_budget::UploadLimits,
};
use bytes::Bytes;

fn limits() -> UploadLimits {
    UploadLimits {
        bytes: 64 * 1024 * 1024,
        attempts: 10_000,
    }
}
async fn directory(provider: &Arc<Provider>) -> Partitions {
    provider
        .connection("test")
        .into_partitions(limits(), Arc::new(Cache::new(1024 * 1024, None).unwrap()))
        .await
        .unwrap()
}
async fn acquire(connection: &Connection) -> Root {
    for _ in 0..4 {
        let (mut root, outcome) = budget::attempt(connection).await;
        match outcome {
            Outcome::Confirmed => {
                activate(&mut root).await;
                return root;
            }
            Outcome::Conflict => (),
            _ => panic!("unexpected uncertain claim"),
        }
    }
    panic!("claim remained contended");
}

#[tokio::test]
async fn partitions_own_revoke_and_collect_independently() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let a = directory.partition(1, limits()).await.unwrap();
    let b = directory.partition(2, limits()).await.unwrap();
    let old = acquire(&a).await;
    let other = acquire(&b).await;
    let mut successor = a.root().unwrap();
    // This candidate needs an observation from its own control session.
    let seen = successor.observe().await.unwrap();
    let prepared = successor.claim(seen).unwrap();
    let mut moved_other = acquire(&b).await;
    assert!(
        matches!(
            successor.finish(prepared.execute().await).unwrap(),
            Outcome::Confirmed
        ),
        "another partition's publication must not overwrite this comparison"
    );
    activate(&mut successor).await;
    assert!(old.permission().await.unwrap().is_none());
    assert!(other.permission().await.unwrap().is_none());
    assert!(successor.permission().await.unwrap().is_some());
    assert!(moved_other.permission().await.unwrap().is_some());
    let reads = provider.reads.load(Ordering::SeqCst);
    let calls = provider.calls.load(Ordering::SeqCst);
    for _ in 0..8 {
        assert!(successor.permission().await.unwrap().is_some());
        assert!(moved_other.permission().await.unwrap().is_some());
    }
    assert_eq!(provider.reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls,
        "serving validation does not update shared authority"
    );
    successor.collection().collect().await.unwrap();
    assert!(
        moved_other.permission().await.unwrap().is_some(),
        "collector crossed partition boundary"
    );
    validate(&mut moved_other).await;
    assert!(successor.permission().await.unwrap().is_some());
}

#[tokio::test]
async fn partition_objects_ranges_cursors_and_deletion_are_scoped() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let a = directory.partition(0, limits()).await.unwrap();
    let b = directory.partition(u32::MAX, limits()).await.unwrap();
    let a = a.objects();
    let b = b.objects();
    let key = format!("tables/{}", Uuid::new_v4().simple());
    a.put(&key, Bytes::from_static(b"abcdef")).await.unwrap();
    b.put(&key, Bytes::from_static(b"other!")).await.unwrap();
    assert_eq!(a.get_range(&key, 1..4, 6).await.unwrap(), b"bcd"[..]);
    assert_eq!(a.list("tables/", None, 1).await.unwrap(), vec![key.clone()]);
    assert!(a.list("tables/", Some(&key), 1).await.unwrap().is_empty());
    a.delete(&key).await.unwrap();
    assert!(a.get(&key, 6).await.unwrap().is_none());
    assert_eq!(b.get(&key, 6).await.unwrap().unwrap().bytes, b"other!"[..]);
    for key in ["../tables/x", "/tables/x", "tables/../x", "tables//x"] {
        assert!(a.get(key, 6).await.is_err());
        assert!(a.put(key, Bytes::new()).await.is_err());
        assert!(a.delete(key).await.is_err());
    }
    assert!(a.cas("tables/x", None, Bytes::new()).await.is_err());
    for key in [
        "partitions/0/tables/x",
        "partitions/0000000A/tables/x",
        "partitions/00000000/partitions/00000001/tables/x",
    ] {
        assert!(crate::providers::collection_key(key).is_err());
    }
}

#[tokio::test]
async fn partition_directory_splits_preserves_limits_and_refuses_legacy_open() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    for id in 0..66 {
        directory.partition(id, limits()).await.unwrap();
    }
    for id in [0, 32, 65] {
        let partition = directory
            .partition(id, UploadLimits::default())
            .await
            .unwrap();
        assert_eq!(
            partition
                .objects()
                .upload_budget()
                .await
                .unwrap()
                .unwrap()
                .limits,
            limits()
        );
        assert!(
            acquire(&partition)
                .await
                .permission()
                .await
                .unwrap()
                .is_some()
        );
    }
    assert!(
        provider
            .connection("test")
            .into_published(None)
            .await
            .is_err()
    );
    assert!(provider.root().observe().await.is_err());
    let reopened = provider
        .connection("test")
        .into_partitions(
            UploadLimits::default(),
            Arc::new(Cache::new(0, None).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(reopened.upload_budget().await.unwrap().limits, limits());
    assert!(reopened.upload_budget().await.unwrap().allocated.attempts > 0);
    let empty = Provider::new();
    let exhausted = empty
        .connection("test")
        .into_partitions(
            UploadLimits::default(),
            Arc::new(Cache::new(0, None).unwrap()),
        )
        .await
        .unwrap();
    assert!(
        exhausted.partition(0, limits()).await.is_err(),
        "control publication must obey shared upload ceiling"
    );
    assert!(
        empty
            .files
            .list("manifests/", None, 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn stale_partition_observation_needs_no_write_and_cannot_reconfirm_old_owner() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let partition = directory.partition(3, limits()).await.unwrap();
    let mut old = acquire(&partition).await;
    let historical = provider.history.lock().unwrap().len() - 1;
    let successor = acquire(&partition).await;

    // An intentionally stale bucket pointer still resolves immutable metadata.
    // Observing it must neither mutate authority nor restore serving permission.
    *provider.stale.lock().unwrap() = Some(historical);
    let writes = provider.calls.load(Ordering::SeqCst);
    let seen = old.observe().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), writes);
    assert_eq!(seen.record.unwrap().identity.incarnation, old.incarnation);
    let validate = old.validate(seen).unwrap();
    *provider.stale.lock().unwrap() = None;
    assert!(matches!(
        old.finish(validate.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert!(old.permission().await.unwrap().is_none());
    assert!(successor.permission().await.unwrap().is_some());
}

#[tokio::test]
async fn partition_stale_comparison_and_lost_publication_reply() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let partition = directory.partition(3, limits()).await.unwrap();
    let authority = partition.authority();
    let before = authority.read().await.unwrap();
    let mut next = before.info.clone();
    next.insert("note".into(), "one".into());
    let current = authority.replace(&before, next).await.unwrap().unwrap();
    assert!(
        authority
            .replace(&before, before.info.clone())
            .await
            .unwrap()
            .is_none()
    );
    provider
        .lose_publication_reply
        .store(true, Ordering::SeqCst);
    let mut next = current.info.clone();
    next.insert("note".into(), "two".into());
    assert!(
        authority.replace(&current, next).await.is_err(),
        "lost acknowledgement must stay uncertain"
    );
    assert_eq!(
        authority.read().await.unwrap().info["note"],
        "two",
        "uncertain publication actually committed"
    );
    assert!(
        authority
            .replace(&current, current.info.clone())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn partition_publication_receipt_needs_no_followup_read() {
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let partition = directory.partition(3, limits()).await.unwrap();
    let authority = partition.authority();
    let before = authority.read().await.unwrap();
    let mut first = before.info.clone();
    first.insert("note".into(), "one".into());
    // The exact CAS replies, but all subsequent metadata reads hang.
    provider
        .pause_reads_after_publication
        .store(true, Ordering::SeqCst);
    let receipt = tokio::time::timeout(Duration::from_secs(1), authority.replace(&before, first))
        .await
        .expect("confirmed publication must not wait for a readback")
        .unwrap()
        .unwrap();
    assert!(provider.pause_all_reads.load(Ordering::SeqCst));
    assert_eq!(receipt.revision, before.revision + 1);
    assert_eq!(receipt.info["note"], "one");
    assert!(receipt.version.is_some());
    provider.pause_all_reads.store(false, Ordering::SeqCst);

    // The returned token is the directory's actual pointer: it can update this
    // record once, then cannot overwrite a later independent publication.
    let mut second = receipt.info.clone();
    second.insert("note".into(), "two".into());
    let successor = authority.replace(&receipt, second).await.unwrap().unwrap();
    assert_eq!(successor.revision, receipt.revision + 1);
    assert!(
        authority
            .replace(&receipt, receipt.info.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(authority.read().await.unwrap().info["note"], "two");
}

#[tokio::test]
async fn delayed_partition_receipt_keeps_its_own_pointer_after_a_newer_write() {
    let provider = Provider::new();
    let first = directory(&provider).await;
    let second = directory(&provider).await;
    let a = first.partition(3, limits()).await.unwrap().authority();
    let b = second.partition(3, limits()).await.unwrap().authority();
    let before = a.read().await.unwrap();
    let mut info = before.info.clone();
    info.insert("note".into(), "one".into());
    provider
        .pause_publication_reply
        .store(true, Ordering::SeqCst);
    let pending = tokio::spawn(async move { a.replace(&before, info).await });
    tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
        .await
        .unwrap();
    let published = b.read().await.unwrap();
    assert_eq!(published.info["note"], "one");
    let mut next = published.info.clone();
    next.insert("note".into(), "two".into());
    b.replace(&published, next).await.unwrap().unwrap();
    provider.release.notify_one();
    let receipt = pending.await.unwrap().unwrap().unwrap();
    assert_eq!(receipt.revision, published.revision);
    assert_eq!(receipt.info, published.info);
    assert_eq!(
        receipt.version.as_ref().unwrap().0.e_tag,
        published.version.as_ref().unwrap().0.e_tag
    );
    // A historical success grants no right to overwrite its successor.
    assert!(
        b.replace(&published, published.info.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(b.read().await.unwrap().info["note"], "two");
}

#[tokio::test]
async fn layout_is_immutable_survives_publication_and_refuses_populated_directories() {
    use crate::providers::partitions::Layout;
    let provider = Provider::new();
    let directory = directory(&provider).await;
    assert!(Layout::new(0).is_err());
    assert!(Layout::new(1025).is_err());
    let layout = Layout::new(8).unwrap();
    directory.configure(layout).await.unwrap();
    directory.configure(layout).await.unwrap();
    assert!(directory.configure(Layout::new(4).unwrap()).await.is_err());
    let partition = layout.partition("app").unwrap();
    assert!(partition < layout.partitions());
    assert!(layout.partition("../app").is_err());
    let connection = directory.partition(partition, limits()).await.unwrap();
    let _owner = acquire(&connection).await;
    directory.maintenance().await.unwrap();
    directory.configure(layout).await.unwrap();
    assert!(directory.configure(Layout::new(4).unwrap()).await.is_err());
    let fresh = Provider::new();
    let populated = super::partitions::directory(&fresh).await;
    populated.partition(0, limits()).await.unwrap();
    assert!(populated.configure(layout).await.is_err());
    let wire: serde_json::Value = serde_json::from_str(
        &provider.history.lock().unwrap().last().unwrap().info["durable-db-root"],
    )
    .unwrap();
    assert_eq!(
        wire["layout"],
        serde_json::json!({"format":1,"partitions":8})
    );
}

#[tokio::test]
async fn concurrent_layout_installation_has_one_winner_and_lost_reply_is_recoverable() {
    use crate::providers::partitions::Layout;
    let provider = Provider::new();
    let directory = directory(&provider).await;
    let a = Layout::new(2).unwrap();
    let b = Layout::new(4).unwrap();
    let (a_result, b_result) = tokio::join!(directory.configure(a), directory.configure(b));
    assert_ne!(a_result.is_ok(), b_result.is_ok());
    let winner = if a_result.is_ok() { a } else { b };
    let reopened = provider
        .connection("test")
        .into_partitions(
            UploadLimits::default(),
            Arc::new(Cache::new(0, None).unwrap()),
        )
        .await
        .unwrap();
    reopened.configure(winner).await.unwrap();
    let fresh = Provider::new();
    let directory = super::partitions::directory(&fresh).await;
    fresh.lose_publication_reply.store(true, Ordering::SeqCst);
    assert!(directory.configure(a).await.is_err());
    directory.configure(a).await.unwrap();
    assert!(directory.configure(b).await.is_err());
}

#[tokio::test]
async fn a_publication_staged_before_layout_installation_preserves_the_winner() {
    use crate::providers::partitions::Layout;
    let provider = Provider::new();
    let directory = directory(&provider).await;
    provider.pause_publication.store(true, Ordering::SeqCst);
    let writer = directory.clone();
    let pending = tokio::spawn(async move { writer.partition(0, limits()).await });
    provider.entered.notified().await;
    let layout = Layout::new(4).unwrap();
    directory.configure(layout).await.unwrap();
    provider.release.notify_one();
    pending.await.unwrap().unwrap();
    directory.configure(layout).await.unwrap();
    assert!(directory.configure(Layout::new(2).unwrap()).await.is_err());
}

#[test]
fn mapping_vectors_pin_the_persisted_hash_contract() {
    use crate::providers::partitions::Layout;
    assert_eq!(Layout::new(1).unwrap().partition("a").unwrap(), 0);
    assert_eq!(Layout::new(1).unwrap().partition("my-app").unwrap(), 0);
    assert_eq!(Layout::new(1).unwrap().partition("z_9").unwrap(), 0);
    assert_eq!(Layout::new(1).unwrap().partition("a-b").unwrap(), 0);
    assert_eq!(Layout::new(2).unwrap().partition("a").unwrap(), 1);
    assert_eq!(Layout::new(2).unwrap().partition("my-app").unwrap(), 1);
    assert_eq!(Layout::new(2).unwrap().partition("z_9").unwrap(), 1);
    assert_eq!(Layout::new(2).unwrap().partition("a-b").unwrap(), 1);
    assert_eq!(Layout::new(8).unwrap().partition("a").unwrap(), 5);
    assert_eq!(Layout::new(8).unwrap().partition("my-app").unwrap(), 1);
    assert_eq!(Layout::new(8).unwrap().partition("z_9").unwrap(), 7);
    assert_eq!(Layout::new(8).unwrap().partition("a-b").unwrap(), 1);
    assert_eq!(Layout::new(1024).unwrap().partition("a").unwrap(), 325);
    assert_eq!(Layout::new(1024).unwrap().partition("my-app").unwrap(), 913);
    assert_eq!(Layout::new(1024).unwrap().partition("z_9").unwrap(), 607);
    assert_eq!(Layout::new(1024).unwrap().partition("a-b").unwrap(), 225);
    let invalid: Layout = serde_json::from_str(r#"{"format":1,"partitions":0}"#).unwrap();
    assert!(invalid.partition("a").is_err());
    assert!(Layout::new(2).unwrap().partition("UPPERCASE").is_err());
}
