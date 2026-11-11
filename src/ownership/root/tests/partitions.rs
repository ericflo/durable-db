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
