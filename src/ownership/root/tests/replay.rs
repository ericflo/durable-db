use super::catalog::{scan, seal, source, successor};
use super::*;
use crate::storage::Storage;
use bytes::Bytes;

#[tokio::test]
async fn replay_requires_every_prior_cut_and_preserves_transaction_order_after_expiry() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let (_old, late) = source(&provider, &clock).await;
    let mut middle = successor(&provider, &clock).await;
    let mut incomplete = middle.replay().unwrap();
    assert!(incomplete.read_next().await.is_err());
    assert!(!incomplete.completed());
    let journal = scan(&mut middle).await;
    seal(&mut middle, journal).await;
    let mut writer = middle.take_journal(QueueLimits::default()).unwrap();
    for bytes in [b"second".as_slice(), b"third".as_slice()] {
        let upload = writer.prepare(&[Bytes::copy_from_slice(bytes)]).unwrap();
        writer.complete(upload.send().await).unwrap();
        assert!(writer.pop_durable().unwrap().is_some());
    }
    let mut root = successor(&provider, &clock).await;
    let mut incomplete = root.replay().unwrap();
    // An incomplete catalog exposes no earlier frames, even if those are valid.
    assert!(incomplete.read_next().await.is_err());
    assert!(!incomplete.completed());
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let _completion = late.send().await;
    drop(writer);
    drop(middle);
    let mut replay = root.replay().unwrap();
    clock.set(clock.now.load(Ordering::SeqCst) + 10);
    for (epoch, position, bytes) in [
        (1, 1, b"acknowledged".as_slice()),
        (2, 1, b"second".as_slice()),
        (2, 2, b"third".as_slice()),
    ] {
        assert!(!replay.completed());
        let batch = replay.read_next().await.unwrap().unwrap();
        assert_eq!(batch.identity().epoch, epoch);
        assert_eq!(batch.position(), position);
        assert_eq!(batch.records().collect::<Vec<_>>(), vec![bytes]);
    }
    assert!(replay.read_next().await.unwrap().is_none());
    assert!(replay.completed());
    assert!(replay.read_next().await.unwrap().is_none());
    // Immutable replay may finish after expiry; completion grants no authority.
    assert!(root.permission().unwrap().is_none());
}

#[tokio::test]
async fn ordered_replay_pages_a_split_catalog_with_empty_and_nonempty_journals() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut root = active(&provider, &clock).await;
    let mut empty = root.replay().unwrap();
    assert!(!empty.completed());
    assert!(empty.read_next().await.unwrap().is_none());
    assert!(empty.completed());
    for epoch in 1u64..=130 {
        if epoch % 3 != 0 {
            let mut writer = root.take_journal(QueueLimits::default()).unwrap();
            let upload = writer
                .prepare(&[Bytes::copy_from_slice(&epoch.to_be_bytes())])
                .unwrap();
            writer.complete(upload.send().await).unwrap();
            assert!(writer.pop_durable().unwrap().is_some());
        }
        root = successor(&provider, &clock).await;
        let journal = scan(&mut root).await;
        seal(&mut root, journal).await;
    }
    let mut replay = root.replay().unwrap();
    for epoch in (1u64..=130).filter(|n| n % 3 != 0) {
        let batch = replay.read_next().await.unwrap().unwrap();
        assert_eq!(batch.identity().epoch, epoch);
        assert_eq!(batch.position(), 1);
        assert_eq!(
            batch.records().collect::<Vec<_>>(),
            vec![epoch.to_be_bytes().as_slice()]
        );
    }
    assert!(replay.read_next().await.unwrap().is_none());
    assert!(replay.completed());
}

#[tokio::test]
async fn selected_frame_errors_do_not_advance_replay_or_complete_it() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let (_old, _late) = source(&provider, &clock).await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let keys = provider.files.list("journal/", None, 10).await.unwrap();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    let original = provider
        .files
        .get(key, crate::journal::MAX_BATCH_BYTES)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    for corrupt in [false, true] {
        if corrupt {
            let mut bytes = original.to_vec();
            bytes[0] ^= 1;
            provider.files.put(key, bytes.into()).await.unwrap();
        } else {
            provider.files.delete(key).await.unwrap();
        }
        let mut replay = root.replay().unwrap();
        assert!(replay.read_next().await.is_err());
        assert!(!replay.completed());
        provider.files.put(key, original.clone()).await.unwrap();
        let batch = replay.read_next().await.unwrap().unwrap();
        assert_eq!(batch.identity().epoch, 1);
        assert_eq!(batch.position(), 1);
        assert!(replay.read_next().await.unwrap().is_none());
        assert!(replay.completed());
    }
}

#[tokio::test]
async fn catalog_count_alone_cannot_hide_a_noncanonical_key_or_epoch_gap() {
    use crate::directory::{Directory, Pointer};
    use sha2::{Digest, Sha256};
    let provider = Provider::new();
    let clock = ManualClock::default();
    let (_old, _late) = source(&provider, &clock).await;
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let original_prefix = provider
        .files
        .list("manifests/", None, 10)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let bytes = provider
        .files
        .get(&original_prefix, 112)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    let header_key = format!("catalogs/{}", provider.record().catalog.unwrap().simple());
    let header_bytes = provider
        .files
        .get(&header_key, 4096)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    let original: serde_json::Value =
        serde_json::from_slice(&header_bytes[..header_bytes.len() - 32]).unwrap();
    for key in [
        "invalid".to_owned(),
        format!("{:016x}-{}", 3, Uuid::new_v4().simple()),
    ] {
        let pointer = Pointer {
            key: format!("manifests/{key}.{}", Uuid::new_v4()),
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        provider
            .files
            .put(&pointer.key, bytes.clone())
            .await
            .unwrap();
        let index = Directory::new(provider.files.clone())
            .insert_many(None, &BTreeMap::from([(key, pointer)]))
            .await
            .unwrap();
        let mut header = original.clone();
        header["index"] = serde_json::to_value(index).unwrap();
        let mut body = serde_json::to_vec(&header).unwrap();
        let checksum = Sha256::digest(&body);
        body.extend_from_slice(&checksum);
        // Corruption fixture: even a checksum-valid header with the right count
        // cannot turn the wrong key set into completed recovery.
        provider.files.put(&header_key, body.into()).await.unwrap();
        let mut replay = root.replay().unwrap();
        assert!(replay.read_next().await.is_err());
        assert!(!replay.completed());
    }
}
