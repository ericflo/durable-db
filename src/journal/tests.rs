use super::*;
use crate::storage::{Local, Object, Storage, Version};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use uuid::Uuid;

#[tokio::test]
async fn selected_replay_reuses_positive_reads_within_cache_and_frame_bounds() {
    use crate::cache::Cache;
    let files = Arc::new(Files::default());
    let cache = Arc::new(Cache::new(4096, None).unwrap());
    let mut writer = Writer::new(files.clone(), Uuid::new_v4(), QueueLimits::default()).unwrap();
    for value in [b"first".as_slice(), b"second"] {
        let upload = writer.prepare(&[Bytes::copy_from_slice(value)]).unwrap();
        writer.complete(upload.send().await).unwrap();
        drop(writer.pop_durable().unwrap().unwrap());
    }
    let selected = writer.durable_prefix();
    let mut discovery =
        Reader::discover(files.clone(), writer.log()).with_cache(Some(cache.clone()));
    while discovery.read_next().await.unwrap().is_some() {}
    assert_eq!(discovery.finished_prefix(), Some(selected));
    let gets = files.gets.load(Ordering::SeqCst);
    assert_eq!(gets, 3); // Two positive reads and the end-of-journal probe.
    let mut replay = Reader::replay(files.clone(), selected).with_cache(Some(cache.clone()));
    let first = replay.read_next().await.unwrap().unwrap();
    assert!(replay.read_next().await.is_err()); // Cache hits retain the frame bound.
    drop(first);
    assert!(replay.read_next().await.unwrap().is_some());
    assert!(replay.read_next().await.unwrap().is_none());
    assert_eq!(replay.finished_prefix(), Some(selected));
    assert_eq!(files.gets.load(Ordering::SeqCst), gets);

    let mut wrong = selected;
    wrong.digest[0] ^= 1;
    let mut replay = Reader::replay(files.clone(), wrong).with_cache(Some(cache.clone()));
    assert!(replay.read_next().await.unwrap().is_some());
    assert!(replay.read_next().await.is_err()); // Cache cannot override the cut.
    assert!(replay.finished_prefix().is_none());
    files.fail_read.store(true, Ordering::SeqCst);
    assert!(
        Reader::discover(files.clone(), writer.log())
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .is_err()
    );

    // Unrelated cached data evicts both frames under the same byte ceiling.
    cache.insert("other", Bytes::from(vec![0; 3800])).await;
    assert!(cache.memory_bytes().await <= 4096);
    let before = files.gets.load(Ordering::SeqCst);
    let mut replay = Reader::replay(files.clone(), selected).with_cache(Some(cache.clone()));
    while replay.read_next().await.unwrap().is_some() {}
    assert_eq!(files.gets.load(Ordering::SeqCst) - before, 2);
    assert!(cache.memory_bytes().await <= 4096);
    // A new process/cache has no remembered provider observations.
    let before = files.gets.load(Ordering::SeqCst);
    let mut replay = Reader::replay(files.clone(), selected)
        .with_cache(Some(Arc::new(Cache::new(4096, None).unwrap())));
    while replay.read_next().await.unwrap().is_some() {}
    assert_eq!(files.gets.load(Ordering::SeqCst) - before, 2);
}

#[tokio::test]
async fn journal_cache_does_not_trust_inserts_disk_absence_errors_or_large_frames() {
    use crate::cache::Cache;
    let files = Arc::new(Files::default());
    let log = LogId::fresh(Uuid::new_v4()).unwrap();
    let key = log.key(1);
    let (bytes, digest) = frame::encode(log, 1, [0; 32], &[Bytes::from_static(b"value")]).unwrap();
    let selected = Prefix {
        log,
        position: 1,
        digest,
    };
    let disk = tempfile::tempdir().unwrap();
    let cache = Arc::new(Cache::new(4096, Some((disk.path().into(), 4096))).unwrap());
    cache.insert(&key, bytes.clone()).await;
    // Ordinary insertion is not evidence that these bytes reached storage.
    assert!(
        Reader::replay(files.clone(), selected)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .is_err()
    );
    drop(cache);
    let cache = Arc::new(Cache::new(4096, Some((disk.path().into(), 4096))).unwrap());
    assert!(
        cache
            .get(&key, &hex::encode(Sha256::digest(&bytes)))
            .await
            .is_some()
    );
    assert!(
        Reader::replay(files.clone(), selected)
            .with_cache(Some(cache))
            .read_next()
            .await
            .is_err()
    ); // Disk promotion cannot grant provenance.

    let cache = Arc::new(Cache::new(1024 * 1024, None).unwrap());
    assert!(
        Reader::discover(files.clone(), log)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .unwrap()
            .is_none()
    );
    files.fail_read.store(true, Ordering::SeqCst);
    assert!(
        Reader::discover(files.clone(), log)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .is_err()
    );
    files
        .objects
        .lock()
        .unwrap()
        .insert(key.clone(), Bytes::from_static(b"corrupt"));
    assert!(
        Reader::discover(files.clone(), log)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .is_err()
    );
    let (wrong_chain, _) = frame::encode(log, 1, [1; 32], &[Bytes::new()]).unwrap();
    files
        .objects
        .lock()
        .unwrap()
        .insert(key.clone(), wrong_chain);
    assert!(
        Reader::discover(files.clone(), log)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .is_err()
    );
    assert_eq!(cache.memory_bytes().await, 0);
    files.objects.lock().unwrap().insert(key.clone(), bytes);
    assert!(
        Reader::replay(files.clone(), selected)
            .with_cache(Some(cache.clone()))
            .read_next()
            .await
            .unwrap()
            .is_some()
    ); // Earlier absence was not cached.
    assert!(cache.memory_bytes().await > 0);

    let (large, digest) =
        frame::encode(log, 1, [0; 32], &[Bytes::from(vec![0; 512 * 1024])]).unwrap();
    let selected = Prefix {
        log,
        position: 1,
        digest,
    };
    files.objects.lock().unwrap().insert(key, large);
    for limit in [0, 1024 * 1024] {
        let cache = Arc::new(Cache::new(limit, None).unwrap());
        let before = files.gets.load(Ordering::SeqCst);
        for _ in 0..2 {
            assert!(
                Reader::replay(files.clone(), selected)
                    .with_cache(Some(cache.clone()))
                    .read_next()
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(files.gets.load(Ordering::SeqCst) - before, 2);
        assert_eq!(cache.memory_bytes().await, 0);
    }
}

#[tokio::test]
async fn resetting_selected_replay_preserves_the_budget_of_a_retained_frame() {
    let files = Arc::new(Files::default());
    let mut writer = Writer::new(files.clone(), Uuid::new_v4(), QueueLimits::default()).unwrap();
    let upload = writer.prepare(&[Bytes::from_static(b"retained")]).unwrap();
    writer.complete(upload.send().await).unwrap();
    let prefix = writer.pop_durable().unwrap().unwrap().prefix();
    let mut reader = Reader::replay(files.clone(), prefix);
    assert!(reader.reset(prefix).is_err());
    let batch = reader.read_next().await.unwrap().unwrap();
    assert!(reader.read_next().await.unwrap().is_none());
    let gets = files.gets.load(Ordering::SeqCst);
    assert!(reader.reset(prefix).is_err());
    assert_eq!(files.gets.load(Ordering::SeqCst), gets);
    drop(batch);
    reader.reset(prefix).unwrap();
    assert!(reader.read_next().await.unwrap().is_some());
}

#[derive(Default)]
struct Files {
    objects: Mutex<BTreeMap<String, Bytes>>,
    attempts: Mutex<Vec<(String, Bytes)>>,
    puts: AtomicUsize,
    gets: AtomicUsize,
    fail_read: AtomicBool,
    // 1: unknown before storage; 2: unknown after storage; 3/4: pause before/after.
    fault: AtomicU8,
    paused: Notify,
    resume: Notify,
}

#[async_trait]
impl Storage for Files {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(
            !self.fail_read.swap(false, Ordering::SeqCst),
            "injected read failure"
        );
        let objects = self.objects.lock().unwrap();
        let Some(bytes) = objects.get(key) else {
            return Ok(None);
        };
        anyhow::ensure!(bytes.len() <= max, "object exceeds requested bound");
        Ok(Some(Object {
            bytes: bytes.clone(),
            version: Version(UpdateVersion {
                e_tag: None,
                version: None,
            }),
        }))
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.attempts
            .lock()
            .unwrap()
            .push((key.into(), Bytes::copy_from_slice(&bytes)));
        let fault = self.fault.swap(0, Ordering::SeqCst);
        anyhow::ensure!(fault != 1, "injected unknown upload before visibility");
        if fault == 3 {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        self.objects
            .lock()
            .unwrap()
            .insert(key.into(), Bytes::copy_from_slice(&bytes));
        if fault == 4 {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        anyhow::ensure!(fault != 2, "injected lost upload response");
        Ok(())
    }

    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        anyhow::bail!("journal must not use per-batch conditional publication")
    }

    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        anyhow::bail!("journal recovery must use point reads")
    }

    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("journal must not collect without a controller decision")
    }
}

fn writer(files: Arc<dyn Storage>) -> Writer {
    Writer::new(files, Uuid::new_v4(), QueueLimits::default()).unwrap()
}

async fn append(writer: &mut Writer, value: &'static [u8]) -> Batch {
    let upload = writer.prepare(&[Bytes::from_static(value)]).unwrap();
    writer.complete(upload.send().await).unwrap();
    writer.pop_durable().unwrap().unwrap()
}

async fn wait_for_pause(files: &Files) {
    tokio::time::timeout(Duration::from_secs(5), files.paused.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn pipelined_completion_never_skips_a_hole_and_recovery_uses_no_control_calls() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    let first = writer.prepare(&[Bytes::from_static(b"first")]).unwrap();
    let second = writer
        .prepare(&[Bytes::from_static(b"second"), Bytes::new()])
        .unwrap();
    files.fault.store(3, Ordering::SeqCst);
    let in_flight = tokio::spawn(first.send());
    wait_for_pause(&files).await;
    writer.complete(second.send().await).unwrap();
    assert!(writer.pop_durable().unwrap().is_none());
    assert_eq!(writer.durable_prefix().position(), 0);
    files.resume.notify_one();
    writer.complete(in_flight.await.unwrap()).unwrap();
    assert_eq!(
        writer
            .pop_durable()
            .unwrap()
            .unwrap()
            .records()
            .collect::<Vec<_>>(),
        vec![b"first".as_slice()]
    );
    assert_eq!(
        writer
            .pop_durable()
            .unwrap()
            .unwrap()
            .records()
            .collect::<Vec<_>>(),
        vec![b"second".as_slice(), &[]]
    );
    assert_eq!(writer.pending_bytes(), 0);
    assert_eq!(writer.reserved_bytes(), 0);
    let expected = writer.durable_prefix();
    drop(writer);

    let mut reader = Reader::discover(files.clone(), expected.log());
    assert!(reader.finished_prefix().is_none());
    assert_eq!(
        reader
            .read_next()
            .await
            .unwrap()
            .unwrap()
            .prefix()
            .position(),
        1
    );
    assert_eq!(
        reader.read_next().await.unwrap().unwrap().prefix(),
        expected
    );
    assert!(reader.read_next().await.unwrap().is_none());
    assert_eq!(reader.finished_prefix(), Some(expected));
    assert_eq!(files.puts.load(Ordering::SeqCst), 2);
    assert_eq!(files.gets.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn uncertain_upload_is_resolved_by_exact_bytes_without_a_second_put() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    let upload = writer.prepare(&[Bytes::from_static(b"one")]).unwrap();
    files.fault.store(2, Ordering::SeqCst);
    let mut result = upload.send().await;
    assert_eq!(result.resolution(), Resolution::Unknown);
    assert!(
        result.take_unsent().is_none(),
        "an uncertain PUT cannot be sent again"
    );
    writer.complete(result).unwrap();
    assert!(writer.pop_durable().unwrap().is_none());
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Durable);
    assert_eq!(
        writer
            .pop_durable()
            .unwrap()
            .unwrap()
            .records()
            .collect::<Vec<_>>(),
        vec![b"one".as_slice()]
    );
    assert_eq!(files.puts.load(Ordering::SeqCst), 1);
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Durable);
    assert!(writer.resolve(2).await.is_err());
}

#[tokio::test]
async fn missing_resolution_remains_unknown_and_a_late_upload_can_resolve_it() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    let upload = writer.prepare(&[Bytes::from_static(b"one")]).unwrap();
    files.fault.store(1, Ordering::SeqCst);
    let mut result = upload.send().await;
    assert!(
        result.take_unsent().is_none(),
        "missing bytes do not establish no request"
    );
    writer.complete(result).unwrap();
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Unknown);
    assert!(writer.pop_durable().unwrap().is_none());
    assert!(writer.pending_bytes() > 0);
    let (key, bytes) = files.attempts.lock().unwrap()[0].clone();
    files.objects.lock().unwrap().insert(key, bytes);
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Durable);
    assert!(writer.pop_durable().unwrap().is_some());
    assert_eq!(files.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn resolving_a_paused_upload_keeps_its_reservation_until_the_future_ends() {
    let files = Arc::new(Files::default());
    let mut writer = Writer::new(
        files.clone(),
        Uuid::new_v4(),
        QueueLimits::new(1, 1024).unwrap(),
    )
    .unwrap();
    let upload = writer.prepare(&[Bytes::from_static(b"one")]).unwrap();
    files.fault.store(4, Ordering::SeqCst);
    let in_flight = tokio::spawn(upload.send());
    wait_for_pause(&files).await;
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Durable);
    drop(writer.pop_durable().unwrap().unwrap());
    assert_eq!(writer.pending_batches(), 0);
    assert!(writer.reserved_bytes() > 0);
    assert!(writer.prepare(&[Bytes::from_static(b"two")]).is_err());
    in_flight.abort();
    assert!(matches!(in_flight.await, Err(error) if error.is_cancelled()));
    assert_eq!(writer.reserved_bytes(), 0);
    assert!(writer.prepare(&[Bytes::from_static(b"two")]).is_ok());
}

#[tokio::test]
async fn a_delayed_unknown_reply_does_not_erase_a_successful_resolution() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    let upload = writer.prepare(&[Bytes::from_static(b"one")]).unwrap();
    files.fault.store(2, Ordering::SeqCst);
    let delayed = upload.send().await;
    assert_eq!(writer.resolve(1).await.unwrap(), Resolution::Durable);
    writer.complete(delayed).unwrap();
    assert!(writer.pop_durable().unwrap().is_some());
    assert_eq!(writer.reserved_bytes(), 0);
}

#[tokio::test]
async fn selected_prefix_survives_late_suffix_upload_and_local_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(Local::open(directory.path().into()).unwrap());
    let mut writer = writer(local.clone());
    drop(append(&mut writer, b"accepted").await);
    let late = writer
        .prepare(&[Bytes::from_static(b"unacknowledged")])
        .unwrap();
    let mut discovery = Reader::discover(local.clone(), writer.log());
    assert!(discovery.read_next().await.unwrap().is_some());
    assert!(discovery.read_next().await.unwrap().is_none());
    let selected = discovery.finished_prefix().unwrap();
    // The local fixture supplies an atomic selection. Distributed permission
    // expiry and conditional control publication are separate, still-open work.
    assert!(
        local
            .cas("heads/journal-selection", None, selected.encode())
            .await
            .unwrap()
    );
    assert_eq!(late.send().await.resolution(), Resolution::Durable);
    assert!(discovery.read_next().await.unwrap().is_none());
    assert_eq!(discovery.finished_prefix(), Some(selected));
    drop(discovery);
    drop(writer);
    drop(local);

    let reopened = Arc::new(Local::open(directory.path().into()).unwrap());
    let selected_bytes = reopened
        .get("heads/journal-selection", 112)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    let persisted = Prefix::decode(&selected_bytes).unwrap();
    let mut reader = Reader::replay(reopened.clone(), persisted);
    assert_eq!(
        reader
            .read_next()
            .await
            .unwrap()
            .unwrap()
            .records()
            .collect::<Vec<_>>(),
        vec![b"accepted".as_slice()]
    );
    assert!(reader.read_next().await.unwrap().is_none());
    assert_eq!(reader.finished_prefix(), Some(selected));
    assert_eq!(reopened.list("journal/", None, 10).await.unwrap().len(), 2);
    assert!(reopened.list("blocks/", None, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn missing_selected_data_and_read_errors_never_become_end_of_log() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    drop(append(&mut writer, b"one").await);
    let selected = writer.durable_prefix();
    let mut reader = Reader::replay(files.clone(), selected);
    let bytes = files
        .objects
        .lock()
        .unwrap()
        .remove(&selected.log.key(1))
        .unwrap();
    assert!(reader.read_next().await.is_err());
    assert!(reader.finished_prefix().is_none());
    files
        .objects
        .lock()
        .unwrap()
        .insert(selected.log.key(1), bytes);
    files.fail_read.store(true, Ordering::SeqCst);
    assert!(reader.read_next().await.is_err());
    assert!(reader.finished_prefix().is_none());
    assert!(reader.read_next().await.unwrap().is_some());
    assert!(reader.read_next().await.unwrap().is_none());
    assert_eq!(reader.finished_prefix(), Some(selected));
    let mut empty = Reader::discover(files.clone(), LogId::fresh(Uuid::new_v4()).unwrap());
    files.fail_read.store(true, Ordering::SeqCst);
    assert!(empty.read_next().await.is_err());
    assert!(empty.finished_prefix().is_none());
    assert!(empty.read_next().await.unwrap().is_none());
    assert_eq!(empty.finished_prefix().unwrap().position(), 0);
}

#[tokio::test]
async fn resolution_and_replay_reject_substitution_even_with_a_valid_checksum() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    let upload = writer.prepare(&[Bytes::from_static(b"expected")]).unwrap();
    files.fault.store(2, Ordering::SeqCst);
    writer.complete(upload.send().await).unwrap();
    let original = files.objects.lock().unwrap()[&writer.log().key(1)].clone();
    let (changed, _) =
        frame::encode(writer.log(), 1, [0; 32], &[Bytes::from_static(b"changed")]).unwrap();
    files
        .objects
        .lock()
        .unwrap()
        .insert(writer.log().key(1), changed.clone());
    assert!(writer.resolve(1).await.is_err());
    assert!(writer.pop_durable().unwrap().is_none());
    files
        .objects
        .lock()
        .unwrap()
        .insert(writer.log().key(1), original);
    writer.resolve(1).await.unwrap();
    drop(writer.pop_durable().unwrap().unwrap());
    let selected = writer.durable_prefix();
    files
        .objects
        .lock()
        .unwrap()
        .insert(writer.log().key(1), changed);
    let mut reader = Reader::replay(files.clone(), selected);
    assert!(reader.read_next().await.is_err());
    assert!(reader.finished_prefix().is_none());
    let (wrong_chain, _) =
        frame::encode(writer.log(), 1, [1; 32], &[Bytes::from_static(b"changed")]).unwrap();
    files
        .objects
        .lock()
        .unwrap()
        .insert(writer.log().key(1), wrong_chain);
    assert!(
        Reader::discover(files, writer.log())
            .read_next()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn queue_bounds_include_unresolved_work_and_returned_batch_lifetimes() {
    let files = Arc::new(Files::default());
    let mut writer = Writer::new(
        files.clone(),
        Uuid::new_v4(),
        QueueLimits::new(2, 250).unwrap(),
    )
    .unwrap();
    let first = writer.prepare(&[Bytes::new()]).unwrap();
    let second = writer.prepare(&[Bytes::new()]).unwrap();
    assert_eq!(writer.pending_bytes(), 240);
    assert!(writer.prepare(&[Bytes::new()]).is_err());
    writer.complete(first.send().await).unwrap();
    let batch = writer.pop_durable().unwrap().unwrap();
    assert!(writer.prepare(&[Bytes::new()]).is_err());
    drop(batch);
    let third = writer.prepare(&[Bytes::new()]).unwrap();
    assert_eq!(third.position(), 3);
    drop(second); // Never sent; remains unresolved, consumes its queue reservation.
    writer.complete(third.send().await).unwrap();
    assert!(writer.pop_durable().unwrap().is_none());
    assert_eq!(writer.resolve(2).await.unwrap(), Resolution::Unknown);
    assert_eq!(writer.pending_bytes(), 240);
    assert_eq!(writer.reserved_bytes(), 240);
}

#[tokio::test]
async fn fresh_epochs_never_reopen_an_old_position_or_accept_foreign_completions() {
    let files = Arc::new(Files::default());
    let partition = Uuid::new_v4();
    let mut first = Writer::new(files.clone(), partition, QueueLimits::default()).unwrap();
    let mut second = Writer::new(files, partition, QueueLimits::default()).unwrap();
    assert_ne!(first.log(), second.log());
    let upload = first.prepare(&[Bytes::new()]).unwrap();
    assert!(second.complete(upload.send().await).is_err());
}

#[tokio::test]
async fn replay_keeps_one_frame_budget_until_the_consumer_releases_its_views() {
    let files = Arc::new(Files::default());
    let mut writer = writer(files.clone());
    drop(append(&mut writer, b"one").await);
    drop(append(&mut writer, b"two").await);
    let mut reader = Reader::replay(files.clone(), writer.durable_prefix());
    let first = reader.read_next().await.unwrap().unwrap();
    assert!(reader.read_next().await.is_err());
    assert!(reader.finished_prefix().is_none());
    assert_eq!(files.gets.load(Ordering::SeqCst), 1);
    assert!(first.records().eq([b"one".as_slice()]));
    drop(first);
    let last = reader.read_next().await.unwrap().unwrap();
    assert!(last.records().eq([b"two".as_slice()]));
    // Finishing allocates no additional frame and can proceed with the last held.
    assert!(reader.read_next().await.unwrap().is_none());
    assert_eq!(reader.finished_prefix(), Some(writer.durable_prefix()));
}

fn rehash(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
}

#[test]
fn frame_and_prefix_decoding_reject_bad_lengths_identities_and_trailing_bytes() {
    let log = LogId::fresh(Uuid::new_v4()).unwrap();
    let (original, digest) =
        frame::encode(log, 1, [0; 32], &[Bytes::from_static(b"payload")]).unwrap();
    for offset in [0, 8, 24, 40] {
        // Format, partition, epoch and position.
        let mut bytes = original.to_vec();
        bytes[offset] ^= 1;
        rehash(&mut bytes);
        assert!(Batch::decode(bytes.into(), log, 1).is_err());
    }
    for (offset, value) in [(80, u32::MAX), (84, u32::MAX)] {
        let mut bytes = original.to_vec();
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        rehash(&mut bytes);
        assert!(Batch::decode(bytes.into(), log, 1).is_err());
    }
    let mut trailing = original.to_vec();
    trailing.insert(trailing.len() - 32, 0);
    rehash(&mut trailing);
    assert!(Batch::decode(trailing.into(), log, 1).is_err());
    assert!(Batch::decode(original.slice(..original.len() - 1), log, 1).is_err());
    assert!(frame::encoded_len(&[]).is_err());
    assert!(frame::encoded_len(&vec![Bytes::new(); MAX_RECORDS + 1]).is_err());
    assert!(frame::encoded_len(&[Bytes::from(vec![0; MAX_RECORD_BYTES + 1])]).is_err());
    assert!(frame::encoded_len(&vec![Bytes::from(vec![0; MAX_RECORD_BYTES]); 4]).is_err());
    let prefix = Prefix {
        log,
        position: 1,
        digest,
    };
    assert_eq!(Prefix::decode(&prefix.encode()).unwrap(), prefix);
    assert_eq!(
        Prefix::decode(&Prefix::empty(log).encode()).unwrap(),
        Prefix::empty(log)
    );
    let mut bad_empty = prefix.encode().to_vec();
    bad_empty[40..48].fill(0);
    rehash(&mut bad_empty);
    assert!(Prefix::decode(&bad_empty).is_err());
    assert!(Prefix::decode(&prefix.encode()[..111]).is_err());
    let log = LogId::root(uuid::Uuid::from_u128(0xabcdef));
    for position in [1, u64::MAX] {
        assert_eq!(
            LogId::parse_key(&log.key(position)).unwrap(),
            (log, position)
        );
    }
    let key = log.key(1);
    for bad in [
        log.key(0),
        key[..89].into(),
        format!("{key}0"),
        key.to_uppercase(),
        key.replace("journal/", "unknown/"),
        LogId::root(uuid::Uuid::nil()).key(1),
    ] {
        assert!(LogId::parse_key(&bad).is_err());
    }
}

proptest::proptest! {
    #[test]
    fn frame_roundtrip_and_corruption(records in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096), 1..20), offset in proptest::prelude::any::<usize>()) {
        let log = LogId::fresh(Uuid::new_v4()).unwrap();
        let records: Vec<Bytes> = records.into_iter().map(Bytes::from).collect();
        let (bytes, _) = frame::encode(log, 1, [0; 32], &records).unwrap();
        let batch = Batch::decode(bytes.clone(), log, 1).unwrap();
        proptest::prop_assert!(batch.records().eq(records.iter().map(Bytes::as_ref)));
        let mut corrupted = bytes.to_vec();
        let at = offset % corrupted.len();
        corrupted[at] ^= 1;
        proptest::prop_assert!(Batch::decode(corrupted.into(), log, 1).is_err());
    }
}

#[tokio::test]
async fn checkpoint_resume_verifies_the_exact_selected_chain_without_reading_covered_frames() {
    let files = Arc::new(Files::default());
    let mut writer = Writer::new(files.clone(), Uuid::new_v4(), QueueLimits::default()).unwrap();
    let mut prefixes = Vec::new();
    for value in [b"one", b"two"] {
        let upload = writer.prepare(&[Bytes::copy_from_slice(value)]).unwrap();
        writer.complete(upload.send().await).unwrap();
        prefixes.push(writer.pop_durable().unwrap().unwrap().prefix());
    }
    let (cut, end) = (prefixes[0], prefixes[1]);
    files.objects.lock().unwrap().remove(&cut.log.key(1));
    let mut reader = Reader::resume(files.clone(), cut, end).unwrap();
    let frame = reader.read_next().await.unwrap().unwrap();
    assert_eq!(frame.records().collect::<Vec<_>>(), [b"two".as_slice()]);
    drop(frame);
    assert!(reader.read_next().await.unwrap().is_none());
    assert_eq!(reader.finished_prefix(), Some(end));
    assert!(Reader::resume(files.clone(), end, cut).is_err());
    let foreign = Prefix::empty(LogId::fresh(Uuid::new_v4()).unwrap());
    assert!(Reader::resume(files.clone(), foreign, end).is_err());
    let mut bad = cut.encode().to_vec();
    bad[48] ^= 1;
    rehash(&mut bad);
    let bad = Prefix::decode(&bad).unwrap();
    assert!(Reader::resume(files.clone(), bad, cut).is_err());
    assert!(
        Reader::resume(files.clone(), bad, end)
            .unwrap()
            .read_next()
            .await
            .is_err()
    );
    files.objects.lock().unwrap().remove(&end.log.key(2));
    assert!(
        Reader::resume(files, cut, end)
            .unwrap()
            .read_next()
            .await
            .is_err()
    );
}
