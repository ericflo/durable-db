use super::*;
use crate::{journal::PreparedUpload, storage::Storage};
use bytes::Bytes;

pub(super) async fn successor(provider: &Arc<Provider>) -> Root {
    let mut root = provider.root();
    claim(&mut root).await;

    validate(&mut root).await;
    activate(&mut root).await;
    root
}
pub(super) async fn source(provider: &Arc<Provider>) -> (Root, PreparedUpload) {
    let mut root = active(provider).await;
    let mut writer = root.take_journal(QueueLimits::default()).unwrap();
    let upload = writer
        .prepare(&[Bytes::from_static(b"acknowledged")])
        .unwrap();
    writer.complete(upload.send().await).unwrap();
    assert_eq!(
        writer.pop_durable().unwrap().unwrap().prefix().position(),
        1
    );
    assert!(root.permission().await.unwrap().is_some());
    let late = writer
        .prepare(&[Bytes::from_static(b"late unacknowledged")])
        .unwrap();
    (root, late)
}
pub(super) async fn scan(root: &mut Root) -> JournalRecovery {
    let mut recovery = root.recovery().unwrap();
    let mut journal = recovery.next().await.unwrap().unwrap();
    while journal.read_next().await.unwrap().is_some() {}
    journal
}
pub(super) async fn seal(root: &mut Root, journal: JournalRecovery) {
    let request = root.select_prefix(journal).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
}

#[tokio::test]
async fn selecting_ancestral_journals_reads_the_registration_chain_once() {
    for count in [8u8, 17] {
        let provider = Provider::new();
        for epoch in 1..=count {
            let mut root = active(&provider).await;
            let mut writer = root.take_journal(QueueLimits::default()).unwrap();
            let upload = writer.prepare(&[Bytes::from(vec![epoch])]).unwrap();
            writer.complete(upload.send().await).unwrap();
            assert!(writer.pop_durable().unwrap().is_some());
        }
        let cache = Arc::new(crate::cache::Cache::new(64 * 1024, None).unwrap());
        let mut root = successor(&provider).await.with_cache(cache.clone());
        let files = registration::Faults::fail_at(provider.files.clone(), "never/", 0);
        root.files = files.clone();
        let before = provider.calls.load(Ordering::SeqCst);
        root.select_journals().await.unwrap();
        let reads = files.frame_reads.load(Ordering::SeqCst);
        assert_eq!(reads, 2 * u64::from(count)); // Each frame plus each end probe.
        let batches = u64::from(count).div_ceil(8);
        assert_eq!(provider.calls.load(Ordering::SeqCst) - before, batches);
        // One immutable prefix per journal; one index page and header per batch.
        assert_eq!(
            files.puts.load(Ordering::SeqCst),
            u64::from(count) + 2 * batches
        );
        assert!(files.registration_reads.load(Ordering::SeqCst) <= u64::from(count) + 1);
        let mut recovery = root.recovery().unwrap();
        for epoch in (1..=count).rev() {
            let mut journal = recovery.next().await.unwrap().unwrap();
            assert!(journal.is_selected());
            assert_eq!(journal.registration().identity().epoch, u64::from(epoch));
            assert_eq!(
                journal
                    .read_next()
                    .await
                    .unwrap()
                    .unwrap()
                    .records()
                    .collect::<Vec<_>>(),
                vec![&[epoch][..]]
            );
            assert!(journal.read_next().await.unwrap().is_none());
            assert_eq!(journal.finished_prefix().unwrap().position(), 1);
        }
        assert!(recovery.next().await.unwrap().is_none());
        let mut replay = root.replay().unwrap();
        for epoch in 1..=count {
            let batch = replay.read_next().await.unwrap().unwrap();
            assert_eq!(batch.identity().epoch, u64::from(epoch));
            assert_eq!(batch.records().collect::<Vec<_>>(), vec![&[epoch][..]]);
        }
        assert!(replay.read_next().await.unwrap().is_none());
        assert_eq!(files.frame_reads.load(Ordering::SeqCst), reads);
        // Cached replay is still not a serving capability after revocation.
        provider.revoke_current().await;
        assert!(root.permission().await.unwrap().is_none());
        assert!(cache.memory_bytes().await <= 64 * 1024);
    }
}

#[tokio::test]
async fn lost_selection_reply_and_complete_reopen_never_extend_a_selected_prefix() {
    let provider = Provider::new();

    let (old, late) = source(&provider).await;
    let mut root = successor(&provider).await;
    let journal = scan(&mut root).await;
    assert!(!journal.is_selected());
    assert_eq!(journal.finished_prefix().unwrap().position(), 1);
    let request = root.select_prefix(journal).unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Uncertain(_)
    ));
    assert!(matches!(root.recovery(), Err(Error::NotReady)));
    // Late bytes do not authorize a new choice after a lost catalog reply.
    let _completion = late.send().await;
    validate(&mut root).await;
    let journal = scan(&mut root).await;
    assert!(journal.is_selected());
    assert_eq!(journal.finished_prefix().unwrap().position(), 1);
    assert!(root.select_prefix(journal).is_err());
    drop(root);
    drop(old);
    let current = provider.history.lock().unwrap().last().unwrap().clone();
    *provider.history.lock().unwrap() = vec![current];

    let mut root = successor(&provider).await;
    // The immediate predecessor's empty journal is selected without replacing
    // the older decision inherited by the conditional claim.
    let journal = scan(&mut root).await;
    assert_eq!(journal.registration().identity().epoch, 2);
    assert_eq!(journal.finished_prefix().unwrap().position(), 0);
    seal(&mut root, journal).await;
    let mut recovery = root.recovery().unwrap();
    let mut empty = recovery.next().await.unwrap().unwrap();
    assert!(empty.is_selected());
    assert!(empty.read_next().await.unwrap().is_none());
    let mut prior = recovery.next().await.unwrap().unwrap();
    assert!(prior.is_selected());
    let batch = prior.read_next().await.unwrap().unwrap();
    assert_eq!(
        batch.records().collect::<Vec<_>>(),
        vec![b"acknowledged".as_slice()]
    );
    drop(batch);
    assert!(prior.read_next().await.unwrap().is_none());
    assert_eq!(prior.finished_prefix().unwrap().position(), 1);
    assert!(recovery.next().await.unwrap().is_none());
}

#[tokio::test]
async fn selection_revalidates_the_same_owner_and_refuses_a_successor() {
    for takeover in [false, true] {
        let provider = Provider::new();
        let (_first, first_late) = source(&provider).await;
        let (_old, late) = source(&provider).await;
        let mut root = successor(&provider).await;
        let identity = root.identity().unwrap().unwrap();
        provider.lose_reply.store(true, Ordering::SeqCst);
        provider.pause_read.store(true, Ordering::SeqCst);
        let selecting = tokio::spawn(async move {
            let result = root.select_journals().await;
            (root, result)
        });
        tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
            .await
            .expect("uncertain selection must revalidate without a new claim");
        assert_eq!(provider.record().identity, identity);
        assert!(provider.record().catalog.is_some());
        let _completion = late.send().await;
        let _completion = first_late.send().await;
        if takeover {
            let _next = successor(&provider).await;
        }
        let calls = provider.calls.load(Ordering::SeqCst);
        provider.release.notify_one();
        let (mut root, result) = selecting.await.unwrap();
        if takeover {
            assert!(matches!(result, Err(Error::NotOwner)));
            assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
            assert!(root.permission().await.unwrap().is_none());
        } else {
            result.unwrap();
            assert_eq!(provider.record().identity, identity);
            let mut recovery = root.recovery().unwrap();
            for _ in 0..2 {
                let mut journal = recovery.next().await.unwrap().unwrap();
                assert!(journal.is_selected());
                while journal.read_next().await.unwrap().is_some() {}
                assert_eq!(journal.finished_prefix().unwrap().position(), 1);
            }
            assert!(recovery.next().await.unwrap().is_none());
            assert_eq!(provider.calls.load(Ordering::SeqCst), calls + 1);
        }
    }
}

#[tokio::test]
async fn selection_retry_reuses_completed_scan_without_adopting_late_frames() {
    // Definite comparison conflict, unknown comparison, and either outcome of
    // a lost catalog PUT reply all retain the same completed discovery.
    for mode in 0..4 {
        let provider = Provider::new();
        let (_old, late) = source(&provider).await;
        let mut root = successor(&provider).await;
        let identity = root.identity().unwrap().unwrap();
        let files = registration::Faults::fail_at(
            provider.files.clone(),
            "catalogs/",
            if mode >= 2 { mode - 1 } else { 0 },
        );
        root.files = files.clone();
        let calls = provider.calls.load(Ordering::SeqCst);
        if mode == 0 {
            provider.pause.store(true, Ordering::SeqCst);
        } else {
            provider.pause_read.store(true, Ordering::SeqCst);
            if mode == 1 {
                provider.fail_replacements.store(1, Ordering::SeqCst);
            }
        }
        let selecting = tokio::spawn(async move {
            root.select_journals().await.unwrap();
            root
        });
        tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
            .await
            .unwrap();
        assert_eq!(files.frame_reads.load(Ordering::SeqCst), 2);
        if mode == 0 {
            let before = provider.read().await.unwrap();
            let mut info = before.info.clone();
            info.insert("administrative-fixture".into(), "changed".into());
            assert!(provider.replace(&before, info).await.unwrap().is_some());
        }
        let _completion = late.send().await;
        provider.release.notify_one();
        let mut root = selecting.await.unwrap();
        // Conflict/unknown CAS: two selection comparisons, no validation CAS.
        // Failed catalog PUT: only the retry reaches the register. Mode 0 also
        // includes the fixture's unrelated administrative update.
        assert_eq!(
            provider.calls.load(Ordering::SeqCst) - calls,
            match mode {
                0 => 3,
                1 => 2,
                _ => 1,
            }
        );
        assert_eq!(files.frame_reads.load(Ordering::SeqCst), 2);
        assert_eq!(root.identity().unwrap(), Some(identity));
        // Each retry stages fresh objects; an old delayed PUT is never replayed
        // and cannot replace the second catalog's already selected cut.
        assert_eq!(files.puts.load(Ordering::SeqCst), 6);
        files.release().await;
        assert_eq!(
            provider
                .files
                .list("catalogs/", None, 8)
                .await
                .unwrap()
                .len(),
            2
        );
        let journal = scan(&mut root).await;
        assert!(journal.is_selected());
        assert_eq!(journal.finished_prefix().unwrap().position(), 1);
    }
}

#[tokio::test]
async fn observed_selection_retry_validates_only_at_publication_and_preserves_metadata() {
    for race in 0..3 {
        let provider = Provider::new();
        let (_old, _late) = source(&provider).await;
        let mut root = successor(&provider).await;
        let journal = scan(&mut root).await;
        root.abandon();
        let before = provider.read().await.unwrap();
        let mut info = before.info.clone();
        info.insert("administrative-fixture".into(), "preserved".into());
        let challenge = Uuid::new_v4();
        info.insert(
            "ddb-challenge".into(),
            format!("{}:{challenge}", root.incarnation),
        );
        provider.replace(&before, info).await.unwrap().unwrap();
        let observed = root.observe().await.unwrap();
        assert!(matches!(root.recovery(), Err(Error::NotReady)));
        let select = root.select_prefixes_observed(observed, &[journal]).unwrap();
        assert!(matches!(root.recovery(), Err(Error::NotReady)));
        match race {
            1 => {
                root.collection().collect().await.unwrap();
            }
            2 => {
                let _next = successor(&provider).await;
            }
            _ => {}
        }
        let before = provider.calls.load(Ordering::SeqCst);
        let result = root.finish(select.execute().await).unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), before + 1);
        if race == 0 {
            assert!(matches!(result, Outcome::Confirmed));
            assert_eq!(provider.record().challenge, Some(challenge));
            assert_eq!(
                provider.read().await.unwrap().info["administrative-fixture"],
                "preserved"
            );
            assert!(scan(&mut root).await.is_selected());
        } else {
            assert!(matches!(result, Outcome::Conflict));
            assert!(matches!(root.recovery(), Err(Error::NotReady)));
        }
    }
}

#[tokio::test]
async fn selection_retry_budget_bounds_fresh_uploads_and_never_confirms_failed_writes() {
    let provider = Provider::new();
    let (_old, _late) = source(&provider).await;
    let mut root = successor(&provider).await;
    let files = registration::Faults::fail_at(provider.files.clone(), "never/", 0);
    root.files = files.clone();
    provider.fail_replacements.store(10, Ordering::SeqCst);
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!(root.select_journals().await.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst) - calls, 5);
    assert_eq!(files.puts.load(Ordering::SeqCst), 15); // Initial plus four fresh retries.
    assert_eq!(files.frame_reads.load(Ordering::SeqCst), 2);
    assert!(matches!(root.recovery(), Err(Error::NotReady)));
    assert!(provider.record().catalog.is_none());
}

#[tokio::test]
async fn selection_retry_restarts_only_when_collection_snapshot_changes() {
    for data_sweep in [false, true] {
        let provider = Provider::new();
        let (_old, late) = source(&provider).await;
        let mut root = successor(&provider).await;
        let collection = root.collection();
        let files = registration::Faults::fail_at(provider.files.clone(), "never/", 0);
        root.files = files.clone();
        provider.pause.store(true, Ordering::SeqCst);
        let selecting = tokio::spawn(async move {
            root.select_journals().await.unwrap();
            root
        });
        tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
            .await
            .unwrap();
        assert_eq!(files.frame_reads.load(Ordering::SeqCst), 2);
        if data_sweep {
            crate::storage::immutable::Blob::upload(
                provider.files.clone(),
                Bytes::from_static(b"orphan"),
            )
            .await
            .unwrap();
        }
        let before = provider.record();
        assert!(collection.collect().await.unwrap() > 0);
        assert_eq!(provider.record().collection > before.collection, data_sweep);
        let _completion = late.send().await;
        provider.release.notify_one();
        let mut root = selecting.await.unwrap();
        // A data sweep invalidates the scan. A metadata-only sweep removes the
        // first attempt's objects, but retrying stages fresh ones from the same cut.
        // The longer rescan also confirms its speculative end-of-log probe.
        assert_eq!(
            files.frame_reads.load(Ordering::SeqCst),
            if data_sweep { 6 } else { 2 }
        );
        let journal = scan(&mut root).await;
        assert!(journal.is_selected());
        assert_eq!(
            journal.finished_prefix().unwrap().position(),
            if data_sweep { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn abandoned_selection_and_successor_claim_are_serialized_in_both_orders() {
    for selection_first in [true, false] {
        let provider = Provider::new();

        let (_old, late) = source(&provider).await;
        let mut root = successor(&provider).await;
        let journal = scan(&mut root).await;
        let selection = root.select_prefix(journal).unwrap();
        root.abandon();
        let mut next = provider.root();
        let observed = next.observe().await.unwrap();
        let claim_request = next.claim(observed).unwrap();
        if selection_first {
            assert!(matches!(
                root.finish(selection.execute().await).unwrap(),
                Outcome::Ignored
            ));
            assert!(matches!(
                next.finish(claim_request.execute().await).unwrap(),
                Outcome::Conflict
            ));
            // A losing candidate cannot retarget; a fresh one inherits the cut.
            next = provider.root();
            claim(&mut next).await;
            assert!(provider.record().catalog.is_some());
        } else {
            assert!(matches!(
                next.finish(claim_request.execute().await).unwrap(),
                Outcome::Confirmed
            ));
            assert!(matches!(
                root.finish(selection.execute().await).unwrap(),
                Outcome::Ignored
            ));
            assert!(provider.record().catalog.is_none());
        }
        let _completion = late.send().await;

        validate(&mut next).await;
        activate(&mut next).await;
        let mut recovery = next.recovery().unwrap();
        drop(recovery.next().await.unwrap().unwrap()); // Empty epoch 2.
        let mut prior = recovery.next().await.unwrap().unwrap();
        while prior.read_next().await.unwrap().is_some() {}
        assert_eq!(prior.is_selected(), selection_first);
        assert_eq!(
            prior.finished_prefix().unwrap().position(),
            if selection_first { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn stale_parallel_scans_cannot_replace_the_first_selected_prefix() {
    let provider = Provider::new();

    let (_old, late) = source(&provider).await;
    let mut root = successor(&provider).await;
    let earlier = scan(&mut root).await;
    let _completion = late.send().await;
    let later = scan(&mut root).await;
    assert_eq!(later.finished_prefix().unwrap().position(), 2);
    seal(&mut root, earlier).await;
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!(matches!(
        root.select_prefix(later),
        Err(Error::StaleRecovery)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    let recovered = scan(&mut root).await;
    assert!(recovered.is_selected());
    assert_eq!(recovered.finished_prefix().unwrap().position(), 1);
}

#[tokio::test]
async fn discovery_requires_activation_and_selection_requires_a_complete_bound_scan() {
    let provider = Provider::new();

    let (_old, _late) = source(&provider).await;
    let mut root = provider.root();
    assert!(matches!(root.recovery(), Err(Error::NotReady)));
    claim(&mut root).await;
    assert!(matches!(root.recovery(), Err(Error::NotReady)));

    validate(&mut root).await;
    assert!(matches!(root.recovery(), Err(Error::NotReady)));
    activate(&mut root).await;
    let mut recovery = root.recovery().unwrap();
    let incomplete = recovery.next().await.unwrap().unwrap();
    assert!(root.select_prefix(incomplete).is_err());
    let complete = scan(&mut root).await;
    let mut next = successor(&provider).await;
    assert!(matches!(next.select_prefix(complete), Err(Error::NotOwner)));
    let complete = scan(&mut next).await;

    let mut third = provider.root();
    claim(&mut third).await;
    let late = next.select_prefix(complete).unwrap();
    assert!(matches!(
        next.finish(late.execute().await).unwrap(),
        Outcome::Conflict
    ));
}

#[tokio::test]
async fn missing_or_corrupt_catalog_objects_never_become_unselected_history() {
    let provider = Provider::new();

    let (_old, _late) = source(&provider).await;
    let mut root = successor(&provider).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    for namespace in ["catalogs/", "indexes/", "manifests/"] {
        let keys = provider.files.list(namespace, None, 100).await.unwrap();
        assert_eq!(keys.len(), 1);
        let key = &keys[0];
        let original = provider.files.get(key, 65536).await.unwrap().unwrap().bytes;
        for corrupt in [false, true] {
            if corrupt {
                let mut bytes = original.to_vec();
                bytes[0] ^= 1;
                provider.files.put(key, bytes.into()).await.unwrap();
            } else {
                provider.files.delete(key).await.unwrap();
            }
            let mut recovery = root.recovery().unwrap();
            assert!(recovery.next().await.is_err());
            provider.files.put(key, original.clone()).await.unwrap();
            // Failure did not advance the cursor or downgrade to discovery.
            let mut journal = recovery.next().await.unwrap().unwrap();
            assert!(journal.is_selected());
            while journal.read_next().await.unwrap().is_some() {}
            assert_eq!(journal.finished_prefix().unwrap().position(), 1);
        }
        if namespace == "catalogs/" {
            use sha2::{Digest, Sha256};
            for field in ["format", "id", "index"] {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&original[..original.len() - 32]).unwrap();
                match field {
                    "format" => value["format"] = serde_json::json!(2),
                    "id" => value["id"] = serde_json::json!(Uuid::nil().to_string()),
                    _ => value["index"]["height"] = serde_json::json!(0),
                }
                let mut body = serde_json::to_vec(&value).unwrap();
                let checksum = Sha256::digest(&body);
                body.extend_from_slice(&checksum);
                provider.files.put(key, body.into()).await.unwrap();
                assert!(root.recovery().unwrap().next().await.is_err());
            }
            provider
                .files
                .put(key, Bytes::from(vec![0; 4097]))
                .await
                .unwrap();
            assert!(root.recovery().unwrap().next().await.is_err());
            provider.files.put(key, original).await.unwrap();
        }
    }
}

#[tokio::test]
async fn failed_catalog_uploads_never_send_the_conditional_publication() {
    for namespace in ["manifests/", "indexes/", "catalogs/"] {
        for mode in [1, 2] {
            let provider = Provider::new();

            let (_older, _older_late) = source(&provider).await;
            let (_old, _late) = source(&provider).await;
            let files =
                super::registration::Faults::fail_at(provider.files.clone(), namespace, mode);
            let connection = Connection::new(files.clone(), provider.clone(), "test");
            let mut root = connection.root().unwrap();
            claim(&mut root).await;

            validate(&mut root).await;
            activate(&mut root).await;
            let mut recovery = root.recovery().unwrap();
            let mut journals = Vec::new();
            while let Some(mut journal) = recovery.next().await.unwrap() {
                while journal.read_next().await.unwrap().is_some() {}
                journals.push(journal);
            }
            assert_eq!(journals.len(), 2);
            let calls = provider.calls.load(Ordering::SeqCst);
            let request = root.select_prefixes(&journals).unwrap();
            assert!(matches!(
                root.finish(request.execute().await).unwrap(),
                Outcome::Uncertain(_)
            ));
            assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
            assert!(provider.record().catalog.is_none());
            // Neither a lost successful PUT nor a later upload is a publication.
            files.release().await;
            validate(&mut root).await;
            let journal = scan(&mut root).await;
            assert!(!journal.is_selected());
            seal(&mut root, journal).await;
            assert!(scan(&mut root).await.is_selected());
        }
    }
}

#[tokio::test]
async fn catalog_splits_index_pages_and_recovers_one_hundred_thirty_decisions() {
    let provider = Provider::new();

    let mut root = active(&provider).await;
    for _ in 0..130 {
        drop(root);
        root = successor(&provider).await;
    }
    root.select_journals().await.unwrap();
    let mut recovery = root.recovery().unwrap();
    let mut remaining = 130;
    while let Some(mut journal) = recovery.next().await.unwrap() {
        assert!(journal.is_selected());
        assert_eq!(journal.registration().identity().epoch, remaining);
        assert!(journal.read_next().await.unwrap().is_none());
        remaining -= 1;
    }
    assert_eq!(remaining, 0);
    let id = provider.record().catalog.unwrap();
    let key = format!("catalogs/{}", id.simple());
    let bytes = provider.files.get(&key, 4096).await.unwrap().unwrap().bytes;
    let header: serde_json::Value = serde_json::from_slice(&bytes[..bytes.len() - 32]).unwrap();
    assert_eq!(header["index"]["count"], 130);
    assert_eq!(header["index"]["height"], 2);
}

#[tokio::test]
async fn renewals_during_a_long_scan_do_not_require_restarting_unchanged_history() {
    let provider = Provider::new();

    let (_old, _late) = source(&provider).await;
    let mut root = successor(&provider).await;
    let mut recovery = root.recovery().unwrap();
    let mut journal = recovery.next().await.unwrap().unwrap();
    for _ in 0..3 {
        validate(&mut root).await;
    }
    while journal.read_next().await.unwrap().is_some() {}
    seal(&mut root, journal).await;
    let recovered = scan(&mut root).await;
    assert!(recovered.is_selected());
    assert_eq!(recovered.finished_prefix().unwrap().position(), 1);
}

#[tokio::test]
async fn batch_selection_rejects_mixed_snapshots_duplicates_and_unbounded_input() {
    let provider = Provider::new();
    let (_first, _) = source(&provider).await;
    let (_second, _) = source(&provider).await;
    let mut root = successor(&provider).await;
    let before = provider.calls.load(Ordering::SeqCst);
    assert!(root.select_prefixes(&[]).is_err());
    let mut too_many = Vec::new();
    for _ in 0..9 {
        too_many.push(scan(&mut root).await);
    }
    assert!(root.select_prefixes(&too_many).is_err());
    let a = scan(&mut root).await;
    let b = scan(&mut root).await;
    assert!(root.select_prefixes(&[a, b]).is_err());
    for incomplete_first in [false, true] {
        let mut recovery = root.recovery().unwrap();
        let mut a = recovery.next().await.unwrap().unwrap();
        let mut b = recovery.next().await.unwrap().unwrap();
        let complete = if incomplete_first { &mut b } else { &mut a };
        while complete.read_next().await.unwrap().is_some() {}
        assert!(root.select_prefixes(&[a, b]).is_err());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    let mut old = root.recovery().unwrap();
    let mut a = old.next().await.unwrap().unwrap();
    let mut b = old.next().await.unwrap().unwrap();
    while a.read_next().await.unwrap().is_some() {}
    while b.read_next().await.unwrap().is_some() {}
    seal(&mut root, a).await;
    let mut current = root.recovery().unwrap();
    assert!(current.next().await.unwrap().unwrap().is_selected());
    let mut fresh = current.next().await.unwrap().unwrap();
    while fresh.read_next().await.unwrap().is_some() {}
    let before = provider.calls.load(Ordering::SeqCst);
    // The first member matches the current snapshot; the second does not.
    assert!(matches!(
        root.select_prefixes(&[fresh, b]),
        Err(Error::StaleRecovery)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    root.select_journals().await.unwrap();
}
