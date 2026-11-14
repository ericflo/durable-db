use super::*;
use crate::{
    storage::Storage,
    upload_budget::{UploadLimits, UploadRequest},
};
use bytes::Bytes;

fn limits(attempts: u64) -> UploadLimits {
    UploadLimits {
        bytes: 4096,
        attempts,
    }
}

#[tokio::test]
async fn permit_format_refuses_timed_cloud_namespaces() {
    let local = Provider::new();
    claim(&mut local.root()).await;
    assert_eq!(
        local.read().await.unwrap().info["durable-db-format"],
        "owned-candidate-23-unbudgeted",
        "remote roots use a fresh format"
    );
    local.root().observe().await.unwrap();

    let cloud = Provider::new();
    let connection = cloud
        .connection("test")
        .with_upload_budget(limits(2))
        .await
        .unwrap();
    assert_eq!(
        cloud.read().await.unwrap().info["durable-db-format"],
        "owned-candidate-23"
    );
    let root = connection.root().unwrap();
    root.observe().await.unwrap();
    cloud
        .history
        .lock()
        .unwrap()
        .last_mut()
        .unwrap()
        .info
        .insert("durable-db-format".into(), "owned-candidate-22".into());
    let before = cloud.calls.load(Ordering::SeqCst);
    assert!(matches!(root.observe().await, Err(Error::Incompatible)));
    assert!(
        cloud
            .connection("test")
            .with_upload_budget(limits(2))
            .await
            .is_err()
    );
    assert_eq!(
        cloud.calls.load(Ordering::SeqCst),
        before,
        "refuse before mutation"
    );
}

pub(super) async fn attempt(connection: &Connection) -> (Root, Outcome) {
    let mut root = connection.root().unwrap();
    let observed = root.observe().await.unwrap();
    let request = root.claim(observed).unwrap();
    let outcome = root.finish(request.execute().await).unwrap();
    (root, outcome)
}

#[tokio::test]
async fn native_budget_bootstrap_loss_reopen_and_format_refusal() {
    let provider = Provider::new();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        provider
            .connection("test")
            .with_upload_budget(limits(2))
            .await
            .is_err()
    );
    assert!(
        provider
            .files
            .list("tables/", None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let connection = provider
        .connection("test")
        .with_upload_budget(limits(99))
        .await
        .unwrap();
    let status = connection.objects().upload_budget().await.unwrap().unwrap();
    assert_eq!(
        status.limits,
        limits(2),
        "retry must not change the installed ceiling"
    );
    assert_eq!(status.allocated, UploadLimits::default());
    assert!(
        provider
            .connection("other")
            .with_upload_budget(limits(2))
            .await
            .is_err()
    );
    // A connection that omitted enforcement cannot open the budgeted format.
    assert!(provider.root().observe().await.is_err());
    let before = provider.calls.load(Ordering::SeqCst);
    let unbudgeted = Provider::new();
    claim(&mut unbudgeted.root()).await;
    assert!(
        unbudgeted
            .connection("test")
            .with_upload_budget(limits(2))
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);

    let full = Provider::new();
    full.history.lock().unwrap()[0].info = (0..5)
        .map(|i| (format!("custom-{i}"), "keep".into()))
        .collect();
    assert!(
        full.connection("test")
            .with_upload_budget(limits(2))
            .await
            .is_err()
    );
    assert_eq!(
        full.calls.load(Ordering::SeqCst),
        0,
        "leave room for the root and full receipt window"
    );
}

#[tokio::test]
async fn native_budget_charges_abandoned_bootstrap_and_survives_replacement() {
    let provider = Provider::new();

    let connection = provider
        .connection("test")
        .with_upload_budget(limits(4))
        .await
        .unwrap();
    let (_, outcome) = attempt(&connection).await;
    assert!(
        matches!(outcome, Outcome::Conflict),
        "credit allocation invalidates the earlier comparison"
    );
    let (mut root, outcome) = attempt(&connection).await;
    assert!(matches!(outcome, Outcome::Confirmed));
    assert_eq!(
        provider
            .files
            .list("ownership/", None, 10)
            .await
            .unwrap()
            .len(),
        2
    );

    validate(&mut root).await;
    activate(&mut root).await;
    let mut writer = root.take_journal(QueueLimits::default()).unwrap();
    let upload = writer.prepare(&[Bytes::from_static(b"refused")]).unwrap();
    writer.complete(upload.send().await).unwrap();
    assert_eq!(writer.durable_prefix().position(), 0);
    assert!(
        provider
            .files
            .list("journal/", None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let status = connection.objects().upload_budget().await.unwrap().unwrap();
    assert_eq!(status.allocated, limits(4));
    drop((root, writer, connection));
    let reopened = provider
        .connection("test")
        .with_upload_budget(limits(200))
        .await
        .unwrap();
    assert_eq!(
        reopened.objects().upload_budget().await.unwrap().unwrap(),
        status
    );
    let (_, outcome) = attempt(&reopened).await;
    assert!(matches!(outcome, Outcome::Uncertain(_)));
    assert_eq!(
        provider
            .files
            .list("ownership/", None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
    let request = UploadRequest {
        request_id: "fund".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 8192,
            attempts: 8,
        },
    };
    let funded = reopened
        .objects()
        .replenish_upload_budget(&request)
        .await
        .unwrap();
    assert_eq!(funded.status.allocated, status.allocated);
    assert!(
        reopened
            .objects()
            .replenish_upload_budget(&request)
            .await
            .unwrap()
            .replayed
    );
    let (_, outcome) = attempt(&reopened).await;
    assert!(matches!(outcome, Outcome::Conflict));
    let (_, outcome) = attempt(&reopened).await;
    assert!(matches!(outcome, Outcome::Confirmed));
}

#[tokio::test]
async fn native_budget_unknown_grant_and_late_upload_never_refund_credit() {
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(limits(2))
        .await
        .unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    // Prefunding can apply remotely while its reply is lost. It must not
    // create any local upload rights, objects, or permission to claim.
    assert!(connection.fund_claim().await.is_err());
    assert!(!provider.lose_reply.load(Ordering::SeqCst));
    assert!(
        connection
            .objects()
            .put("tables/retry", Bytes::new())
            .await
            .is_err()
    );
    assert!(
        provider
            .files
            .list("tables/", None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        connection
            .objects()
            .upload_budget()
            .await
            .unwrap()
            .unwrap()
            .allocated
            .attempts,
        2
    );

    let provider = Provider::new();
    let delayed = registration::Faults::fail_at(provider.files.clone(), "tables/", 2);
    let connection = Connection::new(delayed.clone(), provider.clone(), "test")
        .with_upload_budget(limits(1))
        .await
        .unwrap();
    assert!(
        connection
            .objects()
            .put("tables/late", Bytes::from_static(b"late"))
            .await
            .is_err()
    );
    drop(connection);
    let replacement = provider
        .connection("test")
        .with_upload_budget(limits(1))
        .await
        .unwrap();
    assert!(
        replacement
            .objects()
            .put("tables/replacement", Bytes::new())
            .await
            .is_err()
    );
    delayed.release().await;
    assert!(
        provider
            .files
            .get("tables/late", 4)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        provider.files.list("tables/", None, 10).await.unwrap(),
        ["tables/late"]
    );
}

#[tokio::test]
async fn database_budget_isolation_lost_reply_reopen_and_directory_split() {
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 10_000,
        })
        .await
        .unwrap();
    let root = connection.root().unwrap().with_cache(Arc::new(
        crate::cache::Cache::new(1024 * 1024, None).unwrap(),
    ));
    let a = root.database_budget("a").unwrap().unwrap();
    let b = root.database_budget("b").unwrap().unwrap();
    for ledger in [&a, &b] {
        ledger
            .replenish(&UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 200,
                    attempts: 1,
                },
            })
            .await
            .unwrap();
    }
    // Race two allocations from the same directory root. The loser must retain
    // the winner's ledger when rebuilding its path, not resurrect old counters.
    provider.pause.store(true, Ordering::SeqCst);
    let waiting = {
        let a = a.clone();
        tokio::spawn(async move { a.charge(60).await })
    };
    provider.entered.notified().await;
    b.charge(80).await.unwrap();
    provider.release.notify_one();
    waiting.await.unwrap().unwrap();
    for ledger in [&a, &b] {
        assert!(
            ledger
                .charge(1)
                .await
                .unwrap_err()
                .downcast_ref::<crate::upload_budget::Exhausted>()
                .unwrap()
                .database
        );
        assert_eq!(ledger.status().await.unwrap().allocated.attempts, 1);
    }
    // Keep both byte and attempt dimensions; discarded rights never return.
    let c = root.database_budget("c").unwrap().unwrap();
    c.replenish(&UploadRequest {
        request_id: "fund".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 200,
            attempts: 10,
        },
    })
    .await
    .unwrap();
    c.charge(100).await.unwrap();
    assert!(c.charge(1).await.is_err());
    let d = root.database_budget("d").unwrap().unwrap();
    d.replenish(&UploadRequest {
        request_id: "fund".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 200,
            attempts: 1,
        },
    })
    .await
    .unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(d.charge(50).await.is_err());
    assert!(
        d.charge(50).await.is_err(),
        "a lost allocation response creates no local rights"
    );
    let reopened = connection
        .root()
        .unwrap()
        .database_budget("d")
        .unwrap()
        .unwrap();
    reopened.initialize_database().await.unwrap();
    assert_eq!(
        reopened.status().await.unwrap().limits.attempts,
        1,
        "creation cannot reset preprovisioned limits"
    );
    assert!(reopened.charge(1).await.is_err());
    // More entries than one directory page and far more than bucket fields.
    for i in 0..130 {
        root.database_budget(&format!("tenant-{i:03}"))
            .unwrap()
            .unwrap()
            .initialize_database()
            .await
            .unwrap();
    }
    for name in ["a", "b", "c", "d"] {
        let reopened = root.database_budget(name).unwrap().unwrap();
        assert!(reopened.charge(1).await.is_err());
    }
    let before = provider.read().await.unwrap();
    let directory: crate::directory::Root =
        serde_json::from_str(&before.info[crate::upload_budget::DATABASE_FIELD]).unwrap();
    assert_eq!(directory.count, 134);
    assert!(directory.height > 1);
    metadata::budget(&before.info).unwrap();
    assert!(before.info.len() <= 10);
    assert!(
        root.observe().await.is_ok(),
        "owner metadata accepts and preserves the budget directory"
    );
    // Every sweep is bounded; a replacement resumes the durable cursors. More
    // than one page of LIVE manifests must not hide obsolete objects forever.
    for _ in 0..8 {
        root.collection().collect().await.unwrap();
    }
    assert_eq!(
        provider
            .files
            .list("manifests/", None, 1000)
            .await
            .unwrap()
            .len(),
        134
    );
    for name in ["a", "b", "c", "d"] {
        assert!(
            root.database_budget(name)
                .unwrap()
                .unwrap()
                .charge(1)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn database_budget_mixed_batch_and_retries_need_no_extra_credit() {
    use crate::{
        auth::Access,
        engine::{CommitRequest, CreateRequest},
        transactions::live::Limits,
    };
    let provider = Provider::new();

    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 10_000,
        })
        .await
        .unwrap();
    let _ = attempt(&connection).await;
    let (mut root, outcome) = attempt(&connection).await;
    assert!(matches!(outcome, Outcome::Confirmed));

    validate(&mut root).await;
    activate(&mut root).await;
    let mut owner = live::open(root, Limits::default()).await;
    let admin = Access::Administrator;
    let database = crate::transactions::record::Database {
        name: "a",
        generation: Uuid::from_u128(1),
    };
    let mutations = [crate::transactions::record::Mutation {
        key: "x",
        value: Some(b"x"),
    }];
    let journal = 120
        + crate::transactions::record::Commit::encoded_len(database, "first", &mutations).unwrap();
    let rows =
        crate::index::recovery::checkpoint::contribution_bytes(database, "first", &mutations);
    // Creation: 753 journal bytes + 8,543 bytes of catalog row/manifest allowance.
    let creation_admission = 18_592;
    let admission = creation_admission + 2 * (journal + rows) as u64;
    for name in ["a", "b"] {
        owner
            .database_budget(name)
            .unwrap()
            .unwrap()
            .replenish(&UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: if name == "a" {
                        admission - 1
                    } else {
                        admission
                    },
                    attempts: 3,
                },
            })
            .await
            .unwrap();
        let request = CreateRequest {
            request_id: "create".into(),
            limits: crate::engine::Limits {
                max_keys: 10,
                max_bytes: 1000,
            },
        };
        let mut creation = owner
            .prepare_creation(name, &request, &admin)
            .await
            .unwrap();
        owner
            .complete(creation.upload.take().unwrap().send().await)
            .await
            .unwrap();
    }
    let spent_after_creation = owner
        .database_budget("a")
        .unwrap()
        .unwrap()
        .status()
        .await
        .unwrap();
    let first: CommitRequest = serde_json::from_value(serde_json::json!({"request_id":"first","expected_revision":0,"writes":[{"key":"x","value":"eA=="}]})).unwrap();
    // Journal alone fits, but its checkpoint contribution does not. Refusal
    // must precede both durable credit allocation and shared journal ordering.
    let mut files_before = Vec::new();
    for prefix in ["manifests/", "indexes/", "journal/", "tables/"] {
        files_before.push((
            prefix,
            provider.files.list(prefix, None, 1000).await.unwrap(),
        ));
    }
    let calls = provider.calls.load(Ordering::SeqCst);
    let refused = owner
        .prepare_authorized(&[("a", &first, &admin)])
        .await
        .unwrap();
    assert!(refused.upload.is_none());
    assert!(matches!(
        owner.authorized_result(&refused.tickets[0], &admin).await,
        Err(crate::Error::Limit(_))
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    for (prefix, before) in files_before {
        assert_eq!(
            provider.files.list(prefix, None, 1000).await.unwrap(),
            before
        );
    }
    let budget = owner.database_budget("a").unwrap().unwrap();
    assert_eq!(budget.status().await.unwrap(), spent_after_creation);
    budget
        .replenish(&UploadRequest {
            request_id: "row-credit".into(),
            expected_configuration_revision: 1,
            limits: UploadLimits {
                bytes: admission,
                attempts: 3,
            },
        })
        .await
        .unwrap();
    let group = owner
        .prepare_authorized(&[("a", &first, &admin), ("a", &first, &admin)])
        .await
        .unwrap();
    let pending = owner
        .prepare_authorized(&[("a", &first, &admin)])
        .await
        .unwrap();
    assert!(pending.upload.is_none());
    owner
        .complete(group.upload.unwrap().send().await)
        .await
        .unwrap();
    for (ticket, replayed) in [
        (&group.tickets[0], false),
        (&group.tickets[1], true),
        (&pending.tickets[0], true),
    ] {
        let result = owner
            .authorized_result(ticket, &admin)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.revision, 1);
        assert_eq!(result.replayed, replayed);
    }
    let retry = owner
        .prepare_authorized(&[("a", &first, &admin)])
        .await
        .unwrap();
    assert!(retry.upload.is_none());
    let mut second = first.clone();
    second.request_id = "second".into();
    second.expected_revision = 1;
    let group = owner
        .prepare_authorized(&[
            ("a", &second, &admin),
            ("a", &second, &admin),
            ("b", &first, &admin),
        ])
        .await
        .unwrap();
    for ticket in &group.tickets[..2] {
        assert!(
            matches!(
                owner.authorized_result(ticket, &admin).await,
                Err(crate::Error::Limit(_))
            ),
            "refusal needs no journal durability fence"
        );
    }
    owner
        .complete(group.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner
            .authorized_result(&group.tickets[2], &admin)
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
async fn collection_preserves_recovery_catalogs_across_takeovers_and_name_collisions() {
    let provider = Provider::new();

    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 256,
        })
        .await
        .unwrap();
    assert!(matches!(attempt(&connection).await.1, Outcome::Conflict));
    let mut collision = None;
    for epoch in 1..=3 {
        let (mut root, outcome) = attempt(&connection).await;
        assert!(matches!(outcome, Outcome::Confirmed));

        validate(&mut root).await;
        activate(&mut root).await;
        if epoch > 1 {
            let journal = super::catalog::scan(&mut root).await;
            let identity = journal.registration().identity();
            collision.get_or_insert_with(|| {
                format!("{:016x}-{}", identity.epoch, identity.incarnation.simple())
            });
            if epoch == 2 {
                // A staged catalog listed before the fence must not be adopted
                // afterward. A failed publication must rebuild from its origin.
                let request = root.select_prefix(journal).unwrap();
                provider.pause.store(true, Ordering::SeqCst);
                let task = tokio::spawn(request.execute());
                provider.entered.notified().await;
                assert!(root.collection().collect().await.unwrap() > 0);
                provider.release.notify_one();
                assert!(matches!(
                    root.finish(task.await.unwrap()).unwrap(),
                    Outcome::Conflict
                ));
                validate(&mut root).await;
                let journal = super::catalog::scan(&mut root).await;
                super::catalog::seal(&mut root, journal).await;
            } else {
                super::catalog::seal(&mut root, journal).await;
            }
        }
        // The first sweep after takeover has a catalog and no tenant ledger.
        // Later sweeps must protect both trees even when their key names match.
        root.collection().collect().await.unwrap();
        let mut replay = root.replay().unwrap();
        for expected in 1..epoch {
            let batch = replay.read_next().await.unwrap().unwrap();
            assert_eq!(
                batch.records().collect::<Vec<_>>(),
                vec![[expected as u8].as_slice()]
            );
        }
        assert!(replay.read_next().await.unwrap().is_none());
        if let Some(name) = &collision {
            let ledger = root.database_budget(name).unwrap().unwrap();
            ledger.initialize_database().await.unwrap();
            ledger.charge(1).await.unwrap();
            let spent = ledger.status().await.unwrap();
            root.collection().collect().await.unwrap();
            assert_eq!(ledger.status().await.unwrap(), spent);
            let mut replay = root.replay().unwrap();
            assert!(replay.read_next().await.unwrap().is_some());
        }
        let mut writer = root.take_journal(QueueLimits::default()).unwrap();
        let upload = writer.prepare(&[Bytes::from(vec![epoch as u8])]).unwrap();
        writer.complete(upload.send().await).unwrap();
        writer.pop_durable().unwrap().unwrap();
    }
    let collector = connection.root().unwrap().collection();
    // A missing/corrupt catalog is an error, not an empty protected tree.
    let header = format!("catalogs/{}", provider.record().catalog.unwrap().simple());
    let orphan = format!("manifests/orphan.{}", Uuid::new_v4());
    provider.files.put(&orphan, Bytes::new()).await.unwrap();
    provider
        .files
        .put(&header, Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    assert!(collector.collect().await.is_err());
    assert!(provider.files.get(&orphan, 1).await.unwrap().is_some());
    provider.files.delete(&header).await.unwrap();
    assert!(collector.collect().await.is_err());
    assert!(provider.files.get(&orphan, 1).await.unwrap().is_some());
}

#[tokio::test]
async fn database_budget_cache_reuses_bytes_not_credit_or_authority() {
    use crate::cache::Cache;
    for bytes in [0, 128, 1024 * 1024] {
        let provider = Provider::new();
        let faults = Arc::new(http::Faults::default());
        let connection = Connection::new(
            Arc::new(http::Files {
                inner: provider.files.clone(),
                faults: faults.clone(),
            }),
            provider.clone(),
            "test",
        )
        .with_upload_budget(UploadLimits {
            bytes: 32 * 1024 * 1024,
            attempts: 128,
        })
        .await
        .unwrap();
        let cache = Arc::new(Cache::new(bytes, None).unwrap());
        let root = connection.root().unwrap().with_cache(cache.clone());
        root.database_budget("app")
            .unwrap()
            .unwrap()
            .initialize_database()
            .await
            .unwrap();
        // Fresh allocators share immutable bytes, never the last owner's rights.
        for grant in 1..=2 {
            faults.metadata_reads.store(0, Ordering::SeqCst);
            let calls = provider.calls.load(Ordering::SeqCst);
            let ledger = root.database_budget("app").unwrap().unwrap();
            ledger.charge(100).await.unwrap();
            assert_eq!(
                faults.metadata_reads.load(Ordering::SeqCst) == 0,
                bytes > 128,
                "warm funding should reuse immutable metadata within the existing cache budget"
            );
            assert!(
                provider.calls.load(Ordering::SeqCst) > calls,
                "cached bytes grant no credit"
            );
            assert!(cache.memory_bytes().await <= bytes);
            // An uncached observer must see every independently allocated grant.
            assert_eq!(
                connection
                    .root()
                    .unwrap()
                    .database_budget("app")
                    .unwrap()
                    .unwrap()
                    .status()
                    .await
                    .unwrap()
                    .allocated
                    .attempts,
                grant * 32
            );
        }
        // A new process starts cold: fetch the page and ledger once, then reuse
        // that page while staging the grant instead of downloading it again.
        faults.metadata_reads.store(0, Ordering::SeqCst);
        let cold = connection
            .root()
            .unwrap()
            .with_cache(Arc::new(Cache::new(bytes, None).unwrap()));
        cold.database_budget("app")
            .unwrap()
            .unwrap()
            .charge(100)
            .await
            .unwrap();
        assert_eq!(
            faults.metadata_reads.load(Ordering::SeqCst),
            if bytes > 128 { 2 } else { 3 }
        );
        // Wrong cached bytes are misses, not a substitute for the digest from
        // the newly observed authority. Tiny/disabled caches remain functional.
        for prefix in ["indexes/", "manifests/"] {
            for key in provider.files.list(prefix, None, 128).await.unwrap() {
                cache.insert(&key, Bytes::from_static(b"corrupt")).await;
            }
        }
        faults.metadata_reads.store(0, Ordering::SeqCst);
        let ledger = root.database_budget("app").unwrap().unwrap();
        assert_eq!(ledger.status().await.unwrap().allocated.attempts, 96);
        assert!(faults.metadata_reads.load(Ordering::SeqCst) > 0);
        provider.fail_replacements.store(8, Ordering::SeqCst);
        assert!(
            ledger.status().await.is_err(),
            "warm metadata cannot replace a fresh read barrier"
        );
    }
}

#[tokio::test]
async fn database_budget_metadata_reuses_staging_and_bounds_stale_comparisons() {
    async fn objects(provider: &Provider) -> Vec<String> {
        let mut objects = provider.files.list("manifests/", None, 128).await.unwrap();
        objects.extend(provider.files.list("indexes/", None, 128).await.unwrap());
        objects
    }
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 32 * 1024 * 1024,
            attempts: 128,
        })
        .await
        .unwrap();
    let root = connection.root().unwrap();
    let ledger = root.database_budget("app").unwrap().unwrap();
    let refill = UploadRequest {
        request_id: "refill".into(),
        expected_configuration_revision: 0,
        limits: UploadLimits {
            bytes: 64 * 1024 * 1024 + 1,
            attempts: 16_384,
        },
    };
    for action in 0..3 {
        provider.pause_budget.store(true, Ordering::SeqCst);
        let task = {
            let ledger = ledger.clone();
            let refill = refill.clone();
            tokio::spawn(async move {
                match action {
                    0 => ledger.initialize_database().await,
                    1 => ledger
                        .replenish(&refill)
                        .await
                        .map(|_| ())
                        .map_err(Into::into),
                    _ => ledger.charge(100).await,
                }
            })
        };
        provider.entered.notified().await;
        let staged = objects(&provider).await;
        // A status/read barrier changes the nonce, not the tenant directory or
        // metadata collection fence. Initialization also crosses a physical grant.
        connection.authority().read_fresh().await.unwrap();
        provider.release.notify_one();
        task.await.unwrap().unwrap();
        assert_eq!(
            objects(&provider).await,
            staged,
            "control conflict reuploaded metadata"
        );
    }
    assert_eq!(objects(&provider).await.len(), 6);
    let status = ledger.status().await.unwrap();
    assert_eq!(status.configuration_revision, 1);
    assert_eq!(status.allocated.bytes, 16 * 1024 * 1024);
    assert_eq!(status.allocated.attempts, 32);

    let mut refill = refill;
    refill.request_id = "next".into();
    refill.expected_configuration_revision = 1;
    refill.limits.bytes += 1;
    for malformed in [false, true] {
        provider.pause_budget.store(true, Ordering::SeqCst);
        let task = {
            let ledger = ledger.clone();
            let refill = refill.clone();
            tokio::spawn(async move { ledger.replenish(&refill).await })
        };
        provider.entered.notified().await;
        let staged = objects(&provider).await;
        if malformed {
            let before = provider.read().await.unwrap();
            let mut info = before.info.clone();
            info.insert("durable-db-format".into(), "owned-candidate-22".into());
            provider.replace(&before, info).await.unwrap().unwrap();
        } else {
            let captured = provider.history.lock().unwrap().len() - 1;
            connection.authority().read_fresh().await.unwrap();
            *provider.stale.lock().unwrap() = Some(captured);
        }
        let calls = provider.calls.load(Ordering::SeqCst);
        provider.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert!(provider.calls.load(Ordering::SeqCst) - calls < 8);
        assert_eq!(
            objects(&provider).await,
            staged,
            "refusal rebuilt immutable metadata"
        );
        *provider.stale.lock().unwrap() = None;
        if !malformed {
            assert_eq!(ledger.status().await.unwrap(), status);
        }
    }
}

#[tokio::test]
async fn database_budget_collection_fences_paused_publishers_and_unknown_deletes() {
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 1024 * 1024,
            attempts: 32,
        })
        .await
        .unwrap();
    let root = connection.root().unwrap().with_cache(Arc::new(
        crate::cache::Cache::new(1024 * 1024, None).unwrap(),
    ));
    let ledger = root.database_budget("app").unwrap().unwrap();
    ledger.initialize_database().await.unwrap();
    let collector = root.collection();
    // Metadata is uploaded, but its captured directory comparison is delayed.
    provider.pause.store(true, Ordering::SeqCst);
    let task = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.charge(100).await })
    };
    provider.entered.notified().await;
    assert!(collector.collect().await.unwrap() > 0);
    provider.release.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(
        connection
            .root()
            .unwrap()
            .database_budget("app")
            .unwrap()
            .unwrap()
            .status()
            .await
            .unwrap()
            .allocated
            .attempts,
        32,
        "a fenced publisher rebuilt its immutable paths before granting rights"
    );
    let orphan = format!("manifests/orphan.{}", Uuid::new_v4());
    connection
        .objects()
        .put(&orphan, Bytes::new())
        .await
        .unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(collector.collect().await.is_err());
    assert!(
        provider.files.get(&orphan, 1).await.unwrap().is_some(),
        "uncertain fences grant no delete permission"
    );
    // Exhaust all remaining global attempts. The out-of-scope registration identity
    // survives collection; deleting metadata never restores either allowance.
    while connection
        .objects()
        .put("ownership/untouched", Bytes::new())
        .await
        .is_ok()
    {}
    let global = connection.objects().upload_budget().await.unwrap().unwrap();
    let tenant = ledger.status().await.unwrap();
    assert!(collector.collect().await.unwrap() > 0);
    assert!(provider.files.get(&orphan, 1).await.unwrap().is_none());
    assert!(
        provider
            .files
            .get("ownership/untouched", 1)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(ledger.status().await.unwrap(), tenant);
    assert_eq!(
        connection.objects().upload_budget().await.unwrap().unwrap(),
        global
    );
    assert!(
        connection
            .objects()
            .put("tables/refused", Bytes::new())
            .await
            .is_err()
    );
    assert_eq!(
        provider
            .files
            .list("manifests/", None, 128)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        provider
            .files
            .list("indexes/", None, 128)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn maintenance_reserve_survives_uncertainty_and_takeover_without_reissuing_spent_credit() {
    use crate::upload_budget::Allocator;
    let provider = Provider::new();

    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 10_000,
        })
        .await
        .unwrap();
    let _ = attempt(&connection).await;
    let (mut root, outcome) = attempt(&connection).await;
    assert!(matches!(outcome, Outcome::Confirmed));

    validate(&mut root).await;
    activate(&mut root).await;
    let origin = root.recovery_record().unwrap();
    let a = root.database_budget("a").unwrap().unwrap();
    let b = root.database_budget("b").unwrap().unwrap();
    for budget in [&a, &b] {
        budget
            .replenish(&UploadRequest {
                request_id: "initial".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 200,
                    attempts: 3,
                },
            })
            .await
            .unwrap();
        budget.charge(40).await.unwrap();
        let status = budget.status().await.unwrap();
        assert_eq!(status.allocated.bytes, 200);
        assert_eq!(status.maintenance.unwrap().funded_bytes, 100);
    }
    let check = |bucket: &Bucket| root.check_funding_origin(bucket, origin);
    Allocator::fund_maintenance(&[(a.clone(), 30), (b.clone(), 30)], check)
        .await
        .unwrap();
    // A competing refill changes the directory while the complete group CAS
    // is paused. Rebuilding must preserve that refill and debit each name once.
    provider.pause.store(true, Ordering::SeqCst);
    let charges = [(a.clone(), 10), (b.clone(), 10)];
    let funding = Allocator::fund_maintenance(&charges, check);
    tokio::pin!(funding);
    tokio::select! {
        result = &mut funding => panic!("funding did not pause: {}", result.is_ok()),
        _ = provider.entered.notified() => (),
    }
    b.replenish(&UploadRequest {
        request_id: "race".into(),
        expected_configuration_revision: 1,
        limits: UploadLimits {
            bytes: 202,
            attempts: 3,
        },
    })
    .await
    .unwrap();
    provider.release.notify_one();
    funding.await.unwrap();
    assert_eq!(b.status().await.unwrap().limits.bytes, 202);
    assert_eq!(
        a.status()
            .await
            .unwrap()
            .maintenance
            .unwrap()
            .allocated
            .bytes,
        40
    );
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        Allocator::fund_maintenance(&[(a.clone(), 40), (b.clone(), 40)], check)
            .await
            .is_err()
    );
    for budget in [&a, &b] {
        let reserve = budget.status().await.unwrap().maintenance.unwrap();
        assert_eq!(
            reserve.allocated,
            UploadLimits {
                bytes: 80,
                attempts: 3
            }
        );
        assert!(
            budget.charge(22).await.is_err(),
            "uncertain maintenance forfeits admission rights no longer backed by reserve"
        );
    }
    // A later tenant's refusal must not debit its funded neighbor or upload
    // any ledger/object. All names are checked before the first metadata PUT.
    let status = a.status().await.unwrap();
    let before = provider.calls.load(Ordering::SeqCst);
    let manifests = provider.files.list("manifests/", None, 1000).await.unwrap();
    assert!(
        Allocator::fund_maintenance(&[(a.clone(), 10), (b.clone(), 23)], check)
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    assert_eq!(a.status().await.unwrap(), status);
    assert_eq!(
        provider.files.list("manifests/", None, 1000).await.unwrap(),
        manifests
    );
    // Restart creates no admission rights, but the unissued reserve survives.
    // A new owner fences the old claimant, even if it uses a fresh observation.
    let (mut replacement, outcome) = attempt(&connection).await;
    assert!(matches!(outcome, Outcome::Confirmed));

    validate(&mut replacement).await;
    activate(&mut replacement).await;
    let replacement_origin = replacement.recovery_record().unwrap();
    let before = provider.calls.load(Ordering::SeqCst);
    assert!(
        Allocator::fund_maintenance(&[(a.clone(), 1)], check)
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    let fresh_a = replacement.database_budget("a").unwrap().unwrap();
    let fresh_b = replacement.database_budget("b").unwrap().unwrap();
    assert!(fresh_a.charge(1).await.is_err());
    let check = |bucket: &Bucket| replacement.check_funding_origin(bucket, replacement_origin);
    Allocator::fund_maintenance(&[(fresh_a.clone(), 20), (fresh_b, 20)], check)
        .await
        .unwrap();
    let reserve = fresh_a.status().await.unwrap().maintenance.unwrap();
    assert_eq!(
        reserve.allocated,
        UploadLimits {
            bytes: 100,
            attempts: 4
        }
    );
    assert!(
        Allocator::fund_maintenance(&[(fresh_a.clone(), 1)], check)
            .await
            .is_err()
    );
    fresh_a
        .replenish(&UploadRequest {
            request_id: "recovery".into(),
            expected_configuration_revision: 1,
            limits: UploadLimits {
                bytes: 201,
                attempts: 3,
            },
        })
        .await
        .unwrap();
    Allocator::fund_maintenance(&[(fresh_a.clone(), 1)], check)
        .await
        .unwrap();
    let status = fresh_a.status().await.unwrap();
    assert_eq!(status.allocated.bytes, 201);
    let reserve = status.maintenance.unwrap();
    assert_eq!(reserve.funded_bytes, 101);
    assert_eq!(
        reserve.allocated,
        UploadLimits {
            bytes: 101,
            attempts: 5
        }
    );
    assert!(
        fresh_a.charge(1).await.is_err(),
        "a refill for recovery cannot recreate admission rights"
    );

    // Larger groups cross the physical allocator's 32-PUT grant boundary.
    // Its own authority updates must not make complete funding impossible.
    let mut many = Vec::new();
    for n in 0..70 {
        let budget = replacement
            .database_budget(&format!("many-{n:03}"))
            .unwrap()
            .unwrap();
        budget.initialize_database().await.unwrap();
        budget.charge(1).await.unwrap();
        many.push((budget, 1));
    }
    Allocator::fund_maintenance(&many, check).await.unwrap();
    provider.pause_budget.store(true, Ordering::SeqCst);
    let funding = Allocator::fund_maintenance(&many, check);
    tokio::pin!(funding);
    tokio::select! {
        result = &mut funding => panic!("funding did not pause: {}", result.is_ok()),
        _ = provider.entered.notified() => (),
    }
    let collector = replacement.collection();
    let generation = provider.record().collection;
    for _ in 0..4 {
        collector.collect().await.unwrap();
    }
    assert_eq!(provider.record().collection, generation);
    provider.release.notify_one();
    funding.await.unwrap();
    for (budget, _) in &many {
        let reserve = budget.status().await.unwrap().maintenance.unwrap();
        assert_eq!(reserve.allocated.bytes, 2);
        assert_eq!(reserve.allocated.attempts, 2);
    }
}

#[tokio::test]
async fn catalog_admission_charges_creation_without_blocking_policy_revocation() {
    use crate::{
        auth::Access,
        engine::{CommitRequest, CreateRequest, PolicyAction, PolicyRequest},
        transactions::live::{CatalogResult, Limits},
    };
    let provider = Provider::new();

    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 1024 * 1024 * 1024,
            attempts: 1024,
        })
        .await
        .unwrap();
    let (mut root, mut outcome) = attempt(&connection).await;
    if matches!(outcome, Outcome::Conflict) {
        (root, outcome) = attempt(&connection).await;
    }
    assert!(matches!(outcome, Outcome::Confirmed));

    validate(&mut root).await;
    activate(&mut root).await;
    for (name, attempts) in [
        ("empty", 0),
        ("lost", 1),
        ("a", 1),
        ("b", 2),
        ("durable-catalog", 2),
    ] {
        root.database_budget(name)
            .unwrap()
            .unwrap()
            .replenish(&UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 64 * 1024 * 1024,
                    attempts,
                },
            })
            .await
            .unwrap();
    }
    let mut owner = super::live::open(root, Limits::default()).await;
    let admin = Access::Administrator;
    let create = CreateRequest {
        request_id: "create".into(),
        limits: Default::default(),
    };
    let rotate = PolicyRequest {
        request_id: "rotate".into(),
        expected_policy_revision: 1,
        change: PolicyAction::Rotate,
    };
    let before = provider.files.list("journal/", None, 100).await.unwrap();
    let refused = owner
        .prepare_creation("empty", &create, &admin)
        .await
        .unwrap();
    assert!(refused.upload.is_none());
    assert!(matches!(
        owner.catalog_result(&refused).await,
        Err(crate::Error::Limit(_))
    ));
    assert_eq!(
        provider.files.list("journal/", None, 100).await.unwrap(),
        before
    );
    assert!(matches!(
        owner.info_authorized("empty", &admin).await,
        Err(crate::Error::NotFound)
    ));

    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        owner
            .prepare_creation("lost", &create, &admin)
            .await
            .is_err()
    );
    let refused = owner
        .prepare_creation("lost", &create, &admin)
        .await
        .unwrap();
    assert!(refused.upload.is_none());
    assert!(matches!(
        owner.catalog_result(&refused).await,
        Err(crate::Error::Limit(_))
    ));
    assert_eq!(
        provider.files.list("journal/", None, 100).await.unwrap(),
        before,
        "an uncertain grant never authorizes a catalog upload"
    );
    assert_eq!(
        owner
            .database_budget("lost")
            .unwrap()
            .unwrap()
            .status()
            .await
            .unwrap()
            .allocated
            .attempts,
        1
    );

    for name in ["a", "b", "durable-catalog"] {
        let ledger = owner.database_budget(name).unwrap().unwrap();
        let mut creation = owner.prepare_creation(name, &create, &admin).await.unwrap();
        let funded = ledger.status().await.unwrap();
        assert!(funded.allocated.bytes > 0);
        assert_eq!(
            funded.maintenance.as_ref().unwrap().funded_bytes * 2,
            funded.allocated.bytes
        );
        let pending = owner.prepare_creation(name, &create, &admin).await.unwrap();
        assert!(pending.upload.is_none());
        owner
            .complete(creation.upload.take().unwrap().send().await)
            .await
            .unwrap();
        let replay = owner.prepare_creation(name, &create, &admin).await.unwrap();
        assert!(
            matches!(owner.catalog_result(&replay).await.unwrap(), Some(CatalogResult::Created(r)) if r.replayed)
        );
        assert_eq!(
            ledger.status().await.unwrap(),
            funded,
            "creation retries must not recharge"
        );
    }
    let write: CommitRequest = serde_json::from_value(
        serde_json::json!({"request_id":"write", "expected_revision":0,
        "writes":[{"key":"a","value":"eA=="}]}),
    )
    .unwrap();
    let a = owner.database_budget("a").unwrap().unwrap();
    let a_status = a.status().await.unwrap();
    let before = provider.files.list("journal/", None, 100).await.unwrap();
    let refused = owner
        .prepare_authorized(&[("a", &write, &admin)])
        .await
        .unwrap();
    assert!(refused.upload.is_none());
    assert!(matches!(
        owner.authorized_result(&refused.tickets[0], &admin).await,
        Err(crate::Error::Limit(_))
    ));
    assert_eq!(
        provider.files.list("journal/", None, 100).await.unwrap(),
        before
    );
    // Exhausted tenants cannot prevent an administrator revoking credentials.
    let mut change = owner.prepare_policy("a", &rotate, &admin).await.unwrap();
    let pending = owner.prepare_policy("a", &rotate, &admin).await.unwrap();
    assert!(pending.upload.is_none());
    owner
        .complete(change.upload.take().unwrap().send().await)
        .await
        .unwrap();
    let replay = owner.prepare_policy("a", &rotate, &admin).await.unwrap();
    assert!(
        matches!(owner.catalog_result(&replay).await.unwrap(), Some(CatalogResult::Policy(r)) if r.replayed)
    );
    assert_eq!(a.status().await.unwrap(), a_status);
    assert_eq!(owner.policy_info("a", &admin).await.unwrap().revision, 2);
    // Funded neighbors still commit. A user's catalog-like name and a key
    // naming another tenant must not redirect this ordinary data charge.
    for name in ["b", "durable-catalog"] {
        let ledger = owner.database_budget(name).unwrap().unwrap();
        let funded = ledger.status().await.unwrap();
        let prepared = owner
            .prepare_authorized(&[(name, &write, &admin)])
            .await
            .unwrap();
        assert!(prepared.upload.is_some());
        owner
            .complete(prepared.upload.unwrap().send().await)
            .await
            .unwrap();
        assert!(
            owner
                .authorized_result(&prepared.tickets[0], &admin)
                .await
                .unwrap()
                .is_some()
        );
        let replay = owner
            .prepare_authorized(&[(name, &write, &admin)])
            .await
            .unwrap();
        assert!(replay.upload.is_none());
        let mut next = write.clone();
        next.request_id = "again".into();
        next.expected_revision = 1;
        let refused = owner
            .prepare_authorized(&[(name, &next, &admin)])
            .await
            .unwrap();
        assert!(refused.upload.is_none());
        assert!(matches!(
            owner.authorized_result(&refused.tickets[0], &admin).await,
            Err(crate::Error::Limit(_))
        ));
        assert_eq!(
            ledger.status().await.unwrap(),
            funded,
            "identical retry must not recharge"
        );
    }
    assert_eq!(
        a.status().await.unwrap(),
        a_status,
        "neighbors must not debit a"
    );
    a.replenish(&UploadRequest {
        request_id: "refill".into(),
        expected_configuration_revision: 1,
        limits: UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 2,
        },
    })
    .await
    .unwrap();
    let prepared = owner
        .prepare_authorized(&[("a", &write, &admin)])
        .await
        .unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert!(
        owner
            .authorized_result(&prepared.tickets[0], &admin)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn repeated_large_overwrites_reuse_copy_prepayment_and_recover() {
    use crate::{
        auth::Access,
        engine::{CommitRequest, CreateRequest, ReadRequest},
        transactions::live::Limits,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 1024 * 1024 * 1024,
            attempts: 100_000,
        })
        .await
        .unwrap();
    let limits = Limits::resident(128 * 1024 * 1024)
        .unwrap()
        .share(16)
        .unwrap();
    let root = super::checkpoint::budgeted_active(&connection).await;
    let mut owner = live::open(root, limits).await;
    let admin = Access::Administrator;
    let name = "overwrite-load";
    let ledger = owner.database_budget(name).unwrap().unwrap();
    ledger
        .replenish(&UploadRequest {
            request_id: "fund".into(),
            expected_configuration_revision: 0,
            limits: UploadLimits {
                bytes: 64 * 1024 * 1024,
                attempts: 10_000,
            },
        })
        .await
        .unwrap();
    let mut create = owner
        .prepare_creation(
            name,
            &CreateRequest {
                request_id: "create".into(),
                limits: crate::engine::Limits {
                    max_bytes: 1024 * 1024,
                    max_keys: 1000,
                },
            },
            &admin,
        )
        .await
        .unwrap();
    owner
        .complete(create.upload.take().unwrap().send().await)
        .await
        .unwrap();
    owner.maintain().await.unwrap();
    let mut last = None;
    for revision in 0..100 {
        let request: CommitRequest = serde_json::from_value(serde_json::json!({
            "expected_revision": revision, "request_id": format!("write-{revision:04}"),
            "writes": [{"key":"value", "value":STANDARD.encode(vec![revision as u8; 64 * 1024])}],
        }))
        .unwrap();
        let mut batch = owner
            .prepare_authorized(&[(name, &request, &admin)])
            .await
            .unwrap();
        assert!(
            batch.upload.is_some(),
            "write {revision} refused under the unchanged 64 MiB ceiling"
        );
        owner
            .complete(batch.upload.take().unwrap().send().await)
            .await
            .unwrap();
        assert_eq!(
            owner
                .authorized_result(&batch.tickets[0], &admin)
                .await
                .unwrap()
                .unwrap()
                .revision,
            revision + 1
        );
        owner.maintain().await.unwrap();
        last = Some(request);
    }
    let status = ledger.status().await.unwrap();
    assert!(status.allocated.bytes < status.limits.bytes);
    assert!(status.maintenance.unwrap().allocated.bytes > 0);
    drop(owner);
    let mut root = super::checkpoint::budgeted_active(&connection).await;
    root.select_journals().await.unwrap();
    let mut owner = live::open(root, limits).await;
    let request = last.unwrap();
    let replay = owner
        .prepare_authorized(&[(name, &request, &admin)])
        .await
        .unwrap();
    assert!(replay.upload.is_none());
    let result = owner
        .authorized_result(&replay.tickets[0], &admin)
        .await
        .unwrap()
        .unwrap();
    assert!(result.replayed);
    assert_eq!(result.revision, 100);
    let read: ReadRequest = serde_json::from_value(serde_json::json!({"keys":["value"]})).unwrap();
    let result = owner.read_authorized(name, &read, &admin).await.unwrap();
    assert_eq!(result.revision, 100);
    assert_eq!(
        result.values,
        vec![Some(STANDARD.encode(vec![99; 64 * 1024]))]
    );
}
