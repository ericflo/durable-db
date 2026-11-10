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

async fn attempt(connection: &Connection, clock: &ManualClock) -> (Root<ManualClock>, Outcome) {
    let mut root = connection.root(clock.clone(), profile()).unwrap();
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
    assert!(
        provider
            .root(&ManualClock::default())
            .observe()
            .await
            .is_err()
    );
    let before = provider.calls.load(Ordering::SeqCst);
    let unbudgeted = Provider::new();
    claim(&mut unbudgeted.root(&ManualClock::default())).await;
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
    let clock = ManualClock::default();
    let connection = provider
        .connection("test")
        .with_upload_budget(limits(2))
        .await
        .unwrap();
    let (_, outcome) = attempt(&connection, &clock).await;
    assert!(
        matches!(outcome, Outcome::Conflict),
        "credit allocation invalidates the earlier comparison"
    );
    let (mut root, outcome) = attempt(&connection, &clock).await;
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
    clock.set(23);
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
    assert_eq!(status.allocated, limits(2));
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
    let (_, outcome) = attempt(&reopened, &clock).await;
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
            attempts: 6,
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
    let (_, outcome) = attempt(&reopened, &clock).await;
    assert!(matches!(outcome, Outcome::Conflict));
    let (_, outcome) = attempt(&reopened, &clock).await;
    assert!(matches!(outcome, Outcome::Confirmed));
}

#[tokio::test]
async fn native_budget_unknown_grant_and_late_upload_never_refund_credit() {
    let provider = Provider::new();
    let connection = provider
        .connection("test")
        .with_upload_budget(limits(1))
        .await
        .unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        connection
            .objects()
            .put("tables/lost", Bytes::new())
            .await
            .is_err()
    );
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
        1
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
    let root = connection.root(ManualClock::default(), profile()).unwrap();
    let a = root.database_budget("a").unwrap().unwrap();
    let b = root.database_budget("b").unwrap().unwrap();
    for ledger in [&a, &b] {
        ledger
            .replenish(&UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 100,
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
            bytes: 100,
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
            bytes: 100,
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
        .root(ManualClock::default(), profile())
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
        root.budget_collection().unwrap().collect().await.unwrap();
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
    let clock = ManualClock::default();
    let connection = provider
        .connection("test")
        .with_upload_budget(UploadLimits {
            bytes: 64 * 1024 * 1024,
            attempts: 10_000,
        })
        .await
        .unwrap();
    let _ = attempt(&connection, &clock).await;
    let (mut root, outcome) = attempt(&connection, &clock).await;
    assert!(matches!(outcome, Outcome::Confirmed));
    clock.set(23);
    validate(&mut root).await;
    activate(&mut root).await;
    let mut owner = live::open(root, Limits::default()).await;
    let admin = Access::Administrator;
    for name in ["a", "b"] {
        owner
            .database_budget(name)
            .unwrap()
            .unwrap()
            .replenish(&UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 4096,
                    attempts: 1,
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
    let first: CommitRequest = serde_json::from_value(serde_json::json!({"request_id":"first","expected_revision":0,"writes":[{"key":"x","value":"eA=="}]})).unwrap();
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
    let root = connection.root(ManualClock::default(), profile()).unwrap();
    let ledger = root.database_budget("app").unwrap().unwrap();
    ledger.initialize_database().await.unwrap();
    let collector = root.budget_collection().unwrap();
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
        ledger.status().await.unwrap().allocated.attempts,
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
    // Exhaust all remaining global attempts. The out-of-scope table identity
    // survives collection; deleting metadata never restores either allowance.
    while connection
        .objects()
        .put("tables/untouched", Bytes::new())
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
            .get("tables/untouched", 1)
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
