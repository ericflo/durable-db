use super::live::open;
use super::*;
use crate::{
    Error,
    auth::{Access, Auth},
    engine::{CommitRequest, CreateRequest, PolicyAction, PolicyRequest, ReadRequest},
    transactions::live::{self, CatalogResult, Owner},
};
use catalog::{scan, seal, successor};

fn create(id: &str) -> CreateRequest {
    CreateRequest {
        request_id: id.into(),
        limits: Default::default(),
    }
}
fn write() -> CommitRequest {
    serde_json::from_str(
        r#"{"expected_revision":0,"request_id":"write","writes":[{"key":"x","value":"eA=="}]}"#,
    )
    .unwrap()
}
async fn credential(
    owner: &Owner<ManualClock>,
    db: &str,
    auth: &Auth,
    write: bool,
) -> (Access, String) {
    let policy = owner.policy_info(db, &Access::Administrator).await.unwrap();
    let (token, id) = auth
        .issue_for_policy(
            db.into(),
            write,
            3600,
            policy.incarnation,
            policy.generation,
        )
        .unwrap();
    (auth.access(&token, db, write).unwrap(), id)
}

#[tokio::test]
async fn native_lifecycle_creation_is_durable_idempotent_and_cannot_expose_the_internal_catalog() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            checkpoint_frames: 1,
            ..Default::default()
        },
    )
    .await;
    let admin = Access::Administrator;
    let request = create("create");
    let mut creation = owner
        .prepare_creation("app", &request, &admin)
        .await
        .unwrap();
    assert!(owner.catalog_result(&creation).unwrap().is_none());
    assert!(matches!(
        owner.policy_info("app", &admin).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        owner.prepare_authorized(&[("app", &write(), &admin)]).await,
        Err(Error::NotFound)
    ));
    let duplicate = owner
        .prepare_creation("app", &request, &admin)
        .await
        .unwrap();
    assert!(duplicate.upload.is_none());
    assert!(owner.catalog_result(&duplicate).unwrap().is_none());
    assert!(matches!(
        owner
            .prepare_creation("app", &create("different"), &admin)
            .await,
        Err(Error::Busy)
    ));
    owner
        .complete(creation.upload.take().unwrap().send().await)
        .await
        .unwrap();
    let CatalogResult::Created(first) = owner.catalog_result(&creation).unwrap().unwrap() else {
        panic!("wrong result kind")
    };
    assert!(!first.replayed);
    let CatalogResult::Created(retry) = owner.catalog_result(&duplicate).unwrap().unwrap() else {
        panic!("wrong result kind")
    };
    assert!(retry.replayed);
    assert_eq!(
        serde_json::to_value(&retry.info).unwrap(),
        serde_json::to_value(&first.info).unwrap()
    );
    assert_eq!(owner.usage().versions, 0);
    let empty = owner.info_authorized("app", &admin).await.unwrap();
    assert_eq!((empty.revision, empty.keys, empty.bytes), (0, 0, 0));
    assert!(serde_json::to_value(&empty).unwrap().get("pages").is_none());
    let auth = Auth::new(&"31".repeat(32)).unwrap();
    let (writer, _) = credential(&owner, "app", &auth, true).await;
    let (reader, _) = credential(&owner, "app", &auth, false).await;
    assert!(matches!(
        owner
            .prepare_creation("other", &create("other"), &writer)
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner
            .prepare_authorized(&[("app", &write(), &reader)])
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner
            .read_authorized("unknown", &ReadRequest::default(), &writer)
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner
            .prepare_creation("app", &create("other"), &admin)
            .await,
        Err(Error::Conflict)
    ));
    let mut changed = request.clone();
    changed.limits.max_bytes += 1;
    assert!(matches!(
        owner.prepare_creation("app", &changed, &admin).await,
        Err(Error::Invalid(_))
    ));
    let prepared = owner
        .prepare_authorized(&[("app", &write(), &writer)])
        .await
        .unwrap();
    assert!(
        owner
            .authorized_result(&prepared.tickets[0], &writer)
            .await
            .unwrap()
            .is_none()
    );
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner
            .authorized_result(&prepared.tickets[0], &writer)
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let read = ReadRequest {
        keys: vec!["x".into()],
        ..Default::default()
    };
    assert_eq!(
        owner
            .read_authorized("app", &read, &reader)
            .await
            .unwrap()
            .values,
        vec![Some("eA==".into())]
    );
    let info = owner.info_authorized("app", &reader).await.unwrap();
    assert_eq!((info.revision, info.keys, info.bytes), (1, 1, 2));
    assert!(matches!(
        owner.info_authorized("other", &reader).await,
        Err(Error::Unauthorized)
    ));
    let legacy = auth
        .access(&auth.issue("app".into(), true, 3600).unwrap(), "app", true)
        .unwrap();
    assert!(matches!(
        owner.read_authorized("app", &read, &legacy).await,
        Err(Error::Unauthorized)
    ));
    // Even an identically named public database has a different generation.
    let mut internal_name = owner
        .prepare_creation("durable-catalog", &create("reserved-name"), &admin)
        .await
        .unwrap();
    owner
        .complete(internal_name.upload.take().unwrap().send().await)
        .await
        .unwrap();
    let read = ReadRequest {
        keys: vec!["app".into()],
        ..Default::default()
    };
    assert_eq!(
        owner
            .read_authorized("durable-catalog", &read, &admin)
            .await
            .unwrap()
            .values,
        vec![None]
    );
    // Reopen from a new owner: policy and permanent creation receipt share recovery.
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    assert!(owner.catalog_result(&creation).is_err());
    let mut owner = open(root, live::Limits::default()).await;
    let retry = owner
        .prepare_creation("app", &request, &admin)
        .await
        .unwrap();
    assert!(retry.upload.is_none());
    let CatalogResult::Created(result) = owner.catalog_result(&retry).unwrap().unwrap() else {
        panic!("wrong result kind")
    };
    assert!(result.replayed);
    assert_eq!(
        serde_json::to_value(result.info).unwrap(),
        serde_json::to_value(first.info).unwrap()
    );
    let restored = owner.info_authorized("app", &reader).await.unwrap();
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(&info).unwrap()
    );
    assert_eq!(
        owner
            .read_authorized("app", &ReadRequest::default(), &writer)
            .await
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
async fn native_lifecycle_pending_revocation_fences_writes_and_durable_revocation_fences_replays() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            checkpoint_frames: 3,
            ..Default::default()
        },
    )
    .await;
    let admin = Access::Administrator;
    let mut creation = owner
        .prepare_creation("app", &create("create"), &admin)
        .await
        .unwrap();
    owner
        .complete(creation.upload.take().unwrap().send().await)
        .await
        .unwrap();
    let auth = Auth::new(&"32".repeat(32)).unwrap();
    let (writer, token) = credential(&owner, "app", &auth, true).await;
    let (reader, _) = credential(&owner, "app", &auth, false).await;
    let write = write();
    let prepared = owner
        .prepare_authorized(&[("app", &write, &writer)])
        .await
        .unwrap();
    let revoke = PolicyRequest {
        request_id: "revoke".into(),
        expected_policy_revision: 1,
        change: PolicyAction::Revoke { token_id: token },
    };
    let mut change = owner.prepare_policy("app", &revoke, &admin).await.unwrap();
    let duplicate = owner.prepare_policy("app", &revoke, &admin).await.unwrap();
    assert!(duplicate.upload.is_none());
    assert!(matches!(
        owner.prepare_authorized(&[("app", &write, &writer)]).await,
        Err(Error::Busy)
    ));
    // A later policy upload cannot publish across the earlier data gap.
    owner
        .complete(change.upload.take().unwrap().send().await)
        .await
        .unwrap();
    assert!(owner.catalog_result(&change).unwrap().is_none());
    assert_eq!(
        owner
            .read_authorized("app", &ReadRequest::default(), &writer)
            .await
            .unwrap()
            .revision,
        0
    );
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    assert!(owner.catalog_result(&change).unwrap().is_some());
    assert!(owner.catalog_result(&duplicate).unwrap().is_some());
    assert!(matches!(
        owner
            .read_authorized("app", &ReadRequest::default(), &writer)
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner.authorized_result(&prepared.tickets[0], &writer).await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner.prepare_authorized(&[("app", &write, &writer)]).await,
        Err(Error::Unauthorized)
    ));
    assert_eq!(
        owner
            .read_authorized("app", &ReadRequest::default(), &reader)
            .await
            .unwrap()
            .revision,
        1
    );
    let mut rotate = owner
        .prepare_policy(
            "app",
            &PolicyRequest {
                request_id: "rotate".into(),
                expected_policy_revision: 2,
                change: PolicyAction::Rotate,
            },
            &admin,
        )
        .await
        .unwrap();
    owner
        .complete(rotate.upload.take().unwrap().send().await)
        .await
        .unwrap();
    assert!(matches!(
        owner
            .read_authorized("app", &ReadRequest::default(), &reader)
            .await,
        Err(Error::Unauthorized)
    ));
    let (fresh, _) = credential(&owner, "app", &auth, true).await;
    assert_eq!(
        owner
            .read_authorized("app", &ReadRequest::default(), &fresh)
            .await
            .unwrap()
            .revision,
        1
    );
    // Old policy retries return their original policy result, not current state.
    let replay = owner.prepare_policy("app", &revoke, &admin).await.unwrap();
    let CatalogResult::Policy(result) = owner.catalog_result(&replay).unwrap().unwrap() else {
        panic!("wrong result kind")
    };
    assert!(result.replayed);
    assert_eq!(result.policy.revision, 2);
    let mut root = successor(&provider, &clock).await;
    let journal = scan(&mut root).await;
    seal(&mut root, journal).await;
    let owner = open(root, live::Limits::default()).await;
    assert!(matches!(
        owner
            .read_authorized("app", &ReadRequest::default(), &writer)
            .await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        owner
            .read_authorized("app", &ReadRequest::default(), &reader)
            .await,
        Err(Error::Unauthorized)
    ));
    assert_eq!(
        owner
            .read_authorized("app", &ReadRequest::default(), &fresh)
            .await
            .unwrap()
            .revision,
        1
    );
}

fn quota_write(revision: u64, id: &str, writes: &[(&str, Option<&[u8]>)]) -> CommitRequest {
    use base64::Engine as _;
    serde_json::from_value(serde_json::json!({
        "expected_revision": revision, "request_id": id,
        "writes": writes.iter().map(|(key, value)| serde_json::json!({
            "key": key, "value": value.map(|v| base64::engine::general_purpose::STANDARD.encode(v))
        })).collect::<Vec<_>>()
    }))
    .unwrap()
}
async fn apply_quota_write(
    owner: &mut Owner<ManualClock>,
    request: &CommitRequest,
) -> crate::Result<crate::engine::CommitResult> {
    let prepared = owner
        .prepare_authorized(&[("app", request, &Access::Administrator)])
        .await?;
    if let Some(upload) = prepared.upload {
        owner.complete(upload.send().await).await?;
    }
    Ok(owner
        .authorized_result(&prepared.tickets[0], &Access::Administrator)
        .await?
        .unwrap())
}

#[tokio::test]
async fn native_lifecycle_quotas_survive_retries_pending_writes_checkpoints_and_takeover() {
    for checkpoint_frames in [1, 256] {
        let provider = Provider::new();
        let clock = ManualClock::default();
        let limits = live::Limits {
            checkpoint_frames,
            ..Default::default()
        };
        let mut owner = open(active(&provider, &clock).await, limits).await;
        let admin = Access::Administrator;
        for db in ["app", "neighbor"] {
            let mut request = create(db);
            request.limits = crate::engine::Limits {
                max_keys: 1,
                max_bytes: 4,
            };
            let mut prepared = owner.prepare_creation(db, &request, &admin).await.unwrap();
            owner
                .complete(prepared.upload.take().unwrap().send().await)
                .await
                .unwrap();
        }
        let first = quota_write(0, "first", &[("x", Some(b"abc"))]);
        let competing = quota_write(0, "competing", &[("y", Some(b"abc"))]);
        let too_large = quota_write(0, "too-large", &[("x", Some(b"abcd"))]);
        let group = owner
            .prepare_authorized(&[
                ("app", &first, &admin),
                ("app", &first, &admin),
                ("app", &competing, &admin),
                ("neighbor", &too_large, &admin),
                ("neighbor", &first, &admin),
            ])
            .await
            .unwrap();
        assert!(
            owner
                .authorized_result(&group.tickets[0], &admin)
                .await
                .unwrap()
                .is_none()
        );
        owner
            .complete(group.upload.unwrap().send().await)
            .await
            .unwrap();
        for i in [0, 1, 4] {
            let result = owner
                .authorized_result(&group.tickets[i], &admin)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.revision, 1);
            assert_eq!(result.replayed, i == 1);
        }
        assert!(matches!(
            owner.authorized_result(&group.tickets[2], &admin).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            owner.authorized_result(&group.tickets[3], &admin).await,
            Err(Error::Limit(_))
        ));
        let controls = provider.calls.load(Ordering::SeqCst);
        for request in [
            quota_write(1, "keys", &[("y", Some(b""))]),
            quota_write(1, "bytes", &[("x", Some(b"abcd"))]),
        ] {
            let rejected = owner
                .prepare_authorized(&[("app", &request, &admin)])
                .await
                .unwrap();
            assert!(rejected.upload.is_none());
            assert!(matches!(
                owner.authorized_result(&rejected.tickets[0], &admin).await,
                Err(Error::Limit(_))
            ));
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), controls);
        // Missing deletes, empty values and replacements count the final map,
        // even when sorted mutation order temporarily exceeds the key limit.
        let edits = [
            quota_write(1, "missing", &[("missing", None)]),
            quota_write(2, "empty", &[("x", Some(b""))]),
            quota_write(3, "replace", &[("aa", Some(b"zz")), ("x", None)]),
            quota_write(
                4,
                "duplicate",
                &[("aa", Some(b"oversized")), ("aa", Some(b"zz"))],
            ),
        ];
        for (i, request) in edits.iter().enumerate() {
            assert_eq!(
                apply_quota_write(&mut owner, request)
                    .await
                    .unwrap()
                    .revision,
                i as u64 + 2
            );
        }
        let mut root = successor(&provider, &clock).await;
        let journal = scan(&mut root).await;
        seal(&mut root, journal).await;
        let mut owner = open(root, limits).await;
        // Resolve the original receipt before comparing its stale expected
        // revision or charging its now-over-quota hypothetical writes again.
        let retry = apply_quota_write(&mut owner, &first).await.unwrap();
        assert_eq!(retry.revision, 1);
        assert!(retry.replayed);
        assert!(matches!(
            apply_quota_write(
                &mut owner,
                &quota_write(5, "after-recovery", &[("x", Some(b""))])
            )
            .await,
            Err(Error::Limit(_))
        ));
        let deleted = quota_write(5, "delete", &[("aa", None)]);
        assert_eq!(
            apply_quota_write(&mut owner, &deleted)
                .await
                .unwrap()
                .revision,
            6
        );
        assert_eq!(
            apply_quota_write(
                &mut owner,
                &quota_write(6, "refill", &[("x", Some(b"abc"))])
            )
            .await
            .unwrap()
            .revision,
            7
        );
        let read = ReadRequest {
            keys: vec!["x".into(), "aa".into()],
            ..Default::default()
        };
        assert_eq!(
            owner
                .read_authorized("app", &read, &admin)
                .await
                .unwrap()
                .values,
            vec![Some("YWJj".into()), None]
        );
        assert_eq!(
            owner
                .read_authorized("neighbor", &read, &admin)
                .await
                .unwrap()
                .revision,
            1
        );
    }
}

#[tokio::test]
async fn native_renewal_waits_for_checkpoint_without_abandoning_its_invocation() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(
        active(&provider, &clock).await,
        live::Limits {
            checkpoint_frames: 1,
            ..Default::default()
        },
    )
    .await;
    let control = owner.control();
    let mut creation = owner
        .prepare_creation("app", &create("create"), &Access::Administrator)
        .await
        .unwrap();
    let uploaded = creation.upload.take().unwrap().send().await;
    provider.pause.store(true, Ordering::SeqCst);
    let finish = tokio::spawn(async move {
        owner.complete(uploaded).await.unwrap();
        owner
    });
    provider.entered.notified().await;
    let calls = provider.calls.load(Ordering::SeqCst);
    let mut renew = tokio::spawn(async move { control.renew().await.unwrap() });
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut renew)
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    provider.release.notify_one();
    let owner = finish.await.unwrap();
    assert!(matches!(renew.await.unwrap(), Outcome::Confirmed));
    assert!(provider.record().checkpoint.is_some());
    assert!(owner.catalog_result(&creation).unwrap().is_some());
    assert_eq!(
        owner
            .info_authorized("app", &Access::Administrator)
            .await
            .unwrap()
            .revision,
        0
    );
}

#[tokio::test]
async fn native_cancelled_renewal_keeps_reads_live_and_does_not_strand_the_control_gate() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut owner = open(active(&provider, &clock).await, live::Limits::default()).await;
    let mut creation = owner
        .prepare_creation("app", &create("create"), &Access::Administrator)
        .await
        .unwrap();
    owner
        .complete(creation.upload.take().unwrap().send().await)
        .await
        .unwrap();
    let control = owner.control();
    let stalled = control.clone();
    provider.pause.store(true, Ordering::SeqCst);
    let renewal = tokio::spawn(async move { stalled.renew().await });
    provider.entered.notified().await;
    // Waiting on network holds no permission mutex or owner/data borrow.
    assert_eq!(
        owner
            .info_authorized("app", &Access::Administrator)
            .await
            .unwrap()
            .revision,
        0
    );
    renewal.abort();
    assert!(matches!(renewal.await, Err(error) if error.is_cancelled()));
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        control.renew().await.unwrap(),
        Outcome::Uncertain(_)
    ));
    assert!(matches!(control.renew().await.unwrap(), Outcome::Confirmed));
    assert!(owner.catalog_result(&creation).unwrap().is_some());
}

#[tokio::test(start_paused = true)]
async fn native_renewal_scheduler_bounds_timeouts_skips_missed_ticks_and_stops_on_drop() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let owner = open(active(&provider, &clock).await, live::Limits::default()).await;
    let control = owner.control();
    assert!(
        control
            .keep_alive(Duration::ZERO, Duration::from_secs(1))
            .is_err()
    );
    provider.pause.store(true, Ordering::SeqCst);
    let renewal = control
        .keep_alive(Duration::from_millis(10), Duration::from_millis(5))
        .unwrap();
    provider.entered.notified().await;
    let calls = provider.calls.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(6)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls + 1,
        "timeout must release the control gate"
    );
    assert!(
        owner
            .read_request(
                crate::transactions::record::Database {
                    name: "app",
                    generation: Uuid::from_u128(9)
                },
                &ReadRequest::default()
            )
            .await
            .is_ok()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls + 2,
        "missed ticks cannot cause a renewal burst"
    );
    drop(renewal);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls + 2);
    clock.set(100);
    assert!(
        owner
            .read_request(
                crate::transactions::record::Database {
                    name: "app",
                    generation: Uuid::from_u128(9)
                },
                &ReadRequest::default()
            )
            .await
            .is_err()
    );
}
