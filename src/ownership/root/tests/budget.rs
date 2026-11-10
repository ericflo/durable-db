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
