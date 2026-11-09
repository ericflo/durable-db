use super::*;
use crate::providers::register::Register;
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Provider {
    bucket: Mutex<Bucket>,
    calls: AtomicUsize,
    lose_reply: AtomicBool,
    oversized_reply: AtomicBool,
}

impl Provider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            bucket: Mutex::new(Bucket {
                revision: 0,
                version: None,
                info: metadata("open"),
            }),
            calls: AtomicUsize::new(0),
            lose_reply: AtomicBool::new(false),
            oversized_reply: AtomicBool::new(false),
        })
    }

    fn session(self: &Arc<Self>) -> Session {
        Session::new(Arc::new(Register(self.clone())))
    }
}

fn metadata(phase: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("phase".into(), phase.into())])
}

#[async_trait]
impl Authority for Provider {
    async fn read(&self) -> anyhow::Result<Bucket> {
        Ok(self.bucket.lock().unwrap().clone())
    }

    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut bucket = self.bucket.lock().unwrap();
        // Pin B2's identical-payload behavior. The connection wrapper must
        // defeat this shortcut even when the caller only revalidates a view.
        if bucket.info == info {
            return Ok(Some(bucket.clone()));
        }
        if old.revision != bucket.revision {
            return Ok(None);
        }
        bucket.revision += 1;
        bucket.info = info;
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost response");
        }
        if self.oversized_reply.swap(false, Ordering::SeqCst) {
            let mut response = bucket.clone();
            response.info = metadata(&"x".repeat(MAX_METADATA_BYTES));
            return Ok(Some(response));
        }
        Ok(Some(bucket.clone()))
    }

    async fn delete_versions(&self, _: &str) -> anyhow::Result<()> {
        unreachable!("control requests do not delete objects")
    }
}

async fn revalidate(session: &mut Session) {
    let observed = session.observe().await.unwrap();
    let metadata = observed.metadata().clone();
    let request = session.begin(observed, metadata).unwrap();
    assert!(session.confirmed().is_none());
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Confirmed
    ));
}

#[tokio::test]
async fn reading_does_not_validate_or_replace_a_confirmed_observation() {
    let provider = Provider::new();
    let mut session = provider.session();
    session.observe().await.unwrap();
    assert!(session.confirmed().is_none());
    revalidate(&mut session).await;

    let mut peer = provider.session();
    let observed = peer.observe().await.unwrap();
    let request = peer.begin(observed, metadata("frozen")).unwrap();
    assert!(matches!(
        peer.finish(request.execute().await),
        Outcome::Confirmed
    ));
    assert_eq!(
        session.observe().await.unwrap().metadata()["phase"],
        "frozen"
    );
    // This is a historical comparison result, not a continuing ownership grant.
    assert_eq!(session.confirmed().unwrap().metadata()["phase"], "open");
}

#[tokio::test]
async fn abandoned_prepared_write_cannot_apply_after_revalidation() {
    let provider = Provider::new();
    let mut session = provider.session();
    let observed = session.observe().await.unwrap();
    let delayed = session.begin(observed.clone(), metadata("frozen")).unwrap();
    assert!(matches!(
        session.begin(observed, metadata("other")),
        Err(Error::Busy)
    ));
    session.abandon();
    revalidate(&mut session).await;
    let nonce = session.confirmed().unwrap().metadata()["durable-db-probe"].clone();

    assert!(matches!(
        session.finish(delayed.execute().await),
        Outcome::Ignored
    ));
    assert_eq!(provider.bucket.lock().unwrap().info["phase"], "open");
    assert_eq!(
        session.confirmed().unwrap().metadata()["durable-db-probe"],
        nonce
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn abandoned_write_can_apply_but_its_reply_cannot_restore_an_old_view() {
    let provider = Provider::new();
    let mut session = provider.session();
    let observed = session.observe().await.unwrap();
    let delayed = session.begin(observed, metadata("frozen")).unwrap();
    session.abandon();
    let late_reply = delayed.execute().await;
    assert_eq!(provider.bucket.lock().unwrap().info["phase"], "frozen");
    assert!(session.confirmed().is_none());
    revalidate(&mut session).await;

    let observed = session.confirmed().unwrap().clone();
    let request = session.begin(observed, metadata("replacement")).unwrap();
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Confirmed
    ));
    assert!(matches!(session.finish(late_reply), Outcome::Ignored));
    assert_eq!(
        session.confirmed().unwrap().metadata()["phase"],
        "replacement"
    );
}

#[tokio::test]
async fn lost_response_and_later_conflict_leave_the_first_outcome_unresolved() {
    let provider = Provider::new();
    let mut session = provider.session();
    let stale = session.observe().await.unwrap();
    let request = session.begin(stale.clone(), metadata("frozen")).unwrap();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Uncertain(_)
    ));
    assert!(session.confirmed().is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let request = session.begin(stale, metadata("frozen")).unwrap();
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Conflict
    ));
    assert!(session.confirmed().is_none());
    assert_eq!(provider.bucket.lock().unwrap().info["phase"], "frozen");
    revalidate(&mut session).await;
    assert_eq!(session.confirmed().unwrap().metadata()["phase"], "frozen");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn restart_rejects_foreign_observations_and_completions_without_clearing_its_request() {
    let provider = Provider::new();
    let mut old = provider.session();
    let observed = old.observe().await.unwrap();
    let mut replacement = provider.session();
    assert!(matches!(
        replacement.begin(observed.clone(), metadata("foreign")),
        Err(Error::ForeignObservation)
    ));
    let old_request = old.begin(observed, metadata("frozen")).unwrap();
    let old_reply = old_request.execute().await;

    let observed = replacement.observe().await.unwrap();
    let own_request = replacement
        .begin(observed, metadata("replacement"))
        .unwrap();
    assert!(matches!(replacement.finish(old_reply), Outcome::Ignored));
    assert!(matches!(
        replacement.finish(own_request.execute().await),
        Outcome::Confirmed
    ));
    assert_eq!(
        replacement.confirmed().unwrap().metadata()["phase"],
        "replacement"
    );
}

#[tokio::test]
async fn oversized_success_is_uncertain_even_when_the_write_applied() {
    let provider = Provider::new();
    let mut session = provider.session();
    revalidate(&mut session).await;
    let observed = session.confirmed().unwrap().clone();
    let request = session.begin(observed, metadata("frozen")).unwrap();
    provider.oversized_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Uncertain(_)
    ));
    assert!(session.confirmed().is_none());
    assert_eq!(provider.bucket.lock().unwrap().info["phase"], "frozen");
    revalidate(&mut session).await;
    assert_eq!(session.confirmed().unwrap().metadata()["phase"], "frozen");
}

#[tokio::test]
async fn bounds_and_exhaustion_fail_before_invalidating_the_last_confirmation() {
    let provider = Provider::new();
    let mut session = provider.session();
    revalidate(&mut session).await;
    let observed = session.confirmed().unwrap().clone();
    let many_fields = (0..MAX_FIELDS)
        .map(|i| (format!("field-{i}"), String::new()))
        .collect();
    for oversized in [
        metadata(&"x".repeat(MAX_METADATA_BYTES)),
        metadata(&"\0".repeat(2000)),
        many_fields,
    ] {
        assert!(matches!(
            session.begin(observed.clone(), oversized),
            Err(Error::TooLarge)
        ));
        assert!(session.confirmed().is_some());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let mut fields: BTreeMap<String, String> = (0..MAX_FIELDS - 1)
        .map(|i| (format!("field-{i}"), String::new()))
        .collect();
    fields.insert("durable-db-probe".into(), "old".into());
    let request = session.begin(observed, fields).unwrap();
    assert!(matches!(
        session.finish(request.execute().await),
        Outcome::Confirmed
    ));
    assert_eq!(session.confirmed().unwrap().metadata().len(), MAX_FIELDS);

    session.sequence = u64::MAX;
    let observed = session.confirmed().unwrap().clone();
    assert!(matches!(
        session.begin(observed, metadata("never sent")),
        Err(Error::Exhausted)
    ));
    assert!(session.confirmed().is_some());
    session.abandon();
    assert!(session.confirmed().is_none());
    provider.bucket.lock().unwrap().info = metadata(&"x".repeat(MAX_METADATA_BYTES));
    assert!(session.observe().await.is_err());
    assert!(session.confirmed().is_none());
}
