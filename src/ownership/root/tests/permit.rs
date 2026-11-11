use super::*;
use crate::{
    storage::{Object, Storage, Version},
    transactions::{
        live::{self, Request},
        record::{Database, Mutation},
    },
};
use bytes::Bytes;

struct DelayedRead {
    files: Arc<Remote>,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl Storage for DelayedRead {
    async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
        let result = self.files.get(key, max).await;
        if key.starts_with("permits/") && self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        result
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.files.put(key, bytes).await
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        unreachable!()
    }
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        self.files.list(prefix, after, limit).await
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn late_positive_reply_covers_only_the_sealed_result_and_recovered_prefix() {
    let provider = Provider::new();
    let files = Arc::new(DelayedRead {
        files: provider.files.clone(),
        pause: false.into(),
        entered: Default::default(),
        release: Default::default(),
    });
    let connection = Connection::new(files.clone(), provider.clone(), "test");
    let mut root = connection.root().unwrap();
    claim(&mut root).await;
    activate(&mut root).await;
    let mut owner = super::live::open(root, live::Limits::default()).await;
    let db = Database {
        name: "a",
        generation: Uuid::from_u128(10),
    };
    let snapshot = owner.snapshot(db).await.unwrap();
    let prepared = owner
        .prepare(&[Request {
            snapshot: &snapshot,
            id: "first",
            reads: &[],
            mutations: &[Mutation {
                key: "x",
                value: Some(b"one"),
            }],
        }])
        .await
        .unwrap();
    owner
        .complete(prepared.upload.unwrap().send().await)
        .await
        .unwrap();
    let ticket = prepared.tickets[0];
    files.pause.store(true, Ordering::SeqCst);
    let read = tokio::spawn(async move {
        let result = owner.outcome(ticket).await;
        (owner, result)
    });
    tokio::time::timeout(Duration::from_secs(2), files.entered.notified())
        .await
        .unwrap();
    let mut next = super::catalog::successor(&provider).await;
    let prefix = super::catalog::scan(&mut next).await;
    assert_eq!(prefix.finished_prefix().unwrap().position(), 1);
    super::catalog::seal(&mut next, prefix).await;
    let mut next = super::live::open(next, live::Limits::default()).await;
    let current = next.snapshot(db).await.unwrap();
    assert_eq!(
        next.get(&current, "x")
            .await
            .unwrap()
            .unwrap()
            .value
            .unwrap(),
        b"one"[..]
    );
    files.release.notify_one();
    let (mut owner, result) = read.await.unwrap();
    assert_eq!(result.unwrap(), live::Outcome::Committed(1));
    // The late positive response cannot be reused by a subsequent request,
    // including an exact receipt retry or an absent-key read.
    assert!(owner.outcome(ticket).await.is_err());
    assert!(owner.get(&snapshot, "absent").await.is_err());
    assert!(owner.snapshot(db).await.is_err());
}

#[tokio::test]
async fn interrupted_claim_does_not_hide_an_earlier_live_owner() {
    let provider = Provider::new();
    let old = active(&provider).await;
    let mut interrupted = provider.root();
    claim(&mut interrupted).await;
    assert!(old.permission().await.unwrap().is_some());
    let next = super::catalog::successor(&provider).await;
    assert!(next.permission().await.unwrap().is_some());
    assert!(old.permission().await.unwrap().is_none());
    assert!(interrupted.permission().await.unwrap().is_none());
    assert!(
        provider
            .files
            .get(&super::super::permit::key(interrupted.incarnation), 24)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn permit_creation_is_single_attempt_and_requires_positive_resolution() {
    for mode in [1, 2] {
        let provider = Provider::new();
        let files = super::registration::Faults::fail_at(provider.files.clone(), "permits/", mode);
        let connection = Connection::new(files.clone(), provider.clone(), "test");
        let mut root = connection.root().unwrap();
        let observed = root.observe().await.unwrap();
        let prepared = root.claim(observed).unwrap();
        let outcome = root.finish(prepared.execute().await).unwrap();
        assert_eq!(
            files.puts.load(Ordering::SeqCst),
            2,
            "one registration and one permit PUT"
        );
        if mode == 1 {
            assert!(matches!(outcome, Outcome::Confirmed));
            activate(&mut root).await;
            assert!(root.permission().await.unwrap().is_some());
        } else {
            assert!(matches!(outcome, Outcome::Uncertain(_)));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
            files.release().await;
            assert!(root.permission().await.unwrap().is_none());
            let observed = root.observe().await.unwrap();
            assert!(matches!(root.claim(observed), Err(Error::Bound)));
        }
        assert_eq!(
            provider
                .files
                .list("permits/", None, 8)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn collection_revokes_unpublished_claims_and_preserves_the_current_permit() {
    let provider = Provider::new();
    let root = active(&provider).await;
    let mut late = provider.root();
    let observed = late.observe().await.unwrap();
    let prepared = late.claim(observed).unwrap();
    provider.pause.store(true, Ordering::SeqCst);
    let work = tokio::spawn(prepared.execute());
    // Registration and permit already uploaded before the captured comparison.
    tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
        .await
        .unwrap();
    assert_eq!(root.collection().collect().await.unwrap(), 1);
    assert!(root.permission().await.unwrap().is_some());
    provider.release.notify_one();
    assert!(matches!(
        late.finish(work.await.unwrap()).unwrap(),
        Outcome::Conflict
    ));
    assert!(
        provider
            .files
            .get(&super::super::permit::key(late.incarnation), 24)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn corrupt_permit_and_corrupt_control_confirmation_never_grant_permission() {
    let provider = Provider::new();
    let mut root = active(&provider).await;
    provider
        .files
        .put(
            &super::super::permit::key(root.incarnation),
            Bytes::from_static(b"corrupt"),
        )
        .await
        .unwrap();
    assert!(root.permission().await.is_err());
    let observed = root.observe().await.unwrap();
    let prepared = root.validate(observed).unwrap();
    provider.corrupt_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.finish(prepared.execute().await),
        Err(Error::Incompatible)
    ));
    assert!(root.permission().await.is_err());
}

#[cfg(unix)]
#[test]
fn ownership_refuses_forked_handles_before_io_or_locking() {
    use std::{io, os::unix::process::CommandExt, process::Command};
    let provider = Provider::new();
    let root = provider.root();
    assert!(root.available().is_ok());
    let mut child = Command::new("/bin/true");
    // SAFETY: the mismatched PID path performs no allocation, I/O or locking.
    unsafe {
        child.pre_exec(move || {
            if root.available().is_err() {
                Err(io::Error::from_raw_os_error(1))
            } else {
                Ok(())
            }
        });
    }
    assert_eq!(
        child.spawn().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}
