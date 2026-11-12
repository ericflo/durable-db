use super::*;
use crate::{
    journal::Reader,
    storage::{Object, Storage, Version},
};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub(super) struct Faults {
    inner: Arc<Remote>,
    mode: AtomicU64,
    fail_prefix: Option<&'static str>,
    pub(super) puts: AtomicU64,
    pub(super) registration_reads: AtomicU64,
    pub(super) frame_reads: AtomicU64,
    delayed: Mutex<Option<(String, Bytes)>>,
}
impl Faults {
    fn new(inner: Arc<Remote>, mode: u64) -> Arc<Self> {
        Arc::new(Self {
            inner,
            mode: AtomicU64::new(mode),
            fail_prefix: None,
            puts: AtomicU64::new(0),
            registration_reads: AtomicU64::new(0),
            frame_reads: AtomicU64::new(0),
            delayed: Mutex::new(None),
        })
    }
    pub(super) fn fail_at(inner: Arc<Remote>, prefix: &'static str, mode: u64) -> Arc<Self> {
        Arc::new(Self {
            inner,
            mode: AtomicU64::new(mode),
            fail_prefix: Some(prefix),
            puts: AtomicU64::new(0),
            registration_reads: AtomicU64::new(0),
            frame_reads: AtomicU64::new(0),
            delayed: Mutex::new(None),
        })
    }
    pub(super) async fn release(&self) {
        let delayed = self.delayed.lock().unwrap().take();
        if let Some((key, bytes)) = delayed {
            self.inner.put(&key, bytes).await.unwrap();
        }
    }
}
#[async_trait]
impl Storage for Faults {
    async fn get(&self, key: &str, limit: usize) -> anyhow::Result<Option<Object>> {
        if key.starts_with("ownership/") {
            self.registration_reads.fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with("journal/") {
            self.frame_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, limit).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let mode = if key.starts_with(self.fail_prefix.unwrap_or("")) {
            self.mode.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if mode == 2 {
            *self.delayed.lock().unwrap() = Some((key.into(), bytes));
            anyhow::bail!("upload still pending");
        }
        self.inner.put(key, bytes).await?;
        if mode == 1 {
            anyhow::bail!("upload committed; response lost");
        }
        Ok(())
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        unreachable!("registration publication uses the separate provider register")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        unreachable!("registration recovery must not depend on listing")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        unreachable!("registration must not delete history")
    }
}

#[tokio::test]
async fn unknown_registration_requires_a_positive_read_before_root_publication() {
    for mode in [1, 2] {
        let provider = Provider::new();
        let files = Faults::new(provider.files.clone(), mode);
        let connection = Connection::new(files.clone(), provider.clone(), "test");

        let mut root = connection.root().unwrap();
        let observed = root.observe().await.unwrap();
        let request = root.claim(observed).unwrap();
        let outcome = root.finish(request.execute().await).unwrap();
        assert_eq!(
            files.puts.load(Ordering::SeqCst),
            if mode == 1 { 2 } else { 1 }
        );
        if mode == 1 {
            assert!(matches!(outcome, Outcome::Confirmed));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(outcome, Outcome::Uncertain(_)));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
            let mut successor = connection.root().unwrap();
            claim(&mut successor).await;
            let delayed = files.delayed.lock().unwrap().take().unwrap();
            files.inner.put(&delayed.0, delayed.1).await.unwrap();
            assert!(root.permission().await.unwrap().is_none());
            assert_eq!(
                provider.record().identity.incarnation,
                successor.incarnation
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            // The late unreferenced upload does not become an ancestor.
            let mut chain = successor.registrations().unwrap();
            assert_eq!(chain.next().await.unwrap().unwrap().identity().epoch, 1);
            assert!(chain.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn cold_recovery_discovers_every_registered_journal_without_process_memory_or_listing() {
    let provider = Provider::new();
    let files = Faults::new(provider.files.clone(), 0);
    let connection = Connection::new(files.clone(), provider.clone(), "test");

    for epoch in 1..=3 {
        let mut root = connection.root().unwrap();
        assert!(matches!(
            root.take_journal(QueueLimits::default()),
            Err(Error::NotReady)
        ));
        claim(&mut root).await;

        validate(&mut root).await;
        activate(&mut root).await;
        let mut writer = root.take_journal(QueueLimits::default()).unwrap();
        assert!(matches!(
            root.take_journal(QueueLimits::default()),
            Err(Error::JournalOpened)
        ));
        let mut registration = root.registrations().unwrap();
        assert_eq!(
            writer.log(),
            registration.next().await.unwrap().unwrap().journal()
        );
        let upload = writer.prepare(&[Bytes::from(vec![epoch as u8])]).unwrap();
        writer.complete(upload.send().await).unwrap();
        assert!(writer.pop_durable().unwrap().is_some());
        // Root, writer, reader and all local log identities are dropped here.
    }
    // The provider keeps only its current register, not an old metadata cache.
    let current = provider.history.lock().unwrap().last().unwrap().clone();
    *provider.history.lock().unwrap() = vec![current];

    let mut replacement = connection.root().unwrap();
    claim(&mut replacement).await;

    validate(&mut replacement).await;
    activate(&mut replacement).await;
    let mut registrations = replacement.registrations().unwrap();
    assert_eq!(
        registrations
            .next()
            .await
            .unwrap()
            .unwrap()
            .identity()
            .epoch,
        4
    );
    for epoch in (1..=3).rev() {
        let registration = registrations.next().await.unwrap().unwrap();
        assert_eq!(registration.identity().epoch, epoch);
        let mut journal = Reader::discover(files.clone(), registration.journal());
        let batch = journal.read_next().await.unwrap().unwrap();
        assert_eq!(
            batch.records().collect::<Vec<_>>(),
            vec![&[epoch as u8][..]]
        );
        drop(batch);
        assert!(journal.read_next().await.unwrap().is_none());
    }
    assert!(registrations.next().await.unwrap().is_none());
    // This tests discovery of immutable bytes, not durable prefix selection or
    // database acknowledgements. Those remain necessary before serving data.
}

#[tokio::test]
async fn registration_encoding_accepts_both_namespace_size_boundaries() {
    for length in [1, 128] {
        let provider = Provider::new();
        let connection = provider.connection(&"p".repeat(length));
        let mut root = connection.root().unwrap();
        claim(&mut root).await;
        let key = provider
            .files
            .list("ownership/", None, 2)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            provider
                .files
                .get(&key, 234)
                .await
                .unwrap()
                .unwrap()
                .bytes
                .len(),
            106 + length
        );
        let mut chain = root.registrations().unwrap();
        assert_eq!(chain.next().await.unwrap().unwrap().identity().epoch, 1);
        assert!(chain.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn missing_corrupt_or_incompatible_registrations_never_truncate_recovery() {
    let provider = Provider::new();

    let mut root = provider.root();
    claim(&mut root).await;
    let key = provider
        .files
        .list("ownership/", None, 2)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let original = provider.files.get(&key, 234).await.unwrap().unwrap().bytes;
    let mut chain = root.registrations().unwrap();
    provider.files.delete(&key).await.unwrap();
    assert!(chain.next().await.is_err());
    for offset in 0..original.len() {
        let mut corrupt = original.to_vec();
        corrupt[offset] ^= 1;
        provider.files.put(&key, corrupt.into()).await.unwrap();
        assert!(chain.next().await.is_err(), "unchecked byte {offset}");
    }
    // Recomputed checksums must not bypass semantic binding: version, identity,
    // predecessor, reserved encoding fields, prefix length and namespace are independently checked.
    for offset in [0, 8, 16, 32, 40, 56, 64, 72, 74] {
        let mut invalid = original.to_vec();
        invalid[offset] ^= 1;
        let end = invalid.len() - 32;
        let digest = Sha256::digest(&invalid[..end]);
        invalid[end..].copy_from_slice(&digest);
        provider.files.put(&key, invalid.into()).await.unwrap();
        assert!(chain.next().await.is_err(), "unchecked field at {offset}");
    }
    provider
        .files
        .put(&key, Bytes::from(vec![0; 235]))
        .await
        .unwrap();
    assert!(chain.next().await.is_err());
    provider.files.put(&key, original).await.unwrap();
    assert_eq!(chain.next().await.unwrap().unwrap().identity().epoch, 1);
    assert!(chain.next().await.unwrap().is_none());
}
