//! Bounded groups of readers sealed before their shared remote read barrier.
use crate::metrics::{Event, REGISTRY};
use crate::publication::{Authority, Bucket};
use anyhow::Context;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

const CAPACITY: usize = 64;
type Reply = oneshot::Sender<anyhow::Result<Bucket>>;

pub(crate) struct Observations {
    queue: mpsc::Sender<Reply>,
}

impl Observations {
    pub fn new(authority: Arc<dyn Authority>) -> Self {
        let (queue, mut pending) = mpsc::channel::<Reply>(CAPACITY);
        tokio::spawn(async move {
            while let Some(first) = pending.recv().await {
                let mut members = vec![first];
                // A small, bounded gathering window also lets concurrent readers
                // share a barrier when the provider responds very quickly.
                tokio::time::sleep(Duration::from_millis(1)).await;
                while members.len() < CAPACITY {
                    match pending.try_recv() {
                        Ok(reply) => members.push(reply),
                        Err(_) => break,
                    }
                }
                members.retain(|reply| !reply.is_closed());
                if members.is_empty() {
                    continue;
                }
                REGISTRY.event(Event::ReadGroups, 1);
                REGISTRY.event(Event::ReadGroupMembers, members.len());
                // Membership is now sealed. Readers arriving during ANY part of
                // read_fresh, including after its CAS but before its reply, must
                // wait for the NEXT barrier. Sharing an in-flight future or a
                // completed result with them would violate linearizability.
                match authority.read_fresh().await {
                    Ok(bucket) => {
                        for reply in members {
                            let _ = reply.send(Ok(bucket.clone()));
                        }
                    }
                    Err(error) => {
                        for reply in members {
                            let _ = reply.send(Err(anyhow::anyhow!("{error:#}")));
                        }
                    }
                }
            }
        });
        Self { queue }
    }

    pub async fn read(&self) -> anyhow::Result<Bucket> {
        let (reply, result) = oneshot::channel();
        self.queue.try_send(reply).map_err(|_| {
            REGISTRY.event(Event::ObservationQueueRejected, 1);
            anyhow::anyhow!("publication observation queue unavailable")
        })?;
        result
            .await
            .context("publication observation worker stopped")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    #[derive(Default)]
    struct Gated {
        revision: AtomicU64,
        calls: AtomicUsize,
        fail: AtomicBool,
        validated: Notify,
        acknowledge: Notify,
    }
    #[async_trait]
    impl Authority for Gated {
        async fn read(&self) -> anyhow::Result<Bucket> {
            Ok(Bucket {
                revision: self.revision.load(Ordering::SeqCst),
                version: None,
                info: BTreeMap::new(),
            })
        }
        async fn read_fresh(&self) -> anyhow::Result<Bucket> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // This boundary represents a successful remote validation whose
            // acknowledgement can be arbitrarily delayed.
            let snapshot = self.read().await?;
            self.validated.notify_one();
            self.acknowledge.notified().await;
            anyhow::ensure!(
                !self.fail.swap(false, Ordering::SeqCst),
                "lost barrier reply"
            );
            Ok(snapshot)
        }
        async fn replace(
            &self,
            _: &Bucket,
            _: BTreeMap<String, String>,
        ) -> anyhow::Result<Option<Bucket>> {
            unreachable!()
        }
        async fn delete_versions(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }
    async fn validated(authority: &Gated) {
        tokio::time::timeout(Duration::from_secs(5), authority.validated.notified())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn readers_after_remote_validation_cannot_join_its_delayed_reply() {
        let authority = Arc::new(Gated::default());
        let observations = Arc::new(Observations::new(authority.clone()));
        let first = tokio::spawn({
            let observations = observations.clone();
            async move { observations.read().await.unwrap() }
        });
        validated(&authority).await;
        authority.revision.store(1, Ordering::SeqCst); // Acknowledged write.
        let late = tokio::spawn({
            let observations = observations.clone();
            async move { observations.read().await.unwrap() }
        });
        authority.acknowledge.notify_one();
        assert_eq!(first.await.unwrap().revision, 0);
        validated(&authority).await;
        authority.acknowledge.notify_one();
        assert_eq!(late.await.unwrap().revision, 1);
        assert_eq!(authority.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn bounded_groups_share_failures_and_recover_after_cancelled_readers() {
        let authority = Arc::new(Gated::default());
        let observations = Observations::new(authority.clone());
        // Enqueue synchronously, so every member is admitted before sealing.
        let mut replies = Vec::new();
        for _ in 0..CAPACITY {
            let (reply, result) = oneshot::channel();
            observations.queue.try_send(reply).unwrap();
            replies.push(result);
        }
        assert!(observations.read().await.is_err()); // Full queue is bounded.
        replies.truncate(3); // Cancellations do not cancel surviving readers.
        validated(&authority).await;
        authority.fail.store(true, Ordering::SeqCst);
        authority.acknowledge.notify_one();
        for reply in replies {
            assert!(reply.await.unwrap().is_err());
        }
        assert_eq!(authority.calls.load(Ordering::SeqCst), 1);

        authority.revision.store(2, Ordering::SeqCst);
        let next = tokio::spawn(async move { observations.read().await.unwrap() });
        validated(&authority).await;
        authority.acknowledge.notify_one();
        assert_eq!(next.await.unwrap().revision, 2);
        assert_eq!(authority.calls.load(Ordering::SeqCst), 2);
    }
}
