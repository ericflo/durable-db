//! One local control request at a time; never hold permission state across I/O.
use super::*;
use std::sync::Mutex;

pub struct Control {
    root: Arc<Mutex<Root>>,
    pub(super) request: Arc<tokio::sync::Mutex<()>>,
}
impl Clone for Control {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            request: self.request.clone(),
        }
    }
}
impl Control {
    pub(super) fn new(root: Root) -> Self {
        Self {
            root: Arc::new(Mutex::new(root)),
            request: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub(super) fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Root>> {
        self.root
            .lock()
            .map_err(|_| anyhow::anyhow!("owner control retired"))
    }
    /// Cancelled/unknown requests are abandoned before a fresh exact validation.
    /// This confirms metadata only; it does not grant response permission.
    pub async fn refresh(&self) -> anyhow::Result<root::Outcome> {
        let _request = self.request.lock().await;
        self.refresh_locked().await
    }
    /// Administrator-only namespace backup slot. Validate and serialize with
    /// control/checkpoint publication; never hold permission state across I/O.
    /// A returned identity remains tentative unless the outcome is Confirmed.
    pub async fn retain_checkpoint(
        &self,
    ) -> anyhow::Result<(root::RetainedCheckpoint, root::Outcome)> {
        let _request = self.request.lock().await;
        self.confirm_locked().await?;
        let (retained, prepared) = self.lock()?.retain_checkpoint()?;
        let completed = prepared.execute().await;
        Ok((retained, self.lock()?.finish(completed)?))
    }
    /// Confirm the durable slot after restart or an ambiguous retention reply.
    /// An administrator must coordinate release with all backup readers.
    pub async fn retained_checkpoint(&self) -> anyhow::Result<Option<root::RetainedCheckpoint>> {
        let _request = self.request.lock().await;
        self.confirm_locked().await?;
        Ok(self.lock()?.retained_checkpoint()?)
    }
    /// Release only the specified identity, or confirm that the slot is empty.
    /// Unknown replies require revalidation; a different retained identity is
    /// never removed by a retry of an older release.
    pub async fn release_checkpoint(&self, id: uuid::Uuid) -> anyhow::Result<root::Outcome> {
        let _request = self.request.lock().await;
        self.confirm_locked().await?;
        let prepared = self.lock()?.release_checkpoint(id)?;
        let completed = prepared.execute().await;
        Ok(self.lock()?.finish(completed)?)
    }
    async fn confirm_locked(&self) -> anyhow::Result<()> {
        ensure!(
            matches!(self.refresh_locked().await?, root::Outcome::Confirmed),
            "ownership validation did not confirm; retry from a fresh observation"
        );
        Ok(())
    }
    // The request gate spans I/O, but permission checks take only the short
    // root mutex. Abandon can therefore affect only a cancelled prior request,
    // never another live caller's checkpoint or update.
    pub(super) async fn refresh_locked(&self) -> anyhow::Result<root::Outcome> {
        let observation = self.lock()?.observe();
        let observed = observation.await?;
        let prepared = {
            let mut root = self.lock()?;
            root.abandon();
            root.validate(observed)?
        };
        let completed = prepared.execute().await;
        Ok(self.lock()?.finish(completed)?)
    }
    pub(super) fn check(&self, identity: root::Identity) -> anyhow::Result<()> {
        ensure!(
            self.lock()?.identity()? == Some(identity),
            "owner has no serving permission"
        );
        Ok(())
    }
    /// The caller has already sealed the complete result, including errors.
    pub(super) async fn validate(&self, identity: root::Identity) -> anyhow::Result<()> {
        let permission = self.lock()?.permission();
        if permission.await? != Some(identity) {
            self.lock()?.retire();
            anyhow::bail!("owner permit is absent");
        }
        Ok(())
    }
}
