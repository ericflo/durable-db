//! One local control request at a time; never hold permission state across I/O.
use super::*;
use std::{sync::Mutex, time::Duration};

pub struct Control<C> {
    root: Arc<Mutex<Root<C>>>,
    pub(super) request: Arc<tokio::sync::Mutex<()>>,
}
impl<C> Clone for Control<C> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            request: self.request.clone(),
        }
    }
}
impl<C: Clock> Control<C> {
    pub(super) fn new(root: Root<C>) -> Self {
        Self {
            root: Arc::new(Mutex::new(root)),
            request: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub(super) fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Root<C>>> {
        self.root
            .lock()
            .map_err(|_| anyhow::anyhow!("owner control retired"))
    }
    /// Cancelled/unknown requests are abandoned before a fresh exact validation.
    /// The old interval is never extended by the act of abandoning a request.
    pub async fn renew(&self) -> anyhow::Result<root::Outcome> {
        let _request = self.request.lock().await;
        self.renew_locked().await
    }
    // The request gate spans I/O, but permission checks take only the short
    // root mutex. Abandon can therefore affect only a cancelled prior request,
    // never another live caller's checkpoint or renewal.
    pub(super) async fn renew_locked(&self) -> anyhow::Result<root::Outcome> {
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
            self.lock()?.permission()? == Some(identity),
            "owner has no serving permission"
        );
        Ok(())
    }
}

/// Own this guard for as long as recovery/serving needs renewal. Dropping it
/// cancels the task; a remote operation may still apply and grants no local
/// permission until an exact confirmation. It never replaces an incarnation.
pub struct Renewal(tokio::task::JoinHandle<()>);
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl<C: Clock + Send + 'static> Control<C> {
    /// Keep at most one renewal in flight, including time waiting for another
    /// control operation. Timers only schedule work: request-start deadlines and
    /// final permission checks remain authoritative. Missed ticks never burst.
    /// Choose cadence/timeout for the qualified profile and provider latency;
    /// these settings cannot extend an expired grant or qualify a system clock.
    pub fn keep_alive(&self, period: Duration, timeout: Duration) -> anyhow::Result<Renewal> {
        let valid = Duration::from_millis(1)..=Duration::from_secs(3600);
        ensure!(
            valid.contains(&period) && valid.contains(&timeout),
            "renewal period and timeout must be 1 ms..1 hour"
        );
        let control = self.clone();
        Ok(Renewal(tokio::spawn(async move {
            let mut ticks = tokio::time::interval(period);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let _ = tokio::time::timeout(timeout, control.renew()).await;
            }
        })))
    }
}
