//! Advisory failure detection through storage when peer networking disagrees.
//! An answer delays takeover; neither an answer nor a timeout grants authority.
use super::*;
use std::time::Duration;

impl Observation {
    /// Also includes an owner still activating or recovering.
    pub(crate) fn incumbent(&self) -> Option<Route> {
        let record = self.record?;
        Some(Route {
            identity: record.identity,
            address: record.address?,
        })
    }
    pub(crate) fn challenge(&self) -> Option<Uuid> {
        self.record.and_then(|record| record.challenge)
    }
    /// A withdrawn route uses the owner-bound hint to name the departing member.
    /// Other members wait before falling back; a replacement of that member may
    /// proceed directly. Neither case establishes ownership or serving permission.
    pub(crate) fn withdrawn_member(&self) -> Option<(Identity, Uuid)> {
        let record = self.record?;
        (record.address.is_none() && record.phase == Phase::Open)
            .then_some((record.identity, record.challenge?))
    }
}
impl Root {
    /// Join one owner-bound challenge. An uncertain publication never permits
    /// takeover. This raw session mutation cannot activate the foreign owner.
    pub(crate) async fn challenge(&mut self, observed: Observation) -> Result<Option<Uuid>, Error> {
        self.available()?;
        if self.epoch.is_some() {
            return Err(Error::Bound);
        }
        let mut record = observed.record.ok_or(Error::NotOwner)?;
        if let Some(nonce) = record.challenge {
            return Ok(Some(nonce));
        }
        let nonce = Uuid::new_v4();
        record.challenge = Some(nonce);
        Ok(self.publish_hint(observed, record).await?.then_some(nonce))
    }

    /// Withdraw only this retiring incarnation's route. Exact comparison keeps
    /// a delayed or uncertain shutdown hint from clearing a successor's route.
    /// Permission and selected history are unchanged; takeover still fences.
    pub(crate) async fn withdraw_route(
        &mut self,
        observed: Observation,
        incarnation: Uuid,
        member: Uuid,
    ) -> Result<bool, Error> {
        self.available()?;
        if member.is_nil() {
            return Err(Error::Incompatible);
        }
        if self.epoch.is_some() {
            return Err(Error::Bound);
        }
        let mut record = observed.record.ok_or(Error::NotOwner)?;
        if record.identity.incarnation != incarnation {
            return Err(Error::NotOwner);
        }
        record.address = None;
        record.challenge = Some(member);
        self.publish_hint(observed, record).await
    }

    // Hint publication never confirms or activates the foreign incarnation.
    async fn publish_hint(&mut self, observed: Observation, record: Record) -> Result<bool, Error> {
        let metadata = metadata::encode(
            observed.raw.metadata(),
            &self.prefix,
            self.encoding,
            record,
            self.budgeted,
        )?;
        self.session.abandon();
        let request = self.session.begin(observed.raw, metadata)?;
        Ok(matches!(
            self.session.finish(request.execute().await),
            control::Outcome::Confirmed
        ))
    }

    /// Bounded background work for this incarnation, from confirmed claim until
    /// its placement slot is retired. A yielding incumbent stops answering but
    /// can still serve until revoked. The callback is only a scheduling hint.
    pub(crate) fn answer_challenges<F>(&self, wanted: F) -> impl Future<Output = ()> + Send + use<F>
    where
        F: Fn() -> bool + Send,
    {
        let identity = self.epoch.map(|epoch| Identity {
            epoch,
            incarnation: self.incarnation,
        });
        let authority = self.session.authority();
        let prefix = self.prefix.clone();
        let encoding = self.encoding;
        let budgeted = self.budgeted;
        async move {
            let Some(identity) = identity else {
                return;
            };
            let interval = Duration::from_secs(3);
            let mut poll =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut answering = None;
            loop {
                // Provider latency consumes this period, not an additional sleep.
                // Missed ticks never queue overlapping observations or answers.
                poll.tick().await;
                let eligible = wanted();
                if answering != Some(eligible) {
                    tracing::info!(owner = %identity.incarnation, eligible,
                        "native owner challenge eligibility");
                    answering = Some(eligible);
                }
                if !eligible {
                    continue;
                }
                let started = tokio::time::Instant::now();
                let mut stage = "observe";
                let answer = async {
                    let observed = authority.read().await?;
                    let Some(mut record) =
                        metadata::decode(&observed.info, &prefix, encoding, budgeted)?
                    else {
                        return Ok::<_, anyhow::Error>("idle");
                    };
                    if record.identity == identity && record.challenge.is_some() {
                        record.challenge = None;
                        let info =
                            metadata::encode(&observed.info, &prefix, encoding, record, budgeted)?;
                        // Lost replies and conflicts are harmless. A subsequent
                        // pass re-observes; it never replays an old comparison.
                        stage = "answer";
                        return Ok(if authority.replace(&observed, info).await?.is_some() {
                            "confirmed"
                        } else {
                            "conflict"
                        });
                    }
                    Ok("idle")
                };
                let outcome = answer.await.unwrap_or("error");
                if outcome != "idle" {
                    tracing::info!(owner = %identity.incarnation, stage, outcome,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "native owner challenge reply");
                }
            }
        }
    }
}
