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
    ) -> Result<bool, Error> {
        self.available()?;
        if self.epoch.is_some() {
            return Err(Error::Bound);
        }
        let mut record = observed.record.ok_or(Error::NotOwner)?;
        if record.identity.incarnation != incarnation {
            return Err(Error::NotOwner);
        }
        record.address = None;
        record.challenge = None;
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
    /// its placement slot is retired. The callback decides whether to answer an
    /// actual pending challenge; neither its decision nor the reply grants authority.
    pub(crate) fn answer_challenges<F>(
        &self,
        mut wanted: F,
    ) -> impl Future<Output = ()> + Send + use<F>
    where
        F: FnMut() -> bool + Send,
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
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let answer = async {
                    let observed = authority.read().await?;
                    let Some(mut record) =
                        metadata::decode(&observed.info, &prefix, encoding, budgeted)?
                    else {
                        return Ok::<_, anyhow::Error>(());
                    };
                    if record.identity == identity && record.challenge.is_some() && wanted() {
                        record.challenge = None;
                        let info =
                            metadata::encode(&observed.info, &prefix, encoding, record, budgeted)?;
                        // Lost replies and conflicts are harmless. A subsequent
                        // pass re-observes; it never replays an old comparison.
                        authority.replace(&observed, info).await?;
                    }
                    Ok(())
                };
                let _ = answer.await;
            }
        }
    }
}
