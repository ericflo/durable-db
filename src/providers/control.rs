//! Conditional control requests with explicit uncertainty and invocation identity.
//!
//! A confirmed observation names one successful comparison, not ongoing serving
//! permission. The owner must separately check its root interval, invalidate
//! affected placements before a freeze, and complete their drains. This module
//! implements the request-handling subset of `PlacementRequests`, not a controller.
use crate::publication::{Authority, Bucket};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

const MAX_FIELDS: usize = 64;
const MAX_METADATA_BYTES: usize = 8192;
// The common register adds a fresh mutation identity before sending a change.
const MUTATION_HEADROOM: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("a control request is already active")]
    Busy,
    #[error("observation belongs to another control session")]
    ForeignObservation,
    #[error("control metadata exceeds its field or byte bound")]
    TooLarge,
    #[error("control invocation sequence exhausted")]
    Exhausted,
}

/// A provider observation bound to one session. Reading it does not validate it.
/// Deliberately has no Debug implementation: metadata is not safe to log.
#[derive(Clone)]
pub struct Observation {
    session: Uuid,
    bucket: Bucket,
}

impl Observation {
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.bucket.info
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Invocation {
    session: Uuid,
    sequence: u64,
}

/// One local control caller. Only its current invocation can confirm a result.
/// Previously abandoned operations may still apply at the provider.
/// Metadata is limited to 64 fields and 8 KiB of encoded JSON. This bounds
/// control metadata, not prepared requests retained by a caller or process RSS.
pub struct Session {
    authority: Arc<dyn Authority>,
    id: Uuid,
    sequence: u64,
    active: Option<Invocation>,
    confirmed: Option<Observation>,
}

impl Session {
    // Connections always supply the common nonce-stamping register wrapper.
    pub(super) fn new(authority: Arc<dyn Authority>) -> Self {
        Self {
            authority,
            id: Uuid::new_v4(),
            sequence: 0,
            active: None,
            confirmed: None,
        }
    }

    /// A raw observation may be stale. It never changes `confirmed()`.
    pub async fn observe(&self) -> anyhow::Result<Observation> {
        let bucket = self.authority.read().await?;
        metadata_bound(&bucket.info, MAX_METADATA_BYTES)?;
        Ok(Observation {
            session: self.id,
            bucket,
        })
    }

    /// The last current-invocation success, cleared before every new request.
    /// Other coordinators can change the register after this comparison.
    pub fn confirmed(&self) -> Option<&Observation> {
        self.confirmed.as_ref()
    }

    /// Reserve an invocation before any network send. Errors leave the session
    /// unchanged. Each prepared request owns its captured comparison and payload.
    pub fn begin(
        &mut self,
        observed: Observation,
        metadata: BTreeMap<String, String>,
    ) -> Result<Prepared, Error> {
        if self.active.is_some() {
            return Err(Error::Busy);
        }
        if observed.session != self.id {
            return Err(Error::ForeignObservation);
        }
        if metadata.len() == MAX_FIELDS && !metadata.contains_key("durable-db-probe") {
            return Err(Error::TooLarge);
        }
        metadata_bound(&metadata, MAX_METADATA_BYTES - MUTATION_HEADROOM)?;
        let sequence = self.sequence.checked_add(1).ok_or(Error::Exhausted)?;
        let invocation = Invocation {
            session: self.id,
            sequence,
        };
        self.sequence = sequence;
        self.confirmed = None;
        self.active = Some(invocation);
        Ok(Prepared {
            authority: self.authority.clone(),
            invocation,
            observed: observed.bucket,
            metadata,
        })
    }

    /// Forget local interest, not the provider operation. It may still apply.
    /// A subsequent exact comparison must establish any new confirmed view.
    pub fn abandon(&mut self) {
        self.active = None;
        self.confirmed = None;
    }

    pub fn finish(&mut self, completed: Completed) -> Outcome {
        if self.active != Some(completed.invocation) {
            return Outcome::Ignored;
        }
        self.active = None;
        match completed.result {
            Ok(Some(bucket)) => {
                if let Err(error) = metadata_bound(&bucket.info, MAX_METADATA_BYTES) {
                    return Outcome::Uncertain(error.into());
                }
                self.confirmed = Some(Observation {
                    session: self.id,
                    bucket,
                });
                Outcome::Confirmed
            }
            Ok(None) => Outcome::Conflict,
            Err(error) => Outcome::Uncertain(error),
        }
    }
}

/// A single network invocation. Not cloneable; execution consumes the request.
#[must_use = "execute the request or explicitly abandon the session"]
pub struct Prepared {
    authority: Arc<dyn Authority>,
    invocation: Invocation,
    observed: Bucket,
    metadata: BTreeMap<String, String>,
}

impl Prepared {
    /// Makes one register call with no retry. If this future is dropped, the
    /// caller must abandon its session invocation; remote completion is unknown.
    pub async fn execute(self) -> Completed {
        Completed {
            invocation: self.invocation,
            result: self.authority.replace(&self.observed, self.metadata).await,
        }
    }
}

/// Only execution can produce a completion; it remains bound to its invocation.
#[must_use = "finish the invocation or explicitly abandon the session"]
pub struct Completed {
    invocation: Invocation,
    result: anyhow::Result<Option<Bucket>>,
}

/// Conflict describes this invocation only, not an earlier uncertain operation.
#[must_use = "uncertain outcomes require revalidation; they are not aborts"]
pub enum Outcome {
    Confirmed,
    Conflict,
    Uncertain(anyhow::Error),
    Ignored,
}

fn metadata_bound(metadata: &BTreeMap<String, String>, limit: usize) -> Result<(), Error> {
    // Bound serialization's temporary allocation before encoding JSON escapes.
    if metadata.len() > MAX_FIELDS
        || metadata
            .iter()
            .try_fold(0usize, |size, (key, value)| {
                size.checked_add(key.len())?.checked_add(value.len())
            })
            .is_none_or(|size| size > limit)
        || serde_json::to_vec(metadata)
            .map_err(|_| Error::TooLarge)?
            .len()
            > limit
    {
        return Err(Error::TooLarge);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
