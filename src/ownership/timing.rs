//! Checked intervals for the candidate `GrantBounds` timing contract.
//!
//! For elapsed time t in the model's normalized reference units, the supplied
//! clock must advance between t - E and
//! 2t + E, without moving backwards. E covers the complete elapsed measurement,
//! including resolution and sampling error. This module does not establish that
//! contract or select an operating-system clock. In particular, a clock stopped
//! by VM suspension or restored with process memory cannot satisfy the contract.
//!
//! All wire participants must use the same bounds and units. Controllers must
//! authenticate that profile and bind grants to current incarnations/placements.
//! These intervals alone never authorize reads, grants or acknowledgements.
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("the root lifetime must be positive and all intervals must fit in u64 nanoseconds")]
    InvalidBounds,
    #[error("clock unavailable, out of range or moved backwards; this timeline is retired")]
    Retired,
    #[error("timestamp belongs to another process timeline")]
    ForeignTimeline,
    #[error("deadline exceeds the clock's representable range")]
    Overflow,
    #[error("child lifetime exceeds the timing profile's maximum")]
    InvalidChildLifetime,
}

/// Cluster-wide timing assumptions. No production values are selected here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    lifetime: u64,
    margin: u64,
    drain: u64,
}

impl Bounds {
    pub fn new(lifetime: Duration, error: Duration) -> Result<Self, Error> {
        let lifetime = nanos(lifetime).ok_or(Error::InvalidBounds)?;
        let margin = nanos(error)
            .and_then(|n| n.checked_mul(3))
            .ok_or(Error::InvalidBounds)?;
        let drain = lifetime
            .checked_mul(2)
            .and_then(|n| n.checked_add(margin))
            .filter(|_| lifetime > 0)
            .ok_or(Error::InvalidBounds)?;
        Ok(Self {
            lifetime,
            margin,
            drain,
        })
    }

    pub fn root_lifetime(self) -> Duration {
        Duration::from_nanos(self.lifetime)
    }

    pub fn drain_wait(self) -> Duration {
        Duration::from_nanos(self.drain)
    }

    fn child_nanos(self, remaining: u64) -> u64 {
        remaining.saturating_sub(self.margin) / 2
    }
}

/// Cluster intervals require the continuously advancing clock contract above.
/// The private exclusive-local adapter instead relies on its retained OS lock
/// and uses logical startup progression; it cannot supply cluster permission.
/// No system clock implementation is provided until a platform is qualified.
pub trait Clock {
    /// None means the clock could not be read. Values share one local origin.
    fn read(&self) -> Option<Duration>;
}

/// Captured before sending an invocation. Never serialized or restored.
#[derive(Clone)]
pub struct Start {
    timeline: Arc<()>,
    at: u64,
}

/// An expiry bound, not proof of ownership or permission to serve.
#[derive(Clone)]
pub struct Deadline {
    start: Start,
    end: u64,
}

/// A drain begins only after the controller confirms the relevant fence.
/// It cannot be substituted with the shorter root or child deadline.
pub struct Drain(Deadline);

/// One process-clock origin. A failed or backwards reading permanently retires
/// it. A new timeline needs a new ownership incarnation and fresh recovery.
/// Copying/restoring live process memory is outside the timing contract.
pub struct Timeline<C> {
    clock: C,
    bounds: Bounds,
    // Retained starts keep the identity allocation alive, preventing reuse.
    id: Arc<()>,
    last: u64,
    retired: bool,
}

impl<C: Clock> Timeline<C> {
    pub fn new(clock: C, bounds: Bounds) -> Result<Self, Error> {
        let mut timeline = Self {
            clock,
            bounds,
            id: Arc::new(()),
            last: 0,
            retired: false,
        };
        timeline.sample()?;
        Ok(timeline)
    }

    pub fn begin(&mut self) -> Result<Start, Error> {
        let at = self.sample()?;
        Ok(Start {
            timeline: self.id.clone(),
            at,
        })
    }

    /// Compute the root expiry from invocation start, never from its reply.
    /// Only the matching successful control invocation can earn this interval;
    /// an expired reply earns no usable time. Check again before each use.
    pub fn root_deadline(&self, start: &Start) -> Result<Deadline, Error> {
        self.deadline(start, self.bounds.lifetime)
    }

    /// A child's duration was issued from its parent's remaining interval.
    /// Network delay consumes that duration; it never restarts it on delivery.
    pub fn child_deadline(&self, start: &Start, duration: Duration) -> Result<Deadline, Error> {
        let duration = nanos(duration).ok_or(Error::InvalidChildLifetime)?;
        if duration > self.bounds.child_nanos(self.bounds.lifetime) {
            return Err(Error::InvalidChildLifetime);
        }
        self.deadline(start, duration)
    }

    pub fn begin_drain(&mut self) -> Result<Drain, Error> {
        let start = self.begin()?;
        Ok(Drain(self.deadline(&start, self.bounds.drain)?))
    }

    pub fn drain_complete(&mut self, drain: &Drain) -> Result<bool, Error> {
        self.check(&drain.0.start)?;
        Ok(self.sample()? >= drain.0.end)
    }

    /// Samples the clock now. A zero remaining interval allows no service.
    /// The caller must have finished preparing its consistent view before this
    /// check, and must separately check incarnation, placement and durability.
    pub fn remaining(&mut self, deadline: &Deadline) -> Result<Duration, Error> {
        self.remaining_nanos(deadline).map(Duration::from_nanos)
    }

    pub fn child_lifetime(&mut self, parent: &Deadline) -> Result<Duration, Error> {
        let remaining = self.remaining_nanos(parent)?;
        Ok(Duration::from_nanos(self.bounds.child_nanos(remaining)))
    }

    fn remaining_nanos(&mut self, deadline: &Deadline) -> Result<u64, Error> {
        self.check(&deadline.start)?;
        Ok(deadline.end.saturating_sub(self.sample()?))
    }

    fn deadline(&self, start: &Start, duration: u64) -> Result<Deadline, Error> {
        self.check(start)?;
        let end = start.at.checked_add(duration).ok_or(Error::Overflow)?;
        Ok(Deadline {
            start: start.clone(),
            end,
        })
    }

    fn check(&self, start: &Start) -> Result<(), Error> {
        if self.retired {
            return Err(Error::Retired);
        }
        if !Arc::ptr_eq(&start.timeline, &self.id) {
            return Err(Error::ForeignTimeline);
        }
        Ok(())
    }

    fn sample(&mut self) -> Result<u64, Error> {
        if !self.retired
            && let Some(now) = self
                .clock
                .read()
                .and_then(nanos)
                .filter(|n| *n >= self.last)
        {
            self.last = now;
            return Ok(now);
        }
        self.retired = true;
        Err(Error::Retired)
    }
}

fn nanos(duration: Duration) -> Option<u64> {
    duration.as_nanos().try_into().ok()
}

#[cfg(test)]
mod tests;
