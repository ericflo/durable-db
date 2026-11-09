use crate::{Error, Result};
use std::time::Duration;
use tokio::time::Instant;

const UNIT: u64 = 1_000_000_000;
const MAX_UNITS: u32 = 1_000_000;
pub(super) const COMMIT_COST: u32 = 5;

/// Per-principal, per-process overload protection. Commits cost five units;
/// other authenticated requests cost one. This is not a durable tenant quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimits {
    units_per_second: u32,
    burst_units: u32,
}

impl RateLimits {
    pub const DEFAULT_UNITS_PER_SECOND: u32 = 20;
    pub const DEFAULT_BURST_UNITS: u32 = 40;

    pub fn new(units_per_second: u32, burst_units: u32) -> Result<Self> {
        if !(1..=MAX_UNITS).contains(&units_per_second)
            || !(COMMIT_COST..=MAX_UNITS).contains(&burst_units)
        {
            return Err(Error::Invalid(format!(
                "request budget requires 1..={MAX_UNITS} units per second and {COMMIT_COST}..={MAX_UNITS} burst units"
            )));
        }
        Ok(Self {
            units_per_second,
            burst_units,
        })
    }

    pub fn units_per_second(self) -> u32 {
        self.units_per_second
    }

    pub fn burst_units(self) -> u32 {
        self.burst_units
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            units_per_second: Self::DEFAULT_UNITS_PER_SECOND,
            burst_units: Self::DEFAULT_BURST_UNITS,
        }
    }
}

pub(super) struct Budget {
    credit: u64,
    stamp: Instant,
    limits: RateLimits,
}

impl Budget {
    pub fn new(now: Instant, limits: RateLimits) -> Self {
        Self {
            credit: u64::from(limits.burst_units) * UNIT,
            stamp: now,
            limits,
        }
    }

    fn refilled(&self, now: Instant) -> u64 {
        let earned = now
            .saturating_duration_since(self.stamp)
            .as_nanos()
            .saturating_mul(u128::from(self.limits.units_per_second));
        (u128::from(self.credit).saturating_add(earned))
            .min(u128::from(self.limits.burst_units) * u128::from(UNIT)) as u64
    }

    /// The caller samples time inside the admission lock. Keep a high-water
    /// mark as well: an older reading must never credit the same interval twice.
    pub fn take(&mut self, now: Instant, units: u32) -> bool {
        self.credit = self.refilled(now);
        self.stamp = self.stamp.max(now);
        let cost = u64::from(units) * UNIT;
        if self.credit < cost {
            return false;
        }
        self.credit -= cost;
        true
    }

    pub fn can_evict(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.stamp) >= Duration::from_secs(60)
            && self.refilled(now) == u64::from(self.limits.burst_units) * UNIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_remain_positive_bounded_and_allow_a_commit() {
        for (rate, burst) in [(0, 5), (1, 4), (MAX_UNITS + 1, 5), (1, MAX_UNITS + 1)] {
            assert!(RateLimits::new(rate, burst).is_err());
        }
        assert!(RateLimits::new(1, 5).is_ok());
        assert!(RateLimits::new(MAX_UNITS, MAX_UNITS).is_ok());
        assert_eq!(RateLimits::default(), RateLimits::new(20, 40).unwrap());
    }

    #[test]
    fn fractional_refill_preserves_credit_without_rounding_up() {
        let start = Instant::now();
        let mut budget = Budget::new(start, RateLimits::new(3, 5).unwrap());
        assert!(budget.take(start, 5));
        assert!(!budget.take(start + Duration::from_nanos(333_333_333), 1));
        assert!(budget.take(start + Duration::from_nanos(333_333_334), 1));
        assert!(budget.take(start + Duration::from_secs(1), 2));
        assert!(!budget.take(start + Duration::from_secs(1), 1));
    }

    #[test]
    fn backwards_samples_do_not_credit_an_interval_twice() {
        let start = Instant::now();
        let mut budget = Budget::new(start, RateLimits::new(1, 5).unwrap());
        assert!(budget.take(start, 5));
        assert!(!budget.take(start + Duration::from_secs(2), 5));
        assert!(budget.take(start + Duration::from_secs(1), 1));
        assert!(budget.take(start + Duration::from_secs(2), 1));
        assert!(!budget.take(start + Duration::from_secs(2), 1));
    }

    #[test]
    fn idle_eviction_cannot_recreate_a_depleted_burst() {
        let start = Instant::now();
        let mut budget = Budget::new(start, RateLimits::new(1, 100).unwrap());
        assert!(budget.take(start, 100));
        assert!(!budget.can_evict(start + Duration::from_secs(60)));
        assert!(!budget.can_evict(start + Duration::from_secs(99)));
        assert!(budget.can_evict(start + Duration::from_secs(100)));
        let full = Budget::new(start, RateLimits::default());
        assert!(!full.can_evict(start + Duration::from_secs(59)));
        assert!(full.can_evict(start + Duration::from_secs(60)));
    }

    #[test]
    fn a_long_idle_period_never_accumulates_more_than_one_burst() {
        let start = Instant::now();
        let mut budget = Budget::new(start, RateLimits::new(MAX_UNITS, MAX_UNITS).unwrap());
        assert!(budget.take(start, MAX_UNITS));
        let later = start + Duration::from_secs(100_000);
        assert!(budget.take(later, MAX_UNITS));
        assert!(!budget.take(later, 1));
    }
}
