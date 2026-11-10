use super::*;
use std::{cell::Cell, rc::Rc};

#[derive(Clone)]
struct Manual(Rc<Cell<Option<Duration>>>);

impl Manual {
    fn at(nanos: u64) -> Self {
        Self(Rc::new(Cell::new(Some(Duration::from_nanos(nanos)))))
    }

    fn set(&self, nanos: u64) {
        self.0.set(Some(Duration::from_nanos(nanos)));
    }
}

impl Clock for Manual {
    fn read(&self) -> Option<Duration> {
        self.0.get()
    }
}

fn bounds(lifetime: u64, error: u64) -> Bounds {
    Bounds::new(Duration::from_nanos(lifetime), Duration::from_nanos(error)).unwrap()
}

#[test]
fn delayed_replies_never_restart_the_root_interval() {
    let clock = Manual::at(100);
    let mut time = Timeline::new(clock.clone(), bounds(12, 1)).unwrap();
    let request = time.begin().unwrap();
    clock.set(111);
    let _later_request = time.begin().unwrap();
    let deadline = time.root_deadline(&request).unwrap();
    assert_eq!(time.remaining(&deadline), Ok(Duration::from_nanos(1)));
    clock.set(112);
    assert_eq!(time.remaining(&deadline), Ok(Duration::ZERO));
    clock.set(200);
    let _later_request = time.begin().unwrap();
    let delayed = time.root_deadline(&request).unwrap();
    assert_eq!(time.remaining(&delayed), Ok(Duration::ZERO));
}

#[test]
fn child_reply_consumes_time_from_its_own_request_and_origin() {
    let parent_clock = Manual::at(500);
    let child_clock = Manual::at(1000);
    let profile = bounds(30, 2);
    let mut parent = Timeline::new(parent_clock.clone(), profile).unwrap();
    let mut child = Timeline::new(child_clock.clone(), profile).unwrap();
    let root_request = parent.begin().unwrap();
    let root = parent.root_deadline(&root_request).unwrap();
    let request = child.begin().unwrap();
    parent_clock.set(510);
    let duration = parent.child_lifetime(&root).unwrap();
    assert_eq!(duration, Duration::from_nanos(7));
    child_clock.set(1003);
    let _later_request = child.begin().unwrap();
    let deadline = child.child_deadline(&request, duration).unwrap();
    assert_eq!(child.remaining(&deadline), Ok(Duration::from_nanos(4)));
    child_clock.set(1007);
    assert_eq!(child.remaining(&deadline), Ok(Duration::ZERO));
    parent_clock.set(524);
    assert_eq!(parent.child_lifetime(&root), Ok(Duration::ZERO));
    let zero = child.child_deadline(&request, Duration::ZERO).unwrap();
    assert_eq!(child.remaining(&zero), Ok(Duration::ZERO));
}

#[test]
fn drain_starts_at_confirmation_and_requires_the_full_margin() {
    let clock = Manual::at(200);
    let profile = bounds(12, 1);
    let mut time = Timeline::new(clock.clone(), profile).unwrap();
    let _sent = time.begin().unwrap();
    clock.set(300); // Fence response arrives after an arbitrary delay.
    let drain = time.begin_drain().unwrap();
    assert_eq!(profile.drain_wait(), Duration::from_nanos(27));
    clock.set(326);
    assert_eq!(time.drain_complete(&drain), Ok(false));
    clock.set(327);
    assert_eq!(time.drain_complete(&drain), Ok(true));
}

#[test]
fn restart_cannot_reuse_requests_deadlines_or_drain_origins() {
    let clock = Manual::at(10);
    let profile = bounds(12, 1);
    let mut old = Timeline::new(clock.clone(), profile).unwrap();
    let request = old.begin().unwrap();
    let deadline = old.root_deadline(&request).unwrap();
    let drain = old.begin_drain().unwrap();
    drop(old); // Retained tokens keep the old identity allocation alive.
    // Even an identical numeric reading and bounds get a new process identity.
    let mut replacement = Timeline::new(clock, profile).unwrap();
    assert!(matches!(
        replacement.root_deadline(&request),
        Err(Error::ForeignTimeline)
    ));
    assert!(matches!(
        replacement.child_deadline(&request, Duration::ZERO),
        Err(Error::ForeignTimeline)
    ));
    assert_eq!(
        replacement.remaining(&deadline),
        Err(Error::ForeignTimeline)
    );
    assert_eq!(
        replacement.child_lifetime(&deadline),
        Err(Error::ForeignTimeline)
    );
    assert_eq!(
        replacement.drain_complete(&drain),
        Err(Error::ForeignTimeline)
    );
    let fresh = replacement.begin().unwrap();
    let fresh_deadline = replacement.root_deadline(&fresh).unwrap();
    assert_eq!(
        replacement.remaining(&fresh_deadline),
        Ok(profile.root_lifetime())
    );
}

#[test]
fn a_failed_or_backwards_clock_permanently_retires_the_timeline() {
    for bad in [None, Some(Duration::from_nanos(99)), Some(Duration::MAX)] {
        let clock = Manual::at(100);
        let mut time = Timeline::new(clock.clone(), bounds(12, 1)).unwrap();
        let request = time.begin().unwrap();
        let deadline = time.root_deadline(&request).unwrap();
        let drain = time.begin_drain().unwrap();
        clock.0.set(bad);
        assert_eq!(time.remaining(&deadline), Err(Error::Retired));
        clock.set(101); // Recovery of readings cannot revive this incarnation.
        assert!(matches!(time.begin(), Err(Error::Retired)));
        assert!(matches!(time.root_deadline(&request), Err(Error::Retired)));
        assert!(matches!(
            time.child_deadline(&request, Duration::ZERO),
            Err(Error::Retired)
        ));
        assert!(matches!(time.begin_drain(), Err(Error::Retired)));
        assert_eq!(time.drain_complete(&drain), Err(Error::Retired));
        assert_eq!(time.child_lifetime(&deadline), Err(Error::Retired));
    }
}

#[test]
fn invalid_profiles_and_absolute_overflow_fail_closed() {
    for (lifetime, error) in [
        (Duration::ZERO, Duration::ZERO),
        (Duration::MAX, Duration::ZERO),
        (Duration::from_nanos(1), Duration::MAX),
        (
            Duration::from_nanos(1),
            Duration::from_nanos(u64::MAX / 3 + 1),
        ),
        (Duration::from_nanos(u64::MAX / 2 + 1), Duration::ZERO),
        (Duration::from_nanos(u64::MAX / 2), Duration::from_nanos(1)),
    ] {
        assert_eq!(Bounds::new(lifetime, error), Err(Error::InvalidBounds));
    }
    assert_eq!(
        bounds(u64::MAX / 2, 0).root_lifetime().as_nanos(),
        u128::from(u64::MAX / 2)
    );
    let clock = Manual::at(u64::MAX - 2);
    let mut time = Timeline::new(clock.clone(), bounds(12, 1)).unwrap();
    let request = time.begin().unwrap();
    assert!(matches!(time.root_deadline(&request), Err(Error::Overflow)));
    assert!(matches!(time.begin_drain(), Err(Error::Overflow)));
    assert!(matches!(
        time.child_deadline(&request, Duration::from_nanos(4)),
        Err(Error::Overflow)
    ));
    assert!(matches!(
        time.child_deadline(&request, Duration::from_nanos(5)),
        Err(Error::InvalidChildLifetime)
    ));
    assert!(matches!(
        time.child_deadline(&request, Duration::MAX),
        Err(Error::InvalidChildLifetime)
    ));
    let exact_end = time
        .child_deadline(&request, Duration::from_nanos(2))
        .unwrap();
    assert_eq!(time.remaining(&exact_end), Ok(Duration::from_nanos(2)));
    clock.set(u64::MAX);
    assert_eq!(time.remaining(&exact_end), Ok(Duration::ZERO));
}

#[test]
fn independent_fast_drain_and_slow_old_clock_exclude_old_time() {
    // Enumerate the extremal readings allowed by the model's elapsed-error
    // contract. Origins differ; the old grant began no later than the drain.
    for lifetime in 1..=12 {
        for error in 0..=3 {
            let old_clock = Manual::at(100);
            let new_clock = Manual::at(1000);
            let profile = bounds(lifetime, error);
            let mut old = Timeline::new(old_clock.clone(), profile).unwrap();
            let mut new = Timeline::new(new_clock.clone(), profile).unwrap();
            let start = old.begin().unwrap();
            let permission = old.root_deadline(&start).unwrap();
            let drain = new.begin_drain().unwrap();
            for elapsed in 0u64..=lifetime + 2 * error + 1 {
                old_clock.set(100 + elapsed.saturating_sub(error));
                new_clock.set(1000 + 2 * elapsed + error);
                if new.drain_complete(&drain).unwrap() {
                    assert_eq!(old.remaining(&permission).unwrap(), Duration::ZERO);
                }
            }
            assert!(new.drain_complete(&drain).unwrap());
        }
    }
}

#[test]
fn independently_delayed_child_never_outlives_its_fast_parent() {
    // Child request precedes parent issuance. Enumerate independent delays and
    // extremal allowed readings, checking containment rather than the formula.
    for lifetime in 1..=12 {
        for error in 0..=3 {
            for since_issue in 0..=lifetime + error {
                for since_request in since_issue..=lifetime + error {
                    let parent_clock = Manual::at(10);
                    let child_clock = Manual::at(100);
                    let profile = bounds(lifetime, error);
                    let mut parent = Timeline::new(parent_clock.clone(), profile).unwrap();
                    let mut child = Timeline::new(child_clock.clone(), profile).unwrap();
                    let parent_start = parent.begin().unwrap();
                    let root = parent.root_deadline(&parent_start).unwrap();
                    let child_start = child.begin().unwrap();
                    let duration = parent.child_lifetime(&root).unwrap();
                    let child_deadline = child.child_deadline(&child_start, duration).unwrap();
                    parent_clock.set(10 + 2 * since_issue + error);
                    child_clock.set(100 + since_request.saturating_sub(error));
                    if !child.remaining(&child_deadline).unwrap().is_zero() {
                        assert!(!parent.remaining(&root).unwrap().is_zero());
                    }
                }
            }
        }
    }
}

#[test]
fn measured_guest_suspend_can_finish_a_drain_while_the_old_owner_is_live() {
    // Required unsafe-platform witness from the QEMU short-suspend experiment:
    // 120.2445 ms host elapsed, but guest boottime advanced 335.391747 ms.
    // No timing arithmetic can grant the platform assumption from this sample.
    let old_clock = Manual::at(0);
    let new_clock = Manual::at(0);
    let profile = bounds(200_000_000, 1_000_000);
    let mut old = Timeline::new(old_clock.clone(), profile).unwrap();
    let mut new = Timeline::new(new_clock.clone(), profile).unwrap();
    let request = old.begin().unwrap();
    let deadline = old.root_deadline(&request).unwrap();
    let drain = new.begin_drain().unwrap();
    old_clock.set(120_244_500 * 3 / 2); // Nominal valid independent host clock.
    new_clock.set(335_391_747 * 3 / 2); // Measured invalid guest reading.
    assert!(new.drain_complete(&drain).unwrap());
    assert!(!old.remaining(&deadline).unwrap().is_zero());
}
