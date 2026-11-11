//! Disposable VM probe, compiled only by check-clockbound.py. No production binding.
//! A synthetic V3 calibration isolates counter/disruption behavior from NTP.
//! The 100 ms rate estimate and 1 ms initial bound are experimental inputs,
//! not a calibration guarantee. No daemon runs or changes any system clock.
use clock_bound::{
    Client, ClockStatus,
    nix::sys::time::{TimeSpec, TimeValLike},
    test_utils::{ClockErrorBound, ClockErrorBoundGeneric, ClockErrorBoundLayoutVersion},
};
use std::{
    fs,
    fs::File,
    io::{self, Write},
    os::unix::fs::FileExt,
    time::{Duration, Instant},
};
fn tsc() -> u64 {
    // SAFETY: this probe runs only on the Linux x86_64 VM fixture (SSE2).
    unsafe {
        core::arch::x86_64::_mm_lfence();
        let n = core::arch::x86_64::_rdtsc();
        core::arch::x86_64::_mm_lfence();
        n
    }
}
fn marker() -> u64 {
    let mut h = [0u8; 32];
    File::open("/dev/vmclock0")
        .unwrap()
        .read_exact_at(&mut h, 0)
        .unwrap();
    assert_eq!(&h[..4], b"VCLK");
    u64::from_le_bytes(h[16..24].try_into().unwrap())
}
fn make(path: &str, counter: u64, period: f64, guard: bool) -> Client {
    let ceb = ClockErrorBoundGeneric::builder()
        .as_of_tsc(counter)
        .as_of(TimeSpec::seconds(1000))
        .void_after(TimeSpec::seconds(1030))
        .period(period)
        .period_err(0.001)
        .bound_nsec(1_000_000)
        .disruption_marker(marker())
        .clock_status(ClockStatus::Synchronized)
        .clock_disruption_support_enabled(guard)
        .build(ClockErrorBoundLayoutVersion::V3);
    let ClockErrorBound::V3(body) = ceb else {
        unreachable!()
    };
    // Pinned upstream repr(C) has explicit initialized padding on Linux x86_64.
    let body = unsafe {
        std::slice::from_raw_parts(
            std::ptr::from_ref(&body).cast::<u8>(),
            std::mem::size_of_val(&body),
        )
    };
    assert_eq!(body.len(), 88, "pinned V3 layout changed");
    let mut bytes = Vec::new();
    for n in [0x414D5A4Eu32, 0x43420200, (16 + body.len()) as u32] {
        bytes.extend_from_slice(&n.to_ne_bytes());
    }
    bytes.extend_from_slice(&0x303u16.to_ne_bytes());
    bytes.extend_from_slice(&2u16.to_ne_bytes());
    bytes.extend_from_slice(body);
    fs::write(path, bytes).unwrap();
    Client::new_with_paths(path, "/dev/vmclock0").unwrap()
}
fn main() {
    let mut clients: Option<(Client, Client)> = None;
    let mut previous = 0;
    println!("READY");
    io::stdout().flush().unwrap();
    for line in io::stdin().lines() {
        let line = line.unwrap();
        if line == "reset" {
            drop(clients.take());
            let start = Instant::now();
            let a = tsc();
            std::thread::sleep(Duration::from_millis(100));
            let b = tsc();
            let period = start.elapsed().as_secs_f64() / ((b - a) as f64);
            clients = Some((
                make("/tmp/guarded", b, period, true),
                make("/tmp/unsafe", b, period, false),
            ));
        } else if line == "suspend" {
            fs::write("/sys/power/mem_sleep", "deep").unwrap();
            fs::write("/sys/power/state", "mem").unwrap();
        }
        let (g, _) = clients.as_mut().unwrap();
        let mut g = g.now().unwrap();
        if line == "suspend" {
            // Delay this owner's first post-resume sample until it is not
            // backwards. Intermediate samples are diagnostic only; a paused
            // owner need not run soon enough to observe the counter reset.
            let deadline = Instant::now() + Duration::from_secs(40);
            while (g.earliest.num_nanoseconds() + g.latest.num_nanoseconds()) / 2 < previous {
                assert!(Instant::now() < deadline, "counter did not catch up");
                std::thread::sleep(Duration::from_millis(1));
                g = clients.as_mut().unwrap().0.now().unwrap();
            }
        }
        let u = clients.as_mut().unwrap().1.now().unwrap();
        previous = (g.earliest.num_nanoseconds() + g.latest.num_nanoseconds()) / 2;
        println!(
            "SAMPLE {} {} {:?} {} {} {:?} {}",
            g.earliest.num_nanoseconds(),
            g.latest.num_nanoseconds(),
            g.clock_status,
            u.earliest.num_nanoseconds(),
            u.latest.num_nanoseconds(),
            u.clock_status,
            marker()
        );
        io::stdout().flush().unwrap();
    }
}
