//! Test-only binding. VMClock detects the tested QEMU restoration; it does not
//! establish clock rate bounds or every hypervisor's pause/clone contract.
use crate::ownership::timing::Clock;
use rustix::time::{ClockId, clock_gettime};
use std::{
    fs::File,
    io::Write,
    os::unix::fs::FileExt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Default)]
pub(super) struct BootClock {
    frozen: Arc<AtomicU64>,
    guard: Option<Arc<(File, u64)>>,
}

fn marker(file: &File) -> Option<u64> {
    // Linux's device read copies a consistent header under the VMClock seqcount.
    // Reject short reads and unsupported layouts instead of combining samples.
    let mut header = [0u8; 32];
    if file.read_at(&mut header, 0).ok()? != header.len()
        || &header[..4] != b"VCLK"
        || u32::from_le_bytes(header[4..8].try_into().ok()?) < 32
        || u16::from_le_bytes(header[8..10].try_into().ok()?) != 1
        || u32::from_le_bytes(header[12..16].try_into().ok()?) & 1 != 0
    {
        return None;
    }
    Some(u64::from_le_bytes(header[16..24].try_into().ok()?))
}

fn nanos(id: ClockId) -> Option<u64> {
    let now = clock_gettime(id);
    u64::try_from(now.tv_sec)
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(now.tv_nsec).ok()?)
}

impl BootClock {
    pub(super) fn guarded() -> Self {
        let file = File::open("/dev/vmclock0").unwrap();
        let initial = marker(&file).expect("valid VMClock v1 header");
        Self {
            guard: Some(Arc::new((file, initial))),
            ..Self::default()
        }
    }

    pub(super) fn freeze(&self) {
        self.frozen
            .store(self.read().unwrap().as_nanos() as u64, Ordering::SeqCst);
    }

    fn valid(&self) -> bool {
        self.guard
            .as_ref()
            .is_none_or(|g| marker(&g.0) == Some(g.1))
    }
}

impl Clock for BootClock {
    fn read(&self) -> Option<Duration> {
        if !self.valid() {
            return None;
        }
        let frozen = self.frozen.load(Ordering::SeqCst);
        // Center a nominally unit-rate source inside the model's [1,2] range.
        let now = if frozen != 0 {
            frozen
        } else {
            nanos(ClockId::Boottime)?.checked_mul(3)? / 2
        };
        self.valid().then_some(Duration::from_nanos(now))
    }
}

#[test]
fn vm_marker_failure_retires_the_owner_timeline() {
    use crate::ownership::timing::{Bounds, Error, Timeline};
    for fault in ["changed", "version", "short", "updating"] {
        let file = tempfile::tempfile().unwrap();
        let mut header = [0u8; 32];
        header[..4].copy_from_slice(b"VCLK");
        header[4..8].copy_from_slice(&32u32.to_le_bytes());
        header[8..10].copy_from_slice(&1u16.to_le_bytes());
        header[16..24].copy_from_slice(&7u64.to_le_bytes());
        file.write_all_at(&header, 0).unwrap();
        let clock = BootClock {
            guard: Some(Arc::new((file.try_clone().unwrap(), 7))),
            ..Default::default()
        };
        let mut time = Timeline::new(
            clock,
            Bounds::new(Duration::from_secs(1), Duration::from_millis(1)).unwrap(),
        )
        .unwrap();
        let start = time.begin().unwrap();
        let deadline = time.root_deadline(&start).unwrap();
        match fault {
            "changed" => file.write_all_at(&8u64.to_le_bytes(), 16).unwrap(),
            "version" => file.write_all_at(&2u16.to_le_bytes(), 8).unwrap(),
            "short" => file.set_len(23).unwrap(),
            "updating" => file.write_all_at(&1u32.to_le_bytes(), 12).unwrap(),
            _ => unreachable!(),
        }
        assert_eq!(time.remaining(&deadline), Err(Error::Retired), "{fault}");
        // Even a subsequently valid header cannot revive this timeline.
        file.write_all_at(&header, 0).unwrap();
        assert_eq!(time.remaining(&deadline), Err(Error::Retired));
    }
}

#[test]
#[ignore = "VM serial probe driven by scripts/check-vm-clock.py"]
fn physical_clock_probe() {
    let clock = BootClock::guarded();
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap();
    let source =
        std::fs::read_to_string("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            .unwrap();
    println!("\nREADY {} {}", kernel.trim(), source.trim());
    std::io::stdout().flush().unwrap();
    loop {
        let mut line = String::new();
        assert_ne!(std::io::stdin().read_line(&mut line).unwrap(), 0);
        let boot = nanos(ClockId::Boottime).unwrap();
        let mono = nanos(ClockId::Monotonic).unwrap();
        let real = nanos(ClockId::Realtime).unwrap();
        let accepted = clock.read().is_some();
        let token = marker(&clock.guard.as_ref().unwrap().0).unwrap();
        println!("SAMPLE {boot} {mono} {real} {token} {}", u8::from(accepted));
        std::io::stdout().flush().unwrap();
    }
}
