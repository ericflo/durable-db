#!/usr/bin/env python3
"""Opt-in KVM clock boundary test; requires Docker and /dev/kvm, never pauses the host.

Exit 1 means admission differed from the expected kernel/source policy or an
accepted interval violated the bounds. Finite passes do not qualify production.
"""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

IMAGE = 'durable-db-vm-clock:local'
TEST_PREFIX = 'providers::s3::tests::timing::'
SCENARIOS = ('baseline', 'qmp-stop', 'cgroup-freeze', 'guest-suspend',
             'guest-suspend-short', 'clocksource-fallback', 'snapshot-restore', 'sleep-disabled')


def capabilities(header):
    """Inspect one kernel-consistent VMClock v1 read; never grant permission."""
    size = int.from_bytes(header[4:8], 'little')
    if (len(header) < 32 or header[:4] != b'VCLK' or size < 32
            or int.from_bytes(header[8:10], 'little') != 1
            or int.from_bytes(header[12:16], 'little') & 1
            or len(header) < min(size, 112)):
        raise ValueError('invalid, incomplete or updating VMClock v1 header')

    def field(offset, length=8):
        return (int.from_bytes(header[offset:offset + length], 'little')
                if offset + length <= min(size, len(header)) else None)

    flags = field(24)
    rate_error = field(64) if flags & (1 << 4) else None
    time_error = field(96) if flags & (1 << 6) else None
    generation = field(104) if flags & (1 << 8) else None
    checks = {
        'no_supported_counter': header[10] not in (0, 1),
        'no_tai_time': header[11] != 1,
        'clock_not_reliable': field(34, 1) not in (2, 3),
        'missing_max_rate_error': rate_error is None,
        'missing_max_time_error': time_error is None,
        'invalid_counter_period': field(48) in (None, 0),
        'missing_generation_counter': generation is None,
    }
    return {'header_hex': header.hex(), 'size': size, 'flags': flags,
            'counter_id': header[10], 'time_type': header[11],
            'clock_status': field(34, 1), 'disruption_marker': field(16),
            'counter_period_shift': field(39, 1), 'counter_period_frac_sec': field(48),
            'counter_period_maxerror_rate_frac_sec': rate_error,
            'time_maxerror_nanosec': time_error, 'vm_generation_counter': generation,
            'missing_requirements': [name for name, missing in checks.items() if missing],
            'requirements_present': not any(checks.values()), 'qualified_for_serving': False}


def device_capabilities(path):
    try:
        with path.open('rb', buffering=0) as device:
            return {'device': str(path), **capabilities(os.pread(device.fileno(), 112, 0))}
    except (OSError, ValueError) as error:
        return {'device': str(path), 'unavailable': str(error),
                'requirements_present': False, 'qualified_for_serving': False}


def run(*args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True,
                            timeout=kwargs.pop('timeout', 60), **kwargs)
    if result.returncode:
        print((result.stdout + result.stderr)[-12000:], file=sys.stderr)
        result.check_returncode()
    return result.stdout.strip()


def connect(path):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        connection = socket.socket(socket.AF_UNIX)
        connection.settimeout(15)
        try:
            connection.connect(str(path))
            return connection
        except (FileNotFoundError, ConnectionRefusedError):
            connection.close()
            time.sleep(.05)
    raise RuntimeError('VM socket unavailable')


@contextmanager
def guest(work, source, owner=False):
    name = 'durable-db-clock-' + uuid.uuid4().hex[:12]
    mount = ['--network', 'host' if owner else 'none', '--user', str(os.getuid()), '-v', f'{work}:/work']
    run('docker', 'run', '--rm', *mount, IMAGE, 'qemu-img', 'create', '-f', 'qcow2',
        '/work/snapshot.qcow2', '1M')
    (work / 'container').write_text(name)
    try:
        run('docker', 'run', '--rm', '-d', '--name', name, '--read-only', '--device', '/dev/kvm',
            *mount, IMAGE, 'qemu-system-x86_64', '-accel', 'kvm', '-cpu', 'host', '-m', '512',
            '-smp', '1', '-nodefaults', '-no-reboot', '-display', 'none', '-device', 'vmclock',
            *(['-nic', 'user,model=e1000'] if owner else []),
            '-drive', 'file=/work/snapshot.qcow2,format=qcow2,if=none,id=snap',
            '-kernel', '/work/kernel', '-initrd', '/work/initramfs.gz', '-append',
            f'console=ttyS0 quiet panic=-1 rdinit=/init clocksource={source}',
            '-serial', 'unix:/work/serial.sock,server=on,wait=on',
            '-qmp', 'unix:/work/qmp.sock,server=on,wait=off')
        with connect(work / 'serial.sock') as serial, connect(work / 'qmp.sock') as qmp:
            with serial.makefile('rwb', buffering=0) as stream, qmp.makefile('rwb', buffering=0) as monitor:
                greeting = json.loads(monitor.readline())

                def command(name, arguments=None):
                    message = {'execute': name}
                    if arguments:
                        message['arguments'] = arguments
                    monitor.write((json.dumps(message) + '\n').encode())
                    while True:
                        reply = json.loads(monitor.readline())
                        if 'error' in reply:
                            raise RuntimeError(reply['error'])
                        if 'return' in reply:
                            return reply['return']

                command('qmp_capabilities')
                yield stream, command, greeting['QMP']['version'], name
    finally:
        cleanup_guest(work)
        for filename in ['serial.sock', 'qmp.sock', 'snapshot.qcow2']:
            (work / filename).unlink(missing_ok=True)


def cleanup_guest(work):
    # The outer driver also calls this if the Rust test kills its child helper.
    container = work / 'container'
    if container.exists():
        subprocess.run(['docker', 'rm', '-f', container.read_text()],
                       capture_output=True, timeout=30, check=False)
        container.unlink(missing_ok=True)


def read_until(stream, prefix):
    lines = []
    while True:
        try:
            raw = stream.readline()
        except TimeoutError as error:
            raise RuntimeError('\n'.join(lines[-15:])) from error
        if not raw:
            raise RuntimeError('guest disconnected: ' + '\n'.join(lines[-15:]))
        line = raw.decode(errors='replace').strip()
        if line.startswith(prefix):
            return line
        if not line and not lines:
            continue
        lines.append(line)
        if 'panicked at' in line:
            lines.append(stream.readline().decode(errors='replace').strip())
        if 'panicked at' in line or 'Kernel panic' in line or len(lines) > 100:
            raise RuntimeError('\n'.join(lines[-15:]))


def exercise(work, source, scenario=None, repeat=1, kernel='stock'):
    with guest(work, source) as (stream, command, version, name):
        guest_info = read_until(stream, 'READY ')
        if not guest_info.endswith(' ' + source):
            raise RuntimeError('guest did not select the requested clocksource')

        def sample(request='sample'):
            stream.write((request + '\n').encode())
            return list(map(int, read_until(stream, 'SAMPLE ').split()[1:]))

        rows = []
        modes = [mode for mode in SCENARIOS if
                 (not mode.startswith('guest-suspend') if kernel == 'nosleep' else mode != 'sleep-disabled')]
        cases = [(mode, iteration) for mode in ([scenario] if scenario else modes)
                 for iteration in range(1, repeat + 1)]
        for mode, iteration in cases:
            before = sample('reset')
            started = time.monotonic_ns()
            if mode == 'qmp-stop':
                command('stop')
            elif mode == 'cgroup-freeze':
                run('docker', 'pause', name)
            elif mode == 'snapshot-restore':
                error = command('human-monitor-command', {'command-line': 'savevm lease'})
                if error:
                    raise RuntimeError(error)
            elif mode.startswith('guest-suspend'):
                stream.write(b'suspend\n')
                deadline = time.monotonic() + 10
                while command('query-status')['status'] != 'suspended':
                    if time.monotonic() >= deadline:
                        raise RuntimeError('guest did not enter ACPI suspend')
                    time.sleep(.05)
            time.sleep(0 if mode == 'sleep-disabled' else
                       .05 if mode in ('clocksource-fallback', 'guest-suspend-short') else 3)
            if mode == 'qmp-stop':
                command('cont')
            elif mode == 'cgroup-freeze':
                run('docker', 'unpause', name)
            elif mode == 'snapshot-restore':
                error = command('human-monitor-command', {'command-line': 'loadvm lease'})
                if error:
                    raise RuntimeError(error)
            elif mode.startswith('guest-suspend'):
                command('system_wakeup')
            after = (list(map(int, read_until(stream, 'SAMPLE ').split()[1:])) if mode.startswith('guest-suspend')
                     else sample('source kvm-clock' if mode == 'clocksource-fallback' else
                                 'sleep-disabled' if mode == 'sleep-disabled' else 'sample'))
            elapsed = time.monotonic_ns() - started
            delta = [b - a for a, b in zip(before, after)]
            normalized = delta[0] * 3 // 2
            within = elapsed - 1_000_000 <= normalized <= 2 * elapsed + 1_000_000
            must_accept = (kernel == 'nosleep' and source == 'tsc'
                           and mode not in ('clocksource-fallback', 'snapshot-restore'))
            rows.append({'mode': mode, 'iteration': iteration, 'host_ns': elapsed, 'guest_boottime_ns': delta[0],
                         'guest_monotonic_ns': delta[1], 'guest_realtime_ns': delta[2],
                         'marker_before': before[3], 'marker_after': after[3],
                         'guard_accepts_timestamp': bool(after[4]),
                         'normalized_boottime_ns': normalized, 'within_candidate_bounds': within,
                         'expected_admission': must_accept,
                         'guarded_scenario_passed': bool(after[4]) == must_accept and (not after[4] or within)})
            if mode == 'clocksource-fallback':
                # A new raw probe sample is not a new owner: a Timeline that saw
                # the refusal stays retired, as the focused Rust test checks.
                sample('source ' + source)
        return {'guest': guest_info, 'qemu': version, 'samples': rows}


def pack(work, variables):
    (work / 'root/env').write_text(''.join(f'export {key}={shlex.quote(value)}\n' for key, value in variables.items()))
    run('docker', 'run', '--rm', '--network', 'none', '--user', str(os.getuid()),
        '-v', f'{work}:/work', '-w', '/work/root', IMAGE, 'sh', '-ec',
        'find . -print | cpio -o -H newc | gzip -1 > /work/initramfs.gz')


def owner_child():
    work = Path(os.environ['DURABLE_DB_PAUSE_VM_ROOT'])
    global IMAGE
    IMAGE = os.environ.get('DURABLE_DB_VM_IMAGE', IMAGE)
    report = Path(os.environ['DURABLE_DB_PAUSE_REPORT'])
    variables = {key: os.environ[key] for key in ['DURABLE_DB_PAUSE_FREEZE', 'DURABLE_DB_PAUSE_REPORT']}
    variables.update(DURABLE_DB_PAUSE_GUEST='1',
                     DURABLE_DB_VM_TEST=TEST_PREFIX + 'physical_owner_child',
                     DURABLE_DB_PAUSE_ENDPOINT=os.environ['DURABLE_DB_PAUSE_ENDPOINT'].replace('127.0.0.1', '10.0.2.2'))
    pack(work, variables)
    with guest(work, 'tsc', owner=True) as (stream, command, version, _):
        read_until(stream, 'OWNER_READY')
        command('stop')
        error = command('human-monitor-command', {'command-line': 'savevm owner'})
        if error:
            raise RuntimeError(error)
        report.with_suffix('.ready').write_text('snapshot saved; VM stopped\n')
        assert sys.stdin.readline() == 'resume\n'
        error = command('human-monitor-command', {'command-line': 'loadvm owner'})
        if error:
            raise RuntimeError(error)
        command('cont')
        stream.write(b'resume\n')
        result = json.loads(read_until(stream, 'OWNER_RESULT ').removeprefix('OWNER_RESULT '))
        report.write_text(json.dumps(result))
        with (work / 'owners.jsonl').open('a') as evidence:
            evidence.write(json.dumps({'guard': variables['DURABLE_DB_PAUSE_FREEZE'] == '0',
                                       'applied_acknowledged_served': result, 'qemu': version}) + '\n')


def prepare(work, binary):
    root = work / 'root'
    root.mkdir()
    if binary:
        shutil.copy2(binary, root / 'tests')
        run('strip', '--strip-debug', str(root / 'tests'))
        for library in set(re.findall(r'/[^\s()]+', run('ldd', str(binary)))):
            target = root / library.lstrip('/')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(library, target)
    run('docker', 'run', '--rm', '--network', 'none', '--user', str(os.getuid()),
        '-v', f'{work}:/work', '-w', '/work/root', IMAGE, 'sh', '-ec',
        'mkdir -p bin lib etc/ssl/certs; cp /bin/busybox bin/; cp /lib/ld-musl* lib/; '
        'cp /etc/ssl/certs/ca-certificates.crt etc/ssl/certs/; '
        'if test -d /lib/modules; then '
        'gzip -dc /lib/modules/*/kernel/drivers/ptp/ptp_vmclock.ko.gz > vmclock.ko; '
        'gzip -dc /lib/modules/*/kernel/drivers/net/ethernet/intel/e1000/e1000.ko.gz > e1000.ko; fi; '
        'cp /boot/vmlinuz-virt /work/kernel')
    (root / 'init').write_text('''#!/bin/busybox sh
set -eu
/bin/busybox --install -s /bin
mkdir -p /dev /sys /proc /tmp
mount -t devtmpfs devtmpfs /dev
mount -t sysfs sysfs /sys
mount -t proc proc /proc
if test -f /vmclock.ko; then insmod /vmclock.ko; fi
. /env
if [ "$DURABLE_DB_VM_TEST" = capabilities ]; then
    dd if=/dev/vmclock0 of=/tmp/vmclock.bin bs=112 count=1 2>/dev/null
    echo "VMCLOCK $(od -An -v -tx1 -N112 /tmp/vmclock.bin | tr -d ' \\n')"
    exec sleep 60
fi
if [ "${DURABLE_DB_PAUSE_GUEST:-}" = 1 ]; then
    test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
    if test -f /e1000.ko; then insmod /e1000.ko; fi
    ip link set eth0 up
    ip addr add 10.0.2.15/24 dev eth0
    ip route add default via 10.0.2.2
fi
exec /tests --exact "$DURABLE_DB_VM_TEST" --ignored --nocapture --test-threads=1 < /dev/ttyS0 > /dev/ttyS0 2>&1
''')
    (root / 'init').chmod(0o755)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--clocksource', choices=['kvm-clock', 'tsc'])
    parser.add_argument('--kernel', choices=['stock', 'nosleep'], default='stock')
    parser.add_argument('--scenario', choices=SCENARIOS, help='run one clock fault for a focused check')
    parser.add_argument('--repeat', type=int, choices=range(1, 17), default=1, metavar='1..16')
    parser.add_argument('--owner', action='store_true', help='exercise native takeover across memory restore, including the unsafe control')
    parser.add_argument('--test-binary', type=Path, help='historical timed-owner test artifact from commit 6688d47, built with kache')
    parser.add_argument('--capabilities', action='store_true', help='inspect VMClock prerequisites in a disposable VM; no Rust build or ownership')
    parser.add_argument('--device', type=Path, help='inspect this local VMClock device instead of starting a VM; no build or mutation')
    args = parser.parse_args()
    if args.capabilities or args.device:
        if args.owner or args.scenario or args.clocksource or args.repeat != 1 or args.test_binary or args.kernel != 'stock':
            parser.error('capability inspection does not use interval/owner experiment options')
    if args.device:
        args.output.write_text(json.dumps(device_capabilities(args.device), indent=2) + '\n')
        return 0  # Successful inspection is not successful qualification.
    if args.owner and (args.scenario or args.clocksource or args.repeat != 1):
        parser.error('--owner has its own fixed takeover scenarios')
    if args.owner and args.kernel != 'nosleep':
        parser.error('--owner requires --kernel nosleep; stock kernels must refuse admission')
    if args.scenario and ((args.kernel == 'nosleep' and args.scenario.startswith('guest-suspend'))
                          or (args.kernel == 'stock' and args.scenario == 'sleep-disabled')):
        parser.error('the scenario requires the other kernel profile')
    global IMAGE
    IMAGE = 'durable-db-vm-clock:local' if args.kernel == 'stock' else 'durable-db-vm-nosleep:local'
    if args.capabilities:
        binary = None
    elif args.test_binary:
        binary = args.test_binary.resolve()
    else:
        parser.error('timed ownership was retired; supply --test-binary from historical commit 6688d47')
    with tempfile.TemporaryDirectory(prefix='durable-db-clock-') as directory, ExitStack() as cleanup:
        work = Path(directory)
        cleanup.callback(cleanup_guest, work)
        dockerfile = Path(__file__).with_name('vm-clock.Dockerfile')
        run('docker', 'build', '--target', args.kernel, '-t', IMAGE,
            '-f', str(dockerfile), str(dockerfile.parent), timeout=1800)
        prepare(work, binary)
        if args.capabilities:
            pack(work, {'DURABLE_DB_VM_TEST': 'capabilities'})
            with guest(work, 'tsc') as (stream, _, version, _):
                header = bytes.fromhex(read_until(stream, 'VMCLOCK ').removeprefix('VMCLOCK '))
                runs = [{'qemu': version, **capabilities(header)}]
        elif args.owner:
            output = run(str(binary), '--exact', TEST_PREFIX + 'physical_owner_pause_fences_reads_and_late_acknowledgements',
                         '--nocapture', env={**os.environ, 'DURABLE_DB_PAUSE_VM_ROOT': str(work), 'DURABLE_DB_VM_IMAGE': IMAGE}, timeout=90)
            runs = [json.loads(line) for line in (work / 'owners.jsonl').read_text().splitlines()]
            print(output)
        else:
            pack(work, {'DURABLE_DB_VM_TEST': TEST_PREFIX + 'clock::physical_clock_probe'})
            runs = [exercise(work, source, args.scenario, args.repeat, args.kernel) for source in ([args.clocksource] if args.clocksource else ['kvm-clock', 'tsc'])]
        report = {'kind': 'finite VM clock boundary experiment; not production qualification',
                  'host': run('uname', '-srmo'),
                  'image_id': run('docker', 'image', 'inspect', '--format', '{{.Id}}', IMAGE),
                  'test_binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest() if binary else None,
                  'kernel_sha256': hashlib.sha256((work / 'kernel').read_bytes()).hexdigest(),
                  'configuration': 'KVM, host CPU, one vCPU, 512 MiB, VMClock device' +
                                   ('' if args.capabilities else '; test-only 3/2 clock scaling and 1 ms error allowance'),
                  'kernel_profile': args.kernel,
                  'mode': 'capabilities' if args.capabilities else 'owner-restore' if args.owner else 'clock-intervals', 'runs': runs}
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        if args.owner or args.capabilities:
            return 0
        for result in report['runs']:
            print(result['guest'], [(s['mode'], s['guarded_scenario_passed']) for s in result['samples']])
        return 0 if all(s['guarded_scenario_passed'] for r in report['runs'] for s in r['samples']) else 1


if __name__ == '__main__':
    if sys.argv[1:] == ['--owner-child']:
        owner_child()
    else:
        raise SystemExit(main())
