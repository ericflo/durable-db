#!/usr/bin/env python3
"""Opt-in KVM clock boundary test; requires Docker and /dev/kvm, never pauses the host.

Exit 1 means an accepted guest timestamp violated the candidate bounds. Passing
these finite scenarios does not qualify a production clock or deployment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import uuid

IMAGE = 'durable-db-vm-clock:local'
DOCKERFILE = '''FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2
RUN apk add --no-cache qemu-system-x86_64 qemu-img linux-virt musl-dev gcc cpio
'''


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True,
                          timeout=kwargs.pop('timeout', 60), **kwargs).stdout.strip()


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


def exercise(work, source):
    name = 'durable-db-clock-' + uuid.uuid4().hex[:12]
    mount = ['--network', 'none', '--user', str(os.getuid()), '-v', f'{work}:/work']
    run('docker', 'run', '--rm', *mount, IMAGE, 'qemu-img', 'create', '-f', 'qcow2',
        '/work/snapshot.qcow2', '1M')
    try:
        run('docker', 'run', '--rm', '-d', '--name', name, '--read-only', '--device', '/dev/kvm',
            *mount, IMAGE, 'qemu-system-x86_64', '-accel', 'kvm', '-cpu', 'host', '-m', '128',
            '-smp', '1', '-nodefaults', '-no-reboot', '-display', 'none', '-device', 'vmclock',
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
                while True:
                    line = stream.readline().decode().strip()
                    if line.startswith('READY '):
                        guest = line
                        if not guest.endswith(' ' + source):
                            raise RuntimeError('guest did not select the requested clocksource')
                        break
                    if not line:
                        raise RuntimeError('guest failed to boot')

                def sample():
                    stream.write(b'sample\n')
                    while True:
                        line = stream.readline().decode().strip()
                        if line.startswith('SAMPLE '):
                            return list(map(int, line.split()[1:]))
                        if not line:
                            raise RuntimeError('guest stopped responding')

                rows = []
                for mode in ['baseline', 'qmp-stop', 'cgroup-freeze', 'snapshot-restore']:
                    before = sample()
                    started = time.monotonic_ns()
                    if mode == 'qmp-stop':
                        command('stop')
                    elif mode == 'cgroup-freeze':
                        run('docker', 'pause', name)
                    elif mode == 'snapshot-restore':
                        error = command('human-monitor-command', {'command-line': 'savevm lease'})
                        if error:
                            raise RuntimeError(error)
                    time.sleep(3)
                    if mode == 'qmp-stop':
                        command('cont')
                    elif mode == 'cgroup-freeze':
                        run('docker', 'unpause', name)
                    elif mode == 'snapshot-restore':
                        error = command('human-monitor-command', {'command-line': 'loadvm lease'})
                        if error:
                            raise RuntimeError(error)
                    after = sample()
                    elapsed = time.monotonic_ns() - started
                    delta = [b - a for a, b in zip(before, after)]
                    normalized = delta[0] * 3 // 2
                    within = elapsed - 1_000_000 <= normalized <= 2 * elapsed + 1_000_000
                    rows.append({'mode': mode, 'host_ns': elapsed, 'guest_boottime_ns': delta[0],
                                 'guest_monotonic_ns': delta[1], 'guest_realtime_ns': delta[2],
                                 'marker_before': before[3], 'marker_after': after[3],
                                 'guard_accepts_timestamp': bool(after[4]),
                                 'normalized_boottime_ns': normalized, 'within_candidate_bounds': within,
                                 'guarded_scenario_passed': not after[4] or within})
                return {'guest': guest, 'qemu': greeting['QMP']['version'], 'samples': rows}
    finally:
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=30, check=False)
        for filename in ['serial.sock', 'qmp.sock', 'snapshot.qcow2']:
            (work / filename).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--clocksource', choices=['kvm-clock', 'tsc'])
    args = parser.parse_args()
    probe = Path(__file__).with_name('vm-clock-probe.c').read_bytes()
    with tempfile.TemporaryDirectory(prefix='durable-db-clock-') as directory:
        work = Path(directory)
        (work / 'Dockerfile').write_text(DOCKERFILE)
        (work / 'probe.c').write_bytes(probe)
        run('docker', 'build', '-t', IMAGE, str(work), timeout=240)
        run('docker', 'run', '--rm', '--network', 'none', '--user', str(os.getuid()),
            '-v', f'{work}:/work', '-w', '/work', IMAGE, 'sh', '-ec',
            'gcc -Os -static -o init probe.c; '
            'gzip -dc /lib/modules/*/kernel/drivers/ptp/ptp_vmclock.ko.gz > vmclock.ko; '
            'printf "init\\nvmclock.ko\\n" | cpio -o -H newc | gzip -1 > initramfs.gz; '
            'cp /boot/vmlinuz-virt kernel')
        report = {'kind': 'finite VM clock boundary experiment; not production qualification',
                  'host': run('uname', '-srmo'),
                  'image_id': run('docker', 'image', 'inspect', '--format', '{{.Id}}', IMAGE),
                  'probe_sha256': hashlib.sha256(probe).hexdigest(),
                  'kernel_sha256': hashlib.sha256((work / 'kernel').read_bytes()).hexdigest(),
                  'configuration': 'KVM, host CPU, one vCPU, 128 MiB, no network, VMClock device, 3-second gaps; 3/2 clock scaling and 1 ms comparison allowance',
                  'runs': [exercise(work, source) for source in ([args.clocksource] if args.clocksource else ['kvm-clock', 'tsc'])]}
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        for result in report['runs']:
            print(result['guest'], [(s['mode'], s['guarded_scenario_passed']) for s in result['samples']])
        return 0 if all(s['guarded_scenario_passed'] for r in report['runs'] for s in r['samples']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
