#!/usr/bin/env python3
"""Reproduce ClockBound's VM suspend counterexample. Exit 0 confirms rejection,
not qualification. Requires kache, Docker and /dev/kvm; only the disposable VM
is suspended. The database gains no ClockBound dependency.
"""
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import time
import urllib.request

REV = '371058f8f6a1786d4a4ab3eed9f5f2f16eea7777'
ARCHIVE_SHA = 'd2ede954fe4631785c9457ddc100fdfa1c3252a47410432e250317f24402494f'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    args.output = args.output.resolve()
    os.chdir(repo)
    spec = importlib.util.spec_from_file_location('vm', repo / 'scripts/check-vm-clock.py')
    vm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(vm)
    if not shutil.which('kache'):
        parser.error('source the documented kache build environment first')
    # Keep compiler artifacts cached; source/project and VM state are temporary.
    cache = repo / '.tools/clockbound-target'
    with tempfile.TemporaryDirectory(prefix='durable-db-clockbound-') as directory:
        root = Path(directory)
        archive = urllib.request.urlopen(
            f'https://codeload.github.com/aws/clock-bound/tar.gz/{REV}', timeout=30).read()
        assert hashlib.sha256(archive).hexdigest() == ARCHIVE_SHA
        with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as source:
            source.extractall(root, filter='data')
        project = root / 'probe'
        (project / 'src').mkdir(parents=True)
        (project / 'Cargo.toml').write_text(
            '[package]\nname="durable-clock-probe"\nversion="0.0.0"\nedition="2024"\n'
            '[dependencies]\nclock-bound={path="../clock-bound-' + REV + '/clock-bound",'
            'features=["test-utils"]}\n[workspace]\n')
        shutil.copyfile(repo / 'scripts/clockbound-probe.rs', project / 'src/main.rs')
        vm.run('cargo', 'build', '--manifest-path', str(project / 'Cargo.toml'), timeout=600,
               env={**os.environ, 'RUSTC_WRAPPER': 'kache', 'CARGO_TARGET_DIR': str(cache)})
        binary = cache / 'debug/durable-clock-probe'
        vm.run('docker', 'build', '--target', 'stock', '-t', vm.IMAGE,
               '-f', 'scripts/vm-clock.Dockerfile', 'scripts', timeout=600)
        work = root / 'vm'
        work.mkdir()
        vm.prepare(work, binary)
        vm.pack(work, {'DURABLE_DB_VM_TEST': 'clockbound'})
        rows = []
        with vm.guest(work, 'tsc') as (stream, command, version, name):
            vm.read_until(stream, 'READY')

            def receive(start):
                fields = vm.read_until(stream, 'SAMPLE ').split()[1:]
                assert len(fields) == 7
                return {'guarded': [int(fields[0]), int(fields[1]), fields[2]],
                        'unguarded': [int(fields[3]), int(fields[4]), fields[5]],
                        'marker': int(fields[6]), 'host_window': [start, time.monotonic_ns()]}

            def sample(request):
                start = time.monotonic_ns()
                stream.write((request + '\n').encode())
                return receive(start)

            for mode in ['baseline', 'qmp-stop', 'cgroup-freeze', 'guest-suspend-short', 'snapshot-restore']:
                sample('reset')  # Calibration completes before the timed sample.
                before = sample('sample')
                if mode == 'qmp-stop':
                    command('stop')
                elif mode == 'cgroup-freeze':
                    vm.run('docker', 'pause', name)
                elif mode == 'snapshot-restore':
                    assert not command('human-monitor-command', {'command-line': 'savevm bound'})
                elif mode == 'guest-suspend-short':
                    stream.write(b'suspend\n')
                    deadline = time.monotonic() + 10
                    while command('query-status')['status'] != 'suspended':
                        assert time.monotonic() < deadline, 'guest did not suspend'
                        time.sleep(.01)
                time.sleep(.05 if mode == 'guest-suspend-short' else 3)
                if mode == 'qmp-stop':
                    command('cont')
                elif mode == 'cgroup-freeze':
                    vm.run('docker', 'unpause', name)
                elif mode == 'snapshot-restore':
                    assert not command('human-monitor-command', {'command-line': 'loadvm bound'})
                if mode == 'guest-suspend-short':
                    start = time.monotonic_ns()
                    command('system_wakeup')
                    after = receive(start)
                else:
                    after = sample('sample')
                host = [after['host_window'][0] - before['host_window'][1],
                        after['host_window'][1] - before['host_window'][0]]
                result = {'mode': mode, 'before': before, 'after': after, 'host_elapsed_window_ns': host}
                for kind in ['guarded', 'unguarded']:
                    b, a = before[kind], after[kind]
                    interval = [a[0] - b[1], a[1] - b[0]]
                    result[kind] = {'elapsed_window_ns': interval,
                                    'nondecreasing_midpoint': a[0] + a[1] >= b[0] + b[1],
                                    'trusted': a[2] in ('Synchronized', 'FreeRunning'),
                                    'overlaps_host': interval[0] <= host[1] and host[0] <= interval[1]}
                rows.append(result)
                print(mode, result['guarded'], flush=True)
        report = {'kind': 'ClockBound V3 counter/disruption rejection; synthetic calibration, no NTP qualification',
                  'upstream_commit': REV, 'archive_sha256': ARCHIVE_SHA,
                  'host': vm.run('uname', '-srmo'), 'qemu': version,
                  'image_id': vm.run('docker', 'image', 'inspect', '--format', '{{.Id}}', vm.IMAGE),
                  'sha256': {'binary': digest(binary), 'kernel': digest(work / 'kernel'),
                             'lock': digest(project / 'Cargo.lock'),
                             'probe': digest(repo / 'scripts/clockbound-probe.rs'),
                             'driver': digest(Path(__file__))}, 'samples': rows}
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        for row in rows[:3]:
            assert row['guarded']['trusted'] and row['guarded']['overlaps_host'], row['mode']
        suspend, restore = rows[3:]
        assert suspend['guarded']['trusted'] and suspend['guarded']['nondecreasing_midpoint']
        assert not suspend['guarded']['overlaps_host']
        assert suspend['before']['marker'] == suspend['after']['marker']
        assert not restore['guarded']['trusted'] and restore['unguarded']['trusted']
        assert not restore['unguarded']['overlaps_host']
        print('REJECTED: suspend can leave a trusted, nondecreasing clock outside its reported bound.')


if __name__ == '__main__':
    main()
