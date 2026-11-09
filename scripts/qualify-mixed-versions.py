#!/usr/bin/env python3
"""Explicit disposable-bucket qualification; leaves objects for caller cleanup.

Credentials come from the environment. No secrets, database contents, or provider
response bodies are included in the report. This is a compatibility/crash smoke,
not a complete history checker, autoscaling campaign, or performance benchmark.
"""
import argparse
import base64
import datetime
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--old-binary', required=True)
parser.add_argument('--new-binary', required=True)
parser.add_argument('--bucket', required=True)
parser.add_argument('--provider', choices=['s3', 'b2'], default='s3')
parser.add_argument('--prefix')
parser.add_argument('--output', required=True)
parser.add_argument('--port', type=int, default=17701)
parser.add_argument('--context', required=True)
parser.add_argument('--initial-workers', type=int, choices=[1, 2, 3], default=3)
parser.add_argument('--maintenance-seconds', type=int, default=5)
parser.add_argument('--disposable', action='store_true', required=True)
args = parser.parse_args()
if args.maintenance_seconds < 1:
    parser.error('maintenance interval must be positive')
if not 1024 <= args.port <= 65532:
    parser.error('port must leave room for three unprivileged listeners')
prefix = args.prefix or 'mixed-' + uuid.uuid4().hex
old, new = [str(Path(p).resolve()) for p in [args.old_binary, args.new_binary]]
report = dict(schema=1, started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), context=args.context, provider=args.provider,
              old_binary_sha256=hashlib.sha256(Path(old).read_bytes()).hexdigest(),
              new_binary_sha256=hashlib.sha256(Path(new).read_bytes()).hexdigest(),
              phases=[], http_attempt_statuses={}, complete=False,
              configuration=dict(initial_workers=args.initial_workers, maintenance_seconds=args.maintenance_seconds, cache_bytes=0, workload_credentials='database-scoped'),
              limitations=['Disposable compatibility/crash smoke, not a full operation history checker.',
                           'Kill during a request is timing based, not a guaranteed publication-boundary fault.',
                           'No CPU-triggered HPA, network partition, or sustained-load claim.'])
processes = {}
lock = threading.Lock()
expected = {}
pending_writes = {}
name_prefix = 'case-' + uuid.uuid4().hex + '-'
report['synthetic_namespace'] = name_prefix


def record(status):
    with lock:
        counts = report['http_attempt_statuses']
        counts[str(status)] = counts.get(str(status), 0) + 1


def save():
    report['expected_state'] = [dict(database_index=n, revision=r, value_sha256=None if v is None else hashlib.sha256(base64.b64decode(v)).hexdigest()) for n, (r, v) in ((int(db[-3:]), state) for db, state in expected.items())]
    report['pending_writes'] = list(pending_writes.values())
    Path(args.output).write_text(json.dumps(report, indent=2) + '\n')


with tempfile.TemporaryDirectory(prefix='durable-db-mixed-') as tmp:
    root = Path(tmp)
    subprocess.run([new, 'keygen', '--output', str(root / 'admin.key')], check=True,
                   stdout=subprocess.DEVNULL)
    token = subprocess.check_output([old, 'token', '--key-file', str(root / 'admin.key'), '--admin']).decode().strip()
    scoped_tokens = {}

    def start(index, binary):
        log = open(root / f'{index}.log', 'w')
        command = [binary, 'serve', '--bucket' if args.provider == 's3' else '--b2-bucket',
                   args.bucket, '--prefix', prefix, '--admin-key-file', str(root / 'admin.key'),
                   '--listen', f'127.0.0.1:{args.port + index}', '--memory-cache-mib', '0',
                   '--concurrency', '8', '--maintenance-seconds', str(args.maintenance_seconds)]
        process = subprocess.Popen(command, stdout=log, stderr=log)
        processes[index] = process, log
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('server exited before readiness')
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{args.port + index}/health/ready', timeout=1).close()
                return
            except OSError:
                time.sleep(.1)
        raise RuntimeError('server readiness deadline exceeded')

    def stop(index):
        process, log = processes.pop(index)
        process.kill()  # Deliberately no graceful drain or termination hook.
        process.wait(timeout=10)
        log.close()

    def request(index, db, suffix='', body=None, attempts=16):
        encoded = None if body is None else json.dumps(body).encode()
        path = '/v1/databases/' + name_prefix + db + suffix
        credential = scoped_tokens.get(db, token) if suffix in ['/read', '/commit'] else token
        for attempt in range(attempts):
            try:
                req = urllib.request.Request(f'http://127.0.0.1:{args.port + index}' + path,
                                             data=encoded, headers={'Authorization': 'Bearer ' + credential,
                                                                    'Content-Type': 'application/json'})
                with urllib.request.urlopen(req, timeout=30) as response:
                    record(response.status)
                    return json.load(response)
            except urllib.error.HTTPError as error:
                record(error.code)
                error.close()
                if error.code not in [409, 429, 503]:
                    raise RuntimeError(f'unexpected HTTP status {error.code}') from None
            except (OSError, ValueError):
                record('transport_or_decode_error')
            if attempt + 1 < attempts:
                time.sleep(min(.05 * 2 ** attempt, 1))
        raise RuntimeError('bounded HTTP recovery exhausted')

    def commit(index, db, req):
        pending_writes[db] = dict(database_index=int(db[-3:]), request_id=req['request_id'],
                                  expected_revision=req['expected_revision'],
                                  value_sha256=hashlib.sha256(base64.b64decode(req['writes'][0]['value'])).hexdigest())
        result = request(index, db, '/commit', req)
        assert result['revision'] == req['expected_revision'] + 1
        expected[db] = result['revision'], req['writes'][0]['value']
        del pending_writes[db]

    def phase(label, indices, workers=3):
        phase_result = dict(name=label, processes=len(indices), concurrent_workers=workers,
                            planned_writes=36, started_writes=0, successful_writes=0, failed_writes=0, complete=False)
        report['phases'].append(phase_result)
        def worker(worker_id):
            db = f'db-{worker_id:03}'
            for operation in range(12):
                revision, _ = expected[db]
                req = dict(expected_revision=revision, request_id=str(uuid.uuid4()),
                           writes=[dict(key='value', value=base64.b64encode(f'{label}-{worker_id}-{operation}'.encode()).decode())])
                with lock:
                    phase_result['started_writes'] += 1
                try:
                    commit(indices[(worker_id + operation) % len(indices)], db, req)
                except Exception:
                    with lock:
                        phase_result['failed_writes'] += 1
                    raise
                with lock:
                    phase_result['successful_writes'] += 1
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(worker, range(3)))
            phase_result['complete'] = True
        finally:
            phase_result['not_started'] = 36 - phase_result['started_writes']
            save()

    def verify(index):
        for db, (revision, value) in expected.items():
            result = request(index, db, '/read', dict(keys=['value']))
            assert result['revision'] == revision and result['values'] == [value], 'state mismatch'

    try:
        start(0, old)
        for n in range(70):
            db = f'db-{n:03}'
            request(0, db, body=dict(max_bytes=1048576, max_keys=1000))
            expected[db] = (0, None)
            scoped_tokens[db] = subprocess.check_output([new, 'token', '--key-file', str(root / 'admin.key'),
                                                       '--database', name_prefix + db, '--write']).decode().strip()
        phase('old-only', [0], args.initial_workers)
        start(1, new)
        start(2, new)
        phase('mixed', [0, 1, 2])
        # Issue one write to a process that is then abruptly removed. Replaying
        # the identical body through another process must produce one revision.
        db = 'db-000'
        revision, _ = expected[db]
        req = dict(expected_revision=revision, request_id=str(uuid.uuid4()),
                   writes=[dict(key='value', value=base64.b64encode(b'crash-recovery').decode())])
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(request, 2, db, '/commit', req, 1)
            time.sleep(.01)
            stop(2)
            try:
                pending.result()
                report['interrupted_request'] = 'acknowledged_before_kill'
            except RuntimeError:
                report['interrupted_request'] = 'unknown'
        commit(1, db, req)
        assert request(1, db, '/commit', req)['replayed']
        stop(0)
        phase('new-only', [1])
        stop(1)
        report['all_processes_removed'] = True
        start(0, old)
        verify(0)
        report['old_binary_cold_reopen_verified'] = len(expected)
        stop(0)
        start(1, new)
        verify(1)
        report['new_binary_cold_reopen_verified'] = len(expected)
        report['total_successful_writes'] = 109
        report['complete'] = True
    except Exception as error:
        report['failure'] = type(error).__name__ + ': ' + str(error)
        # Preserve partial acknowledgements and check them through an uncached
        # replacement, including the possibility of a final uncertain write.
        try:
            for index in list(processes):
                stop(index)
            start(1, new)
            recovery = dict(checked=0, mismatches=0, uncertain_observed=0, uncertain_not_observed=0)
            for db, (revision, value) in expected.items():
                result = request(1, db, '/read', dict(keys=['value']))
                actual = result['values'][0]
                if result['revision'] == revision and actual == value:
                    if db in pending_writes:
                        recovery['uncertain_not_observed'] += 1
                elif db in pending_writes and result['revision'] == revision + 1 and actual is not None and hashlib.sha256(base64.b64decode(actual)).hexdigest() == pending_writes[db]['value_sha256']:
                    recovery['uncertain_observed'] += 1
                else:
                    recovery['mismatches'] += 1
                recovery['checked'] += 1
            report['failure_recovery'] = recovery
        except Exception as recovery_error:
            report['failure_recovery_error'] = type(recovery_error).__name__ + ': ' + str(recovery_error)
        raise
    finally:
        for index in list(processes):
            stop(index)
        save()
print(json.dumps(dict(complete=True, databases=70, successful_writes=109)))
