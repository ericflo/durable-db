#!/usr/bin/env python3
"""Disposable-bucket format-1 to format-2 authorization compatibility smoke.

Provider credentials are inherited from the environment. No tokens, raw data,
object names, or provider response bodies are written to the report. Caller owns
bucket cleanup. This is not a partition, HPA, or sustained-load qualification.
"""
import argparse
import hashlib
import json
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--old-binary', required=True)
parser.add_argument('--new-binary', required=True)
parser.add_argument('--bucket', required=True)
parser.add_argument('--provider', choices=['s3', 'b2'], default='s3')
parser.add_argument('--prefix', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--context', required=True)
parser.add_argument('--port', type=int, default=17801)
parser.add_argument('--disposable', action='store_true', required=True)
args = parser.parse_args()
if not 1024 <= args.port <= 65533:
    parser.error('port must leave room for two unprivileged listeners')
old, new = [str(Path(p).resolve()) for p in [args.old_binary, args.new_binary]]
report = dict(schema=1, complete=False, provider=args.provider, context=args.context,
              old_binary_sha256=hashlib.sha256(Path(old).read_bytes()).hexdigest(),
              new_binary_sha256=hashlib.sha256(Path(new).read_bytes()).hexdigest(),
              checks=[], http_statuses={})
namespace = 'policy-' + uuid.uuid4().hex
names = {kind: namespace + '-' + kind for kind in ['legacy', 'protected']}
processes = {}


with tempfile.TemporaryDirectory(prefix='durable-db-policy-upgrade-') as tmp:
    root = Path(tmp)
    subprocess.run([new, 'keygen', '--output', str(root / 'key')], check=True, stdout=subprocess.DEVNULL)
    admin = subprocess.check_output([old, 'token', '--key-file', str(root / 'key'), '--admin'], text=True).strip()

    def start(index, binary):
        log = open(root / f'{index}.log', 'w')
        process = subprocess.Popen([binary, 'serve', '--bucket' if args.provider == 's3' else '--b2-bucket',
                                    args.bucket, '--prefix', args.prefix, '--admin-key-file', str(root / 'key'),
                                    '--listen', f'127.0.0.1:{args.port + index}', '--memory-cache-mib', '0',
                                    '--maintenance-seconds', '5'], stdout=log, stderr=log)
        processes[index] = process, log
        for _ in range(450):
            if process.poll() is not None:
                raise RuntimeError('server exited before readiness')
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{args.port + index}/health/ready', timeout=1).close()
                return
            except OSError:
                time.sleep(.2)
        raise RuntimeError('server readiness deadline exceeded')

    def stop(index):
        process, log = processes.pop(index)
        process.kill()
        process.wait(timeout=10)
        log.close()

    def request(index, path, body=None, token=None, expected=200, method=None):
        data = None if body is None else json.dumps(body).encode()
        for attempt in range(12):
            req = urllib.request.Request(f'http://127.0.0.1:{args.port + index}' + path,
                                         data=data, method=method, headers={'Authorization': 'Bearer ' + (token or admin),
                                                             'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(req, timeout=35) as response:
                    status, result = response.status, json.load(response)
            except urllib.error.HTTPError as error:
                status, result = error.code, None
                error.close()
            report['http_statuses'][str(status)] = report['http_statuses'].get(str(status), 0) + 1
            if status == expected:
                return result
            if expected == 200 and status in [409, 429, 503] and attempt < 11:
                time.sleep(min(.1 * 2 ** attempt, 2))
                continue
            raise RuntimeError(f'expected HTTP {expected}, received {status}')
        raise RuntimeError('bounded request recovery exhausted')

    def db(kind, suffix=''):
        return '/v1/databases/' + names[kind] + suffix

    def write(revision, identity):
        return dict(expected_revision=revision, request_id=identity, writes=[dict(key='fixture', value='aGk=')])

    try:
        start(0, old)
        for kind in names:
            request(0, db(kind), dict(max_bytes=1048576, max_keys=1000))
            request(0, db(kind, '/commit'), write(0, 'seed'))
        legacy_token = subprocess.check_output([old, 'token', '--key-file', str(root / 'key'),
                                                '--database', names['protected'], '--write'], text=True).strip()
        start(1, new)
        atomic_path = '/v1/databases/' + namespace + '-atomic'
        creation = dict(request_id='create-protected', limits=dict(max_bytes=1048576, max_keys=1000))
        created = request(1, atomic_path, creation, method='PUT')
        assert not created['replayed'] and created['info']['policy']['revision'] == 1
        request(0, atomic_path, expected=503)
        request(1, atomic_path + '/commit', write(0, 'atomic-write'))
        replay = request(1, atomic_path, creation, method='PUT')
        assert replay['replayed'] and replay['info'] == created['info']
        report['checks'].append('protected creation is atomic and exact replay retains initial info after a write')
        request(1, db('protected', '/read'), dict(keys=['fixture']), token=legacy_token)
        enable = dict(request_id='enable', expected_policy_revision=0, change=dict(action='enable'))
        policy = request(1, db('protected', '/access'), enable)['policy']
        assert policy['revision'] == 1 and policy['generation'] == 0
        report['checks'].append('legacy data opened and policy enabled without data revision change')
        request(1, db('protected', '/read'), dict(keys=['fixture']), token=legacy_token, expected=401)
        issued = request(1, '/v1/tokens', dict(database=names['protected'], write=True, ttl_seconds=3600))
        token, token_id = issued['token'], issued['token_id']
        request(1, db('protected', '/commit'), write(1, 'protected-write'), token=token)
        for suffix, body in [('', None), ('/read', dict(keys=['fixture'])), ('/commit', write(2, 'old-rejected')), ('/gc', {})]:
            request(0, db('protected', suffix), body, expected=503)
        report['checks'].append('old server refuses protected info/read/write/collection; new server denies old token')
        request(0, db('legacy', '/commit'), write(1, 'legacy-still-writable'))
        revoke = dict(request_id='revoke', expected_policy_revision=1, change=dict(action='revoke', token_id=token_id))
        request(1, db('protected', '/access'), revoke)
        request(1, db('protected', '/read'), dict(keys=['fixture']), token=token, expected=401)
        stop(0)
        stop(1)
        report['checks'].append('all processes removed without graceful shutdown')
        start(0, old)
        request(0, db('protected', '/read'), dict(keys=['fixture']), expected=503)
        assert request(0, db('legacy', '/read'), dict(keys=['fixture']))['revision'] == 2
        stop(0)
        start(1, new)
        request(1, db('protected', '/read'), dict(keys=['fixture']), token=token, expected=401)
        result = request(1, db('protected', '/read'), dict(keys=['fixture']))
        assert result['revision'] == 2 and result['values'] == ['aGk=']
        replay = request(1, db('protected', '/access'), enable)
        assert replay['replayed'] and replay['policy'] == policy
        request(1, db('protected', '/gc'), {})
        replay = request(1, atomic_path, creation, method='PUT')
        assert replay['replayed'] and replay['info'] == created['info']
        report['checks'].append('cold old binary refuses downgrade; new binary retains data, revocation, and original receipt')
        report['complete'] = True
    except Exception as error:
        report['failure_type'] = type(error).__name__
        raise
    finally:
        for index in list(processes):
            stop(index)
        Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(dict(complete=report['complete'], checks=len(report['checks']))))
