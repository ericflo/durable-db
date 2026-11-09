#!/usr/bin/env python3
"""Qualify the real Pi Harness on three disposable, cache-free API processes.

Uses a fresh namespace in an explicitly disposable object-store bucket. Provider
credentials come from a private JSON environment file; bucket cleanup belongs to
the caller. Only this launcher's child processes can be killed by its control API.
"""
import argparse
import datetime
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--storage-env-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--base-port', type=int, default=17844)
    parser.add_argument('--disposable', action='store_true', required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    storage = json.loads(args.storage_env_file.read_text())
    if ('DURABLE_DB_BUCKET' in storage) == ('DURABLE_DB_B2_BUCKET' in storage):
        parser.error('Exactly one dedicated disposable provider is required')
    prefix = 'pi-harness-' + uuid.uuid4().hex
    urls = [f'http://127.0.0.1:{args.base_port+i}' for i in range(3)]
    limits = dict(bytes=268435456, attempts=20000)
    report = dict(schema=1, complete=False, started_at=utc(),
                  source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  provider='B2' if 'DURABLE_DB_B2_BUCKET' in storage else 'S3-compatible',
                  configuration=dict(replicas=3, memory_cache_mib=0, disk_cache=False,
                                     maintenance_seconds=30, upload_budget=limits),
                  restarts=[], budget_observations=[], limitations=[
                      'Three API processes share one physical host.',
                      'Deterministic faux model and synthetic tools; no paid model calls or external tool side effects.',
                      'Short workflow/fault qualification, not sustained-load, latency, or global tool exactly-once evidence.'])
    sources = [Path(__file__), Path('clients/javascript/harness.test.mjs'), Path('clients/javascript/harness-fixture.mjs'),
               Path('clients/javascript/harness-worker.mjs'), Path('clients/javascript/pi-indexed.mjs'),
               Path('clients/javascript/client.mjs'), Path('clients/javascript/package-lock.json')]
    report['source_sha256'] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    processes, logs = [], []
    control_server = None

    def save():
        args.output.write_text(json.dumps(report, indent=2)+'\n')

    def stop():
        active = [p for p in processes if p.poll() is None]
        for process in active:
            process.kill()
        for process in active:
            process.wait(timeout=30)
        processes.clear()
        for log in logs:
            log.close()
        logs.clear()
        return len(active)

    try:
        with tempfile.TemporaryDirectory(prefix='durable-pi-control-') as directory:
            root = Path(directory)
            key = root/'keyring.json'
            key.write_text(json.dumps(dict(format=1, active=secrets.token_hex(32), verify=[], allow_legacy_admin=False)))
            key.chmod(0o600)
            token = subprocess.check_output([str(binary), 'token', '--key-file', str(key), '--admin', '--ttl-seconds', '3600'], text=True).strip()
            token_file = root/'admin.token'; token_file.write_text(token); token_file.chmod(0o600)
            environment = {k: v for k, v in os.environ.items() if not k.startswith(('DURABLE_DB_', 'B2_', 'AWS_'))}
            environment.update(storage)

            def start():
                for index, url in enumerate(urls):
                    log = args.output.with_suffix(f'.server-{index}.log').open('a')
                    logs.append(log)
                    process = subprocess.Popen([str(binary), 'serve', '--listen', url.removeprefix('http://'),
                        '--prefix', prefix, '--admin-key-file', str(key), '--memory-cache-mib', '0',
                        '--maintenance-seconds', '30', '--upload-budget-bytes', str(limits['bytes']),
                        '--upload-budget-attempts', str(limits['attempts'])], env=environment, stdout=log, stderr=log)
                    processes.append(process)
                    deadline = time.monotonic()+60
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            raise RuntimeError('API process exited before readiness')
                        try:
                            urllib.request.urlopen(url+'/health/ready', timeout=1).close()
                            break
                        except OSError:
                            time.sleep(.1)
                    else:
                        raise RuntimeError('API readiness deadline exceeded')

            def budget(stage):
                deadline = time.monotonic()+120
                while True:
                    request = urllib.request.Request(urls[0]+'/v1/storage/upload-budget', headers={'Authorization': 'Bearer '+token})
                    try:
                        with urllib.request.urlopen(request, timeout=30) as response:
                            status = json.load(response)
                        break
                    except urllib.error.HTTPError as error:
                        code = error.code; error.close()
                        if code not in (429, 503):
                            raise RuntimeError(f'Budget observation failed with status {code}') from None
                    except OSError:
                        pass
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Budget observation deadline exceeded')
                    time.sleep(.25)
                previous = report['budget_observations']
                if status['limits'] != limits or status['configuration_revision'] != 0:
                    raise RuntimeError('Upload budget configuration changed')
                for dimension in limits:
                    allocated = status['allocated'][dimension]
                    if allocated > limits[dimension] or previous and allocated < previous[-1]['status']['allocated'][dimension]:
                        raise RuntimeError('Upload allocation violated a ceiling or decreased')
                previous.append(dict(stage=stage, status=status)); save()

            control_token = secrets.token_hex(32)

            class Control(BaseHTTPRequestHandler):
                def log_message(self, *_args):
                    pass

                def do_POST(self):
                    if self.path != '/restart' or not secrets.compare_digest(self.headers.get('Authorization', ''), 'Bearer '+control_token):
                        self.send_error(404); return
                    try:
                        budget('before-all-process-loss')
                        killed = stop()
                        if killed != len(urls):
                            raise RuntimeError('Expected every API process alive before the fault')
                        time.sleep(2)
                        start()
                        budget('after-cold-restart')
                        result = dict(killed=killed, cold_started=len(processes))
                        report['restarts'].append(dict(time_utc=utc(), **result)); save()
                        data = json.dumps(result).encode()
                        self.send_response(200); self.send_header('Content-Type', 'application/json')
                        self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)
                    except Exception as error:
                        report['restart_failure'] = type(error).__name__; save()
                        self.send_error(503)

            start(); budget('initial')
            control_server = HTTPServer(('127.0.0.1', 0), Control)
            control_thread = threading.Thread(target=control_server.serve_forever, daemon=True)
            control_thread.start()
            control_file = root/'control.json'
            control_file.write_text(json.dumps(dict(url=f'http://127.0.0.1:{control_server.server_port}', token=control_token)))
            control_file.chmod(0o600)
            test_env = os.environ.copy()
            test_env.update(DURABLE_DB_TEST_URL=urls[0], DURABLE_DB_TEST_URLS=','.join(urls),
                            DURABLE_DB_TEST_TOKEN_FILE=str(token_file), DURABLE_DB_TEST_CONTROL_FILE=str(control_file))
            result_path = args.output.with_suffix('.vitest.json').resolve()
            with args.output.with_suffix('.test.log').open('w') as log:
                test_process = subprocess.Popen(['clients/javascript/node_modules/.bin/vitest', 'run', '--root', 'clients/javascript',
                    'harness.test.mjs', '--reporter=verbose', '--reporter=json', '--outputFile='+str(result_path)],
                    env=test_env, stdout=log, stderr=log, start_new_session=True)
                try:
                    returncode = test_process.wait(timeout=2700)
                finally:
                    # A timed-out test may have its own Harness worker. Kill only
                    # this launcher's dedicated process group, including children.
                    try:
                        os.killpg(test_process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    test_process.wait(timeout=30)
            test = json.loads(result_path.read_text())
            report['tests'] = [dict(name=case['title'], status=case['status'], duration_ms=case.get('duration'))
                               for group in test['testResults'] for case in group['assertionResults']]
            report['tests_passed'] = test['numPassedTests']
            if returncode or test['numPassedTests'] != 8 or test['numPendingTests'] or len(report['restarts']) != 1:
                raise RuntimeError('Pi Harness qualification did not pass every required case')
            budget('final')
            report['complete'] = True
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__, reason=str(error))
        raise
    finally:
        if control_server:
            control_server.shutdown(); control_server.server_close()
        stop()
        report['finished_at'] = utc()
        save()
    print(json.dumps(dict(complete=report['complete'], tests=report['tests_passed'], cold_replacements=len(report['restarts']))))


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


if __name__ == '__main__':
    main()
