#!/usr/bin/env python3
"""Real HTTP, independent processes, disposable external S3 source of truth.
Requires DURABLE_DB_TEST_BUCKET, AWS credentials/endpoint, and a built executable.
Creates a unique prefix and reports it for cleanup; use only a disposable bucket.
"""
import base64, json, os, pathlib, subprocess, tempfile, time, urllib.request, urllib.error, uuid
binary = str(pathlib.Path(os.environ.get('DURABLE_DB_TEST_BINARY', 'target/debug/durable-db')).resolve())
bucket = os.environ['DURABLE_DB_TEST_BUCKET']
prefix = 'process-' + uuid.uuid4().hex
processes = {}
with tempfile.TemporaryDirectory() as directory:
    root = pathlib.Path(directory)
    subprocess.run([binary, 'keygen', '--output', str(root / 'admin.key')], check=True, stdout=subprocess.DEVNULL)
    token = subprocess.check_output([binary, 'token', '--key-file', str(root / 'admin.key'), '--admin'], text=True).strip()
    def request(port, path, body=None, method=None):
        for attempt in range(12):
            r = urllib.request.Request(f'http://127.0.0.1:{port}{path}', data=None if body is None else json.dumps(body).encode(), headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method=method)
            try:
                with urllib.request.urlopen(r, timeout=35) as response:
                    data = response.read()
                    return json.loads(data) if data else None
            except urllib.error.HTTPError as error:
                if error.code not in (429,503) or attempt==11: raise
                time.sleep(.25 * (attempt+1))
    def start(port):
        log = open(root / f'{port}.log', 'w')
        p = subprocess.Popen([binary, 'serve', '--bucket', bucket, '--prefix', prefix, '--listen', f'127.0.0.1:{port}', '--admin-key-file', str(root / 'admin.key'), '--memory-cache-mib', '0', '--maintenance-seconds', '1'], stdout=log, stderr=log)
        processes[port] = (p, log)
        for _ in range(100):
            if p.poll() is not None: raise RuntimeError((root / f'{port}.log').read_text())
            try:
                request(port,'/health/ready'); return
            except (OSError,urllib.error.URLError): time.sleep(.1)
        raise RuntimeError('server not ready')
    def stop(port, force=False):
        p, log = processes.pop(port)
        p.kill() if force else p.terminate()
        p.wait(timeout=45); log.close()
    try:
        start(17661)
        request(17661,'/v1/databases/test',{'max_bytes':1048576,'max_keys':1000})
        revision=0
        for phase, ports in [('one',[17661]),('three',[17661,17662,17663]),('one-again',[17663])]:
            if phase=='three': start(17662); start(17663)
            if phase=='one-again': stop(17661,True); stop(17662)
            for n in range(12):
                body={'expected_revision':revision,'request_id':str(uuid.uuid4()),'writes':[{'key':f'{revision:04}','value':base64.b64encode(f'value-{revision}'.encode()).decode()}]}
                for _ in range(30):
                    try: result=request(ports[n%len(ports)],'/v1/databases/test/commit',body); break
                    except urllib.error.HTTPError as e:
                        if e.code!=409: raise
                else: raise RuntimeError('maintenance contention never settled')
                revision=result['revision']
            print(json.dumps({'phase':phase,'replicas':len(ports),'revision':revision}),flush=True)
        stop(17663,True)
        start(17664)
        result=request(17664,'/v1/databases/test/read',{'scans':[{'limit':100}]})
        assert result['revision']==36 and len(result['scans'][0]['items'])==36,result
        for n,item in enumerate(result['scans'][0]['items']):
            assert item['key']==f'{n:04}' and base64.b64decode(item['value'])==f'value-{n}'.encode()
        print('All 36 writes survived 1→3→1, SIGKILL, and complete process/cache loss.',flush=True)
    finally:
        for port in list(processes): stop(port,True)
    # The caller owns the disposable bucket/container and its cleanup.
    print('Disposable test object prefix:',prefix,flush=True)
