import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {fork, spawn, spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {mkdtemp, readFile, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash, randomUUID} from 'node:crypto';
import {setTimeout as sleep} from 'node:timers/promises';

const hash = value => createHash('sha256').update(value).digest('hex');
const windowName = 'rolling-replacement-1';

test('worker faults refuse production contexts, control planes and mismatched identities', () => {
  const result = spawnSync('python3', ['-c', `
import copy,runpy
validate=runpy.run_path('scripts/qualify-multinode.py')['kind_worker']
cluster='durable-db-test-0123456789';name=cluster+'-worker'
node={'metadata':{'name':name},'status':{'addresses':[{'type':'InternalIP','address':'172.20.0.2'}]}}
container={'Id':'a'*64,'Name':'/'+name,'Config':{'Labels':{'io.x-k8s.kind.cluster':cluster,'io.x-k8s.kind.role':'worker'}},'State':{'Running':True},'NetworkSettings':{'Networks':{'kind':{'IPAddress':'172.20.0.2'}}}}
assert validate('kind-'+cluster,node,container)['container_id']=='a'*64
cases=[('epsilon',node,container)]
for path,value in [(('Config','Labels','io.x-k8s.kind.cluster'),'production'),(('Config','Labels','io.x-k8s.kind.role'),'control-plane'),(('Name',),'/another-worker'),(('Id',),'short'),(('State','Running'),False),(('NetworkSettings','Networks','kind','IPAddress'),'172.20.0.3')]:
    changed=copy.deepcopy(container);p=changed
    for key in path[:-1]:p=p[key]
    p[path[-1]]=value;cases.append(('kind-'+cluster,node,changed))
for context,node,container in cases:
    try:validate(context,node,container)
    except ValueError:continue
    raise AssertionError('Unsafe worker fault accepted')
`], {encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
});

test('direct ingress observations use fresh sockets, validate reads and bound stalled peers', {timeout: 10000}, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'durable-db-ingress-'));
  t.after(() => rm(directory, {recursive: true}));
  const token = join(directory, 'admin.token'), secret = 'never-include-this-credential';
  await writeFile(token, secret);
  const sockets = new Set(), calls = [];
  let activeReads = 0, peakReads = 0;
  const origins = [];
  for (const mode of ['healthy', 'refusing', 'redirect', 'stalled']) {
    const server = createServer(async (request, response) => {
      sockets.add(request.socket); calls.push(mode);
      assert.equal(request.headers.authorization, 'Bearer '+secret);
      if (request.method === 'POST') {
        peakReads = Math.max(peakReads, ++activeReads);
        response.on('close', () => { activeReads--; });
        await sleep(20);
      }
      if (mode === 'stalled') {
        if (request.url === '/health/ready') return response.end();
        if (request.url.includes('writer-0')) return;
        return response.writeHead(503).end();
      }
      if (mode === 'refusing') return response.writeHead(503).end(secret);
      if (mode === 'redirect') return response.writeHead(307, {location: origins[0]+request.url}).end(secret);
      if (request.url === '/health/ready') return response.end();
      let raw = ''; for await (const chunk of request) raw += chunk;
      assert.equal(request.method, 'POST'); assert.deepEqual(JSON.parse(raw), {keys: ['00000000']});
      // Writer 1 intentionally lies: HTTP 200 alone must not imply a valid read.
      response.end(JSON.stringify({revision: 2, values: [Buffer.from('0/0').toString('base64')]}));
    }).listen(0, '127.0.0.1');
    t.after(() => { server.closeAllConnections(); server.close(); });
    await once(server, 'listening'); origins.push(`http://127.0.0.1:${server.address().port}`);
  }
  const harness = await readFile(new URL('./qualify-multinode.py', import.meta.url), 'utf8');
  const source = harness.match(/INGRESS_PROBE = r"""\n([\s\S]*?)\n"""/)[1];
  const child = spawn(process.execPath, ['-e', source, JSON.stringify({origins, prefix: 'test'}), token]);
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let output = '', errors = '';
  child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { errors += data; });
  const [exit] = await once(child, 'exit');
  assert.equal(exit, 0, errors); assert.equal(errors, ''); assert(!output.includes(secret));
  const rows = JSON.parse(output);
  assert.deepEqual(rows.map(row => [row.ready.status, ...row.reads.map(r => r.status)]),
    [[200,200,200],[503,503,503],[307,307,307],[200,0,503]]);
  assert.deepEqual(rows[0].reads.map(r => r.valid), [true,false]);
  assert.equal(calls.length, 12); assert.equal(sockets.size, 12, 'no connection reuse or replay');
  assert.equal(peakReads, 1, 'diagnostic reads must not create their own admission overload');
  assert(rows[3].reads[0].elapsed_ms>=4900 && rows[3].reads[0].elapsed_ms<7000);
});

async function checked(directory, report, history, succeeds = true) {
  const raw = history.map(record => JSON.stringify(record) + '\n').join('');
  await writeFile(join(directory, 'report.history.jsonl'), raw);
  await writeFile(join(directory, 'report.json'), JSON.stringify({...report, history_sha256: hash(raw)}));
  const result = spawnSync('python3', ['scripts/check-autoscaling-history.py', join(directory, 'report.json')], {encoding: 'utf8'});
  assert.equal(result.status === 0, succeeds, result.stderr || result.stdout);
  return succeeds ? JSON.parse(result.stdout) : undefined;
}
const reportFor = (records, window) => ({schema: 2, complete: true, failures: [],
  writes_acknowledged: records.length, reads_verified: records.length, verified_keys: records.length,
  phases: [{name: windowName}], readback_windows: [window]});

test('checker measures verified readbacks, rejects broken evidence, and preserves schema 1', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'durable-db-history-'));
  t.after(() => rm(directory, {recursive: true}));
  const history = Array.from({length: 4}, (_, database_index) =>
    [[1, 2, 3], [4, 5, 25], [26, 27, 28], [30, 31, 32]].map(([start, end, readback_end], index) => ({
      database_index, request_id: randomUUID(), revision: index + 1, start, end, readback_end,
      value_sha256: hash(`${database_index}/${index}`),
    }))).flat();
  const report = reportFor(history, {name: windowName, target_seconds: 20,
    start: {seconds: 3.5, verified: [1, 1, 1, 1]}, end: {seconds: 29, verified: [3, 3, 3, 3]}});
  const [window] = (await checked(directory, report, history)).readback_windows;
  assert.deepEqual(window.max_completed_readback_gap_seconds, [22, 22, 22, 22]);
  assert.deepEqual(window.open_readback_gap_seconds, [1, 1, 1, 1]);
  assert.deepEqual(window.readbacks, [2, 2, 2, 2]);
  assert.equal(window.observed_within_target, false);
  for (const name of ['asymmetric-peer-isolation', 'symmetric-peer-isolation',
    'one-host-process-pause', 'one-host-processes-resumed', 'scale-one', 'worker-loss', 'worker-return']) {
    const fault = structuredClone(report);
    fault.phases = [{name: `${name}-start`, readback_window: name}];
    fault.readback_windows[0].name = name;
    const result = await checked(directory, fault, history);
    assert.deepEqual(result.readback_windows, [{...window, name}], 'hold-wide progress cannot hide a long gap');
    fault.readback_windows = [];
    await checked(directory, fault, history, false);
  }
  const mutations = [
    (_, h) => { h[1].readback_end = null; },
    (_, h) => { h[1].readback_end = true; },
    (_, h) => { h[1].readback_end = 4; },
    (_, h) => { h[2].start = 24; },
    r => { r.reads_verified--; },
    r => { r.readback_windows = []; },
    r => { delete r.readback_windows[0].end; },
    r => { r.readback_windows[0].start.verified[0] = 2; },
    r => { r.readback_windows[0].end.seconds = 33; },
    r => { r.readback_windows[0].target_seconds = null; },
    r => { r.readback_windows.push(r.readback_windows[0]); },
  ];
  for (const mutate of mutations) {
    const r = structuredClone(report), h = structuredClone(history);
    mutate(r, h);
    await checked(directory, r, h, false);
  }
  const old = await checked(directory, {...report, schema: 1, readback_windows: undefined},
    history.map(({readback_end, ...record}) => record));
  assert.equal(old.histories, 4);
  assert.equal(old.readback_windows, undefined);
});

test('workload timestamps only validated responses after delayed readbacks', {timeout: 15000}, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'durable-db-workload-'));
  t.after(() => rm(directory, {recursive: true}));
  await Promise.all(['admin', 'control'].map(name => writeFile(join(directory, `${name}.token`), name)));
  const rows = new Map(), held = [], retried = new Set(), sockets = new Set();
  let corrupt = false;
  const server = createServer(async (request, response) => {
    sockets.add(request.socket);
    let raw = '';
    for await (const chunk of request) raw += chunk;
    const body = raw ? JSON.parse(raw) : {};
    const [, database, operation] = request.url.match(/\/databases\/([^/]+)(?:\/(.+))?/) ?? [];
    const data = rows.get(database) ?? [];
    const reply = result => response.end(JSON.stringify(result));
    if (request.url === '/v1/tokens') reply({token: 'writer'});
    else if (operation === 'commit') {
      data.push(...body.writes); rows.set(database, data); reply({revision: data.length});
    } else if (operation === 'read') {
      if (body.keys && !retried.has(database)) {
        retried.add(database); response.writeHead(503).end('{}'); return;
      }
      const result = body.scans ? {revision: data.length, scans: [{items: data, next: null}]} :
        {revision: data.length, values: body.keys.map(key => data.find(row => row.key === key).value)};
      if (corrupt && body.keys && database.endsWith('-0')) result.values = ['corrupt'];
      if (body.keys && data.length === 2) held.push(() => reply(result));
      else reply(result);
    } else reply({});
  }).listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const env = {...process.env, DATABASE_URL: `http://127.0.0.1:${server.address().port}`,
    DATABASE_PREFIX: 'test', CREDENTIALS_DIRECTORY: directory, CONTROL_PORT: '0'};
  delete env.WRITER_ORIGINS;
  const child = fork(new URL('./cluster-workload.mjs', import.meta.url), [], {silent: true,
    env});
  t.after(async () => { if (child.exitCode === null) { child.kill(); await once(child, 'exit'); } });
  const [{control_port: port}] = await once(child, 'message');
  const call = async (path, method = 'GET') => {
    const response = await fetch(`http://127.0.0.1:${port}${path}`, {method, headers: {authorization: 'Bearer control'}});
    assert.equal(response.status, 200);
    return response.json();
  };
  const until = async condition => {
    for (let i = 0; i < 200; i++) { if (await condition()) return; await sleep(10); }
    assert.fail('Workload did not reach the test barrier');
  };
  await until(async () => (await call('/status')).state === 'paused');
  await call('/resume', 'POST');
  await until(() => held.length === 4);
  const pending = await call('/status?history=1');
  assert.equal(pending.writes_acknowledged, 8);
  assert.equal(pending.reads_verified, 4);
  assert.ok(pending.history.filter(r => r.revision === 2).every(r => r.readback_end === null));
  const point = async () => { const s = await call('/status'); return {seconds: s.monotonic_seconds, verified: s.verified}; };
  const start = await point();
  await sleep(30);
  held.forEach(release => release());
  await until(async () => (await call('/status')).verified.every(count => count >= 3));
  await call('/pause', 'POST');
  const end = await point();
  await call('/verify', 'POST');
  const state = await call('/status?history=1');
  assert.ok(state.history.filter(r => r.revision === 2).every(r => r.readback_end >= start.seconds));
  for (const record of state.history) {
    assert.deepEqual(record.commit_rpc.statuses, {'200': 1});
    assert.deepEqual(record.read_rpc.statuses, record.revision === 1 ? {'503': 1, '200': 1} : {'200': 1});
    if (record.revision === 1) assert.ok(record.read_rpc.backoff_seconds >= 0.09);
    if (record.revision === 2) assert.ok(record.read_rpc.request_seconds >= 0.03);
    for (const [timing, duration] of [[record.commit_rpc, record.end - record.start],
      [record.read_rpc, record.readback_end - record.end]]) {
      assert.ok(timing.request_seconds > 0);
      assert.ok(timing.longest_request.seconds > 0 && timing.longest_request.seconds <= timing.request_seconds);
      assert.ok(timing.longest_request.status in timing.statuses);
      assert.ok(timing.request_seconds + timing.backoff_seconds <= duration);
      const counts = {};
      for (const [id, statuses] of Object.entries(timing.connections)) {
        assert.match(id, /^[1-9][0-9]*$/);
        assert.ok(Number(id) <= sockets.size, 'Connection identity must describe a socket, not a request');
        for (const [status, count] of Object.entries(statuses)) counts[status] = (counts[status] ?? 0) + count;
      }
      assert.deepEqual(counts, timing.statuses);
    }
  }
  const uses = state.history.flatMap(r => [r.commit_rpc, r.read_rpc]).flatMap(r => Object.keys(r.connections));
  assert.ok(new Set(uses).size < uses.length, 'Fixture must observe actual connection reuse');
  assert.deepEqual(state.failures, []);
  const result = await checked(directory, reportFor(state.history, {name: windowName, start, end, target_seconds: 60}), state.history);
  assert.equal(result.histories, 4);
  assert.equal(result.readback_windows[0].observed_within_target, true);
  corrupt = true;
  await call('/resume', 'POST');
  await until(async () => (await call('/status')).failures.length > 0);
  const failed = await call('/status?history=1');
  assert.equal(failed.history.filter(r => r.database_index === 0).at(-1).readback_end, null);
  assert.equal(failed.failures[0].reason, 'Read after acknowledgement mismatch');
});
