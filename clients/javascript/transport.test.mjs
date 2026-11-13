import { afterEach, expect, it } from 'vitest';
import { createServer } from 'node:http';
import { createServer as tcpServer, connect } from 'node:net';
import { createServer as httpsServer } from 'node:https';
import { getCACertificates, setDefaultCACertificates } from 'node:tls';
import { execFileSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { DurableDb, DbError } from './client.mjs';
import { requestJson } from './transport.mjs';

const cleanup = [];
afterEach(async () => {
  for (const stop of cleanup.splice(0).reverse()) await stop();
});
async function listen(server) {
  const sockets = new Set();
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  cleanup.push(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
  });
  return { port: server.address().port, sockets };
}
const options = (signal) => ({
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: '{}',
  signal: signal ?? AbortSignal.timeout(3000),
});
const until = async (condition) => {
  for (let n = 0; n < 300; n++) {
    if (condition()) return;
    await sleep(5);
  }
  throw new Error('Fixture barrier timed out');
};

it('verifies HTTPS trust and host identity before sending a capability', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'durable-db-tls-'));
  cleanup.push(() => rm(directory, { recursive: true }));
  execFileSync(
    'openssl',
    [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-days',
      '1',
      '-subj',
      '/CN=fixture',
      '-addext',
      'subjectAltName=IP:127.0.0.1',
      '-keyout',
      join(directory, 'key.pem'),
      '-out',
      join(directory, 'cert.pem'),
    ],
    { stdio: 'ignore' },
  );
  const key = await readFile(join(directory, 'key.pem')),
    cert = await readFile(join(directory, 'cert.pem'));
  let received = 0;
  const server = await listen(
    httpsServer({ key, cert }, (request, response) => {
      received++;
      expect(request.headers.authorization).toBe('Bearer capability');
      request.resume();
      response.end('{}');
    }),
  );
  const config = { database: 'tls', token: 'capability', url: `https://127.0.0.1:${server.port}` };
  await expect(new DurableDb(config).read()).rejects.toBeInstanceOf(TypeError);
  expect(received).toBe(0);
  const previous = getCACertificates('default');
  setDefaultCACertificates([...previous, cert.toString()]);
  try {
    expect(await new DurableDb(config).read()).toEqual({});
    await expect(
      new DurableDb({ ...config, url: `https://localhost:${server.port}` }).read(),
    ).rejects.toBeInstanceOf(TypeError);
    expect(received).toBe(1);
  } finally {
    setDefaultCACertificates(previous);
  }
});

it('keeps database paths separate, moves after 503, and never replays a lost acknowledgement', async () => {
  const owners = { a: 0, b: 1 },
    calls = [],
    receipts = new Map();
  let lose = true,
    applications = 0,
    accepted = 0;
  const backends = [];
  for (const side of [0, 1]) {
    backends.push(
      await listen(
        createServer(async (request, response) => {
          let body = '';
          for await (const chunk of request) body += chunk;
          const database = request.url.split('/')[3];
          calls.push({ database, side, body, authorization: request.headers.authorization });
          expect(request.headers['accept-encoding']).toBe('identity');
          response.setHeader('content-type', 'application/json');
          if (owners[database] !== side) {
            response
              .writeHead(503, { connection: 'close' })
              .end(JSON.stringify({ error: 'unavailable' }));
            return;
          }
          if (request.url.endsWith('/commit')) {
            const { request_id } = JSON.parse(body);
            if (!receipts.has(request_id)) {
              receipts.set(request_id, body);
              applications++;
            }
            expect(receipts.get(request_id)).toBe(body);
            if (lose) {
              lose = false;
              response.destroy();
              return;
            }
            response.end(JSON.stringify({ revision: applications, replayed: true }));
          } else response.end(JSON.stringify({ revision: applications, values: [database] }));
        }),
      ),
    );
  }
  // A real L4 Service chooses a backend per connection, not per database.
  const gateway = await listen(
    tcpServer((socket) => {
      const upstream = connect(backends[accepted++ % 2].port, '127.0.0.1');
      socket.pipe(upstream).pipe(socket);
      socket.on('close', () => upstream.destroy());
      upstream.on('error', () => socket.destroy());
    }),
  );
  const config = (database) => ({
    url: `http://127.0.0.1:${gateway.port}`,
    database,
    token: 'token-' + database,
  });
  const a = new DurableDb(config('a')),
    b = new DurableDb(config('b'));
  for (let n = 0; n < 10; n++) {
    expect((await a.read()).values).toEqual(['a']);
    expect((await b.read()).values).toEqual(['b']);
  }
  expect(accepted).toBe(2);
  await new DurableDb({ ...config('a'), token: 'rotated' }).read();
  expect(accepted).toBe(2);
  expect(calls.at(-1).authorization).toBe('Bearer rotated');
  owners.a = 1;
  owners.b = 0;
  await expect(a.read()).rejects.toMatchObject({ status: 503 });
  const retry = async (fn) => {
    for (let n = 0; ; n++) {
      try {
        return await fn();
      } catch (e) {
        if (!(e instanceof DbError) || e.status !== 503 || n >= 3) throw e;
      }
    }
  };
  await retry(() => a.read());
  await retry(() => b.read());
  const before = calls.length;
  const intent = {
    expected_revision: 0,
    request_id: 'lost',
    writes: [{ key: 'k', value: 'dg==' }],
  };
  await expect(a.commit(intent)).rejects.toBeInstanceOf(TypeError);
  await sleep(20);
  expect(calls.length).toBe(before + 1);
  expect(applications).toBe(1);
  expect(await retry(() => a.commit(intent))).toEqual({ revision: 1, replayed: true });
  expect(applications).toBe(1);
  expect(new Set(calls.filter((c) => c.body.includes('lost')).map((c) => c.body)).size).toBe(1);
});

it('bounds active scopes, evicts only idle scopes and releases capacity after cancellation', async () => {
  const held = [];
  const server = await listen(
    createServer((request, response) => {
      request.resume();
      held.push(response);
    }),
  );
  const url = `http://127.0.0.1:${server.port}`;
  const controller = new AbortController();
  const pending = Array.from({ length: 64 }, (_, i) =>
    requestJson(url, 'scope-' + i, options(controller.signal)),
  );
  await until(() => held.length === 64);
  await expect(requestJson(url, 'overflow', options())).rejects.toMatchObject({ status: 429 });
  for (const response of held.splice(0)) response.end('{}');
  await Promise.all(pending);
  // More names than the cache bound: idle agents must close, not accumulate.
  for (let n = 0; n < 70; n++) {
    const next = requestJson(url, 'new-' + n, options());
    await until(() => held.length === 1);
    held.shift().end('{}');
    await next;
  }
  await until(() => server.sockets.size <= 64);
  const aborted = Array.from({ length: 16 }, () =>
    requestJson(url, 'busy', options(controller.signal)),
  );
  await until(() => held.length === 16);
  await expect(requestJson(url, 'busy', options())).rejects.toMatchObject({ status: 429 });
  const settled = Promise.allSettled(aborted);
  controller.abort();
  expect((await settled).every((x) => x.status === 'rejected')).toBe(true);
  held.splice(0);
  const resumed = requestJson(url, 'busy', options());
  await until(() => held.length === 1);
  held.shift().end('{}');
  expect(await resumed).toEqual({});
});

it('bounds aggregate work, response bytes and stalled bodies without retrying', async () => {
  const held = [];
  let requests = 0;
  const server = await listen(
    createServer((request, response) => {
      request.resume();
      requests++;
      if (request.url === '/large') response.end('x'.repeat(1024 * 1024 + 1));
      else if (request.url === '/partial') {
        response.writeHead(200);
        response.write('{');
      } else held.push(response);
    }),
  );
  const url = `http://127.0.0.1:${server.port}`;
  const pending = Array.from({ length: 128 }, (_, i) =>
    requestJson(url, 'aggregate-' + (i % 8), options()),
  );
  await until(() => held.length === 128);
  await expect(requestJson(url, 'extra', options())).rejects.toMatchObject({ status: 429 });
  for (const response of held.splice(0)) response.end('{}');
  await Promise.all(pending);
  const before = requests;
  await expect(requestJson(url + '/large', 'large', options())).rejects.toBeInstanceOf(TypeError);
  await expect(
    requestJson(url + '/partial', 'partial', options(AbortSignal.timeout(50))),
  ).rejects.toBeInstanceOf(TypeError);
  expect(requests).toBe(before + 2);
});
