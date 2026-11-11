import { afterEach, describe, expect, it } from 'vitest';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createDatabase, url } from './database-fixture.mjs';
import { Database, UncertainCommitError } from './dist/index.js';
import { TransactionScope } from './dist/transaction.js';
import { encode } from './client.mjs';

const cleanups = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});
const suite = url ? describe : describe.skip;
async function fixture(intercept) {
  const { client: raw, config } = await createDatabase('sdk');
  let endpoint = url;
  if (intercept) {
    const server = createServer(async (request, response) => {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      if (request.url.endsWith('/commit') && (await intercept({ body, response, raw }))) return;
      try {
        const upstream = await fetch(url + request.url, {
          method: request.method,
          headers: {
            authorization: request.headers.authorization,
            'content-type': 'application/json',
          },
          body: request.method === 'GET' ? undefined : body,
          redirect: 'error',
        });
        response.writeHead(upstream.status, { 'content-type': 'application/json' });
        response.end(await upstream.text());
      } catch {
        response.destroy();
      }
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    endpoint = `http://127.0.0.1:${server.address().port}`;
    cleanups.push(
      () =>
        new Promise((resolve) => {
          server.close(resolve);
          server.closeAllConnections();
        }),
    );
  }
  return {
    db: new Database({ url: endpoint, database: config.database, token: config.token }),
    raw,
  };
}

suite('JSON SDK transactions', () => {
  it('stores typed JSON, reads staged writes, and deletes atomically', async () => {
    const { db } = await fixture();
    const result = await db.transaction(async (tx) => {
      tx.set('settings', { theme: 'dark', nested: ['hello', 3, null] });
      expect(await tx.get('settings')).toEqual({ theme: 'dark', nested: ['hello', 3, null] });
      tx.set('temporary', false);
      tx.delete('temporary');
      return 'saved';
    });
    expect(result).toBe('saved');
    expect(await db.get('settings')).toEqual({ theme: 'dark', nested: ['hello', 3, null] });
    expect(await db.get('temporary')).toBeUndefined();
  });
  it('retries the identical commit after a lost response without rerunning application logic', async () => {
    const bodies = [];
    let lost = false,
      callbacks = 0;
    const { db, raw } = await fixture(async ({ body, response, raw }) => {
      bodies.push(body.toString());
      if (lost) return false;
      lost = true;
      await raw.commit(JSON.parse(body));
      response.destroy();
      return true;
    });
    await db.transaction((tx) => {
      callbacks++;
      tx.set('count', 1);
    });
    expect(callbacks).toBe(1);
    expect(bodies.length).toBeGreaterThanOrEqual(2);
    expect(new Set(bodies).size).toBe(1);
    expect((await raw.info()).revision).toBe(1);
  });
  it('recomputes after a definite conflict and preserves the competing update', async () => {
    let competed = false,
      callbacks = 0;
    const { db } = await fixture(async ({ body, raw }) => {
      if (!competed) {
        competed = true;
        await raw.commit({
          expected_revision: JSON.parse(body).expected_revision,
          request_id: crypto.randomUUID(),
          writes: [{ key: 'count', value: encode('10') }],
        });
      }
      return false;
    });
    await db.transaction(async (tx) => {
      callbacks++;
      tx.set('count', ((await tx.get('count')) ?? 0) + 1);
    });
    expect(callbacks).toBe(2);
    expect(await db.get('count')).toBe(11);
  });
  it('refuses to recompute after an unknown commit followed by a missing-receipt conflict', async () => {
    let first = true,
      callbacks = 0;
    const { db, raw } = await fixture(async ({ body, response, raw }) => {
      if (first) {
        first = false;
        await raw.commit(JSON.parse(body));
        response.destroy();
      } else {
        // Simulate a late retry after bounded HTTP receipt eviction.
        response.writeHead(409, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ error: 'conflict' }));
      }
      return true;
    });
    const result = db.transaction((tx) => {
      callbacks++;
      tx.set('count', callbacks);
    });
    await expect(result).rejects.toBeInstanceOf(UncertainCommitError);
    expect(callbacks).toBe(1);
    expect((await raw.info()).revision).toBe(1);
  });
  it('does not recompute when an expired receipt arrives without prior local uncertainty', async () => {
    let callbacks = 0,
      requests = 0;
    const { db } = await fixture(async ({ response }) => {
      requests++;
      response.writeHead(409, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: 'receipt_expired' }));
      return true;
    });
    const result = db.transaction((tx) => {
      callbacks++;
      tx.set('count', callbacks);
    });
    await expect(result).rejects.toBeInstanceOf(UncertainCommitError);
    expect(callbacks).toBe(1);
    expect(requests).toBe(1);
  });
  it('keeps pagination consistent and rejects a cursor used for another prefix', async () => {
    const { db } = await fixture();
    await db.transaction((tx) => {
      tx.set('tasks/a', 1);
      tx.set('tasks/b', 2);
    });
    const first = await db.list({ prefix: 'tasks/', limit: 1 });
    expect(first.items).toEqual([{ key: 'tasks/a', value: 1 }]);
    expect((await db.list({ prefix: 'tasks/', cursor: first.cursor })).items).toEqual([
      { key: 'tasks/b', value: 2 },
    ]);
    await expect(db.list({ prefix: 'elsewhere/', cursor: first.cursor })).rejects.toThrow('cursor');
    await db.set('tasks/c', 3);
    await expect(db.list({ prefix: 'tasks/', cursor: first.cursor })).rejects.toMatchObject({
      status: 409,
    });
  });
  it('closes the transaction after its callback finishes', async () => {
    const { db } = await fixture();
    let escaped;
    await db.transaction((tx) => {
      escaped = tx;
      tx.set('value', true);
    });
    expect(() => escaped.set('value', false)).toThrow('finished');
    await expect(escaped.get('value')).rejects.toThrow('finished');
  });
});

it('rejects unsupported JSON before sending a write', async () => {
  const db = new Database({ url: 'http://127.0.0.1:1', database: 'invalid', token: 'fixture' });
  for (const value of [
    undefined,
    NaN,
    Infinity,
    1n,
    new Date(),
    { nested: undefined },
    new Array(2),
  ]) {
    await expect(db.set('value', value)).rejects.toThrow(/JSON/);
  }
  await expect(db.set('value', 'x'.repeat(65537))).rejects.toThrow('64 KiB');
});

it('reads JSON accessors once and preserves prototype-named JSON properties', async () => {
  const { toJson, fromJson } = await import('./dist/json.js');
  let reads = 0;
  const input = {
    get value() {
      return ++reads === 1 ? 'original' : undefined;
    },
  };
  expect(fromJson(toJson(input))).toEqual({ value: 'original' });
  expect(reads).toBe(1);
  const unusual = JSON.parse('{"__proto__":{"safe":true}}');
  expect(fromJson(toJson(unusual))).toEqual(unusual);
});

it('bounds concurrent distinct reads and shares pending reads of the same key', async () => {
  const waiting = [];
  const tx = new TransactionScope({ read: () => new Promise((resolve) => waiting.push(resolve)) });
  const reads = Array.from({ length: 128 }, (_, i) => tx.get(`key/${i}`));
  const duplicate = tx.get('key/0');
  await expect(tx.get('one-too-many')).rejects.toThrow('128 distinct');
  expect(waiting).toHaveLength(128);
  for (const resolve of waiting) resolve({ revision: 1, values: [encode('1')] });
  expect(await Promise.all([...reads, duplicate])).toEqual(Array(129).fill(1));
});

it('rejects concurrent reads that observed different revisions', async () => {
  let revision = 0;
  const tx = new TransactionScope({
    read: async () => ({ revision: ++revision, values: [encode('1')] }),
  });
  await expect(Promise.all([tx.get('a'), tx.get('b')])).rejects.toMatchObject({ status: 409 });
});
