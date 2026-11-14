import { describe, expect, it } from 'vitest';
import {
  ConflictError,
  Database,
  ForbiddenError,
  InvalidError,
  NotFoundError,
  QuotaError,
  UnauthorizedError,
} from '../src/index.ts';
import { compareKeys } from '../src/database.ts';
import { admin, createDatabase, recordingFetch, server } from './fixture.ts';

describe('values', () => {
  it('stores, reads and deletes JSON values', async () => {
    const { db } = await createDatabase();
    expect(await db.get('settings')).toBeUndefined();
    const { revision } = await db.set('settings', {
      theme: 'dark',
      sizes: [1, 2.5],
      nested: { on: null },
    });
    expect(revision).toBe(1);
    expect(await db.get('settings')).toEqual({
      theme: 'dark',
      sizes: [1, 2.5],
      nested: { on: null },
    });
    expect(await db.getEntry('settings')).toEqual({
      key: 'settings',
      value: { theme: 'dark', sizes: [1, 2.5], nested: { on: null } },
      version: 1,
    });
    await db.set('count', 0);
    await db.set('flag', false);
    await db.set('text', '');
    expect(await db.getMany(['count', 'missing', 'flag', 'text'])).toEqual([
      0,
      undefined,
      false,
      '',
    ]);
    expect((await db.delete('settings')).revision).toBe(5);
    expect(await db.getEntry('settings')).toBeUndefined();
    expect(db.revision).toBe(5);
  });

  it('rejects null and undefined before sending anything', async () => {
    const db = new Database({ url: 'http://127.0.0.1:1', database: 'unused', token: 'unused' });
    await expect(async () => db.set('key', null)).rejects.toThrow(TypeError);
    await expect(async () => db.set('key', undefined)).rejects.toThrow('use delete()');
  });

  it('accepts keys with slashes, spaces and non-ASCII characters', async () => {
    const { db } = await createDatabase();
    const keys = ['a/b c', 'ü/😀', 'q?x=1&y#z', '\uffff', '😀'];
    for (const key of keys) await db.set(key, key);
    expect(await db.getMany(keys)).toEqual(keys);
    // The server orders keys by UTF-8 bytes. compareKeys agrees; UTF-16 order does not.
    const listed = (await db.list()).items.map((item) => item.key);
    expect(listed).toEqual([...keys].sort(compareKeys));
    expect(listed).not.toEqual([...keys].sort());
  });
});

describe('list and scan', () => {
  async function seeded() {
    const { db } = await createDatabase();
    await db.commit({
      writes: [1, 2, 3, 4, 5]
        .map((n) => ({ key: `tasks/${n}`, value: { n } }))
        .concat({ key: 'other', value: { n: 0 } }),
    });
    return db;
  }

  it('pages through a prefix with a cursor', async () => {
    const db = await seeded();
    const first = await db.list<{ n: number }>({ prefix: 'tasks/', limit: 2 });
    expect(first.items.map((item) => item.value.n)).toEqual([1, 2]);
    expect(first.items[0]).toEqual({ key: 'tasks/1', value: { n: 1 }, version: 1 });
    expect(first.cursor).toBe('tasks/2');
    expect(first.revision).toBe(1);
    const second = await db.list({ prefix: 'tasks/', limit: 2, cursor: first.cursor });
    expect(second.items.map((item) => item.key)).toEqual(['tasks/3', 'tasks/4']);
    const third = await db.list({ prefix: 'tasks/', limit: 2, cursor: second.cursor });
    expect(third.items.map((item) => item.key)).toEqual(['tasks/5']);
    expect(third.cursor).toBeUndefined();
  });

  it('selects by start, end and reverse', async () => {
    const db = await seeded();
    const keys = async (options: Parameters<Database['list']>[0]) =>
      (await db.list(options)).items.map((item) => item.key);
    expect(await keys({ start: 'tasks/2', end: 'tasks/4' })).toEqual(['tasks/2', 'tasks/3']);
    expect(await keys({ prefix: 'tasks/', reverse: true, limit: 2 })).toEqual([
      'tasks/5',
      'tasks/4',
    ]);
    expect(await keys({ prefix: 'tasks/', reverse: true, cursor: 'tasks/4' })).toEqual([
      'tasks/3',
      'tasks/2',
      'tasks/1',
    ]);
    expect(await keys({})).toHaveLength(6);
  });

  it('iterates every entry across pages and stops at a total limit', async () => {
    const { db } = await createDatabase();
    await db.commit({
      writes: Array.from({ length: 2500 }, (_, n) => ({
        key: `n/${String(n).padStart(5, '0')}`,
        value: n,
      })),
    });
    let sum = 0;
    let count = 0;
    for await (const entry of db.scan<number>({ prefix: 'n/' })) {
      sum += entry.value;
      count++;
    }
    expect(count).toBe(2500);
    expect(sum).toBe((2499 * 2500) / 2);
    const some: string[] = [];
    for await (const entry of db.scan({ prefix: 'n/', reverse: true, limit: 3 }))
      some.push(entry.key);
    expect(some).toEqual(['n/02499', 'n/02498', 'n/02497']);
  });

  it('reads keys and scans together at one revision', async () => {
    const db = await seeded();
    const result = await db.read<{ n: number }>({
      keys: ['tasks/1', 'missing'],
      scans: [{ prefix: 'tasks/', limit: 1 }, { prefix: 'o' }],
    });
    expect(result.revision).toBe(1);
    expect(result.values).toEqual([{ key: 'tasks/1', value: { n: 1 }, version: 1 }, undefined]);
    expect(result.scans[0]).toEqual({
      items: [{ key: 'tasks/1', value: { n: 1 }, version: 1 }],
      cursor: 'tasks/1',
      revision: 1,
    });
    expect(result.scans[1]!.items.map((item) => item.key)).toEqual(['other']);
  });
});

describe('commit', () => {
  it('applies writes atomically and enforces key, range and revision checks', async () => {
    const { db } = await createDatabase();
    await db.commit({
      writes: [
        { key: 'a', value: 1 },
        { key: 'b', value: 2 },
      ],
    });
    const a = (await db.getEntry<number>('a'))!;

    await db.commit({
      checks: [
        { key: 'a', version: a.version },
        { key: 'c', version: 0 },
      ],
      writes: [
        { key: 'a', value: a.value + 1 },
        { key: 'c', value: 3 },
        { key: 'b', delete: true },
      ],
    });
    expect(await db.getMany(['a', 'b', 'c'])).toEqual([2, undefined, 3]);

    const stale = db.commit({
      checks: [{ key: 'a', version: a.version }],
      writes: [{ key: 'a', value: 99 }],
    });
    await expect(stale).rejects.toBeInstanceOf(ConflictError);
    await expect(stale).rejects.toMatchObject({ status: 409, code: 'conflict', retryable: false });
    await expect(
      db.commit({
        checks: [{ key: 'c', version: 0 }],
        writes: [
          { key: 'c', value: 4 },
          { key: 'd', value: 4 },
        ],
      }),
    ).rejects.toBeInstanceOf(ConflictError);
    expect(await db.getMany(['a', 'c', 'd'])).toEqual([2, 3, undefined]);

    const page = await db.list({ prefix: 'range/' });
    await db.set('range/1', true);
    await expect(
      db.commit({
        checks: [{ range: { prefix: 'range/' }, revision: page.revision }],
        writes: [{ key: 'e', value: 1 }],
      }),
    ).rejects.toBeInstanceOf(ConflictError);
    await db.commit({
      checks: [{ range: { prefix: 'elsewhere/' }, revision: page.revision }],
      writes: [{ key: 'e', value: 1 }],
    });

    await expect(
      db.commit({ ifRevision: 1, writes: [{ key: 'f', value: 1 }] }),
    ).rejects.toBeInstanceOf(ConflictError);
    await db.commit({ ifRevision: db.revision, writes: [{ key: 'f', value: 1 }] });
    expect(await db.get('f')).toBe(1);
  });

  it('repeats a commit with the same idempotency key without applying it twice', async () => {
    const { db } = await createDatabase();
    const commit = { writes: [{ key: 'n', value: 1 }], idempotencyKey: 'order-17' };
    const first = await db.commit(commit);
    await db.set('other', true);
    expect(await db.commit(commit)).toEqual(first);
    expect((await db.getEntry('n'))!.version).toBe(first.revision);
    await expect(
      db.commit({ writes: [{ key: 'n', value: 2 }], idempotencyKey: 'order-17' }),
    ).rejects.toBeInstanceOf(InvalidError);
  });

  it('applies several transactions in order and stops at the first failure', async () => {
    const { db } = await createDatabase();
    const outcomes = await db.commitAll([
      { writes: [{ key: 'a', value: 1 }] },
      { checks: [{ key: 'a', version: 1 }], writes: [{ key: 'b', value: 2 }] },
      { checks: [{ key: 'a', version: 0 }], writes: [{ key: 'c', value: 3 }] },
      { writes: [{ key: 'd', value: 4 }] },
    ]);
    expect(outcomes.map((outcome) => outcome.revision)).toEqual([1, 2, undefined, undefined]);
    expect(outcomes[2]!.error).toBeInstanceOf(ConflictError);
    expect(outcomes[3]!.error).toBeInstanceOf(ConflictError);
    expect(outcomes[3]!.error!.message).toContain('not attempted');
    expect(await db.getMany(['a', 'b', 'c', 'd'])).toEqual([1, 2, undefined, undefined]);
  });

  it('sends a large group of transactions in several requests', async () => {
    const recorder = recordingFetch();
    const db = (await createDatabase()).open({ fetch: recorder.fetch });
    const megabytes = (count: number) => 'x'.repeat(count * 1024 * 1024);
    const outcomes = await db.commitAll([
      ...[0, 1, 2, 3, 4].map((n) => ({ writes: [{ key: `big/${n}`, value: megabytes(3) }] })),
      // Over the server's 4 MiB value limit: this request is refused, the earlier ones stand.
      { writes: [{ key: 'too-big', value: megabytes(5) }] },
      { writes: [{ key: 'after', value: 1 }] },
    ]);
    expect(recorder.calls.map((call) => call.body.transactions.length)).toEqual([2, 2, 1, 2]);
    expect(outcomes.map((outcome) => outcome.revision)).toEqual([
      1,
      2,
      3,
      4,
      5,
      undefined,
      undefined,
    ]);
    expect(outcomes[5]!.error).toBeInstanceOf(InvalidError);
    expect(((await db.get<string>('big/4')) ?? '').length).toBe(megabytes(3).length);
    expect(await db.getMany(['too-big', 'after'])).toEqual([undefined, undefined]);
  });
});

describe('transaction', () => {
  it('commits buffered writes together and reads its own writes', async () => {
    const { db } = await createDatabase();
    await db.commit({
      writes: [
        { key: 'tasks/1', value: 'one' },
        { key: 'tasks/3', value: 'three' },
      ],
    });
    const result = await db.transaction(async (tx) => {
      expect(await tx.get('tasks/1')).toBe('one');
      tx.set('tasks/2', 'two');
      tx.set('tasks/1', 'uno');
      tx.delete('tasks/3');
      expect(await tx.getMany(['tasks/1', 'tasks/2', 'tasks/3'])).toEqual([
        'uno',
        'two',
        undefined,
      ]);
      expect(await tx.list({ prefix: 'tasks/' })).toEqual({
        items: [
          { key: 'tasks/1', value: 'uno' },
          { key: 'tasks/2', value: 'two' },
        ],
      });
      // Nothing is visible outside before the callback returns.
      expect(await db.get('tasks/2')).toBeUndefined();
      return 'done';
    });
    expect(result).toBe('done');
    expect((await db.list({ prefix: 'tasks/' })).items.map((item) => item.value)).toEqual([
      'uno',
      'two',
    ]);
    expect(db.revision).toBe(2);
  });

  it('pages a list inside a transaction with buffered writes merged in key order', async () => {
    const { db } = await createDatabase();
    await db.commit({ writes: ['b', 'd', 'f'].map((key) => ({ key: `k/${key}`, value: key })) });
    await db.transaction(async (tx) => {
      tx.set('k/a', 'a');
      tx.set('k/c', 'c');
      tx.set('k/g', 'g');
      tx.delete('k/d');
      const first = await tx.list({ prefix: 'k/', limit: 2 });
      expect(first.items.map((item) => item.key)).toEqual(['k/a', 'k/b']);
      const second = await tx.list({ prefix: 'k/', limit: 2, cursor: first.cursor });
      expect(second.items.map((item) => item.key)).toEqual(['k/c', 'k/f']);
      const third = await tx.list({ prefix: 'k/', limit: 2, cursor: second.cursor });
      expect(third).toEqual({ items: [{ key: 'k/g', value: 'g' }] });
      const reversed = await tx.list({ prefix: 'k/', reverse: true, limit: 3 });
      expect(reversed.items.map((item) => item.key)).toEqual(['k/g', 'k/f', 'k/c']);
    });
  });

  it('writes nothing when the callback throws or buffers no write', async () => {
    const { db } = await createDatabase();
    await expect(
      db.transaction((tx) => {
        tx.set('a', 1);
        throw new Error('changed my mind');
      }),
    ).rejects.toThrow('changed my mind');
    expect(await db.transaction((tx) => tx.get('a'))).toBeUndefined();
    expect(db.revision).toBe(0);
  });

  it('reruns the callback when a key it read changes before the commit', async () => {
    const { db, open } = await createDatabase();
    await db.set('count', 1);
    const other = open();
    let runs = 0;
    const seen = await db.transaction(async (tx) => {
      const count = (await tx.get<number>('count')) ?? 0;
      if (++runs === 1) await other.set('count', 10);
      tx.set('count', count + 1);
      return count;
    });
    expect(runs).toBe(2);
    expect(seen).toBe(10);
    expect(await db.get('count')).toBe(11);
  });

  it('reads at the conflicting revision or later when it reruns the callback', async () => {
    const recorder = recordingFetch();
    const { db: other, open } = await createDatabase();
    const db = open({ fetch: recorder.fetch });
    await db.set('count', 1);
    let runs = 0;
    await db.transaction(async (tx) => {
      const count = (await tx.get<number>('count')) ?? 0;
      if (++runs === 1) await other.set('count', 10);
      tx.set('count', count + 1);
    });
    expect(recorder.calls.map((call) => [call.path, call.status])).toEqual([
      ['/commit', 200],
      ['/read', 200],
      ['/commit', 409],
      ['/read', 200],
      ['/commit', 200],
    ]);
    // The conflict named revision 2, so the rerun's session read asks for it: no extra request.
    expect(recorder.calls[3]!.body).toEqual({
      keys: ['count'],
      scans: [],
      consistency: 'session',
      min_revision: 2,
    });
    expect(await db.get('count')).toBe(11);
  });

  it('reruns the callback when a key appears in a range it listed', async () => {
    const { db, open } = await createDatabase();
    await db.set('tasks/1', { done: false });
    const other = open();
    let runs = 0;
    await db.transaction(async (tx) => {
      const { items } = await tx.list({ prefix: 'tasks/' });
      if (++runs === 1) await other.set('tasks/2', { done: false });
      tx.set('stats/open', items.length);
    });
    expect(runs).toBe(2);
    expect(await db.get('stats/open')).toBe(2);
  });

  it('does not rerun for a write outside the page a limited list saw', async () => {
    const { db, open } = await createDatabase();
    await db.commit({ writes: [1, 2, 3].map((n) => ({ key: `tasks/${n}`, value: n })) });
    const other = open();
    let runs = 0;
    await db.transaction(async (tx) => {
      const page = await tx.list({ prefix: 'tasks/', limit: 2 });
      if (++runs === 1) await other.set('tasks/9', 9);
      tx.set(
        'first-page',
        page.items.map((item) => item.key),
      );
    });
    expect(runs).toBe(1);
    await db.transaction(async (tx) => {
      const page = await tx.list({ prefix: 'tasks/', limit: 2 });
      if (++runs === 2) await other.set('tasks/1', 'changed');
      tx.set(
        'first-page',
        page.items.map((item) => item.value),
      );
    });
    expect(runs).toBe(3);
    expect(await db.get('first-page')).toEqual(['changed', 2]);
  });

  it('keeps a counter exact under concurrent increments', async () => {
    const { db, open } = await createDatabase();
    const clients = Array.from({ length: 8 }, () => open());
    await Promise.all(
      clients.map((client) =>
        client.transaction(
          async (tx) => tx.set('count', ((await tx.get<number>('count')) ?? 0) + 1),
          {
            maxAttempts: 50,
          },
        ),
      ),
    );
    expect(await db.get('count')).toBe(8);
  });

  it('gives up with ConflictError after maxAttempts', async () => {
    const { db, open } = await createDatabase();
    const other = open();
    let runs = 0;
    const result = db.transaction(
      async (tx) => {
        await tx.get('contended');
        await other.set('contended', ++runs);
        tx.set('contended', 'mine');
      },
      { maxAttempts: 3 },
    );
    await expect(result).rejects.toBeInstanceOf(ConflictError);
    expect(runs).toBe(3);
    expect(await db.get('contended')).toBe(3);
  });

  it('refuses to be used after the callback returns', async () => {
    const { db } = await createDatabase();
    let leaked: Parameters<Parameters<Database['transaction']>[0]>[0] | undefined;
    await db.transaction((tx) => {
      leaked = tx;
      tx.set('a', 1);
    });
    expect(() => leaked!.set('a', 2)).toThrow('finished');
    await expect(leaked!.get('a')).rejects.toThrow('finished');
  });
});

describe('consistency', () => {
  it('reads strongly by default and sends the highest revision seen on session reads', async () => {
    const recorder = recordingFetch();
    const { db, open } = await createDatabase().then(({ open }) => ({
      db: open({ fetch: recorder.fetch }),
      open,
    }));
    await db.get('a');
    expect(recorder.calls.at(-1)!.body).toEqual({ keys: ['a'], scans: [] });
    await db.set('a', 1);
    await db.set('a', 2);
    expect(await db.get('a', { consistency: 'session' })).toBe(2);
    expect(recorder.calls.at(-1)!.body).toEqual({
      keys: ['a'],
      scans: [],
      consistency: 'session',
      min_revision: 2,
    });
    await db.list({ prefix: 'a', consistency: 'session' });
    expect(recorder.calls.at(-1)!.body).toMatchObject({ consistency: 'session', min_revision: 2 });

    const session = open({ consistency: 'session', fetch: recorder.fetch });
    expect(await session.get('a')).toBe(2);
    expect(recorder.calls.at(-1)!.body).toMatchObject({ consistency: 'session', min_revision: 0 });
    expect(session.revision).toBe(2);
    await session.get('a', { consistency: 'strong' });
    expect(recorder.calls.at(-1)!.body).toEqual({ keys: ['a'], scans: [] });
  });

  it('reads inside a transaction with session consistency', async () => {
    const recorder = recordingFetch();
    const db = (await createDatabase()).open({ fetch: recorder.fetch });
    await db.set('a', 1);
    await db.transaction(async (tx) => {
      tx.set(
        'b',
        ((await tx.get<number>('a')) ?? 0) + (await tx.list({ prefix: 'a' })).items.length,
      );
    });
    const [, get, list, commit] = recorder.calls;
    expect(get!.body).toMatchObject({ keys: ['a'], consistency: 'session', min_revision: 1 });
    expect(list!.body).toMatchObject({
      scans: [{ prefix: 'a' }],
      consistency: 'session',
      min_revision: 1,
    });
    expect(commit!.body.checks).toEqual([
      { key: 'a', version: 1 },
      { range: { prefix: 'a' }, revision: 1 },
    ]);
    expect(commit!.body.idempotency_key).toMatch(/^[0-9a-f-]{36}$/);
    expect(await db.get('b')).toBe(2);
  });
});

describe('errors', () => {
  it('maps the server error codes to typed errors', async () => {
    const { db, database, url } = await createDatabase({ maxBytes: 4096 });
    const missing = new Database({ url, database: 'no-such-database', token: server.adminToken });
    await expect(missing.get('a')).rejects.toBeInstanceOf(NotFoundError);
    await expect(missing.get('a')).rejects.toMatchObject({ status: 404, code: 'not_found' });

    const forged = new Database({ url, database, token: 'ddb_forged' });
    await expect(forged.get('a')).rejects.toBeInstanceOf(UnauthorizedError);

    const reader = new Database({
      url,
      database,
      token: await admin.createToken(database, { access: 'read' }),
    });
    expect(await reader.get('a')).toBeUndefined();
    await expect(reader.set('a', 1)).rejects.toBeInstanceOf(ForbiddenError);

    await expect(db.set('', 1)).rejects.toBeInstanceOf(InvalidError);
    await expect(db.set('', 1)).rejects.toThrow('a key is 1 to 1024 bytes');
    await expect(db.set('big', 'x'.repeat(8192))).rejects.toBeInstanceOf(QuotaError);
    await expect(db.set('big', 'x'.repeat(8192))).rejects.toMatchObject({
      status: 507,
      outcomeUnknown: false,
    });
  });

  it('creates, describes and deletes databases', async () => {
    const name = `admin-${crypto.randomUUID()}`;
    expect(await admin.createDatabase(name, { maxBytes: 1024 * 1024 })).toEqual({
      name,
      revision: 0,
      bytes: 0,
      maxBytes: 1024 * 1024,
      maxRequestsPerSecond: 0,
    });
    await admin.createDatabase(name, { maxBytes: 1024 * 1024 });
    await expect(admin.createDatabase(name, { maxBytes: 2048 })).rejects.toMatchObject({
      status: 409,
      code: 'exists',
    });
    const db = new Database({
      url: server.url,
      database: name,
      token: await admin.createToken(name, { ttlSeconds: 60 }),
    });
    await db.set('a', 1);
    expect((await admin.getDatabase(name)).revision).toBe(1);
    await admin.deleteDatabase(name);
    await expect(db.get('a')).rejects.toMatchObject({ status: 410, code: 'deleted' });
    await expect(db.get('a')).rejects.toBeInstanceOf(NotFoundError);
  });
});
