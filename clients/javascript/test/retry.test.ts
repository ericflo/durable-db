import { describe, expect, it } from 'vitest';
import {
  ConflictError,
  Database,
  InvalidError,
  NetworkError,
  RateLimitError,
  UnavailableError,
} from '../src/index.ts';
import { createDatabase, recordingFetch, type Call } from './fixture.ts';

const failure = (status: number, code: string, headers: Record<string, string> = {}) =>
  new Response(
    JSON.stringify({ error: { code, message: code, retryable: status === 429 || status === 503 } }),
    {
      status,
      headers,
    },
  );

/** A client whose first `count` requests matching `match` are answered by `respond` instead. */
async function intercepted(
  count: number,
  respond: (call: Call, send: () => Promise<Response>) => Promise<Response>,
  match: (call: Call) => boolean = () => true,
  options: { maxRetries?: number } = {},
) {
  const { open, db } = await createDatabase();
  let left = count;
  const recorder = recordingFetch((call, send) =>
    match(call) && left-- > 0 ? respond(call, send) : send(),
  );
  return { db: open({ fetch: recorder.fetch, ...options }), plain: db, calls: recorder.calls };
}

describe('retries', () => {
  it('repeats the identical commit after a lost response, and it applies once', async () => {
    const { db, plain, calls } = await intercepted(1, async (_call, send) => {
      await send(); // The server applies the commit, then the response is lost.
      throw new TypeError('socket closed');
    });
    const result = await db.set('count', 1);
    expect(result).toEqual({ revision: 1 });
    expect(calls).toHaveLength(2);
    expect(calls[1]!.body).toEqual(calls[0]!.body);
    expect(calls[0]!.body.idempotency_key).toMatch(/^[0-9a-f-]{36}$/);
    expect(await plain.getEntry('count')).toEqual({ key: 'count', value: 1, version: 1 });
    expect((await plain.set('next', 1)).revision).toBe(2);
  });

  it('does not rerun a transaction callback when only the commit response was lost', async () => {
    const { db, plain, calls } = await intercepted(
      1,
      async (_call, send) => {
        await send();
        throw new TypeError('socket closed');
      },
      (call) => call.path === '/commit',
    );
    let runs = 0;
    await db.transaction(async (tx) => {
      runs++;
      tx.set('count', ((await tx.get<number>('count')) ?? 0) + 1);
    });
    expect(runs).toBe(1);
    expect(calls.map((call) => call.path)).toEqual(['/read', '/commit', '/commit']);
    expect(await plain.get('count')).toBe(1);
  });

  it('retries 503 and a network error on reads and commits', async () => {
    const reads = await intercepted(2, async (call) => {
      if (reads.calls.length === 1) throw new TypeError('connection refused');
      return failure(503, 'busy');
    });
    expect(await reads.db.get('a')).toBeUndefined();
    expect(reads.calls.map((call) => call.status)).toEqual([0, 503, 200]);

    const commits = await intercepted(2, async () => failure(503, 'storage_unavailable'));
    expect(await commits.db.set('a', 1)).toEqual({ revision: 1 });
    expect(commits.calls.map((call) => call.status)).toEqual([503, 503, 200]);
    expect(new Set(commits.calls.map((call) => call.body.idempotency_key)).size).toBe(1);
  });

  it('waits for Retry-After on 429, and returns a long wait to the caller', async () => {
    const short = await intercepted(1, async () =>
      failure(429, 'rate_limited', { 'retry-after': '0.2' }),
    );
    const started = performance.now();
    expect(await short.db.get('a')).toBeUndefined();
    expect(performance.now() - started).toBeGreaterThanOrEqual(190);
    expect(short.calls.map((call) => call.status)).toEqual([429, 200]);

    const long = await intercepted(1, async () =>
      failure(429, 'rate_limited', { 'retry-after': '60' }),
    );
    const refused = long.db.set('a', 1);
    await expect(refused).rejects.toBeInstanceOf(RateLimitError);
    await expect(refused).rejects.toMatchObject({
      retryAfterMs: 60_000,
      retryable: true,
      outcomeUnknown: false,
    });
    expect(long.calls).toHaveLength(1);
  });

  it('does not retry an error that repeating cannot fix', async () => {
    const { db, calls } = await intercepted(0, async (_call, send) => send());
    await expect(db.set('', 1)).rejects.toBeInstanceOf(InvalidError);
    await db.set('a', 1);
    await expect(
      db.commit({ checks: [{ key: 'a', version: 0 }], writes: [{ key: 'a', value: 2 }] }),
    ).rejects.toBeInstanceOf(ConflictError);
    expect(calls.map((call) => call.status)).toEqual([400, 200, 409]);
  });

  it('gives up after maxRetries and says whether a commit may have been applied', async () => {
    const down = await intercepted(
      Infinity,
      async () => Promise.reject(new TypeError('connection refused')),
      undefined,
      {
        maxRetries: 2,
      },
    );
    const read = down.db.get('a');
    await expect(read).rejects.toBeInstanceOf(NetworkError);
    await expect(read).rejects.toMatchObject({ status: 0, code: 'network', outcomeUnknown: false });
    expect(down.calls).toHaveLength(3);
    const commit = down.db.set('a', 1);
    await expect(commit).rejects.toBeInstanceOf(NetworkError);
    await expect(commit).rejects.toMatchObject({ outcomeUnknown: true });
    expect((await commit.catch((error) => error)).cause).toBeInstanceOf(TypeError);
    expect(down.calls).toHaveLength(6);

    const busy = await intercepted(Infinity, async () => failure(503, 'busy'), undefined, {
      maxRetries: 1,
    });
    const unavailable = busy.db.set('a', 1);
    await expect(unavailable).rejects.toBeInstanceOf(UnavailableError);
    await expect(unavailable).rejects.toMatchObject({
      status: 503,
      code: 'busy',
      outcomeUnknown: true,
    });
    expect(await busy.plain.get('a')).toBeUndefined();
  });

  it('settles an unknown outcome by committing the same commit again', async () => {
    // Every response is lost until the caller gives up; the first attempt was applied.
    const { db, plain } = await intercepted(
      2,
      async (_call, send) => {
        await send();
        throw new TypeError('socket closed');
      },
      (call) => call.path === '/commit',
      { maxRetries: 1 },
    );
    let runs = 0;
    const unknown = await db
      .transaction(async (tx) => tx.set('count', ((await tx.get<number>('count')) ?? 0) + ++runs))
      .catch((error) => error);
    expect(unknown).toBeInstanceOf(NetworkError);
    expect(unknown.outcomeUnknown).toBe(true);
    expect(unknown.commit.writes).toEqual([{ key: 'count', value: 1 }]);
    expect(await db.commit(unknown.commit)).toEqual({ revision: 1 });
    expect(await plain.getEntry('count')).toEqual({ key: 'count', value: 1, version: 1 });
    expect(runs).toBe(1);
  });

  it('reports a definite answer after an unknown one as definite', async () => {
    const { db, plain } = await intercepted(1, async () =>
      Promise.reject(new TypeError('socket closed')),
    );
    await plain.set('a', 1);
    const conflict = db.commit({
      checks: [{ key: 'a', version: 0 }],
      writes: [{ key: 'a', value: 2 }],
    });
    await expect(conflict).rejects.toBeInstanceOf(ConflictError);
    await expect(conflict).rejects.toMatchObject({ outcomeUnknown: false, commit: undefined });
  });

  it('abandons an attempt that exceeds the timeout', async () => {
    const stalled: typeof fetch = (_input, init) =>
      new Promise((_resolve, reject) =>
        init!.signal!.addEventListener('abort', () => reject(init!.signal!.reason)),
      );
    const db = new Database({
      url: 'http://127.0.0.1:1',
      database: 'unused',
      token: 'unused',
      fetch: stalled,
      timeoutMs: 30,
      maxRetries: 1,
    });
    const read = db.get('a');
    await expect(read).rejects.toBeInstanceOf(NetworkError);
    await expect(read).rejects.toThrow('timed out');
  });

  it('sends the token, refuses redirects, and reads errors that are not JSON', async () => {
    const seen: RequestInit[] = [];
    const proxy: typeof fetch = async (_input, init) => {
      seen.push(init!);
      return new Response('<html>Bad Gateway</html>', { status: 502 });
    };
    const db = new Database({
      url: 'http://127.0.0.1:1/',
      database: 'my db',
      token: 'secret',
      fetch: proxy,
      maxRetries: 1,
    });
    const read = db.get('a');
    await expect(read).rejects.toBeInstanceOf(UnavailableError);
    await expect(read).rejects.toMatchObject({
      status: 502,
      code: 'http_502',
      message: '<html>Bad Gateway</html>',
    });
    expect(seen).toHaveLength(2);
    expect(seen[0]!.redirect).toBe('error');
    expect(seen[0]!.headers).toMatchObject({ authorization: 'Bearer secret' });
  });
});
