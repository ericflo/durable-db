# JavaScript and TypeScript client

`@durable-db/client` is the TypeScript client for durable-db's [HTTP API](../../docs/api.md). It has no runtime dependencies and uses the global `fetch`, so it runs on Node.js 22 or later and other runtimes with `fetch` and Web Crypto. It also contains a storage adapter for [pi-durable](#pi-durable-storage).

The package is not on npm. Build it in this repository and add it to an application by path:

```sh
npm ci --prefix clients/javascript     # installs and builds dist/
npm install ./clients/javascript       # run in your application, with the path to this directory
```

## Quick start

Start a server and create a database. `create` prints a token that can read and write it.

```sh
durable-db serve --data ./data
export DURABLE_DB_TOKEN=$(durable-db create demo)
```

```ts
import { Database } from '@durable-db/client';

const db = new Database({
  url: 'http://127.0.0.1:7654',
  database: 'demo',
  token: process.env.DURABLE_DB_TOKEN!,
});

await db.set('users/ada', { name: 'Ada', logins: 1 });
const ada = await db.get<{ name: string; logins: number }>('users/ada');
await db.delete('users/ada');
```

A key is a string of 1 to 1,024 bytes. A value is any JSON except `null`, up to 4 MiB. `get` returns `undefined` for a key that does not exist. The type argument describes what your application stored; the client does not validate it. Keep tokens on the server, never in a browser bundle.

| Method | Result |
| --- | --- |
| `get(key)` | The value, or `undefined`. |
| `getEntry(key)` | `{ key, value, version }`, or `undefined`. The version is the revision of the transaction that last wrote the key. |
| `getMany(keys)` | The values in order, all read at one revision. |
| `set(key, value)`, `delete(key)` | `{ revision }`: the database revision the write created. |
| `list(options)` | One page: `{ items, cursor, revision }`. |
| `scan(options)` | An async iterator over every matching entry. |
| `transaction(callback)` | The callback's result, after its writes are committed. |
| `commit(commit)`, `commitAll(commits)` | Atomic writes with checks you choose. |
| `read({ keys, scans })` | Any number of keys and scans in one round trip, at one revision. |

## Listing

Keys are ordered by their UTF-8 bytes. A range is selected by `prefix`, `start` (inclusive) and `end` (exclusive), in any combination.

```ts
const page = await db.list<{ title: string }>({ prefix: 'tasks/', limit: 20 });
for (const { key, value, version } of page.items) console.log(key, value.title, version);

if (page.cursor !== undefined) {
  const next = await db.list({ prefix: 'tasks/', limit: 20, cursor: page.cursor });
}

for await (const entry of db.scan({ prefix: 'tasks/', reverse: true })) {
  console.log(entry.key);
}
```

`limit` defaults to 100 and is at most 10,000. `cursor` is the last key of the page, present only when more keys remain. Pages are read independently, so a key written between two pages can appear in the later one. `scan` fetches pages of 1,000 as the loop consumes them; its `limit` bounds the total.

## Transactions

```ts
await db.transaction(async (tx) => {
  const from = (await tx.get<number>('accounts/a')) ?? 0;
  const to = (await tx.get<number>('accounts/b')) ?? 0;
  if (from < 30) throw new Error('insufficient funds');
  tx.set('accounts/a', from - 30);
  tx.set('accounts/b', to + 30);
});
```

The callback reads with `tx.get`, `tx.getMany` and `tx.list`, and buffers writes with `tx.set` and `tx.delete`. Reads see the callback's own buffered writes. When the callback returns, the client commits the writes with a version check for every key the callback read and a range check for every range it listed. The commit succeeds only if none of them changed, so the result is the one the callback would have produced running alone.

If something the callback read did change, nothing is written, and the callback runs again after a short random delay, up to `maxAttempts` times (default 10), after which `transaction` throws `ConflictError`. The server's conflict names a revision that includes the change, and the rerun reads at that revision or later, so it sees what caused the conflict. Because the callback can run more than once, send emails and charge cards outside it. If the callback throws, nothing is written and the error is passed on. A callback that buffers no write commits nothing.

Two reads in one callback are separate requests, so while another client is writing they can return values from different revisions. Such a transaction never commits: its checks fail and the callback runs again. Do not treat a mismatch between two values read in a callback as corrupt data.

## Commit with your own checks

`commit` applies one atomic transaction: every write, or none.

```ts
const entry = await db.getEntry<{ stock: number }>('items/42');
if (entry === undefined) throw new Error('no such item');

await db.commit({
  checks: [
    { key: 'items/42', version: entry.version }, // unchanged since it was read
    { key: 'orders/1001', version: 0 }, // does not exist yet
  ],
  writes: [
    { key: 'items/42', value: { stock: entry.value.stock - 1 } },
    { key: 'orders/1001', value: { item: 42 } },
    { key: 'carts/7', delete: true },
  ],
});
```

When a check fails, `commit` throws `ConflictError` and nothing is written. The checks are:

| Check | Passes while |
| --- | --- |
| `{ key, version }` | The key is at this version. Version 0 means it does not exist. |
| `{ range: { prefix, start, end }, revision }` | No key in the range was written after this revision. Use the `revision` of the `list` or `read` that scanned it. |
| `ifRevision` on the commit | The whole database is at this revision. |

A transaction holds at most 10,000 writes and checks. `commitAll([...])` sends several transactions in one round trip. They apply in order, each atomic and with its own revision, and it returns one `{ revision }` or `{ error }` per transaction. The first failure stops the rest.

Every commit carries an idempotency key, by default a random one. Pass `idempotencyKey` to make your own retries safe: a commit repeated with the same key and the same contents returns the first result instead of applying again.

## Consistency

Reads are `strong` by default: they reflect every transaction acknowledged before the read was sent, at the cost of one storage round trip on the server. A `session` read is served from the server's memory.

```ts
const fast = new Database({ url, database, token, consistency: 'session' });
await fast.get('settings'); // session
await fast.get('settings', { consistency: 'strong' }); // one strong read
```

The client remembers the highest revision it has seen, in `db.revision`, and sends it with every session read. A session read therefore always includes this client's own writes and never goes back in time. It can miss a write another client made a moment ago. Reads inside `transaction` are session reads; the checks at commit make that safe.

## Errors and retries

Every failed request throws a `DatabaseError` with `status`, `code` (the server's error code), `retryable` and `message`.

| Class | Status | Meaning |
| --- | --- | --- |
| `InvalidError` | 400 | The request is malformed or exceeds a limit. |
| `UnauthorizedError` | 401 | The token is missing, forged, expired or revoked. |
| `ForbiddenError` | 403 | The token does not cover this database or action. |
| `NotFoundError` | 404, 410 | No such database, or it was deleted. |
| `ConflictError` | 409 | A check failed. Read again and recompute. |
| `RateLimitError` | 429 | The database's request budget is spent. |
| `UnavailableError` | 503 | The server could not complete the request now. |
| `QuotaError` | 507 | The database is at its storage quota. Deletes still succeed. |
| `NetworkError` | none | No response arrived: the connection failed or timed out. |

```ts
import { ConflictError, QuotaError } from '@durable-db/client';

try {
  await db.commit({ checks: [{ key: 'slug/hello', version: 0 }], writes: [{ key: 'slug/hello', value: 1 }] });
} catch (error) {
  if (error instanceof ConflictError) console.log('the slug is taken');
  else if (error instanceof QuotaError) console.log('the database is full');
  else throw error;
}
```

The client repeats the identical request after a network error, a `503`, or a `429` (after the server's `Retry-After`), up to `maxRetries` times (default 5) with exponential backoff and jitter. This is safe for commits because of the idempotency key. Each attempt is abandoned after `timeoutMs` (default 30,000). A `Retry-After` longer than 10 seconds is not waited for; the `RateLimitError` carries it as `retryAfterMs`.

If a commit still fails with `NetworkError` or `UnavailableError`, the server may or may not have applied it. The error then has `outcomeUnknown: true` and `commit`, the commit that was sent with its idempotency key. This applies to `set`, `delete` and `transaction` too. `await db.commit(error.commit)` settles it: it returns the revision if the commit was or now is applied, and throws `ConflictError` if a check no longer holds.

`new Database({ ..., fetch })` takes a custom `fetch`, for example to add tracing.

## Administration

`Admin` manages databases with an administrator token (`durable-db token --admin`).

```ts
import { Admin } from '@durable-db/client';

const admin = new Admin({ url: 'http://127.0.0.1:7654', token: process.env.DURABLE_DB_ADMIN_TOKEN! });
await admin.createDatabase('reports', { maxBytes: 64 * 1024 * 1024 });
const token = await admin.createToken('reports', { access: 'read', ttlSeconds: 3600 });
const { revision, bytes } = await admin.getDatabase('reports');
await admin.deleteDatabase('reports');
```

`createDatabase` succeeds if the database already exists with the same limits. A deleted database's name cannot be used again.

## Testing your application

`@durable-db/client/testing` starts a throwaway server for tests. It runs the `durable-db` executable passed as `binary`, else the one named by `DURABLE_DB_BINARY`, else the one on the `PATH`.

```ts
import { startServer } from '@durable-db/client/testing';

const server = await startServer(); // durable-db serve on a free local port, in a temporary directory
const db = await server.createDatabase(); // a new database and a client with a write token
await db.set('greeting', 'hello');
await server.stop(); // stops the server and deletes its data
```

The [task board example](../../examples/web-app/README.md) uses it.

## pi-durable storage

`@durable-db/client/pi` implements the `Storage` interface of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) 1.6.1 on one database, so a pi-durable Session or Harness keeps its conversations, tasks and documents in durable-db. Install pi-durable 1.6.1 in the application; this package lists it as an optional peer dependency.

```ts
import { Database } from '@durable-db/client';
import { openPiStorage } from '@durable-db/client/pi';
import { Harness } from '@earendil-works/pi-durable';

const db = new Database({ url, database: 'agent-1', token });
const storage = await openPiStorage(db);
const harness = await Harness.open(storage, { models, registry }, context);
```

`models`, `registry` and `context` are pi-durable's own; [test/pi-harness.test.ts](test/pi-harness.test.ts) is a complete setup with a scripted model.

**One owner.** A pi-durable Session is the only writer of its storage. `openPiStorage` records an owner under the storage's prefix and fails if another owner is recorded. `storage.close()`, which `Harness.close()` calls, clears it. After a process dies without closing, open with `openPiStorage(db, { takeover: true })`. A takeover is safe while the old process is still running: every transaction the adapter sends checks the owner, so the old process's next commit fails, and its next read reports the takeover. Decide in your host when to take over; the adapter never does it by itself.

**Prefix.** All keys begin with `prefix` (default `pi/`), so one database can hold several storages, or a storage beside your own keys.

**Round trips.** Reads are session reads. One request serves each of these: a task, a conversation, a submission by ID or by request ID, the current document at an address, a document without history, the newest head marker, and a page of a conversation's history, including what it inherits from the conversations it was forked from. An entry looked up by ID alone is one request when the adapter stored or read that entry since it was opened, otherwise two. Scans of conversations, tasks, submissions and documents take two requests per pass, and so does a document with history. The first read through a conversation the adapter has not seen costs one more request per unseen ancestor.

A commit is one request when it only adds conversations, entries, tasks and submissions with newly minted IDs. Otherwise it is two: one read for everything its checks need, then one request that stores every batch of `Storage.commit(batches)` as its own transaction. Opening takes three requests, one of them a strong read.

**Limits.** A record or document value is stored as one JSON value, so it is limited by the server's value size (4 MiB by default). One batch is one transaction, limited to 10,000 writes; an entry takes two or three, a new task five. A commit the server refuses for such a limit rejects, which ends the Session, but leaves the storage consistent and closable.

**Reads beside commits.** Reads do not wait for a commit that is being written. A read that needed more than one request while a commit of the same storage was written runs again, so its result never mixes the states before and after that commit.

The header of [src/pi/index.ts](src/pi/index.ts) lists the keys the adapter writes.

## Development

```sh
npm ci                  # install and build
npm test                # SDK, retry, adapter, upstream conformance and Harness tests
npm run typecheck       # this package and examples/web-app
npm run format:check    # prettier; npm run format rewrites
npm run bench           # pi-durable's storage benchmarks against the adapter
```

`npm test` and `npm run bench` start a server on a free port with a temporary data directory and stop it afterwards. They run the executable named by `DURABLE_DB_BINARY`, else `target/release/durable-db` in this repository (`cargo build --release`), else `durable-db` on the `PATH`.

| Path | Contents |
| --- | --- |
| `src/database.ts` | `Database` and transactions |
| `src/http.ts`, `src/errors.ts` | Requests, retries and typed errors |
| `src/admin.ts` | `Admin` |
| `src/testing.ts` | `startServer` |
| `src/pi/` | The pi-durable storage adapter |
| `test/` | Tests; `test/pi-conformance.test.ts` runs pi-durable's complete storage conformance suite |
| `bench/pi.ts` | The adapter benchmark |
