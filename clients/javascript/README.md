# JavaScript and TypeScript

Typed JSON storage for server-side applications. The package is private while durable-db is experimental; the [runnable task board](../../examples/web-app/README.md) links it locally. Node.js 24 is the tested runtime.

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token });

await db.set('settings', { theme: 'dark' });
const settings = await db.get<{ theme: string }>('settings');
await db.delete('draft');

const page = await db.list<Task>({ prefix: 'tasks/', limit: 20 });
const next = page.cursor
  ? await db.list<Task>({ prefix: 'tasks/', cursor: page.cursor })
  : undefined;
```

`get<T>` returns `T | undefined`; stored `null` remains `null`. Values must be plain, finite JSON. Types describe your application's schema; they do not perform runtime schema validation. Credentials belong on the application server, never in a browser bundle.

## Transactions

```ts
await db.transaction(async (tx) => {
  const count = (await tx.get<number>('page-views')) ?? 0;
  tx.set('page-views', count + 1);
  tx.set('last-view', { at: new Date().toISOString() });
});
```

All reads use one database revision. Writes become visible together after the callback completes. Reads see earlier staged writes. The transaction returns the callback's result after confirmation, and a thrown callback publishes nothing.

A definite conflict can rerun the callback, up to five attempts. Keep external side effects outside it and await every transaction read. Commit retries after a lost response reuse the identical request, without rerunning application logic. If the SDK cannot establish the outcome, `UncertainCommitError` carries that original request for reconciliation. Do not blindly rerun a non-idempotent operation after this error. `RetryLimitError` means the retry budget was exhausted without an uncertain publication. The SDK stores no extra application receipts.

For application-level replay across separate HTTP requests, use a stable business identity. The task board creates by task ID and completes only unfinished tasks, making those actions safe to repeat. More complex operations may need their own durable idempotency record.

## Bounds and pagination

| Operation | Limit |
| --- | --- |
| Key | 1–1,024 UTF-8 bytes, no NUL or unpaired surrogate |
| JSON value | 64 KiB encoded JSON; at most 64 nesting levels |
| Transaction | 128 distinct read keys, 128 write keys, 1 MiB encoded commit request |
| List | 1–1,000 records per page; default 20 |
| Retries | Five callback attempts; up to five identical commit attempts per callback; 35-second timeout per HTTP attempt |

Server admission limits and database quotas also apply. These are work bounds, not a wall-clock deadline for an arbitrary callback. A read can retry transient transport/service failures up to five times.

`list` returns `{ items: [{ key, value }], cursor }` in UTF-8 byte order. Cursors are opaque and bound to the prefix and revision. If any write changes the database between pages, the next page returns `DbError` with status 409. Restart the listing; cursors are not retained historical snapshots. Prefix scans and explicit indexes are the query model.

## Provisioning and binary values

`DurableDb` remains the lower-level wire client for provisioning, binary storage, adapters and explicit recovery:

```ts
import { DurableDb, encode } from '@durable-db/client';

const wire = new DurableDb({ url, database: 'my-app', token });
const snapshot = await wire.read({ keys: ['message'] });
const request = {
  expected_revision: snapshot.revision,
  request_id: crypto.randomUUID(),
  writes: [{ key: 'message', value: encode('Hello') }],
};
await wire.commit(request);
```

Retain the same request across uncertain retries. This layer makes one HTTP attempt per call and exposes base64 values. `createProtected`, `issueToken` and `changeAccess` need administrator credentials. Redirects are refused; connect to the final HTTPS origin. See the [HTTP contract](../../docs/api.md) and [Pi integration guide](../../docs/clients.md).

## Development

`src/` contains the JSON API, transaction scope, encoding and retry handling. `client.mjs` and `client.d.mts` define the wire API. `pi-indexed.mjs` and `indexed/` implement Pi storage; `pi-reference.mjs` is its reference adapter.

`npm ci` builds the SDK. `npm run format:check` and `npm run typecheck` check the public API and executable examples. From the repository root, `scripts/test-client.sh` runs HTTP, lost-response, concurrent-update, Pi and application tests. For focused SDK/example work, use `scripts/test-client.sh sdk.test.mjs`; the full suite remains the default and the Woodpecker gate.
