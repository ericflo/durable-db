# JavaScript and TypeScript client

An ESM HTTP client for durable-db, with TypeScript declarations for requests, results, database policies and both Pi adapters. The package remains private while the database is experimental. It runs in environments with `fetch`, Web Crypto and the standard encoding APIs; Node.js 24 is the tested server runtime.

```ts
import { DurableDb, encode } from './client.mjs';
import type { CommitRequest } from './client.mjs';

const database = new DurableDb({
  url: 'http://127.0.0.1:7654',
  database: 'my-application',
  token: applicationToken,
});

const snapshot = await database.read({ keys: ['settings/theme'] });
const intent: CommitRequest = {
  expected_revision: snapshot.revision,
  request_id: crypto.randomUUID(),
  writes: [{ key: 'settings/theme', value: encode('dark') }],
};
await database.commit(intent);
```

Retain `intent` before sending it. After uncertainty, retry that exact object. A definite conflict requires a fresh read and an application decision. The client makes one HTTP attempt per call; it never silently reruns application logic. Responses use base64 values and UTF-8 keys. See the [HTTP contract](../../docs/api.md) for bounds, error handling and the finite recent-receipt window.

Use a database-scoped token in server-side application code. `createProtected`, `issueToken` and `changeAccess` require an administrator credential and belong in provisioning code. Redirects are rejected; connect to the final HTTPS origin directly.

The [web application example](../../examples/web-app/README.md) covers settings, pagination, atomic record/index updates and persistent application idempotency. It includes a local API and tests with lost acknowledgements and concurrent edits. The [Pi guide](../../docs/clients.md) explains Session ownership, takeover and Harness recovery.

## Layout

- `client.mjs` / `client.d.mts`: HTTP operations and the public typed wire contract.
- `pi-indexed.mjs`: bounded indexed Pi adapter and ownership lifecycle.
- `indexed/kv.mjs`: revision-pinned reads, detached writes and chunked records.
- `indexed/records.mjs`: records and secondary indexes.
- `indexed/documents.mjs`: document histories and content.
- `pi-reference.mjs`: bounded reference adapter used for compatibility comparisons.
- `*.test.mjs`: HTTP, conformance, fault and actual Harness tests.

`npm run format:check` checks consistent formatting. `npm run typecheck` checks public declarations and executable TypeScript examples. `scripts/test-client.sh` from the repository root runs the complete local HTTP suite. Both checks are part of Woodpecker's build gate.
