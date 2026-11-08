# Clients and pi-durable

`clients/javascript/client.mjs` is a small fetch-based client. It keeps revision and request-ID semantics explicit. The initial API works with any HTTP client.

```js
import { DurableDb, encode } from './client.mjs';
const db = new DurableDb({ url, database: 'demo', token });
const snapshot = await db.read({ keys: ['counter'] });
const operation = {
  expected_revision: snapshot.revision,
  request_id: crypto.randomUUID(),
  writes: [{ key: 'message', value: encode('Hello') }],
};
await db.commit(operation);
// On an unknown transport outcome, retry operation unchanged.
```

## Pi 1.5.0 reference adapter

```js
import { openReferencePiStorage } from './pi-reference.mjs';
const storage = await openReferencePiStorage(db);
// Pass storage to Harness.open(storage, options, context).
```

This adapter verifies end-to-end storage semantics using Pi's `MemoryStorage` as an execution reference and durable-db as authority. Commit batches are chunked into bounded KV values and atomically written with their metadata. Pi receives durability receipts only after the database commits. A storage owner token fences old Sessions; normal open refuses an existing owner. After a crashed owner, explicitly pass `takeover: true`; the old owner can no longer persist changes.

The adapter replays a log into memory and stages a detached reference state before publication. Its default log budget is 8 MiB. It is suitable for conformance, small examples, and validating crash/reopen behavior. It is **not** the final production storage adapter: open/staging work grows with history, and the log budget eventually stops writes. The database engine itself does not use this full-history design.

The production adapter will map conversations, entries, tasks, submissions, documents, document revisions, and secondary indexes directly to ordered keys. It must pass the same Pi conformance suite, implement fork-aware entry reads and document materialization, and use a single database-scoped commit for each valid group prefix. Its Session ownership fence must remain independent of API-pod membership.
