# General web application storage

The repository includes a [TypeScript web application example](https://github.com/ericflo/durable-db/tree/main/examples/web-app) with a local HTTP API and real-database integration tests. Repository access is required while the source is private. Its storage layer demonstrates settings, a task board, explicit indexes and pagination; it uses Node.js 24 without a TypeScript build step.

```ts
import { DurableDb } from '../../clients/javascript/client.mjs';
import { TaskStore } from './storage.ts';

const database = new DurableDb({
  url: 'http://127.0.0.1:7654',
  database: 'my-application',
  token: scopedApplicationToken,
});
const tasks = new TaskStore(database);

// Retain this identity once per user intent, across HTTP retries.
const requestId = crypto.randomUUID();
const created = await tasks.createTask(requestId, 'Draft the release notes');
const page = await tasks.listOpen(undefined, 20);
```

The example keeps database credentials on the application server. A scoped token permits access to its database; application authentication and per-user authorization remain the web application's responsibility.

## Records and atomic changes

Creating a task writes its record, its open-task index entry and an application receipt in one transaction. Completing it changes the record and removes the index entry atomically. This illustrates application-maintained indexes and related-record changes without cross-database transactions.

An application receipt binds an intent ID to its input fingerprint and original result. A retry reads that receipt first. If it is absent, the transaction checks the same revision and writes the receipt with the business effect. Competing or delayed attempts cannot both publish an absent-receipt transition. Reusing an intent ID with different input fails.

These receipts consume the database's quota. Their retention must cover the application's accepted retry lifetime; deleting a receipt prematurely can make a delayed retry apply again. The pattern protects database changes, not external payments or messages. The example injects a lost successful commit response and checks that replay changes the database exactly once.

## Edits and pagination

Settings demonstrate optimistic editing: read a value and revision, prepare one request with an operation ID, then commit against that revision. A conflicting edit returns 409 so the UI can reload and reconcile. An uncertain response requires the same saved request. The database's recent HTTP receipt window is bounded, so late unresolved edits can require application reconciliation.

Open tasks use a prefix scan and record lookups at the same revision. The cursor contains the last key and revision. A write between pages invalidates the snapshot and returns 409; restart the listing. This provides explicit consistency without pretending cursors are retained historical snapshots.

## Development checks

The example's setup command saves its protected-provisioning intent before contacting the database and writes a one-hour application token to a private file. Its localhost server accepts JSON requests and keeps credentials out of responses. The example README provides complete startup and curl commands.

Public client declarations and examples are checked with TypeScript. Four HTTP integration cases cover lost responses and duplicate intents, concurrent creation and atomic index removal, stale pagination, and settings conflicts. See [clients](clients.md) for the lower-level API and Pi adapter, and [the architecture reassessment](architecture-reassessment.md) for the open performance gate. This example demonstrates semantics; it is not evidence that current cloud latency meets a production web application's needs.
