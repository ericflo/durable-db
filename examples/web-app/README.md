# A small web application's storage layer

This TypeScript example uses durable-db for application settings and a task board. It includes a local HTTP API, a typed storage module, and integration tests against the real database server. It runs on Node.js 24 without a TypeScript build step.

| Workload | Code | Database behavior |
| --- | --- | --- |
| Load and edit settings | `settings`, `prepareSettings`, `saveSettings` | An optimistic revision prevents overwriting an edit made since the page loaded |
| Create a task | `createTask` | Task, open-task index and application receipt publish atomically |
| Complete a task | `completeTask` | Task update and index removal publish atomically |
| List open tasks | `listOpen` | Bounded ordered pagination and record lookups share one revision |
| Retry an uncertain operation | `#applyOnce` | A persistent application intent ID prevents duplicate effects, including after the database's recent HTTP receipt window expires |

`storage.ts` is the part to adapt into a framework route or service. `server.ts` is a deliberately small localhost API for trying it. Production routing needs your application's authentication and user authorization; the database credential grants access to the whole database. Never ship an administrator or database credential to a browser.

## Run

From the repository root, build the current source with the documented kache setup. The original v0.1.0 binary predates protected database provisioning. Start the database:

```sh
target/debug/durable-db serve --data .tools/web-app-data
```

In another terminal, create an administrator capability file and provision the example. The setup command persists its creation identity before contacting the database, so rerunning after a failed response uses the same provisioning request.

```sh
umask 077
mkdir -p .tools/web-app
target/debug/durable-db token --key-file .tools/web-app-data/admin.key --admin \
  > .tools/web-app/admin.token
export DURABLE_DB_ADMIN_TOKEN_FILE="$PWD/.tools/web-app/admin.token"
node examples/web-app/setup.ts

export DURABLE_DB_DATABASE=web-app-example
export DURABLE_DB_TOKEN_FILE="$PWD/.tools/web-app/application.token"
node examples/web-app/server.ts
```

The application token lasts one hour. Rerun setup to issue a fresh one, then restart the example server. Set `DURABLE_DB_URL` for a remote server. Use HTTPS and a scoped application credential when moving the storage module into a deployed application.

Create a task. The browser/application must generate the UUID **once per user intent** and retain it across retries. Repeating this exact example returns the same original task:

```sh
curl http://127.0.0.1:3000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"2408c6f4-2c37-477a-b0c1-071cd9a8979a","title":"Publish the first post"}'
curl http://127.0.0.1:3000/tasks
curl http://127.0.0.1:3000/tasks/2408c6f4-2c37-477a-b0c1-071cd9a8979a/complete \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"5e15ca0f-b39b-48c9-9d9e-534331a965c6"}'
```

For settings, GET `/settings`, then send its returned `revision` along with `theme` and a fresh `requestId` to PUT `/settings`. A 409 means the UI must reload and reconcile. After a transport failure, resend the same saved settings request; do not silently read a new revision and overwrite intervening edits. HTTP receipt retention is bounded, so a late unresolved settings edit may require user reconciliation.

## Use the storage module directly

```ts
import { DurableDb } from '../../clients/javascript/client.mjs';
import { TaskStore } from './storage.ts';

const database = new DurableDb({ url, database: 'my-site', token: scopedToken });
const tasks = new TaskStore(database);

// Save this intent in the application's request state before sending it.
const intent = { requestId: crypto.randomUUID(), title: 'Draft the release notes' };
const created = await tasks.createTask(intent.requestId, intent.title);
const page = await tasks.listOpen(undefined, 20);
```

The creation/completion helpers keep an `operations/` receipt in the same transaction as the business records. This allows a fresh database request after an uncertain attempt without repeating the application effect: competing attempts can only publish against a revision where that receipt is absent. Reusing an intent ID with different input is an error. Receipts consume quota and remain until an application-defined retention policy can safely retire their identities. Do not delete them while clients may still retry. This pattern covers database effects, not external payments, emails or other services.

Pagination uses byte-ordered keys, not creation-time sorting. If any write changes the database between pages or index/record lookups, the pinned read returns 409. Restart the listing from the first page; these cursors are not retained historical snapshots. Prefix scans and explicit indexes are the query model; there is no SQL or automatic indexing.

## Check

```sh
npm ci --prefix clients/javascript
npm run --prefix clients/javascript typecheck
scripts/test-client.sh
```

The final command starts a disposable local database and runs the client, Pi and application tests. The application cases cover a lost commit response, duplicate intents, concurrent independent updates, atomic index removal, stale pagination and optimistic settings conflicts. They use synthetic data and scoped credentials.
