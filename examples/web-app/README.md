# A task board in 50 lines of storage code

Settings, a paginated task list, and an atomic completion counter. Start with [storage.ts](storage.ts); [server.ts](server.ts) connects it to Hono routes with Zod validation.

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token });

await db.set('settings', { theme: 'dark' });
const settings = await db.get<{ theme: string }>('settings');
const tasks = await db.list<Task>({ prefix: 'tasks/', limit: 20 });
```

Related changes belong in a transaction:

```ts
await db.transaction(async (tx) => {
  const task = await tx.get<Task>(`tasks/${id}`);
  if (!task || task.completed) return;

  const count = (await tx.get<number>('stats/completed')) ?? 0;
  tx.set(`tasks/${id}`, { ...task, completed: true });
  tx.set('stats/completed', count + 1);
});
```

Completing a task twice increments the counter once. Concurrent completions of different tasks preserve both increments. The SDK handles encoding, snapshot revisions, conflicts and retries. The callback can rerun after a conflict, so keep emails, payments and other external effects outside it.

## Run it

Requires Node.js 24 and durable-db v0.5.1 or later; see the [installation instructions](../../README.md#start-locally).

From the repository root:

```sh
npm ci --prefix clients/javascript
npm ci --prefix examples/web-app
target/debug/durable-db serve --data .tools/web-app-data
```

In a second terminal, still at the repository root:

```sh
umask 077
mkdir -p .tools/web-app
target/debug/durable-db token --key-file .tools/web-app-data/admin.key --admin \
  > .tools/web-app/admin.token
export DURABLE_DB_ADMIN_TOKEN_FILE="$PWD/.tools/web-app/admin.token"
node examples/web-app/setup.ts
node examples/web-app/server.ts
```

Setup creates a protected database and saves a scoped application token. It can be rerun safely; the token expires after an hour, so rerun setup and restart the API when needed. Credentials stay on the application server. This localhost example has no user login; add your application's authentication and authorization when deploying it.

## Try it

```sh
curl http://127.0.0.1:3000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"id":"2408c6f4-2c37-477a-b0c1-071cd9a8979a","title":"Publish the first post"}'

curl http://127.0.0.1:3000/tasks

curl -X POST http://127.0.0.1:3000/tasks/2408c6f4-2c37-477a-b0c1-071cd9a8979a/complete

curl http://127.0.0.1:3000/stats
```

Generate a task ID once and reuse it when retrying creation. An existing ID returns its original task. GET `/settings` reads preferences; PUT `/settings` with `{"theme":"dark"}` replaces them. Settings intentionally use last-write-wins semantics.

A task page contains `items` and a `cursor`; pass the cursor back as `?cursor=...` using URL encoding. Keys sort by UTF-8 byte order. A write between pages invalidates the cursor and returns 409; restart the listing. For SDK bounds and unresolved network outcomes, see the [client guide](../../clients/javascript/README.md).

Run `scripts/test-client.sh` to check the SDK, Pi adapters and example against a disposable local database.
