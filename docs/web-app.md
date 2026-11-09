# Web application storage

Use the JSON SDK for ordinary application data:

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token });
await db.set('settings', { theme: 'dark' });
const settings = await db.get<{ theme: string }>('settings');
const tasks = await db.list<Task>({ prefix: 'tasks/', limit: 20 });
```

The private source repository includes `examples/web-app/`: a short typed storage module, Hono routes, Zod validation, local setup and HTTP integration tests. Its README has complete startup and curl commands. The client package is linked locally while the project remains private.

## Related changes

Completing a task and incrementing a counter takes one transaction:

```ts
await db.transaction(async (tx) => {
  const task = await tx.get<Task>(`tasks/${id}`);
  if (!task || task.completed) return;

  const count = (await tx.get<number>('stats/completed')) ?? 0;
  tx.set(`tasks/${id}`, { ...task, completed: true });
  tx.set('stats/completed', count + 1);
});
```

Concurrent completions preserve both updates. Repeating completion does not increment the counter again. Task creation uses a stable task ID, so retrying the same creation returns the existing task. Settings use a simple last-write-wins replacement.

The SDK handles JSON encoding, snapshot revisions and safe transport retries. Definite conflicts can rerun the callback; keep external side effects outside it. Lost commit responses retry the original request. An unresolved outcome raises `UncertainCommitError` rather than silently rerunning an operation that may already have committed. See [clients](clients.md) for limits and recovery behavior.

## Pagination and deployment

Lists return `{ items, cursor }` in UTF-8 byte order. Pass the opaque cursor back with the same prefix. Any database write between pages invalidates the cursor; a 409 means restart the listing. Cursors do not retain historical snapshots.

Setup creates a protected database and writes a one-hour scoped credential privately. The application keeps that credential on its server. This localhost demonstration has no user login; a deployed application supplies authentication and per-user authorization. A database token grants access to its entire database.

The tests exercise actual HTTP storage, including concurrent task completion and duplicate requests. SDK tests separately inject lost responses, definite conflicts and receipt eviction. They establish behavior within their checked cases; the [replacement design](design-owned-partitions.md) still gates cloud latency and production suitability.
