# Task board

A small HTTP API built with [Hono](https://hono.dev) that keeps its data in durable-db through the [TypeScript client](../../clients/javascript/README.md).

| File | Contents |
| --- | --- |
| [storage.ts](storage.ts) | The storage module: 44 lines, the part to copy |
| [server.ts](server.ts) | Four routes and the server |
| [server.test.ts](server.test.ts) | Tests against a throwaway durable-db server |

## The storage module

Tasks live under `tasks/<id>` and two counters under `stats`. Creating a task numbers it and counts it in one transaction:

```ts
create(title: string) {
  return db.transaction(async (tx) => {
    const stats = (await tx.get<Stats>('stats')) ?? { created: 0, done: 0 };
    const task: Task = { id: String(stats.created + 1).padStart(8, '0'), title, done: false };
    tx.set(taskKey(task.id), task);
    tx.set('stats', { ...stats, created: stats.created + 1 });
    return task;
  });
},
```

The transaction read `stats`, so its commit succeeds only if `stats` is unchanged. When two requests create a task at the same moment, one commits; the other's callback runs again, reads the new count, and takes the next number. No number is used twice and the count matches the tasks.

Completing a task updates the task and the `done` counter together. A task that is already done is returned unchanged, so a repeated or concurrent request counts once.

Listing is one page of the `tasks/` prefix. Keys sort by task number, so pages are in creation order, and the page's `cursor` fetches the next one:

```ts
async list(cursor?: string) {
  const page = await db.list<Task>({ prefix: 'tasks/', limit: 20, cursor });
  return { tasks: page.items.map((item) => item.value), cursor: page.cursor };
},
```

## Test it

From the repository root, with Node.js 24:

```sh
cargo build --release                  # builds target/release/durable-db
npm ci --prefix clients/javascript     # builds the client, which this example uses by path
npm ci --prefix examples/web-app
npm test --prefix examples/web-app
```

The tests start a server on a free port with a temporary data directory and stop it afterwards. They run the executable named by `DURABLE_DB_BINARY`, else `target/release/durable-db`, else `durable-db` on the `PATH`.

## Run it

Start a server:

```sh
target/release/durable-db serve --data /tmp/task-board
```

In a second terminal, create a database, keep its token in the environment, and start the API:

```sh
export DURABLE_DB_TOKEN=$(target/release/durable-db create tasks --key-file /tmp/task-board/admin.key)
npm start --prefix examples/web-app
```

In a third:

```sh
curl http://127.0.0.1:3000/tasks -H 'content-type: application/json' -d '{"title":"Publish the first post"}'
curl http://127.0.0.1:3000/tasks
curl -X POST http://127.0.0.1:3000/tasks/00000001/complete
curl http://127.0.0.1:3000/stats
```

They print:

```json
{"id":"00000001","title":"Publish the first post","done":false}
{"tasks":[{"id":"00000001","title":"Publish the first post","done":false}]}
{"id":"00000001","title":"Publish the first post","done":true}
{"created":1,"done":1}
```

`GET /tasks` returns twenty tasks. When more remain, the response has a `cursor`; pass it as `GET /tasks?cursor=...`.

The API reads `DURABLE_DB_URL` (default `http://127.0.0.1:7654`), `DURABLE_DB_DATABASE` (default `tasks`), `DURABLE_DB_TOKEN` and `PORT` (default 3000). It listens on 127.0.0.1 and has no login; add authentication before exposing it. The database token stays in the API process and never reaches a browser.
