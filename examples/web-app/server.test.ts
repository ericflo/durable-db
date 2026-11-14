import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { startServer, type TestServer } from '@durable-db/client/testing';
import { createApp } from './server.ts';
import { taskStore } from './storage.ts';

// The durable-db executable: $DURABLE_DB_BINARY, else this repository's build, else the PATH's.
const built = fileURLToPath(new URL('../../target/release/durable-db', import.meta.url));
const binary = process.env.DURABLE_DB_BINARY ?? (existsSync(built) ? built : undefined);

let server: TestServer;
before(async () => {
  server = await startServer({ binary });
});
after(() => server.stop());

test('lists tasks in creation order, twenty at a time', async () => {
  const tasks = taskStore(await server.createDatabase());
  for (let n = 1; n <= 25; n++) await tasks.create(`Task ${n}`);
  const first = await tasks.list();
  assert.deepEqual(
    first.tasks.map((task) => task.title),
    Array.from({ length: 20 }, (_, n) => `Task ${n + 1}`),
  );
  const second = await tasks.list(first.cursor);
  assert.deepEqual(
    second.tasks.map((task) => task.title),
    ['Task 21', 'Task 22', 'Task 23', 'Task 24', 'Task 25'],
  );
  assert.equal(second.cursor, undefined);
});

test('completing a task updates it and the counter together, once', async () => {
  const tasks = taskStore(await server.createDatabase());
  const task = await tasks.create('Write the report');
  assert.deepEqual(await tasks.complete(task.id), { ...task, done: true });
  assert.deepEqual(await tasks.complete(task.id), { ...task, done: true });
  assert.equal(await tasks.complete('00000099'), undefined);
  assert.deepEqual(await tasks.stats(), { created: 1, done: 1 });
});

test('concurrent requests keep the numbering and the counters exact', async () => {
  const tasks = taskStore(await server.createDatabase());
  const created = await Promise.all(Array.from({ length: 8 }, (_, n) => tasks.create(`Task ${n}`)));
  assert.deepEqual(
    created.map((task) => task.id).sort(),
    Array.from({ length: 8 }, (_, n) => String(n + 1).padStart(8, '0')),
  );
  // Every task is completed twice at the same time; each counts once.
  await Promise.all([...created, ...created].map((task) => tasks.complete(task.id)));
  assert.deepEqual(await tasks.stats(), { created: 8, done: 8 });
  assert.ok((await tasks.list()).tasks.every((task) => task.done));
});

test('serves the task board over HTTP', async () => {
  const app = createApp(await server.createDatabase());
  const post = (path: string, body?: unknown) =>
    app.request(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });

  const created = await post('/tasks', { title: '  Publish the first post ' });
  assert.equal(created.status, 201);
  const task = await created.json();
  assert.deepEqual(task, {
    id: '00000001',
    title: 'Publish the first post',
    done: false,
  });
  assert.equal((await post('/tasks', { title: '' })).status, 400);

  assert.deepEqual(await (await app.request('/tasks')).json(), {
    tasks: [task],
  });
  assert.deepEqual(await (await post('/tasks/00000001/complete')).json(), {
    ...task,
    done: true,
  });
  assert.equal((await post('/tasks/00000002/complete')).status, 404);
  assert.deepEqual(await (await app.request('/stats')).json(), {
    created: 1,
    done: 1,
  });
});
