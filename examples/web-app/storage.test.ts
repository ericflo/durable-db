import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { Database, DurableDb, DbError } from '@durable-db/client';
import { taskStore } from './storage.ts';

const url = process.env.DURABLE_DB_TEST_URL;
const tokenFile = process.env.DURABLE_DB_TEST_TOKEN_FILE;
const enabled = Boolean(url && tokenFile);
async function admitted<T>(call: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call();
    } catch (error) {
      if (!(error instanceof DbError) || error.status !== 429 || attempt >= 30) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}
async function fixture() {
  const database = `web-${randomUUID()}`;
  const admin = new DurableDb({ url, database, token: readFileSync(tokenFile!, 'utf8').trim() });
  const intent = { request_id: randomUUID() };
  await admitted(() => admin.createProtected(intent));
  const { token } = await admitted(() => admin.issueToken({ write: true }));
  return taskStore(new Database({ url, database, token }));
}

test('repeating a task creation returns the same task', { skip: !enabled }, async () => {
  const tasks = await fixture(),
    id = randomUUID();
  const created = await tasks.create(id, 'Publish the first post');
  assert.deepEqual(await tasks.create(id, 'Publish the first post'), created);
  assert.deepEqual(
    (await tasks.list()).items.map((item) => item.value),
    [created],
  );
});

test(
  'concurrent completions update each task and the shared counter exactly once',
  { skip: !enabled },
  async () => {
    const tasks = await fixture();
    const created = await Promise.all(
      ['Read', 'Write', 'Review'].map((title) => tasks.create(randomUUID(), title)),
    );
    await Promise.all([...created, created[0]].map((task) => tasks.complete(task.id)));
    assert.equal(await tasks.completedCount(), 3);
    assert.ok((await tasks.list()).items.every((item) => item.value.completed));
  },
);

test('a missing task does not increment the counter', { skip: !enabled }, async () => {
  const tasks = await fixture();
  assert.equal(await tasks.complete(randomUUID()), undefined);
  assert.equal(await tasks.completedCount(), 0);
});

test('settings use ordinary typed JSON get and set', { skip: !enabled }, async () => {
  const tasks = await fixture();
  assert.deepEqual(await tasks.settings(), { theme: 'light' });
  await tasks.saveSettings({ theme: 'dark' });
  assert.deepEqual(await tasks.settings(), { theme: 'dark' });
});
