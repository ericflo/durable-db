import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { DurableDb, DbError } from '../../clients/javascript/client.mjs';
import { TaskStore } from './storage.ts';

const url = process.env.DURABLE_DB_TEST_URL;
const tokenFile = process.env.DURABLE_DB_TEST_TOKEN_FILE;
const enabled = Boolean(url && tokenFile);
const pause = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));
async function admitted<T>(operation: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (!(error instanceof DbError) || error.status !== 429 || attempt >= 30) throw error;
      await pause(100);
    }
  }
}
async function fixture() {
  const admin = new DurableDb({
    url,
    database: `web-${randomUUID()}`,
    token: readFileSync(tokenFile!, 'utf8').trim(),
  });
  const creation = { request_id: randomUUID() };
  await admitted(() => admin.createProtected(creation));
  const { token } = await admitted(() => admin.issueToken({ write: true }));
  const client = new DurableDb({ url, database: admin.database, token });
  return { client, store: new TaskStore(client) };
}

test(
  'a lost commit response and duplicate browser intent create one task and receipt',
  { skip: !enabled },
  async () => {
    const { client, store } = await fixture();
    const commit = client.commit.bind(client);
    let loseResponse = true;
    client.commit = async (request) => {
      const result = await commit(request);
      if (loseResponse) {
        loseResponse = false;
        throw new TypeError('Injected connection loss after publication');
      }
      return result;
    };
    const requestId = randomUUID();
    const first = await store.createTask(requestId, 'Ship the application');
    assert.deepEqual(await store.createTask(requestId, 'Ship the application'), first);
    assert.deepEqual((await admitted(() => store.listOpen())).tasks, [first]);
    assert.equal((await admitted(() => client.info())).revision, 1);
    await assert.rejects(store.createTask(requestId, 'Different intent'), /different input/);
  },
);

test(
  'independent concurrent tasks preserve each other; completion atomically removes the open index',
  { skip: !enabled },
  async () => {
    const { client, store } = await fixture();
    const created = await Promise.all(
      ['Read', 'Write', 'Review'].map((title) => store.createTask(randomUUID(), title)),
    );
    const requestId = randomUUID();
    const completed = await store.completeTask(requestId, created[0].id);
    assert.equal(completed.completed, true);
    assert.deepEqual(await store.completeTask(requestId, created[0].id), completed);
    const page = await admitted(() => store.listOpen());
    assert.deepEqual(
      new Set(page.tasks.map((task) => task.id)),
      new Set(created.slice(1).map((task) => task.id)),
    );
    assert.equal((await admitted(() => client.info())).revision, 4);
  },
);

test(
  'pagination pins a revision and refuses to silently mix snapshots after an edit',
  { skip: !enabled },
  async () => {
    const { store } = await fixture();
    for (const title of ['First', 'Second', 'Third']) await store.createTask(randomUUID(), title);
    const first = await admitted(() => store.listOpen(undefined, 1));
    assert.ok(first.next);
    const second = await admitted(() => store.listOpen(first.next, 1));
    assert.notEqual(first.tasks[0].id, second.tasks[0].id);
    await store.createTask(randomUUID(), 'Later');
    await assert.rejects(
      admitted(() => store.listOpen(first.next, 1)),
      (error) => error instanceof DbError && error.status === 409,
    );
  },
);

test(
  'settings use an explicit saved intent and reject a stale optimistic edit',
  { skip: !enabled },
  async () => {
    const { store } = await fixture();
    const initial = await admitted(() => store.settings());
    const intent = store.prepareSettings({ theme: 'dark' }, initial.revision, randomUUID());
    assert.equal((await admitted(() => store.saveSettings(intent))).revision, 1);
    assert.equal((await admitted(() => store.saveSettings(intent))).replayed, true);
    const stale = store.prepareSettings({ theme: 'light' }, initial.revision, randomUUID());
    await assert.rejects(
      admitted(() => store.saveSettings(stale)),
      (error) => error instanceof DbError && error.status === 409,
    );
    assert.deepEqual((await admitted(() => store.settings())).value, {
      theme: 'dark',
    });
  },
);
