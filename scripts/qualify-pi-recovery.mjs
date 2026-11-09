/** Explicit disposable deployment fixture. Credentials are read from a file. */
import { readFile, writeFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { DurableDb } from '../clients/javascript/client.mjs';
import { openIndexedPiStorage } from '../clients/javascript/pi-indexed.mjs';
import { retry } from '../clients/javascript/indexed/kv.mjs';

const [phase, statePath] = process.argv.slice(2);
if (!['seed', 'verify'].includes(phase) || !statePath || !process.env.DURABLE_DB_TEST_URL || !process.env.DURABLE_DB_TEST_TOKEN_FILE)
  throw new Error('Explicit phase, state path, and disposable deployment environment required');
const url = process.env.DURABLE_DB_TEST_URL;
const token = (await readFile(process.env.DURABLE_DB_TEST_TOKEN_FILE, 'utf8')).trim();
let state = phase === 'seed' ? { database: `pi-recovery-${crypto.randomUUID()}` } : JSON.parse(await readFile(statePath, 'utf8'));
const admin = new DurableDb({ url, database: state.database, token });
if (phase === 'seed') {
  const intent = { request_id: crypto.randomUUID() };
  await retry(() => admin.createProtected(intent));
}
const issued = await retry(() => admin.issueToken({ write: true }));
const client = new DurableDb({ url, database: state.database, token: issued.token });
if (phase === 'verify') await assert.rejects(openIndexedPiStorage(client), /already has an owner/);
const storage = await openIndexedPiStorage(client, { takeover: phase === 'verify' });
async function commit(writes) {
  const receipts = [];
  const result = await storage.commit([writes], (_, seq) => receipts.push(seq));
  assert.equal(result.stored, 1); assert.equal(result.refused, undefined); assert.equal(receipts.length, 1);
  return receipts[0];
}
if (phase === 'seed') {
  state.base = await commit([
    { type: 'conversation', value: { id: 1 } },
    { type: 'entry', value: { id: 2, conversationId: 1, kind: 'fixture', data: { kept: true } } },
    { type: 'document.create', record: { id: 3, kind: 'history', scope: { kind: 'conversation', conversationId: 1 }, history: 'rewindable', fork: 'asOf' },
      content: { kind: 'base', version: 1, value: { count: 0 } } },
    { type: 'document.create', record: { id: 4, kind: 'latest', scope: { kind: 'session' } },
      content: { kind: 'base', version: 1, value: { retained: true } } },
  ]);
  state.changed = await commit([{ type: 'document.change', id: 3, content: { kind: 'delta', version: 1, ops: [['s', ['count'], 1]] } }]);
  await writeFile(statePath, JSON.stringify(state), { mode: 0o600 });
  // Deliberately leave ownership claimed as if this host disappeared.
  process.stdout.write(JSON.stringify({ phase, complete: true, owner_left_claimed: true }) + '\n');
} else {
  assert.deepEqual(await storage.conversation(1), { id: 1 });
  assert.equal((await storage.entry(2, {})).commitSeq, state.base);
  assert.deepEqual((await storage.document(3, state.base)).value, { count: 0 });
  assert.deepEqual((await storage.document(3, 'current')).value, { count: 1 });
  assert.deepEqual((await storage.document(4, 'current')).value, { retained: true });
  const next = await storage.mintId(); assert.equal(next, 5);
  const seq = await commit([{ type: 'entry', value: { id: next, conversationId: 1, kind: 'after-reopen', data: {} } }]);
  assert.ok(seq > state.changed);
  assert.deepEqual((await storage.scanEntries({ conversationId: 1, order: 'ascending' }, 10)).items.map(item => item.id), [2, 5]);
  await storage.close();
  process.stdout.write(JSON.stringify({ phase, complete: true, checks: 8, explicit_takeover: true }) + '\n');
}
