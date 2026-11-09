import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { DurableDb, DbError, encode } from './client.mjs';
import { openIndexedPiStorage } from './pi-indexed.mjs';
import { numberKey, retry, unpack } from './indexed/kv.mjs';

const url = process.env.DURABLE_DB_TEST_URL;
const token = process.env.DURABLE_DB_TEST_TOKEN_FILE ? readFileSync(process.env.DURABLE_DB_TEST_TOKEN_FILE, 'utf8').trim() : '';
const suite = url ? describe : describe.skip;
async function fixture() {
  const database = `indexed-${crypto.randomUUID()}`;
  const admin = new DurableDb({ url, database, token });
  const intent = { request_id: crypto.randomUUID() };
  await retry(() => admin.createProtected(intent));
  const issued = await retry(() => admin.issueToken({ write: true }));
  return { admin, client: new DurableDb({ url, database, token: issued.token }) };
}
function gate() {
  let resolve; const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
const root = { type: 'conversation', value: { id: 1 } };
const entry = (id, value = 'fixture') => ({ type: 'entry', value: { id, conversationId: 1, kind: 'test', data: { value } } });
async function commit(storage, writes) {
  const receipts = [];
  const result = await storage.commit([writes], (index, seq) => receipts.push([index, seq]));
  if (result.refused) throw result.refused;
  expect(result.stored).toBe(1); expect(receipts).toHaveLength(1);
  return receipts[0][1];
}
async function noise(client, n) {
  for (let index = 0; index < n; index++) {
    const snapshot = await retry(() => client.read());
    const request = { expected_revision: snapshot.revision, request_id: crypto.randomUUID(),
      writes: [{ key: 'unrelated/noise', value: encode(String(index)) }] };
    await retry(() => client.commit(request));
  }
}

suite('indexed adapter failure and resource boundaries', () => {
  it('opens without scanning or replaying unrelated history and preserves ordered indexes', async () => {
    const { client } = await fixture();
    let reads = 0, scans = 0;
    const observed = { read: async request => {
      scans += request?.scans?.length ?? 0;
      const result = await client.read(request);
      // Admission retries do not add successful metadata reads or replay work.
      // Count scans even on rejected attempts so a hidden history scan fails.
      reads++;
      return result;
    }, commit: request => client.commit(request) };
    const storage = await openIndexedPiStorage(observed);
    await commit(storage, [root]);
    for (let start = 2; start < 302; start += 25) await commit(storage, Array.from({ length: 25 }, (_, index) => entry(start + index)));
    await storage.close(); reads = 0; scans = 0;
    const reopened = await openIndexedPiStorage(observed);
    expect(reads).toBe(1); expect(scans).toBe(0);
    expect(await reopened.mintId()).toBe(302);
    expect((await reopened.entry(301, {})).entry.id).toBe(301);
    const first = await reopened.scanEntries({ conversationId: 1, order: 'ascending' }, 3);
    expect(first.items.map(value => value.id)).toEqual([2, 3, 4]);
    const next = await reopened.scanEntries({ conversationId: 1 }, 3, first.next);
    expect(next.items.map(value => value.id)).toEqual([5, 6, 7]);
    await reopened.close();
  }, 120000);

  it('chunks large records atomically and removes obsolete chunks when a record shrinks', async () => {
    const { client } = await fixture();
    const storage = await openIndexedPiStorage(client);
    const value = { id: 2, conversationId: 1, kind: 'large', version: 1,
      input: { blob: 'x'.repeat(300 * 1024) }, state: { status: 'pending' }, background: false, abortRequested: false };
    await commit(storage, [root, { type: 'task', value }]);
    expect((await storage.task(2)).input.blob.length).toBe(300 * 1024);
    const small = { ...value, input: { blob: 'small' }, state: { status: 'running', checkpoint: {} } };
    await commit(storage, [{ type: 'task', value: small }]);
    expect(await storage.task(2)).toEqual(small);
    const chunks = await retry(() => client.read({ scans: [{ prefix: 'pi-indexed/z/', limit: 100 }] }));
    expect(chunks.scans[0].items).toEqual([]);
    expect((await storage.scanTasks({ status: 'pending' }, 10)).items).toEqual([]);
    expect((await storage.scanTasks({ status: 'running' }, 10)).items).toEqual([small]);
    await storage.close();
  }, 60000);

  it('fences a paused old writer and its close after explicit takeover', async () => {
    const { client } = await fixture();
    const reached = gate(), resume = gate(); let stop = false;
    const paused = { read: request => client.read(request), commit: async request => {
      if (stop) { stop = false; reached.resolve(); await resume.promise; }
      return client.commit(request);
    } };
    const old = await openIndexedPiStorage(paused);
    await commit(old, [root]); stop = true;
    const receipts = [];
    const pending = old.commit([[entry(2)]], (...receipt) => receipts.push(receipt));
    const refused = expect(pending).rejects.toThrow();
    await reached.promise;
    const replacement = await openIndexedPiStorage(client, { takeover: true });
    resume.resolve(); await refused;
    expect(receipts).toEqual([]);
    await expect(old.conversation(1)).rejects.toThrow('failed');
    await old.close();
    expect(await replacement.conversation(1)).toEqual({ id: 1 });
    expect(await replacement.entry(2, {})).toBeUndefined();
    await commit(replacement, [entry(3)]);
    await replacement.close();
  }, 60000);

  it('rejects a fresh read by an old owner after takeover', async () => {
    const { client } = await fixture();
    const old = await openIndexedPiStorage(client);
    await commit(old, [root]);
    const replacement = await openIndexedPiStorage(client, { takeover: true });
    await expect(old.conversation(1)).rejects.toThrow('ownership');
    await old.close();
    expect(await replacement.conversation(1)).toEqual({ id: 1 });
    await replacement.close();
  });

  it('recovers a lost response after the database receipt was evicted without reapplying', async () => {
    const { client, admin } = await fixture();
    let lose = false, original;
    const unreliable = { read: request => client.read(request), commit: async request => {
      if (!lose) return client.commit(request);
      lose = false; original = structuredClone(request);
      await client.commit(request);
      await noise(admin, 130);
      // The real database now returns 409 for the exact evicted receipt.
      return client.commit(request);
    } };
    const storage = await openIndexedPiStorage(unreliable);
    await commit(storage, [root]); lose = true;
    const seq = await commit(storage, [entry(2)]);
    expect(original).toBeDefined();
    expect((await storage.entry(2, {})).commitSeq).toBe(seq);
    expect(await commit(storage, [entry(3)])).toBe(seq + 1);
    await storage.close();
  }, 120000);

  it('a provider failure during later-batch validation cannot masquerade as a semantic refusal', async () => {
    const { client } = await fixture(); let fail = false;
    const unavailable = { read: request => {
      if (fail && request?.keys?.includes('pi-indexed/r/' + numberKey(3))) return Promise.reject(new DbError(503, { error: 'storage_unavailable' }));
      return client.read(request);
    }, commit: request => client.commit(request) };
    const storage = await openIndexedPiStorage(unavailable);
    await commit(storage, [root]); fail = true;
    const received = [];
    await expect(storage.commit([[entry(2)], [entry(3)]], (...receipt) => received.push(receipt))).rejects.toThrow();
    expect(received).toEqual([]);
    const raw = await retry(() => client.read({ keys: ['pi-indexed/r/' + numberKey(2)] }));
    expect(raw.values[0]).toBeNull();
    await storage.close();
    const reopened = await openIndexedPiStorage(client, { takeover: true });
    expect(await reopened.entry(2, {})).toBeUndefined();
    await reopened.close();
  }, 60000);

  it('restarts a conflicted read and waits for every concurrent close', async () => {
    const { client, admin } = await fixture();
    let conflict = false, holdClose = false, metadataReads = 0;
    const reached = gate(), resume = gate();
    const changing = { read: async request => {
      if (request?.keys?.includes('pi-indexed/meta')) metadataReads++;
      if (conflict && request?.keys?.includes('pi-indexed/r/' + numberKey(1))) { conflict = false; await noise(admin, 1); }
      return client.read(request);
    }, commit: async request => {
      if (holdClose && request.writes.some(write => write.key === 'pi-indexed/meta' && unpack(write.value)?.owner === null)) {
        reached.resolve(); await resume.promise;
      }
      return client.commit(request);
    } };
    const storage = await openIndexedPiStorage(changing);
    await commit(storage, [root]); metadataReads = 0; conflict = true;
    expect(await storage.conversation(1)).toEqual({ id: 1 });
    expect(metadataReads).toBeGreaterThanOrEqual(2);
    holdClose = true;
    let firstDone = false, secondDone = false;
    const first = storage.close().then(() => { firstDone = true; });
    await reached.promise;
    const second = storage.close().then(() => { secondDone = true; });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(firstDone).toBe(false); expect(secondDone).toBe(false);
    resume.resolve(); await Promise.all([first, second]);
    const reopened = await openIndexedPiStorage(client);
    expect(await reopened.conversation(1)).toEqual({ id: 1 });
    await reopened.close();
  }, 60000);

  it('rejects oversized staging before publication and preserves acknowledged state after callback failure', async () => {
    const { client } = await fixture(); let publications = 0;
    const counted = { read: request => client.read(request), commit: request => { publications++; return client.commit(request); } };
    const storage = await openIndexedPiStorage(counted);
    await commit(storage, [root]); publications = 0;
    await expect(commit(storage, [entry(2, 'x'.repeat(513 * 1024))])).rejects.toThrow('512 KiB');
    expect(publications).toBe(0);
    await storage.close();
    const reopened = await openIndexedPiStorage(client, { takeover: true });
    await expect(reopened.commit([[entry(3)]], () => { throw new Error('injected callback error'); })).rejects.toThrow('callback');
    await reopened.close();
    const recovered = await openIndexedPiStorage(client, { takeover: true });
    expect((await recovered.entry(3, {})).entry.id).toBe(3);
    expect(await recovered.entry(2, {})).toBeUndefined();
    await recovered.close();
  }, 60000);

  it('requires a document checkpoint before exceeding the configured replay bound', async () => {
    const { client } = await fixture();
    const storage = await openIndexedPiStorage(client, { maxDeltas: 2 });
    const value = { id: 2, kind: 'notes', scope: { kind: 'conversation', conversationId: 1 }, history: 'rewindable', fork: 'asOf' };
    await commit(storage, [root, { type: 'document.create', record: value, content: { kind: 'base', version: 1, value: { count: 0 } } }]);
    const delta = count => ({ type: 'document.change', id: 2, content: { kind: 'delta', version: 1, ops: [['s', ['count'], count]] } });
    const first = await commit(storage, [delta(1)]);
    await commit(storage, [delta(2)]);
    const receipts = [];
    const refused = await storage.commit([[delta(3)]], (...receipt) => receipts.push(receipt));
    expect(refused.stored).toBe(0); expect(String(refused.refused)).toContain('checkpoint');
    expect(receipts).toEqual([]);
    expect((await storage.document(2, 'current')).value).toEqual({ count: 2 });
    await commit(storage, [{ type: 'document.change', id: 2, content: { kind: 'base', version: 1, value: { count: 2 } } }]);
    await commit(storage, [delta(3)]);
    expect((await storage.document(2, 'current')).value).toEqual({ count: 3 });
    expect((await storage.document(2, first)).value).toEqual({ count: 1 });
    await storage.close();
  }, 60000);

  it('scans large latest-document metadata without reading content and reclaims content on retirement', async () => {
    const { client } = await fixture(); const readKeys = [];
    const observed = { read: request => { readKeys.push(...(request.keys ?? [])); return client.read(request); }, commit: request => client.commit(request) };
    const storage = await openIndexedPiStorage(observed);
    const value = { id: 2, kind: 'large-document', scope: { kind: 'session' } };
    await commit(storage, [root, { type: 'document.create', record: value, content: { kind: 'base', version: 1, value: { blob: 'x'.repeat(300 * 1024) } } }]);
    readKeys.length = 0;
    expect((await storage.scanDocuments({ scope: { kind: 'session' }, at: 'current' }, 10)).items.map(item => item.id)).toEqual([2]);
    expect(readKeys.some(key => key.startsWith('pi-indexed/z/') || key.startsWith('pi-indexed/v/'))).toBe(false);
    expect((await storage.document(2, 'current')).value.blob.length).toBe(300 * 1024);
    await commit(storage, [{ type: 'document.retire', id: 2 }]);
    expect(await storage.document(2, 'current')).toBeUndefined();
    expect((await retry(() => client.read({ scans: [{ prefix: 'pi-indexed/z/', limit: 100 }] }))).scans[0].items).toEqual([]);
    await storage.close();
  }, 60000);
});
