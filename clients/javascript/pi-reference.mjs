/**
 * Bounded compatibility/reference adapter for pi-durable 1.5.0.
 * Replays at most maxLogBytes into Pi's reference engine; intentionally NOT the
 * final indexed adapter. A dedicated database/prefix has one fenced Session owner.
 */
import { MemoryStorage } from '@earendil-works/pi-durable/storage/memory';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { encode, decode, DbError } from './client.mjs';
const context = BACKGROUND_CONTEXT;
const text = new TextDecoder();
const bytes = new TextEncoder();

async function retryRead(fn) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (!(e instanceof DbError) || ![429, 503].includes(e.status) || attempt >= 8) throw e;
      await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
    }
  }
}
export async function openReferencePiStorage(
  client,
  { prefix = 'pi/', maxLogBytes = 8 * 1024 * 1024, takeover = false } = {},
) {
  if (!prefix || prefix.length > 100 || maxLogBytes < 1 || maxLogBytes > 64 * 1024 * 1024)
    throw new Error('Invalid adapter bounds');
  const ownerKey = `${prefix}owner`,
    metaKey = `${prefix}meta`,
    logPrefix = `${prefix}log/`;
  const owner = encode(crypto.randomUUID());
  const initial = await retryRead(() => client.read({ keys: [ownerKey, metaKey] }));
  if (initial.values[0] !== null && !takeover)
    throw new Error('Pi storage already has an owner; explicit takeover is required after a crash');
  let meta = initial.values[1],
    log = [],
    logBytes = 0,
    after;
  const encodedChunks = new Map();
  do {
    const result = await retryRead(() =>
      client.read({
        expected_revision: initial.revision,
        scans: [{ prefix: logPrefix, after, limit: 16 }],
      }),
    );
    for (const pair of result.scans[0].items) {
      const chunk = decode(pair.value);
      logBytes += chunk.length;
      if (logBytes > maxLogBytes)
        throw new Error('Pi reference adapter log exceeds configured memory bound');
      const seq = pair.key.slice(logPrefix.length).split('/')[0];
      if (!encodedChunks.has(seq)) encodedChunks.set(seq, []);
      encodedChunks.get(seq).push(chunk);
    }
    after = result.scans[0].next;
  } while (after);
  for (const chunks of encodedChunks.values()) {
    const all = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
      all.set(chunk, offset);
      offset += chunk.length;
    }
    log.push(JSON.parse(text.decode(all)));
  }
  encodedChunks.clear();
  async function reconstruct() {
    const memory = new MemoryStorage();
    for (const batch of log) {
      const result = await memory.commit([batch], () => {}, context);
      if (result.stored !== 1) throw new Error('Stored Pi log is invalid');
    }
    return memory;
  }
  let memory = await reconstruct();
  const claim = {
    expected_revision: initial.revision,
    request_id: crypto.randomUUID(),
    writes: [{ key: ownerKey, value: owner }],
  };
  await retryRead(() => client.commit(claim));
  let closed = false,
    pending = Promise.resolve(),
    maxMint = 0;
  function assertOpen() {
    if (closed) throw new Error('Storage is closed');
  }
  async function persist(writes) {
    for (let attempt = 0; attempt < 12; attempt++) {
      const snapshot = await retryRead(() => client.read({ keys: [ownerKey, metaKey] }));
      if (snapshot.values[0] !== owner || snapshot.values[1] !== meta)
        throw new Error('Pi session lost storage ownership');
      const req = {
        expected_revision: snapshot.revision,
        request_id: crypto.randomUUID(),
        checks: [
          { key: ownerKey, value: owner },
          { key: metaKey, value: meta },
        ],
        writes,
      };
      try {
        return await retryRead(() => client.commit(req));
      } catch (e) {
        if (!(e instanceof DbError) || e.status !== 409 || e.code !== 'conflict') throw e;
      }
    }
    throw new Error('Pi storage contention retry budget exceeded');
  }
  const methods = {
    async mintId() {
      await pending;
      assertOpen();
      const id = await memory.mintId();
      maxMint = Math.max(maxMint, id);
      return id;
    },
    commit(batches, receive, ctx) {
      batches = structuredClone(batches);
      const operation = pending.then(async () => {
        assertOpen();
        const staged = await reconstruct();
        // Candidates minted but not committed still belong to this open Session.
        if (maxMint > 0) {
          while ((await staged.mintId()) < maxMint) {
            /* bounded by the reference adapter's scope */
          }
        }
        const seqs = [];
        const result = await staged.commit(batches, (index, seq) => seqs.push([index, seq]), ctx);
        if (result.stored === 0) return result;
        const writes = [];
        let additional = 0;
        const retained = [];
        for (const [index, seq] of seqs) {
          const data = bytes.encode(JSON.stringify(batches[index]));
          additional += data.length;
          if (logBytes + additional > maxLogBytes)
            throw new Error('Pi reference adapter log budget exhausted');
          retained.push(JSON.parse(text.decode(data)));
          for (let start = 0, chunk = 0; start < data.length; start += 32768, chunk++) {
            writes.push({
              key: `${logPrefix}${String(seq).padStart(16, '0')}/${String(chunk).padStart(6, '0')}`,
              value: encode(data.subarray(start, start + 32768)),
            });
          }
        }
        const nextMeta = encode(JSON.stringify({ seq: seqs.at(-1)[1] }));
        writes.push({ key: metaKey, value: nextMeta });
        if (writes.length > 128)
          throw new Error('Pi commit exceeds durable-db transaction operation budget');
        await persist(writes);
        memory = staged;
        meta = nextMeta;
        log.push(...retained);
        logBytes += additional;
        for (const [index, seq] of seqs) receive(index, seq);
        return result;
      });
      pending = operation.then(
        () => {},
        () => {},
      );
      return operation;
    },
    async close() {
      await pending;
      if (closed) return;
      try {
        await persist([{ key: ownerKey, value: null }]);
      } finally {
        closed = true;
        await memory.close(context);
        log = [];
      }
    },
  };
  return new Proxy(methods, {
    get(target, key) {
      if (key in target) return target[key];
      if (typeof memory[key] !== 'function') return undefined;
      return async (...args) => {
        await pending;
        assertOpen();
        return memory[key](...args);
      };
    },
  });
}
