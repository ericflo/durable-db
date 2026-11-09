/** Indexed pi-durable 1.5.0 storage. One fenced Session per owned prefix. */
import { StorageRequestError } from '@earendil-works/pi-durable';
import { DbError } from './client.mjs';
import { Snapshot, copy, integer, json, pack, unpack, retry, pause } from './indexed/kv.mjs';
import * as records from './indexed/records.mjs';
import * as documents from './indexed/documents.mjs';

const DEFAULT_BOUNDS = Object.freeze({
  maxReadBytes: 8 * 1024 * 1024,
  maxRequests: 256,
  maxScanned: 4096,
  maxAncestry: 256,
  maxDeltas: 1024,
});
function allocator(value) {
  // 2^53 is exactly representable and is the exhausted sentinel after the
  // largest valid ID was stored. It is never returned as a minted ID.
  if (value !== Number.MAX_SAFE_INTEGER + 1) integer(value, 'next ID', 2);
  return value;
}
function metadata(value) {
  const uuid = (item) =>
    typeof item === 'string' && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(item);
  if (
    value?.format !== 1 ||
    !(value.owner === null || typeof value.owner === 'string') ||
    !(value.lastCommit === null || typeof value.lastCommit === 'string')
  )
    throw new Error('Unsupported indexed Pi metadata');
  if (
    Object.keys(value).some(
      (key) => !['format', 'owner', 'lastCommit', 'nextId', 'nextSeq'].includes(key),
    ) ||
    (value.owner !== null && !uuid(value.owner)) ||
    (value.lastCommit !== null && !uuid(value.lastCommit))
  )
    throw new Error('Invalid indexed Pi metadata identity');
  allocator(value.nextId);
  integer(value.nextSeq, 'next sequence');
  return value;
}
async function observe(client, prefix) {
  const result = await retry(() => client.read({ keys: [prefix + 'meta'] }));
  const encoded = result.values[0];
  return {
    revision: result.revision,
    encoded,
    meta: encoded === null ? undefined : metadata(unpack(encoded)),
  };
}

export async function openIndexedPiStorage(
  client,
  { prefix = 'pi-indexed/', takeover = false, ...options } = {},
) {
  if (typeof prefix !== 'string' || !/^[A-Za-z0-9/_-]{1,64}$/.test(prefix))
    throw new StorageRequestError('Invalid indexed prefix');
  const bounds = { ...DEFAULT_BOUNDS, ...options };
  for (const [name, maximum] of Object.entries(DEFAULT_BOUNDS)) {
    if (!Number.isSafeInteger(bounds[name]) || bounds[name] < 1 || bounds[name] > maximum)
      throw new StorageRequestError(`Invalid ${name} bound`);
  }
  for (const name of Object.keys(options))
    if (!(name in DEFAULT_BOUNDS)) throw new StorageRequestError('Unknown indexed adapter option');
  const owner = crypto.randomUUID();
  let targetOwner;
  for (let attempt = 0; attempt < 12; attempt++) {
    const current = await observe(client, prefix);
    const held = current.meta?.owner ?? null;
    if (attempt === 0) targetOwner = held;
    else {
      if (held === owner) return new IndexedStorage(client, prefix, owner, current.encoded, bounds);
      // A takeover targets the first observed owner. Recheck after backoff too:
      // a third host may have claimed between conflict resolution and this read.
      if (held !== null && held !== targetOwner)
        throw new Error('Pi storage ownership changed during open');
    }
    if (current.meta?.owner !== undefined && current.meta.owner !== null && !takeover)
      throw new Error(
        'Pi storage already has an owner; explicit takeover is required after a crash',
      );
    const tx = new Snapshot(client, prefix, current.revision, bounds);
    if (current.meta === undefined && (await tx.scan('', { limit: 1 })).items.length !== 0)
      throw new Error('Uninitialized indexed prefix is not empty');
    const meta = {
      ...(current.meta ?? { format: 1, nextId: 2, nextSeq: 1, lastCommit: null }),
      owner,
    };
    tx.set('meta', meta);
    const request = tx.request(current.encoded, crypto.randomUUID());
    try {
      await retry(() => client.commit(request));
      return new IndexedStorage(client, prefix, owner, pack(meta), bounds);
    } catch (error) {
      // A lost claim response must resolve by its fresh owner identity. Never
      // turn an unresolved claim into a new unrelated claim automatically.
      const observed = await observe(client, prefix);
      if (observed.meta?.owner === owner)
        return new IndexedStorage(client, prefix, owner, observed.encoded, bounds);
      if (!(error instanceof DbError) || error.status !== 409) throw error;
      // A fence or a write by the SAME target can cause a definite conflict.
      // Retry its fresh snapshot without silently targeting another Session.
      const held = observed.meta?.owner ?? null;
      if (held !== null && held !== targetOwner)
        throw new Error('Pi storage ownership changed during open');
      await pause(attempt);
    }
  }
  throw new Error('Pi ownership claim retry budget exceeded');
}

class IndexedStorage {
  constructor(client, prefix, owner, encoded, bounds) {
    this.client = client;
    this.prefix = prefix;
    this.owner = owner;
    this.encoded = encoded;
    this.bounds = bounds;
    this.nextId = metadata(unpack(encoded)).nextId;
    this.pending = Promise.resolve();
    this.closed = false;
    this.closing = false;
    this.failure = undefined;
  }
  enqueue(call, read = false) {
    if (this.closed || this.closing) return Promise.reject(new Error('Pi storage is closed'));
    const operation = this.pending.then(async () => {
      if (this.failure !== undefined)
        throw new Error('Pi storage failed; reopen before continuing', { cause: this.failure });
      try {
        return await call();
      } catch (error) {
        if (!read || !(error instanceof StorageRequestError)) this.failure = error;
        throw error;
      }
    });
    this.pending = operation.then(
      () => {},
      () => {},
    );
    return operation;
  }
  async snapshot() {
    const current = await observe(this.client, this.prefix);
    if (current.meta?.owner !== this.owner || current.encoded !== this.encoded)
      throw new Error('Pi Session lost storage ownership or its state changed');
    return new Snapshot(this.client, this.prefix, current.revision, this.bounds);
  }
  reading(call) {
    return this.enqueue(async () => {
      for (let attempt = 0; attempt < 12; attempt++) {
        try {
          return copy(await call(await this.snapshot()));
        } catch (error) {
          if (!(error instanceof DbError) || error.status !== 409 || attempt === 11) throw error;
          await pause(attempt);
        }
      }
    }, true);
  }
  mintId() {
    return this.enqueue(async () => {
      if (!Number.isSafeInteger(this.nextId))
        throw new StorageRequestError('ID space is exhausted');
      return this.nextId++;
    }, true);
  }
  commit(batches, receive, _context) {
    batches = copy(batches);
    return this.enqueue(async () => {
      for (let attempt = 0; attempt < 12; attempt++) {
        const tx = await this.snapshot();
        const meta = metadata(unpack(this.encoded));
        let nextId = Math.max(meta.nextId, this.nextId),
          nextSeq = meta.nextSeq;
        const sequences = [];
        let refused;
        let mutationStarted = false;
        try {
          for (const batch of batches) {
            let prepared;
            try {
              const ids = batch.flatMap((write) =>
                ['document.create', 'document.copy'].includes(write.type)
                  ? [write.record.id, ...(write.type === 'document.copy' ? [write.source.id] : [])]
                  : ['document.change', 'document.retire'].includes(write.type)
                    ? [write.id]
                    : [write.value.id],
              );
              await tx.getMany(ids.map(records.recordKey));
              prepared = await documents.prepareDocuments(tx, batch);
              const claimed = new Map();
              for (const write of prepared.writes) {
                if (['document.change', 'document.retire'].includes(write.type)) continue;
                const type = write.type === 'document.create' ? 'document' : write.type;
                if (!['conversation', 'entry', 'task', 'submission', 'document'].includes(type))
                  throw new documents.Refused('Unknown Pi write type');
                const id = integer(type === 'document' ? write.record.id : write.value.id);
                const previous = await records.record(tx, id);
                const earlier = claimed.get(id);
                if (['conversation', 'entry', 'document'].includes(type)) {
                  if (previous !== undefined)
                    throw new documents.Refused(`ID ${id} already belongs to ${previous.type}`);
                  if (earlier !== undefined)
                    throw new documents.Refused(`ID ${id} is written more than once`);
                } else {
                  if (previous !== undefined && previous.type !== type)
                    throw new documents.Refused(`ID ${id} already belongs to ${previous.type}`);
                  if (earlier !== undefined && earlier !== type)
                    throw new documents.Refused(`ID ${id} is written as two record types`);
                }
                claimed.set(id, type);
              }
            } catch (error) {
              if (!(error instanceof documents.Refused)) throw error;
              refused = error;
              break;
            }
            integer(nextSeq, 'commit sequence');
            for (const write of prepared.writes) {
              if (['conversation', 'entry', 'task', 'submission'].includes(write.type)) {
                await records.putRecord(tx, write.type, write.value, nextSeq);
                nextId = Math.max(nextId, write.value.id + 1);
              } else if (write.type === 'document.create')
                nextId = Math.max(nextId, write.record.id + 1);
            }
            await documents.applyDocuments(tx, prepared.actions, nextSeq);
            sequences.push(nextSeq++);
          }
          if (sequences.length === 0)
            return refused === undefined ? { stored: 0 } : { stored: 0, refused };
          allocator(nextId);
          integer(nextSeq, 'next sequence');
          const nonce = crypto.randomUUID();
          const updated = { ...meta, nextId, nextSeq, lastCommit: nonce };
          tx.set('meta', updated);
          const request = tx.request(this.encoded, nonce);
          mutationStarted = true;
          try {
            await retry(() => this.client.commit(request));
          } catch (error) {
            const observed = await observe(this.client, this.prefix);
            if (observed.meta?.lastCommit !== nonce) {
              if (
                error instanceof DbError &&
                error.status === 409 &&
                observed.encoded === this.encoded &&
                attempt < 11
              ) {
                await pause(attempt);
                continue;
              }
              throw error;
            }
            // A nonce unique to this attempt witnesses publication after its
            // database receipt was evicted. A takeover still fails closed.
            if (observed.meta.owner !== this.owner)
              throw new Error('Pi ownership changed after an uncertain commit');
          }
          this.encoded = pack(updated);
          this.nextId = nextId;
          for (const [index, seq] of sequences.entries()) receive(index, seq);
          return refused === undefined
            ? { stored: sequences.length }
            : { stored: sequences.length, refused };
        } catch (error) {
          // Only a definite read-snapshot conflict may rebuild staging. A
          // mutation whose outcome is unresolved must never reach this branch.
          if (
            error instanceof DbError &&
            error.status === 409 &&
            !mutationStarted &&
            attempt < 11
          ) {
            await pause(attempt);
            continue;
          }
          throw error;
        }
      }
      throw new Error('Pi commit contention budget exceeded');
    });
  }
  conversation(id, _context) {
    return this.reading(async (tx) => (await records.record(tx, id, 'conversation'))?.value);
  }
  scanConversations(query, limit, cursor, _context) {
    return this.reading((tx) => records.scanConversations(tx, query, limit, cursor));
  }
  entry(idOrConversation, idOrContext, context) {
    return this.reading((tx) =>
      context === undefined
        ? records.entry(tx, idOrConversation)
        : records.entry(tx, idOrContext, idOrConversation),
    );
  }
  findLatestHeadMarker(conversationId, atOrBeforeEntryId, _context) {
    return this.reading(
      async (tx) =>
        (
          await records.scanEntries(
            tx,
            { conversationId, maxEntryId: atOrBeforeEntryId },
            1,
            undefined,
            true,
          )
        ).items[0],
    );
  }
  scanEntries(query, limit, cursor, _context) {
    return this.reading((tx) => records.scanEntries(tx, query, limit, cursor));
  }
  task(id, _context) {
    return this.reading(async (tx) => (await records.record(tx, id, 'task'))?.value);
  }
  scanTasks(query, limit, cursor, _context) {
    return this.reading((tx) => records.scanTasks(tx, query, limit, cursor));
  }
  submission(id, _context) {
    return this.reading(async (tx) => (await records.record(tx, id, 'submission'))?.value);
  }
  scanSubmissions(query, limit, cursor, _context) {
    return this.reading((tx) => records.scanTasks(tx, query, limit, cursor, true));
  }
  submissionByRequest(conversationId, requestId, _context) {
    return this.reading(async (tx) => {
      const id = await tx.get(await records.submissionRequest({ conversationId, requestId }));
      if (id === undefined) return undefined;
      const value = (await records.record(tx, id, 'submission'))?.value;
      if (
        value === undefined ||
        value.conversationId !== conversationId ||
        value.requestId !== requestId
      )
        throw new Error('Invalid submission request index');
      return value;
    });
  }
  findDocument(address, at, _context) {
    return this.reading((tx) => documents.findDocument(tx, address, at));
  }
  document(id, at, _context) {
    return this.reading((tx) => documents.materialize(tx, id, at));
  }
  scanDocuments(query, limit, cursor, _context) {
    return this.reading((tx) => documents.scanDocuments(tx, query, limit, cursor));
  }
  async close(_context) {
    if (this.closed) return;
    if (this.closing) return this.closePromise;
    this.closing = true;
    this.closePromise = this.release();
    return this.closePromise;
  }
  async release() {
    await this.pending;
    try {
      if (this.failure !== undefined) return;
      for (let attempt = 0; attempt < 12; attempt++) {
        const tx = await this.snapshot();
        const updated = { ...metadata(unpack(this.encoded)), owner: null, nextId: this.nextId };
        tx.set('meta', updated);
        const request = tx.request(this.encoded, crypto.randomUUID());
        try {
          await retry(() => this.client.commit(request));
          this.encoded = pack(updated);
          return;
        } catch (error) {
          const current = await observe(this.client, this.prefix);
          if (current.encoded === pack(updated)) return;
          if (
            error instanceof DbError &&
            error.status === 409 &&
            current.encoded === this.encoded &&
            attempt < 11
          ) {
            await pause(attempt);
            continue;
          }
          throw error;
        }
      }
    } finally {
      this.closed = true;
    }
  }
}
