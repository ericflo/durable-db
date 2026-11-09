import { applyImmutableBatches } from '@earendil-works/chord/delta';
import { StorageRequestError } from '@earendil-works/pi-durable';
import { copy, hash, integer, json, numberKey, utf8 } from './kv.mjs';
import { record, recordKey, scanLimit, scanRecords } from './records.mjs';

export const currentOnly = (value) =>
  value.scope.kind !== 'conversation' || value.history === 'latest';
export const alive = (value, at) =>
  at === 'current'
    ? value.retiredAt === undefined
    : value.createdAt <= at && (value.retiredAt === undefined || at < value.retiredAt);
export function scopeIdentity(scope) {
  if (scope.kind === 'session') return ['session'];
  if (scope.kind === 'conversation') return ['conversation', integer(scope.conversationId)];
  if (scope.kind === 'task') return ['task', integer(scope.taskId)];
  throw new StorageRequestError('Invalid document scope');
}
export const addressIdentity = (address) =>
  json([
    address.kind,
    scopeIdentity(address.scope),
    address.key === undefined ? ['singleton'] : ['family', address.key],
  ]);
export const addressHash = async (address) => hash(addressIdentity(address));
export class Refused extends Error {}
const revisionPrefix = (id) => `d/${numberKey(id)}/`;
const basePrefix = (id) => `b/${numberKey(id)}/`;
const currentKey = (id) => `v/${numberKey(id)}`;
function point(at) {
  if (at !== 'current') integer(at, 'document point', 0);
}

export async function materialize(tx, id, at) {
  point(at);
  const stored = await record(tx, id, 'document');
  if (stored === undefined) return undefined;
  const value = stored.value;
  if (at !== 'current' && currentOnly(value))
    throw new StorageRequestError('Document does not retain historical content');
  if (!alive(value, at)) return undefined;
  if (currentOnly(value)) {
    const content = await tx.row(currentKey(id));
    if (content === undefined || content.version !== stored.version)
      throw new Error('Document is missing current content');
    return {
      record: value,
      version: content.version,
      value: content.value,
      deltasSinceBase: content.deltasSinceBase,
    };
  }
  const end = at === 'current' ? undefined : basePrefix(id) + numberKey(at) + '~';
  const bases = await tx.scan(basePrefix(id), { end, reverse: true, limit: 1 });
  if (bases.items.length === 0) throw new Error('Document is missing a required base');
  const seq = bases.items[0].value;
  const base = await tx.row(revisionPrefix(id) + numberKey(seq));
  if (base?.kind !== 'base' || base.seq !== seq) throw new Error('Invalid document base index');
  const deltas = [];
  let after = revisionPrefix(id) + numberKey(seq);
  for (;;) {
    const revisions = await tx.scan(revisionPrefix(id), {
      after,
      end: at === 'current' ? undefined : revisionPrefix(id) + numberKey(at) + '~',
      limit: 8,
    });
    for (const item of revisions.items) {
      const revision = await tx.row(item.key);
      if (revision?.kind !== 'delta' || revision.version !== base.version)
        throw new Error('Document crosses a version boundary without a base');
      deltas.push(revision.ops);
      if (deltas.length > tx.bounds.maxDeltas)
        throw new StorageRequestError('Document replay budget exceeded; create a checkpoint');
    }
    if (!revisions.more) break;
    after = revisions.items.at(-1)?.key;
    if (after === undefined) throw new Error('Document scan made no progress');
  }
  return {
    record: value,
    version: base.version,
    value: applyImmutableBatches(base.value, deltas),
    deltasSinceBase: deltas.length,
  };
}
export async function findDocument(tx, address, at) {
  point(at);
  const identity = addressIdentity(address),
    digest = await hash(identity);
  if (at === 'current') {
    const id = await tx.get(`l/${digest}`);
    if (id === undefined) return undefined;
    const stored = await record(tx, id, 'document');
    if (
      stored === undefined ||
      addressIdentity(stored.value) !== identity ||
      !alive(stored.value, at)
    )
      throw new Error('Invalid live document address');
    return stored.value;
  }
  const prefix = `a/${digest}/`;
  let after;
  for (;;) {
    const result = await tx.scan(prefix, {
      after,
      end: prefix + numberKey(at) + '~',
      reverse: true,
      limit: 4,
    });
    for (const item of result.items) {
      const stored = await record(tx, item.value, 'document');
      if (stored === undefined || addressIdentity(stored.value) !== identity)
        throw new Error('Invalid historical document address');
      if (alive(stored.value, at)) return stored.value;
    }
    if (!result.more) return undefined;
    after = result.items.at(-1)?.key;
    if (after === undefined) throw new Error('Document address scan made no progress');
  }
}
export async function scanDocuments(tx, query, limit, cursor) {
  scanLimit(limit);
  point(query.at);
  const scope = await hash(json(scopeIdentity(query.scope)));
  const prefix =
    query.kind === undefined
      ? `i/d/scope/${scope}/`
      : `i/d/kind/${scope}/${await hash(json(query.kind))}/`;
  const after = cursor?.after;
  if (after !== undefined) integer(after, 'storage cursor', 0);
  return scanRecords(
    tx,
    prefix,
    limit,
    { order: 'ascending', after },
    (value) =>
      json(scopeIdentity(value.scope)) === json(scopeIdentity(query.scope)) &&
      (query.kind === undefined || value.kind === query.kind) &&
      alive(value, query.at),
  );
}

/** Read and validate every document action before changing the batch overlay. */
export async function prepareDocuments(tx, input) {
  const changed = new Set(
    input.flatMap((write) =>
      ['document.create', 'document.copy'].includes(write.type)
        ? [write.record.id]
        : ['document.change', 'document.retire'].includes(write.type)
          ? [write.id]
          : [],
    ),
  );
  const writes = [];
  for (const write of input) {
    if (write.type !== 'document.copy') {
      writes.push(write);
      continue;
    }
    if (changed.has(write.source.id))
      throw new Refused('Fork source document is changed in the copy batch');
    const source = await materialize(tx, write.source.id, write.source.at);
    if (source === undefined) throw new Refused('Fork source document cannot be read');
    if (
      source.record.scope.kind !== 'conversation' ||
      write.record.scope.kind !== 'conversation' ||
      ['kind', 'key', 'history', 'fork'].some((key) => source.record[key] !== write.record[key])
    )
      throw new Refused('Fork source document does not match the copied record');
    writes.push({
      type: 'document.create',
      record: write.record,
      content: { kind: 'base', version: source.version, value: source.value },
    });
  }
  const actions = new Map();
  for (const write of writes) {
    if (!['document.create', 'document.change', 'document.retire'].includes(write.type)) continue;
    const id = integer(write.type === 'document.create' ? write.record.id : write.id);
    if (!actions.has(id)) actions.set(id, { retire: false });
    const action = actions.get(id);
    if (write.type === 'document.create') {
      if (action.create !== undefined || action.content !== undefined)
        throw new Refused('Document has more than one content command');
      action.create = write.record;
      action.content = write.content;
    } else if (write.type === 'document.change') {
      if (action.content !== undefined)
        throw new Refused('Document has more than one content command');
      action.content = write.content;
    } else {
      if (action.retire) throw new Refused('Document is retired more than once');
      action.retire = true;
    }
  }
  const live = new Map();
  for (const [id, action] of actions) {
    const existing = await record(tx, id, 'document');
    if (action.create === undefined && existing === undefined)
      throw new Refused('Unknown document');
    if (action.create !== undefined && existing !== undefined)
      throw new Refused('Document already exists');
    if (existing?.value.retiredAt !== undefined) throw new Refused('Document is retired');
    if (action.content?.kind === 'delta') {
      if (existing === undefined) throw new Refused('Document delta has no base');
      if (existing.version !== action.content.version)
        throw new Refused('Document version transition requires a base');
    }
    if (action.content !== undefined && !['base', 'delta'].includes(action.content.kind))
      throw new Refused('Invalid document content');
    const value = action.create ?? existing.value;
    const digest = await addressHash(value),
      key = `l/${digest}`,
      current = await tx.get(key);
    if (current !== undefined) {
      const incumbent = await record(tx, current, 'document');
      if (incumbent === undefined || addressIdentity(incumbent.value) !== addressIdentity(value))
        throw new Error('Invalid document identity lookup');
    }
    let count = live.has(key) ? live.get(key) : current === undefined ? 0 : 1;
    if (action.retire && current === id) count--;
    if (action.create !== undefined && !action.retire) count++;
    live.set(key, count);
    action.existing = existing;
    action.address = digest;
    if (!currentOnly(value) && action.content !== undefined) {
      action.deltasSinceBase = action.content.kind === 'base' ? 0 : existing.deltasSinceBase + 1;
      action.replayBytes =
        utf8.encode(json(action.content)).length +
        (action.content.kind === 'base' ? 0 : existing.replayBytes);
      if (
        !Number.isSafeInteger(action.deltasSinceBase) ||
        !Number.isSafeInteger(action.replayBytes)
      )
        throw new Error('Invalid document replay accounting');
      if (
        action.deltasSinceBase > tx.bounds.maxDeltas ||
        action.replayBytes > tx.bounds.maxReadBytes / 4
      )
        throw new Refused('Document replay budget exhausted; create a checkpoint');
    }
    // Delta interpretation can fail. Resolve it BEFORE applying any action.
    if (currentOnly(value) && action.content !== undefined) {
      const previous = action.content.kind === 'delta' ? await tx.row(currentKey(id)) : undefined;
      if (action.content.kind === 'delta' && previous?.version !== existing.version)
        throw new Error('Invalid current document content');
      action.materialized =
        action.content.kind === 'base'
          ? {
              version: action.content.version,
              value: copy(action.content.value),
              deltasSinceBase: 0,
            }
          : {
              version: action.content.version,
              value: applyImmutableBatches(previous.value, [action.content.ops]),
              deltasSinceBase: previous.deltasSinceBase + 1,
            };
    }
  }
  if ([...live.values()].some((count) => count > 1))
    throw new Refused('Document address already has a current incarnation');
  return { writes, actions };
}
export async function applyDocuments(tx, actions, seq) {
  for (const [id, action] of actions) {
    const stored =
      action.create === undefined
        ? copy(action.existing)
        : {
            type: 'document',
            value: { ...action.create, createdAt: seq },
            version: action.content.version,
          };
    if (action.retire) stored.value.retiredAt = seq;
    if (action.content !== undefined) {
      stored.version = action.content.version;
      if (currentOnly(stored.value)) await tx.setRow(currentKey(id), action.materialized);
      else {
        stored.deltasSinceBase = action.deltasSinceBase;
        stored.replayBytes = action.replayBytes;
        await tx.setRow(revisionPrefix(id) + numberKey(seq), { ...action.content, seq });
        if (action.content.kind === 'base') tx.set(basePrefix(id) + numberKey(seq), seq);
      }
    }
    if (action.retire && currentOnly(stored.value)) await tx.setRow(currentKey(id), undefined);
    if (action.create !== undefined) {
      tx.set(`a/${action.address}/${numberKey(seq)}/${numberKey(id)}`, id);
      const scope = await hash(json(scopeIdentity(stored.value.scope)));
      tx.set(`i/d/scope/${scope}/${numberKey(id)}`, id);
      tx.set(`i/d/kind/${scope}/${await hash(json(stored.value.kind))}/${numberKey(id)}`, id);
    }
    const liveKey = `l/${action.address}`;
    if (action.retire && (await tx.get(liveKey)) === id) tx.set(liveKey, undefined);
    if (action.create !== undefined && !action.retire) tx.set(liveKey, id);
    await tx.setRow(recordKey(id), stored);
  }
}
