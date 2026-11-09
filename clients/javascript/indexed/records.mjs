import { StorageRequestError } from '@earendil-works/pi-durable';
import { hash, integer, json, numberKey } from './kv.mjs';

export const recordKey = id => `r/${numberKey(id)}`;
export async function record(tx, id, type) {
  integer(id);
  const value = await tx.row(recordKey(id));
  if (value !== undefined && (!['conversation', 'entry', 'task', 'submission', 'document'].includes(value.type)
    || value.value?.id !== id)) throw new Error('Invalid indexed record identity');
  if (value !== undefined && type !== undefined && value.type !== type) return undefined;
  return value;
}
export async function indexes(type, value) {
  const id = numberKey(value.id), result = [];
  switch (type) {
    case 'conversation':
      result.push(`i/c/all/${id}`);
      if (value.owner !== undefined) result.push(`i/c/conversation/${numberKey(value.owner.conversationId)}/${id}`, `i/c/task/${numberKey(value.owner.taskId)}/${id}`);
      break;
    case 'entry':
      result.push(`i/e/${numberKey(value.conversationId)}/${id}`);
      if (value.head !== undefined) result.push(`i/h/${numberKey(value.conversationId)}/${id}`);
      break;
    case 'task': {
      const status = await hash(json(value.state.status)), conversation = numberKey(value.conversationId);
      result.push(`i/t/all/${id}`, `i/t/status/${status}/${id}`, `i/t/conversation/${conversation}/${id}`, `i/t/combined/${conversation}/${status}/${id}`);
      break;
    }
    case 'submission': {
      const status = await hash(json(value.status)), conversation = numberKey(value.conversationId);
      result.push(`i/u/all/${id}`, `i/u/status/${status}/${id}`, `i/u/conversation/${conversation}/${id}`, `i/u/combined/${conversation}/${status}/${id}`);
      break;
    }
    default: throw new Error('Unknown indexed record type');
  }
  return result;
}
export const submissionRequest = async value => `q/${numberKey(value.conversationId)}/${await hash(json(value.requestId))}`;
export async function putRecord(tx, type, value, seq) {
  const old = await record(tx, value.id);
  if (old !== undefined) {
    for (const key of await indexes(type, old.value)) tx.set(key, undefined);
    if (type === 'submission' && old.value.requestId !== undefined) {
      const key = await submissionRequest(old.value);
      if (await tx.get(key) === value.id) tx.set(key, undefined);
    }
  }
  for (const key of await indexes(type, value)) tx.set(key, value.id);
  if (type === 'submission' && value.requestId !== undefined) tx.set(await submissionRequest(value), value.id);
  await tx.setRow(recordKey(value.id), { type, value, ...(type === 'entry' ? { commitSeq: seq } : {}) });
}
export function scanStart(requested, cursor, fallback) {
  if (requested !== undefined && !['ascending', 'descending'].includes(requested)) throw new StorageRequestError('Invalid scan order');
  if (cursor === undefined) return { order: requested ?? fallback, after: undefined };
  integer(cursor.after, 'storage cursor', 0);
  if (cursor.order !== undefined && !['ascending', 'descending'].includes(cursor.order)) throw new StorageRequestError('Invalid storage cursor');
  const order = cursor.order ?? fallback;
  if (requested !== undefined && requested !== order) throw new StorageRequestError('The cursor continues a different scan order');
  return { order, after: cursor.after };
}
export function page(values, limit, order = 'ascending') {
  const items = values.slice(0, limit);
  return { items, ...(values.length > limit ? { next: { after: items.at(-1).id, order } } : {}) };
}
export function scanLimit(limit) { integer(limit, 'scan limit'); if (limit > 1000) throw new StorageRequestError('Scan limit exceeds 1,000'); }
export async function scanRecords(tx, prefix, limit, position, predicate = () => true) {
  scanLimit(limit);
  const reverse = position.order === 'descending';
  let after = position.after === undefined ? undefined : prefix + numberKey(position.after);
  const values = [];
  while (values.length <= limit) {
    const result = await tx.scan(prefix, { after, reverse, limit: Math.min(64, limit + 1) });
    // Prefetch small envelopes in bounded response batches; chunked content is
    // loaded only for selected records, never for an entire table.
    await tx.getMany(result.items.map(item => recordKey(item.value)));
    for (const item of result.items) {
      const stored = await record(tx, item.value);
      if (stored === undefined) throw new Error('Indexed record is missing');
      const expected = { c: 'conversation', t: 'task', u: 'submission', d: 'document' }[prefix.split('/')[1]];
      if (stored.type !== expected) throw new Error('Indexed record has the wrong type');
      if (predicate(stored.value)) values.push(stored.value);
      if (values.length > limit) break;
    }
    if (!result.more || values.length > limit) break;
    after = result.items.at(-1)?.key;
    if (after === undefined) throw new Error('Indexed scan made no progress');
  }
  return page(values, limit, position.order);
}
export async function scanConversations(tx, query, limit, cursor) {
  const prefix = query.ownerTaskId !== undefined ? `i/c/task/${numberKey(query.ownerTaskId)}/`
    : query.ownerConversationId !== undefined ? `i/c/conversation/${numberKey(query.ownerConversationId)}/` : 'i/c/all/';
  return scanRecords(tx, prefix, limit, scanStart(query.order, cursor, 'ascending'),
    value => (query.ownerConversationId === undefined || value.owner?.conversationId === query.ownerConversationId)
      && (query.ownerTaskId === undefined || value.owner?.taskId === query.ownerTaskId));
}
export async function scanTasks(tx, query, limit, cursor, submission = false) {
  const kind = submission ? 'u' : 't';
  const status = query.status === undefined ? undefined : await hash(json(query.status));
  const conversation = query.conversationId === undefined ? undefined : numberKey(query.conversationId);
  const prefix = conversation !== undefined && status !== undefined ? `i/${kind}/combined/${conversation}/${status}/`
    : conversation !== undefined ? `i/${kind}/conversation/${conversation}/`
    : status !== undefined ? `i/${kind}/status/${status}/` : `i/${kind}/all/`;
  return scanRecords(tx, prefix, limit, scanStart(query.order, cursor, 'ascending'), value =>
    (query.conversationId === undefined || value.conversationId === query.conversationId)
    && (query.status === undefined || (submission ? value.status : value.state.status) === query.status)
    && (submission || ((query.kind === undefined || value.kind === query.kind)
      && (query.abortRequested === undefined || value.abortRequested === query.abortRequested)
      && (query.background === undefined || value.background === query.background))));
}
export async function ancestry(tx, conversationId, upper = Number.MAX_SAFE_INTEGER) {
  const result = [], seen = new Set();
  for (let current = conversationId; ; ) {
    if (seen.has(current) || result.length >= tx.bounds.maxAncestry) throw new StorageRequestError('Conversation ancestry budget or cycle detected');
    seen.add(current);
    const conversation = await record(tx, current, 'conversation');
    if (conversation === undefined) throw new StorageRequestError('Unknown conversation');
    result.push({ id: current, upper });
    if (conversation.value.parent === undefined) return result;
    upper = Math.min(upper, integer(conversation.value.parent.at));
    current = integer(conversation.value.parent.conversationId);
  }
}
export async function entry(tx, id, conversationId) {
  const value = await record(tx, id, 'entry');
  if (conversationId !== undefined) {
    const segments = await ancestry(tx, conversationId, id);
    if (value === undefined || !segments.some(segment => segment.id === value.value.conversationId && id <= segment.upper)) return undefined;
  }
  return value === undefined ? undefined : { entry: value.value, commitSeq: value.commitSeq };
}
export async function scanEntries(tx, query, limit, cursor, heads = false) {
  scanLimit(limit);
  const position = scanStart(query.order, cursor, 'descending');
  let min = query.minEntryId ?? 1, max = query.maxEntryId ?? Number.MAX_SAFE_INTEGER;
  integer(min); integer(max);
  if (position.after !== undefined) {
    if (position.order === 'ascending') min = Math.max(min, position.after + 1);
    else max = Math.min(max, position.after - 1);
  }
  const segments = await ancestry(tx, query.conversationId, max);
  if (position.order === 'ascending') segments.reverse();
  const values = [];
  for (const segment of segments) {
    if (segment.upper < min) continue;
    const prefix = `i/${heads ? 'h' : 'e'}/${numberKey(segment.id)}/`;
    let after;
    while (values.length <= limit) {
      const result = await tx.scan(prefix, { after, reverse: position.order === 'descending',
        start: prefix + numberKey(min), end: prefix + numberKey(segment.upper) + '~', limit: Math.min(64, limit + 1) });
      await tx.getMany(result.items.map(item => recordKey(item.value)));
      for (const item of result.items) {
        const stored = await record(tx, item.value, 'entry');
        if (stored === undefined || stored.value.conversationId !== segment.id) throw new Error('Invalid entry index');
        values.push(stored.value);
        if (values.length > limit) break;
      }
      if (!result.more || values.length > limit) break;
      after = result.items.at(-1)?.key;
      if (after === undefined) throw new Error('Entry scan made no progress');
    }
    if (values.length > limit) break;
  }
  return page(values, limit, position.order);
}
