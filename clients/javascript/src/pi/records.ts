import { StorageRequestError } from '@earendil-works/pi-durable';
import type {
  ConversationQuery,
  ConversationRecord,
  Cursor,
  DocumentRecord,
  EntryQuery,
  EntryRecord,
  Page,
  ScanOrder,
  Seq,
  StorageWrite,
  SubmissionQuery,
  SubmissionRecord,
  TaskQuery,
  TaskRecord,
} from '@earendil-works/pi-durable';
import type { JsonValue } from '@earendil-works/chord/json';
import { hash, integer, numberKey, type View } from './view.ts';

export type AnyTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
export type TableWrite = Extract<
  StorageWrite,
  { type: 'conversation' | 'entry' | 'task' | 'submission' }
>;

/**
 * What `r/<id>` holds. Every record ID has one, which makes the ID namespace global. An entry's
 * content lives under its conversation instead, where one scan returns a page of history.
 */
export type Stored =
  | { type: 'conversation'; value: ConversationRecord }
  | { type: 'entry'; conversationId: number }
  | { type: 'task'; value: AnyTask }
  | { type: 'submission'; value: SubmissionRecord }
  | { type: 'document'; value: DocumentRecord; version: number };
export type StoredEntry = { value: EntryRecord; commitSeq: Seq };

type Scanned = Exclude<Stored, { type: 'entry' }>;
type Values = {
  conversation: ConversationRecord;
  task: AnyTask;
  submission: SubmissionRecord;
  document: DocumentRecord;
};
type Position = { order: ScanOrder; after: number | undefined };

const TYPES = ['conversation', 'entry', 'task', 'submission', 'document'];
const KNOWN_LIMIT = 4096;

/** Facts that never change once stored, kept across operations to save round trips. */
export interface Known {
  /** Conversation records: identity and ancestry are immutable. */
  conversations: Map<number, ConversationRecord>;
  /** The conversation each entry belongs to. */
  entryOwners: Map<number, number>;
}

export function remember<V>(map: Map<number, V>, id: number, value: V): void {
  if (map.size >= KNOWN_LIMIT) map.delete(map.keys().next().value!);
  map.set(id, value);
}

export const recordKey = (id: number): string => `r/${numberKey(id)}`;
export const entryKey = (conversationId: number, id: number): string =>
  `e/${numberKey(conversationId)}/${numberKey(id)}`;
const headKey = (conversationId: number, id: number): string =>
  `h/${numberKey(conversationId)}/${numberKey(id)}`;
export const requestKey = async (conversationId: number, requestId: string): Promise<string> =>
  `q/${numberKey(conversationId)}/${await hash(requestId)}`;

export async function record(view: View, id: number): Promise<Stored | undefined>;
export async function record<K extends Stored['type']>(
  view: View,
  id: number,
  type: K,
): Promise<Extract<Stored, { type: K }> | undefined>;
export async function record(view: View, id: number, type?: string): Promise<Stored | undefined> {
  const stored = (await view.get(recordKey(integer(id)))) as Stored | undefined;
  if (stored === undefined) return undefined;
  if (!TYPES.includes(stored.type) || (stored.type !== 'entry' && stored.value?.id !== id))
    throw new Error(`Record ${id} is stored under another identity`);
  return type === undefined || stored.type === type ? stored : undefined;
}

async function indexes(stored: Scanned): Promise<string[]> {
  const id = numberKey(stored.value.id);
  switch (stored.type) {
    case 'conversation': {
      const { owner } = stored.value;
      return [
        `i/c/all/${id}`,
        ...(owner === undefined
          ? []
          : [
              `i/c/conversation/${numberKey(owner.conversationId)}/${id}`,
              `i/c/task/${numberKey(owner.taskId)}/${id}`,
            ]),
      ];
    }
    case 'task':
    case 'submission': {
      const table = stored.type === 'task' ? 't' : 'u';
      const status = await hash(
        stored.type === 'task' ? stored.value.state.status : stored.value.status,
      );
      const conversation = numberKey(stored.value.conversationId);
      return [
        `i/${table}/all/${id}`,
        `i/${table}/status/${status}/${id}`,
        `i/${table}/conversation/${conversation}/${id}`,
        `i/${table}/combined/${conversation}/${status}/${id}`,
      ];
    }
    case 'document':
      throw new Error('Documents maintain their own indexes');
  }
}

/** Stage one table write. The checks on its ID have already passed. */
export async function putRecord(view: View, write: TableWrite, seq: number): Promise<void> {
  const id = write.value.id;
  if (write.type === 'entry') {
    const stored: StoredEntry = { value: write.value, commitSeq: seq as Seq };
    view.set(recordKey(id), { type: 'entry', conversationId: write.value.conversationId });
    view.set(entryKey(write.value.conversationId, id), stored);
    if (write.value.head !== undefined) view.set(headKey(write.value.conversationId, id), stored);
    return;
  }
  const previous = await record(view, id, write.type);
  const stale = previous === undefined ? [] : await indexes(previous as Scanned);
  const fresh = await indexes(write as Scanned);
  for (const key of stale) if (!fresh.includes(key)) view.set(key, undefined);
  for (const key of fresh) if (!stale.includes(key)) view.set(key, id);
  if (write.type === 'submission') {
    const before = (previous as { value: SubmissionRecord } | undefined)?.value;
    const request =
      write.value.requestId === undefined
        ? undefined
        : await requestKey(write.value.conversationId, write.value.requestId);
    const earlier =
      before?.requestId === undefined
        ? undefined
        : await requestKey(before.conversationId, before.requestId);
    // A request ID that moved leaves its old lookup, unless another submission took it over.
    if (earlier !== undefined && earlier !== request) {
      const holder = (await view.get(earlier)) as SubmissionRecord | undefined;
      if (holder?.id === id) view.set(earlier, undefined);
    }
    if (request !== undefined) view.set(request, write.value);
  }
  view.set(recordKey(id), { type: write.type, value: write.value });
}

export function scanStart(
  requested: ScanOrder | undefined,
  cursor: Cursor | undefined,
  fallback: ScanOrder,
): Position {
  if (requested !== undefined && requested !== 'ascending' && requested !== 'descending')
    throw new StorageRequestError(`Invalid scan order: ${String(requested)}`);
  if (cursor === undefined) return { order: requested ?? fallback, after: undefined };
  const { after, order: stored } = cursor;
  if (typeof after !== 'number' || !Number.isSafeInteger(after) || after < 0)
    throw new StorageRequestError('Invalid storage cursor');
  if (stored !== undefined && stored !== 'ascending' && stored !== 'descending')
    throw new StorageRequestError('Invalid storage cursor');
  const order = stored ?? fallback;
  if (requested !== undefined && requested !== order)
    throw new StorageRequestError(
      `The cursor continues a ${order} scan; the query asks for ${requested}`,
    );
  return { order, after };
}

function toPage<T extends { id: number }>(
  values: T[],
  limit: number,
  order: ScanOrder,
): Page<T, Cursor> {
  const items = values.slice(0, limit);
  return values.length > limit ? { items, next: { after: items.at(-1)!.id, order } } : { items };
}

/**
 * A page of records in ID order from an index whose values are record IDs. Each pass is two
 * round trips: the index, then the records it names. `filtered` says that `matches` may drop
 * records, so each pass reads ahead.
 */
export async function scanRecords<K extends keyof Values>(
  view: View,
  prefix: string,
  type: K,
  limit: number,
  position: Position,
  matches: (value: Values[K]) => boolean,
  filtered: boolean,
): Promise<Page<Values[K], Cursor>> {
  integer(limit, 'scan limit');
  const reverse = position.order === 'descending';
  const values: Values[K][] = [];
  let after = position.after === undefined ? undefined : prefix + numberKey(position.after);
  for (;;) {
    const wanted = limit + 1 - values.length;
    const found = await view.scan({
      prefix,
      after,
      reverse,
      limit: filtered ? Math.max(wanted, 256) : wanted,
    });
    const ids = found.items.map((item) => item.value as number);
    await view.load(ids.map(recordKey));
    for (const id of ids) {
      const stored = (await record(view, id, type)) as { value: Values[K] } | undefined;
      if (stored === undefined) throw new Error(`Indexed ${type} ${id} is missing`);
      if (matches(stored.value)) values.push(stored.value);
      if (values.length > limit) break;
    }
    if (!found.more || values.length > limit) break;
    after = found.items.at(-1)!.key;
  }
  return toPage(values, limit, position.order);
}

export function scanConversations(
  view: View,
  query: ConversationQuery,
  limit: number,
  cursor: Cursor | undefined,
): Promise<Page<ConversationRecord, Cursor>> {
  const prefix =
    query.ownerTaskId !== undefined
      ? `i/c/task/${numberKey(query.ownerTaskId)}/`
      : query.ownerConversationId !== undefined
        ? `i/c/conversation/${numberKey(query.ownerConversationId)}/`
        : 'i/c/all/';
  return scanRecords(
    view,
    prefix,
    'conversation',
    limit,
    scanStart(query.order, cursor, 'ascending'),
    (value) =>
      query.ownerConversationId === undefined ||
      value.owner?.conversationId === query.ownerConversationId,
    query.ownerTaskId !== undefined && query.ownerConversationId !== undefined,
  );
}

async function statusPrefix(
  table: 't' | 'u',
  conversationId: number | undefined,
  status: string | undefined,
): Promise<string> {
  const conversation = conversationId === undefined ? undefined : numberKey(conversationId);
  if (status === undefined)
    return conversation === undefined
      ? `i/${table}/all/`
      : `i/${table}/conversation/${conversation}/`;
  return conversation === undefined
    ? `i/${table}/status/${await hash(status)}/`
    : `i/${table}/combined/${conversation}/${await hash(status)}/`;
}

export async function scanTasks(
  view: View,
  query: TaskQuery,
  limit: number,
  cursor: Cursor | undefined,
): Promise<Page<AnyTask, Cursor>> {
  return scanRecords(
    view,
    await statusPrefix('t', query.conversationId, query.status),
    'task',
    limit,
    scanStart(query.order, cursor, 'ascending'),
    (value) =>
      (query.kind === undefined || value.kind === query.kind) &&
      (query.abortRequested === undefined || value.abortRequested === query.abortRequested) &&
      (query.background === undefined || value.background === query.background),
    query.kind !== undefined ||
      query.abortRequested !== undefined ||
      query.background !== undefined,
  );
}

export async function scanSubmissions(
  view: View,
  query: SubmissionQuery,
  limit: number,
  cursor: Cursor | undefined,
): Promise<Page<SubmissionRecord, Cursor>> {
  return scanRecords(
    view,
    await statusPrefix('u', query.conversationId, query.status),
    'submission',
    limit,
    scanStart(query.order, cursor, 'ascending'),
    () => true,
    false,
  );
}

export async function submissionByRequest(
  view: View,
  conversationId: number,
  requestId: string,
): Promise<SubmissionRecord | undefined> {
  const found = (await view.get(await requestKey(integer(conversationId), requestId))) as
    SubmissionRecord | undefined;
  if (
    found !== undefined &&
    (found.conversationId !== conversationId || found.requestId !== requestId)
  )
    throw new Error('A submission is stored under another request');
  return found;
}

export async function conversation(
  view: View,
  known: Known,
  id: number,
): Promise<ConversationRecord | undefined> {
  const cached = known.conversations.get(id);
  if (cached !== undefined) return cached;
  const value = (await record(view, id, 'conversation'))?.value;
  if (value !== undefined) remember(known.conversations, id, value);
  return value;
}

/**
 * The conversations whose entries `conversationId` sees, itself first, each with the highest
 * entry ID visible in it: a fork sees its parent only up to the fork point.
 */
async function ancestry(
  view: View,
  known: Known,
  conversationId: number,
  upper: number,
): Promise<{ id: number; upper: number }[]> {
  const segments: { id: number; upper: number }[] = [];
  const seen = new Set<number>();
  for (let current = conversationId; ;) {
    if (seen.has(current)) throw new Error(`Conversation ${current} is its own ancestor`);
    seen.add(current);
    const found = await conversation(view, known, integer(current));
    if (found === undefined) throw new StorageRequestError(`Unknown conversation: ${current}`);
    segments.push({ id: current, upper });
    if (found.parent === undefined) return segments;
    upper = Math.min(upper, found.parent.at);
    current = found.parent.conversationId;
  }
}

export async function entry(
  view: View,
  known: Known,
  id: number,
  conversationId?: number,
): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined> {
  integer(id);
  let owners: number[];
  if (conversationId !== undefined) {
    owners = (await ancestry(view, known, conversationId, id))
      .filter((segment) => id <= segment.upper)
      .map((segment) => segment.id);
  } else {
    let owner = known.entryOwners.get(id);
    if (owner === undefined) {
      owner = (await record(view, id, 'entry'))?.conversationId;
      if (owner === undefined) return undefined;
      remember(known.entryOwners, id, owner);
    }
    owners = [owner];
  }
  const found = (await view.getMany(owners.map((owner) => entryKey(owner, id)))).find(
    (value) => value !== undefined,
  ) as StoredEntry | undefined;
  return found === undefined ? undefined : { entry: found.value, commitSeq: found.commitSeq };
}

export async function findLatestHeadMarker(
  view: View,
  known: Known,
  conversationId: number,
  atOrBefore: number | undefined,
): Promise<(EntryRecord & { readonly head: EntryRecord['id'] }) | undefined> {
  const upper = atOrBefore === undefined ? Number.MAX_SAFE_INTEGER : integer(atOrBefore);
  const segments = await ancestry(view, known, conversationId, upper);
  const pages = await view.load(
    [],
    segments.map((segment) => {
      const prefix = `h/${numberKey(segment.id)}/`;
      return { prefix, end: `${prefix}${numberKey(segment.upper)}~`, reverse: true, limit: 1 };
    }),
  );
  const newest = pages.find((page) => page.items.length > 0)?.items[0]!.value as
    StoredEntry | undefined;
  return newest?.value as (EntryRecord & { readonly head: EntryRecord['id'] }) | undefined;
}

/** A page of one conversation's visible history. One round trip once its ancestry is known. */
export async function scanEntries(
  view: View,
  known: Known,
  query: EntryQuery,
  limit: number,
  cursor: Cursor | undefined,
): Promise<Page<EntryRecord, Cursor>> {
  integer(limit, 'scan limit');
  const { order, after } = scanStart(query.order, cursor, 'descending');
  const reverse = order === 'descending';
  let min = integer(query.minEntryId ?? 1);
  let max = integer(query.maxEntryId ?? Number.MAX_SAFE_INTEGER);
  // The cursor narrows the bound on the side the scan moves away from.
  if (after !== undefined && reverse) max = Math.min(max, after - 1);
  if (after !== undefined && !reverse) min = Math.max(min, after + 1);
  const segments = await ancestry(view, known, query.conversationId, max);
  if (min > max) return { items: [] };
  if (!reverse) segments.reverse();
  const scans = segments
    .filter((segment) => segment.upper >= min)
    .map((segment) => {
      const prefix = `e/${numberKey(segment.id)}/`;
      return {
        prefix,
        start: prefix + numberKey(min),
        end: `${prefix}${numberKey(segment.upper)}~`,
        reverse,
        limit: limit + 1,
      };
    });
  const pages = await view.load([], scans);
  const values: EntryRecord[] = [];
  for (let index = 0; index < scans.length && values.length <= limit; index++) {
    for (let page = pages[index]!; ;) {
      for (const item of page.items) values.push((item.value as StoredEntry).value);
      if (!page.more || values.length > limit) break;
      page = await view.scan({
        ...scans[index]!,
        after: page.items.at(-1)!.key,
        limit: limit + 1 - values.length,
      });
    }
  }
  return toPage(values, limit, order);
}
