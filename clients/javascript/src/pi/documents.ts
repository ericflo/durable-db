import { applyImmutableBatches } from '@earendil-works/chord/delta';
import { StorageRequestError } from '@earendil-works/pi-durable';
import type {
  Cursor,
  DocumentAddress,
  DocumentContent,
  DocumentCreate,
  DocumentPoint,
  DocumentQuery,
  DocumentRecord,
  JsonObject,
  Page,
  StorageWrite,
  StoredDocument,
} from '@earendil-works/pi-durable';
import { record, recordKey, scanRecords, type Stored } from './records.ts';
import { hash, integer, json, numberKey, type View } from './view.ts';

/** A commit that pi-durable's own rules reject. Its batch is not stored; the storage goes on. */
export class Refused extends Error {}

type StoredDocumentRecord = Extract<Stored, { type: 'document' }>;
/** `v/<id>`: the whole value of a document that keeps no history. */
type Current = { version: number; value: JsonObject; deltasSinceBase: number };
/** `d/<id>/<seq>`: one stored change of a document that keeps its history. */
type Revision = DocumentContent & { seq: number };

interface Action {
  create?: DocumentCreate;
  content?: DocumentContent;
  retire: boolean;
  existing?: StoredDocumentRecord;
  /** The new `v/<id>` value, computed before anything is staged because a delta can fail. */
  current?: Current;
}
export type Actions = Map<number, Action>;

const currentOnly = (value: DocumentCreate | DocumentRecord): boolean =>
  value.scope.kind !== 'conversation' || value.history === 'latest';
const alive = (value: DocumentRecord, at: DocumentPoint): boolean =>
  at === 'current'
    ? value.retiredAt === undefined
    : value.createdAt <= at && (value.retiredAt === undefined || at < value.retiredAt);

function scopeIdentity(scope: DocumentAddress['scope']): unknown[] {
  if (scope.kind === 'session') return ['session'];
  if (scope.kind === 'conversation') return ['conversation', integer(scope.conversationId)];
  if (scope.kind === 'task') return ['task', integer(scope.taskId)];
  throw new StorageRequestError('Invalid document scope');
}
const addressIdentity = (address: DocumentAddress): string =>
  json([
    address.kind,
    scopeIdentity(address.scope),
    address.key === undefined ? ['singleton'] : ['family', address.key],
  ]);
const liveKey = async (address: DocumentAddress): Promise<string> =>
  `l/${await hash(addressIdentity(address))}`;
const currentKey = (id: number): string => `v/${numberKey(id)}`;
const revisionPrefix = (id: number): string => `d/${numberKey(id)}/`;
const basePrefix = (id: number): string => `b/${numberKey(id)}/`;
const through = (prefix: string, at: DocumentPoint): string | undefined =>
  at === 'current' ? undefined : `${prefix}${numberKey(at)}~`;

function point(at: DocumentPoint): void {
  if (at !== 'current') integer(at, 'document point', 0);
}

/** The keys a commit reads for `write`, so that one round trip can fetch them for every write. */
export async function keysRead(write: StorageWrite): Promise<string[]> {
  switch (write.type) {
    case 'document.create':
      return [recordKey(write.record.id), await liveKey(write.record)];
    case 'document.copy':
      return [
        recordKey(write.record.id),
        await liveKey(write.record),
        recordKey(write.source.id),
        currentKey(write.source.id),
      ];
    case 'document.change':
      return write.content.kind === 'delta'
        ? [recordKey(write.id), currentKey(write.id)]
        : [recordKey(write.id)];
    case 'document.retire':
      return [recordKey(write.id)];
    default:
      return [];
  }
}

/** One incarnation's value at a point. One round trip without history, two with it. */
export async function materialize(
  view: View,
  id: number,
  at: DocumentPoint,
): Promise<StoredDocument | undefined> {
  point(at);
  const [bases] = await view.load(
    [recordKey(integer(id)), currentKey(id)],
    [{ prefix: basePrefix(id), end: through(basePrefix(id), at), reverse: true, limit: 1 }],
  );
  const stored = await record(view, id, 'document');
  if (stored === undefined) return undefined;
  if (at !== 'current' && currentOnly(stored.value))
    throw new StorageRequestError(`Document ${id} does not retain historical content`);
  if (!alive(stored.value, at)) return undefined;
  if (currentOnly(stored.value)) {
    const current = (await view.get(currentKey(id))) as Current | undefined;
    if (current === undefined || current.version !== stored.version)
      throw new Error(`Document ${id} is missing its current content`);
    return { record: stored.value, ...current };
  }
  if (bases!.items.length === 0) throw new Error(`Document ${id} is missing a required base`);
  const seq = bases!.items[0]!.value as number;
  const prefix = revisionPrefix(id);
  const [base, ...tail] = (
    await view.scanAll({ prefix, start: prefix + numberKey(seq), end: through(prefix, at) })
  ).map((item) => item.value as Revision);
  if (base?.kind !== 'base' || base.seq !== seq)
    throw new Error(`Document ${id} has an invalid base index`);
  const deltas = tail.map((revision) => {
    if (revision.kind !== 'delta' || revision.version !== base.version)
      throw new Error(`Document ${id} crosses a stored version boundary without a base`);
    return revision.ops;
  });
  return {
    record: stored.value,
    version: base.version,
    value: applyImmutableBatches(base.value, deltas),
    deltasSinceBase: deltas.length,
  };
}

export async function findDocument(
  view: View,
  address: DocumentAddress,
  at: DocumentPoint,
): Promise<DocumentRecord | undefined> {
  point(at);
  const identity = addressIdentity(address);
  const mismatch = () => new Error('A document is stored under another address');
  if (at === 'current') {
    const live = (await view.get(await liveKey(address))) as DocumentRecord | undefined;
    if (live !== undefined && addressIdentity(live) !== identity) throw mismatch();
    return live;
  }
  // Incarnations of the address by creation, newest first: the first one alive at `at`.
  const prefix = `a/${await hash(identity)}/`;
  for (let after: string | undefined; ;) {
    const found = await view.scan({
      prefix,
      after,
      end: through(prefix, at),
      reverse: true,
      limit: 16,
    });
    const ids = found.items.map((item) => item.value as number);
    await view.load(ids.map(recordKey));
    for (const id of ids) {
      const stored = await record(view, id, 'document');
      if (stored === undefined || addressIdentity(stored.value) !== identity) throw mismatch();
      if (alive(stored.value, at)) return stored.value;
    }
    if (!found.more) return undefined;
    after = found.items.at(-1)!.key;
  }
}

export async function scanDocuments(
  view: View,
  query: DocumentQuery,
  limit: number,
  cursor: Cursor | undefined,
): Promise<Page<DocumentRecord, Cursor>> {
  point(query.at);
  const after = cursor?.after;
  if (
    after !== undefined &&
    (typeof after !== 'number' || !Number.isSafeInteger(after) || after < 0)
  )
    throw new StorageRequestError('Invalid storage cursor');
  const scope = await hash(scopeIdentity(query.scope));
  return scanRecords(
    view,
    query.kind === undefined
      ? `i/d/scope/${scope}/`
      : `i/d/kind/${scope}/${await hash(query.kind)}/`,
    'document',
    limit,
    { order: 'ascending', after },
    (value) => alive(value, query.at),
    true,
  );
}

/**
 * Check one batch's document writes against the view, resolving copies to their content. Throws
 * `Refused` for what pi-durable's rules reject. Nothing is staged, so a refused batch leaves the
 * view as it was.
 */
export async function prepareDocuments(
  view: View,
  writes: readonly StorageWrite[],
): Promise<Actions> {
  const actions: Actions = new Map();
  const changed = new Set<number>();
  for (const write of writes) {
    if (write.type === 'document.create' || write.type === 'document.copy')
      changed.add(write.record.id);
    else if (write.type === 'document.change' || write.type === 'document.retire')
      changed.add(write.id);
  }
  for (const write of writes) {
    if (
      write.type !== 'document.create' &&
      write.type !== 'document.copy' &&
      write.type !== 'document.change' &&
      write.type !== 'document.retire'
    )
      continue;
    const id = integer(
      write.type === 'document.create' || write.type === 'document.copy'
        ? write.record.id
        : write.id,
    );
    let action = actions.get(id);
    if (action === undefined) actions.set(id, (action = { retire: false }));
    if (write.type === 'document.retire') {
      if (action.retire) throw new Refused(`Document ${id} is retired more than once`);
      action.retire = true;
      continue;
    }
    if (action.content !== undefined)
      throw new Refused(`Document ${id} has more than one content command`);
    if (write.type === 'document.change') action.content = write.content;
    else {
      action.create = write.record;
      action.content =
        write.type === 'document.create' ? write.content : await copied(view, write, changed);
    }
  }

  // An address holds at most one incarnation that is not retired. Only a creation can exceed
  // that, so only the addresses this batch creates at are read; elsewhere the count only falls.
  const createdAt = new Set<string>();
  for (const { create } of actions.values())
    if (create !== undefined) createdAt.add(await liveKey(create));
  const live = new Map<string, number>();
  for (const [id, action] of actions) {
    const { create, content } = action;
    const existing = await record(view, id, 'document');
    if (create === undefined && existing === undefined)
      throw new Refused(`Unknown document: ${id}`);
    if (create !== undefined && existing !== undefined)
      throw new Refused(`Document ${id} already exists`);
    if (existing?.value.retiredAt !== undefined) throw new Refused(`Document ${id} is retired`);
    if (content !== undefined && content.kind !== 'base' && content.kind !== 'delta')
      throw new Refused(`Document ${id} has invalid content`);
    if (content?.kind === 'delta') {
      if (existing === undefined) throw new Refused(`Document ${id} delta has no base`);
      if (existing.version !== content.version)
        throw new Refused(`Document ${id} version transition requires a base`);
    }
    action.existing = existing;
    if (content !== undefined && currentOnly(create ?? existing!.value)) {
      if (content.kind === 'base')
        action.current = { version: content.version, value: content.value, deltasSinceBase: 0 };
      else {
        const before = (await view.get(currentKey(id))) as Current | undefined;
        if (before?.version !== existing!.version)
          throw new Error(`Document ${id} is missing its current content`);
        action.current = {
          version: content.version,
          value: applyImmutableBatches(before.value, [content.ops]),
          deltasSinceBase: before.deltasSinceBase + 1,
        };
      }
    }
    if (create === undefined && !action.retire) continue;
    const key = await liveKey(create ?? existing!.value);
    let count = live.get(key);
    if (count === undefined)
      count = createdAt.has(key) && (await view.get(key)) !== undefined ? 1 : 0;
    if (action.retire && existing !== undefined) count--;
    if (create !== undefined && !action.retire) count++;
    live.set(key, count);
  }
  for (const count of live.values())
    if (count > 1) throw new Refused('Document address already has a current incarnation');
  return actions;
}

/** The base a `document.copy` creates its document from: the source's value at the chosen point. */
async function copied(
  view: View,
  write: Extract<StorageWrite, { type: 'document.copy' }>,
  changed: Set<number>,
): Promise<Extract<DocumentContent, { kind: 'base' }>> {
  const { record: target, source } = write;
  const refuse = (reason: string, cause?: unknown) =>
    new Refused(`Document copy ${target.id} was rejected: ${reason}`, { cause });
  if (changed.has(source.id)) throw refuse('its source is changed in the copy batch');
  let stored: StoredDocument | undefined;
  try {
    stored = await materialize(view, source.id, source.at);
  } catch (error) {
    // An invalid request is the copy's fault. Anything else is a storage failure.
    if (error instanceof StorageRequestError) throw refuse('its source cannot be read', error);
    throw error;
  }
  if (stored === undefined) throw refuse('its source cannot be read');
  const from = stored.record;
  if (
    from.scope.kind !== 'conversation' ||
    target.scope.kind !== 'conversation' ||
    from.kind !== target.kind ||
    from.key !== target.key ||
    from.history !== target.history ||
    from.fork !== target.fork
  )
    throw refuse('its source does not match the copied record');
  return { kind: 'base', version: stored.version, value: stored.value };
}

/** Stage one checked batch's document writes at commit sequence `seq`. */
export async function applyDocuments(view: View, actions: Actions, seq: number): Promise<void> {
  const created: [string, DocumentRecord][] = [];
  for (const [id, action] of actions) {
    const { create, content, existing } = action;
    let value = (
      create === undefined ? existing!.value : { ...create, createdAt: seq }
    ) as DocumentRecord;
    if (action.retire) value = { ...value, retiredAt: seq } as DocumentRecord;
    if (content !== undefined && !currentOnly(value)) {
      view.set(revisionPrefix(id) + numberKey(seq), { ...content, seq } satisfies Revision);
      if (content.kind === 'base') view.set(basePrefix(id) + numberKey(seq), seq);
    }
    if (currentOnly(value)) {
      if (!action.retire && action.current !== undefined) view.set(currentKey(id), action.current);
      else if (action.retire && create === undefined) view.set(currentKey(id), undefined);
    }
    if (create !== undefined) {
      const scope = await hash(scopeIdentity(value.scope));
      const address = await hash(addressIdentity(value));
      view.set(`a/${address}/${numberKey(seq)}/${numberKey(id)}`, id);
      view.set(`i/d/scope/${scope}/${numberKey(id)}`, id);
      view.set(`i/d/kind/${scope}/${await hash(value.kind)}/${numberKey(id)}`, id);
      if (!action.retire) created.push([await liveKey(value), value]);
    } else if (action.retire) view.set(await liveKey(value), undefined);
    const version = content?.version ?? existing!.version;
    if (create !== undefined || action.retire || version !== existing!.version)
      view.set(recordKey(id), { type: 'document', value, version } satisfies StoredDocumentRecord);
  }
  // After every retirement: a batch may retire an address's incarnation and create its next.
  for (const [key, value] of created) view.set(key, value);
}
