/**
 * pi-durable 1.5.2 `Storage` on one durable-db database.
 *
 * Keys under the storage's prefix:
 *
 *   owner                     who may write: the fence every transaction checks
 *   meta                      the next commit sequence and record ID
 *   r/<id>                    a conversation, task, submission or document record; for an
 *                             entry, the conversation that holds it
 *   e/<conversation>/<id>     an entry and its commit sequence
 *   h/<conversation>/<id>     the same, for entries that carry a head
 *   i/...                     indexes for scans; the value is the record ID
 *   q/<conversation>/<hash>   a submission, by its request ID
 *   l/<hash>                  the current document at an address
 *   a/<hash>/<seq>/<id>       every document that ever lived at an address
 *   v/<id>                    the value of a document that keeps no history
 *   d/<id>/<seq>, b/<id>/<seq>  the changes of a document that keeps history, and its bases
 */
import type { Context } from '@earendil-works/chord/context';
import type {
  CommitResult,
  ConversationId,
  ConversationQuery,
  ConversationRecord,
  Cursor,
  DocumentAddress,
  DocumentId,
  DocumentPoint,
  DocumentQuery,
  DocumentRecord,
  EntryId,
  EntryQuery,
  EntryRecord,
  Id,
  Page,
  Seq,
  Storage,
  StorageWrite,
  StoredDocument,
  SubmissionId,
  SubmissionQuery,
  SubmissionRecord,
  TaskId,
  TaskQuery,
} from '@earendil-works/pi-durable';
import { ConflictError, DatabaseError } from '../errors.ts';
import type { Check, Commit, Database } from '../database.ts';
import * as documents from './documents.ts';
import * as records from './records.ts';
import { View, type Reader } from './view.ts';

export interface PiStorageOptions {
  /** Every key of this storage begins with it, so one database can hold several. Default `pi/`. */
  prefix?: string;
  /**
   * Take the storage from an owner that did not close it, such as a process that crashed. The
   * previous owner's later commits fail.
   */
  takeover?: boolean;
}

type Ownership = { format: 2; owner: string | null };
type Meta = { nextId: number; nextSeq: number };

/**
 * Open the storage of one pi-durable Session. A storage has one owner at a time: opening fails
 * while another owner holds it, unless `takeover` is set.
 */
export async function openPiStorage(
  db: Database,
  options: PiStorageOptions = {},
): Promise<Storage> {
  const { prefix = 'pi/', takeover = false } = options;
  if (typeof prefix !== 'string' || !/^[A-Za-z0-9/_.-]{1,64}$/.test(prefix))
    throw new TypeError('prefix must be 1 to 64 characters from A-Z a-z 0-9 / _ . -');
  const ownerKey = `${prefix}owner`;
  const metaKey = `${prefix}meta`;
  // The one strong read: who owns the storage now.
  const found = await db.read(
    { keys: [ownerKey, metaKey], scans: [{ prefix, limit: 1 }] },
    { consistency: 'strong' },
  );
  const held = found.values[0];
  const holder = held?.value as Ownership | undefined;
  if (held === undefined && found.scans[0]!.items.length > 0)
    throw new Error(`Keys under ${JSON.stringify(prefix)} are not a Pi storage`);
  if (held !== undefined && holder?.format !== 2)
    throw new Error(`The Pi storage under ${JSON.stringify(prefix)} has an unsupported format`);
  if (holder?.owner != null && !takeover)
    throw new Error(
      'Pi storage already has an owner. Close it there, or open with takeover: true after a crash.',
    );
  const owner = crypto.randomUUID();
  let claimed: number;
  try {
    ({ revision: claimed } = await db.commit({
      checks: [{ key: ownerKey, version: held?.version ?? 0 }],
      writes: [{ key: ownerKey, value: { format: 2, owner } satisfies Ownership }],
    }));
  } catch (error) {
    if (!(error instanceof ConflictError)) throw error;
    throw new Error('Pi storage ownership changed while opening it', { cause: error });
  }
  // Every commit of the previous owner either came before the claim or fails its fence, so a
  // read at the claim's revision or later has the final counters.
  const meta = (await db.get<Meta>(metaKey, { consistency: 'session' })) ?? {
    nextId: 2,
    nextSeq: 1,
  };
  if (!validCounters(meta)) throw new Error('The Pi storage counters are invalid');
  return new PiStorage(db, prefix, owner, claimed, meta);
}

// 2^53 is the next ID once the largest valid ID is stored: representable, and never minted.
const validCounters = (meta: Meta): boolean =>
  Number.isSafeInteger(meta.nextSeq) &&
  meta.nextSeq >= 1 &&
  (Number.isSafeInteger(meta.nextId) || meta.nextId === Number.MAX_SAFE_INTEGER + 1) &&
  meta.nextId >= 2;

/** The record ID a write claims, and for which kind of record. */
function claim(write: StorageWrite): { id: number; type: string } | undefined {
  switch (write.type) {
    case 'conversation':
    case 'entry':
    case 'task':
    case 'submission':
      return { id: write.value.id, type: write.type };
    case 'document.create':
    case 'document.copy':
      return { id: write.record.id, type: 'document' };
    default:
      return undefined;
  }
}

/** One global ID namespace: conversations, entries and documents are written once. */
async function checkIds(view: View, writes: readonly StorageWrite[]): Promise<void> {
  const claimed = new Map<number, string>();
  for (const write of writes) {
    const claiming = claim(write);
    if (claiming === undefined) continue;
    const { id, type } = claiming;
    const existing = (await records.record(view, id))?.type;
    const earlier = claimed.get(id);
    const rewritable = type === 'task' || type === 'submission';
    if (existing !== undefined && !(rewritable && existing === type))
      throw new documents.Refused(`ID ${id} already belongs to ${existing}`);
    if (earlier !== undefined && !rewritable)
      throw new documents.Refused(`ID ${id} is written more than once`);
    if (earlier !== undefined && earlier !== type)
      throw new documents.Refused(`ID ${id} is written as two record types`);
    claimed.set(id, type);
  }
}

class PiStorage implements Storage {
  readonly #db: Database;
  readonly #prefix: string;
  readonly #owner: string;
  readonly #ownerKey: string;
  readonly #fence: Check;
  readonly #known: records.Known = { conversations: new Map(), entryOwners: new Map() };
  /** Commits and close run one at a time, in the order they were called. */
  #queue: Promise<unknown> = Promise.resolve();
  /** Changes whenever a commit starts or stops writing; odd while one is writing. */
  #stamp = 0;
  /** The commit being written. */
  #writing: Promise<void> | undefined;
  /** Reads repeating because a commit was written while they ran. The next commit waits. */
  readonly #rereads = new Set<Promise<unknown>>();
  #reads = 0;
  #drained: (() => void) | undefined;
  #nextId: number;
  #nextSeq: number;
  /** Every stored record ID is below this, so an ID at or above it needs no existence read. */
  #unused: number;
  #closing: Promise<void> | undefined;
  #failure: unknown;

  constructor(db: Database, prefix: string, owner: string, claimed: number, meta: Meta) {
    this.#db = db;
    this.#prefix = prefix;
    this.#owner = owner;
    // The owner key changes only when ownership does, so its version at the claim is the fence.
    this.#ownerKey = `${prefix}owner`;
    this.#fence = { key: this.#ownerKey, version: claimed };
    this.#nextId = meta.nextId;
    this.#nextSeq = meta.nextSeq;
    this.#unused = meta.nextId;
  }

  async commit(
    input: readonly (readonly StorageWrite[])[],
    receive: (index: number, seq: Seq) => void,
    _context: Context,
  ): Promise<CommitResult> {
    this.#assertOpen();
    // Detach from the caller before the first await.
    const batches = structuredClone(input);
    return this.#serial(async () => {
      this.#assertSound();
      const view = new View(this.#read);
      // One round trip for everything the checks of every batch read.
      const reads: string[] = [];
      for (const write of batches.flat()) {
        const claiming = claim(write);
        if (claiming !== undefined) {
          const key = records.recordKey(claiming.id);
          if (claiming.id >= this.#unused) view.assumeAbsent(key);
          else reads.push(key);
        }
        reads.push(...(await documents.keysRead(write)));
      }
      await view.load(reads);

      const prefix = this.#prefix;
      const staged: { seq: number; nextId: number; unused: number; transaction: Commit }[] = [];
      let nextId = this.#nextId;
      let unused = this.#unused;
      let refused: unknown;
      for (const writes of batches) {
        let actions: documents.Actions;
        try {
          await checkIds(view, writes);
          actions = await documents.prepareDocuments(view, writes);
        } catch (error) {
          if (!(error instanceof documents.Refused)) throw error;
          refused = error;
          break;
        }
        const seq = this.#nextSeq + staged.length;
        if (!Number.isSafeInteger(seq)) throw new Error('Commit sequence space is exhausted');
        for (const write of writes) {
          const claiming = claim(write);
          if (claiming !== undefined) unused = Math.max(unused, claiming.id + 1);
          if (
            write.type === 'conversation' ||
            write.type === 'entry' ||
            write.type === 'task' ||
            write.type === 'submission'
          )
            await records.putRecord(view, write, seq);
        }
        await documents.applyDocuments(view, actions, seq);
        nextId = Math.max(nextId, unused);
        view.set('meta', { nextId, nextSeq: seq + 1 } satisfies Meta);
        staged.push({
          seq,
          nextId,
          unused,
          transaction: {
            checks: [this.#fence],
            writes: view
              .takeBatch()
              .map(({ key, value }) =>
                value === undefined
                  ? { key: prefix + key, delete: true }
                  : { key: prefix + key, value },
              ),
          },
        });
      }

      // One round trip stores every accepted batch, each as its own transaction.
      let stored = 0;
      let failure: unknown;
      if (staged.length > 0) {
        while (this.#rereads.size > 0) await Promise.allSettled(this.#rereads);
        let written!: () => void;
        this.#writing = new Promise((resolve) => (written = resolve));
        this.#stamp++;
        try {
          const outcomes = await this.#db.commitAll(staged.map(({ transaction }) => transaction));
          failure = outcomes.find((outcome) => outcome.error !== undefined)?.error;
          stored = outcomes.filter((outcome) => outcome.error === undefined).length;
        } catch (error) {
          failure = error;
        } finally {
          this.#stamp++;
          this.#writing = undefined;
          written();
        }
      }
      for (let index = 0; index < stored; index++) {
        for (const write of batches[index]!) {
          if (write.type === 'conversation')
            records.remember(this.#known.conversations, write.value.id, write.value);
          else if (write.type === 'entry')
            records.remember(this.#known.entryOwners, write.value.id, write.value.conversationId);
        }
        const batch = staged[index]!;
        this.#nextSeq = batch.seq + 1;
        this.#nextId = Math.max(this.#nextId, batch.nextId);
        this.#unused = batch.unused;
      }
      for (let index = 0; index < stored; index++) receive(index, staged[index]!.seq as Seq);
      // The only check is the fence, so a conflict means another owner took over. After any
      // other definite refusal, such as an oversized value, this storage still knows what is
      // stored. After an unknown outcome it does not.
      if (failure instanceof ConflictError)
        throw this.#fail(
          new Error('Pi storage was taken over by another owner', { cause: failure }),
        );
      if (failure !== undefined)
        throw failure instanceof DatabaseError && !failure.outcomeUnknown
          ? failure
          : this.#fail(failure);
      return refused === undefined ? { stored } : { stored, refused };
    });
  }

  async mintId<I extends Id<string>>(): Promise<I> {
    this.#assertOpen();
    if (!Number.isSafeInteger(this.#nextId)) throw new Error('ID space is exhausted');
    return this.#nextId++ as I;
  }

  conversation(id: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
    return this.#reading(async (view) => (await records.record(view, id, 'conversation'))?.value);
  }

  scanConversations(
    query: ConversationQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<ConversationRecord, Cursor>> {
    return this.#reading((view) => records.scanConversations(view, query, limit, cursor));
  }

  entry(
    first: EntryId | ConversationId,
    second: Context | EntryId,
    context?: Context,
  ): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined> {
    return this.#reading((view) =>
      context === undefined
        ? records.entry(view, this.#known, first)
        : records.entry(view, this.#known, second as EntryId, first),
    );
  }

  findLatestHeadMarker(
    conversationId: ConversationId,
    atOrBeforeEntryId: EntryId | undefined,
    _context: Context,
  ): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
    return this.#reading((view) =>
      records.findLatestHeadMarker(view, this.#known, conversationId, atOrBeforeEntryId),
    );
  }

  scanEntries(
    query: EntryQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<EntryRecord, Cursor>> {
    return this.#reading((view) => records.scanEntries(view, this.#known, query, limit, cursor));
  }

  task(id: TaskId, _context: Context): Promise<records.AnyTask | undefined> {
    return this.#reading(async (view) => (await records.record(view, id, 'task'))?.value);
  }

  scanTasks(
    query: TaskQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<records.AnyTask, Cursor>> {
    return this.#reading((view) => records.scanTasks(view, query, limit, cursor));
  }

  submission(id: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
    return this.#reading(async (view) => (await records.record(view, id, 'submission'))?.value);
  }

  scanSubmissions(
    query: SubmissionQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<SubmissionRecord, Cursor>> {
    return this.#reading((view) => records.scanSubmissions(view, query, limit, cursor));
  }

  submissionByRequest(
    conversationId: ConversationId,
    requestId: string,
    _context: Context,
  ): Promise<SubmissionRecord | undefined> {
    return this.#reading((view) => records.submissionByRequest(view, conversationId, requestId));
  }

  findDocument(
    address: DocumentAddress,
    at: DocumentPoint,
    _context: Context,
  ): Promise<DocumentRecord | undefined> {
    return this.#reading((view) => documents.findDocument(view, address, at));
  }

  document(
    id: DocumentId,
    at: DocumentPoint,
    _context: Context,
  ): Promise<StoredDocument | undefined> {
    return this.#reading((view) => documents.materialize(view, id, at));
  }

  scanDocuments(
    query: DocumentQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<DocumentRecord, Cursor>> {
    return this.#reading((view) => documents.scanDocuments(view, query, limit, cursor));
  }

  /** Wait for operations underway, then give up ownership so that the next open needs no takeover. */
  close(_context: Context): Promise<void> {
    return (this.#closing ??= this.#serial(async () => {
      if (this.#reads > 0) await new Promise<void>((resolve) => (this.#drained = resolve));
      // After a failed commit the stored state is not known here; the next owner takes over.
      if (this.#failure !== undefined) return;
      try {
        await this.#db.commit({
          checks: [this.#fence],
          writes: [
            { key: this.#ownerKey, value: { format: 2, owner: null } satisfies Ownership },
            // Keep IDs that were minted but never stored from being minted again.
            {
              key: `${this.#prefix}meta`,
              value: { nextId: this.#nextId, nextSeq: this.#nextSeq } satisfies Meta,
            },
          ],
        });
      } catch (error) {
        // A conflict means another owner took over: there is nothing left to release.
        if (!(error instanceof ConflictError)) throw error;
      }
    }));
  }

  #serial<T>(task: () => Promise<T>): Promise<T> {
    const run = this.#queue.then(task);
    this.#queue = run.catch(() => {});
    return run;
  }

  /**
   * A read. Reads run beside each other and beside a commit. One request sees one revision. A
   * read of several requests could see the state before a commit of this storage in one and the
   * state after it in another, so when a commit was written meanwhile, the read runs again.
   */
  async #reading<T>(call: (view: View) => Promise<T>): Promise<T> {
    this.#assertOpen();
    this.#reads++;
    try {
      this.#assertSound();
      const stamp = this.#stamp;
      const view = new View(this.#read);
      let result: T | undefined;
      let error: unknown;
      try {
        result = await call(view);
      } catch (thrown) {
        error = thrown ?? new Error('A read failed');
      }
      if (view.requests > 1 && (stamp % 2 === 1 || stamp !== this.#stamp)) {
        const again = (async () => {
          await this.#writing;
          this.#assertSound();
          return call(new View(this.#read));
        })();
        this.#rereads.add(again);
        try {
          return await again;
        } finally {
          this.#rereads.delete(again);
        }
      }
      if (error !== undefined) throw error;
      return result as T;
    } finally {
      if (--this.#reads === 0) this.#drained?.();
    }
  }

  /** Session reads return this storage's own commits. The owner key rides along on each one. */
  readonly #read: Reader = async (keys, scans) => {
    const prefix = this.#prefix;
    const within = (key: string | undefined) => (key === undefined ? undefined : prefix + key);
    const { values, scans: pages } = await this.#db.read(
      {
        keys: [this.#ownerKey, ...keys.map((key) => prefix + key)],
        scans: scans.map(({ after, ...scan }) => ({
          ...scan,
          prefix: prefix + scan.prefix,
          start: within(scan.start),
          end: within(scan.end),
          cursor: within(after),
        })),
      },
      { consistency: 'session' },
    );
    if ((values[0]?.value as Ownership | undefined)?.owner !== this.#owner)
      throw this.#fail(new Error('Pi storage was taken over by another owner'));
    return {
      values: values.slice(1).map((entry) => entry?.value),
      scans: pages.map((page) => ({
        items: page.items.map(({ key, value }) => ({ key: key.slice(prefix.length), value })),
        more: page.cursor !== undefined,
      })),
    };
  };

  /** Operations that began before `close()` finish; later ones are refused. */
  #assertOpen(): void {
    if (this.#closing !== undefined) throw new Error('Pi storage is closed');
    this.#assertSound();
  }

  #assertSound(): void {
    if (this.#failure !== undefined)
      throw new Error('Pi storage failed; open it again', { cause: this.#failure });
  }

  /** Record the first failure after which the stored state is no longer known here. */
  #fail<E>(error: E): E {
    this.#failure ??= error;
    return error;
  }
}
