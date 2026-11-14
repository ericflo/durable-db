import { ConflictError, DatabaseError, errorFromBody } from './errors.ts';
import {
  backoff,
  connect,
  request,
  sleep,
  type Connection,
  type ConnectionOptions,
} from './http.ts';

/**
 * `strong` reads reflect every transaction acknowledged before the read was sent and cost one
 * storage round trip. `session` reads are served from the server's memory at the highest
 * revision this client has seen or newer, so a client always reads its own writes.
 */
export type Consistency = 'strong' | 'session';

export interface DatabaseOptions extends ConnectionOptions {
  /** The database's name. */
  database: string;
  /** Consistency of reads that do not choose one. Default `strong`. */
  consistency?: Consistency;
}

export interface ReadOptions {
  consistency?: Consistency;
}

export interface Item<T = unknown> {
  key: string;
  value: T;
}

/** A stored value with its version: the revision of the transaction that last wrote the key. */
export interface Entry<T = unknown> extends Item<T> {
  version: number;
}

/** A set of keys: those with `prefix`, at or after `start`, and before `end`. */
export interface Range {
  prefix?: string;
  start?: string;
  end?: string;
}

export interface Scan extends Range {
  /** Return keys in descending order. */
  reverse?: boolean;
  /** At most this many items. Default 100, at most 10,000. */
  limit?: number;
  /** Continue after a previous page: its `cursor`. */
  cursor?: string;
}

export interface ListOptions extends Scan, ReadOptions {}

export interface Page<T = unknown> {
  items: Entry<T>[];
  /** Present when more keys remain. Pass it to the next call. */
  cursor?: string;
  /** The revision the page was read at. */
  revision: number;
}

export interface ReadRequest {
  keys?: readonly string[];
  scans?: readonly Scan[];
}

/** Keys and scans read together at one revision. */
export interface ReadResult<T = unknown> {
  revision: number;
  /** One per requested key; `undefined` for an absent key. */
  values: (Entry<T> | undefined)[];
  /** One per requested scan. */
  scans: Page<T>[];
}

export type Write = { key: string; value: unknown } | { key: string; delete: true };

export type Check =
  /** Passes while `key` is at `version`. Version 0 means the key does not exist. */
  | { key: string; version: number }
  /** Passes while no key in `range` was written after `revision`. */
  | { range: Range; revision: number };

/** One atomic transaction: every write applies, or none does. */
export interface Commit {
  /** At least one write; each key at most once. */
  writes: readonly Write[];
  /** If any check fails, nothing is written and the commit fails with `ConflictError`. */
  checks?: readonly Check[];
  /** Passes only while the whole database is at this revision. */
  ifRevision?: number;
  /** Repeating a commit with the same key returns the first result. Default: a random key. */
  idempotencyKey?: string;
}

export interface CommitResult {
  /** The database revision this commit created. */
  revision: number;
}

export type CommitOutcome =
  { revision: number; error?: undefined } | { error: DatabaseError; revision?: undefined };

export interface TransactionOptions {
  /** Times the callback may run before a `ConflictError` is thrown. Default 10. */
  maxAttempts?: number;
}

/** Reads and buffered writes of one `Database.transaction()` callback. */
export interface Transaction {
  /** The value of `key`, or `undefined`. Sees this transaction's own writes. */
  get<T = unknown>(key: string): Promise<T | undefined>;
  getMany<T = unknown>(keys: readonly string[]): Promise<(T | undefined)[]>;
  /** One page of a range. Sees this transaction's own writes. */
  list<T = unknown>(options?: Scan): Promise<{ items: Item<T>[]; cursor?: string }>;
  /** Buffer a write. Nothing is sent until the callback returns. */
  set(key: string, value: unknown): void;
  /** Buffer a delete. */
  delete(key: string): void;
}

interface WireRead {
  revision: number;
  values: ({ value: unknown; version: number } | null)[];
  scans: { items: Entry[]; next: string | null }[];
}

interface WireResults {
  results: (
    { revision: number } | { error: { code: string; message: string; retryable: boolean } }
  )[];
}

// The server accepts 32 MiB per request, and a character is at most three bytes of UTF-8.
// Larger groups of transactions are sent in parts.
const MAX_REQUEST_CHARS = 8 * 1024 * 1024;

/** One database: an ordered map from string keys to JSON values, changed by atomic transactions. */
export class Database {
  readonly #connection: Connection;
  readonly #path: string;
  readonly #consistency: Consistency;
  #revision = 0;

  constructor(options: DatabaseOptions) {
    if (typeof options.database !== 'string' || options.database === '')
      throw new TypeError('database must be the name of a database');
    this.#connection = connect(options);
    this.#path = `/v1/databases/${encodeURIComponent(options.database)}`;
    this.#consistency = options.consistency ?? 'strong';
  }

  /** The highest revision this client has seen. `session` reads are at this revision or newer. */
  get revision(): number {
    return this.#revision;
  }

  /** The value of `key`, or `undefined` when it does not exist. */
  async get<T = unknown>(key: string, options?: ReadOptions): Promise<T | undefined> {
    return (await this.getEntry<T>(key, options))?.value;
  }

  /** The value of `key` with its version, or `undefined` when it does not exist. */
  async getEntry<T = unknown>(key: string, options?: ReadOptions): Promise<Entry<T> | undefined> {
    return (await this.read<T>({ keys: [key] }, options)).values[0];
  }

  /** The values of `keys`, in order, all read at one revision. */
  async getMany<T = unknown>(
    keys: readonly string[],
    options?: ReadOptions,
  ): Promise<(T | undefined)[]> {
    return (await this.read<T>({ keys }, options)).values.map((entry) => entry?.value);
  }

  /** One page of a range, in key order. */
  async list<T = unknown>(options: ListOptions = {}): Promise<Page<T>> {
    const { consistency, ...scan } = options;
    return (await this.read<T>({ scans: [scan] }, { consistency })).scans[0]!;
  }

  /** Every entry of a range, fetched a page at a time. `limit` bounds the total. */
  async *scan<T = unknown>(options: ListOptions = {}): AsyncGenerator<Entry<T>, void, undefined> {
    let remaining = options.limit ?? Infinity;
    let cursor = options.cursor;
    while (remaining > 0) {
      const page = await this.list<T>({ ...options, cursor, limit: Math.min(remaining, 1000) });
      yield* page.items;
      remaining -= page.items.length;
      if (page.cursor === undefined) return;
      cursor = page.cursor;
    }
  }

  /** Any number of keys and scans in one round trip, all read at one revision. */
  async read<T = unknown>(
    { keys = [], scans = [] }: ReadRequest,
    options: ReadOptions = {},
  ): Promise<ReadResult<T>> {
    const consistency = options.consistency ?? this.#consistency;
    const result = await request<WireRead>(this.#connection, 'POST', `${this.#path}/read`, {
      keys,
      scans: scans.map(({ cursor, ...scan }) =>
        cursor === undefined ? scan : { ...scan, after: cursor },
      ),
      ...(consistency === 'session' ? { consistency, min_revision: this.#revision } : {}),
    });
    const revision = this.#saw(result.revision);
    return {
      revision,
      values: result.values.map((found, index) =>
        found === null
          ? undefined
          : { key: keys[index]!, value: found.value as T, version: found.version },
      ),
      scans: result.scans.map(({ items, next }) => ({
        items: items as Entry<T>[],
        ...(next === null ? {} : { cursor: next }),
        revision,
      })),
    };
  }

  /** Store `value`, any JSON except `null`, under `key`. */
  set(key: string, value: unknown): Promise<CommitResult> {
    return this.commit({ writes: [{ key, value }] });
  }

  /** Delete `key`. Deleting an absent key succeeds. */
  delete(key: string): Promise<CommitResult> {
    return this.commit({ writes: [{ key, delete: true }] });
  }

  /** Apply one atomic transaction. Throws `ConflictError` when a check fails. */
  async commit(commit: Commit): Promise<CommitResult> {
    const sent = { ...commit, idempotencyKey: commit.idempotencyKey ?? crypto.randomUUID() };
    try {
      const path = `${this.#path}/commit`;
      const result = await request<CommitResult>(this.#connection, 'POST', path, wire(sent), true);
      return { revision: this.#saw(result.revision) };
    } catch (error) {
      if (error instanceof DatabaseError && error.outcomeUnknown) error.commit = sent;
      throw error;
    }
  }

  /**
   * Apply several transactions in order in one round trip, each atomic and with its own
   * revision. The first failure stops the rest, which report a `ConflictError`. A group too
   * large for one request is sent in several.
   */
  async commitAll(commits: readonly Commit[]): Promise<CommitOutcome[]> {
    const outcomes: CommitOutcome[] = [];
    const parts = commits.map((commit) => JSON.stringify(wire(commit)));
    for (let start = 0; start < parts.length;) {
      let end = start + 1;
      for (let size = parts[start]!.length; end < parts.length; end++) {
        size += parts[end]!.length;
        if (size > MAX_REQUEST_CHARS) break;
      }
      let results: WireResults['results'];
      try {
        ({ results } = await request<WireResults>(
          this.#connection,
          'POST',
          `${this.#path}/commit`,
          `{"transactions":[${parts.slice(start, end).join(',')}]}`,
          true,
        ));
      } catch (error) {
        // The first request failing fails the call. A later one leaves earlier parts stored.
        if (start === 0 || !(error instanceof DatabaseError)) throw error;
        for (let index = start; index < end; index++) outcomes.push({ error });
        break;
      }
      for (const result of results)
        outcomes.push(
          'error' in result
            ? { error: errorFromBody(result.error) }
            : { revision: this.#saw(result.revision) },
        );
      if (outcomes.at(-1)?.error !== undefined) break;
      start = end;
    }
    while (outcomes.length < commits.length)
      outcomes.push({ error: conflict('not attempted: an earlier transaction failed') });
    return outcomes;
  }

  /**
   * Run `callback` as one transaction. It reads through `tx` and buffers writes; when it returns,
   * the writes are committed with a check for every key and range it read, so the result is as if
   * it ran alone. If another transaction changed something it read, the callback runs again,
   * reading at the revision of that change or later. Keep side effects outside it. A callback
   * that buffers no write commits nothing.
   */
  async transaction<T>(
    callback: (tx: Transaction) => T | Promise<T>,
    { maxAttempts = 10 }: TransactionOptions = {},
  ): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      const tx = new BufferedTransaction(this);
      try {
        const result = await callback(tx);
        const commit = tx.finish();
        if (commit !== undefined) await this.commit(commit);
        return result;
      } catch (error) {
        tx.finish();
        if (!(error instanceof ConflictError) || attempt >= maxAttempts) throw error;
        // The conflict names a revision that includes its cause, so the next attempt's
        // session reads see it. A conflict raised locally carries none; ask the server.
        if (error.revision !== undefined) this.#saw(error.revision);
        else await this.read({}, { consistency: 'strong' });
      }
      await sleep(backoff(attempt, 5, 1_000));
    }
  }

  #saw(revision: number): number {
    if (revision > this.#revision) this.#revision = revision;
    return revision;
  }
}

function wire({ writes, checks, ifRevision, idempotencyKey }: Commit) {
  return {
    ...(checks === undefined || checks.length === 0 ? {} : { checks }),
    ...(ifRevision === undefined ? {} : { if_revision: ifRevision }),
    writes: writes.map((write) => {
      if ('delete' in write) return { key: write.key, delete: true };
      if (write.value === undefined || write.value === null)
        throw new TypeError(
          `The value for ${JSON.stringify(write.key)} is ${write.value}. ` +
            'Values are JSON other than null; use delete() to remove a key.',
        );
      return { key: write.key, value: write.value };
    }),
    idempotency_key: idempotencyKey ?? crypto.randomUUID(),
  };
}

const conflict = (message: string) =>
  new ConflictError(message, { status: 409, code: 'conflict', retryable: false });

/** Keys ordered as the server orders them: by UTF-8 bytes, which is Unicode code point order. */
export function compareKeys(a: string, b: string): number {
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index++) {
    let x = a.charCodeAt(index);
    let y = b.charCodeAt(index);
    if (x === y) continue;
    // UTF-16 places surrogates (astral characters) before U+E000..U+FFFF; code points do not.
    if (x >= 0xd800) x += x < 0xe000 ? 0x2000 : -0x800;
    if (y >= 0xd800) y += y < 0xe000 ? 0x2000 : -0x800;
    return x - y;
  }
  return a.length - b.length;
}

class BufferedTransaction implements Transaction {
  readonly #db: Database;
  readonly #versions = new Map<string, number>();
  readonly #ranges: Check[] = [];
  readonly #writes = new Map<string, Write>();
  #open = true;

  constructor(db: Database) {
    this.#db = db;
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return (await this.getMany<T>([key]))[0];
  }

  async getMany<T = unknown>(keys: readonly string[]): Promise<(T | undefined)[]> {
    this.#assertOpen();
    const unwritten = keys.filter((key) => !this.#writes.has(key));
    const stored = new Map<string, Entry<T> | undefined>();
    if (unwritten.length > 0) {
      const { values } = await this.#db.read<T>({ keys: unwritten }, { consistency: 'session' });
      this.#assertOpen();
      unwritten.forEach((key, index) => {
        const version = values[index]?.version ?? 0;
        const seen = this.#versions.get(key);
        if (seen !== undefined && seen !== version)
          throw conflict(`key ${JSON.stringify(key)} changed while the transaction ran`);
        this.#versions.set(key, version);
        stored.set(key, values[index]);
      });
    }
    return keys.map((key) => {
      const write = this.#writes.get(key);
      if (write === undefined) return stored.get(key)?.value;
      return 'delete' in write ? undefined : (write.value as T);
    });
  }

  async list<T = unknown>(options: Scan = {}): Promise<{ items: Item<T>[]; cursor?: string }> {
    this.#assertOpen();
    const { prefix = '', start, end, reverse = false, limit = 100, cursor } = options;
    const page = await this.#db.list<T>({ ...options, consistency: 'session' });
    this.#assertOpen();
    // The check covers what this page saw: from the cursor through its last key.
    const seen: Range = { prefix, start, end };
    if (cursor !== undefined) seen[reverse ? 'end' : 'start'] = reverse ? cursor : cursor + '\0';
    if (page.cursor !== undefined)
      seen[reverse ? 'start' : 'end'] = reverse ? page.cursor : page.cursor + '\0';
    this.#ranges.push({ range: seen, revision: page.revision });

    const inSeen = (key: string) =>
      key.startsWith(prefix) &&
      (seen.start === undefined || compareKeys(key, seen.start) >= 0) &&
      (seen.end === undefined || compareKeys(key, seen.end) < 0);
    const merged = new Map<string, T>(page.items.map((item) => [item.key, item.value]));
    for (const [key, write] of this.#writes) {
      if (!inSeen(key)) continue;
      if ('delete' in write) merged.delete(key);
      else merged.set(key, write.value as T);
    }
    const items = [...merged]
      .map(([key, value]) => ({ key, value }))
      .sort((a, b) => (reverse ? compareKeys(b.key, a.key) : compareKeys(a.key, b.key)));
    if (items.length > limit) {
      items.length = limit;
      return { items, cursor: items[limit - 1]!.key };
    }
    return page.cursor === undefined ? { items } : { items, cursor: page.cursor };
  }

  set(key: string, value: unknown): void {
    this.#assertOpen();
    if (value === undefined || value === null)
      throw new TypeError(
        `The value for ${JSON.stringify(key)} is ${value}; use delete() instead.`,
      );
    this.#writes.set(key, { key, value });
  }

  delete(key: string): void {
    this.#assertOpen();
    this.#writes.set(key, { key, delete: true });
  }

  /** Close the transaction and return what to commit, if it wrote anything. */
  finish(): Commit | undefined {
    this.#open = false;
    if (this.#writes.size === 0) return undefined;
    return {
      writes: [...this.#writes.values()],
      checks: [
        ...Array.from(this.#versions, ([key, version]) => ({ key, version })),
        ...this.#ranges,
      ],
    };
  }

  #assertOpen(): void {
    if (!this.#open)
      throw new Error('This transaction has finished. Await every read inside the callback.');
  }
}
