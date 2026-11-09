import { DurableDb, DbError } from '../client.mjs';
import type { CommitRequest } from '../client.mjs';
import { fromJson, toJson, key } from './json.js';
import { read } from './requests.js';

/** Read and stage JSON values within one database snapshot. */
export interface Transaction {
  get<T>(key: string): Promise<T | undefined>;
  set<T>(key: string, value: T): void;
  delete(key: string): void;
}

/** One optimistic transaction. Callbacks may rerun; keep external side effects outside them. */
export class TransactionScope implements Transaction {
  #client: DurableDb;
  #revision: number | undefined;
  #reads = new Set<string>();
  #pending = new Map<string, Promise<void>>();
  #values = new Map<string, string | null>();
  #writes = new Map<string, string | null>();
  #open = true;
  constructor(client: DurableDb) {
    this.#client = client;
  }
  #assertOpen(): void {
    if (!this.#open) throw new Error('This transaction has finished');
  }
  async get<T>(name: string): Promise<T | undefined> {
    this.#assertOpen();
    key(name);
    if (this.#writes.has(name)) {
      const value = this.#writes.get(name)!;
      return value === null ? undefined : fromJson<T>(value);
    }
    if (!this.#values.has(name)) {
      let pending = this.#pending.get(name);
      if (!pending) {
        if (!this.#reads.has(name) && this.#reads.size >= 128)
          throw new RangeError('A transaction may read at most 128 distinct keys');
        this.#reads.add(name);
        pending = this.#load(name);
        this.#pending.set(name, pending);
      }
      try {
        await pending;
      } finally {
        this.#pending.delete(name);
      }
    }
    const value = this.#values.get(name)!;
    return value === null ? undefined : fromJson<T>(value);
  }
  async #load(name: string): Promise<void> {
    const snapshot = await read(this.#client, {
      keys: [name],
      expected_revision: this.#revision,
    });
    this.#assertOpen();
    // Concurrent callback reads must never combine different revisions.
    if (this.#revision !== undefined && this.#revision !== snapshot.revision)
      throw new DbError(409, { error: 'conflict' });
    this.#revision = snapshot.revision;
    this.#values.set(name, snapshot.values[0]);
  }
  set<T>(name: string, value: T): void {
    this.#assertOpen();
    key(name);
    if (!this.#writes.has(name) && this.#writes.size >= 128)
      throw new RangeError('A transaction may write at most 128 keys');
    this.#writes.set(name, toJson(value));
  }
  delete(name: string): void {
    this.#assertOpen();
    key(name);
    if (!this.#writes.has(name) && this.#writes.size >= 128)
      throw new RangeError('A transaction may write at most 128 keys');
    this.#writes.set(name, null);
  }
  /** @internal */
  async finish(): Promise<CommitRequest | undefined> {
    this.#assertOpen();
    this.#open = false;
    if (this.#writes.size === 0) return undefined;
    const revision = this.#revision ?? (await read(this.#client, {})).revision;
    const request = {
      request_id: crypto.randomUUID(),
      expected_revision: revision,
      writes: [...this.#writes].map(([key, value]) => ({ key, value })),
    };
    if (new TextEncoder().encode(JSON.stringify(request)).length > 1024 * 1024)
      throw new RangeError('Transaction request exceeds 1 MiB');
    return request;
  }
  /** @internal */
  close(): void {
    this.#open = false;
  }
}
