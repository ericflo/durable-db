import { DurableDb, DbError, encode } from '../client.mjs';
import type { ClientOptions, CommitRequest } from '../client.mjs';
import { fromJson, key } from './json.js';
import { read, attempts, sleep, transient } from './requests.js';
import { RetryLimitError, UncertainCommitError, InvalidCursorError } from './errors.js';
import { TransactionScope } from './transaction.js';
import type { Transaction } from './transaction.js';

export interface Item<T> {
  key: string;
  value: T;
}
export interface Page<T> {
  items: Item<T>[];
  cursor: string | null;
}
export interface ListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string | null;
}

/** JSON storage with automatic safe retries and explicit transaction boundaries. */
export class Database {
  #client: DurableDb;
  constructor(options: ClientOptions) {
    this.#client = new DurableDb(options);
  }
  async get<T>(name: string): Promise<T | undefined> {
    key(name);
    const snapshot = await read(this.#client, { keys: [name] });
    return snapshot.values[0] === null ? undefined : fromJson<T>(snapshot.values[0]);
  }
  set<T>(name: string, value: T): Promise<void> {
    return this.transaction((tx) => tx.set(name, value));
  }
  delete(name: string): Promise<void> {
    return this.transaction((tx) => tx.delete(name));
  }
  async list<T>({ prefix = '', limit = 20, cursor }: ListOptions = {}): Promise<Page<T>> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
      throw new RangeError('Page size must be 1–1000');
    let after: string | undefined, revision: number | undefined;
    if (prefix) key(prefix);
    if (cursor) {
      try {
        if (cursor.length > 8192) throw new InvalidCursorError();
        const decoded = fromJson<{ prefix: string; after: string; revision: number }>(cursor);
        if (
          !decoded ||
          decoded.prefix !== prefix ||
          typeof decoded.after !== 'string' ||
          !decoded.after.startsWith(prefix) ||
          !Number.isSafeInteger(decoded.revision) ||
          decoded.revision < 0
        )
          throw new InvalidCursorError();
        key(decoded.after);
        after = decoded.after;
        revision = decoded.revision;
      } catch (cause) {
        throw new InvalidCursorError(cause);
      }
    }
    const snapshot = await read(this.#client, {
      expected_revision: revision,
      scans: [{ prefix, after, limit }],
    });
    const page = snapshot.scans[0];
    return {
      items: page.items.map((item) => ({ key: item.key, value: fromJson<T>(item.value) })),
      cursor:
        page.next === null
          ? null
          : encode(JSON.stringify({ prefix, after: page.next, revision: snapshot.revision })),
    };
  }
  async transaction<T>(operation: (transaction: Transaction) => T | Promise<T>): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt < attempts; attempt++) {
      const transaction = new TransactionScope(this.#client);
      let result: T, request: CommitRequest | undefined;
      try {
        result = await operation(transaction);
        request = await transaction.finish();
      } catch (error) {
        transaction.close();
        if (!(error instanceof DbError) || error.status !== 409) throw error;
        lastError = error;
        await sleep(attempt);
        continue;
      }
      if (!request) return result;
      let uncertain = false;
      for (let retry = 0; retry < attempts; retry++) {
        try {
          await this.#client.commit(request);
          return result;
        } catch (error) {
          lastError = error;
          if (error instanceof DbError && error.status === 409) {
            if (uncertain) throw new UncertainCommitError(request, error);
            break; // Definite conflict before uncertainty: rerun the callback.
          }
          if (!transient(error)) {
            if (uncertain) throw new UncertainCommitError(request, error);
            throw error;
          }
          if (!(error instanceof DbError) || error.status !== 429) uncertain = true;
          if (retry + 1 === attempts) {
            if (uncertain) throw new UncertainCommitError(request, error);
            throw new RetryLimitError(error);
          }
          await sleep(retry);
        }
      }
      await sleep(attempt);
    }
    throw new RetryLimitError(lastError);
  }
}
