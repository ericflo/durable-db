import { StorageRequestError } from '@earendil-works/pi-durable';

/** A scan of keys relative to the storage's prefix. */
export interface ScanSpec {
  prefix: string;
  start?: string;
  end?: string;
  after?: string;
  reverse?: boolean;
  limit: number;
}

export interface ScanPage {
  items: { key: string; value: unknown }[];
  /** More keys remain after the last item. */
  more: boolean;
}

/** One round trip: every key and scan read at one revision. */
export type Reader = (
  keys: readonly string[],
  scans: readonly ScanSpec[],
) => Promise<{ values: unknown[]; scans: ScanPage[] }>;

export const json = (value: unknown): string => JSON.stringify(value);

export function integer(value: unknown, name = 'ID', minimum = 1): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum)
    throw new StorageRequestError(`Invalid ${name}`);
  return value as number;
}

/** Fixed-width decimal, so that key order is numeric order. */
export const numberKey = (value: number): string =>
  String(integer(value, 'ordered number', 0)).padStart(16, '0');

const hashes = new Map<string, string>();

/**
 * SHA-256 of a value's JSON text, for strings that become part of a key. JSON escapes lone
 * surrogates, so two strings that differ only there still hash differently.
 */
export async function hash(value: unknown): Promise<string> {
  const text = json(value);
  let hex = hashes.get(text);
  if (hex === undefined) {
    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
    hex = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
    if (hashes.size >= 4096) hashes.clear();
    hashes.set(text, hex);
  }
  return hex;
}

const MAX_KEYS_PER_READ = 5000;
const MAX_SCAN_LIMIT = 10_000;

/**
 * What one storage operation sees: keys it has read, and for a commit the writes it has staged.
 * A read returns a staged write before the stored value.
 */
export class View {
  readonly #read: Reader;
  /** Round trips made so far. */
  requests = 0;
  readonly #cache = new Map<string, unknown>();
  readonly #staged = new Map<string, unknown>();
  #batch = new Map<string, unknown>();

  constructor(read: Reader) {
    this.#read = read;
  }

  /** Fetch `keys` that are not known yet and run `scans`, in one round trip when it fits. */
  async load(keys: readonly string[], scans: readonly ScanSpec[] = []): Promise<ScanPage[]> {
    const missing = [...new Set(keys)].filter(
      (key) => !this.#staged.has(key) && !this.#cache.has(key),
    );
    const staged = scans.map((scan) => this.#stagedIn(scan));
    const requests = scans.map((scan, index) => ({
      ...scan,
      limit: Math.min(MAX_SCAN_LIMIT, scan.limit + staged[index]!.length),
    }));
    let pages: ScanPage[] = [];
    for (let offset = 0; offset === 0 || offset < missing.length; offset += MAX_KEYS_PER_READ) {
      const part = missing.slice(offset, offset + MAX_KEYS_PER_READ);
      if (part.length === 0 && requests.length === 0) break;
      this.requests++;
      const result = await this.#read(part, offset === 0 ? requests : []);
      part.forEach((key, index) => this.#cache.set(key, result.values[index]));
      if (offset === 0) pages = result.scans;
    }
    return pages.map((page, index) => {
      for (const item of page.items) this.#cache.set(item.key, item.value);
      return merge(scans[index]!, page, staged[index]!);
    });
  }

  /** The values of `keys`; `undefined` for an absent key. */
  async getMany(keys: readonly string[]): Promise<unknown[]> {
    await this.load(keys);
    return keys.map((key) =>
      this.#staged.has(key) ? this.#staged.get(key) : this.#cache.get(key),
    );
  }

  async get(key: string): Promise<unknown> {
    return (await this.getMany([key]))[0];
  }

  async scan(scan: ScanSpec): Promise<ScanPage> {
    return (await this.load([], [scan]))[0]!;
  }

  /** Every item of a range, however many pages it takes. */
  async scanAll(scan: Omit<ScanSpec, 'limit' | 'after'>): Promise<ScanPage['items']> {
    const items: ScanPage['items'] = [];
    for (let after: string | undefined; ; after = items.at(-1)!.key) {
      const page = await this.scan({ ...scan, after, limit: MAX_SCAN_LIMIT });
      items.push(...page.items);
      if (!page.more) return items;
      if (page.items.length === 0) throw new Error('A scan made no progress');
    }
  }

  /** Record that `key` is known not to exist, so that reading it costs no round trip. */
  assumeAbsent(key: string): void {
    if (!this.#cache.has(key)) this.#cache.set(key, undefined);
  }

  /** Stage a write in the current batch; `undefined` deletes the key. */
  set(key: string, value: unknown): void {
    this.#staged.set(key, value);
    this.#batch.set(key, value);
  }

  /** The writes staged since the last call, as one transaction's writes. */
  takeBatch(): { key: string; value: unknown }[] {
    const writes = Array.from(this.#batch, ([key, value]) => ({ key, value }));
    this.#batch = new Map();
    return writes;
  }

  #stagedIn({ prefix, start, end, after, reverse }: ScanSpec): [string, unknown][] {
    const found: [string, unknown][] = [];
    for (const entry of this.#staged) {
      const key = entry[0];
      if (
        key.startsWith(prefix) &&
        (start === undefined || key >= start) &&
        (end === undefined || key < end) &&
        (after === undefined || (reverse ? key < after : key > after))
      )
        found.push(entry);
    }
    return found;
  }
}

/** A stored page with the staged writes of its range applied. Keys here are ASCII. */
function merge(scan: ScanSpec, page: ScanPage, staged: [string, unknown][]): ScanPage {
  if (staged.length === 0 && page.items.length <= scan.limit) return page;
  const reverse = scan.reverse === true;
  const values = new Map(page.items.map((item) => [item.key, item.value]));
  // When the stored page was cut short, staged keys past its last key belong to a later page.
  const last = page.more ? page.items.at(-1)?.key : undefined;
  for (const [key, value] of staged) {
    if (last !== undefined && (reverse ? key < last : key > last)) continue;
    if (value === undefined) values.delete(key);
    else values.set(key, value);
  }
  const ordered = [...values].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  if (reverse) ordered.reverse();
  return {
    items: ordered.slice(0, scan.limit).map(([key, value]) => ({ key, value })),
    more: page.more || ordered.length > scan.limit,
  };
}
