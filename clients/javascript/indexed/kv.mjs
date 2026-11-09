import { StorageRequestError } from '@earendil-works/pi-durable';
import { encode, decode, DbError } from '../client.mjs';

export const utf8 = new TextEncoder();
const text = new TextDecoder('utf-8', { fatal: true });
export const copy = (value) => (value === undefined ? undefined : structuredClone(value));
export const json = (value) => JSON.stringify(value);
export const pack = (value) => encode(utf8.encode(json(value)));
export const unpack = (value) =>
  value === null ? undefined : JSON.parse(text.decode(decode(value)));
export function integer(value, name = 'ID', minimum = 1) {
  if (!Number.isSafeInteger(value) || value < minimum)
    throw new StorageRequestError(`Invalid ${name}`);
  return value;
}
export const numberKey = (value) => String(integer(value, 'ordered number', 0)).padStart(16, '0');
export async function hash(value) {
  const bytes = typeof value === 'string' ? utf8.encode(value) : value;
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
    .map((v) => v.toString(16).padStart(2, '0'))
    .join('');
}
export const pause = (attempt) =>
  new Promise((resolve) => setTimeout(resolve, Math.min(1000, 50 * 2 ** attempt)));
export function retryable(error) {
  return !(error instanceof DbError) || [429, 503].includes(error.status);
}
export async function retry(call) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call();
    } catch (error) {
      if (!retryable(error) || attempt >= 8) throw error;
      await pause(attempt);
    }
  }
}

/** One database revision, bounded read work, and a detached write overlay. */
export class Snapshot {
  constructor(client, prefix, revision, bounds) {
    this.client = client;
    this.prefix = prefix;
    this.revision = revision;
    this.bounds = bounds;
    this.cache = new Map();
    this.writes = new Map();
    this.bytes = 0;
    this.requests = 0;
    this.scanned = 0;
  }
  account(value) {
    this.bytes += value === null ? 0 : value.length;
    if (this.bytes > (this.bounds.maxReadBytes * 4) / 3)
      throw new StorageRequestError('Indexed read byte budget exceeded');
  }
  async read(body) {
    if (++this.requests > this.bounds.maxRequests)
      throw new StorageRequestError('Indexed read request budget exceeded');
    return retry(() => this.client.read({ ...body, expected_revision: this.revision }));
  }
  async getMany(keys) {
    const missing = [...new Set(keys)].filter(
      (key) => !this.writes.has(key) && !this.cache.has(key),
    );
    for (let offset = 0; offset < missing.length; offset += 8) {
      const batch = missing.slice(offset, offset + 8);
      const result = await this.read({ keys: batch.map((key) => this.prefix + key) });
      result.values.forEach((value, index) => {
        this.account(value);
        this.cache.set(batch[index], value);
      });
      if (this.cache.size > this.bounds.maxScanned)
        throw new StorageRequestError('Indexed point-read budget exceeded');
    }
    return keys.map((key) =>
      unpack(this.writes.has(key) ? this.writes.get(key) : this.cache.get(key)),
    );
  }
  async get(key) {
    return (await this.getMany([key]))[0];
  }
  set(key, value) {
    const encoded = value === undefined ? null : pack(value);
    if (encoded !== null && decode(encoded).length > 65536)
      throw new StorageRequestError('Indexed value exceeds 64 KiB');
    this.writes.set(key, encoded);
    // Reserve one metadata write and two ownership/metadata checks.
    if (this.writes.size > 125)
      throw new StorageRequestError('Indexed commit exceeds transaction operation budget');
  }
  async scan(prefix, { after, start, end, reverse = false, limit = 64 } = {}) {
    const relevant = [...this.writes].filter(
      ([key]) =>
        key.startsWith(prefix) &&
        (start === undefined || key >= start) &&
        (end === undefined || key < end) &&
        (after === undefined || (reverse ? key < after : key > after)),
    );
    const result = await this.read({
      scans: [
        {
          prefix: this.prefix + prefix,
          ...(after === undefined ? {} : { after: this.prefix + after }),
          ...(start === undefined ? {} : { start: this.prefix + start }),
          ...(end === undefined ? {} : { end: this.prefix + end }),
          reverse,
          limit: Math.min(1000, limit + relevant.length),
        },
      ],
    });
    const values = new Map();
    for (const pair of result.scans[0].items) {
      const key = pair.key.slice(this.prefix.length);
      this.account(pair.value);
      this.scanned++;
      if (this.scanned > this.bounds.maxScanned)
        throw new StorageRequestError('Indexed scan budget exceeded; narrow the query');
      this.cache.set(key, pair.value);
      values.set(key, pair.value);
      if (this.cache.size > this.bounds.maxScanned)
        throw new StorageRequestError('Indexed key budget exceeded');
    }
    for (const [key, value] of relevant) {
      if (value === null) values.delete(key);
      else values.set(key, value);
    }
    const ordered = [...values].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    if (reverse) ordered.reverse();
    const selected = ordered.slice(0, limit);
    return {
      items: selected.map(([key, value]) => ({ key, value: unpack(value) })),
      more:
        ordered.length > limit ||
        (result.scans[0].next !== null && result.scans[0].next !== undefined),
    };
  }
  /** Large records are chunked within the SAME final database transaction. */
  async row(key) {
    const stored = await this.get(key);
    if (stored === undefined) return undefined;
    if (stored.encoding === 'inline') return copy(stored.value);
    if (
      stored.encoding !== 'chunks' ||
      !Number.isSafeInteger(stored.count) ||
      stored.count < 1 ||
      stored.count > 16 ||
      !Number.isSafeInteger(stored.bytes) ||
      stored.bytes < 1 ||
      stored.bytes > 512 * 1024 ||
      typeof stored.digest !== 'string' ||
      !/^[a-f0-9]{64}$/.test(stored.digest)
    )
      throw new Error('Invalid indexed record envelope');
    const chunkPrefix = `z/${await hash(key)}/`;
    const chunks = await this.getMany(
      Array.from({ length: stored.count }, (_, i) => chunkPrefix + numberKey(i)),
    );
    const data = new Uint8Array(stored.bytes);
    let offset = 0;
    for (const chunk of chunks) {
      if (typeof chunk !== 'string') throw new Error('Missing indexed record chunk');
      const bytes = decode(chunk);
      if (bytes.length > 32768 || offset + bytes.length > data.length)
        throw new Error('Invalid indexed record chunk');
      data.set(bytes, offset);
      offset += bytes.length;
    }
    if (offset !== data.length || (await hash(data)) !== stored.digest)
      throw new Error('Indexed record checksum mismatch');
    return JSON.parse(text.decode(data));
  }
  async setRow(key, value) {
    const old = await this.get(key);
    if (old !== undefined && !['inline', 'chunks'].includes(old.encoding))
      throw new Error('Invalid previous indexed record envelope');
    const data = value === undefined ? undefined : utf8.encode(json(value));
    if (data && data.length > 512 * 1024)
      throw new StorageRequestError('Indexed record exceeds 512 KiB');
    let count = 0;
    if (data === undefined) this.set(key, undefined);
    else if (data.length <= 24576) this.set(key, { encoding: 'inline', value });
    else {
      count = Math.ceil(data.length / 32768);
      const chunkPrefix = `z/${await hash(key)}/`;
      for (let i = 0; i < count; i++)
        this.set(chunkPrefix + numberKey(i), encode(data.subarray(i * 32768, (i + 1) * 32768)));
      this.set(key, { encoding: 'chunks', count, bytes: data.length, digest: await hash(data) });
    }
    if (old?.encoding === 'chunks') {
      if (!Number.isSafeInteger(old.count) || old.count < 1 || old.count > 16)
        throw new Error('Invalid previous indexed record envelope');
      const chunkPrefix = `z/${await hash(key)}/`;
      for (let i = count; i < old.count; i++) this.set(chunkPrefix + numberKey(i), undefined);
    }
  }
  request(metadata, nonce) {
    const writes = [...this.writes].map(([key, value]) => ({ key: this.prefix + key, value }));
    const request = {
      expected_revision: this.revision,
      request_id: nonce,
      checks: [{ key: this.prefix + 'meta', value: metadata }],
      writes,
    };
    if (
      writes.length + request.checks.length > 128 ||
      utf8.encode(json(request)).length > 1024 * 1024
    )
      throw new StorageRequestError('Indexed commit exceeds database transaction budget');
    return request;
  }
}
