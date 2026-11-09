import { encode, decode } from '../client.mjs';

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export function fromJson<T>(value: string): T {
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(decode(value))) as T;
}

export function toJson(value: unknown): string {
  const ancestors = new Set<object>();
  // Copy each property once so accessors cannot change it between validation and encoding.
  function copy(item: unknown, depth: number): Json {
    if (depth > 64) throw new TypeError('JSON nesting exceeds 64 levels');
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return item;
    if (typeof item === 'number' && Number.isFinite(item)) return item;
    if (typeof item !== 'object' || item === null || ancestors.has(item))
      throw new TypeError('Values must be finite, acyclic JSON');
    if (
      !Array.isArray(item) &&
      Object.getPrototypeOf(item) !== Object.prototype &&
      Object.getPrototypeOf(item) !== null
    )
      throw new TypeError('Values must be plain JSON objects');
    ancestors.add(item);
    let result: Json;
    if (Array.isArray(item)) {
      result = [];
      for (let i = 0; i < item.length; i++) result.push(copy(item[i], depth + 1));
    } else {
      const object: { [key: string]: Json } = Object.create(null);
      for (const name of Object.keys(item))
        object[name] = copy((item as Record<string, unknown>)[name], depth + 1);
      result = object;
    }
    ancestors.delete(item);
    return result;
  }
  const serialized = JSON.stringify(copy(value, 0));
  if (new TextEncoder().encode(serialized).length > 65536)
    throw new RangeError('A value exceeds 64 KiB');
  return encode(serialized);
}

export function key(value: string): void {
  if (
    !value ||
    !value.isWellFormed() ||
    value.includes('\0') ||
    new TextEncoder().encode(value).length > 1024
  )
    throw new TypeError('Keys must contain 1–1024 valid UTF-8 bytes without NUL');
}
