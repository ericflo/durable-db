import type { Storage } from '@earendil-works/pi-durable';
import type { DurableDb } from './client.mjs';

export interface IndexedStorageOptions {
  prefix?: string;
  /** Explicit host replacement; never use automatic takeover on browser reconnect. */
  takeover?: boolean;
  maxReadBytes?: number;
  maxRequests?: number;
  maxScanned?: number;
  maxAncestry?: number;
  maxDeltas?: number;
}
/** Opens one fenced Pi Session over a database prefix. API pods remain interchangeable. */
export function openIndexedPiStorage(
  client: Pick<DurableDb, 'read' | 'commit'>,
  options?: IndexedStorageOptions,
): Promise<Storage>;
