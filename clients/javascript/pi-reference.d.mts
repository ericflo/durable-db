import type { Storage } from '@earendil-works/pi-durable';
import type { DurableDb } from './client.mjs';

/** Bounded replay adapter retained for compatibility and conformance comparisons. */
export function openReferencePiStorage(
  client: Pick<DurableDb, 'read' | 'commit'>,
  options?: { prefix?: string; maxLogBytes?: number; takeover?: boolean },
): Promise<Storage>;
