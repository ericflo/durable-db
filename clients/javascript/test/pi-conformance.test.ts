import { describe, expect, it } from 'vitest';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import {
  createStorageConformance,
  registerStorageConformance,
} from '@earendil-works/pi-durable/testing';
import { openPiStorage } from '../src/pi/index.ts';
import { createDatabase } from './fixture.ts';

// pi-durable's complete storage conformance suite. Each case gets an empty storage under its
// own prefix of one database.
const { db } = await createDatabase();
let opened = 0;

registerStorageConformance(
  { describe, expect, it },
  'pi-durable 1.5.2 storage conformance',
  async (use) => {
    const storage = await openPiStorage(db, { prefix: `case-${++opened}/` });
    try {
      await use(storage);
    } finally {
      await storage.close(BACKGROUND_CONTEXT);
    }
  },
);

it('registered all 32 upstream cases', () => {
  const cases = createStorageConformance({ assertions: {} as never, withStorage: async () => {} });
  expect(cases).toHaveLength(32);
});
