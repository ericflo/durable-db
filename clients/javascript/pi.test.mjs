import { describe, expect, it } from 'vitest';
import { registerStorageConformance } from '@earendil-works/pi-durable/testing';
import { createDatabase, url } from './database-fixture.mjs';
import { openIndexedPiStorage } from './pi-indexed.mjs';
import { openReferencePiStorage } from './pi-reference.mjs';

const suite = url ? describe : describe.skip;
suite('pi-durable 1.5.0 against the HTTP database', () => {
  for (const [name, open] of [
    ['reference', openReferencePiStorage],
    ['indexed', openIndexedPiStorage],
  ])
    registerStorageConformance(
      { describe, expect, it },
      `durable-db ${name} adapter`,
      async (use) => {
        const { client } = await createDatabase('pi');
        const storage = await open(client);
        try {
          await use(storage);
        } finally {
          await storage.close();
        }
      },
    );
});
