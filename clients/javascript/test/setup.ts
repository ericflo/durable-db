import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import type { TestProject } from 'vitest/node';
import { startServer } from '../src/testing.ts';

declare module 'vitest' {
  export interface ProvidedContext {
    server: { url: string; adminToken: string };
  }
}

/**
 * One local server for the whole run: $DURABLE_DB_BINARY, else this repository's release build,
 * else `durable-db` on the PATH.
 */
export default async function setup(project: TestProject) {
  const built = fileURLToPath(new URL('../../../target/release/durable-db', import.meta.url));
  const binary = process.env.DURABLE_DB_BINARY ?? (existsSync(built) ? built : undefined);
  const server = await startServer({ binary });
  project.provide('server', { url: server.url, adminToken: server.adminToken });
  return () => server.stop();
}
