/** A throwaway local server for tests. Node.js only. */
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Admin, type DatabaseLimits } from './admin.ts';
import { Database, type DatabaseOptions } from './database.ts';

export interface TestServerOptions {
  /** The `durable-db` executable. Default: `$DURABLE_DB_BINARY`, then `durable-db` on the PATH. */
  binary?: string;
}

export interface TestServer {
  url: string;
  adminToken: string;
  admin: Admin;
  /** Create a database and return a client holding a write token for it. */
  createDatabase(
    name?: string,
    options?: Partial<DatabaseOptions> & { limits?: DatabaseLimits },
  ): Promise<Database>;
  /** Stop the server and delete its data. */
  stop(): Promise<void>;
}

/** Start `durable-db serve` on a free local port with a temporary data directory. */
export async function startServer(options: TestServerOptions = {}): Promise<TestServer> {
  const binary = options.binary ?? process.env.DURABLE_DB_BINARY ?? 'durable-db';
  const directory = await mkdtemp(join(tmpdir(), 'durable-db-'));
  const data = join(directory, 'data');
  const url = `http://127.0.0.1:${await freePort()}`;
  const child = spawn(
    binary,
    ['serve', '--data', data, '--listen', new URL(url).host, '--no-fsync'],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  let log = '';
  for (const stream of [child.stdout, child.stderr])
    stream.on('data', (chunk) => (log = (log + chunk).slice(-4096)));
  const failed = new Promise<never>((_, reject) => {
    child.once('error', reject);
    child.once('exit', (code) => reject(new Error(`durable-db exited with ${code}\n${log}`)));
  });
  failed.catch(() => {});
  const kill = () => child.kill();
  process.once('exit', kill);
  const stop = async () => {
    process.off('exit', kill);
    // A child that never started has no pid and never exits.
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      child.kill();
      await once(child, 'exit');
    }
    await rm(directory, { recursive: true, force: true });
  };
  try {
    await Promise.race([ready(url), failed]);
    const adminToken = execFileSync(
      binary,
      ['token', '--admin', '--key-file', join(data, 'admin.key')],
      { encoding: 'utf8' },
    ).trim();
    const admin = new Admin({ url, token: adminToken });
    return {
      url,
      adminToken,
      admin,
      async createDatabase(name = `test-${crypto.randomUUID()}`, { limits, ...rest } = {}) {
        await admin.createDatabase(name, limits);
        const token = await admin.createToken(name);
        return new Database({ ...rest, url, database: name, token });
      },
      stop,
    };
  } catch (error) {
    await stop();
    throw error;
  }
}

async function freePort(): Promise<number> {
  const server = createServer().listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as { port: number };
  server.close();
  await once(server, 'close');
  return port;
}

async function ready(url: string): Promise<void> {
  for (let attempt = 0; attempt < 500; attempt++) {
    try {
      if ((await fetch(`${url}/readyz`)).ok) return;
    } catch {
      // Not listening yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error('durable-db did not become ready within 10 seconds');
}
