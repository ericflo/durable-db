import { inject } from 'vitest';
import { Admin, Database, type DatabaseLimits, type DatabaseOptions } from '../src/index.ts';

export const server = inject('server');
export const admin = new Admin({ url: server.url, token: server.adminToken });

/** A new empty database with a write token. */
export async function createDatabase(limits?: DatabaseLimits) {
  const database = `test-${crypto.randomUUID()}`;
  await admin.createDatabase(database, limits);
  const options = { url: server.url, database, token: await admin.createToken(database) };
  const open = (extra: Partial<DatabaseOptions> = {}) => new Database({ ...options, ...extra });
  return { ...options, db: open(), open };
}

export interface Call {
  path: string;
  body: any;
  status: number;
}

/** A `fetch` that records each request it forwards and can fail chosen ones. */
export function recordingFetch(
  intercept?: (call: Call, send: () => Promise<Response>) => Promise<Response>,
) {
  const calls: Call[] = [];
  const fetcher: typeof fetch = async (input, init) => {
    const call: Call = {
      path: new URL(String(input)).pathname.replace(/^\/v1\/databases\/[^/]+/, ''),
      body: typeof init?.body === 'string' ? JSON.parse(init.body) : undefined,
      status: 0,
    };
    calls.push(call);
    const send = () => fetch(input, init);
    const response = await (intercept ? intercept(call, send) : send());
    call.status = response.status;
    return response;
  };
  return { calls, fetch: fetcher };
}
