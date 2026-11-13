import http from 'node:http';
import https from 'node:https';

export class DbError extends Error {
  constructor(status, body) {
    super(body.message ?? `HTTP ${status}`);
    this.status = status;
    this.code = body.error;
  }
}

// A working Service connection is useful for this database, not every database.
// Bound both cached agents and outstanding work; never evict an active request.
const pools = new Map();
let active = 0;
const maxBody = 1024 * 1024;
const busy = () =>
  new DbError(429, { error: 'busy', message: 'client request capacity exhausted' });

export async function requestJson(address, database, { method, headers, body, signal }) {
  const url = new URL(address);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.hash)
    throw new TypeError('Use a final HTTP(S) origin without credentials or a fragment');
  if (typeof database !== 'string' || database.length > 1024)
    throw new TypeError('Invalid database transport scope');
  if (body !== undefined && Buffer.byteLength(body) > maxBody)
    throw new RangeError('Request exceeds 1 MiB');
  signal?.throwIfAborted();
  const id = JSON.stringify([url.origin, database]);
  let pool = pools.get(id);
  if (active >= 128 || (pool?.active ?? 0) >= 16) throw busy();
  if (!pool) {
    if (pools.size === 64) {
      const idle = [...pools].find(([, entry]) => entry.active === 0);
      if (!idle) throw busy();
      idle[1].agent.destroy();
      pools.delete(idle[0]);
    }
    const client = url.protocol === 'https:' ? https : http;
    pool = {
      client,
      active: 0,
      agent: new client.Agent({
        keepAlive: true,
        maxSockets: 16,
        maxTotalSockets: 16,
        maxFreeSockets: 1,
      }),
    };
  }
  pools.delete(id);
  pools.set(id, pool);
  pool.active++;
  active++;
  try {
    return await new Promise((resolve, reject) => {
      // node:http sends once. It neither follows redirects nor retries a reset.
      const request = pool.client.request(
        url,
        { method, headers, signal, agent: pool.agent },
        async (response) => {
          try {
            if (response.statusCode >= 300 && response.statusCode < 400)
              throw new TypeError('Database redirects are refused');
            const chunks = [];
            let bytes = 0;
            for await (const chunk of response) {
              bytes += chunk.length;
              if (bytes > maxBody) throw new TypeError('Response exceeds 1 MiB');
              chunks.push(chunk);
            }
            const result = JSON.parse(Buffer.concat(chunks).toString('utf8'));
            if (response.statusCode < 200 || response.statusCode >= 300)
              throw new DbError(response.statusCode, result);
            resolve(result);
          } catch (error) {
            if (!response.readableEnded) response.destroy();
            reject(
              error instanceof DbError
                ? error
                : new TypeError('Database response failed', { cause: error }),
            );
          }
        },
      );
      request.on('error', (cause) => reject(new TypeError('Database request failed', { cause })));
      request.on('upgrade', (_, socket) => {
        socket.destroy();
        reject(new TypeError('Database protocol upgrades are refused'));
      });
      request.end(body);
    });
  } finally {
    pool.active--;
    active--;
  }
}
