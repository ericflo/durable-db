import { expect, it } from 'vitest';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { DurableDb } from './client.mjs';

// A redirect must never convert a mutation to GET or replay it at another path.
// A positive-looking redirected JSON response cannot acknowledge the original.
for (const status of [301, 302, 303, 307, 308])
  it(`refuses HTTP ${status} without forwarding a commit or its capability`, async () => {
    const requests = [];
    const server = createServer((request, response) => {
      request.resume();
      requests.push({
        method: request.method,
        path: request.url,
        authorization: request.headers.authorization,
      });
      if (request.url.endsWith('/commit')) {
        response.writeHead(status, { location: '/redirected' });
        response.end();
      } else {
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ revision: 42 }));
      }
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    try {
      const client = new DurableDb({
        url: `http://127.0.0.1:${server.address().port}`,
        database: 'fixture',
        token: 'synthetic-capability',
      });
      await expect(
        client.commit({ expected_revision: 0, request_id: crypto.randomUUID(), writes: [] }),
      ).rejects.toThrow();
      expect(requests).toEqual([
        {
          method: 'POST',
          path: '/v1/databases/fixture/commit',
          authorization: 'Bearer synthetic-capability',
        },
      ]);
    } finally {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    }
  });
