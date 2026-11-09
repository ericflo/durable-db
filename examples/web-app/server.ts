/** Local demonstration API. Add application authentication before public hosting. */
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { DurableDb, DbError } from '../../clients/javascript/client.mjs';
import { TaskStore, OperationPending } from './storage.ts';

const port = Number(process.env.PORT ?? 3000);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT');
const tokenFile = process.env.DURABLE_DB_TOKEN_FILE;
const database = process.env.DURABLE_DB_DATABASE;
if (!tokenFile || !database) throw new Error('Set DURABLE_DB_TOKEN_FILE and DURABLE_DB_DATABASE');
const store = new TaskStore(
  new DurableDb({
    url: process.env.DURABLE_DB_URL ?? 'http://127.0.0.1:7654',
    database,
    token: readFileSync(tokenFile, 'utf8').trim(),
  }),
);

function string(value: unknown): string {
  if (typeof value !== 'string') throw new Error('Expected a string');
  return value;
}
function revision(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error('Expected a nonnegative integer revision');
  }
  return value;
}

createServer(async (request, response) => {
  response.setHeader('content-type', 'application/json');
  response.setHeader('cache-control', 'no-store');
  let readingInput = false;
  try {
    const host = request.headers.host;
    if (host !== `127.0.0.1:${port}` && host !== `localhost:${port}`) {
      response.writeHead(403).end('{"error":"unexpected_host"}');
      return;
    }
    if (request.headers.origin && request.headers.origin !== `http://${host}`) {
      response.writeHead(403).end('{"error":"unexpected_origin"}');
      return;
    }
    const url = new URL(request.url ?? '/', `http://${host}`);
    if (request.method === 'GET' && url.pathname === '/settings') {
      response.end(JSON.stringify(await store.settings()));
      return;
    }
    if (request.method === 'GET' && url.pathname === '/tasks') {
      const after = url.searchParams.get('after');
      const cursor =
        after === null
          ? undefined
          : {
              after,
              revision: revision(Number(url.searchParams.get('revision') ?? NaN)),
            };
      response.end(JSON.stringify(await store.listOpen(cursor)));
      return;
    }
    if (
      !['POST', 'PUT'].includes(request.method ?? '') ||
      request.headers['content-type']?.split(';')[0] !== 'application/json'
    ) {
      response.writeHead(405).end('{"error":"expected_json_write"}');
      return;
    }
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of request) {
      size += chunk.length;
      if (size > 8192) {
        response.writeHead(413).end('{"error":"body_too_large"}');
        return;
      }
      chunks.push(chunk);
    }
    readingInput = true;
    const input: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (typeof input !== 'object' || input === null) throw new Error('Expected a JSON object');
    readingInput = false;
    const body = input as Record<string, unknown>;
    const requestId = string(body.requestId);
    if (request.method === 'POST' && url.pathname === '/tasks') {
      response.end(JSON.stringify(await store.createTask(requestId, string(body.title))));
      return;
    }
    const complete = /^\/tasks\/([a-f0-9-]+)\/complete$/.exec(url.pathname);
    if (request.method === 'POST' && complete) {
      response.end(JSON.stringify(await store.completeTask(requestId, complete[1])));
      return;
    }
    if (request.method === 'PUT' && url.pathname === '/settings') {
      const theme = body.theme;
      if (theme !== 'light' && theme !== 'dark') throw new Error('Invalid theme');
      const intent = store.prepareSettings({ theme }, revision(body.revision), requestId);
      response.end(JSON.stringify(await store.saveSettings(intent)));
      return;
    }
    response.writeHead(404).end('{"error":"route_not_found"}');
  } catch (error) {
    // Do not expose credentials, stored values, or provider diagnostics.
    const uncertain =
      error instanceof OperationPending ||
      error instanceof TypeError ||
      error instanceof DOMException ||
      (error instanceof SyntaxError && !readingInput);
    const status = error instanceof DbError ? error.status : uncertain ? 503 : 400;
    response.writeHead(status).end(
      JSON.stringify({
        error:
          status === 409
            ? 'reload_snapshot'
            : status === 503
              ? 'retry_same_intent'
              : 'request_failed',
      }),
    );
  }
}).listen(port, '127.0.0.1', () => console.log(`Task API: http://127.0.0.1:${port}`));
