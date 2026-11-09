import { readFileSync } from 'node:fs';
import { Hono } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import { HTTPException } from 'hono/http-exception';
import { serve } from '@hono/node-server';
import { zValidator } from '@hono/zod-validator';
import { z } from 'zod';
import {
  Database,
  DbError,
  UncertainCommitError,
  RetryLimitError,
  InvalidCursorError,
} from '@durable-db/client';
import { taskStore } from './storage.ts';

const tasks = taskStore(
  new Database({
    url: process.env.DURABLE_DB_URL ?? 'http://127.0.0.1:7654',
    database: process.env.DURABLE_DB_DATABASE ?? 'web-app-example',
    token: readFileSync(
      process.env.DURABLE_DB_TOKEN_FILE ?? '.tools/web-app/application.token',
      'utf8',
    ).trim(),
  }),
);
const app = new Hono();
app.use(bodyLimit({ maxSize: 8192 }));
app.use(async (c, next) => {
  const url = new URL(c.req.url);
  const origin = c.req.header('origin');
  if (
    !['localhost', '127.0.0.1'].includes(url.hostname) ||
    (origin && origin !== url.origin) ||
    c.req.header('sec-fetch-site') === 'cross-site'
  )
    return c.json({ error: 'Unexpected origin' }, 403);
  await next();
});
const taskInput = z.object({ id: z.uuid(), title: z.string().trim().min(1).max(200) });
const settingsInput = z.object({ theme: z.enum(['light', 'dark']) });

app.get('/tasks', async (c) => c.json(await tasks.list(c.req.query('cursor'))));
app.post('/tasks', zValidator('json', taskInput), async (c) => {
  const { id, title } = c.req.valid('json');
  return c.json(await tasks.create(id, title));
});
app.post('/tasks/:id/complete', zValidator('param', z.object({ id: z.uuid() })), async (c) => {
  const result = await tasks.complete(c.req.valid('param').id);
  return result ? c.json(result) : c.json({ error: 'Task not found' }, 404);
});
app.get('/stats', async (c) => c.json({ completed: await tasks.completedCount() }));
app.get('/settings', async (c) => c.json(await tasks.settings()));
app.put('/settings', zValidator('json', settingsInput), async (c) => {
  await tasks.saveSettings(c.req.valid('json'));
  return c.json({ saved: true });
});
app.onError((error, c) => {
  if (error instanceof HTTPException) return error.getResponse();
  if (error instanceof InvalidCursorError) return c.json({ error: error.message }, 400);
  if (error instanceof DbError && error.status === 409)
    return c.json({ error: 'Reload this page and try again' }, 409);
  if (error instanceof UncertainCommitError || error instanceof RetryLimitError)
    return c.json({ error: 'Operation could not be confirmed' }, 503);
  return c.json({ error: 'Storage is temporarily unavailable' }, 503);
});

serve({ fetch: app.fetch, hostname: '127.0.0.1', port: Number(process.env.PORT ?? 3000) });
console.log('Task API listening on localhost');
