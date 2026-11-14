import { fileURLToPath } from 'node:url';
import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { Database, DatabaseError } from '@durable-db/client';
import { taskStore } from './storage.ts';

export function createApp(db: Database) {
  const tasks = taskStore(db);
  const app = new Hono();

  app.get('/tasks', async (c) => c.json(await tasks.list(c.req.query('cursor'))));

  app.post('/tasks', async (c) => {
    const body = await c.req.json().catch(() => undefined);
    const title = typeof body?.title === 'string' ? body.title.trim() : '';
    if (title === '' || title.length > 200)
      return c.json({ error: 'title must be 1 to 200 characters' }, 400);
    return c.json(await tasks.create(title), 201);
  });

  app.post('/tasks/:id/complete', async (c) => {
    const task = await tasks.complete(c.req.param('id'));
    return task ? c.json(task) : c.json({ error: 'no such task' }, 404);
  });

  app.get('/stats', async (c) => c.json(await tasks.stats()));

  app.onError((error, c) => {
    if (!(error instanceof DatabaseError)) throw error;
    console.error(`storage error: ${error.code}`);
    return c.json({ error: 'storage is unavailable' }, 503);
  });
  return app;
}

// Run as a program, not imported by a test.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const token = process.env.DURABLE_DB_TOKEN;
  if (!token) throw new Error('Set DURABLE_DB_TOKEN to a token for the database');
  const db = new Database({
    url: process.env.DURABLE_DB_URL ?? 'http://127.0.0.1:7654',
    database: process.env.DURABLE_DB_DATABASE ?? 'tasks',
    token,
  });
  const port = Number(process.env.PORT ?? 3000);
  serve({ fetch: createApp(db).fetch, hostname: '127.0.0.1', port });
  console.log(`Task board on http://127.0.0.1:${port}`);
}
