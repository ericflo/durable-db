/** Provision one local example database and write its scoped credential privately. */
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { resolve, join } from 'node:path';
import { DurableDb } from '@durable-db/client';

const adminFile = process.env.DURABLE_DB_ADMIN_TOKEN_FILE;
if (!adminFile)
  throw new Error('Set DURABLE_DB_ADMIN_TOKEN_FILE to an administrator capability file');
const directory = resolve('.tools/web-app');
await mkdir(directory, { recursive: true, mode: 0o700 });
const database = process.env.DURABLE_DB_DATABASE ?? 'web-app-example';
const path = join(directory, 'creation.json');
const initial = {
  database,
  request_id: randomUUID(),
  limits: { max_bytes: 8 * 1024 * 1024, max_keys: 10000 },
};
try {
  await writeFile(path, JSON.stringify(initial), { flag: 'wx', mode: 0o600 });
} catch (error) {
  if (!(error instanceof Error) || !('code' in error) || error.code !== 'EEXIST') throw error;
}
const saved: typeof initial = JSON.parse(await readFile(path, 'utf8'));
if (saved.database !== database)
  throw new Error('Saved provisioning intent names another database');
const admin = new DurableDb({
  url: process.env.DURABLE_DB_URL ?? 'http://127.0.0.1:7654',
  database,
  token: (await readFile(adminFile, 'utf8')).trim(),
});
await admin.createProtected({ request_id: saved.request_id, limits: saved.limits });
const issued = await admin.issueToken({ write: true, ttl_seconds: 3600 });
const tokenFile = join(directory, 'application.token');
await writeFile(tokenFile, issued.token, { mode: 0o600 });
console.log(`Provisioned ${database}; scoped token saved to ${tokenFile} (expires in one hour).`);
