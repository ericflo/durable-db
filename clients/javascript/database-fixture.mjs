import { readFileSync } from 'node:fs';
import { DurableDb } from './client.mjs';
import { retry } from './indexed/kv.mjs';

export const urls = (process.env.DURABLE_DB_TEST_URLS ?? process.env.DURABLE_DB_TEST_URL ?? '')
  .split(',')
  .filter(Boolean);
export const url = urls[0];
const token = process.env.DURABLE_DB_TEST_TOKEN_FILE
  ? readFileSync(process.env.DURABLE_DB_TEST_TOKEN_FILE, 'utf8').trim()
  : '';

export async function createDatabase(prefix) {
  const database = `${prefix}-${crypto.randomUUID()}`;
  const admin = new DurableDb({ url, database, token });
  // Long contract cases exercise receipt eviction and document tails. Native
  // CI explicitly funds that work; dedicated budget tests keep the defaults.
  if (process.env.DURABLE_DB_TEST_ADMISSION_BYTES) {
    const grant = {
      request_id: crypto.randomUUID(),
      expected_configuration_revision: 0,
      limits: { bytes: Number(process.env.DURABLE_DB_TEST_ADMISSION_BYTES), attempts: 16384 },
    };
    await retry(() => admin.request('/upload-budget', grant));
  }
  const creation = { request_id: crypto.randomUUID() };
  await retry(() => admin.createProtected(creation));
  const issued = await retry(() => admin.issueToken({ write: true }));
  const client = new DurableDb({ url, database, token: issued.token });
  return { admin, client, config: { urls, database, token: issued.token } };
}
