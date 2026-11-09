import { DurableDb, DbError } from '../client.mjs';
import type { ReadRequest, ReadResult } from '../client.mjs';

export const attempts = 5;
export const sleep = (attempt: number) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.min(1000, 100 * 2 ** attempt) * (1 + Math.random() / 2)),
  );
export const transient = (error: unknown) =>
  error instanceof DbError
    ? [429, 500, 502, 503, 504].includes(error.status)
    : error instanceof TypeError || error instanceof DOMException || error instanceof SyntaxError;
export async function read(client: DurableDb, request: ReadRequest): Promise<ReadResult> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await client.read(request);
    } catch (error) {
      if (!transient(error) || attempt + 1 === attempts) throw error;
      await sleep(attempt);
    }
  }
}
