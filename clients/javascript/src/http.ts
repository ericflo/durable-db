import { DatabaseError, NetworkError, errorFromBody, errorFromStatus } from './errors.ts';

export interface ConnectionOptions {
  /** The server's address, such as `http://127.0.0.1:7654`. */
  url: string;
  /** A bearer token: a database token for `Database`, an administrator token for `Admin`. */
  token: string;
  /** A `fetch` implementation. Defaults to the global one. */
  fetch?: typeof fetch;
  /** Milliseconds before one HTTP attempt is abandoned. Default 30,000. */
  timeoutMs?: number;
  /** Times a request is repeated after a network error, `429` or `503`. Default 5. */
  maxRetries?: number;
}

export interface Connection {
  url: string;
  token: string;
  fetch: typeof fetch;
  timeoutMs: number;
  maxRetries: number;
}

export function connect(options: ConnectionOptions): Connection {
  if (typeof options.url !== 'string' || !/^https?:\/\//.test(options.url))
    throw new TypeError('url must be an http(s) address such as http://127.0.0.1:7654');
  if (typeof options.token !== 'string' || options.token === '')
    throw new TypeError('token must be a non-empty string');
  return {
    url: options.url.replace(/\/+$/, ''),
    token: options.token,
    fetch: options.fetch ?? ((input, init) => fetch(input, init)),
    timeoutMs: options.timeoutMs ?? 30_000,
    maxRetries: options.maxRetries ?? 5,
  };
}

/** A longer `Retry-After` than this is returned to the caller instead of waited for. */
const MAX_WAIT_MS = 10_000;

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** Exponential backoff with jitter: between half and all of `base * 2^attempt`, at most `cap`. */
export const backoff = (attempt: number, base: number, cap: number): number =>
  Math.min(cap, base * 2 ** attempt) * (0.5 + Math.random() / 2);

/**
 * One JSON request. The identical bytes are sent again after a network error or a retryable
 * status, so a commit must carry an idempotency key.
 */
export async function request<T>(
  connection: Connection,
  method: string,
  path: string,
  body?: unknown,
  commit = false,
): Promise<T> {
  const payload = body === undefined || typeof body === 'string' ? body : JSON.stringify(body);
  let outcomeUnknown = false;
  for (let attempt = 0; ; attempt++) {
    let error: DatabaseError;
    try {
      const response = await connection.fetch(connection.url + path, {
        method,
        headers: {
          authorization: `Bearer ${connection.token}`,
          ...(payload === undefined ? {} : { 'content-type': 'application/json' }),
        },
        body: payload,
        // A redirect must not carry the token or the write to another origin.
        redirect: 'error',
        signal: AbortSignal.timeout(connection.timeoutMs),
      });
      const text = await response.text();
      if (response.ok) return JSON.parse(text) as T;
      error = failure(response, text);
    } catch (cause) {
      const timedOut = cause instanceof Error && cause.name === 'TimeoutError';
      error = new NetworkError(timedOut ? 'the request timed out' : 'the request failed', {
        status: 0,
        code: 'network',
        retryable: true,
        cause,
      });
    }
    if (commit) {
      // Without a response, or with a server failure, the commit may have been applied. Any
      // other answer except 429 is definite: a repeat of an applied commit returns its result.
      const definite =
        error.status !== 0 && (error.status < 500 || error.code === 'quota_exceeded');
      if (!definite) outcomeUnknown = true;
      else if (error.status !== 429) outcomeUnknown = false;
    }
    const wait = error.retryAfterMs ?? backoff(attempt, 50, 2_000);
    if (!error.retryable || attempt >= connection.maxRetries || wait > MAX_WAIT_MS) {
      error.outcomeUnknown = outcomeUnknown;
      throw error;
    }
    await sleep(wait);
  }
}

function failure(response: Response, text: string): DatabaseError {
  const seconds = Number(response.headers.get('retry-after') ?? NaN);
  const retryAfterMs = Number.isFinite(seconds) && seconds >= 0 ? seconds * 1000 : undefined;
  try {
    const body = JSON.parse(text);
    if (typeof body?.error?.code === 'string')
      return errorFromBody(body.error, response.status, retryAfterMs);
  } catch {
    // Not the server's JSON: fall through to the status.
  }
  return errorFromStatus(response.status, text, retryAfterMs);
}
