import type { Commit } from './database.ts';

export interface ErrorDetails {
  status: number;
  code: string;
  retryable: boolean;
  retryAfterMs?: number;
  revision?: number;
  cause?: unknown;
}

/** A request that failed. Subclasses name the cases an application usually handles. */
export class DatabaseError extends Error {
  /** HTTP status, or 0 when no response arrived. */
  readonly status: number;
  /** The server's error code, such as `conflict`; `network` when no response arrived. */
  readonly code: string;
  /** Whether repeating the identical request can succeed. The client has already retried. */
  readonly retryable: boolean;
  /** The delay the server asked for with `Retry-After`. */
  readonly retryAfterMs: number | undefined;
  /** True when a commit was sent and may or may not have been applied. */
  outcomeUnknown = false;
  /**
   * With `outcomeUnknown`, the commit that was sent, including its idempotency key. Passing it
   * to `Database.commit()` again returns its result if it was applied, and applies it if not.
   */
  commit?: Commit;

  constructor(message: string, details: ErrorDetails) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.name = new.target.name;
    this.status = details.status;
    this.code = details.code;
    this.retryable = details.retryable;
    this.retryAfterMs = details.retryAfterMs;
  }
}

/** 400: the request is malformed or exceeds a limit. */
export class InvalidError extends DatabaseError {}
/** 401: the token is missing, forged, expired or revoked. */
export class UnauthorizedError extends DatabaseError {}
/** 403: the token does not cover this database or action. */
export class ForbiddenError extends DatabaseError {}
/** 404 or 410: no such database, or it was deleted. */
export class NotFoundError extends DatabaseError {}
/** 409: a check failed and nothing was written. Read again and recompute. */
export class ConflictError extends DatabaseError {
  /** A revision that includes whatever caused the conflict. Reading at it or later sees it. */
  readonly revision: number | undefined;

  constructor(message: string, details: ErrorDetails) {
    super(message, details);
    this.revision = details.revision;
  }
}
/** 429: the database's request budget is spent. */
export class RateLimitError extends DatabaseError {}
/** 503: the server could not complete the request now. */
export class UnavailableError extends DatabaseError {}
/** 507: the database is at its storage quota. Deletes still succeed. */
export class QuotaError extends DatabaseError {}
/** No response arrived: the connection failed or the request timed out. */
export class NetworkError extends DatabaseError {}

const CODES: Record<string, [status: number, type: typeof DatabaseError]> = {
  invalid: [400, InvalidError],
  unauthorized: [401, UnauthorizedError],
  forbidden: [403, ForbiddenError],
  not_found: [404, NotFoundError],
  conflict: [409, ConflictError],
  exists: [409, DatabaseError],
  deleted: [410, NotFoundError],
  rate_limited: [429, RateLimitError],
  busy: [503, UnavailableError],
  ownership_moved: [503, UnavailableError],
  storage_unavailable: [503, UnavailableError],
  too_large: [413, InvalidError],
  quota_exceeded: [507, QuotaError],
};

/** The error for `{"code", "message", "retryable"}` as the server sends it. */
export function errorFromBody(
  body: { code?: unknown; message?: unknown; retryable?: unknown; revision?: unknown },
  status?: number,
  retryAfterMs?: number,
): DatabaseError {
  const code = typeof body.code === 'string' ? body.code : `http_${status ?? 0}`;
  const [knownStatus, type] = CODES[code] ?? [status ?? 0, DatabaseError];
  return new type(typeof body.message === 'string' ? body.message : code, {
    status: status ?? knownStatus,
    code,
    retryable: typeof body.retryable === 'boolean' ? body.retryable : false,
    retryAfterMs,
    revision: typeof body.revision === 'number' ? body.revision : undefined,
  });
}

/** The error for a response that is not the server's JSON error, such as one from a proxy. */
export function errorFromStatus(
  status: number,
  text: string,
  retryAfterMs?: number,
): DatabaseError {
  const type =
    status === 429
      ? RateLimitError
      : status >= 502 && status <= 504
        ? UnavailableError
        : DatabaseError;
  return new type(text.trim().slice(0, 200) || `HTTP ${status}`, {
    status,
    code: `http_${status}`,
    retryable: status === 429 || (status >= 502 && status <= 504),
    retryAfterMs,
  });
}
