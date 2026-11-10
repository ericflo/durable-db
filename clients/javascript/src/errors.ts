import type { CommitRequest } from '../client.mjs';

/** An earlier HTTP attempt may have committed. Keep this request for reconciliation. */
export class UncertainCommitError extends Error {
  readonly request: CommitRequest;
  constructor(request: CommitRequest, cause: unknown) {
    super('The transaction outcome is unresolved. Keep its original request for reconciliation.', {
      cause,
    });
    this.name = 'UncertainCommitError';
    this.request = structuredClone(request);
  }
}
export class RetryLimitError extends Error {
  constructor(cause: unknown) {
    super('The database remained busy or changed too often. Try the operation again.', { cause });
    this.name = 'RetryLimitError';
  }
}

export class InvalidCursorError extends Error {
  constructor(cause?: unknown) {
    super('The cursor is invalid or belongs to another listing.', { cause });
    this.name = 'InvalidCursorError';
  }
}
