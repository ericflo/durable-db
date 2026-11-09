/** A database-scoped HTTP client. Values use standard base64 on the wire. */
export interface ClientOptions {
  url?: string;
  database: string;
  token: string;
}
export interface Limits {
  max_bytes: number;
  max_keys: number;
}
export interface PolicyInfo {
  incarnation: string;
  generation: number;
  revision: number;
  revoked_tokens: number;
}
export interface DatabaseInfo {
  revision: number;
  bytes: number;
  keys: number;
  pages: number;
  limits: Limits;
  policy?: PolicyInfo;
}
export interface Scan {
  prefix?: string;
  start?: string;
  end?: string;
  after?: string;
  reverse?: boolean;
  limit: number;
}
export interface ReadRequest {
  keys?: readonly string[];
  scans?: readonly Scan[];
  /** Pin subsequent reads to the first response's revision; a change returns 409. */
  expected_revision?: number;
}
export interface Pair {
  key: string;
  value: string;
}
export interface ScanResult {
  items: Pair[];
  next: string | null;
}
export interface ReadResult {
  revision: number;
  values: (string | null)[];
  scans: ScanResult[];
}
export interface Write {
  key: string;
  /** Base64 bytes, or null to delete. An empty string stores zero bytes. */
  value: string | null;
}
export interface CommitRequest {
  expected_revision: number;
  /** Retain this ID and the exact request body across uncertain retries. */
  request_id: string;
  writes: readonly Write[];
  checks?: readonly Write[];
}
export interface CommitResult {
  revision: number;
  replayed: boolean;
}
export interface CreationRequest {
  request_id: string;
  limits?: Limits;
}
export type AccessChange =
  { action: 'enable' } | { action: 'rotate' } | { action: 'revoke'; token_id: string };
export interface AccessRequest {
  request_id: string;
  expected_policy_revision: number;
  change: AccessChange;
}
export interface TokenRequest {
  write?: boolean;
  ttl_seconds?: number;
}
export interface TokenResult {
  token: string;
  token_id?: string;
}
export class DbError extends Error {
  constructor(status: number, body: { message?: string; error?: string });
  status: number;
  code: string | undefined;
}
export function encode(bytes: string | Uint8Array): string;
export function decode(value: string): Uint8Array;
export class DurableDb {
  constructor(options: ClientOptions);
  url: string;
  database: string;
  token: string;
  /** Low-level escape hatch; prefer the typed endpoint methods. */
  request(path: string, body?: unknown, method?: string): Promise<unknown>;
  /** Legacy creation. Use createProtected for new provisioning. */
  create(limits?: Limits): Promise<DatabaseInfo>;
  createProtected(request: CreationRequest): Promise<{ info: DatabaseInfo; replayed: boolean }>;
  changeAccess(request: AccessRequest): Promise<{ policy: PolicyInfo; replayed: boolean }>;
  issueToken(request?: TokenRequest): Promise<TokenResult>;
  info(): Promise<DatabaseInfo>;
  read(request?: ReadRequest): Promise<ReadResult>;
  /** This method performs one HTTP attempt; it does not silently recompute a transaction. */
  commit(request: CommitRequest): Promise<CommitResult>;
}
