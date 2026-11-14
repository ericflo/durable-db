import { connect, request, type Connection, type ConnectionOptions } from './http.ts';

export interface DatabaseInfo {
  name: string;
  revision: number;
  /** Stored bytes, counted against `maxBytes`. */
  bytes: number;
  maxBytes: number;
  /** 0 means the server's default applies. */
  maxRequestsPerSecond: number;
}

export interface DatabaseLimits {
  maxBytes?: number;
  maxRequestsPerSecond?: number;
}

export interface TokenOptions {
  /** Default `write`. */
  access?: 'read' | 'write';
  /** Seconds until the token expires. Omit for a token that does not expire. */
  ttlSeconds?: number;
}

interface WireInfo {
  name: string;
  revision: number;
  bytes: number;
  max_bytes: number;
  max_requests_per_second: number;
}

/** Database management. Needs an administrator token; keep it out of application servers. */
export class Admin {
  readonly #connection: Connection;

  constructor(options: ConnectionOptions) {
    this.#connection = connect(options);
  }

  /** Create a database. Succeeds when it already exists with the same limits. */
  async createDatabase(name: string, limits: DatabaseLimits = {}): Promise<DatabaseInfo> {
    return info(
      await request<WireInfo>(this.#connection, 'PUT', path(name), {
        max_bytes: limits.maxBytes,
        max_requests_per_second: limits.maxRequestsPerSecond,
      }),
    );
  }

  async getDatabase(name: string): Promise<DatabaseInfo> {
    return info(await request<WireInfo>(this.#connection, 'GET', path(name)));
  }

  /** Delete a database for good. Its name cannot be used again. */
  async deleteDatabase(name: string): Promise<void> {
    await request(this.#connection, 'DELETE', path(name));
  }

  /** Mint a token that grants `access` to one database. */
  async createToken(
    name: string,
    { access = 'write', ttlSeconds }: TokenOptions = {},
  ): Promise<string> {
    const { token } = await request<{ token: string }>(
      this.#connection,
      'POST',
      `${path(name)}/tokens`,
      { access, ttl_seconds: ttlSeconds },
    );
    return token;
  }
}

const path = (name: string) => `/v1/databases/${encodeURIComponent(name)}`;

const info = (wire: WireInfo): DatabaseInfo => ({
  name: wire.name,
  revision: wire.revision,
  bytes: wire.bytes,
  maxBytes: wire.max_bytes,
  maxRequestsPerSecond: wire.max_requests_per_second,
});
