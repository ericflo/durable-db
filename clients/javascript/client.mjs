/** UTF-8 keys, opaque binary values, and explicit snapshot/revision boundaries. */
export class DbError extends Error {
  constructor(status, body) { super(body.message ?? `HTTP ${status}`); this.status = status; this.code = body.error; }
}
export function encode(bytes) {
  if (typeof bytes === "string") bytes = new TextEncoder().encode(bytes);
  let binary = "";
  for (let i = 0; i < bytes.length; i += 8192) binary += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return btoa(binary);
}
export function decode(value) { return Uint8Array.from(atob(value), c => c.charCodeAt(0)); }
export class DurableDb {
  constructor({ url = "http://127.0.0.1:7654", database, token }) {
    this.url = url.replace(/\/$/, ""); this.database = database; this.token = token;
  }
  async request(path, body, method = "POST") {
    return this.#request(`${this.url}/v1/databases/${encodeURIComponent(this.database)}${path}`, body, method);
  }
  async #request(url, body, method = "POST") {
    const response = await fetch(url, {
      method, headers: { authorization: `Bearer ${this.token}`, "content-type": "application/json" },
      ...(method === "GET" ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(35_000),
    });
    const result = await response.json();
    if (!response.ok) throw new DbError(response.status, result);
    return result;
  }
  create(limits = { max_bytes: 64 * 1024 * 1024, max_keys: 100000 }) { return this.request("", limits); }
  /** Keep the SAME creation object across every retry, including unknown outcomes. */
  createProtected({ request_id, limits = { max_bytes: 64 * 1024 * 1024, max_keys: 100000 } }) {
    return this.request("", { request_id, limits }, "PUT");
  }
  changeAccess({ request_id, expected_policy_revision, change }) {
    return this.request("/access", { request_id, expected_policy_revision, change });
  }
  issueToken({ write = false, ttl_seconds = 3600 } = {}) {
    return this.#request(`${this.url}/v1/tokens`, { database: this.database, write, ttl_seconds });
  }
  info() { return this.request("", undefined, "GET"); }
  read({ keys = [], scans = [], expected_revision } = {}) { return this.request("/read", { keys, scans, expected_revision }); }
  /** Keep the SAME request_id and expected_revision when retrying an uncertain outcome. */
  commit({ expected_revision, writes, checks = [], request_id = crypto.randomUUID() }) {
    return this.request("/commit", { expected_revision, request_id, checks, writes });
  }
}
