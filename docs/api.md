# HTTP API

Everything is JSON over HTTP under `/v1`. A database is an ordered map from string keys to JSON values, changed by atomic transactions.

```sh
export URL=http://127.0.0.1:7654/v1/databases/demo
export AUTH="Authorization: Bearer $(durable-db create demo)"

curl -X PUT $URL/keys/greeting -H "$AUTH" -d '"hello"'       # {"revision":1}
curl $URL/keys/greeting -H "$AUTH"                            # "hello"
```

## Concepts

**Revision.** Each committed transaction increases the database's revision by one. Revision 0 is the empty database.

**Version.** A key's version is the revision of the transaction that last wrote it. An absent key has version 0. Versions let a transaction say "only if this key is unchanged since I read it".

**Keys** are UTF-8 strings of 1 to 1,024 bytes that do not begin with U+0000, ordered by their bytes. **Values** are any JSON value except `null`, stored as the exact text sent.

**Consistency.** Writes are strictly serializable: every transaction takes effect at one instant, in revision order, and an acknowledged transaction is durable in the bucket. Reads offer two levels:

| Level | Guarantee | Cost |
| --- | --- | --- |
| `strong` (default) | Linearizable: reflects every transaction acknowledged before the read was sent. | One storage round trip, shared by all reads waiting at that moment. |
| `session` | A consistent snapshot at revision `min_revision` or newer. Reading your own writes and never going backwards are guaranteed when you pass the highest revision you have seen. | Served from memory. |

A `session` read can lag behind another client's very recent write, and only while a database is moving between servers. Transactions are unaffected: a commit that depends on a stale read fails its check and is retried.

## Authentication

Send `Authorization: Bearer <token>`. An **administrator token** manages databases. A **database token** grants `read` or `write` on one database. Tokens are signed with the deployment's key and checked without touching storage.

```sh
durable-db token --admin                                  # from the signing key
durable-db token --database demo --access read --ttl 3600
```

Or ask a server, which also stamps the database's current token generation:

```http
POST /v1/databases/demo/tokens
{"access": "write", "ttl_seconds": 86400}

{"token": "ddb_..."}
```

`PATCH /v1/databases/demo` with `{"revoke_tokens": true}` invalidates every token issued for that database so far.

## Databases

Administrator token required, except `GET` on one database, which any token for it may call.

| Request | Effect |
| --- | --- |
| `PUT /v1/databases/{name}` | Create. Body (optional): `{"max_bytes": 1073741824, "max_requests_per_second": 0}`. `201` when created, `200` when it already exists with these settings, `409` when it exists with others. |
| `GET /v1/databases/{name}` | `{"name", "revision", "bytes", "max_bytes", "max_requests_per_second", "tables"}` |
| `PATCH /v1/databases/{name}` | Change `max_bytes` or `max_requests_per_second`, or set `"revoke_tokens": true`. |
| `DELETE /v1/databases/{name}` | Delete permanently. The name cannot be reused. |
| `GET /v1/databases?after=&limit=` | `{"databases": [...], "next": "name" or null}`, in name order. |

A name is 1 to 128 characters from `A-Z a-z 0-9 . _ -` and does not begin with `.`.

`bytes` counts stored table bytes plus unflushed writes. When it would exceed `max_bytes`, transactions that add data fail with `507`; deletes still succeed.

## Read

```http
POST /v1/databases/{name}/read
{
  "keys": ["users/42", "users/43"],
  "scans": [{"prefix": "tasks/", "limit": 50}],
  "consistency": "strong"
}
```

```json
{
  "revision": 1207,
  "values": [{"value": {"name": "Ada"}, "version": 1190}, null],
  "scans": [{
    "items": [{"key": "tasks/0001", "value": {"done": false}, "version": 88}],
    "next": "tasks/0001"
  }]
}
```

Every key and scan in one request is read at the same revision. `values` has one entry per requested key, `null` for an absent key.

A scan selects keys by any combination of:

| Field | Meaning |
| --- | --- |
| `prefix` | Keys beginning with this string. |
| `start` | Keys greater than or equal to this. |
| `end` | Keys less than this. |
| `after` | Continue past this key, in the scan's direction. Pass the previous page's `next`. |
| `reverse` | `true` returns keys in descending order. |
| `limit` | At most this many items; default 100, maximum 10,000. |

`next` is the last key returned when more keys remain, otherwise `null`. A page also ends early at 8 MiB.

For `"consistency": "session"`, add `"min_revision": N` with the highest revision you have seen from this database. If that revision is still being stored, the read waits for it. A `min_revision` the database has never reached is a `400`.

## Commit

```http
POST /v1/databases/{name}/commit
{
  "checks": [{"key": "accounts/a", "version": 1190}],
  "writes": [
    {"key": "accounts/a", "value": {"balance": 70}},
    {"key": "accounts/b", "value": {"balance": 130}},
    {"key": "pending/9", "delete": true}
  ],
  "idempotency_key": "transfer-7f3a"
}
```

```json
{"revision": 1208}
```

All writes apply together or not at all. A transaction needs at least one write and may write each key once.

**Checks** make it conditional. If any check fails the response is `409` with code `conflict` and nothing is written:

| Check | Passes when |
| --- | --- |
| `{"key": K, "version": V}` | Key `K` is at version `V`. Use `0` for "must not exist". |
| `{"range": {...}, "revision": R}` | No key in the range was written after revision `R`. The range takes `prefix`, `start` and `end` as in a scan. Use the `revision` of the read that scanned it. |
| top-level `"if_revision": R` | The whole database is still at revision `R`. |

Read, compute, and commit with a check for each key and range you read: the transaction then behaves as if it ran alone. On `409`, read again and recompute. The conflict carries `"revision"`: pass it as `min_revision` and the read is guaranteed to include whatever caused the conflict. Range checks are verified against recent writes only; a range read too long ago fails with `409` and is simply read again.

There is no read at an exact past revision. Two separate reads may see different revisions; the checks catch it at commit.

**Idempotency.** With an `idempotency_key` (1 to 255 bytes), repeating the same transaction returns the original result instead of applying it again, for the next 100,000 revisions. Reusing a key with different writes is a `400`. Always send one when a retry is possible.

**Several transactions** in one request are applied in order, each atomically, each with its own revision:

```http
{"transactions": [{"writes": [...]}, {"checks": [...], "writes": [...]}]}
```

```json
{"results": [{"revision": 1209}, {"error": {"code": "conflict", "message": "...", "retryable": false}}]}
```

Each transaction takes the same fields as a single one, including its own `idempotency_key`; nothing but `transactions` may appear at the top level. The first failure stops the request; later transactions report `conflict` with the message "not attempted". The status is `200` whenever the request itself was understood. If any transaction is malformed the whole request is a `400` and none is applied.

## Single keys

| Request | Effect |
| --- | --- |
| `GET /v1/databases/{name}/keys/{key}` | The value as the response body, with `x-ddb-version` and `x-ddb-revision` headers. `404` when absent. Add `?consistency=session` for a read from memory. |
| `PUT /v1/databases/{name}/keys/{key}` | Store the body, which must be JSON. Returns `{"revision": N}`. |
| `DELETE /v1/databases/{name}/keys/{key}` | Delete the key. Returns `{"revision": N}`. |

Percent-encode the key in the path; `/` may be left as is.

## Errors

```json
{"error": {"code": "conflict", "message": "key \"accounts/a\" is at version 1201, not 1190", "retryable": false}}
```

| Status | Code | Meaning | What to do |
| --- | --- | --- | --- |
| 400 | `invalid` | The request is malformed or exceeds a limit. | Fix the request. |
| 401 | `unauthorized` | Missing, forged, expired or revoked token. | Obtain a token. |
| 403 | `forbidden` | The token does not cover this database or action. | |
| 404 | `not_found` | No such database, or no such key on a single-key `GET`. | |
| 409 | `conflict` | A check failed. The error carries `revision`. | Read at that revision or later, and recompute. |
| 409 | `exists` | The database exists with different settings. | |
| 410 | `deleted` | The database was deleted. | |
| 413 | `too_large` | The request body exceeds 32 MiB. | Split the request. |
| 429 | `rate_limited` | The database's request budget is spent. | Retry after `Retry-After` seconds. |
| 503 | `busy`, `ownership_moved`, `storage_unavailable` | The request could not be completed now: ownership is moving, storage did not answer, or the server is shedding load. | Repeat the identical request. |
| 507 | `quota_exceeded` | The database is at `max_bytes`. | Delete data or raise the quota. |

A `503` on a commit means the outcome is **unknown**: the transaction may have been applied. Repeating it with the same `idempotency_key` is always safe and returns the definite result. Servers repeat such commits themselves when a key is present, so clients see this rarely.

## Limits

| | Default |
| --- | --- |
| Key | 1 to 1,024 bytes |
| Value | 4 MiB (`--max-value-bytes`) |
| Request body | 32 MiB |
| Writes plus checks per transaction | 10,000 |
| Keys plus scans per read | 10,000 |
| Scan page | 10,000 items or 8 MiB |
| One read's result | 64 MiB |
| Database size | 1 GiB (`max_bytes`, per database) |

## Health and metrics

`GET /healthz` answers `200` while the process runs. `GET /readyz` answers `200` when storage is reachable; use it as the readiness probe. `GET /metrics` returns Prometheus text: request and storage latency histograms, cache statistics and open databases. No key, value, token or database name appears in metrics or logs.
