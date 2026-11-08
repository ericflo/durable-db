# HTTP API

Every data endpoint requires `Authorization: Bearer <token>`. JSON uses UTF-8. Keys are ordered by their UTF-8 bytes, independent of locale. Values are standard base64. `null` means absent in a read/check and delete in a write; an empty string is a present zero-byte value.

## Endpoints

| Method and path | Meaning |
| --- | --- |
| `POST /v1/databases/{db}` | Admin: create with `max_bytes` and `max_keys` quotas |
| `GET /v1/databases/{db}` | Revision, logical usage, pages, and limits |
| `POST /v1/databases/{db}/read` | Atomic read of keys and/or ordered scans |
| `POST /v1/databases/{db}/commit` | Revision-checked atomic writes and optional value checks |
| `POST /v1/databases/{db}/gc` | Admin: one bounded maintenance sweep |
| `POST /v1/tokens` | Admin: mint a database-scoped token |
| `GET /metrics` | Admin: Prometheus request, rejection, and cache metrics |
| `GET /health/live` | Process is serving HTTP |
| `GET /health/ready` | Startup completed and server is not draining |

Readiness does not continuously probe the object provider; provider outages return 503 from data requests. Liveness never depends on storage availability, avoiding restart storms.

## Read a snapshot

```json
{
  "expected_revision": 12,
  "keys": ["settings/theme"],
  "scans": [{
    "prefix": "entries/",
    "start": "entries/000010",
    "end": "entries/000100",
    "after": "entries/000020",
    "reverse": false,
    "limit": 50
  }]
}
```

`expected_revision` is optional for the first read. All keys and scans in one request observe the same manifest. Subsequent requests in the same transaction must send the returned revision. A revision change is a 409; restart the whole transaction. This is optimistic snapshot validation, not a pinned long-running historical snapshot.

`start` is inclusive, `end` exclusive. `after` is exclusive in the chosen direction. Prefix and bounds are conjunctive. A scan returns `{items: [{key, value}], next}`; `next` is the last returned key when another matching item exists. Repeat the query with that cursor and revision. Zero-length prefixes are allowed.

## Commit

```json
{
  "expected_revision": 12,
  "request_id": "d338624a-86b4-4128-85fb-2a31a86b4e60",
  "checks": [{"key":"settings/theme","value":"ZGFyaw=="}],
  "writes": [
    {"key":"settings/theme","value":"bGlnaHQ="},
    {"key":"obsolete","value":null}
  ]
}
```

Checks observe the pre-commit snapshot. Writes are applied in order, so repeated writes to a key end with the final value. The response is `{revision, replayed}`. Each nonempty accepted commit increments the logical revision once, including a logical no-op. A fresh fence also protects storage maintenance but is not a public revision.

A client must retain its exact serialized operation intent across transport retries. Do not mint a new request ID or read a new revision after an unknown write outcome and blindly reapply a non-idempotent operation. The last 128 receipts are retained. Beyond that window, an unchanged retry fails its original revision; the application may need to inspect state to resolve the outcome.

## Errors and bounds

Errors use `{error,message}`: 400 invalid request, 401 invalid/mismatched credentials, 404 missing database, 409 conflict, 413 resource limit, 429 admission limit, 503 provider failure or deadline. Axum's malformed-JSON/body-extraction errors may use plain text. Provider error details, object names, secrets, and values are not returned to callers.

| Resource | Initial bound |
| --- | --- |
| Database name | 1–64 lowercase ASCII letters, digits, `_` or `-` |
| Key | 1–1,024 UTF-8 bytes; no NUL |
| Value | 64 KiB decoded |
| Request body | 1 MiB |
| Operations | 128 checks+writes, or keys+scans |
| Scan page | 1–1,000 entries |
| Read result data | 1 MiB across all values and scan keys |
| Immutable data loaded/rewritten per operation | 8 MiB each |
| Manifest | 512 KiB; at most 2,048 pages |
| Database logical quota | Configurable up to 256 MiB and 1,000,000 keys |
| Default cache | 64 MiB accounted encoded bytes; at most 4,096 entries |
| Default concurrent admitted requests | 8 per pod |
| Per-database admission | 20 tokens/sec, burst 40 per pod; commit costs 5, other requests 1 |
| Request deadline | 30 seconds |

Quota accounting includes user key and value bytes. Physical object bytes, receipts, manifests, and transient orphan uploads are additional. Limits reject before publishing; they never silently truncate. Use bounded scans and values, and choose an ingress spend/rate budget for an Internet-facing deployment.
