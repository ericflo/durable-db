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
| `POST /v1/databases/{db}/access` | Admin: enable durable authorization, revoke a token ID, or rotate its generation |
| `POST /v1/tokens` | Admin: mint a database-scoped token |
| `GET /metrics` | Administrator or metrics-only credential: bounded Prometheus latency, outcomes, provider, cache, admission, and maintenance metrics; see [observability](observability.md) |
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

## Durable token revocation

Creation currently preserves legacy format 1 for upgrade compatibility. Before issuing application credentials for a protected deployment, upgrade all servers, then enable the database policy with an administrator request:

```json
{
  "request_id": "a-unique-policy-operation-id",
  "expected_policy_revision": 0,
  "change": {"action": "enable"}
}
```

Send this to `POST /v1/databases/{db}/access`. The response is `{policy: {incarnation, generation, revision, revoked_tokens}, replayed}`. Activation moves this database to manifest format 2 and invalidates all legacy database tokens at the policy publication. Old binaries refuse the database after activation, including administrator reads and collection. This migration has no downgrade operation. Complete the binary rollout first and retain a suitable recovery plan. Other databases may remain in legacy format during migration.

`POST /v1/tokens` accepts `{database, write, ttl_seconds}`. For an enabled policy it returns `{token, token_id}` bound to the current incarnation and generation. For a legacy database it returns the legacy `{token}` envelope. Issuance racing a generation change may return a token that is already invalid; request another token after observing the current policy. For offline issuance, supply both `--incarnation` and `--generation` from fresh administrator info to `durable-db token`. Omitting both produces a legacy token, which protected databases refuse.

To revoke one token, submit a new policy request ID and the current **policy** revision, with `"change": {"action": "revoke", "token_id": "<issued UUID>"}`. To invalidate all tokens and clear the revocation set, use `"change": {"action": "rotate"}`. This advances the database credential generation; it does not rotate the shared signing key. At most 128 token IDs can be revoked per generation. Rotation is required before adding another ID when that set is full. Revocations never expire based on a server's clock. Repeating `enable` cannot reset a policy.

Policy changes leave the **data** revision unchanged and advance the separate policy revision. They retain the last 32 policy receipts, independent of data-write receipts. After an unknown outcome, retry the identical request ID, expected policy revision, and body. A retained receipt returns its original policy result, which may precede the current policy; use administrator info for the latest state. Beyond the receipt window, the old expected revision prevents reapplication and returns 409. Reusing a retained request ID with different content returns 400. A conflict requires inspecting fresh policy state before deciding on a new administrative operation.

Info, reads, writes, and write-receipt replay authorize against the same validated manifest they use. A read already in flight may finish from its earlier authorized snapshot; an operation invoked after an acknowledged revocation cannot use that old policy. A write prepared before a policy change loses the exact manifest comparison if the policy publishes first. Administrator operations remain privileged. [Security boundaries](security.md) and [verification limits](verification.md#durable-authorization-lifecycle) still apply.

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
