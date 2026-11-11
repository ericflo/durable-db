# HTTP API

Local and cloud CLI serving use the native backend. Cloud membership, ownership and forwarding are integrated; production deployment and performance qualification remain open.

Native protected creation also charges the target database's admission budget. Exhaustion returns `413 limit_exceeded`; identical retries reuse admission. Policy changes remain namespace-funded so tenant exhaustion cannot prevent credential revocation. This also applies to cloud CLI serving.

Administrator tokens from current `token --admin` are expiring `ddb_control_` capabilities. Token TTL defaults to one hour and is bounded to 30 days. See the [key rollout procedure](design-signing-rotation.md) before migrating control clients or rotating keys.

Every data endpoint requires `Authorization: Bearer <token>`. JSON uses UTF-8. Keys are ordered by their UTF-8 bytes, independent of locale. Values are standard base64. `null` means absent in a read/check and delete in a write; an empty string is a present zero-byte value.

The shared router can also serve a recovered native owner through `api::Native::new(owner, capacity)` and `App::new`. This library path supports protected creation, info, reads, commits, tokens, and access-policy changes with the same authentication and request budgets. Its bounded server-owned worker continues accepted writes after a caller disconnects, pipelines immutable uploads outside the owner lock, resolves unknown PUT outcomes by exact GET, and retries checkpoint maintenance. Capacity bounds queued, executing and pending replies, including duplicate requests. Each HTTP commit currently occupies one journal frame; cross-request batching remains open.

Cloud readiness keeps routing and administration reachable during partition recovery. It grants no permission to serve database responses. Native worker health checks local retirement state without provider I/O or waiting behind maintenance; a failed provider observation alone does not require replacing an idle owner. Local serving retains the directory lock; remote native serving uses `api::Native::new(owner, capacity)` with a recovered owner and no clock or renewal task. Results are prepared before a fresh uncached permit GET; one validation can release a sealed group of durable replies. Missing/corrupt permission and provider errors refuse service. Cloud election, routing and placement use the native controller and DNS membership. Native legacy POST creation and `/gc` return `501 unsupported_operation`; use protected PUT creation and automatic maintenance. Namespace upload ceilings and per-database admission are implemented. Maintenance collects obsolete budget/catalog metadata, covered journals, table/manifest objects and abandoned permits. Complete tenant cost attribution remains open. This is still experimental.

## Endpoints

| Method and path | Meaning |
| --- | --- |
| `PUT /v1/databases/{db}` | Admin: idempotent protected creation with a permanent request identity |
| `POST /v1/databases/{db}` | Admin: legacy format-1 creation with `max_bytes` and `max_keys` quotas |
| `GET /v1/databases/{db}` | Revision, live key/byte usage, access policy, and limits |
| `POST /v1/databases/{db}/read` | Atomic read of keys and/or ordered scans |
| `POST /v1/databases/{db}/commit` | Revision-checked atomic writes and optional value checks |
| `POST /v1/databases/{db}/gc` | Returns `501`; native maintenance runs automatically |
| `POST /v1/databases/{db}/access` | Admin: enable durable authorization, revoke a token ID, or rotate its generation |
| `POST /v1/tokens` | Admin: mint a database-scoped token |
| `GET /v1/storage/upload-budget` | Admin: authoritative cumulative upload ceilings and allocation, or `null` when unbudgeted |
| `POST /v1/storage/upload-budget` | Admin: revision-checked, idempotent increase of upload ceilings |
| `GET /v1/databases/{db}/upload-budget` | Admin, native backend: database admission/reserve allowance and allocation; a missing ledger reports zeros |
| `POST /v1/databases/{db}/upload-budget` | Admin, native backend: preprovision or increase the database admission/reserve allowance |
| `GET /metrics` | Administrator or metrics-only credential: bounded Prometheus latency, outcomes, provider, cache, admission, and maintenance metrics; see [observability](observability.md) |
| `GET /health/live` | Process is serving HTTP |
| `GET /health/ready` | HTTP routing/administration is available and server is not draining; individual cloud partitions may still be recovering |

Database info reports logical usage. The experimental `pages` field has been removed: native tables are shared across databases, so a per-database physical page count is not a useful API contract.

Readiness does not continuously probe the object provider; provider outages return 503 from data requests. Liveness never depends on storage availability, avoiding restart storms.

Authenticated requests share a weighted [request budget](deployment.md#request-budgets) per principal and pod. Commits cost five units; other requests cost one. Default refill is 20 units/second with a burst of 40. HTTP 429 means a request or engine capacity limit was reached; back off and retry the identical request. An earlier attempt with that identity may still be pending. These configurable policy limits are separate from database quotas and measured engine capacity.

## Create a protected database

Use `PUT /v1/databases/{db}` with an administrator credential:

```json
{
  "request_id": "persist-this-unique-provisioning-id",
  "limits": {"max_bytes": 67108864, "max_keys": 100000}
}
```

The operation atomically creates a protected database with revocable tokens enabled from the start (format 2 in the existing engine; a durable catalog row in native serving). The response is `{info, replayed}`; initial `info.policy` has a fresh incarnation, generation zero, policy revision one, and no revocations. Legacy application tokens cannot access it. Mint scoped tokens through `POST /v1/tokens` after creation.

Persist the exact creation request in your provisioning system before sending it. A retry with the same ID and limits returns the original initial info even after later data writes, policy changes, and collection. Its receipt lasts for the database lifetime, independently of the bounded transaction/policy receipt windows. A different creation ID returns 409; reusing the original ID with different limits returns 400. An existing legacy database cannot be claimed by this operation. PUT never replaces an existing database. On a transport failure or 503, retry the identical creation request; observing only that the name exists is insufficient evidence that your creation succeeded.

`POST /v1/databases/{db}` retains the original legacy behavior for compatibility. It accepts only `{max_bytes,max_keys}`, has no creation receipt, and starts without durable revocation. New provisioning should use PUT. All servers must support format 2 before serving protected databases.

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

`expected_revision` is optional for the first read. All keys and scans in one request observe one committed snapshot. Subsequent requests in the same transaction must send the returned revision. A revision change is a 409; restart the whole transaction. This is optimistic snapshot validation, not a pinned long-running historical snapshot.

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

Only `409 conflict` authorizes a fresh transaction attempt before any uncertain write outcome. The native engine also distinguishes `409 receipt_expired`: the receipt no longer establishes the original outcome, so reconcile using the original request instead of generating a new ID. The JSON SDK raises `UncertainCommitError` for this case; Pi adapters stop instead of rebuilding the write. This distinction is implemented in the native library while HTTP router integration remains pending.

## Durable token revocation

PUT creation starts protected. For an existing legacy database, upgrade all servers, then enable its database policy with an administrator request:

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

## Cumulative upload admission

The native cloud CLI uses a format-6 shared partition directory with [upload admission](design-upload-admission.md) for fresh B2/S3 namespaces. Initialize with both `--upload-budget-bytes` and `--upload-budget-attempts`. These are cumulative ceilings for immutable payload bytes and PUT attempts, shared by every database and process using the authority. They do not measure logical live data or the complete provider bill. Earlier cloud storage formats require export with their previous binary and restore into a fresh namespace; local storage is unbudgeted.

The administrator GET returns `configuration_revision`, `limits: {bytes, attempts}`, and `allocated: {bytes, attempts}`. Allocation includes whole grants, uncertain allocations, and unused credit lost with a process. It is deliberately conservative. POST takes absolute replacement ceilings:

```json
{"request_id":"fund-next","expected_configuration_revision":0,"limits":{"bytes":2147483648,"attempts":200000}}
```

At least one ceiling must increase and neither may decrease. The result contains current `status` and `replayed`. Eight administrative receipts retain exact request identity; an evicted request's stale expected revision conflicts. Retry an uncertain change with its identical ID and body. Startup flags never reset an initialized ledger. Administrative refill stays available before owner readiness. Existing owners can read while immutable upload credit is exhausted; acquiring an owner, publishing checkpoints and writing data require funded uploads. Fenced physical collection itself needs no immutable PUT credit, but virtual partition-control publication still uses the shared directory allowance. Exhaustion returns `503 storage_unavailable` because a previously submitted proposal may still commit. Resolve the original write identity after a refill; do not reinterpret exhaustion as a definite abort.

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

Native database budgets use these same request/result shapes at `/v1/databases/{db}/upload-budget`, with an additional `maintenance: {funded_bytes, allocated: {bytes, attempts}}` in status. Defaults on protected creation are 64 MiB of combined admission/reserve funding and 16,384 transaction admissions; an administrator-preprovisioned ledger is preserved. Each byte grant divides its allocation equally between volatile admission rights and durable maintenance reserve. The nested counter counts construction claims and their conservative byte allowance, including lost replies; the outer allocation includes all grants and reserve funding.

Write-admission exhaustion returns `413 limit_exceeded` before journal order, so funded neighbors can continue. Admission prepays construction reserve; retries or recovery claim fresh allowance before construction. If repeated failures exhaust reserve and the remaining ceiling, shared checkpointing can stop until an explicit refill. Neither credential rotation, checkpoints nor process replacement resets spent credit. See [native upload admission](design-upload-admission.md#native-serving) for units and exclusions. The native cloud CLI implements database-specific budget routes; unbudgeted local mode returns `501`.
