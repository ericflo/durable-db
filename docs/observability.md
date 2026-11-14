# Observability and incident response

`GET /metrics` exports Prometheus text metrics. Scrape each process separately, use `job="durable-db"`, and attach cluster/instance labels in the monitoring configuration. Issue a metrics-only credential on the server side:

```sh
umask 077
durable-db token --key-file /run/secrets/admin.key --metrics \
  --ttl-seconds 2592000 > /secure/path/metrics.token
```

Mount only the resulting token into the scraper. It expires after the requested lifetime (at most 30 days); renew it before expiration. It can access only `GET /metrics`, and cannot read data, write, run collection, or mint credentials. Its HMAC domain is separate from database capabilities and it does not contain the signing key. Administrator tokens remain accepted for compatibility. Keep both the endpoint and credential behind TLS and network controls.

For example, the Prometheus scrape configuration can use:

```yaml
scrape_configs:
  - job_name: durable-db
    authorization:
      credentials_file: /run/secrets/durable-db-metrics-token
    static_configs:
      - targets: [durable-db.internal:7654]
```

Configure the deployment's actual service discovery and TLS endpoint; the example alone does not install monitoring. Metrics and administrative control requests share two reserved process slots, separate from tenant data capacity, and retain their own per-principal limits. Filling the tenant admission table does not prevent these fixed principals from creating entries. This reserve does not guarantee metrics availability during provider failures, shutdown or every overload.

Metrics use fixed operation, outcome, and event names. They contain no database names, object paths, keys, values, tokens, or provider response messages. All series have bounded cardinality. Counters reset when the process restarts; aggregate `rate` or `increase` across replicas rather than subtracting counters across restarts.

Upload admission adds `upload_grant_bytes`, `upload_grant_attempts`, `upload_charged_bytes`, `upload_charged_attempts`, and `upload_budget_exhausted` events. Grants count only confirmed allocations observed by that process; lost responses and process loss can strand additional durable allocation. Charges precede upload invocation and include attempts whose responses are lost. These process counters are diagnostics, not the authoritative balance or provider billing. The trusted control plane should poll `GET /v1/storage/upload-budget` with an administrator capability, monitor both remaining dimensions, and replenish deliberately with a durable request ID. Metrics-only credentials cannot read or modify that administrative endpoint.

Journal grouping adds `journal_frames_prepared` and `journal_records_prepared` events. Their ratio shows records per prepared frame, including catalog changes. These count preparation, not successful PUTs or client acknowledgements; cancellation and unknown uploads can separate those events. The Kubernetes load runner captures both counters per pod before and after the workload, including setup.

## Measurements

| Metric | Meaning |
| --- | --- |
| `durable_db_operations_total{operation,outcome}` | Completed or cancelled attempts; outcomes distinguish success, conflict, rejection, authorization failure, invalid requests, service unavailability, errors, and cancellation |
| `durable_db_operation_duration_seconds{operation}` | Histogram including failures and cancellation; bounds range from 1 ms to 30 s, followed by `+Inf` |
| `durable_db_operations_inflight{operation}` | Attempts currently awaiting completion; cancellation releases the gauge |
| `durable_db_events_total{event}` | Sealed read groups and their members, queue rejection, submitted proposals, publication groups/members/rebases, rejected proposals, cache hits/misses, collection candidates, and acknowledged object deletions |
| `durable_db_requests_total` | Legacy count of requests admitted to the handler |
| `durable_db_rejected_total` | Legacy count of rate/concurrency rejections; the operation outcomes cover additional rejection paths |
| `durable_db_cache_bytes` | Current accounted memory-cache usage, including estimated entry overhead; not process RSS |

HTTP operations include body decoding, admission, and handler execution. Health probes are excluded. `forward_discovery` covers cached-route lookup, its lock and any authoritative discovery; `forward_request` covers one peer request including its response body. `native_read_lock` isolates the wait for the serving view, while `native_read` covers authorization, value retrieval and fresh permit validation after that lock is acquired. `native_prepare` measures worker-side write preparation, including the owner-lock wait, authorization, cold lookups and admission funding; it excludes time queued before the worker and the subsequent journal upload. Publication timing includes retries and path rebuilding. Provider-boundary operations cover authority reads/replacements, immutable object GET/PUT/list, and physical version reclamation. B2 native operations additionally expose each native API attempt; a refreshed authorization retry appears as another attempt. Startup bucket discovery is included in native listing counters after the bounded-read-retry change. Earlier profiling reports predate that instrumentation. Read-only native retries have three attempts and a ten-second deadline; each actual native request appears in the counters. Conditional publication errors are never retried as if their outcome were known.

These are operational measurements, **not a provider billing meter**. Object-store SDK retries can occur inside a measured call. A successful physical-deletion call can remove several versions, and a failed response can hide a successful deletion. Candidate counts are scan activity, not an estimate of the whole garbage backlog. A cancelled publication may have committed; metrics cannot resolve its transaction outcome.

Acquisition logs expose the work inside the larger recovery stages. `native
acquisition owner` connects a partition ID to its activated owner's opaque
incarnation after activation succeeds. The same owner ID links the following fixed-field records:

- `native activation progress`: `revocation` starts after reading the claimant's
  registration; `publication` follows predecessor revocation and the claimant's
  permit check; `submitted` follows the conditional publication attempt. The
  predecessor count includes the checkpoint owner. Submission is not confirmation.
- `native selection progress`: discovery, publication, exact validation, cursor
  advance and completion. Journal/frame counts are cumulative discovery work,
  including rescans after uncertainty; they are not unique durable totals.
- `native catalog completed`: time spent uploading the immutable prefix records,
  index and header, independently of the subsequent conditional publication.
- `native checkpoint progress`: permission, confirmation when needed, allowance
  calculation, funding, staging and publication. `completed` and `deferred`
  records retain the final stage and whole-call duration. No-op polls emit none.
- `journal upload failed`: owner, position, fixed error class and `unsent`.
  Only `unsent=true` preserves the original handle for retry; spent credit is
  never refunded. `other` includes admission errors and unclassified provider
  errors, so it does not identify an HTTP status or prove a write reached storage.
- `native worker retired`: the fixed retirement reason, linked to subsequent
  resident removal and acquisition by owner/partition. Process readiness does
  not establish availability of every database during recovery.

Shared-publication logs additionally record `reused_pages` on helper attempts.
It means a completed directory upload survived a metadata-only comparison
conflict; it does not mean the comparison was skipped. `queued_receipt` confirms
the exact proposal before helper admission; `helping_receipt` confirms it after
admission and cancels redundant local helping. Their zero attempt/preparation/
comparison fields describe the observer only. An interrupted admitted helper may
already have uploaded pages or issued a conditional write; do not treat those
zeroes as its cost or refund its funding. Older samples omit `reused_pages`.

Progress timestamps are cumulative monotonic elapsed milliseconds within one
call; subtract adjacent values from that call for stage duration. Catalogue
completion has its own interval. A failed or cancelled call may have no terminal
record. The qualification harness retains only these fixed fields from bounded
pod-log samples; disappearance and rotation can omit events. These timings add
no provider calls and grant no authority. Checkpoint funding and immutable uploads
allow concurrent reads; publication and projection replacement remain exclusive.

Measured operations lasting at least one second also emit `slow operation` with
fixed operation/outcome labels and elapsed milliseconds. Cancellation emits a
terminal sample when the future is dropped; abrupt process death may prevent it.
These records separate discovery, forwarding, lock contention and provider work,
but nested durations overlap and must not be summed. `forward transport failed`
adds only the opaque owner incarnation, connect/timeout classification and
elapsed time for failed peer sends; it never logs the URL, request or raw error.
Peer forwarding permits two seconds to establish a connection within its existing
25-second overall request deadline. Failure invalidates that cached route; a
caller retries explicitly. A witnessed connection failure also pauses sends to
that exact route for three seconds. Each retry still discovers the owner, so a
changed incarnation or address bypasses the pause immediately. After expiry, the
same owner can be tried again. HTTP refusals alone do not start this pause. These
deadlines schedule availability, never serving authority. A cancelled send has no transport-error classification. Pod logs are bounded
samples, not complete request traces or cross-host clock measurements.

For example, inspect the publication-register conflict fraction:

```promql
sum(rate(durable_db_operations_total{job="durable-db",operation="authority_replace",outcome="conflict"}[5m]))
/
clamp_min(sum(rate(durable_db_operations_total{job="durable-db",operation="authority_replace"}[5m])), 0.001)
```

Compare p95 latency across operations, with enough observations for the quantile to be meaningful:

```promql
histogram_quantile(0.95,
  sum by (operation, le) (
    rate(durable_db_operation_duration_seconds_bucket{job="durable-db"}[5m])
  )
)
```

The ratio of `read_group_members` to `read_groups` measures barrier sharing. `publication_group_members / publication_groups` measures the selected group size for acknowledged helper CAS calls; an unknown CAS response can make those counters undercount successful publication. `proposal_submitted` counts acknowledged uploads, while `proposal_candidates`, `proposal_rejected`, and `proposal_delete_ack` expose proposal scan, validation, and cleanup activity. These counters do not measure the pending queue or establish that every submitted proposal eventually committed. Compare `cache_hit` and `cache_miss`, but remember that cache misses include checksum mismatches and cache-disabled configurations. High cache hit rates do not eliminate the authoritative read barrier.

## Alert rules and qualification

The repository provides [initial alert rules](prometheus/alerts.yml) and [synthetic rule tests](prometheus/alerts.test.yml). Woodpecker runs those tests with pinned Prometheus 3.16.0 `promtool`. They cover missing discovery, delayed firing, error recovery, distinguishing conflicts from provider errors, saturated observation admission, and persistent maintenance failures. Thresholds are provisional diagnostic thresholds, not a qualified SLO. Adapt the scrape job and grouping to the deployment before installing them.

```sh
cd deploy/prometheus
promtool test rules alerts.test.yml
```

No alerts are automatically installed into an existing monitoring service. Synthetic rule tests do not test a real notification receiver, on-call routing, or an operator's recovery procedure. Operators must configure and test their own notification receiver and recovery procedure. Persistent cleanup-backlog qualification and a measured latency/error budget are follow-up work beyond the [1.0 scope](production-readiness.md).

## Unavailable

Check whether discovery has removed all targets, scraping authentication has failed, or the processes themselves are down. Compare Kubernetes readiness, restart counts, and rollout state. Inspect sanitized startup errors and provider connectivity. Restore a healthy process with the same authoritative bucket/prefix and signing configuration; caches may be empty. Never initialize a replacement bucket over the original state as an outage workaround. In local mode, verify that the directory's exclusive owner is actually gone before restarting.

## Provider errors

Compare authority, immutable-object, and native B2 operation outcomes and latency. An authorization failure calls for checking the mounted workload credential and its permissions; native authorization refresh is automatic. A provider outage or service-unavailable response calls for reducing offered work and preserving identical retries. B2 documents native rate limiting with HTTP `429`, while its S3-compatible API uses `503 SlowDown`; a generic native `503` alone does not establish throttling. See [Backblaze's rate-limit guide](https://www.backblaze.com/docs/cloud-storage-rate-limits).

`response_validation_unavailable` counts failed final response validations while the local owner remains valid. The server withholds the application result with HTTP 503 and retains the worker; an identical retry can resolve a durable receipt. Confirmed revocation still stops serving. This counter diagnoses failed observations, not their provider cause, and cannot account for samples lost when a process disappears.

Preserve the request ID, expected revision, and exact transaction body after an uncertain write. Do not turn uncertainty into a new write ID or blindly delete its uploaded objects. Repeated checksum or manifest-validation errors require preserving the authoritative objects for diagnosis; do not run an external lifecycle cleanup or replace the directory root manually.

## Publication contention

Compare register conflicts, rebases, read-group size, publication duration, and maintenance activity. Separate a hot database's expected-revision conflicts from contention between unrelated database publications. Reduce excess concurrency or arrival rate while retaining identical retries for uncertain outcomes. Adding replicas can increase shared-register competition. Do not disable read validation or collection fences to improve the graph. Reproduce the overload in a disposable deployment before changing publication parameters.

## Observation admission

Compare queue rejection with read-barrier duration and provider outcomes. Each process admits at most 64 sealed observations plus 64 queued observations; additional work fails promptly. Provider stalls can keep a group in flight after callers time out. Reduce the incoming workload and investigate the slow provider operation. A larger unbounded queue would increase latency and memory use without repairing provider progress.

## Maintenance

Compare maintenance errors, candidate activity, acknowledged deletions, and provider failures. A conflict safely defers work; sustained errors can leave storage accumulating. Verify physical version-list/delete permissions and any external retention policy. Preserve all publication/collection fences. Increase maintenance capacity only after measuring its effect on foreground writes; a larger cluster can duplicate collection work. A deletion counter is evidence of activity, not proof that the whole backlog is bounded. Use disposable-bucket and restore drills to validate any recovery procedure before applying it to important data.
