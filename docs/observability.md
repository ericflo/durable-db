# Observability and incident response

The administrator-only `GET /metrics` endpoint exports Prometheus text metrics. Scrape each process separately, use `job="durable-db"`, and attach cluster/instance labels in the monitoring configuration. Protect the administrator credential and keep the endpoint behind the deployment's TLS and network controls. A separate metrics-only credential is not yet implemented.

Metrics use fixed operation, outcome, and event names. They contain no database names, object paths, keys, values, tokens, or provider response messages. All series have bounded cardinality. Counters reset when the process restarts; aggregate `rate` or `increase` across replicas rather than subtracting counters across restarts.

## Measurements

| Metric | Meaning |
| --- | --- |
| `durable_db_operations_total{operation,outcome}` | Completed or cancelled attempts; outcomes distinguish success, conflict, rejection, authorization failure, invalid requests, service unavailability, errors, and cancellation |
| `durable_db_operation_duration_seconds{operation}` | Histogram including failures and cancellation; bounds range from 1 ms to 30 s, followed by `+Inf` |
| `durable_db_operations_inflight{operation}` | Attempts currently awaiting completion; cancellation releases the gauge |
| `durable_db_events_total{event}` | Sealed read groups and their members, queue rejection, publication rebases, cache hits/misses, collection candidates, and acknowledged object deletions |
| `durable_db_requests_total` | Legacy count of requests admitted to the handler |
| `durable_db_rejected_total` | Legacy count of rate/concurrency rejections; the operation outcomes cover additional rejection paths |
| `durable_db_cache_bytes` | Current accounted memory-cache usage, including estimated entry overhead; not process RSS |

HTTP operations include body decoding, admission, and handler execution. Health probes are excluded. Publication timing includes retries and path rebuilding. Provider-boundary operations cover authority reads/replacements, immutable object GET/PUT/list, and physical version reclamation. B2 native operations additionally expose each native API attempt; a refreshed authorization retry appears as another attempt. Startup bucket discovery is not included in native listing counters.

These are operational measurements, **not a provider billing meter**. Object-store SDK retries can occur inside a measured call. A successful physical-deletion call can remove several versions, and a failed response can hide a successful deletion. Candidate counts are scan activity, not an estimate of the whole garbage backlog. A cancelled publication may have committed; metrics cannot resolve its transaction outcome.

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

The ratio of `read_group_members` to `read_groups` measures barrier sharing. Compare `cache_hit` and `cache_miss`, but remember that cache misses include checksum mismatches and cache-disabled configurations. High cache hit rates do not eliminate the authoritative read barrier.

## Alert rules and qualification

The repository provides [initial alert rules](prometheus/alerts.yml) and [synthetic rule tests](prometheus/alerts.test.yml). Woodpecker runs those tests with pinned Prometheus 3.16.0 `promtool`. They cover missing discovery, delayed firing, error recovery, distinguishing conflicts from provider errors, saturated observation admission, and persistent maintenance failures. Thresholds are provisional diagnostic thresholds, not a qualified SLO. Adapt the scrape job and grouping to the deployment before installing them.

```sh
cd deploy/prometheus
promtool test rules alerts.test.yml
```

No alerts are automatically installed into an existing monitoring service. Synthetic rule tests do not test a real notification receiver, on-call routing, or an operator's recovery procedure. Those drills, persistent cleanup-backlog measurement, and a measured latency/error budget remain production gates.

## Unavailable

Check whether discovery has removed all targets, scraping authentication has failed, or the processes themselves are down. Compare Kubernetes readiness, restart counts, and rollout state. Inspect sanitized startup errors and provider connectivity. Restore a healthy process with the same authoritative bucket/prefix and signing configuration; caches may be empty. Never initialize a replacement bucket over the original state as an outage workaround. In local mode, verify that the directory's exclusive owner is actually gone before restarting.

## Provider errors

Compare authority, immutable-object, and native B2 operation outcomes and latency. An authorization failure calls for checking the mounted workload credential and its permissions; native authorization refresh is automatic. A provider outage or service-unavailable response calls for reducing offered work and preserving identical retries. B2 documents native rate limiting with HTTP `429`, while its S3-compatible API uses `503 SlowDown`; a generic native `503` alone does not establish throttling. See [Backblaze's rate-limit guide](https://www.backblaze.com/docs/cloud-storage-rate-limits).

Preserve the request ID, expected revision, and exact transaction body after an uncertain write. Do not turn uncertainty into a new write ID or blindly delete its uploaded objects. Repeated checksum or manifest-validation errors require preserving the authoritative objects for diagnosis; do not run an external lifecycle cleanup or replace the directory root manually.

## Publication contention

Compare register conflicts, rebases, read-group size, publication duration, and maintenance activity. Separate a hot database's expected-revision conflicts from contention between unrelated database publications. Reduce excess concurrency or arrival rate while retaining identical retries for uncertain outcomes. Adding replicas can increase shared-register competition. Do not disable read validation or collection fences to improve the graph. Reproduce the overload in a disposable deployment before changing publication parameters.

## Observation admission

Compare queue rejection with read-barrier duration and provider outcomes. Each process admits at most 64 sealed observations plus 64 queued observations; additional work fails promptly. Provider stalls can keep a group in flight after callers time out. Reduce the incoming workload and investigate the slow provider operation. A larger unbounded queue would increase latency and memory use without repairing provider progress.

## Maintenance

Compare maintenance errors, candidate activity, acknowledged deletions, and provider failures. A conflict safely defers work; sustained errors can leave storage accumulating. Verify physical version-list/delete permissions and any external retention policy. Preserve all publication/collection fences. Increase maintenance capacity only after measuring its effect on foreground writes; a larger cluster can duplicate collection work. A deletion counter is evidence of activity, not proof that the whole backlog is bounded. Use disposable-bucket and restore drills to validate any recovery procedure before applying it to important data.
