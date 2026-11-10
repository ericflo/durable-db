# Deployment and operations

The service is experimental. Initial real-B2, S3-compatible, and Kubernetes replica-transition tests pass; they are not a production readiness certification.

The [v0.2.0 release](https://github.com/ericflo/durable-db/releases/tag/v0.2.0) provides a static Linux x86_64 binary. While the repository is private, `gh` must be authenticated with repository access. Install without root or a Rust toolchain:

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
durable-db serve --data ./data
```

For laptop use, run `durable-db serve --data ./data`. The process exclusively locks its store directory. Stop it before opening the same directory elsewhere. Local mode must not be shared across pods or placed on a network filesystem.

For S3, every pod receives the same bucket, prefix, and administrator signing key file. Supply AWS credentials through the provider's environment or workload identity. Run `durable-db serve --listen 0.0.0.0:7654 --bucket BUCKET --admin-key-file /run/secrets/admin.key`. A startup probe rejects providers that fail conditional object writes. B2 uses the same publication implementation through its native revision binding; see [B2 configuration](b2.md).

Use `/health/live` for liveness and `/health/ready` for readiness. On SIGTERM the server drops readiness and drains requests. Completed writes survive pod removal because their authority is object storage; clients retry ambiguous writes with identical request bodies and IDs. Readiness currently indicates server startup/drain state, not a continuous provider health check.

Set pod CPU/memory limits and concurrency/cache limits together. Memory cache bytes are a payload budget, not a process RSS ceiling. Bound ingress body size, authenticate at the service, terminate TLS at trusted ingress, and retain provider credentials only on the service side. See [security](security.md) for the remaining cluster-wide abuse protection work.

Provider HTTP requests do not follow redirects. Configure the final regional S3 endpoint directly. The shared S3 connector preserves the SDK's environment-configured timeouts, connection pooling, HTTP version, randomized DNS, proxy/exclusions/proxy CA, content type, and certificate validation settings. It uses system trust roots; `disable_system_certificates` fails startup because this CLI has no replacement-root configuration. SDK-only programmatic custom resolvers, root certificates, and default headers are not exposed. AWS credential providers and server-side encryption configuration remain handled by the SDK. Immutable uploads have one attempt per invocation; reads retain bounded SDK retries.

Every pod performs bounded jittered maintenance. Collection uses a durable publication fence; it does not depend on pod leases or reliable termination hooks. Failed deletion is retried in later sweeps. Both providers delete obsolete physical file versions. S3 also collects superseded versions of its directory control object. External retention must not prevent reclamation or delete live files.

## Kubernetes manifests

`deploy/kubernetes/` is a Kustomize base with a Deployment, Service, HPA (1–8 replicas), and disruption budget. It uses a read-only root filesystem, unprivileged UID, bounded disposable disk cache, startup/readiness/liveness probes, and graceful drain. CPU-based autoscaling requires Metrics Server; provider latency can limit throughput even when CPU is low. Calibrate scaling against real workload measurements.

Build/pull the runtime image, then set its immutable digest in an overlay. The base's `durable-db:local` is a deliberate image placeholder, not a published registry claim. Woodpecker publishes each successful commit's image in the configured private registry.

Create namespace `durable-db`, a `durable-db-signing` Secret containing `admin.key`, and a `durable-db-storage` Secret containing either `DURABLE_DB_B2_BUCKET`, `B2_APPLICATION_KEY_ID`, `B2_APPLICATION_KEY`, or `DURABLE_DB_BUCKET` and the S3 provider settings. Keep secrets out of Git. When changing provider type in an existing Secret, replace its complete `data` map and remove the old provider fields; applying new `stringData` alone may leave old environment variables behind. Startup refuses simultaneous B2 and S3 selection. Never mount a shared local store directory across pods. Apply the configured overlay with `kubectl apply -k`.

The multi-process integration test (`scripts/test-process-scaling.py`) exercised 1→3→1 servers, SIGKILL, and cache-free restart against B2 and versioned MinIO: all 36 writes survived. The format-3 B2 rerun included more than 70 databases with concurrent maintenance.

`scripts/test-kubernetes.py` passed against real B2 using the Woodpecker-built image: HPA bounds changed 1→3→1, all 36 writes were retained, and forced deletion of the last pod recovered with fresh disposable caches. This tests the HPA controller responding to bounds, not CPU load. The qualification cluster lacked a working resource-metrics API; CPU-triggered scaling, node drains, and partitions remain separate qualification work. The script records HPA conditions so that distinction stays visible.

Use a registry address reachable by the **nodes' container runtime**, not just by pods. For example, a build may push through a cluster DNS service while the nodes pull the same registry through a configured NodePort alias. The qualification run used this arrangement; it did not modify cluster-wide registry or metrics settings.


## Request budgets

v0.2.0 exposes two per-principal, per-pod overload settings:

| Flag | Environment variable | Default | Accepted range |
| --- | --- | --- | --- |
| `--request-units-per-second` | `DURABLE_DB_REQUEST_UNITS_PER_SECOND` | 20 | 1–1,000,000 |
| `--request-burst-units` | `DURABLE_DB_REQUEST_BURST_UNITS` | 40 | 5–1,000,000 |

A commit attempt costs five units; other authenticated requests cost one. A new principal starts with the burst allowance, which refills continuously up to that ceiling. With defaults, an otherwise idle principal can attempt eight commits immediately and sustain at most four per second. Mixed requests share the same budget. Raising concurrency alone does not raise this ceiling.

All tokens for one database share its budget. All administrator credentials share one administrator budget across databases; metrics-only credentials have their own budget. Applications should use database-scoped credentials. Rate or concurrency rejection returns HTTP 429 before the handler runs; clients should back off. Invalid configuration fails before storage initialization.

Choose these settings alongside concurrency, memory and measured provider capacity, and keep them consistent across the pod template. They are policy limits, not measured database throughput. Scaling and process restarts create additional initial allowances; these transient limits do not replace durable quotas or cluster-wide spending controls. See [security](security.md).

## Signing-key changes

v0.2.0 reads either the original hex key file or a bounded JSON keyring through the same `--admin-key-file` setting. Follow the [expansion, promotion, and retirement procedure](design-signing-rotation.md). Key files are loaded at startup: update the Secret and complete the corresponding rolling restart, including the template used by new autoscaled pods. A still-serving process outside the current control-plane view must be fenced from clients or included in the completion check. Keep the old key trusted during an availability rollback; a compromised or deliberately retired key must not be reintroduced by restoring an old Secret.

For isolated qualification on an existing cluster, `scripts/qualify-multinode.py` creates a fresh namespace with explicit CPU/memory/pod quotas, private fixture Secrets, an authenticated control channel, and only namespace-scoped faults. It never drains or reboots nodes. Supply a dedicated disposable provider configuration and an image reference supported by the nodes' container runtime, pinned by digest; a registry address usable inside pods may not be a valid host pull address. The script deletes its own namespace on exit; its caller must remove the disposable provider bucket and key. `scripts/check-autoscaling-history.py` checks its resulting append history.

The qualifier optionally holds six replicas at its four-writer workload for `--steady-seconds 3600` before disruption. This pilot is bounded to one hour and 100,000 write intents, with unchanged 1 GiB/100,000-attempt upload ceilings. `--observe-memory` samples RSS, working set, cumulative CPU, pod identity, and restart counters from the kubelet statistics API; it requires the operator's existing node-proxy read permission and retains only the new namespace's API-container counters. An unplanned restart/replacement fails that steady memory phase instead of hiding growth behind a reset. Lightweight control polling excludes the append history; full histories are retrieved for final or failed-run evidence. This pilot does not substitute for the required 72-hour declared-load campaign.
