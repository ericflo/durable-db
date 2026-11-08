# Deployment and operations

The service is experimental. Kubernetes qualification and cloud B2 tests are still in progress; do not treat current local tests as a production readiness certification.

For laptop use, run `durable-db serve --data ./data`. The process exclusively locks its store directory. Stop it before opening the same directory elsewhere. Local mode must not be shared across pods or placed on a network filesystem.

For S3, every pod receives the same bucket, prefix, and administrator signing key file. Supply AWS credentials through the provider's environment or workload identity. Run `durable-db serve --listen 0.0.0.0:7654 --bucket BUCKET --admin-key-file /run/secrets/admin.key`. A startup probe rejects providers that fail conditional object writes. B2 uses the same publication implementation through its native revision binding; see [B2 configuration](b2.md).

Use `/health/live` for liveness and `/health/ready` for readiness. On SIGTERM the server drops readiness and drains requests. Completed writes survive pod removal because their authority is object storage; clients retry ambiguous writes with identical request bodies and IDs. Readiness currently indicates server startup/drain state, not a continuous provider health check.

Set pod CPU/memory limits and concurrency/cache limits together. Memory cache bytes are a payload budget, not a process RSS ceiling. Bound ingress body size, authenticate at the service, terminate TLS at trusted ingress, and retain provider credentials only on the service side. See [security](security.md) for the remaining cluster-wide abuse protection work.

Every pod performs bounded jittered maintenance. Collection uses a durable publication fence; it does not depend on pod leases or reliable termination hooks. Failed deletion is retried in later sweeps. Both providers delete obsolete physical file versions. S3 also collects superseded versions of its directory control object. External retention must not prevent reclamation or delete live files.

## Kubernetes manifests

`deploy/kubernetes/` is a Kustomize base with a Deployment, Service, HPA (1–8 replicas), and disruption budget. It uses a read-only root filesystem, unprivileged UID, bounded disposable disk cache, startup/readiness/liveness probes, and graceful drain. CPU-based autoscaling requires Metrics Server; provider latency can limit throughput even when CPU is low. Calibrate scaling against real workload measurements.

Build/pull the runtime image, then set its immutable digest in an overlay. The base's `durable-db:local` is a deliberate image placeholder, not a published registry claim. Woodpecker publishes each successful commit's image in the configured private registry.

Create namespace `durable-db`, a `durable-db-signing` Secret containing `admin.key`, and a `durable-db-storage` Secret containing either `DURABLE_DB_B2_BUCKET`, `B2_APPLICATION_KEY_ID`, `B2_APPLICATION_KEY`, or `DURABLE_DB_BUCKET` and the S3 provider settings. Keep secrets out of Git. Never mount a shared local store directory across pods. Apply the configured overlay with `kubectl apply -k`.

The local multi-process integration test (`scripts/test-process-scaling.py`) exercised 1→3→1 servers, SIGKILL, and cache-free restart against MinIO: all 36 writes survived. This is process-level evidence, not yet a Kubernetes HPA qualification.
