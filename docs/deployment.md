# Deployment and operations

The service is experimental. Kubernetes qualification and cloud B2 tests are still in progress; do not treat current local tests as a production readiness certification.

For laptop use, run `durable-db serve --data ./data`. The process exclusively locks its store directory. Stop it before opening the same directory elsewhere. Local mode must not be shared across pods or placed on a network filesystem.

For S3, every pod receives the same bucket, prefix, and administrator signing key file. Supply AWS credentials through the provider's environment or workload identity. Run `durable-db serve --listen 0.0.0.0:7654 --bucket BUCKET --admin-key-file /run/secrets/admin.key`. A startup probe rejects providers that fail conditional object writes. B2 will use its separate [native publication protocol](b2.md).

Use `/health/live` for liveness and `/health/ready` for readiness. On SIGTERM the server drops readiness and drains requests. Completed writes survive pod removal because their authority is object storage; clients retry ambiguous writes with identical request bodies and IDs. Readiness currently indicates server startup/drain state, not a continuous provider health check.

Set pod CPU/memory limits and concurrency/cache limits together. Memory cache bytes are a payload budget, not a process RSS ceiling. Bound ingress body size, authenticate at the service, terminate TLS at trusted ingress, and retain provider credentials only on the service side. See [security](security.md) for the remaining cluster-wide abuse protection work.

Every pod performs bounded jittered maintenance. Collection uses a durable publication fence; it does not depend on pod leases or reliable termination hooks. Failed deletion is retried in later sweeps. Versioned S3 buckets additionally need noncurrent-version lifecycle management; ordinary delete does not reclaim historical versions.
