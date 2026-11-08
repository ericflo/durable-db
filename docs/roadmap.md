# Roadmap and release gates

## Initial experimental release — in progress

One shared publication implementation on B2 and S3, with a persistent B+tree database directory, modeled conditional publication and version-aware reclamation. Working single-node local service; S3 CAS backend with compatibility probe; ordered pages and atomic revision-checked writes; bounded caches; signed scoped credentials; logical quotas and admission limits; automatic fenced collection and incremental compaction; a checked TLA+ model with a negative control; Woodpecker CI; a public documentation site; and a bounded Pi reference adapter.

## Before a hosted preview

- Finish the indexed Pi storage adapter and run its full conformance/recovery suite.
- Add durable cluster-wide physical allocation/spend admission, request and bandwidth budgets, and abuse tests across replicas.
- Add tenant export, deletion/retention, individual token revocation, overlapping key rotation, and durable audit delivery.
- Test 1→N→1 in a disposable Kubernetes cluster with HPA, node drains, SIGKILL, network partitions, and provider errors. Report timelines, not only final success.
- Remove the single bucket publication throughput bottleneck through a modeled group-commit/partitioning design before claiming high throughput across many active sites.
- Run performance and soak tests against AWS with published latency distributions, cost/transaction, cache hit rates, and per-database contention. Optimize only from those measurements.
- Mechanize the unbounded safety argument and record the abstraction/refinement obligations.

## Before general availability

- Versioned storage migrations and mixed-version rolling upgrades, with rollback rules.
- Backups, point-in-time recovery/retention, restore drills, and corruption repair tooling.
- Multilevel data-page indexes and larger individual databases without unbounded manifests or request memory.
- B2 cloud qualification, contention measurements, and fault testing of its native bucket-revision publication protocol.
- Independent protocol/security review, fuzzing, fault-injection campaigns, release signing/attestation, and cross-platform install verification.

Cross-database transactions, arbitrary SQL, row-level authorization, and automatic conflict-free merging are not part of the initial contract. They require separate designs rather than silent additions to the manifest protocol.
