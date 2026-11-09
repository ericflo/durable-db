# Production readiness

**Release decision: not ready for production.** v0.1.0 is an experimental release. The passing models, cloud tests, and 10,005-database directory test establish a useful foundation. They do not establish safe public multitenancy, an operating envelope, disaster recovery, or a proof of the implementation.

This document tracks the gates for the first production release. A gate closes only with reproducible evidence tied to a commit and configuration. A design, a passing happy-path test, or a successful deployment is not sufficient. The [verification record](verification-results.md) separates completed experiments from planned qualification.

## Contract to qualify

The initial contract is atomic, revision-checked transactions and ordered scans within one database. Acknowledged writes must survive the loss of every API process and cache. Uncertain writes require identical retries; the recent receipt window is bounded. Cross-database transactions are outside this contract.

Current per-database limits are 256 MiB, 1,000,000 keys, and 2,048 pages, with smaller request and rewrite budgets. Large database counts and high active throughput are different requirements. The directory has been tested at 10,005 databases; the first production capacity campaign should extend that to 100,000 and include concurrent creation, pagination, and collection. This is a test target, not a capacity guarantee.

Total databases, active databases, peak read/write arrival rates, value sizes, contention, and p99 latency targets must be recorded before choosing a production topology. Measure B2 first, then AWS S3, using the same implementation and workload. Include physical object versions, provider calls, and bandwidth when estimating cost. A laptop-to-cloud measurement is not a regional Kubernetes SLO.

Backup retention and recovery objectives are not yet selected. A proposed starting point is 30-day point-in-time recovery with at most five minutes of backup lag. Recovery time must be measured against a declared database size and number of simultaneous restores. Neither this policy nor its cost is currently implemented or promised.

## Release gates

| Gate | Current evidence or gap | Evidence required to close |
| --- | --- | --- |
| Publication safety | Seven finite TLA+ models cover publication, collection, directory reachability, retries, rebasing, and sealed read groups; negative controls and race tests | Compose the models; document storage/network assumptions; mechanize the inductive safety argument and implementation refinement obligations; independently review the protocol |
| Cloud performance | Initial B2 baseline failed at 0.341 successful ops/s; rebasing/read-group changes completed the short workload at 0.724 ops/s, with poor tail latency; profiling still shows costly repeated uploads and register conflicts | Model and implement combined publications; publish B2 and AWS p50/p95/p99, failures, retries, sustained throughput, physical amplification, and cost at the selected workload |
| Public multitenancy | Exact logical quotas, scoped tokens, bounded requests/caches, per-pod rate and concurrency limits | Durable cluster-wide upload, request, bandwidth, and administrative creation budgets; adversarial tests show that adding pods, retries, cancellation, and delayed uploads cannot bypass them |
| Credentials and lifecycle | Expiring tokens; replacement signing key invalidates existing tokens | Individual token revocation, overlapping key rotation, tenant export/deletion/retention, and durable audit delivery, exercised during rolling deploys and partitions |
| Recovery | Object-store authority and cache-free reopening pass | Automated backup retention/PITR, isolated backup credentials, consistent snapshots during writes and collection, restore into an empty deployment, corruption diagnosis, and recurring restore drills |
| Pi integration | All 32 upstream tests pass for the bounded reference replay adapter | Indexed adapter with bounded recovery work, full upstream conformance, competing owners, crash/reopen, cancellation, and long-history tests on B2 |
| Autoscaling and maintenance | HPA-bound transitions 1→3→1 and last-pod replacement pass | Actual load-triggered scaling, 1→3→8→1→0→1, node drains, SIGKILL/OOM, network partitions, provider throttling/outages, and bounded cleanup progress; verify operation histories throughout |
| Operations | Fixed-label request/provider latency and outcome metrics, cache/collection activity, cancellation accounting, synthetic alert tests, and initial runbooks | Measured cleanup backlog, end-to-end alert delivery and recovery drills, metrics-only credentials, qualified latency/error budgets, and production dashboard review |
| Upgrades | Versioned formats reject unsupported state; no upgrade campaign | Mixed-version readers/writers/collectors, migration interruption, downgrade refusal where required, rollback and restore procedure, and compatibility fixtures from every supported release |
| Supply chain and platforms | Static Linux x86_64 release; kache and Woodpecker; dependency auditing | Fresh Rust/npm advisory gates, image/toolchain review, signed release checksums/provenance/SBOM, installation and upgrade tests on each advertised platform, and independent security review |
| Sustained operation | Short integration tests only | At least 72 hours at the selected steady and burst workload with failures injected; no acknowledged-data loss, quota bypass, unbounded memory/object growth, or unexplained history violations; report all failures |

## Implementation order

1. Close immediate admission and integrity gaps; enforce fresh dependency audits; measure the existing B2 protocol under contention. Keep latency distributions and failures in the same report.
2. Decide the publication throughput design from those measurements. Start any batching/partitioning change in TLA+ and retain one shared B2/S3 implementation. Do not remove the conditional read barrier to improve a benchmark.
3. Model and implement durable resource admission and authorization lifecycle. A physical reservation must account for uploads arriving after a worker pauses or dies. Refunding an expired reservation before proving that its uploads cannot arrive is unsafe. Logical bytes, current physical bytes, and cumulative provider spend need separate accounting.
4. Add retention-aware snapshots and recovery before exposing the service to important data. Collection must honor retained roots, and restore must not depend on the original pods, caches, or mutable root still existing.
5. Complete the indexed Pi adapter and operational API against the resulting lifecycle and quota contracts.
6. Qualify mixed versions, provider failures, Kubernetes autoscaling, sustained load, recovery, and release artifacts. Publish evidence and resolve the independent review findings before changing the production status.

## Reproducible load baseline

`scripts/benchmark-http.mjs` creates synthetic databases on a **disposable test deployment**. It seeds one value per database, measures writes and reads with a configured number of workers, then verifies all seeded databases. The caller owns cleanup. Use scoped test-bucket credentials on the servers; the benchmark takes an administrator token from a file and does not print it.

```sh
node scripts/benchmark-http.mjs \
  --urls http://127.0.0.1:7654 \
  --token-file /path/to/test-admin-token \
  --databases 70 --workers 1,4 --operations 20 --value-bytes 1024 \
  --binary target/release/durable-db --output benchmark.json \
  --context 'Describe provider region, server placement, caches, concurrency, and maintenance'
```

Each worker owns one database so publication conflicts can retry the same request without recomputing application state. Ambiguous retries retain the exact ID, revision, and body. Reports include attempted/failed/not-started operations, HTTP statuses, full operation latency including backoff, successful latency separately, elapsed throughput, verification, and binary hash. A failed worker stops instead of issuing a new write ID after an unresolved outcome. The harness writes partial results and exits unsuccessfully on any failed phase or verification.

After a failed measured phase, the harness still checks final state, allowing an uncertain write to be either visible or absent at that read. This does not turn the timed-out operation into a success or establish that a delayed write can never arrive later. A lost-acknowledgement fixture checks failure accounting and identical retry bodies.

This is a closed-loop baseline: each worker waits for its preceding operation. It does not measure queue growth under a fixed arrival rate, establish a p99 SLO from a small sample, measure provider billing, or replace an open-loop overload test and soak campaign. The first B2 run [failed its progress gate](verification-results.md#b2-contention-baseline-release-gate-failed); publication contention is the next implementation priority.
