# Production readiness

**Release decision: not ready for production.** v0.1.0 is an experimental release. The passing models, cloud tests, and 100,005-entry directory qualification establish a useful foundation. They do not establish safe public multitenancy, an operating envelope, disaster recovery, or a proof of the implementation.

This document tracks the gates for the first production release. A gate closes only with reproducible evidence tied to a commit and configuration. A design, a passing happy-path test, or a successful deployment is not sufficient. The [verification record](verification-results.md) separates completed experiments from planned qualification.

## Contract to qualify

The initial contract is atomic, revision-checked transactions and ordered scans within one database. Acknowledged writes must survive the loss of every API process and cache. Uncertain writes require identical retries; the recent receipt window is bounded. Cross-database transactions are outside this contract.

Current per-database limits are 256 MiB, 1,000,000 keys, and 2,048 pages, with smaller request and rewrite budgets. Large database counts and high active throughput are different requirements. The directory has been tested at 100,005 entries with sequential insertion, periodic collection, pagination, and an uncached reopen of every head. Concurrent HTTP provisioning and collection at that population, and a declared active workload, remain capacity gates. This synthetic-provider result is not a cloud capacity guarantee.

Total databases, active databases, peak read/write arrival rates, value sizes, contention, and p99 latency targets must be recorded before choosing a production topology. Measure B2 first, then AWS S3, using the same implementation and workload. Include physical object versions, provider calls, and bandwidth when estimating cost. A laptop-to-cloud measurement is not a regional Kubernetes SLO.

Backup retention and recovery objectives are not yet selected. A proposed starting point is 30-day point-in-time recovery with at most five minutes of backup lag. Recovery time must be measured against a declared database size and number of simultaneous restores. Neither this policy nor its cost is currently implemented or promised.

## Release gates

| Gate | Current evidence or gap | Evidence required to close |
| --- | --- | --- |
| Publication safety | Nineteen finite TLA+ models plus an unbounded abstract page-reachability proof cover conditional provider mutations, pinned-owner takeover, logical backup/restore, global data collection without uploads, the upload-credit allocator, signing-key rollout, the indexed Pi Session design, protected creation, authorization, publication, collection, directory reachability, retries, rebasing, sealed read groups, and cross-replica publication groups; negative controls and race tests | Compose the models; document storage/network assumptions; extend the unbounded proof to complete protocol composition and mechanize implementation refinement; independently review the protocol |
| Replacement journal | Bounded Rust frames, ordered replay, thirteen adversarial tests, B2/MinIO reconnect checks, a 4,134-state model, an 88-obligation prefix proof and a 1,047-obligation abstract repeated root/journal composition theorem, a 473-obligation delegated permission theorem, and five bounded delegated journal instances | Extend delegated journal history beyond finite instances; implement and compose clock conversion, transaction semantics, physical admission and reclamation; prove implementation refinement |
| Cloud performance | Initial B2 baseline failed at 0.341 successful ops/s; the combiner completed the short workload at 1.051 ops/s with retries and poor tail latency; native read retries now recover bounded transient observations, with a separate B2 rerun passing | Replace the shared per-request authority path, compose ownership and durable-log recovery, then publish B2 and AWS p50/p95/p99, failures, retries, sustained throughput, physical amplification, and cost at the selected workload |
| Public multitenancy | Exact logical quotas, scoped tokens, bounded requests/caches, per-pod limits, and a cumulative global immutable-upload ceiling for fresh format-5 namespaces | Per-tenant physical admission, sustained cleanup under adversarial load, and distributed request, bandwidth, and administrative creation budgets; adversarial tests show that adding pods, retries, cancellation, and delayed uploads cannot bypass them |
| Credentials and lifecycle | Expiring administrator/metrics/tenant capabilities; bounded overlapping signing keys; durable individual revocation and generations; idempotent protected creation | Qualify key rollout in the deployment and offline issuers; tenant export/deletion/retention and durable audit delivery, exercised during rolling deploys and partitions |
| Recovery | Object-store authority and cache-free reopening pass; consistent logical export and resumable protected restore pass B2 source removal → versioned-MinIO restore and cold verification | Automated backup retention/PITR, isolated backup credentials, consistent snapshots during writes and collection, restore into an empty deployment, corruption diagnosis, and recurring restore drills |
| Pi integration | All 32 upstream cases pass for both adapters locally and the indexed adapter on B2; eight actual Harness cases pass on three cache-free B2 and MinIO API processes, including cancellation, live-owner takeover, model/tool SIGKILL, and complete API replacement | Long-running workflows, host-side scheduling and external-effect fencing, and Harness qualification across physical hosts |
| Autoscaling and maintenance | CPU-driven 1→3→8→1, drain, SIGKILL, confirmed kernel OOM, provider partition, and 0→1 cold recovery pass on kind with B2 (35 writes), versioned MinIO (468 writes), and format-5 budgeted MinIO/B2 (433/49 writes); manual scaling, pod loss, rolling replacement, provider egress restriction, and cold recovery pass across three physical hosts (130 writes) | Qualify whole-host/control-plane loss, independent rack failure domains, sustained memory bounds, broader partitions/provider throttling, and bounded cleanup progress; verify operation histories throughout |
| Operations | Fixed-label request/provider latency and outcome metrics, cache/collection activity, cancellation accounting, synthetic alert tests, expiring metrics-only credentials, and initial runbooks | Measured cleanup backlog, end-to-end alert delivery and recovery drills, qualified latency/error budgets, and production dashboard review |
| Upgrades | One old/new format-3 pair passes mixed writes, SIGKILL, and cold rollback/reopen on versioned MinIO and B2; broader upgrade campaign open | Mixed-version readers/writers/collectors, migration interruption, downgrade refusal where required, rollback and restore procedure, and compatibility fixtures from every supported release |
| Supply chain and platforms | Static Linux x86_64 release; kache and Woodpecker; dependency auditing | Fresh Rust/npm advisory gates, image/toolchain review, signed release checksums/provenance/SBOM, installation and upgrade tests on each advertised platform, and independent security review |
| Sustained operation | Short integration tests and a [one-hour physical B2 baseline](verification-results.md#one-hour-physical-b2-baseline--2026-11-09); no production-load qualification | At least 72 hours at the selected steady and burst workload with failures injected; no acknowledged-data loss, quota bypass, unbounded memory/object growth, or unexplained history violations; report all failures |

## Implementation order

**Current priority: [replace the shared serving and durability path](design-owned-partitions.md).** The existing read barrier and shared publication path have not met the performance brief. Establish a safe ownership/read protocol and a measured batched durability path before expanding release or sustained-load qualification of a replacement.

1. Compose a safe serving-authority protocol with immutable log batches and handoff seals. State the provider, timing and membership assumptions; retain one B2/S3 storage algorithm.
2. Implement a small vertical prototype and measure warm reads, batched durable writes and independent active databases. Include stale-owner fencing and total-compute-loss recovery in that same experiment. Compare end-to-end cost with the [raw provider baseline](design-owned-partitions.md#what-the-provider-measurements-establish).
3. Integrate per-tenant physical admission, fair request/egress budgets, authorization and lifecycle. Do not refund rights that could still produce delayed uploads.
4. Add multilevel indexes, bounded compaction, retention-aware snapshots and recovery. Reclamation must honor retained roots, and restore must work without the original compute or caches.
5. Run Pi and general application workloads against the resulting protocol, including request uncertainty, host ownership and the typed SDK.
6. Qualify mixed versions, provider failures, Kubernetes autoscaling, sustained load, disaster recovery and release artifacts. Resolve independent review findings before changing the production status.

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
