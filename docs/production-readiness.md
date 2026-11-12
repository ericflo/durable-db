# Production readiness

**Release decision: not ready for production.** v0.23.10 is the latest experimental release. The passing models, cloud tests, and 100,005-entry directory qualification establish a useful foundation. They do not establish safe public multitenancy, an operating envelope, disaster recovery, or a proof of the implementation.

The production release must retain one installable executable, directory-backed laptop development, and Kubernetes deployments on the existing hosts that scale down to one node. A special clock/compute profile is not an acceptable substitute. Cluster data stays authoritative in B2 or S3 through one common storage algorithm, with bounded disposable caches and automatic maintenance. Performance, tenant protection and correctness are joint acceptance criteria.

This document tracks the gates for the first production release. A gate closes only with reproducible evidence tied to a commit and configuration. A design, a passing happy-path test, or a successful deployment is not sufficient. The [verification record](verification-results.md) separates completed experiments from planned qualification.

## Contract to qualify

The initial contract is atomic, revision-checked transactions and ordered scans within one database. Acknowledged writes must survive the loss of every API process and cache. Uncertain writes require identical retries; the recent receipt window is bounded. Cross-database transactions are outside this contract.

Current per-database limits are 256 MiB, 1,000,000 keys, with smaller request and rewrite budgets. Large database counts and high active throughput are different requirements. The directory has been tested at 100,005 entries with sequential insertion, periodic collection, pagination, and an uncached reopen of every head. Concurrent HTTP provisioning and collection at that population, and a declared active workload, remain capacity gates. This synthetic-provider result is not a cloud capacity guarantee.

Total databases, active databases, peak read/write arrival rates, value sizes, contention, and p99 latency targets must be recorded before choosing a production topology. Measure B2 first, then AWS S3, using the same implementation and workload. Include physical object versions, provider calls, and bandwidth when estimating cost. A laptop-to-cloud measurement is not a regional Kubernetes SLO.

The first production release needs automatic, independently stored backups with a declared backup interval, retention bound and measured restore time. Continuous point-in-time recovery and a 30-day history are optional later features, not requirements of the original brief. Neither is currently implemented or promised.

## Remaining production gates, in order

The critical path is qualifying clustered recovery and performance. The native cloud cutover is implemented; membership churn, collection and provider failures must now meet a declared operating envelope. A release number is not a readiness measure. Close usable end-to-end capabilities, use focused checks during development, and run full qualification when a capability is ready.

Sizes describe remaining engineering scope: **medium** means a few focused implementation and validation changes; **large** means substantial integration and a failure campaign. They are estimates, not calendar delivery promises. Gate 1 now qualifies the integrated native cloud CLI, DNS membership and bounded residency on the existing hosts.

| Order | Gate and rough size | What must be true for 1.0 |
| --- | --- | --- |
| **1** | **Native clustered serving — medium to large** | v0.23.10 recovers all 553 / 709 acknowledgements in two existing-host B2 peer-isolation campaigns; all six recovery targets and 36 administrative probes pass. Only one of four writers progresses during each two-way hold. Next make placement converge under partial failures, repeat rolling/scale-down and host-loss qualification with the batched path, and qualify worst-case memory and funded administration during churn. The earlier broader campaigns still have unmet recovery targets. [Measurements and failed runs](verification-results.md). |
| **2** | **Tenant isolation and lifecycle — medium to large** | Finish charging administrative work and retries, reduce unused tenant-credit loss across handoffs, bound creation/request/egress costs across replicas, and prevent an exhausted tenant from stopping funded neighbors' ordinary operation. Verify credential rotation/revocation, tenant deletion and data isolation. Existing logical quotas and durable admission/reserve funding cover only part of this. |
| **3** | **Automatic maintenance and recovery — medium** | Bound growth of every physical object class, including registrations and catalog headers; qualify large-image cleanup and concurrent readers. Schedule bounded backups to independently protected storage, recover interrupted jobs and demonstrate restore after source loss. The retained checkpoint/export primitives already exist. |
| **4** | **Performance and autoscaling qualification — large, plus a 72-hour run** | Declare workload and latency/throughput targets, then measure the native serving path on B2 and AWS S3 using the same algorithm. Exercise Pi and web-app workloads, contention, provider failures, scale up/down, rolling replacement and whole-host loss. Check histories, memory, object growth and cost throughout a sustained run. Earlier shared-engine tests do not qualify the replacement. |
| **5** | **Composed correctness and security review — large** | Connect the TLA+ ownership, journal, transaction, admission, retention and collection arguments under explicit provider/platform assumptions. Keep bounded checks and negative controls small; verify the implementation's corresponding boundaries with adversarial tests. Resolve independent protocol and tenant-isolation review findings. Do not present bounded exploration as an unbounded proof or as a proof of Rust. |
| **6** | **Operational release — medium** | Verify installation, supported-version upgrades, interruption/rollback or export/restore, alert delivery and operator recovery procedures on the existing Kubernetes hosts. Keep dependency audits, kache/Woodpecker builds, checked release artifacts, README and Pages current. Publish the supported operating envelope and remaining exclusions. |

The next engineering targets for the existing four-writer failure fixture are
**60 seconds for every writer to resume acknowledged/read-back progress after
single-process loss or a replica change**, and **120 seconds after a cold API
process becomes ready following total compute loss**. These are targets to test,
not achieved SLOs. Measure from fault injection or readiness to each writer's
progress, separately from budget inspection and diagnostic collection. Provider
outage recovery starts when connectivity is restored; the outage duration must
also remain visible. Broader production targets still need a declared workload,
region, provider conditions and repeatability.

The current harness retains its outer 180-second identical-request retry limit
and 240-second readiness/progress checks, with two further verified readbacks
from every writer before each next fault. Passing those ceilings alone does not
meet the engineering targets. New reports record controller-monotonic upper bounds from the fault command
(or cold readiness) to two further readbacks per writer after readiness. They
include readiness and polling delay, exclude subsequent budget/diagnostic work,
and report target attainment separately from campaign success. Earlier
phase-to-phase durations include diagnostics and are not direct measurements
of the new targets.

Gate 1 now follows [revocable writer permits](design-owned-partitions.md#revocable-writer-permits): fresh per-writer object validation, permanent revocation before journal recovery, no bounded-clock assumption and no bucket-wide conditional update for ordinary requests. The ownership, routing, layout and residency models establish separate bounded properties; their composition and implementation correspondence remain open. The integrated CLI has completed per-writer handoff, provider-interruption and cold-replacement checks on B2. Repeatability, host failures and declared latency/concurrency targets remain decisive checks. Restricted-profile and ClockBound findings remain [fallback evidence](design-owned-partitions.md#serving-authority-remains-the-central-decision); they do not narrow the required hosts.

Gate 5 starts with every protocol change and reviews the integrated result; it is not a postponed proof-writing phase. The [verification record](verification.md) and [results](verification-results.md) retain the detailed checked bounds, negative controls, provider observations and unfinished obligations. Existing evidence includes native local recovery, native HTTP B2/MinIO tests with revocable permits and independent partitions, historical clock experiments, older-engine Kubernetes/physical-host campaigns, and standalone release tests. Those scopes remain distinct.

## Scope that can wait until after 1.0

These are recommended deferrals within the original brief, not claims that they are implemented:

- **Continuous PITR, a 30-day history and arbitrary historical queries.** Keep scheduled retained backups, bounded retention and tested restore in 1.0.
- **Additional binary platforms.** Additional architectures and operating systems may follow the existing Linux x86_64 binary/local-directory path. Supporting the existing Kubernetes hosts, including pause and replacement safety without special clock hardware, cannot be deferred.
- **Exposing key/range validation in the public API.** Keep the existing per-database revision transaction contract initially; independent databases must still scale and meet the declared performance target.
- **A complete mechanized proof of the Rust program.** The brief calls for a TLA-based correctness argument from the start. Composed protocol reasoning, explicit assumptions, negative controls and implementation validation remain gates; full source-language refinement is separate research work.
- **Optional packaging and presentation breadth.** Extra package-manager channels, a hosted dashboard and additional deployment examples can follow. Keep the single-file installer, dependency checks, usable documentation and basic operational tooling.

Correctness, meaningful performance, one-node/laptop operation, shared B2/S3 storage semantics, tenant protection, automatic maintenance, Kubernetes scaling, and clear docs remain required. None is traded away to label a release production-ready.

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

This is a closed-loop baseline: each worker waits for its preceding operation. It does not measure queue growth under a fixed arrival rate, establish a p99 SLO from a small sample, measure provider billing, or replace an open-loop overload test and soak campaign. The first B2 run [failed its progress gate](verification-results.md#b2-contention-baseline-release-gate-failed); that older serving path remains a baseline for the native replacement.
