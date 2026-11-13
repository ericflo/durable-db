# Production readiness

**Latest release: v1.0.1, within the scoped 1.0 release line.** The first production release is deliberately limited to one application or organization trust domain per deployment. Shared hosting of mutually untrusted tenants is a later release. This replaces the broader six-gate release plan; it does not describe those deferred capabilities as complete.

## Supported 1.0 scope

- One static Linux x86_64 executable; directory-backed laptop development and native serving on ordinary Kubernetes hosts, scaling down to one replica. No calibrated clock, special VM profile, external consensus service or persistent API-node disk is required.
- One dedicated B2 bucket per deployment, containing many logical databases. B2 remains authoritative; bounded memory and optional local-disk caches are disposable. The common S3 implementation remains available, with MinIO integration coverage; AWS production qualification is deferred.
- The HTTP API and TypeScript KV SDK: atomic revision-checked transactions, exact retries within the bounded receipt window and ordered snapshot reads/scans within one database. Cross-database transactions are outside the API contract. Existing key, value, request and database limits remain enforced.
- Database-scoped, revocable capabilities, administrator-only provisioning, logical quotas, bounded admission and cumulative upload funding. These protect access and limit work; they are not fair billing or availability isolation between hostile tenants. Keep all databases in one administrative trust domain.
- Automatic checkpoints and physical collection. Upload ceilings require operator funding; deletion does not refund cumulative allowances. Reads, control traffic and egress are not capped by the immutable-upload ceiling.
- Manual consistent export, archive inspection and verified resumable restore into a fresh destination. Pause application writes for a reliable export window, store the archive under independent protection, and verify restoration before routing traffic. No scheduled backup, continuous PITR or restore-time SLO is promised.

Use trusted private networking for peer traffic and TLS at application ingress. Keep signing keys and provider credentials on servers. Do not configure external lifecycle expiration on database objects. Set Kubernetes CPU/memory limits, concurrency and maximum replica count together. In-process cache budgets are not an RSS ceiling. See [deployment](deployment.md), [security](security.md), [recovery](recovery.md) and [API bounds](api.md#errors-and-bounds).

A logical directory has been tested at 100,005 entries; this is not a promise of 100,005 simultaneously active databases. The short cloud campaigns are four sequential writers. No p99 latency, throughput or availability SLO is declared. Capacity-test the intended workload before admitting it; published measurements retain their exact scope and failures.

## Initial 1.0.0 release checks

The checks for this supported scope passed. A data-loss, authorization or bounded-resource failure still blocks a release. A missed engineering latency target remains visible and motivates follow-up work; it is not converted into an SLO or silently removed from the evidence.

1. **Worker-loss durability.** The exact 1.0 candidate recovered all **504 acknowledged writes** after SIGKILL of a whole disposable Kubernetes worker, its return, scale-down and replacement of every API process. All four independent histories pass. The largest worker-loss readback gap is **52.93s**; scale-down is **65.23s**, above the 60-second target. Containerized workers share a host kernel; physical power and rack-loss qualification are deferred. The separate existing-host run recovered **1,848 acknowledgements** and measured a **74.68s** scale-down gap.
2. **Supported boundary.** Woodpecker **384** passed dependency audits, Rust formatting/Clippy/tests, client and native Pi contracts, capacity checks, existing small TLA+ models/negative controls and abstract publication/journal proofs. Those are component checks, not a composed proof of the entire protocol or of Rust. The Pi adapters remain experimental pending broader cloud Harness/upgrade qualification; the supported application interface is HTTP/TypeScript KV.
3. **Checked executable and operations.** The downloaded static archive independently passed protected creation, exact retries, ordered reads, export/restore and kill/reopen with an empty toolchain path. The installer pins the release and verifies its checksum. The operating instructions specify funding, private networking, resource bounds and manual backups. Both campaigns' buckets, keys and namespaces, plus the disposable worker cluster, were removed. Experimental-version migration uses export/restore; mixed-version rolling upgrades are not a 1.0 guarantee.

[Exact release and campaign evidence](evidence/release-1.0.0-2026-11-13.json) records artifact hashes, source, histories, failed timing targets and cleanup. [Verification results](verification-results.md) explain their limits. The initial v1.0.0 runtime matched v0.24.26. v1.0.1 prioritizes requested recovery; the [focused B2 comparison](verification-results.md#requested-partitions-recover-first-2026-11-13) observed a 68.27s scale-down gap versus 111.92s on v1.0.0 in separate short runs, still above the target. These patches do not complete the deferred features or establish an SLO.

## Changes to the previous six-gate plan

| Previous gate | Required for this 1.0 | Deferred |
| --- | --- | --- |
| 1. Clustered serving | Existing-host scaling/recovery and whole Kubernetes worker loss, with histories and cold readback | Exhaustive topology tests, physical power/rack loss, permanent-partition availability and availability SLOs |
| 2. Tenant isolation | Access controls, logical quotas and bounded work within one trust domain | Mutually untrusted tenant hosting, complete administrative attribution, fair shared-maintenance progress and billing isolation |
| 3. Maintenance/recovery | Automatic checkpoint/collection and verified manual export/restore | Backup scheduling/orchestration, continuous PITR and broad large-image retention campaigns |
| 4. Performance | Publish current measurements, costs/bounds and missed targets honestly | The 72-hour soak, AWS production qualification and throughput/p99 commitments |
| 5. Correctness/security | Existing model/proof controls, implementation fault tests and focused release review | Comprehensive protocol composition, full Rust refinement and independent security review |
| 6. Operations | Checked Linux executable/installer, resource settings and recovery procedure | Additional binary platforms and broader upgrade/rollback coverage |

These remain follow-up work, not discarded goals. The intended multi-tenant hosted service must not adopt the single-trust-domain 1.0 scope as evidence that its stronger isolation needs are met.

## Qualification interpretation

[Verification results](verification-results.md) retain passing and failed campaigns. Direct readback windows use the workload's monotonic clock and per-writer counters. They include the largest completed gap and the right-censored unfinished gap at close. Controller bounds include readiness/polling delays and are separate diagnostics; a bound over 60 seconds does not alone establish a 60-second service gap.

The engineering targets remain 60 seconds for each writer to resume after a replica change or single failure, and 120 seconds after API readiness following total compute loss. The harness keeps its outer 180-second identical-request retry limit and 240-second readiness/progress checks. These are fixture targets, not production commitments. Acknowledged data recovery is mandatory even when a timing target is missed.

Native ownership uses [revocable writer permits](design-owned-partitions.md#revocable-writer-permits): fresh object validation and predecessor revocation before recovery, without a bounded-clock assumption. Historical shared-engine and restricted-clock results do not qualify this serving path. [Verification](verification.md) records each current model's bounds, assumptions and unfinished composition obligations.

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
