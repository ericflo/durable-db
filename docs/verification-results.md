# Verification results

The v0.1.0 binary was built from `f431d88c072cc924634126e4b2ba8f1ec30697b0` by Woodpecker on 2026-11-08. The build gate passed Rust checks, all five bounded models and their negative controls, and Pi conformance. Commands and bounds are in [verification](verification.md).

| Check | Result |
| --- | --- |
| Initial singleton-root cluster model | 8,575,596 distinct states; no invariant violations |
| Shared-page cluster model | 36,252,536 distinct states; 208,784,241 generated; depth 32; no invariant violations |
| Shared directory publication model | 8,246,048 distinct states; 61,692,865 generated; depth 21; no invariant violations |
| B2 missing-fence negative control | Missing-manifest counterexample at depth 8 |
| Shared publication fault tests | Ten tests cover publication/collection races, concurrent database creation through splits, metadata admission, missing-index refusal, read-only barrier retries, idle collection, and lost response recovery |
| Unsafe collector negative control | Counterexample found: publish then delete a formerly unreferenced object |
| Full Rust suite | 23 tests passed, including 24 randomized reference-map cases; cloud tests run separately |
| pi-durable 1.5.0 reference adapter | All 32 upstream storage conformance tests passed over HTTP (65 seconds) |
| Documentation site | Desktop/mobile rendering, no mobile overflow, 0→6 pod illustration controls passed |
| Independent HTTP processes | 1→3→1 servers, SIGKILL, all processes removed and reopened without caches; all 36 writes verified against both real B2 and versioned MinIO; format-3 B2 rerun passed with over 70 databases and concurrent maintenance after the retry fix |
| Real S3-compatible endpoint | Format 3 on versioned and unversioned MinIO: commits, receipts, three collection layers, 71 databases across leaf splits, pagination, and uncached reopen passed |
| Real B2 endpoint | Format 3, dedicated bucket/scoped key: five commits, receipts, physical reclamation; 70 additional databases across leaf splits, pagination and uncached reopen; both tests passed in 92 seconds |
| Persistent directory stress | 10,005 databases, three-level tree, ordered pagination, update, full index collection sweep, and every database reopened without caches; passed in 106 seconds in a debug build |
| Directory witness model | 145,037 distinct/generated states, depth 6; wrong-witness negative control finds a counterexample |
| Publication retry model | 44,354 distinct states; 117,886 generated; depth 21; crossing a collector fence triggers the negative-control counterexample |
| Read barrier model | 39,332 distinct states; 276,019 generated; depth 13; negative control detects stale reads |
| Native HTTP wire test | Stale observation rejected by read barrier; exact version deletion verified |

The static Linux x86_64 artifact passed checksum verification and standalone local create/commit/read with an empty environment and no Rust toolchain on PATH. Kubernetes HPA bounds 1→3→1 and forced last-pod deletion passed on real B2: all 36 keys and revision 36 were recovered. The phases reached revisions 12, 24, and 36 at 23:01:09, 23:02:13, and 23:02:50 UTC. The cluster lacked resource metrics, so this is not CPU-load-triggered autoscaling qualification. No cloud latency or Kubernetes production-readiness claim follows from these tests.

## Production hardening audit (2026-11-08)

The first hardening pass adds per-database concurrency admission, separate administrator admission, manifest/page structural validation, and expected-digest checks for memory-cache hits. All 27 Rust tests pass, including cancellation and adversarial stored-state cases. All 32 Pi 1.5.0 conformance tests pass with the updated test tooling. After updating the object-store/XML/TLS dependencies, versioned and unversioned MinIO qualification pass again. Both real-B2 integration tests also pass (175 seconds on the bucket already used for the contention experiments), including receipts, physical reclamation, directory splits, and uncached reopening. The publication algorithm and storage format are unchanged. These hardening changes are on main; the original v0.1.0 downloadable artifact predates them.

RustSec found two `quick-xml` denial-of-service advisories in the previous lockfile ([RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html), [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html)) and an unused default-feature path to unmaintained `atomic-polyfill`. Updating the object-store client and XML parser, and disabling unused postcard defaults, removes those findings. cargo-audit 0.22.2 reports zero vulnerabilities or warnings against advisory database commit `7a40350e1b8ba5b656d909f88222a8059a83d88d`. The client test tools and documentation builder also had npm audit findings; their updated lockfiles report zero vulnerabilities. Fresh Rust and npm audits are now required Woodpecker gates outside Docker build caching. This is an advisory-database result, not an independent security review.

## Publication contention changes (2026-11-08)

The next source changes safely rebase staged publications across unrelated database writes, seal bounded groups before sharing a remote read barrier, and cache immutable manifest bytes within the existing cache budget. The storage format and provider bindings are unchanged. All 33 local Rust tests pass, including the 10,005-database test (139 seconds), rebase/collector interleavings, lost-acknowledgement recovery, delayed-barrier responses, queue saturation/cancellation, and stale cached manifests. Versioned and unversioned MinIO tests pass again. All seven bounded models and nine negative configurations pass their expected checks; the new rebase and observation models explore 195,642 and 25,194 distinct states respectively. See [model bounds and obligations](verification.md).

Cloud load qualification remains open. These changes do not close the failed performance gate below or establish an operating envelope. The original v0.1.0 release artifact predates them.

## Initial local latency baseline

One release-build run on an AMD Ryzen AI Max+ 395 laptop, local filesystem with fsync, 1 KiB values, sequential operations. Other development work was active. This is an initial microbenchmark, not cloud performance or a throughput claim.

| Operation | Samples | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Durable local write | 200 | 13.962 ms | 22.998 ms | 25.913 ms |
| Cached local key read | 2,000 | 0.313 ms | 0.346 ms | 0.486 ms |

Reproduce with `cargo run --locked --release --example bench` using the pinned kache wrapper. The results expose an optimization target: synchronous publication and page rewriting currently dominate small writes.

## B2 contention baseline: release gate failed

On 2026-11-08, three release-build processes on the same Ryzen laptop used B2 `us-west-002` over a residential Internet connection. Each had 64 MiB memory cache, no disk cache, concurrency eight, and a 60-second maintenance interval. The shared directory held 70 synthetic databases with one 1 KiB value each. This is a closed-loop diagnostic, not a regional Kubernetes SLO or a statistically robust p99 estimate. Setup is excluded; retry and backoff time is included. The binary hash and individual samples are retained in the [failed-run JSON](evidence/b2-contention-2026-11-08.json).

| Phase | Planned / started / succeeded / failed / not started | Successful ops/s | All-started p50 / p95 / max |
| --- | --- | --- | --- |
| One writer | 20 / 20 / 20 / 0 / 0 | 0.545 | 1,691 / 2,726 / 4,182 ms |
| One reader | 20 / 20 / 20 / 0 / 0 | 9.120 | 91 / 197 / 224 ms |
| Four writers, distinct databases | 80 / 39 / 36 / 3 / 41 | 0.341 | 4,397 / 38,939 / 45,226 ms |

The concurrent write phase received 68 HTTP `409` responses and 25 HTTP `503` responses. Three workers exhausted their 12-attempt budget. The harness stopped the remaining phases and exited unsuccessfully; the four-reader phase was not run. The initial harness did not perform its final verification after a failed phase. A separate fresh process with zero memory cache subsequently read all 70 databases: contents and revisions matched the 56 acknowledged measured writes plus seeds, with no mismatches; the three unacknowledged writes were not present at those reads. This checks the final observed state, not a complete linearizability history. The [recovery report](evidence/b2-recovery-2026-11-08.json) retains those counts. The harness now performs final-state verification after a failed measured phase, including whether an uncertain write became visible; a simulated lost-acknowledgement test checks that failed operations remain failures in its latency and throughput report.

A diagnostic rerun delayed maintenance for 24 hours, retained the existing directory population, added four databases, and ran four workers for ten writes each. All 40 writes completed, but required 59 conflicts and one service-unavailable response. Successful throughput was 0.543 operations/second; write p50/p95/max were 2,259/22,217/22,534 ms. All 40 reads completed at 9.190 operations/second, and the four active databases verified afterward. Sanitized server logging identified one native B2 HTTP `503`; it does not retrospectively identify every `503` in the first run. See the [isolation-run JSON](evidence/b2-isolation-2026-11-08.json). Delaying maintenance is a diagnostic control, not a deployment recommendation.

These runs establish a publication-progress/performance blocker. They do not isolate every source of latency or constitute a controlled attribution of all failures to maintenance. Three API processes did not achieve acceptable concurrent write performance in this workload; process count stayed fixed while writer concurrency changed. The next design must address competing publications, repeated uploads, and maintenance interference together, with a model before implementation. Increasing retry limits or removing the read barrier would not close this gate.

## B2 rebase diagnostic

A rebase-only candidate was tested with the same 70-database, three-process workload and 60-second maintenance interval in a new dedicated B2 `us-west-002` bucket on 2026-11-08. This candidate predates the grouped observations, immutable-manifest cache, and reduced idle-maintenance work. The [raw report](evidence/b2-rebase-2026-11-08.json) records its binary hash and source base. All measured operations completed and all 70 databases verified afterward, but latency and throughput remain unsuitable for a production performance claim.

| Phase | Successful / planned | Successful ops/s | p50 / p95 / max |
| --- | --- | --- | --- |
| One writer | 20 / 20 | 0.256 | 3,607 / 7,989 / 9,575 ms |
| One reader | 20 / 20 | 1.508 | 298 / 2,201 / 2,257 ms |
| Four writers | 80 / 80 | 0.526 | 4,984 / 14,510 / 31,250 ms |
| Four readers | 80 / 80 | 4.992 | 219 / 2,451 / 6,394 ms |

The four-writer phase required 11 HTTP `409` and four HTTP `503` retries. This is improved completion in one short experiment, not a controlled demonstration of its cause, a statistical latency comparison, or closure of the performance gate. Sanitized logs again show native B2 `503` responses; their cause was not established. The task-owned bucket's 380 remaining physical versions were removed and its scoped workload key revoked after the run.

## Grouped reads and publication profiling

Commit `b12d2ae` adds sealed observation groups, immutable-manifest caching, and shared idle-maintenance observations to the rebase change. The same three-process, 70-database workload completed in a fresh B2 bucket with maintenance every 60 seconds. All 100 measured writes and 100 measured reads completed, and all 70 databases verified. The four-writer phase reached 0.724 successful operations/second, with p50/p95/max 3,462/12,320/30,927 ms and ten HTTP `409` retries. Four readers reached 3.624 operations/second, with two HTTP `503` retries. These short runs do not demonstrate a stable production envelope. The [complete report](evidence/b2-grouped-2026-11-08.json) includes every sample and the single-worker phases.

Commit `d889213` adds fixed-cardinality operational metrics. A diagnostic run delayed maintenance for 24 hours, added four databases to the same directory (74 total), and used four workers for ten writes and ten reads each. All operations and final-state checks passed, but writes still achieved only 0.687 operations/second and reads 4.074. There was one HTTP `409` retry. See the [workload report](evidence/b2-profile-isolation-2026-11-08.json) and [per-process metric snapshots](evidence/b2-profile-metrics-2026-11-08.json).

Those snapshots cover each process's lifetime, including startup, setup, and final verification, rather than only the measured phases. Aggregated across the three processes, 322 immutable PUT calls averaged 362 ms; 208 authority replacements averaged 484 ms, including 81 definite conflicts. There were 65 path rebases. Sealed read groups served 93 members through 76 barriers. SDK-internal retries are included in a call's latency but are not individually counted. These measurements motivate combining pending publications and reducing repeated path uploads; they do not establish that maintenance is the only bottleneck.

All 35 local Rust tests pass after the metrics changes, including cancellation accounting, HTTP label privacy, and the 10,005-database stress test. Versioned and unversioned MinIO qualification passes again. The five initial Prometheus alert rules pass synthetic firing/recovery tests with pinned `promtool` 3.16.0; this does not test real notification delivery or a production recovery drill.
