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

The real HTTP metrics endpoint also passes `promtool check metrics`. Both B2 integration tests pass again (124 seconds) after correcting two assumptions in the shared-bucket test harness: creates may receive a definite conflict from another test's collector, and another collector may have already removed the manifests whose deletions a test expected to count. The initial two runs failed those assertions. Creates now retry only definite conflicts within a fixed budget; reclamation checks the resulting object namespace over bounded collection passes rather than attributing work to a particular collector. Unknown outcomes still fail these integration tests visibly. After qualification, all 348 remaining physical versions in the task-owned bucket were deleted, the bucket removed, and its scoped workload key revoked.


## Publication combining qualification

The format-3 combiner submits durable proposals that another replica can publish, bulk-copies directory paths, and reclaims proposals through bounded optional cleanup plus the fenced collector. All 41 Rust tests pass, including the 10,005-database regression (285 seconds in this debug run), 65 abandoned proposals across bounded groups, lost proposal and group responses, competing targets, and collection invalidating a loaded selection. All eight bounded models and thirteen negative configurations pass. The new model covers 13,049,720 states with role-preserving symmetry and 42,716,288 without symmetry; [bounds and remaining obligations](verification.md#cross-replica-publication-combining) are explicit. All 32 upstream Pi cases and the benchmark recovery-accounting fixture pass.

Versioned and unversioned MinIO qualification passes. The first attempt exposed a missed S3 physical-deletion namespace check for proposals; it was corrected before the passing runs. A separate [mixed-version experiment](evidence/mixed-combining-minio-2026-11-08.json) runs the previous Woodpecker static binary from `058e2c3` alongside the new candidate, with no caches and maintenance every five seconds. It completes 109 writes through old-only, mixed, and new-only phases, recovers an unknown request after SIGKILL with an identical receipt, removes all processes, and verifies all 70 databases through both old and new cold reopen. The report retains 235 admission rejections, one conflict, and one transport error. This qualifies that tested pair on versioned MinIO; it is not AWS, B2 mixed-version, HPA, or arbitrary-upgrade qualification.

The same candidate binary (`09e190e145b72a6fa9f3b9f287c6788c1be043eeaf195608a2aaaf04dc88be65`) was measured against a fresh B2 `us-west-002` bucket with the existing three-process, 70-database laptop workload, 64 MiB cache per process, and maintenance every 60 seconds. All 100 writes, 100 reads, and 70 final database checks passed. The [workload report](evidence/b2-combining-2026-11-08.json) retains every sample.

| Phase | Successful / planned | Successful ops/s | p50 / p95 / max |
| --- | --- | --- | --- |
| One writer | 20 / 20 | 0.235 | 2,169 / 4,352 / 40,313 ms |
| One reader | 20 / 20 | 4.572 | 79 / 1,085 / 1,269 ms |
| Four writers | 80 / 80 | 1.051 | 2,946 / 6,284 / 9,033 ms |
| Four readers | 80 / 80 | 2.854 | 1,141 / 3,351 / 3,566 ms |

The write phases required four and ten HTTP `503` retries respectively; the four-reader phase required three. Whole-lifetime [metric snapshots](evidence/b2-combining-metrics-2026-11-08.json), including setup, maintenance, and verification, show 275 acknowledged proposal uploads, 208 acknowledged group publications with 249 selected members (1.20 members/group), and 76 failed-CAS rebuilds. There were 1,146 immutable PUT calls averaging 328 ms and 1,566 authority replacements averaging 286 ms, including 221 conflicts. Fifteen native authority reads failed. These totals cover a different workload/window from the earlier isolation profile and must not be treated as a controlled per-transaction comparison.

The short concurrent throughput result improves on earlier measurements, but **the production performance gate remains open**. Single-writer latency, provider retries, tail latency, and amplification are still substantial. A separate real-B2 integration run then passed receipt/reclamation/reopen checks but failed the directory-creation test on a native metadata HTTP `503`. That failure is retained in the qualification record; bounded read-only provider recovery is the next correction. No soak, regional SLO, billing, or independent proof claim follows from this campaign.


## Bounded B2 observation recovery

After the native metadata HTTP `503` failure above, bucket/version listings now have three bounded attempts under a ten-second deadline. Startup discovery uses the same path. A wire test injects two `503` observations, then a stale observation, and checks that the conditional barrier still rejects stale state. It separately commits a metadata update but returns `503`: exactly one update request is sent and the caller receives uncertainty. Exhausted reads stop after three attempts. Another test checks numeric `Retry-After`, a non-JSON gateway body, permanent rejection, and refusal to shorten a delay beyond the local budget. Formatting, clippy, and these provider tests pass.

Both real-B2 integration tests then pass in 151 seconds, including 70 database creations, splits, collection, receipts, and uncached reopening. One create proposal was invalidated by a competing collector and recovered through observation of its uniquely owned test name. The earlier failed run remains recorded. This result does not imply every provider outage is hidden or that the earlier latency measurements improve; those measurements predate this recovery change.
