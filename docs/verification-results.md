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


## Monitoring capability and control admission

An expiring metrics-only credential now authorizes only `GET /metrics`. HTTP tests deny database info/read/write/create, explicit collection, and token minting with that credential. Cross-key signatures, expired correctly signed credentials, and attempts to reinterpret metrics capabilities as database tokens are rejected. A 4,097-principal HTTP test fills the tenant admission table and verifies that the next tenant is rejected while administrator and monitoring access remain available. This fixes a table-capacity denial of service; the global request semaphore remains shared and is not a reserved control-plane capacity guarantee.

Formatting, clippy, all four HTTP tests, and the credential-expiry test pass. A separate executable-level check mints the credential with the CLI, rejects conflicting scope flags, scrapes a real HTTP server, denies token minting, and passes `promtool check metrics` against the actual response. No signing key or capability appears in the metrics response. Notification delivery, metrics credential renewal automation, and overload drills remain qualification work.


## B2 rolling compatibility and partial-run recovery

The [B2 mixed-version campaign](evidence/mixed-combining-b2-2026-11-08.json) uses the previous static Woodpecker binary from `058e2c3` and the new binary from `b58c909`, with zero memory/disk caches and maintenance every 60 seconds. After an old-only baseline, three processes share writes across old/new versions, one process is killed during a request, the exact interrupted request is recovered, and new-only writes continue. All 109 writes complete. Removing every process and cold reopening independently through each version verifies all 70 databases. The run retains eight HTTP `409` responses, seventeen `503` responses, and one transport error; recovery uses bounded identical retries. It is a compatibility/crash smoke for this pair on B2 `us-west-002`, not a sustained performance, AWS, partition, or HPA result.

Two earlier failed attempts are retained. The [first attempt](evidence/mixed-combining-b2-failed-2026-11-08.json) exhausted its recovery budget during the old-only baseline with three writers sharing one administrator allowance and maintenance every five seconds. Its older harness did not preserve enough expected state for a complete recovery check. A [second attempt](evidence/mixed-combining-b2-admin-limit-2026-11-08.json) serialized the old baseline and reduced maintenance frequency, but still shared the administrator credential across three mixed/new workers; the third worker exhausted retries while the others continued. This exposed a workload configuration problem: all administrator requests correctly share one per-principal allowance, which was two concurrent requests here. The passing campaign uses distinct database-scoped credentials for application writes/reads. The harness now records started, failed, and unstarted writes per phase, retains synthetic namespace and value hashes, and attempts recovery even after a failed phase.

A separate [fresh-process recovery check](evidence/mixed-combining-b2-partial-recovery-2026-11-08.json) verifies all 70 databases from the second failed run against its retained hashes/revisions: all 97 acknowledged writes remain, no mismatches occur, and the one uncertain write is absent at observation. This does not turn the failed progress test into a pass or prove that an arbitrarily delayed request can never arrive. All campaign processes were stopped before deleting the disposable bucket and revoking its scoped workload key.


## Durable database authorization

The authorization model was checked before implementation: 472,151 distinct states and four negative controls. The engine now authorizes against the same manifest it uses for info, data reads/writes, and receipt recovery; a missing-page read restart repeats authorization. Explicit activation moves a database to format 2, with individual token IDs, a fresh incarnation, a monotonically increasing generation, 128 revocation slots, and 32 separate policy receipts. Data revisions stay unchanged. Creation remains legacy-compatible until activation. Overlapping signing-key rotation, deletion/recreation, and audit delivery remain open.

All 52 Rust tests pass, including six authorization tests and the publication-helper revocation test. The 10,005-database regression passes separately in release mode (43 seconds), with the same assertions used in debug builds. Adversarial cases cover activation and revocation during a prepared write, a lost policy response, exact replay returning its original result after later policy changes, receipt eviction refusing reapplication, revocation-set exhaustion/rotation, cross-database denial, malformed manifests, and restart after a collected page. A proposal submitted by a disappearing caller can be invalidated by revocation; later helpers cannot publish it. A different database's helper can publish an administrative policy proposal and recover its receipt.

A [real old/new executable test on versioned MinIO](evidence/policy-upgrade-minio-2026-11-08.json) opens format-1 data with the new binary, activates policy, checks that the old binary refuses info/read/write/collection for that database while continuing to write a legacy database, revokes a freshly issued token, kills all processes, and verifies cold downgrade refusal plus new-binary data/revocation/receipt recovery. Both processes use zero caches and five-second maintenance. The report includes binary hashes and every status count. This is a compatibility smoke for the tested pair; it does not prove arbitrary rolling deployments or availability under partition.

The same executable compatibility test also [passes against a fresh real B2 bucket](evidence/policy-upgrade-b2-2026-11-08.json): activation, old-format refusal, token revocation, forced all-process loss, cold data recovery, and original policy receipt replay. It uses the same zero-cache configuration and five-second maintenance. All 32 upstream Pi cases and the HTTP benchmark recovery fixture also pass after the authorization changes. The disposable B2 bucket and its scoped key are removed after qualification.


## Idempotent protected creation

The creation model passed all 1,177 states and four required negative controls before implementation. Protected PUT creation atomically installs format 2, the initial access policy, and a permanent creation receipt. Exact retries recover the original zero-usage info and incarnation after later data writes and generation changes. An existing name without the matching receipt never establishes ownership. Legacy POST creation remains explicitly separate for compatibility.

All 57 Rust tests pass, including the unchanged 10,005-database test in release mode (44 seconds). Tests lose the creation acknowledgement, force identical and competing creators to race, recover an abandoned proposal through another database's helper, corrupt stored receipt fields, and recover the original result after 130 data commits and 34 policy changes plus collection. Real HTTP tests check administrator-only creation, legacy-token denial from the first published state, scoped-token access, and exact/mismatched retry responses.

The expanded executable compatibility campaign passes on [versioned MinIO](evidence/protected-creation-minio-2026-11-08.json) and [real B2](evidence/protected-creation-b2-2026-11-08.json). Both retain the migration/refusal/revocation checks and add protected creation, original-info replay after data writes, and replay after killing every process. Reports record the tested binary hashes and status counts. The task-owned B2 bucket, 16 physical versions, and scoped key were removed afterward. All 32 upstream Pi cases now pass on protected databases using freshly minted database-scoped application credentials; the HTTP benchmark recovery fixture also passes. These checks do not close the wider lifecycle, backup, cost-isolation, or sustained-load gates.


## Indexed Pi storage and Session fencing

After checking `PiSession.tla`, the indexed adapter replaces journal reconstruction with bounded record and secondary-index lookups. Documents have address/incarnation and base/revision indexes, while latest-only content is separate from scan metadata. Chunked values and their indexes publish in the same database transaction. Authoritative owner UUIDs fence Session replacement independently of API-pod membership. Limits and the separate reference adapter are documented in [clients](clients.md).

All 74 JavaScript HTTP cases pass: the 32 upstream Pi 1.5.0 cases against each adapter, plus ten independent indexed-adapter tests. The first conformance run passed 63/64 and exposed handling of the largest representable ID; the allocator now stores an exact exhausted sentinel while consistently refusing further minting. The final run passes the full suite and benchmark recovery fixture. Tests run on protected databases with scoped credentials.

The fault tests verify opening a 300-entry history with one metadata read and no scan/replay; 300 KiB chunked record replacement and chunk reclamation; a paused writer losing to takeover; stale reads/close failing to alter a new owner; a real lost-response recovery after 130 unrelated commits evict the database receipt; provider failure during later-batch validation publishing no prefix; snapshot-read restart; concurrent close callers waiting for release; oversized staging causing no publication; callback failure preserving durable data; required document checkpoints before exhausting replay bounds; and metadata-only scans of large latest documents followed by content reclamation at retirement. The tests are grouped into ten cases. They do not qualify cloud performance, arbitrary crashes, every input, or a production migration from the reference format. Cloud qualification follows separately.


## Indexed Pi on B2 and complete process loss

The indexed adapter from `4101076` passes all **32 upstream Pi 1.5.0 conformance cases** against a disposable real B2 `us-west-002` bucket. The reference adapter's 32 cases were intentionally skipped in this cloud run; they are not counted as passing cloud cases. The server has zero caches and automatic maintenance every 60 seconds. The [sanitized report](evidence/pi-indexed-b2-2026-11-09.json) records every selected case, duration, source commit, server binary hash, and the recovery fixture hash. The run lasted about 21 minutes, including recovery; it is not a performance benchmark.

A separate fixture leaves Session ownership claimed with conversation/entry data and both rewindable and latest-only documents. The launcher SIGKILLs every server process, reopens against B2 with empty caches, confirms ordinary reopen refuses the stale owner, and performs explicit takeover. Eight checks cover the root and entry, historical and current document values, latest-only content, next ID, increasing commit sequence, and an ordered scan after a new durable write. All pass. The bucket's remaining 124 physical versions were deleted, the bucket removed, and its scoped workload key revoked afterward. This closes that bounded cloud conformance/crash-recovery experiment, not the broader Pi Harness integration, long-running workload, or Kubernetes gate.


## Signing-key overlap and administrator migration

Current source builds support a bounded four-key verification ring with one active issuer, and `token --admin` now emits an expiring, domain-separated capability containing only expiration. Raw legacy key files preserve old administrator-envelope compatibility; new JSON keyrings disable it by default. Keyring creation refuses duplicates, more than four keys, malformed input, and file overwrite, and uses mode 0600 on Unix. A failed configuration does not quote key values in diagnostics. The [rollout procedure](design-signing-rotation.md) separates binary upgrade, key expansion, issuer promotion, and retirement, and explicitly accounts for stale serving processes, offline issuers, legacy non-expiring envelopes, and clock skew.

The new finite model checks 1,189 states; both premature-promotion and premature-retirement controls produce counterexamples. The local Rust checks pass: 61 regular tests plus the 10,005-database release-mode regression (41.58 seconds). Real HTTP tests run independent zero-cache replicas with different active keys, issue through both, enforce durable per-token revocation across the overlap, then remove every server and reject the retired key on cold restart. Tests also reject expired, forged, cross-domain, and legacy-disabled capabilities. This verifies the implemented transitions in those fixtures, not an arbitrary Kubernetes key-delivery/fencing procedure.

All 74 JavaScript cases and the benchmark identity/recovery fixture pass with the new administrator envelope. The first rerun passed 73/74 and exposed a brittle test assertion: a rejected admission attempt was counted as an additional successful metadata read. The test now counts successful point reads and counts every attempted scan; the adapter behavior is unchanged. A versioned-MinIO compatibility rerun with the older `058e2c3` binary also passes all five protected-creation, policy activation, old-engine refusal, SIGKILL, and cold-recovery checks. Its bootstrap administrator token is deliberately minted by the old binary so both versions can verify it. The new keyring format and administrator envelope require the binary-upgrade order documented above.

The documentation build now rejects links to unpublished Markdown targets and correctly rewrites digit-containing filenames such as `b2.md`. The local link check caught and corrected two previously broken B2 links; private integration planning remains excluded from the public output.


## CPU-driven Kubernetes scaling and injected failures

Woodpecker pipeline 17's static runtime from `2733364` passes a three-node kind campaign using Kubernetes 1.36.4, metrics-server 0.9.1, and external **versioned MinIO**, with 64 MiB memory and 128 MiB disposable disk caches per API pod. Metrics-server's kubelet TLS verification is disabled only in this disposable local test cluster. The shared Kubernetes cluster and its default context were not changed. The [report](evidence/kubernetes-hpa-minio-2026-11-09.json) records the exact image digest, harness hash, CPU measurements, every phase, retries, and limitations; the [acknowledgement history](evidence/kubernetes-hpa-minio-2026-11-09.history.jsonl) contains only synthetic operation IDs and value hashes.

With HPA minimum held at one, real database read/write load drives **1→3→8** ready replicas. While four independent append writers continue, the harness drains a worker through normal eviction/PDB handling, sends SIGKILL to an identified API container process, and blocks one worker's provider traffic for 40 seconds. The firewall counter must show intercepted packets. The HPA remains active and may legitimately adjust replica count during disruptions. After healing, all pending writer requests resolve using their original identities. Removing the workload causes CPU-driven downscaling to one. The controller then removes the HPA temporarily, scales to zero until all API pods disappear, restores one replica, and verifies every acknowledged key from object storage with empty pod caches.

All **285 acknowledged writes** are recovered exactly. An independent pass over the recorded histories verifies unique request IDs, contiguous per-database revisions, ordered non-overlapping invocations for each sequential writer, and every expected synthetic value hash. The report retains admission errors, provider failures, and transport/decode errors instead of dropping them from the totals. This is a short controlled campaign on one physical host and a compatible S3 endpoint; it does not establish independent failure domains, AWS behavior, B2 HPA behavior, a contested-register history, OOM recovery, or a production capacity/SLO.

The [first incomplete attempt](evidence/kubernetes-hpa-first-incomplete-2026-11-09.json) also reached 1→3→8, but was stopped after a drain because the harness wrongly required eight replicas throughout normal HPA adjustments. Its partial evidence remains recorded. The corrected harness waits for the currently requested healthy replica count during faults, while still requiring three and eight for the CPU-scaling assertions. No database failure was inferred from the old harness expectation.
