# Verification results

## Peer failure and raw control observations (2026-11-12, release pending)

Candidate `689cfcf` passes [Woodpecker #231](https://ci.eflorenzano.com/repos/18/pipeline/231/1)
and the independently downloaded static executable checks, but **fails the fresh
B2/Kubernetes campaign**. Two writers exhaust the unchanged 180-second retry
limit during scale-to-six; tenant-budget observation also fails. The stopped-peer
fault is not reached. The partial snapshot records 219 acknowledged/read-back
writes, without a complete cold recovery or history check. This is a failed
availability qualification, not a finding of lost acknowledgements.

The auxiliary metrics locate substantial delay in bucket-metadata updates:
one new pod spent 207.82 seconds across 22 calls, while its 23 list-bucket calls
took 2.36 seconds in total. These measure client-observed request durations;
they do not identify a provider rate limit. Four of six metric reads succeeded;
administrator admission refused the other two. The namespace, bucket, key and
all 554 remaining physical object versions were removed. The failed run remains
in the [campaign evidence](evidence/native-recovery-2026-11-11.json).

The follow-up removes redundant conditional writes from raw partition control
observations. The common manifest reader still validates immutable bytes and
bounds retries; ordinary storage reads retain their freshness barrier. Claims,
activation and public budget status still require exact comparisons. Routed data
responses still require fresh writer-permit validation. No wire or storage format
changes, special clocks or additional coordinator are introduced.

The stale-observation regression fails on the previous implementation. It now
reads an intentionally old partition pointer without an authority write, then
checks that exact validation fails and the revoked owner cannot serve. All nine
partition tests and 329 enabled Rust checks pass, with formatting and strict
clippy. The existing acquisition model already permits historical observations:
117,741 states in its main bound and 30,349 in the three-candidate bound pass,
along with six negative controls. These are bounded protocol checks, not a proof
of Rust or a guarantee of liveness.
The corrected provider-cost assertion requires one raw read and zero updates or
barriers. Versioned MinIO passes 1→2→1→0→1 handoff and cold recovery in 2.61 seconds.
The B2 fixture stops on a single-attempt HTTP 503; its failure remains recorded.
All four provider-fixture buckets, both B2 keys, both MinIO containers and 173
remaining physical versions were removed. A fresh Kubernetes campaign with the
existing bounded identical retries must qualify this correction before release.

## v0.23.5 independent recovery scheduling (2026-11-12)

[v0.23.5](https://github.com/ericflo/durable-db/releases/tag/v0.23.5) packages
`503faaa`, verified by [Woodpecker #228](https://ci.eflorenzano.com/repos/18/pipeline/228/1).
The downloaded static executable passes standalone installation, transactions,
ordered reads, export/restore and forced-stop recovery.

Recovery fills each freed slot immediately, with at most four acquisitions per
process. Membership changes cancel only partitions whose own placement moved;
other recoveries keep their progress. Failed work keeps retry backoff, removed
owners retire promptly, and data responses still require fresh permit validation.
The batch-draining reconciler, whole-pass cancellation and eight-probe limit were
removed; the controller is 20 lines smaller. No wire or storage format changed.

The queued-partition and partial-placement regressions both fail against the old
scheduler. All 325 enabled local Rust checks, formatting and strict clippy pass.
The residency model retains 19 states and two negative controls; layout retains
56 states and two controls. These bounded checks do not prove Rust or liveness
under permanent membership disagreement. CI also passes the client contract,
formal checks and 10,005-database capacity regression.

The **exact release executable** passes the complete B2/Kubernetes campaign on
the existing hosts. All **1,350 acknowledged writes** were read back and recovered,
and the independent checker passes all four sequential histories. Every phase
requires two further verified readbacks from each writer. No deadline was raised.

| Phase, measured between boundaries | Earlier v0.23.3 run | v0.23.5 run |
| --- | ---: | ---: |
| Initial ready node to writer progress | 121 s | 184 s |
| Scale to three | 144 s | 80 s |
| Scale to six | 149 s | 77 s |
| Forced pod loss | 189 s | 113 s |
| Rolling replacement | 165 s | 141 s |
| Scale down to one | 177 s | 134 s |
| Cold replacement after all pods were removed | 127 s | 106 s |

The longest measured write was **123 seconds**, previously 151 seconds. Recovery
improved in this run, but initial setup was slower and waits still reach minutes.
These are observations, not an isolated A/B test or an SLO: placements, histories
and provider conditions differ, and the candidate includes v0.23.4's health fix.
Phase durations include orchestration, diagnostics and readback barriers. Five
steady-memory samples cover 30 pod observations; the maximum sampled RSS is
5,283,840 bytes. They do not measure recovery peaks or worst-case memory. The
run does not qualify AWS, HPA, whole-host loss, unreachable peers retained in DNS,
arbitrary concurrent histories or sustained load.
[Exact source, hashes, configuration, timings and limits](evidence/native-recovery-2026-11-11.json).

The disposable namespace, scoped key, bucket and all 1,247 remaining object
versions were removed. Qualification used the verified CI runtime image by
digest; no separate temporary registry image was created. The [production
gates](production-readiness.md) remain open.

## v0.23.4 local health and complete B2 recovery (2026-11-11)

[v0.23.4](https://github.com/ericflo/durable-db/releases/tag/v0.23.4) packages
`2d62975`, verified by [Woodpecker #224](https://ci.eflorenzano.com/repos/18/pipeline/224/1).
The independently downloaded static executable passes installation without a
toolchain, transactions, ordered reads, export/restore and forced-stop recovery.

Native health checks local retirement state without provider I/O or waiting
behind an exclusive owner operation. Temporary permit-read errors therefore
preserve idle owners instead of triggering unnecessary acquisition; actual data
responses still require fresh permit validation. Unknown journal outcomes still
retire and reacquire. This patch changes no wire or storage format.

The outage regression failed against the previous probe and now passes with
unchanged owner identity and no additional authority writes. Existing tests
check responsiveness during paused admission, revoked-owner refusal and unknown
journal recovery. All 324 enabled local Rust checks, strict clippy, the existing
110-state routing model and five negative controls pass. The obsolete network
health test was replaced, and outdated exhausted-credit deployment advice was
removed. The harness also reads refusal categories from the actual `error` field.

Separately, the **v0.23.3 static release binary**, before this patch, passes the
first complete native B2/Kubernetes campaign with the stronger per-writer gate:
1→3→6→1, forced pod loss, rolling replacement, provider interruption and total
compute replacement. Every writer must complete two further verified readbacks
before the next fault. **1,791 writes were acknowledged, read back and recovered**;
the independent sequential-history checker passes all four histories.

Rolling recovery takes about 165 seconds, scale-down 177 seconds and cold
replacement 127 seconds between phase boundaries, including orchestration and
the readback barrier. Those long pauses remain an availability concern. The
180-second identical-request retry deadline and 240-second rollout/progress
bounds were not relaxed. This single run does not isolate the earlier debug
failure's cause, measure the health patch's cloud effect, establish an SLO, or
qualify AWS, HPA, host loss, peak memory or a sustained workload.
[Exact source, configuration, history hash and limitations](evidence/native-recovery-2026-11-11.json).

The namespace, temporary image, scoped workload key, bucket and all 1,073 remaining
physical versions were removed. Production gates remain open.

## v0.23.3 routing readiness during recovery (2026-11-11)

[v0.23.3](https://github.com/ericflo/durable-db/releases/tag/v0.23.3) packages
`5a69e41`, verified by [Woodpecker #220](https://ci.eflorenzano.com/repos/18/pipeline/220/1)
and its tag pipeline. The controller keeps HTTP routing and administrator refill
reachable during partition recovery or exhausted upload credit. Each database
response still requires recovered ownership and a fresh permit; readiness grants
no data-serving authority. The aggregate placement-readiness scan and flag resets
were removed: 16 controller lines deleted, seven added. Formats are unchanged.

All 324 enabled Rust checks, strict clippy, the routing model's 110 states and
five negative controls pass. The downloaded static executable passes installation
without a toolchain, transactions, ordered reads, export/restore and forced-stop
recovery. Versioned MinIO also passes the strengthened sixteen-partition startup
and zero-credit administrator/cold-recovery fixtures; this does not qualify AWS.

The fresh B2/Kubernetes debug run completes 1→3→6, its short steady phase, forced
pod loss, and rolling replacement plus aggregate write progress in about
**85 seconds**, under the original one-slot policy and 240-second gate. It then
fails one client's 180-second retry deadline during scale-down after provider
interruption: **915 acknowledgements, 912 readbacks**, without final recovered
history verification. This establishes rollout progress, not full writer recovery
or a production SLO. [Exact scope and evidence](evidence/native-recovery-2026-11-11.json).

The harness now requires two new verified readbacks from **every writer** before
advancing to another fault. A focused regression detects the old aggregate-only
check; timeout diagnostics identify read/commit and final HTTP status while
redacting unknown response text. The subsequent static-release campaign above
passes that stronger barrier. The unused rollout-headroom option was removed. Cleanup deleted
the latest bucket/key/namespace and 1,211 B2 versions, plus the MinIO container,
two buckets and 503 versions. The inconclusive prior headroom experiment removed
its bucket/key/namespace and 481 versions before reaching rolling replacement.

## v0.23.2 same-owner acquisition recovery (2026-11-11)

[v0.23.2](https://github.com/ericflo/durable-db/releases/tag/v0.23.2) packages
`49615c5`, verified by [Woodpecker #217](https://ci.eflorenzano.com/repos/18/pipeline/217/1)
and the tag pipeline. Lost claim/activation responses now trigger bounded exact
validation of the same owner. A confirmed fence still requires activation; a
successor stops recovery. No registration or permit creation is replayed. The
existing journal-selection validation loop is shared, and wire/storage formats
are unchanged.

All 324 enabled Rust checks pass, including the real HTTP regression requiring
epoch one after a lost claim reply. The activation-aware acquisition model passes
both bounded instances and six mandatory controls; registration checks still
pass. The independently downloaded static binary passes toolchain-free
installation, transactions, ordered reads, export/restore and forced-stop recovery.

The fresh B2/Kubernetes debug campaign again passes 1→3→6, the steady phase and
forced pod loss, but fails the same 240-second rolling deadline. It captures
1,044 acknowledgements/readbacks and two client retry-deadline failures; final
recovered-history verification and later phases are not reached. Bounded timing
samples show completed acquisitions between 12 and 86 seconds. This does not
establish a cloud speedup or production availability.
[Configuration, timing scope and cleanup](evidence/native-recovery-2026-11-11.json).

The namespace, bucket, scoped key and 611 physical versions were removed. An
image-address error was corrected before the workload started in a separate
empty fixture. Cleanup also removed an obsolete three-node local kind cluster,
its credentials, temporary contexts and the duplicated validation/reporting code.
The following headroom experiment failed at pod-loss recovery before exercising
its new setting; the option was removed. v0.23.3 addresses HTTP readiness.
The production gates remain open.

## v0.23.1 recovery progress fixes (2026-11-11)

[v0.23.1](https://github.com/ericflo/durable-db/releases/tag/v0.23.1) packages
`664ee9b`, verified by [Woodpecker #214](https://ci.eflorenzano.com/repos/18/pipeline/214/1).
Journal selection resolves uncertainty by validating the same incarnation rather
than immediately adding a new owner. Peer changes that leave local placement
unchanged preserve pending recovery. Recovered idle partitions schedule a normal
checkpoint, including zero-frame cuts, so later healthy handoffs stop at a recent
owner. Foreground reads do not require this opportunistic checkpoint. Wire and
storage formats are unchanged. The independently downloaded static binary passes
installation with an empty toolchain PATH, transactions, ordered reads,
export/restore and forced-stop recovery. The release tag pipeline also passes.

All 322 enabled Rust checks, strict clippy and formatting pass. Focused regressions
cover lost replies, late uploads, takeover during selection, paused acquisitions
and seven idle handoffs. The original restart rules fail their regressions. The
existing catalog, registration, placement and resident-budget models retain their
negative controls; these remain bounded component checks, not a composed proof of
Rust or all deployments.

Two fresh B2/Kubernetes campaigns use the same sixteen-partition layout, four
writers, sixty-second collection interval and deadlines. Both reach one, three
and six ready pods, complete the short steady phase and recover from forced pod
loss. Both fail the 240-second rolling-replacement gate. The first captures
1,067 acknowledgements and 1,064 readbacks; the second captures 1,107 of each,
plus one client's 180-second identical-retry failure. Neither reaches final
recovered-history verification, provider interruption, scale-down or cold recovery.
These are debug-image qualification runs, not release-performance evidence.
Scale-up was slower in the second run; no cloud speedup is claimed.
[Exact configurations, hashes, failures and cleanup](evidence/native-recovery-2026-11-11.json).

Both namespaces, buckets, scoped keys, 1,357 physical versions and temporary
image manifests were removed. Two obsolete recovery restart paths and the
site's misleading older-engine fault-test badge were replaced. Clustered
availability and the other [production gates](production-readiness.md) remain open.

## v0.23.0 native cloud CLI (2026-11-11)

[v0.23.0](https://github.com/ericflo/durable-db/releases/tag/v0.23.0) packages
`74a6c83`, verified by [Woodpecker #207](https://ci.eflorenzano.com/repos/18/pipeline/207/1).
The independently downloaded static executable passes installation without a
toolchain, transactions, ordered reads, export/restore and forced-stop recovery.
The public default installer downloads that identical binary; the tag pipeline passes.
It integrates headless-service DNS discovery, native
cloud serving for B2 and S3, and aggregate resident index/validation/journal
budgets. Resident reservations survive retirement and pending I/O. Cloud startup
requires upload ceilings and preserves administrator access before owners are
ready, including a fresh zero-credit namespace. Prior cloud formats require
export with their previous binary and restore into a fresh namespace; local
format 15 remains compatible.

Qualification found and fixed three progress problems: a 429 response could pin
a stale route, a whole-recovery timeout repeatedly abandoned useful acquisition,
and every partition scheduled the same shared directory collector. A further
failure showed an uncertain absent journal upload polling forever. The native
worker now retires in that case; ordinary reacquisition revokes its permit and
selects its durable prefix. A delayed old PUT after the successor commits remains
excluded through another reopen. Absence never proves an abort or authorizes
repeating the original physical PUT.

All 320 enabled Rust checks, formatting and strict clippy pass. The routing,
residency, layout, collection and revocation models retain their negative
controls. Focused Rust mutations detect premature resident release, eager
zero-credit initialization, stale-route caching, a whole-recovery deadline and
indefinite unresolved-upload polling. Restoring serial acquisition also fails the
paused-partition progress regression.

At `14ba978`, the actual debug CLI passes zero-credit bootstrap, refill, three cold
processes, exact receipts and incompatible-layout refusal on B2 in 74.864 seconds
and versioned MinIO in 4.628 seconds. Default sixteen-partition startup passes in
129.345 and 2.027 seconds respectively. Earlier B2 default startup exceeded 240
seconds; two Kubernetes candidates failed readiness/convergence, and a recovery
run exposed the indefinite polling defect. Failed attempts remain recorded.
The first Kubernetes run of that image passes one-to-three-to-six-pod readiness on the existing
hosts, then the steady phase stops with an unclassified HTTP 413 after 874
acknowledged and read-back writes ([partial history](evidence/native-cloud-cli-partial-2026-11-11.history.jsonl)). A separate MinIO reproduction exhausts the
default 64 MiB tenant allowance on its fourth cold owner, with only 300 tiny
appends: unused grants are deliberately lost across ownership changes. The
qualification fixture now preprovisions finite tenant ceilings and records their
monotone counters alongside the unchanged shared physical ceiling. Grant sizing
and default-quota usability remain production work, not claims that larger test
allowances solve tenant isolation. The funded repeat still times out scaling from
one to three pods after 317 acknowledgements/readbacks; two pods were ready and
none restarted. Acquisition now runs in batches of at most four under the same
resident reservations, replacing the serialized loop. The focused paused-time
regression verifies independent progress and cancellation; a fresh cloud campaign
is required. At `74a6c83`, default sixteen-partition startup measures 78.610
seconds on B2 and 1.218 seconds on MinIO; these are single functional runs, not
latency guarantees. All completed disposable runs removed their buckets, keys and containers.
The temporary debug image then reaches one, three and six ready pods, completes
a short steady phase and recovers from forced pod loss. Rolling replacement fails
a client retry deadline after 924 acknowledgements: the fixture still targeted
original pod IPs as they disappeared. The manual routing endpoint is removed;
clients now use the stable Kubernetes Service. This failure does not complete
the recovery-history check or qualify the release image. The subsequent release-image
campaign uses the stable Service and passes one/three/six readiness, the two-minute
steady phase and forced pod-loss replacement. Its rolling update exceeds the
240-second deadline, with a client retry deadline failure too. The partial record
contains 926 acknowledgements and 924 completed readbacks; it is not a recovered
history check. Six steady-phase samples peak at 4.07 MiB RSS per pod, with no
restarts in that phase; this tiny workload does not establish a memory bound.
Provider interruption, scale-down and cold recovery are not reached. The native
cluster gate remains open. All 921 remaining bucket versions, the scoped key and
test namespace were removed. The tested container binary matches the GitHub
release executable by SHA-256.
[Evidence](evidence/native-cloud-cli-2026-11-11.json).

Native Kubernetes scaling and the production operating envelope remain under
qualification. MinIO results do not
qualify AWS S3; component budgets do not establish an RSS bound. The old cloud CLI
serving/maintenance branch and seven obsolete qualification scripts (1,294 lines)
were removed, along with an orphaned publication working-design document. Historical evidence keeps its original engine and deployment scope.

## v0.22.0 membership-driven native placement (2026-11-11)

[v0.22.0](https://github.com/ericflo/durable-db/releases/tag/v0.22.0) packages `9dfd19a`, verified by [Woodpecker #199](https://ci.eflorenzano.com/repos/18/pipeline/199/1). The independently downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs that identical binary. The release tag pipeline also passes. Both 99-test JavaScript/Pi contracts and the optimized 10,005-database regression (199.28 seconds) pass.

The native library now installs an immutable database-to-partition mapping and automatically acquires, recovers and places partitions from membership snapshots. Requests retain the selected local or remote backend through authorization and dispatch. A bounded rotating sweep prevents one failed partition from starving later acquisitions; routine probes preserve readiness.

Independent node transports and caches pass a **1 → 2 → 1 → 0 → 1** compute sequence over real B2 (115.46 seconds) and versioned MinIO (1.67 seconds). Values and exact retry receipts survive complete compute replacement; the still-live predecessor is refused, the unmoved owner keeps its identity, incompatible layouts fail and upload ceilings survive recovery. An earlier B2 attempt failed on HTTP 500 during the final budget-status read; that failure is retained beside the successful rerun. All three campaigns removed their buckets, scoped keys, containers and 447 remaining physical versions. This is supplied-membership HTTP qualification, not Kubernetes discovery, AWS, CLI cutover or an operating envelope. [Evidence](evidence/cluster-placement-2026-11-11.json).

All 310 enabled Rust checks, formatting and strict clippy pass. Six focused controller/TCP checks take 0.29 seconds. The 42-state layout and 47-state routing models pass with five negative controls; five deliberately broken Rust variants are detected. Existing registration, permit and publication/rebase checks also pass. These remain bounded component arguments, not a composed proof of the implementation.

The manual-placement cloud fixture and duplicate live-limit validation were replaced; shared TCP helpers avoid a second server harness. No dependency, collector or storage algorithm was added. Kubernetes membership discovery, aggregate process budgets and the cloud CLI cutover are the next integration step.

## v0.21.0 native owner forwarding (2026-11-11)

[v0.21.0](https://github.com/ericflo/durable-db/releases/tag/v0.21.0) packages `af57066`, verified by [Woodpecker #196](https://ci.eflorenzano.com/repos/18/pipeline/196/1). The independently downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs that identical binary. The release tag pipeline also passes. Both 99-test JavaScript/Pi contracts and the optimized 10,005-database regression (238.68 seconds) pass.

The native library now discovers an advertised owner and forwards one bounded HTTP attempt to its exact incarnation. Live takeover, stale routes, address reuse, lost acknowledgements and token revocation are checked. An uncertain write returns to the caller for an identical retry; it is never silently replayed by the forwarder. Normal tenant policy and fresh permit validation remain on the owner.

All 300 enabled Rust checks, strict clippy and formatting pass. The focused TCP/root group takes 0.10 seconds. The 35-state routing model and two negative controls pass; two deliberately broken Rust variants are detected. Real B2 (51.42 seconds) and versioned MinIO (1.48 seconds) pass the expanded four-server/two-partition fixture at `80841e5`. Both disposable buckets, all 88 remaining physical versions, the temporary B2 key and MinIO container were removed. [Evidence](evidence/owner-routing-2026-11-11.json).

Duplicate journal-selection loops were removed, and the existing cloud HTTP fixture now shares server setup with its gateway. No new dependency, collector or storage algorithm was added. Automatic election, database mapping, placement and cloud CLI cutover remain the next integration gate; this is an experimental library milestone.

## v0.20.0 independent partition control (2026-11-11)

[v0.20.0](https://github.com/ericflo/durable-db/releases/tag/v0.20.0) packages `00637a4`, verified by [Woodpecker #193](https://ci.eflorenzano.com/repos/18/pipeline/193/1). The independently downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer downloads the identical binary. Both 99-test JavaScript/Pi contracts and the optimized 10,005-database regression (195.89 seconds) pass. The release tag pipeline also passes.

The native library now stores independent partition control records in the existing shared B+tree directory. Each partition reuses the native owner, permit, journal, checkpoints, tenant ledgers and collector within an isolated object prefix. Shared upload admission covers control and data; partition ceilings remain additional. Format 6 separates these control records from legacy cloud database manifests. Local format and the cloud CLI serving path are unchanged.

The four focused tests pass in 0.64 seconds; all 295 enabled Rust tests, formatting and strict clippy pass. Real HTTP qualification passes on B2 (52.98 seconds) and versioned MinIO (0.99 seconds): three servers, two partitions, live takeover, checkpoint recovery, exact retries, independent values/progress and shared metadata collection. The final fixture also detects duplicate provider metrics at the virtual partition boundary. All disposable resources were removed. The publication/rebase and registration checks pass. The existing combining model now uses two revision advances instead of three, retaining two targets/two helpers and all four negative controls; its positive run takes 43 seconds.

This reuses the directory and native engine, adds no dependency or replacement storage algorithm, and removes stale timed-controller instructions. Automatic membership, election, routing, placement policy and the cloud CLI cutover remain open. Shared maintenance credit/attribution and cluster failure/performance qualification remain production gates. [Evidence](evidence/partition-directory-2026-11-11.json).

## v0.19.0 native permits (2026-11-11)

[v0.19.0](https://github.com/ericflo/durable-db/releases/tag/v0.19.0) packages `3364728`, verified by [Woodpecker #188](https://ci.eflorenzano.com/repos/18/pipeline/188/1). The independently downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer downloads the identical checked binary. Both 99-test JavaScript/Pi contracts and the optimized 10,005-database regression (197.08 seconds) pass in CI; the release tag pipeline also passes.

The native library now creates write-once positive permits, revokes every unresolved ancestor before recovery, and validates sealed response groups using fresh uncached GETs. It takes no clock and runs no lease renewal. A checkpoint can bound revocation traversal only through an owner that already completed it; a new model negative control detects cutting at an unfinished owner. The collector uses its existing list-before-fence rule to remove unpublished/obsolete permits while protecting the current root.

All 291 enabled local Rust tests, formatting and strict clippy pass. The six focused permit regressions pass in under 0.01 seconds after compilation; deliberately accepting absent permission and revoking only the immediate predecessor both fail. The registration model checks 34,061/281,563 states with four unsafe and two coverage controls; permit, transaction, publication and local-exclusion models pass with their controls. These are bounded component checks, not a proof of Rust or complete cluster composition.

The timed runtime, local logical startup adapter, renewal scheduler and fifteen obsolete timing/delegation CI stages are removed. Historical models, proofs and pinned VM evidence remain reproducible as fallback research; about 130 lines of superseded timing instructions were condensed into that record. Remote format 23 refuses older timed writers; released local format 15 is unchanged. A real v0.18.1 → new binary → v0.18.1 local-directory drill preserves values, revisions and exact retry receipts. Native protected creation now charges the target tenant; policy revocation remains namespace-funded.

Real B2 and versioned MinIO pass live predecessor refusal, two cold reopens, budget preservation, retained export/restore and automatic cleanup at `50b1655` in 202.19/182.44 seconds. For 100 warmed point-and-scan HTTP reads, B2 records 25.50 ms median, 48.70 ms p95 and 80.61 ms p99; MinIO records 2.69/2.97/3.10 ms. Each read uses one fresh permit GET and zero authority calls. These sequential debug samples are not a throughput or latency SLO. A preceding B2 run sampled checkpoint files before publication; the fixture now waits for a completed checkpoint, and the earlier failure and cleanup remain recorded. All four buckets, both temporary B2 keys and both MinIO containers were removed. [Bounds, source hashes, cloud results and cleanup](evidence/native-permits-2026-11-11.json).

 Cloud CLI election/routing, partition placement, provider qualification and the production failure/performance campaign remain gates. This is a native library milestone, not a completed cluster cutover.

## Timing-independent ownership candidate (2026-11-11)

The first production release must support the existing Kubernetes hosts. Revocable writer permits now replace restricted clock profiles as the selected ownership experiment: create a positive permit once, validate sealed response groups with fresh GETs, revoke before selecting the predecessor's durable journal prefix. No bucket-wide conditional update is required on that proposed ordinary path, but an object GET per group remains.

The 856-state one-handoff model passes in under one second, along with seven required unsafe counterexamples and two delayed-event coverage traces. It does not yet prove repeated takeover/placement/collection or change runtime behavior. Provider consistency and latency remain decisive obligations. The old standalone timed model and its eight configurations were removed, its CI stage was replaced, and the design's duplicated clock exposition was shortened; historical clock/restricted-profile evidence remains. [Scope and reproduction](verification.md#candidate-revocable-writer-permits), [evidence](evidence/revocable-owner-2026-11-11.json).

The shared provider fixture also passes at `4f450dc` on B2 in 15.36 seconds and versioned MinIO in 0.36 seconds. Each checks 97 post-revocation negative GETs, recovery of the validated prefix and exclusion of a late suffix, with no shared authority calls on the measured path. B2's 32-sample phases show 32–67 ms median GET latency, a one-second outlier and about 200 validations/second at concurrency 16. This is a provider primitive, not database performance or AWS qualification. All disposable resources were removed. [Measurements, limits and command](verification.md#permit-provider-experiment).

The existing registration/acquisition model now composes durable ancestor discovery with permanent permit revocation. Its two- and three-candidate checks pass at 34,061 and 281,563 states in one/seven seconds, with three unsafe controls and two coverage traces. Revoking only the immediate predecessor fails; a third claimant can complete revocation after an interrupted second claimant. This adds no second controller or proof tree. The runtime still needs the new boundary wired into its existing journal/HTTP path. Local formatting, strict clippy and 302 enabled Rust tests pass; the large directory sweep remains the separate optimized Woodpecker check.

## ClockBound VM boundary rejection (2026-11-11)

The pinned upstream ClockBound V3 client does not qualify our stock QEMU/KVM guest. A short ACPI suspend leaves the disruption marker unchanged and the client reports synchronized time. Waiting until its next owner-visible timestamp is nondecreasing still produces an elapsed interval of −1.31 to 2.73 ms against an external interval of at least 61.15 ms. Thus a backwards-clock guard is insufficient. Ordinary pauses pass; snapshot restore is detected, and the deliberately unguarded client exposes the stale interval.

The experiment uses synthetic calibration to isolate counter/disruption behavior, not an NTP daemon or a production clock source. It reuses the VM harness, adds no database dependency, and changes no runtime protocol, storage format or release. The speculative ClockBound shortcut in the design document was removed. [Scope and reproduction](verification.md#clockbound-counter-and-disruption-experiment); [source hashes and observations](evidence/vm-clock-2026-11-10.json). A qualified platform contract or a different serving-authority mechanism is still required for native clustered serving.

## Unreleased catalog admission (2026-11-10)

Protected creation now pays from the target database’s existing admission budget before journal ordering. The blanket internal-catalog exemption is replaced with target-bound creation admission; policy changes remain operator-funded so tenant exhaustion cannot block credential revocation. No allocator or model is added. Identical pending/committed retries reuse admission, lost grant replies create no usable rights, and a refused tenant cannot leave a shared journal gap. Budgeted native format 22 excludes older writers; local format 15 and the released cloud CLI format are unchanged. This is not yet full administrative-cost accounting: policy changes, shared catalog construction claims and ledger publication work remain namespace-funded.

[Woodpecker #181](https://ci.eflorenzano.com/repos/18/pipeline/181/1) passes the full verification/build step, including the optimized 10,005-database check in 198.24 seconds. The enabled Rust suite passes 302 tests, and the native client contract passes 99 tests with one opt-in skip. The focused regression takes 0.02 seconds. The existing 10,402-state tenant model and all six negative controls pass. Four deliberate Rust faults are detected: omitted creation admission, wrong payer, catalog-name aliasing and tenant exhaustion blocking credential revocation. Byte-exact HTTP fixtures now include creation’s charge and retain their below-budget refusal checks. [Milestone evidence](evidence/catalog-admission-2026-11-10.json).

At `33965e6`, native HTTP qualification passes on real B2 in 201.74 seconds and versioned MinIO in 182.30 seconds. Creation spends 18,596 bytes of the target's allowance; refusal, neighbor progress, durable recovery funding, two cold reopens, retained export/restore and automatic collection pass. Both buckets, all 46 remaining physical versions, the temporary B2 key and the MinIO container were removed. An earlier report-only macro compilation failure was fixed without raising the recursion limit; its empty test resources were also removed. These small-image logical-clock tests do not qualify AWS, physical timing or a performance SLO.

## v0.18.1 metadata retry reuse (2026-11-10)

[v0.18.1](https://github.com/ericflo/durable-db/releases/tag/v0.18.1) packages `8382253`, verified by [Woodpecker #176](https://ci.eflorenzano.com/repos/18/pipeline/176/1). The independently downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs the identical binary. Both 99-test client contracts and the optimized 10,005-database check pass in Woodpecker.

Tenant ledger initialization, replenishment, grants and checkpoint funding now share one conditional publication path. Ordinary control updates reuse staged metadata; competing ledger updates and collection require rebuilding. Lost replies never grant another attempt, malformed formats fail closed, and eight unsuccessful comparisons against unchanged scope stop without further uploads.

The focused budget group passes in 0.68 seconds. Paused initialization, refill and grant updates retain exactly six metadata objects across three control conflicts. A deliberate reupload variant fails the first case with four objects instead of two. Three additional Rust negative controls detect missing directory, collection-fence and format checks. The existing 44,354-state publication-retry model and its unsafe control pass; no new model or dependency was added. The separate single-record retry path was removed.

Native HTTP qualification passes on B2 (201.60 seconds) and versioned MinIO (182.27 seconds): tenant refusal and neighbor progress, automatic journal/table/metadata collection, two cold reopens with preserved accounting and receipts, and retained export restored after source collection. Both disposable buckets, all 46 remaining physical versions, the temporary B2 key and the MinIO container were removed. These small-image tests use a logical clock and stopped predecessor servers; they do not qualify physical timing or AWS.

Storage formats and admission policy are unchanged from v0.18.0. This reduces administrative retry amplification; complete tenant attribution, scheduled backups and qualified cluster timing remain open. [Milestone evidence](evidence/metadata-retry-2026-11-10.json).

## v0.18.0 durable maintenance funding (2026-11-10)

[v0.18.0](https://github.com/ericflo/durable-db/releases/tag/v0.18.0) packages `8f0b282`, verified by [Woodpecker #172](https://ci.eflorenzano.com/repos/18/pipeline/172/1). The independently downloaded static executable passes standalone transactions, ordered reads, backup/restore and forced-stop recovery. The public default installer installs the identical checked binary; the tag pipeline also passes. Both 99-test client contracts pass in Woodpecker; the optimized 10,005-database check passes in 201.18 seconds. The independent website redesign is preserved, and the publisher now rebases and preserves separate site edits.

Native tenant byte grants now prepay separate admission rights and durable maintenance reserve. A checkpoint claims every participating tenant's conservative allowance in one conditional directory update before uploading. Retry and recovery consume fresh reserve; lost replies, cancellation and takeover never recreate spent rights. An exhausted group refuses without debiting a funded neighbor, and a user database named `durable-catalog` is charged normally. Resolving already staged publication makes no new claim.

The updated two-tenant model checks 10,402 states and six required counterexamples. Rust regressions cover competing refills, lost funding replies, takeover, reserve exhaustion/refill, cancelled uploads, bounded physical attempts and collection races. Eight deliberate Rust defects are detected. A 70-tenant funding group crosses the physical allocator and directory batching boundaries, survives ordinary authority changes, and rebuilds after collection. The existing 44,354-state publication-retry model and its negative control guard that rebasing rule. Formatting, strict clippy and all 300 enabled local Rust tests pass. The previous two-instance admission wrapper, duplicate ledger-writing path and obsolete retry-accounting documentation were removed; no dependency, journal or table encoding was added. [Milestone evidence](evidence/maintenance-funding-2026-11-10.json).

Real B2 and versioned MinIO pass at `8f0b282` in 207.08 and 182.28 seconds. Recovery increases the tenant construction charge from 51,581 to 105,008 bytes out of 105,516 funded bytes, without new admission allocation. A second cold reopen preserves those counters. Retained export/restore and automatic collection pass. Both the initial and final cloud campaigns passed; all four buckets, two workload keys, two MinIO containers and 92 remaining physical versions were removed. These small logical-clock fixtures do not qualify physical timing, large-image recovery, AWS or a performance SLO.

Budgeted native format `owned-candidate-21` requires a fresh namespace. Local format `owned-candidate-15` and cloud CLI publication formats are unchanged. Finite reserve cannot fund unlimited faults: repeated failures can stop shared maintenance until an explicit refill. Administrative metadata, stronger tenant progress isolation, scheduled backups, physical ownership timing and AWS qualification remain open. This remains experimental.

## v0.17.0 retained checkpoint export (2026-11-10)

[v0.17.0](https://github.com/ericflo/durable-db/releases/tag/v0.17.0) packages `f430354`, verified by [Woodpecker #166](https://ci.eflorenzano.com/repos/18/pipeline/166/1). The independently downloaded static binary passes transactions/retries, ordered reads, export/restore and forced-stop recovery. The public default installer installs that exact binary. The optimized 10,005-database capacity check passes in 198.47 seconds.

The Rust operator API now exports a database from one retained checkpoint while live writes advance. Catalog identity, original logical quotas, revision and values all come from that exact cut. The resulting archive uses the existing verified, resumable HTTP restore. This adds 81 net runtime Rust lines, no dependencies and no format or admission changes. Both exporters now verify temporary output before publishing it without replacement. There is no new CLI/HTTP retained-export endpoint or scheduler.

The expanded TCP regression exports 11 live keys and 524,358 logical bytes in two restore chunks, removes the source, and restores twice into an independent destination. It also checks empty databases/values, Unicode, tombstones, 64 KiB values, original quotas, absent post-cut databases, cancellation, corrupt reads/files and a racing output creator. Five deliberately broken Rust variants fail. The focused TCP test takes 1.34 seconds after compilation. All 299 local Rust tests, formatting, strict clippy, both 99-test client contracts and four web-app tests pass.

The existing logical-backup model now includes immutable retained reads: 6,888 distinct states, 40,320 generated, depth 16 and four negative controls, with no extra distinct states or configuration. This is bounded protocol checking, not Rust refinement or a filesystem-durability proof. Duplicate catalog parsing and file publication, validation after publication, and stale current documentation were removed. [Milestone evidence](evidence/retained-export-2026-11-10.json).

B2 and versioned MinIO pass on their first runs (204.36 and 182.23 seconds). Each exports two databases without source PUTs, explicitly releases and collects the old image, then restores each archive twice into an independent local deployment. Both temporary buckets, all 46 remaining physical versions, the scoped B2 key and MinIO container were removed. These small logical-clock fixtures do not qualify physical timing, AWS, large restores or performance SLOs.

Per-tenant retry/recovery attribution remains open: charging a depleted tenant after accepting its write could block shared recovery; durable funding for already accepted work must precede that change. Scheduled backups, multiple restore points, independent backup credentials, physical timing/placement and broader production qualification also remain open.

## v0.16.1 checkpoint retry pacing (2026-11-10)

[v0.16.1](https://github.com/ericflo/durable-db/releases/tag/v0.16.1) packages `a50ad42`, verified by [Woodpecker #162](https://ci.eflorenzano.com/repos/18/pipeline/162/1). The independently downloaded static binary passes transactions/retries, ordered reads, export/restore and forced-stop recovery. The public default installer installs that exact binary. The optimized 10,005-database capacity check passes in 198.61 seconds.

Failed or cancelled native checkpoint construction now waits 1, 2, 4, 8, 16, then 30 seconds between attempt starts. The owner arms the gate before I/O, so incoming requests and collection-invalidated staging cannot accelerate new uploads. Exact validation of an already staged publication still proceeds without uploading again. Confirmed publication resets pacing; the next ordinary checkpoint starts immediately. The change adds 18 runtime Rust lines, no persisted fields or dependencies, and preserves both v0.16.0 formats and admission prices.

The expanded cancellation regression checks seven failed attempts at exact virtual-time boundaries and 700 immediate calls, applied PUT cancellation, publication cancellation and successful reset. The collection race still rebuilds and cold-recovers correctly. A real TCP burst refuses 32 new writes while resolving 32 exact retries with unchanged tenant allocation; idle maintenance resumes after the provider heals. Four unsafe Rust variants fail, including a separate TCP failure when pacing is removed. The focused four-test group takes 0.01 seconds and the TCP test 1.11 seconds after compilation.

All 299 local Rust tests, formatting, strict clippy and both 99-test client contracts pass. The unchanged 65,664-state checkpoint model and nine controls pass; waiting is a stuttering restriction, not a proof of runtime retry rates or the Rust implementation. The unpaced path and stale current test-count/timing claims were removed; existing fixtures were extended instead of duplicating tests or models. [Milestone evidence](evidence/checkpoint-retry-pacing-2026-11-10.json).

B2 and versioned MinIO pass on their first runs (202.97 and 182.02 seconds): admission/refusal, exact retries, two cold reopens, retention/release and automatic collection. Both buckets, the scoped B2 key, the MinIO container and 46 physical versions were removed. These are logical-clock, small-image regression checks; the injected retry faults run locally. They do not qualify physical timing, sustained availability or AWS.

This limits retry amplification within one owner; restart discards pacing state while retaining cumulative upload allocation. Every attempted PUT in a budgeted native namespace still consumes namespace credit. Per-tenant retry/recovery attribution, retained-image export/restore, scheduled backups, physical timing and broader production qualification remain open.

## v0.16.0 manifest encoding and split admission (2026-11-10)

[v0.16.0](https://github.com/ericflo/durable-db/releases/tag/v0.16.0) packages `3e1bb26`, verified by [Woodpecker #159](https://ci.eflorenzano.com/repos/18/pipeline/159/1). The independently downloaded static binary passes transactions/retries, ordered reads, export/restore and forced-stop recovery. The public default installer installs that exact binary. The optimized 10,005-database capacity check passes in 197.34 seconds.

Tenant admission now includes new manifest entries, node headers and split/root growth for one checkpoint construction attempt. The allowance uses the writer’s byte/row limits to fund new table boundaries without pricing a maximum-size path for every retained row. It uses the same authenticated descriptors and cached seeks, with no new value/index reads, persisted fields or dependencies. The change adds 33 net runtime Rust lines. Budgeted native format `owned-candidate-20` refuses older policies and requires a fresh namespace; local format `owned-candidate-15` remains compatible with v0.9.0 and later.

The strengthened existing tests count all uploaded checkpoint bytes, including manifests, across minimum/maximum fields, tombstones, 4,097 short rows, byte-limited chunks, and manifest splits through 8,193 tables. A 65-table case uses maximum 2,048-byte boundaries. The adversarial retained-row rewrite uploads 533,873 bytes against a 2,819,918-byte allowance. Ordinary pricing retains its read counts; append pricing remains cached. Real TCP checks refuse one byte short before allocation or PUT, preserve neighboring writes and replay exact retries. Five Rust negative controls detect missing direct-row growth, inadequate split propagation, omitted retained-table growth, omitted byte/row split shares and missing append growth.

All 299 local Rust tests, formatting, strict clippy and both 99-test client contracts pass with the existing test allowance unchanged. The unchanged 5,776-state tenant-admission model and its three negative controls pass; it abstracts byte amounts and does not prove the sizing argument or Rust refinement. The old table-only upload assertions and incomplete manifest allowance were replaced, and current accounting documentation was rewritten in place. [Milestone evidence](evidence/manifest-growth-admission-2026-11-10.json).

B2 and versioned MinIO pass on the first run (208.78 and 181.88 seconds), including tenant budget refusal, exact retries, two cold reopens, retention/release and automatic cleanup. Both buckets, the scoped B2 key, the MinIO container and 46 physical versions were removed. These use a logical clock and small manifests, not production timing, sustained load or AWS.

These are conservative admission bounds, not provider billing or future upload rights. Maintenance retries/recovery and administrative work remain outside tenant attribution. Retained-image export/restore, scheduled backups, tighter admission bounds, physical timing and broader production qualification remain open.

## v0.15.0 existing manifest-path admission (2026-11-10)

[v0.15.0](https://github.com/ericflo/durable-db/releases/tag/v0.15.0) packages `e817b95`, verified by [Woodpecker #156](https://ci.eflorenzano.com/repos/18/pipeline/156/1). The independently downloaded static binary passes transactions/retries, ordered reads, logical export/restore and forced-stop recovery. The public default installer downloads that exact binary. The optimized 10,005-database capacity check passes in 196.55 seconds.

Native tenant admission now includes the authenticated leaf and ancestor bytes that a write can force checkpointing to copy. Sorted ranges deduplicate shared paths with nine temporary node references. An append after all base tables pays for the rightmost path; missing path evidence fails closed. This adds 37 net runtime Rust lines, no persisted fields and no dependencies. Budgeted native format `owned-candidate-19` refuses earlier policies and requires a fresh namespace. Local format `owned-candidate-15` remains compatible with v0.9.0 and later.

The 183-table regression charges three touched tables and their shared path with the same four cold manifest reads and no table reads; a second case crosses two leaves while charging their root once. The append case reads only two manifest nodes, then none when warm. Real TCP checks refuse a write one byte below its allowance before allocation or PUT, preserve neighboring progress and reuse exact retries. Four Rust negative controls detect omitted path/append charges, the resulting HTTP acceptance, and duplicate ancestor charges. All 299 local Rust tests, formatting, strict clippy and both 99-test client contracts pass. The unchanged 5,776-state tenant-admission model and three negative controls pass; it abstracts byte amounts and does not prove routing or Rust refinement.

Versioned MinIO and a fresh B2 run pass native HTTP admission/refusal, exact retries, two cold reopens, retention/release and automatic cleanup. The first B2 fixture stopped after an immutable PUT error surfaced as HTTP 503 on a neighbor’s commit; the underlying provider/transport cause was not captured. The rerun used unchanged source and assertions, with no implicit retries. All disposable buckets/keys and 63 physical versions were removed. These logical-clock, small-image checks do not qualify production timing, sustained availability or AWS.

The old table-only price and zero-charge append case were replaced. These are conservative admission contributions, not provider billing or a reservation for future uploads. New manifest entries/split overhead, retries/recovery, retained-image export/restore, scheduled backups and physical ownership qualification remain open. [Milestone evidence](evidence/manifest-path-admission-2026-11-10.json).

## v0.14.0 stable checkpoint manifest groups (2026-11-10)

[v0.14.0](https://github.com/ericflo/durable-db/releases/tag/v0.14.0) packages `b6219cc`, verified by [Woodpecker #153](https://ci.eflorenzano.com/repos/18/pipeline/153/1). The exact downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs that same executable. The optimized musl 10,005-database check passes in 196.73 seconds, and both client contracts pass all 99 enabled tests.

Checkpoint rewrites now preserve base manifest groups and split overflowing groups evenly. An insertion into a 4,096-table manifest uploads five nodes instead of 68; an 8,193-table fixture retains unrelated siblings after insertion, deletion and whole-leaf removal. A serving-projection rewrite merges three tables into one with four uploads instead of six, the same ten reads, and all 540 values/deletions intact. Unchanged reconstruction makes no uploads, including after a split.

The change adds 32 net runtime Rust lines and no encoding, admission-policy or dependency changes. Existing budgeted format `owned-candidate-18` and local format `owned-candidate-15` remain compatible. Balanced splits keep 128 repeated prefix insertions into 64 tables at height one. Forward/reverse order, root split/collapse, applied-but-lost responses and cancellation pass. Three Rust negative controls reproduce suffix amplification, a misplaced data-buffer flush and excessive height growth.

All 298 local Rust tests, formatting, strict clippy and both 99-test client contracts pass. The unchanged 31,104-state checkpoint-overlay model and eight negative controls pass. The old regrouping path, redundant fixture base builds and outdated verification paragraph were removed. These are deterministic I/O results, not cloud latency/throughput qualification. Full-manifest traversal, sparse-node compaction, tenant manifest/retry accounting, retained-image export/restore, scheduled backups and physical timing remain open. [Milestone evidence](evidence/manifest-boundaries-2026-11-10.json).

The existing native HTTP qualification passes on real B2 and versioned MinIO on the first run (198.47 and 182.03 seconds), including budget refusal, two cold reopens, retention/release and automatic cleanup. Both buckets, all 46 remaining physical versions, the scoped B2 key and the MinIO container were removed. These use small manifests, logical clocks and stopped predecessors; the multi-level amplification regressions run locally. They do not qualify large-manifest cloud performance, physical timing or AWS.

The exact v0.13.0 binary writes 260 transactions; v0.14.0 reopens its checkpoint and suffix, resolves the latest receipt and writes again. After another forced stop, v0.13.0 reopens that new value and receipt. This checks sequential local upgrade/downgrade, not concurrent mixed-version serving or large-image compatibility. Temporary servers, database directories and tokens were removed.

## v0.13.0 retained-row reblocking admission (2026-11-10)

[v0.13.0](https://github.com/ericflo/durable-db/releases/tag/v0.13.0) packages `c9ed530`, verified by [Woodpecker #150](https://ci.eflorenzano.com/repos/18/pipeline/150/1). The exact downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs that same executable. The optimized musl 10,005-database check passes in 198.68 seconds, and both client contracts pass all 99 enabled tests.

Native tenant admission now covers data/index encoding when maintenance repacks unchanged rows. The previous recorded-data-plus-index price underfunded an eight-block fixture by 7,657 bytes after a small insertion moved 1 KiB keys onto block boundaries. The replacement reserves five times each touched base table’s authenticated data size, with no extra reads, allocations or data/index format changes. It is intentionally conservative: the 533,678-byte rebuilt image receives 2,622,515 bytes of allowance including its new rows. Tightening this bound remains useful work; it is not provider billing.

All 295 local Rust tests, formatting and strict clippy pass. The reblocking regression takes 0.14 seconds; cold pricing retains four manifest reads and no table reads; TCP checks refuse one byte short before allocation or PUT. Restoring old pricing fails both the encoded-image and HTTP checks. The unchanged 5,776-state admission model and all three negative controls pass. The old copy-size loop is removed. Budgeted native format `owned-candidate-18` excludes older policies and requires a fresh namespace; local/unbudgeted format remains `owned-candidate-15`. Manifest/retry accounting, retained-image export/restore, scheduling and physical timing remain open. [Milestone evidence](evidence/retained-row-reblocking-2026-11-10.json).

The first release pipeline exposed two long native Pi cases exhausting the default 64 MiB allowance. Native contract checks now explicitly fund 512 MiB per database through the existing administrator endpoint; defaults and refusal tests remain unchanged. Both client suites pass all 99 enabled tests locally (native 28.19 seconds, local CLI 9.21 seconds). Their repeated database setup and retry loops were consolidated into one fixture.

Real B2 and versioned MinIO pass on their first runs (200.53 and 181.99 seconds). Both refuse insufficient funding without PUTs, preserve values, receipts and balances across two cold reopens, and exercise retention/release and automatic cleanup. Warm reads require no authority updates or object GETs. Both buckets, all 46 remaining physical versions, the scoped B2 key and the MinIO container were removed. These are logical-clock fixtures with stopped predecessors, not physical timing, AWS or performance-SLO qualification.

The exact v0.12.0 binary writes 260 transactions; v0.13.0 reopens its checkpoint and suffix with values and receipts intact, then adds a write that v0.12.0 successfully reopens. Forced stops separate each transition. This checks sequential local compatibility, not mixed-version clustered serving. Release verification leaves no test server, tokens or database directory.

Client contracts now branch from completed debug Rust checks and run alongside optimized compilation and capacity checks. Artifact assembly still requires both branches. This removes an unnecessary serial dependency; no end-to-end speedup is claimed from differently cached runs. Superseded copy-size pricing, duplicate client-fixture setup, stale retention claims and temporary qualification files were removed.

## v0.12.0 checkpoint retention (2026-11-10)

[v0.12.0](https://github.com/ericflo/durable-db/releases/tag/v0.12.0) packages `10eb661`, verified by [Woodpecker #146](https://ci.eflorenzano.com/repos/18/pipeline/146/1). The exact downloaded static executable passes standalone transactions, ordered reads, backup/restore and forced-stop recovery; the public default installer installs that same binary. The optimized musl 10,005-database regression passes in 205.59 seconds, and the local/native client contract checks each pass all 99 enabled tests.

The native Rust operator API now retains one published namespace checkpoint through ownership takeover and collection, with explicit identity-checked release. It adds no immutable uploads or clock-dependent expiry. The collector protects the current and retained images; publication preserves backup-slot changes without relaxing the collection-generation check. Binaries before v0.12.0 refuse an occupied slot. This is an in-process operator primitive; public retention routes, scheduled backups, multiple restore points and retained-image restore remain open.

All 294 local Rust tests pass, along with formatting and strict clippy. Four focused retention regressions take 0.02 seconds and the real TCP write/read/retention test takes 0.03 seconds. Omitting retained-root protection makes cold readback fail. The unchanged publication model passes 175,546 states and four negative controls; its abstract TLAPS proof passes 35 obligations and the negative fence control. The current-checkpoint-only collector branch and the obsolete retention-plan paragraph were replaced. [Milestone evidence](evidence/checkpoint-retention-2026-11-10.json) records the checked scope.

Real B2 and versioned MinIO pass on their first attempts (200.70 and 181.90 seconds). Both retain old checkpoint values at their exact cut through current writes, two cold reopens and automatic collection, then reclaim the old image after explicit release. Retention itself sends no immutable PUTs. Both buckets, all 46 remaining physical versions, the scoped B2 key and the MinIO container were removed. These are logical-clock fixtures with stopped predecessors, not physical timing, AWS or performance-SLO qualification.

The exact v0.11.0 binary writes 260 transactions; v0.12.0 reopens its checkpoint and suffix with values and receipts intact. A disposable offline metadata fixture installs a valid retained descriptor: v0.11.0 refuses it without changing authority, while v0.12.0 opens it. Removing that fixture field permits v0.11.0 to reopen again. This checks sequential compatibility and refusal behavior; offline metadata editing is not an operator recovery procedure. The initial drill hit HTTP 429 at the default rate budget; a larger bounded fixture budget passes without runtime changes. The milestone adds 165 net runtime Rust lines including comments and no new runtime dependencies. Retained-row reblocking, manifest/retry accounting, scheduled retention and physical ownership qualification remain open.

## v0.11.0 checkpoint encoding admission (2026-11-10)

[v0.11.0](https://github.com/ericflo/durable-db/releases/tag/v0.11.0) packages `e94ea95`, verified by [Woodpecker #143](https://ci.eflorenzano.com/repos/18/pipeline/143/1). The exact downloaded static binary passes standalone transactions, ordered reads, backup/restore and forced-stop recovery without a toolchain; the public default installer installs that same verified binary. The optimized musl 10,005-database regression passes in 199.10 seconds, and the local/native client contract checks each pass all 99 enabled tests.

Native write admission now prices each new metadata, receipt and mutation row as a singleton data/index pair, covering its block header, index header, boundary keys, extent and digest. This adds nine net runtime Rust lines and no provider calls or allocations. Existing base-table copy charges remain. The small TCP fixture charges 402 additional bytes; one byte below the complete allowance refuses before allocation or PUT, and an exact retry remains free.

All 289 local Rust tests, formatting and strict clippy pass. Actual singleton encodings match their allowance; maximum-field, 128-mutation and multi-table checkpoint cases fit the sum. Removing the encoding allowance makes the HTTP regression wrongly admit a write (200 instead of 413). The unchanged 5,776-state admission model and all three negative controls pass. The row-only calculator and test that stripped block headers/ignored indexes were removed.

Real B2 and versioned MinIO pass the native HTTP fixture on their first runs (142.03 and 121.61 seconds). Each charges 2,318 and 2,652 bytes for its two writes, including shared base copies, and preserves values, receipts and balances across cold reopens and automatic cleanup. Both buckets, 45 physical versions, the scoped B2 key and MinIO container were removed. These logical-clock fixtures do not qualify physical timing, AWS or a performance SLO.

Budgeted native format `owned-candidate-17` excludes older admission policies; it requires a fresh budgeted namespace. Unbudgeted/local format stays `owned-candidate-15`. An exact-binary upgrade drill writes 260 transactions with v0.10.0, kills it, opens the same directory with v0.11.0 and writes again, then kills it and reopens with v0.10.0. Values and exact retry receipts survive both transitions without export. This checks sequential local compatibility, not concurrent mixed-version cluster serving. [Milestone evidence](evidence/row-overhead-admission-2026-11-10.json) records the scope and verification. Retained-row reblocking, manifest and retry costs remain outside complete tenant attribution. Durable retention and calibrated clock/placement qualification remain open; [the clock infrastructure requirements](design-owned-partitions.md#handoff-fixes-an-exact-old-prefix) now identify the required devices, guarantees and fault-injection access.

## Clock error prerequisites (2026-11-10)

The existing timing proof now has **21 strict TLAPS obligations** and three required negative controls. Its added lemma shows that per-reading maximum error `A` supplies the elapsed error `E = 2A`; no replacement lease algorithm or rate scaling is needed. Treating missing maximum-error flags as zero fails the separate capability regression. All 288 Rust tests, strict clippy, formatting and the existing physical process-pause test pass.

The new capability mode reuses the VM harness and completes in **2.27 seconds** with its cached stock image, without compiling Rust or a custom kernel. QEMU 10.1.5 exposes a disruption marker but an invalid counter, unknown clock status and no maximum-error or generation fields. The development host and all three cluster nodes lack VMClock. Their NIC PTP devices do not by themselves establish calibrated error bounds. **These platforms do not qualify the proposed VMClock source.** Repeating pause tests on the marker-only device cannot supply the missing guarantee.

[Evidence](evidence/vm-clock-2026-11-10.json) contains the raw device header, read-only host inventory, proof results and an unchanged stock-kernel probe regression. The existing evidence record and docs were updated; obsolete four-lemma/two-control counts and the claim that native HTTP never uses the timing component were removed. Disposable guests and test files were cleaned up. No runtime code, storage format or binary release changes; v0.10.0 remains current. Clustered native serving stays gated on a calibrated source, incarnation handling and placement qualification.

## v0.10.0 shared-table index admission (2026-11-10)

[v0.10.0](https://github.com/ericflo/durable-db/releases/tag/v0.10.0) packages `eb2915a`, verified by [Woodpecker #139](https://ci.eflorenzano.com/repos/18/pipeline/139/1). The exact downloaded static Linux x86_64 binary passes standalone transactions, ordered reads, backup/restore and SIGKILL/reopen without a toolchain. The public default installer verifies the checksum and installs that same binary. The musl 10,005-database test passes in 198.61 seconds; 99 enabled tests pass against the local CLI in 10.65 seconds and against the separate native library fixture in 64.33 seconds.

Native write admission now charges both data and index objects for every base table a change can replace. Pricing uses existing authenticated manifest descriptors and adds no provider calls. The 183-table fixture checks three rewritten pairs with four manifest reads and no table reads; the TCP regression rejects one byte short before allocation/PUT and admits the exact boundary. Restoring data-only pricing wrongly admits the write and fails that regression.

The change adds 13 net runtime Rust lines. All 288 local Rust tests pass; the focused admission group takes 0.27 seconds, TCP refusal 0.10 seconds, and the larger checkpoint check 0.69 seconds. The existing 5,776-state model and three negative controls pass without expanding its state space.

Budgeted format `owned-candidate-16` excludes older accounting policies. Unbudgeted storage stays at `owned-candidate-15`: a published v0.9.0 binary creates a checkpoint and suffix, the new binary reopens that same directory and writes, then v0.9.0 reopens again. Values, revisions and exact retry receipts survive both forced-stop transitions without export. The data-only shortcut and forced format changes for unbudgeted storage are removed.

Real B2 and versioned MinIO pass the native HTTP fixture, preserving values, receipts and nonrefundable budgets across two cold reopens and automatic cleanup. The first B2 run returned a 503 after one immutable-upload error; a fresh run passed with no retry-policy change. Its underlying cause remains unexplained. All disposable resources from both runs were removed.

[Complete evidence](evidence/index-copy-admission-2026-11-10.json) records that failure, the corrected fixture expectation and the compatibility drill. This accounts for existing table/index copies; growth in new encoding, manifests, retries and recovery remain outside full tenant attribution. Cluster timing/placement, durable retention and production qualification remain open.

## v0.9.0 native local CLI (2026-11-10)

[v0.9.0](https://github.com/ericflo/durable-db/releases/tag/v0.9.0) packages `cf1a2bf`, verified by [Woodpecker #136](https://ci.eflorenzano.com/repos/18/pipeline/136/1). The downloaded static Linux x86_64 binary passes standalone checks without a toolchain, and the public default installer installs that exact binary after checksum verification. The musl 10,005-database regression passes in 198.29 seconds. All 99 enabled client tests pass against the new local CLI in 11.33 seconds; the separate native library fixture also passes all 99 in 64.87 seconds.

Local `serve --data` now selects the native journal, index, recovered HTTP worker, checkpoints and automatic collection. A lifetime directory lock supplies single-host exclusion; no physical ownership clock is required. Shared/network filesystems and cloned live process memory remain unsupported. The cloud CLI stays on the shared publication engine while physical timing and placement are qualified.

All 287 local Rust tests, 99 enabled client/Pi tests and four web-app tests pass. Real process tests cover pause, duplicate-server refusal, resume, kill, two cold reopens, exact receipts, automatic collection and missing-authority refusal. Removing the blocking mutation guard fails the cancellation regression. The new ownership model checks 57 states with two negative controls; the existing checkpoint model and nine controls also pass. The published v0.8.0 binary exports three values; the new binary refuses its directory, restores into a fresh native directory, and verifies values and quotas after SIGKILL/reopen.

Real B2 and versioned MinIO also pass the shared collector fixture, preserving values, receipts and budgets through two cold reopens. Both buckets, remaining physical versions, the B2 key and MinIO container were removed. These fixtures use logical clocks and stopped predecessors.

[Complete evidence](evidence/native-local-cli-2026-11-10.json) records the checks and two corrected harness errors. This removes legacy local CLI selection, its separate maintenance task, the optional native-collection branch and obsolete benchmark provisioning. It does not qualify clustered timing or production operation. [Existing local directories require export and restore](recovery.md#local-format-upgrades).


## v0.8.0 shared-table copy admission (2026-11-10)

[v0.8.0](https://github.com/ericflo/durable-db/releases/tag/v0.8.0) packages `346f57c`, verified by [Woodpecker #133](https://ci.eflorenzano.com/repos/18/pipeline/133/1). The downloaded static Linux x86_64 archive passes standalone transactions, ordered reads, backup/restore and SIGKILL/reopen with no toolchain. The public default installer verifies the GitHub asset checksum and installs the same binary. The musl 10,005-database regression passes in 199.04 seconds; all 99 enabled native client tests pass in CI in 67.97 seconds, within the unchanged 90-second deadline.

Commit `278e5f6` charges each distinct native write for its journal record, new checkpoint rows and the base data tables its changes can force maintenance to copy. This includes unchanged neighbors' rows and replaces the receipt-only surcharge. Manifest seeks price table descriptors without reading indexes or values; duplicate routes count once. New databases in gaps and exact upper-bound mutations are covered. Distinct writes can pay repeatedly before checkpointing. This is conservative admission, not precise billing or a reservation for future uploads. Native format `owned-candidate-15` requires a fresh namespace; the CLI engine is unchanged.

All 283 local Rust tests and 99 enabled native client tests pass. A TCP test measures actual encoded objects, refuses one byte short before allocation/PUT and accepts the exact boundary; deliberately undercharging by one byte fails it. In the 183-table fixture, pricing reads four manifest nodes and covers exactly the three replaced base data objects, with no index/value reads. A shared-table test checks gap inserts, append-only generations, duplicate keys, dirty writes, expiry, cached pricing and missing-manifest refusal. The model remains 5,776 states with three negative controls. The change adds 54 net runtime Rust lines and removes the receipt-only calculator, special dirty-window rule and superseded test/control.

Real B2 and versioned MinIO pass the HTTP fixture, charging 1,765 and 2,113 bytes for its two writes, including 1,337 and 1,685 bytes of shared base data. Refusal and exact retries issue no immutable PUT; neighbors continue. Two cold reopens and automatic cleanup preserve balances, receipts and values. B2 finishes in 138.34 seconds, MinIO in 121.49 seconds. The initial B2 run received HTTP 500 on a fixture orphan-marker upload; it was cleaned up and repeated in a fresh bucket without changing retry semantics. All three disposable buckets, physical versions, both B2 keys and the MinIO container were removed. [Complete evidence](evidence/shared-table-admission-2026-11-10.json).

New block/index/manifest overhead, administrative catalog/ledger writes, maintenance retries/recovery, physical timing/native CLI cutover, durable retention and broader performance/AWS qualification remain open. These provider fixtures use a logical clock and stopped old servers, not production timing or sustained load.

## v0.7.0 retained-receipt admission (2026-11-10)

[v0.7.0](https://github.com/ericflo/durable-db/releases/tag/v0.7.0) packages `c759469`, verified by [Woodpecker #130](https://ci.eflorenzano.com/repos/18/pipeline/130/1). Its downloaded static Linux x86_64 archive passes standalone transactions, ordered reads, backup/restore and SIGKILL/reopen without a toolchain. The public default installer verifies the GitHub asset checksum and installs the same binary.

Commit `d00d9ad` charges a database for the base receipt window its first post-checkpoint write may rewrite, before assigning journal order. Once a committed write dirties the database, later writes pay only their direct contribution until checkpointing. The charge uses metadata already loaded for retry resolution: no additional provider call, allocation, scan, ledger or background budget check. Concurrent first changes can overcharge, and expiring receipts still count conservatively. The runtime adds 18 net Rust lines. Native format `owned-candidate-14` requires a fresh namespace; the CLI engine remains unchanged.

All 283 local Rust tests and 99 enabled native client tests pass. The TCP test rejects new-row-only funding without a PUT/allocation, accepts the exact additional 70 bytes, then admits a dirty write with only its direct 420-byte contribution. Omitting the retained-receipt charge makes this test fail. The encoder test compares cold checkpoint metadata with actual rewritten row bytes and covers reset after checkpointing and receipt expiration. The model stays at 5,776 states with three negative controls; direct work is one unit and retained receipts the other. The superseded control/config and an outdated non-HTTP recovery comment were removed, and repeated row-byte test code was consolidated.

Real B2 and versioned MinIO both pass the extended HTTP fixture: 428 bytes admit the initial write, and the first change after checkpointing needs another 500 bytes (including a 72-byte retained receipt). New-row-only funding is refused before a PUT/allocation. Two cold reopens and automatic collection preserve the cumulative 928-byte allocation, receipts and values; funded neighbors continue. All disposable provider resources were removed. [Complete evidence](evidence/retained-receipt-admission-2026-11-10.json).

CI now runs the capacity regression on the shipped musl target, removing the separate optimized GNU build. The 10,005-database test passes in 220.46 seconds, and the following release binary build completes from the warm cache in 0.28 seconds. The binary is byte-for-byte identical to published v0.7.0. The evidence preserves the original timings; different cache state and concurrent load prevent a controlled end-to-end speedup claim. No test gate was removed.

This covers the changed database's base receipt window once per admitted first change. Unchanged shared-table rows, block/index/manifest overhead, metadata and maintenance retries/recovery remain outside tenant attribution. Physical timing/native CLI cutover, durable retention, large-image cleanup and production/AWS qualification remain open.

## v0.6.0 tenant checkpoint-row admission (2026-11-10)

[v0.6.0](https://github.com/ericflo/durable-db/releases/tag/v0.6.0) packages `99d3dca`, verified by [Woodpecker #128](https://ci.eflorenzano.com/repos/18/pipeline/128/1). The downloaded static Linux x86_64 archive passes standalone transaction, ordered-read, backup/restore and SIGKILL/reopen checks without a toolchain. The public default installer verifies its GitHub asset checksum and installs the same binary.

Commit `9b198aa` changes native write admission from journal-only charging to the atomic sum of journal bytes and the checkpoint rows a transaction introduces: values/tombstones, its new receipt and database metadata. It reuses the existing ledger, adds no provider call to a cached grant, and adds 20 net runtime Rust lines. Duplicate row-sizing arithmetic and the obsolete journal-only rule were removed. Native format `owned-candidate-13` requires a fresh namespace so older controllers cannot undercharge; the CLI engine and storage format are unchanged.

All 282 local Rust tests and 99 enabled native client tests pass. The focused four-test budget group takes 0.51 seconds; the encoder check takes 0.11 seconds. Restoring the old journal-only charge makes both the mixed-batch and real TCP refusal tests fail. The reused admission model checks 5,776 states in under one second, with three negative controls; the existing credit model passes 11,006 states and five controls. These are bounded checks, not an implementation proof.

Real B2 and versioned MinIO both refuse a 428-byte write at a 427-byte allowance without an immutable PUT or credit allocation, then accept it at the exact boundary. Two cold reopens and automatic journal/table/metadata collection preserve values, receipts and the 856-byte cumulative allocation for two writes. Funded neighbors continue and exact retries cost no new admission. All disposable buckets, physical versions, the B2 test key and MinIO container were removed. [Complete evidence](evidence/tenant-checkpoint-admission-2026-11-10.json).

This is direct row-contribution accounting, charged once per admitted attempt. It does not cover retained-row/receipt rewrites, shared block/index/manifest costs, metadata, retries/recovery or request/egress costs. Complete tenant maintenance accounting, physical timing/native CLI cutover, durable retention and large-image cleanup qualification remain open.

## v0.5.1 checkpoint index reads (2026-11-10)

[v0.5.1](https://github.com/ericflo/durable-db/releases/tag/v0.5.1) packages commit `7eae7de`, verified by [Woodpecker #125](https://ci.eflorenzano.com/repos/18/pipeline/125/1). The exact downloaded static Linux x86_64 archive passed standalone transaction, ordered-read, backup/restore and SIGKILL/reopen checks without a toolchain on `PATH`. The public default installer verified the GitHub asset checksum and installed the same binary. Native client contracts passed 99 enabled tests in CI (66 seconds).

Commit `9e0892c` removes table-index reads for unchanged checkpoint ranges. An uncached 183-table fixture drops from 187 reads to four manifest reads for an unchanged image. Deleting a value exactly at an upper bound uses ten reads, touches no objects belonging to the two idle database generations and preserves all 540 values/deletions and exact retry windows. The runtime change adds ten net Rust lines, with no new cache, ledger or storage format. A four-line follow-up rejects cross-leaf manifest disorder before a resident range lookup; its negative control reproduces the former panic. The old unconditional index scan and brittle table-packing assertions were removed.

The native HTTP fixture passes on real B2 and versioned MinIO, preserving values, receipts and nonrefundable budgets across two cold reopens and automatic collection. Each sweep retains exactly three published checkpoint objects in this fixture. Both buckets, all remaining physical versions, the B2 test key and MinIO container were removed. [Complete evidence](evidence/checkpoint-index-reads-2026-11-10.json) includes the 281-test local Rust suite, 99 native client tests, the unchanged 31,104-state model with eight negative controls, and Rust controls that fail for the previous implementation and an incorrect boundary comparison.

The Woodpecker step now forwards termination signals to `buildctl` and preserves normal exit codes. A real disposable BuildKit job canceled in 5.8 seconds; its temporary context was removed. The record includes the preceding canceled-build delay and probe setup failures. No GitHub Actions workflow was added.

This reduces shared maintenance amplification; it does not complete tenant maintenance accounting. Full-manifest traversal remains. Retained objects are checked when read or explicitly audited; rewriting a checkpoint is not a full-image audit. Physical ownership timing/native CLI cutover, durable retention, large-image cleanup progress and production/AWS qualification remain open. Native format stays `owned-candidate-12`.

## v0.5.0 native table and manifest collection (2026-11-10)

[v0.5.0](https://github.com/ericflo/durable-db/releases/tag/v0.5.0) packages commit `99db9f2`, verified by [Woodpecker #121](https://ci.eflorenzano.com/repos/18/pipeline/121/1). Its downloaded static Linux x86_64 archive passed standalone checks without a toolchain on `PATH`, including backup/restore and recovery after SIGKILL. The public default installer verified the GitHub asset checksum and installed the same binary. The native JavaScript contract passed 99 enabled tests in CI (67 seconds); the CLI still uses the shared publication engine.

Native maintenance now reclaims obsolete checkpoint data, indexes and manifest nodes. It uses the existing collector and bounded manifest walker, protecting the published checkpoint before an exact fence. Staging cannot adopt a later collection generation after its objects may have been deleted. The live owner rebuilds invalidated staging with fresh identities. Native format `owned-candidate-12` requires a fresh namespace.

Commit `ba739d2` passes the HTTP fixture on real B2 and versioned MinIO. Each starts with six checkpoint objects and retains exactly five after collection: two data/index pairs and one manifest. After another checkpoint and sweep, five live objects remain again. Two cold reopens preserve values, retry receipts and nonrefundable budgets. The [complete evidence](evidence/native-table-collection-2026-11-10.json) includes 279 local Rust tests, 99 native client tests, the existing 175,546-state publication model with four negative controls, the 35-obligation abstract proof, and cleanup of both buckets, physical versions, the temporary B2 key and MinIO container.

The initial B2 attempt stopped on an unexplained 503 before the collection checks; a rerun passed without adding request retries. The initial MinIO attempt exposed an incorrect one-table fixture assumption; the test now checks the published table count. Both failures are retained in the evidence. Raw handles are not retention pins, and traversal work grows with checkpoint size. Large-image progress, durable reader/backup retention, full tenant maintenance accounting and physical ownership timing remain open. These are component results, not production or AWS qualification.

## v0.4.0 inline checkpoint descriptors (2026-11-10)

[v0.4.0](https://github.com/ericflo/durable-db/releases/tag/v0.4.0) packages commit `7535bab`, checked by [Woodpecker #118](https://ci.eflorenzano.com/repos/18/pipeline/118/1). The downloaded static Linux x86_64 archive passed standalone checks with no toolchain on `PATH`, including backup/restore and recovery after SIGKILL. The public installer downloaded the GitHub asset, verified its checksum and installed the same binary.

The native checkpoint descriptor now lives in authority metadata, removing its header object and upload/read/cache paths. Snapshot creation performs zero object reads; an unchanged rewrite makes zero uploads. Runtime Rust shrank by 42 lines. Native format `owned-candidate-11` requires a fresh namespace when coming from an older candidate.

The [qualification record](evidence/inline-checkpoints-2026-11-10.json) includes 276 local Rust tests, the unchanged bounded checkpoint model and nine negative controls, and passing real B2/versioned MinIO HTTP tests. Automatic maintenance retains the recovery catalog and the uncovered journal suffix, then reclaims the suffix after a new checkpoint; two complete cold reopens preserve values, exact retry receipts and budgets. Both buckets, physical versions, the temporary key and container were removed. The native JavaScript contract passed 99 enabled tests in CI (67 seconds). An earlier CI run exceeded its 90-second deadline; three local runs passed, and the harness now streams diagnostics. The earlier timeout remains unexplained and is recorded in the evidence. This does not qualify physical timing, AWS or an SLO. The CLI still uses the shared publication engine. Obsolete table/manifest reclamation and full tenant maintenance accounting remain open.

## v0.2.0 experimental release (2026-11-10)

[v0.2.0](https://github.com/ericflo/durable-db/releases/tag/v0.2.0) packages commit `6ae12e4`, verified by [Woodpecker #109](https://ci.eflorenzano.com/repos/18/pipeline/109/1). The build gate passed dependency audits, Rust tests including the directory-capacity regression, client/Pi contracts, maintained models/proofs and Pages delivery. The static Linux x86_64 archive contains the exact tested binary and Apache-2.0 license. A standalone run with no toolchain on `PATH` passed protected creation/retry, point and ordered reads, export/inspect/restore, and recovery after a forced process stop. The public installer then downloaded the GitHub asset, verified its checksum and installed the same binary. [Release evidence](evidence/release-0.2.0-2026-11-10.json) records hashes, checks and scope. Temporary servers, data, tokens and installation files were removed.

The CLI still uses the shared publication engine; native serving remains a library integration. This release does not close physical timing, full tenant maintenance accounting or native journal/checkpoint reclamation gates.

## Native HTTP budgets and recovery (2026-11-10)

Commit `aad7ef8` passes the extended native HTTP fixture on real B2 and versioned MinIO. Three owners and two complete cache/connection reopens preserve values, exact retry receipts, tenant balances and nonrefundable namespace allocations. The normal maintenance task runs both before and after the first takeover, when a recovery catalog exists. Both providers then recover that collected namespace again. The [complete evidence](evidence/native-catalog-collection-2026-11-10.json) includes configuration, timings, the initial fixture failures and cleanup. Every remaining physical version, both disposable buckets, the temporary B2 key and the MinIO container were removed.

This closes a coverage gap in the [earlier two-owner fixture](evidence/native-http-budgets-2026-11-10.json): it stopped after the first takeover and never collected a populated recovery catalog. The native collector had protected only budget metadata although the catalog shares its prefixes. A regression failed with a missing catalog page. Native format `owned-candidate-9` protects both roots from the same authority and rejects earlier experimental formats. No in-place migration is provided. The first extended cloud runs correctly exhausted the old 16 MiB fixture allowance on owner three; the corrected fixture provides 32 MiB and verifies 24 MiB remains allocated across all three connections, without refunding abandoned rights.

The same run checks exhausted-tenant refusal before an immutable PUT, neighbor progress, idempotent refill and 100 warmed point-and-scan HTTP reads with **zero authority calls and zero object GETs**. It uses a logical clock, stopped old servers, a debug build, local loopback and a 4 MiB cache. It does not qualify physical timing, AWS, sustained performance or an SLO. Full tenant maintenance accounting and journal/checkpoint reclamation remain open. Earlier sections retain the component and commit they qualified.

## Initial release (2026-11-08)

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


## S3 conditional-write ambiguity

A provider-binding review found that the S3 SDK's transient-status retry can turn a committed conditional metadata update with a lost acknowledgement into a subsequent `412`, violating the binding's definite-conflict contract. The S3 control-register client now disables those automatic retries. Immutable file I/O keeps its separate bounded SDK retry policy, and the shared B2/S3 publication algorithm is unchanged. The caller sees uncertainty and can resolve it through the existing durable proposal/receipt protocol.

A real HTTP wire fixture stores the requested authority update, returns `503`, and would return `412` on a repeated old comparison. The fixed binding sends exactly one PUT and returns an error; a fresh GET proves that revision 1 committed. A separately invoked stale comparison still returns a definite mismatch. A temporary negative control enabling one SDK retry fails the uncertainty assertion, confirming the test detects the original binding behavior; the safe configuration was restored and passes. Formatting, clippy, all 62 regular Rust tests, and both versioned and unversioned MinIO integration tests pass. The 10,005-database regression had just passed before this isolated wire-binding correction; it is also part of the next Woodpecker gate. This closes a transport-contract gap, not an AWS outage or billing qualification.


## Kernel OOM recovery under autoscaling

An extended [kind/MinIO campaign](evidence/kubernetes-oom-minio-2026-11-09.json) repeats CPU-driven 1→3→8, node drain, SIGKILL, a confirmed 40-second provider partition, CPU downscaling to one, and 0→1 cold recovery. It additionally identifies an API container by pod UID and runtime container ID, temporarily reduces only that container's cgroup memory limit to 1 MiB, restores the original limit, and requires the runtime to report **OOMKilled, exit 137**. The allocation limit is a deliberate fault injection, not a recommended operating limit or a natural-load memory-envelope measurement.

All **468 acknowledged writes** survive and verify exactly after every API pod disappears. The independent [history check](evidence/kubernetes-oom-minio-2026-11-09.history.jsonl) again confirms contiguous per-database revisions, unique request IDs, invocation order, and value hashes. There are 7,141 verified read responses, 1,075 intercepted provider packets, 8,697 admission rejections, 126 HTTP 503 responses, and 122 transport/decode errors retained in the report. This campaign uses an explicit **25m CPU request**, 70% HPA target, and the existing 1-core limit to make eight replicas repeatably necessary for this finite workload. It says nothing about recommended CPU requests or throughput capacity.

A preceding [calibration attempt](evidence/kubernetes-oom-calibration-incomplete-2026-11-09.json) with a 100m request settled at four replicas and was stopped before fault injection. That incomplete result is retained; reaching a smaller CPU-appropriate replica count was not a database correctness failure. The passing OOM run remains a one-host, compatible-S3 test and predates the separate S3 control-retry correction. B2/independent-node qualification and sustained-load memory bounds remain open.


## Kubernetes fault campaign on real B2

The corrected [B2 campaign](evidence/kubernetes-b2-2026-11-09.json) passes the same CPU-triggered 1→3→8, worker drain, SIGKILL, confirmed kernel OOM, 40-second provider partition, CPU downscale to one, complete API-pod removal, and cold 0→1 recovery sequence. It uses Woodpecker 18's static `48d6969` image, a dedicated B2 `us-west-002` bucket with a scoped workload key, a JSON keyring with the legacy administrator envelope disabled, and an expiring administrator capability. A **15m CPU request**, 70% target, 1-core limit, and 60-second maintenance interval exercise this finite cloud-latency-bound workload; these are qualification settings, not production sizing advice. The three Kubernetes nodes share one physical laptop.

All **35 acknowledged writes** recover exactly; all **1,340 checked read responses** match the synthetic fixture. The [recorded acknowledgement histories](evidence/kubernetes-b2-2026-11-09.history.jsonl) pass the independent revision, identity, invocation-order, and value-hash checks. The provider partition intercepts 669 packets. The report retains 301 HTTP 503 responses, 2,341 admission rejections, and nine transport/decode errors, including deliberately injected failures. This short fault experiment is not a throughput benchmark or evidence that the previously failed production performance gate is closed.

The [first B2 calibration](evidence/kubernetes-b2-calibration-incomplete-2026-11-09.json) used a 2m CPU request and could not downscale while background maintenance exceeded the target. It was stopped after the fault phases; a separate read pass verified all 23 acknowledged writes before the repeat. That incomplete result and its reason are preserved. The final run reaches one replica through CPU demand reduction, with the same configuration used during scale-up. No replica-bound override is used for that downscale; the HPA is removed only for the explicit all-pods-zero phase.

All test API pods are stopped before removing the disposable B2 bucket and revoking its workload key. Resource-metrics installation and all disruption commands are confined to the named disposable kind cluster; the shared Kubernetes default context remains unchanged. Independent physical failure domains, longer partitions, real AWS, sustained memory/cost limits, and the 72-hour campaign remain release gates.

## Durable immutable-upload admission and transport audit

The format-4 milestone introduced the [cumulative upload ledger](design-upload-admission.md), shared by B2 and S3. Its [qualification report](evidence/upload-budget-2026-11-09.json) records 75 passing regular Rust tests, the separate 10,005-database release regression (40.86 seconds), formatting/clippy, and the expanded credit model: 8,690 distinct states, 32,261 generated, depth 16, with four effective negative controls. Tests include concurrent allocation, complete replica replacement, lost allocation/refill acknowledgements, delayed arrivals, zero-byte requests, independent byte/attempt limits, bounded receipt eviction, administrator-only HTTP, and proposal recovery after exhaustion. Finite checking is not an unbounded or composed implementation proof.

A transport audit additionally reproduced an HTTP `303` turning a conditional S3 PUT into a GET that the SDK interpreted as success. Disabling SDK retries alone does not prevent this. Both bindings now use one shared HTTP policy with redirects and lower-layer retries disabled, and a separate SDK client with zero upload retries. The SDK continues to supply endpoint selection, credentials, signing, and encryption headers; read retries remain bounded. Wire tests cover common redirect statuses, committed `503` responses, transient upload errors, empty payloads, and a lost connection after the complete upload body arrives. Each immutable invocation sends at most one PUT; a new invocation must charge again.

The cloud fixture passes on real B2 and versioned/unversioned MinIO. It exhausts a fresh eight-attempt namespace, drops every backend instance, reopens without cache or budget flags, verifies committed data, refuses another upload, collects orphan metadata, raises ceilings without resetting allocation, replays the same administrative request, writes again, and reopens. A separate real-process guard starts a format-3 binary against that namespace and requires explicit refusal; a current server then proves the budget and database remain unchanged. This covers fresh format-4 deployment, not format-3 migration or downgrade.

The first B2 run failed during metadata collection: the extra ledger plus collection cursors reached eleven bucket metadata entries, and B2 rejected the update. That failed result is retained. Format 4 now packs control fields into one shared document while keeping its incompatible root envelope visible to older binaries. Entry and byte admission reserve later fences, cursors, receipts, and old startup probes. Both disposable B2 buckets and their scoped keys were removed after qualification. A guard-harness assertion also initially confused allocated credit with the full raised ceiling; the corrected check compares actual authoritative snapshots before and after the old probe.

This is a global immutable-upload ceiling, not complete multitenant spend admission. One tenant can still consume shared credit. Control traffic, reads, egress, and other provider charges are outside its scope. At this milestone, data-page GC and compaction required upload credit; only metadata collection was qualified at exhaustion. The following format-5 milestone changes that collection boundary. These remain production gates along with sustained load, independent failure domains, backup/restore, AWS, and proof/review obligations.

## Upload-free page collection and independent credit replenishment

Fresh budgeted namespaces now initialize format 5. Its [collector](design-budgetless-collection.md) can reclaim orphan data pages using authority fencing and physical deletion without an immutable upload. The engine captures the original collection fence before preparing pages; proposal helpers preserve that fence. Missing or corrupt protected metadata stops collection. Existing format-3 and format-4 namespaces retain their prior behavior; no in-place migration or downgrade is supported.

The [qualification report](evidence/global-collection-2026-11-09.json) records **86 regular Rust tests**, formatting/clippy, and **10,005 databases each in formats 3 and 5** (83.12 seconds together). Tests cover exhaustion, uncertain fences, restart across cursor boundaries, abandoned durable proposals, retained pages, missing protected indexes/manifests, malformed manifests with valid digests, and two writers racing two collectors. Temporarily removing the original engine-fence comparison causes the paused-writer regression to fail; restoring it passes.

The model was checked before implementation. Its final configurations explore **1,620,524 states** across two databases and **6,442,262 states** with two concurrent collectors. Three unsafe controls reproduce live-object deletion. The credit model now includes independent replenishment that preserves previously confirmed rights, with **11,002 states** and four effective unsafe controls. Its stronger invariant accounts for both charged and remaining usable credit. These are bounded checks, not an unbounded or composed Rust proof.

Real B2 and both versioned and unversioned MinIO pass the exhausted-credit fixture: 142 allocated attempts, three committed replacements, and 127 additional orphans. After removing all backend instances, fresh instances reclaim **129 orphan pages** across the 128-object cursor boundary, leave durable allocation unchanged, and recover the live value at revision 3. Ordinary maintenance resumes the sweep after another reopen. The final B2 test takes 81.39 seconds; this is not a performance benchmark. Both MinIO configurations also repeat the exhaustion/refill fixture.

A real format-4 executable refuses the new namespace on B2 and versioned MinIO. A fresh format-5 process then confirms unchanged authoritative budgets and data. The task-owned B2 bucket, its three remaining physical versions, and its workload key are removed afterward. Compaction remains funded, and all listing/read/control/deletion traffic remains outside the upload ceiling. Sustained cleanup under contention, current-format Kubernetes fault campaigns, AWS, backup retention, and the broader production gates remain open.

## Format-5 autoscaling with durable upload limits

The [current-format MinIO campaign](evidence/kubernetes-budget-minio-2026-11-09.json) uses Woodpecker 22's `d655c3c` static image and a fresh format-5 namespace with a 1 GiB/100,000-attempt cumulative upload ceiling. CPU-driven 1→3→8, node drain, SIGKILL, confirmed OOMKilled/137, a 40-second provider partition, CPU downscale to one, complete pod removal, and cold recovery all pass. All **433 acknowledged writes** recover exactly, and **12,805 read responses** are checked. The [history](evidence/kubernetes-budget-minio-2026-11-09.history.jsonl) passes the now-reusable `scripts/check-autoscaling-history.py` validator.

The authority's upload allocation increases monotonically from zero to **100,663,296 bytes and 2,720 attempts**, stays within its fixed ceilings, and survives full replica replacement. These are allocated credits, including stranded rights, not billed bytes or actual successful uploads. The run retains 11,548 admission rejections, 86 HTTP 503 responses, 76 transport/decode errors, and one HTTP 409. The partition intercepts 1,590 packets. It uses 25m requested CPU, 15-second maintenance, and disposable caches; these are fault-fixture settings, not production sizing. All nodes share one physical host. B2 format-5 repetition, independent failure domains, and sustained-cost/cleanup qualification remain separate work.

## Logical backup after complete source removal

The [recovery qualification](evidence/logical-backup-2026-11-09.json) records the new binary's digest, source hashes, **94 passing regular Rust tests**, eight targeted recovery tests rerun after the final changes, formatting/clippy, and the 6,888-state backup model with four required unsafe controls. Tests use real HTTP and CLI processes, including an interrupted 16,641-key restore, resumption after verifying its prefix, exact repetition after 131 chunks evict early data receipts, corrupt/oversized archives causing no server calls, source mutation, foreign destinations, large and arbitrary binary values, archive replacement after private validation, redirects, and committed errors with identical retries.

The cloud run exports **274 records / 724,134 logical bytes** from a protected format-5 B2 database using a scoped read credential. Offline inspection validates the same digest. The launcher then SIGKILLs the source and deletes the entire disposable B2 bucket, its 20 remaining physical versions, and its workload key **before restoring** into a fresh versioned-MinIO namespace. Every value is independently compared. After killing the destination and reopening with zero caches, repeating restore verifies the same data and revision without writing new chunks. The old source credential is rejected despite the same signing key and database name, because the restored database has a fresh incarnation.

The first seed attempt stopped on a valid maintenance-fence `409`; its incomplete report and cleanup are retained. The fixture and restore client now retry identical commits within a bounded window. Creation conflicts and source read-revision conflicts still stop immediately. This finite synthetic recovery run does not establish a recovery-time SLO, retained snapshots during sustained writes, PITR, independent-region disaster recovery, backup scheduling, or archive authenticity. The [recovery procedure](recovery.md) requires protecting the archive and keeping a partial restore destination isolated until full verification succeeds.

## Format-5 budgeted B2 autoscaling

The [budgeted B2 repeat](evidence/kubernetes-budget-b2-2026-11-09.json) passes CPU-driven 1→3→8→1, node drain, SIGKILL, confirmed kernel OOM, a 40-second provider partition, and complete pod loss followed by cold recovery. It uses the same `d655c3c` static image as the MinIO repeat, with a fresh format-5 namespace, 1 GiB/100,000-attempt ceilings, 15m requested CPU, and 60-second maintenance. All **49 acknowledged writes** recover exactly; **2,128 read responses** are verified. The [history](evidence/kubernetes-budget-b2-2026-11-09.history.jsonl) passes the sequential identity/order/hash validator.

Allocation remains monotonic through replica loss, ending at **83,886,080 bytes / 544 attempts**, within unchanged authoritative ceilings. The report retains 3,380 admission rejections, 643 HTTP 503 responses, 55 transport/decode errors, and three HTTP 409 responses. The network fault intercepts 1,377 provider packets. The task's API pods and Secrets are removed before deleting the B2 bucket, its remaining 65 physical versions, and its scoped key.

An earlier setup attempt is retained in the report: applying a new Secret through `stringData` left old S3 fields in its existing `data`, and startup correctly refused simultaneous B2/S3 providers. No workload initialized. Replacing the complete task-owned Secret corrected the fixture. This is a short one-host kind campaign; it does not qualify independent hosts, rack-level faults, long-duration cleanup, AWS, or a performance SLO.

## Replicas on three physical hosts

The [three-host B2 campaign](evidence/physical-b2-2026-11-09.json) uses Woodpecker 23's `50e435c` image pinned by digest. A new, resource-limited namespace runs 1→3→6 API replicas across the three operator-documented bare-metal hosts. Four sequential writers use scoped credentials and rotate requests across the actual pod endpoints. The test forces one API pod to disappear, replaces every replica through a rollout, restricts provider egress, scales down to one, removes all API pods while writers remain active, and cold-starts three replicas.

All **130 acknowledged writes** recover exactly. The [history](evidence/physical-b2-2026-11-09.history.jsonl) passes the independent identity, per-writer invocation order, revision and hash checks. A separate connection probe succeeds before the namespace NetworkPolicy restriction, fails during it, and succeeds after recovery. Existing-connection handling remains the CNI's behavior; this is not a claim that every in-flight connection was terminated instantly. The report retains nine HTTP 409 responses, twenty admission rejections, 29 HTTP 503 responses, and 53 transport/decode errors. Upload allocation remains within its ceilings, ending at 134,217,728 bytes / 928 attempts.

The complete test namespace is removed, then the dedicated B2 bucket, its 185 remaining physical versions, and its scoped key are deleted. No host was drained, rebooted, or reconfigured. All three hosts share a rack and top-of-rack switch. This campaign uses manual replica changes: the cluster lacks the resource-metrics API, so CPU-driven HPA remains covered by the separate kind campaigns. Whole-host, rack, and control-plane faults, sustained operation, AWS, and production SLOs remain open.

Three setup attempts are retained. The first used an in-cluster registry DNS name that containerd on the hosts could not resolve. The next raw Service-IP reference attempted HTTPS against the HTTP registry, diagnosed on the third attempt. The successful run uses the hosts' existing `localhost:32000` mirror configuration with the same verified image digest. Those failed attempts initialized no database workload and their namespaces were removed.

## Unbounded abstract publication/collection proof

The [proof evidence](evidence/publication-proof-2026-11-09.json) records **35 proved TLAPS obligations**, with strict checking and fresh fingerprints. `PublicationSafetyProof.tla` establishes that the abstract live root only references existing pages for arbitrary jobs, collectors, page identities, fence advances, and execution length. The proof uses no finite TLC state constraint. A temporary model omitting the collector fence fails one induction obligation; the proof gate requires that failure.

A separate TLC regression over the same core model explores 2,346,968 states, with both missing-origin and missing-collector negative controls producing live-object-loss counterexamples. The pinned Ubuntu verification container passes the proof, toolchain checksum/version checks, and the broken-proof control. Woodpecker now requires both the model and proof stages before assembling artifacts. The [proof document](proof-publication.md) gives the assumptions, trust boundary, reproduction commands, and mapping to Rust. Implementation refinement, complete protocol composition, liveness, and independent review remain distinct obligations.

## Directory capacity at 100,005 entries

The extended [directory campaign](evidence/directory-capacity-2026-11-09.json) passes with **100,005 entries**, a three-level tree, and **2,091 remaining index pages**. It permutes insertion order, sweeps obsolete paths every 1,000 insertions, checks bounded point and paginated reads, compares the full ordered listing, updates a reused subtree, collects again, and reopens without cache before checking every head value. The format-5 test takes 547.556 seconds with an in-memory provider. This is directory-layer correctness evidence, not a cloud throughput benchmark or an end-to-end HTTP provisioning test.

The earlier attempt incorrectly asserted that this population must require four levels. A fanout-64 three-level tree can hold it. That failed fixture and its log digest are retained; it did not reach final listing/reopen verification. The corrected complete run passes. The regular 10,005-entry regression still passes for both formats 3 and 5 in 82.27 seconds together, with 262 remaining index pages each. The ordinary Rust suite passes 95 tests, formatting, and clippy. Command resource counters include compilation and must not be interpreted as the database's memory envelope.

The same change corrects backup-client acceptance of an IPv6 loopback origin such as `http://[::1]:7654`; strict HTTPS/loopback, credential, path, query, and fragment checks remain covered. Concurrent creation/collection at 100,005 databases, sustained operation, cloud performance, and implementation refinement remain separate gates.

## Actual Pi Harness recovery and cancellation — 2026-11-09

The [Harness qualification](evidence/pi-harness-2026-11-09.json) runs Pi 1.5.0 itself, using its deterministic faux model and synthetic tools. All **eight cases pass on B2 and versioned MinIO** through three cache-free API processes with automatic maintenance and a 256 MiB / 20,000-attempt cumulative upload ceiling. Cases cover a lost commit response, duplicate submission after reopen, cancelling a wait, withdrawing queued input, durable conversation abort, replacing a still-running owner, complete API-process loss during a live tool, and Harness-host SIGKILL during model requests and safe/unsafe tools. Each cloud run cold-replaces all three API processes, then continues the existing Harness. This is one physical host per campaign; no external model provider is called.

The B2 experiment found that a definite fence conflict or progress by the existing owner could incorrectly reject explicit takeover. The correction first checks the `OwnershipClaim` model, pins the initial owner across retries, and refuses any different owner appearing during backoff. Both model negative controls and a deliberately broken JavaScript check detect unsafe retargeting. The corrected eight-case B2 repeat passes in 777.69 seconds; MinIO passes in 34.90 seconds. These are fixture durations, not latency or throughput claims. Final allocated upload rights are 50,331,648 bytes / 896 attempts on B2 and 50,331,648 bytes / 640 attempts on MinIO; rights are not a measurement of actual bytes transferred.

The full local JavaScript suite passes **88 cases**, with only the cloud-only API replacement case skipped. This includes 64 upstream conformance cases, twelve indexed-adapter cases, seven Harness cases, and five real HTTP redirect regressions. The transport now rejects 301/302/303/307/308 redirects so a redirected response cannot acknowledge the original request; all five negative controls failed before the fix. Rust formatting, clippy, and 95 regular tests pass.

The initial seven-case B2 run and the later incomplete run exposing takeover are retained in the evidence summary. Their buckets and scoped keys were removed, as were all 24 remaining physical versions, the bucket, and the scoped key from the final run. Database ownership fencing prevents an old Harness from storing a new result; it does not undo external side effects. Long-running workloads, host scheduling, and fencing enforced by external tool services remain production obligations.

## Memory pilot instrumentation preflight — 2026-11-09

The [three-host B2 preflight](evidence/physical-memory-preflight-2026-11-09.json) adds a 60-second steady phase at six replicas to the existing pod-loss, rollout, egress-restriction, scale-down and complete-pod-loss campaign. All **159 acknowledged writes** recover exactly; the [history](evidence/physical-memory-preflight-2026-11-09.history.jsonl) passes the independent sequential fixture check. Four sample rounds record every API pod's UID, node, restart count, cumulative CPU and kubelet memory counters. No API pod restarts or changes identity during the steady phase. Maximum recorded container RSS is 2,691,072 bytes and working set is 3,076,096 bytes for this tiny synthetic dataset. These kubelet counters are not equivalent to a complete process heap profile or a worst-case memory bound.

The evidence retains three incomplete controller/setup attempts: a shadowed sampler function, an incorrect container-name filter, and a credential-refresh naming error stopped before the workload. The corrected run repeats the entire campaign. Its namespace, all 159 remaining physical versions, bucket and scoped workload key are removed. The instrumentation supports a bounded one-hour pilot; neither this preflight nor that pilot closes the 72-hour production soak gate.

## Client readability and a general web application example — 2026-11-09

The [client/example record](evidence/client-web-app-2026-11-09.json) records consistent formatting across the HTTP and Pi modules, exported TypeScript declarations, and the runnable `examples/web-app` task-board API. TypeScript checks the public contracts and executable examples. All **88 existing client/Pi HTTP cases** still pass, with the cloud-only replacement case skipped locally. Four additional real-database application cases pass: lost-response deduplication, concurrent task changes with atomic index removal, snapshot pagination conflicts, and optimistic settings edits.

A separate localhost smoke provisions twice using the saved creation identity, checks private scoped-token permissions, runs create/replay/list/complete/settings through the example API, and rejects a cross-origin mutation. The current npm audit reports no vulnerabilities. The fixture uses synthetic data and stops its owned processes afterward. These checks improve usability and exercise transaction semantics; they do not qualify the existing cloud design's performance.


## JSON SDK and simplified task board — 2026-11-09

The earlier example above exposed too much storage plumbing to application code. Its replacement uses the typed `Database` API: `get`, `set`, `delete`, ordered `list`, and optimistic `transaction`. The actual storage module is **48 lines**, down from 234. Task identity and the unfinished-to-completed transition provide application replay behavior; the SDK manages revisions, JSON encoding and bounded retries without adding hidden receipts. Settings deliberately use last-write-wins replacement.

The [SDK record](evidence/json-sdk-2026-11-09.json) captures **98 passing JavaScript cases** with one cloud-only case skipped, four passing application cases, and ten focused SDK cases rerun after the final cursor/error changes. A lost successful response retries identical bytes once application logic has run. A missing receipt after uncertainty returns an unresolved result instead of recomputing. Removing that implementation guard fails the regression. `TransactionClient.tla` explores 41 states and its unsafe control violates the at-most-one-effect invariant; assumptions and limits are in [verification](verification.md).

Concurrent task completion exposed retry backoff that expired before the server's admission window reopened; bounded exponential backoff with jitter fixes that observed burst. The API smoke also exposed body-limit exceptions being incorrectly mapped to 503; the example now preserves framework HTTP errors. Final setup replay, private credentials, task replay/completion, settings, validation, body limits, Host and Origin checks pass. Current audits for the client, example and site report zero advisories. This improves the application-facing API; it does not resolve the engine's cloud performance gate.


## One-hour physical B2 baseline — 2026-11-09

The [one-hour record](evidence/physical-memory-hour-2026-11-09.json) keeps six API replicas on three physical hosts for 3,600 seconds, with four sequential writers, 8 MiB memory caches, no disk cache and explicit immutable-upload ceilings. The subsequent sequence forces API-pod loss, rolling replacement, provider-egress restriction, scale-down to one, complete API-pod removal and cold restart at three. All **2,083 acknowledged writes** recover exactly, and the [independent history](evidence/physical-memory-hour-2026-11-09.history.jsonl) passes the checker. The task namespace, 131 remaining physical versions, bucket and scoped workload key are removed.

The steady phase collects 195 rounds / 1,170 pod observations with six stable pod identities and zero restarts. Maximum sampled kubelet container RSS is 16,449,536 bytes; maximum working set is 13,987,840 bytes. These counters have separate observation semantics and are not complete heap measurements or worst-case memory bounds. This tiny synthetic dataset and same-rack placement do not establish a production memory envelope or independent-host/rack recovery.

The steady phase acknowledges 1,953 writes, approximately **0.543 writes/second**. This is a baseline of the existing shared-authority engine, not a performance acceptance result. Neither the one-hour run nor its successful recovery closes the architecture/performance gate or the 72-hour declared-load soak requirement. Longer qualification should follow a candidate that first meets the intended operating envelope.


## Raw provider capacity and candidate log handoff — 2026-11-09

The [bounded raw-provider record](evidence/provider-floor-2026-11-09.json) measures the existing Rust B2 and S3-compatible bindings independently of engine publication. Six PUT and six GET phases each contain 32 operations, across 1 KiB/64 KiB objects and concurrency 1/4/16. Both backends verify all 194 objects after reconnecting, their ordered listings match, and measured phases add no authority-register reads, replacements or read barriers. All operations succeed. The B2 bucket/key and 194 physical versions are removed; the dedicated versioned MinIO bucket and 197 physical versions are also removed.

B2's 64 KiB PUT phases reach 3.57, 10.30 and 23.67 objects/second at concurrency 1, 4 and 16. Median latency is respectively 267.9, 347.2 and 387.7 ms; p95 is 753.8, 623.9 and 1,100.5 ms. These are object operations from the development host, not database transactions, a production SLO, or an apples-to-apples speedup over the four-writer engine baseline. Full attempt samples, context, failure counts and logical control-operation deltas remain in the evidence. The loopback MinIO result is not AWS qualification.

The [WAL handoff candidate](evidence/wal-handoff-candidate-2026-11-09.json) explored 1,214 states, 3,720 generated, at depth 21, with three old log positions. Uploads could finish out of order and during a stepwise recovery scan. Acknowledgement required a contiguous durable prefix; the successor fixed a durable prefix before serving and preserved it across controller loss. Five mandatory controls exposed acknowledgement gaps, early recovery, stale negative reads, late acknowledgement and reopening a seal. This model has since been replaced by the journal component model and proof below; its evidence retains the original source commit.

Local Rust formatting, clippy and all 95 regular tests pass; the new provider benchmark is opt-in. The [candidate design](design-owned-partitions.md) maps the original product brief to the replacement path and records its central remaining obligation: compose serving authority with durable acknowledgement and recovery, then implement and measure that complete path. These experiments do not establish that replacement yet.

## Combined serving/log candidate — 2026-11-09

The subsequent `OwnedWal` model checks **570,280 states**, 2,709,529 generated, depth 28. All seven required unsafe variants fail their stated invariants. One ownership transfer combines request-start expiry, permission checked after immutable upload, contiguous acknowledgement, drain, stepwise recovery, durable sealing and one new durable batch. A replacement process can have a different clock origin and must restart an unfinished drain interval. The previous model's assumed end of old acknowledgement permission is now established by the candidate's timing rules within this instance.

The positive TLC run takes three seconds; the standard checker with seven controls takes 15.753 seconds locally. These are development-check timings, not database performance. [Evidence](evidence/owned-wal-candidate-2026-11-09.json) states exact limits and hashes. The result still assumes continuously advancing bounded-rate clocks, fresh negative object reads and atomic durable control records. Repeated ownership changes, delegated renewal, general transactions, collection, admission and liveness are not established. There is no replacement runtime or change to the runtime clock rule.

## Delegated permission candidate — 2026-11-09

An isolated delegation model checks **1,258,368 states**, 6,754,337 generated, depth 15, and five required unsafe variants. It contains one acquired root grant, two independently clocked owners, delayed messages, pauses/crashes and successor activation. A child interval must fit within the parent's remaining interval after accounting for the clock-rate ratio, and must start at the child's request. Copying the full remaining duration fails. [Evidence](evidence/delegated-grant-candidate-2026-11-09.json) records source/log hashes and exact bounds.

This supports investigating one durable root renewal followed by network grants to owners; it is not a measured capacity result or a complete renewal/placement protocol. The positive TLC run takes seven seconds and the checker with five controls takes 17.551 seconds locally. Repeated renewal, reassignment, restarts, log composition, platform clock guarantees and liveness remain open. The implementation is unchanged.


## Immutable journal component — 2026-11-09

The replacement engine now has a bounded Rust journal component. It pipelines immutable frames, resolves uncertain responses by exact point reads, consumes only a consecutive durable prefix, and replays a previously selected prefix after restart. Frame reservations follow active uploads and returned record views; replay permits one live frame at a time. The existing page collector cannot reclaim its separate namespace. The current HTTP engine does not yet use it.

The accompanying model explores **4,134 states**, 18,455 generated, depth 26, and requires six unsafe variants to fail. The abstract prefix proof passes **88 obligations** without a log-length bound; removing contiguous consumption fails its induction step. It assumes an external end to old serving permission and atomic durable selection. It neither proves those controller obligations nor establishes Rust refinement or whole-database correctness. The older handoff-only model and overlapping architecture reassessment have been retired, with historical evidence and the published URL retained.

Real B2 and versioned MinIO each pass sixteen frames containing 512 records, three injected lost upload responses resolved without another PUT, reconnect, persisted-prefix replay and exclusion of a delayed seventeenth frame. The batch path makes no authority reads, conditional replacements or read barriers. Selection uses the existing publication layer as a fixture. Both dedicated buckets, all physical versions and the temporary B2 workload key were removed. MinIO is not AWS qualification; this is component correctness evidence, not transaction throughput, automatic ownership or a production operating envelope. [The complete record](evidence/journal-2026-11-09.json) contains source hashes, test results and bounds.

All 110 Rust tests pass locally, including the 10,005-database regression; opt-in cloud tests run separately as described above. Formatting and all-target clippy pass. The documentation build validates 580 local links.


## Root renewal and repeated takeover candidate — 2026-11-09

Two `RootControl` instances pass **952,597** and **3,417,821** states. They separately exercise three successive epochs and repeated root/child grants during a takeover. Five intentionally faulty variants fail the required overlap or grant-containment invariant. Two reachability controls establish successive activation and actual deadline extensions; the initial request-count-only coverage assertion was strengthened before acceptance.

The root must renew through the exact current-epoch conditional register, anchor each grant at request start, drain from fence observation and conditionally activate before delegating. Lost replies cannot create permission. This model assumes an elapsed-time upper-bound contract and does not implement or prove its clock conversion. Election, placement, log history, resource bounds and runtime composition remain open. [The evidence record](evidence/root-control-candidate-2026-11-09.json) retains exact configurations and log hashes. No production status or runtime assumption changes follow from this component result.


## Candidate timing margins — 2026-11-09

The clock arithmetic proof passes **19 obligations** with arbitrary nonnegative integer durations and measurement-error allowances. It derives a `2L + 3E` takeover drain and a conservatively rounded child grant from a factor-two rate contract with additive interval error `E`. Two temporary mutations remove the required drain/child margins and fail their proof obligations. [The evidence](evidence/grant-bounds-2026-11-09.json) records exact sources and logs. These are implications of an assumed clock contract; platform bounds, implementation and full protocol composition remain unestablished. No runtime timing rule changes.

## Delegated ownership and journal history — 2026-11-09

The replacement protocol now has an unbounded abstract proof of delegated ownership and journal recovery. The permission proof checks 687 obligations; the composed history proof checks 785. Both import the actual root transitions, and the history proof permits surviving writers to continue their journals after coordinator replacement. Replacing a writer requires a fresh generation.

Four finite permission instances and five history instances remain mandatory regressions, with unsafe variants and reachability controls. The proofs also require three permission mutations and six journal mutations to fail. [The verification scope](verification.md#candidate-delegated-journal-recovery) records the bounds and assumptions; [the history evidence](evidence/partition-journal-candidate-2026-11-09.json) pins the inputs and results.

This establishes abstract history safety under a timing contract, atomic placement/prefix selection and fresh immutable object observations. It does not establish an implementation of those contracts, a replacement transaction engine, performance, resource isolation or liveness. Production status remains open.

## Delayed control application — 2026-11-09

The delegated permission and journal proofs now admit control updates that apply after their caller's permission expires. Their 687 and 785 obligations still pass, with all nine required proof mutations. Four reachability controls require actual late freezes, move completions and prefix selections with acknowledged history, while keeping the original safety invariants enabled. This expands the atomic model toward provider behavior; it does not complete the request/reply refinement or adopt a clock-based serving implementation. [Scope and evidence](verification.md#candidate-delayed-control-application).

## Bounded transaction validation — 2026-11-09

The internal replacement-engine validator accepts disjoint-key writers from one snapshot while rejecting conflicting accepted predecessors, including writes awaiting durability. It checks scan phantoms, preserves strict whole-database revision conditions, expires snapshots when conflict history is reclaimed, and bounds metadata and range-validation work. Twelve Rust tests include 1,024 generated histories checked against a serial reference. Its preceding finite model checks 4,248,703 states, five unsafe controls and two concurrency/reclamation witnesses. [Evidence and limits](evidence/transaction-validation-2026-11-09.json).

The component is not connected to the HTTP engine and establishes no database throughput or production-readiness claim. Durable value views, ownership, journal admission, receipts, authenticated snapshots and client/API support still need composition.

## Placement request uncertainty — 2026-11-09

A finite request/reply component checks 359,761 states with four fault controls and four event-coverage traces. One continuing coordinator handles stale observations, late provider application and late replies without restoring old-generation permission. The model stops local issuance before a change and requires an exact validated view before resuming. The coverage includes a timed-out freeze that succeeds and later permits the replacement generation to serve.

This is one part of the control wire protocol. Root replacement, clocks, journal selection and the serving implementation remain open. [Scope and evidence](verification.md#candidate-placement-requests-and-uncertain-replies).

## Placement requests during coordinator replacement — 2026-11-09

The bounded `PlacementTakeover` composition checks 3,379,987 states with the actual root fence, renewal, drain and activation actions. Four faulty variants fail. Its required recovery trace contains an applied freeze with an uncertain reply, root replacement, exact placement revalidation and generation-two permission after generation-one permission ends.

It starts at a reachable active root and permits one successor. It does not prove arbitrary takeover, platform timing, journal selection, a runtime controller or production readiness. [Scope and evidence](verification.md#candidate-placement-requests-across-root-replacement).
