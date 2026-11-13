# Verification

Current local and cloud serving use the native engine. The [native cloud CLI](#native-cloud-cli), [DNS membership](#dns-membership) and [resident-owner accounting](#resident-owner-accounting) sections describe the latest integration. Earlier publication-engine and timed-ownership experiments below retain their original scope; they do not qualify the replacement. The [production gates](production-readiness.md) state what is still required.

## Native checkpoint retention

The operator control handle can retain one already published namespace checkpoint and release it explicitly by fresh UUID. The authoritative record binds that identity to the complete checkpoint descriptor, including its exact journal cut. Retain/release use the existing conditional control invocation and uncertainty handling. A losing comparison cannot resurrect an old image; a lost response requires exact validation. An empty-slot release still compares, so a delayed retry cannot remove a new slot. The retained field is bounded to 1 KiB and reserved alongside worst-case owner, collection and budget metadata within the common B2 limit. No object upload or tenant allocation is needed. Old decoders refuse the new field until explicit release removes it.

This reuses `PublicationSafety`: `live` is the union of current and retained checkpoint objects plus the existing catalog/budget roots. Retaining a published image keeps existing live references without fresh objects; release removes one root. A collector lists before fencing, protects the union, and changes the persisted generation before deleting candidate tables. Recovery publication can preserve a changed backup slot only while all recovery-origin fields, including that generation, match. No new model state or transition is needed. The unchanged bounded instance has **175,546 distinct states, 810,459 generated, depth 16**, with four required negative controls. The abstract TLAPS proof checks **35 obligations** and its negative fence control. These establish abstract reachability under the documented authority/object assumptions, not identity semantics, Rust refinement or physical timing.

Focused Rust tests exercise lost retain/release replies, stale publication after takeover and collection, cold readback of values/receipts after journal reclamation, explicit release and reclamation, identity-safe retries, malformed metadata, maximum metadata sizes and backup-slot changes during staged publication. A real TCP fixture continues writes and reads at the latest revision while the retained image remains at its old cut, then reclaims it after release. Omitting retained-root protection makes the cold-readback regression fail. The API requires an administrator's in-process control handle; it is not exposed to tenant HTTP clients. Retained-image export now feeds the existing logical restore; its catalog metadata and values use the same snapshot. The expanded TCP regression removes the source before restoring, checks two restore chunks, original quotas, empty databases/values, Unicode keys, tombstones, cancellation, corrupted reads/files and a racing output creator. Five Rust negative controls detect wrong generation, wrong quotas, exported tombstones, replacement and verification after publication. Multiple restore points, public administrative routes, scheduled backups, tenant-isolated snapshots and independent disaster recovery remain open.

## Exclusive local serving

Local `serve --data` uses the native HTTP worker, journal, index, recovery, checkpoints and collector. `api::open_local` holds the existing filesystem lock throughout ownership and refuses legacy or authority-missing native data. The controller retains the directory guard directly; the former logical startup clock is removed. Reads and acknowledgements retain the owner/lock; pending blocking writes and deletions retain their own lock references after cancellation. Forked handles refuse service. Network/shared filesystems and cloned live process memory are outside this single-host contract.

`LocalExclusive.tla` checks two fresh process incarnations and two outstanding mutations per process: **57 distinct states, 163 generated, depth 9**. It checks exclusive ownership, retention of the lock across outstanding I/O, and recovery before readiness. Negative controls release the guard on cancellation or expose unrecovered state; both violate the invariant. Pauses are stuttering; process exit assumes the kernel has retired old computation and I/O. Filesystem lock/fsync semantics and journal correctness are assumptions, checked separately from the ownership abstraction. This is not Rust refinement or cluster timing qualification.

The real CLI regression pauses a serving process, refuses a second process on its directory, resumes, kills it, recovers through two cold reopens and resolves exact retries. It creates 260 writes, observes automatic journal collection and verifies missing-authority refusal. A second test refuses legacy data without replacing it. The cancellation regression holds a blocking mutation after its caller and backend are dropped; reopening remains refused until it completes. Removing the retained guard makes the test fail. The existing checkpoint model and nine negative controls also pass. [Milestone evidence](evidence/native-local-cli-2026-11-10.json) records complete validation and the published-binary upgrade drill. Cloud ownership/placement are described below; scheduled backup retention remains open.

The original publication engine is represented by `spec/Cluster.tla`; its publication/collection properties are component evidence for the native design. Do not equate model checking with a proof that the Rust binary is correct. The model abstracts implementation details; TLC explores finite configured instances. An [unbounded TLAPS proof](proof-publication.md) now establishes abstract data-page reachability during publication and collection. Complete protocol composition, liveness under stated fairness assumptions, and a checked refinement from implementation to model remain separate obligations.

## What the model represents

Two interchangeable nodes, two fresh write attempts, immutable objects, a manifest with retained page references, conditional publication, durability receipts, collection candidate snapshots and fences, arbitrary node restarts/scale-downs, scale-ups, and storage partitions. A node can disappear at any protocol boundary. Collectors and writers can interleave. Object names cannot be reused. The epoch bound is three; no symmetry reduction hides different node roles.

Checked invariants: referenced objects exist, acknowledgements refer to durable history, an attempt is not committed twice, the current manifest is the latest committed state, and basic state types hold. The abstract durable history is a specification witness, not a claim that the database retains all historical values.

`UnsafeGC.cfg` disables the collection fence. TLC must find a reference to a deleted object; the verification script treats failure to find that counterexample as a failed test. This checks that the model is sensitive to a real protocol bug.

## Reproduce

```sh
scripts/check-model.sh
```

The script pins TLC 1.7.4 by SHA-256 (`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`). It checks the hash on every run and records detailed output under `.tools/model-safe.log` and `.tools/model-unsafe.log`. CI runs the same command. Finite-state results and test summaries are recorded in [verification results](verification-results.md).

## Per-process request admission

`RequestBudget.tla` models weighted admission, refill, captured timestamps and idle entry eviction against an independent budget that advances with processing time. Two positive instances check burst 5, rates 1 and 2, idle threshold 2 and time bound 6: **201** and **222** distinct states, respectively. Both charges of one unit and five units are possible. Allowing stale pre-lock timestamps or evicting a depleted entry must violate `NoFreeCredit`. Run `scripts/check-model.sh RequestBudget`; Woodpecker requires all four checks.

`src/api/rate.rs` uses bounded integer credit with nanosecond precision. The HTTP admission lock surrounds timestamp capture and charging, and eviction requires both 60 seconds of inactivity and a fully refilled budget. Tests cover fractional refill, old readings, burst saturation, invalid configurations and slow-refill eviction. Controlled-clock HTTP tests check shared token identity, independent principals, commit weighting, and churn of the full bounded principal map.

The unchanged interrupted-restore/receipt-eviction regression took 57.82 seconds with the old default budget and 1.07 seconds with the larger bounded fixture budget, both in release mode on the development host. All eight debug backup tests fell from 59.49 to 12.73 seconds. These are single local test runs measuring development feedback, not database throughput or a production SLO. [Evidence](evidence/request-budget-2026-11-09.json).

These are finite checks of local overload protection, not a Rust refinement proof or a durable distributed quota. New processes start with new allowances; clock rate, ingress connections, CPU/memory cost per request and cooperating principals are outside this model. The [deployment policy](deployment.md#request-budgets) and [security limits](security.md) remain explicit.

## Mapping to Rust

This table maps the earlier publication engine. Native ownership, journal and serving boundaries are described in their corresponding sections below.

| Model action | Implementation boundary |
| --- | --- |
| Begin | `Engine::commit`: fresh manifest/version and staged page changes |
| Upload | `Storage::put`: globally fresh immutable objects |
| Commit | `Storage::cas`: replace manifest only at the observed version |
| Ack | Successful API response after CAS |
| ListGarbage | Complete one bounded candidate listing |
| ReadFence / Fence | Fresh manifest read and conditional fence replacement |
| Collect | Delete only listed candidates absent from the fenced manifest |
| Crash/scale/partition | Lost volatile state, unavailable requests, and recoverable object state |

## Implementation tests

The suite checks reference-map behavior for random mutations, ordered/paginated scans over split pages, atomic conditional checks, quota refusal before uploads, ownership locking and local reopen, cache bounds and corrupted-cache misses, cross-database authorization, request-size limits, ambiguous commit acknowledgement, concurrent replicas with GC, and a deterministic pause between upload and manifest publication. A separate opt-in test exercises a real S3-compatible HTTP endpoint.

Both JavaScript adapters run Pi 1.5.0's own storage conformance suite over the HTTP service. The bounded reference adapter and implemented indexed adapter each pass 32 upstream cases; the indexed adapter also passes these cases on B2. The actual Harness additionally passes eight workflow and recovery cases on B2 and versioned MinIO through three cache-free API processes. These tests establish the recorded finite workflows, not sustained performance or exactly-once external effects.

The production hardening tests add malformed manifests, foreign object references, invalid accounting, page boundary/length mismatches, duplicate map entries, trailing bytes, and memory-cache digest mismatches. Rejected manifests must not allow reads, writes, compaction, or collection to proceed. A streaming-body HTTP test holds a database's full concurrency allowance, verifies that another database and the administrator can still work, and checks cancellation releases the allowance. These changes validate inputs and admission; they add no publication or collector transitions to the existing models. They do not prove arbitrary corruption recovery or strict global resource isolation.

## Remaining proof and qualification obligations

- Extend the unbounded abstract page-reachability proof to the complete protocol and its implementation. TLC's finite checks remain separate evidence.
- Liveness under eventual provider availability, fair admission, and bounded competing maintenance. Availability is impossible during arbitrary permanent partitions or when all nodes remain down.
- Implementation/model refinement, filesystem and network cancellation, malformed/corrupt object behavior, allocator/resource bounds, cryptographic assumptions, and schema upgrades.
- Whole-host/control-plane faults, independent rack failure domains, long-duration soak, and AWS qualification. Historical publication-engine tests pass CPU-triggered HPA, drain, SIGKILL, injected kernel OOM, provider partition, and all-pod loss on a three-node kind cluster with real B2 and MinIO. A separate three-physical-host B2 campaign passes manual scaling, pod replacement, egress restriction, and cold recovery. Those hosts share one rack; host shutdown and rack loss have not been injected.
- Independent security review and strict cluster-wide physical-storage/spend admission.

The repository must keep these limitations visible until the corresponding evidence exists. “Proven in all situations” is not an accurate description of this initial release.

## B2 authority refinement

`Bucket.tla` models the immutable manifest layer selected by bucket metadata. `publication::Published::cas` implements Upload→Publish; `collect_manifests` implements List→Read→Fence→Delete. Native `ifRevisionIs` must implement atomic revision comparison; bucket listings may be stale because `Authority::read_fresh` validates them through a conditional update. Physical file deletion removes versions, not markers. A successful head read supplies the unique manifest identity as the engine version. A bucket revision conflict is safe even when another database caused it.

The shared publication adapter tests exercise injected revision authority, paused publication, lost acknowledgments, page, manifest, and index collection, empty-cache recovery, and bounded metadata admission. Real-provider qualification and a mechanized composition proof remain separate obligations.

The publication model now applies to both providers. B2 revision numbers and S3 opaque object versions implement the same compare-and-publish boundary. `ReadBarrier.tla` checks stale directory observations with two readers, three values, and four revision advances. Its negative control skips validation and returns a stale snapshot. Provider-specific wire tests complement these abstractions; no bounded model run proves the whole implementation.

## Persistent directory

`Directory.tla` models immutable leaves and ordered, nonoverlapping branch ranges, including reuse of existing nodes under new roots. With four keys and five allocated nodes, it checks that a node is reachable from a root exactly when that node appears on its witness-key search path. The negative control uses the wrong witness and must violate `WitnessReachability`. The model does not establish Rust B+tree balancing, checksums, or publication composition by itself.

The Rust stress test creates 10,005 databases in permuted order, forces a three-level tree, paginates every entry (including prefix/suffix ordering edge cases), updates an existing entry, sweeps obsolete index pages, and reads every database from a fresh uncached backend. Deterministic publication/collection races and lost acknowledgements also run through the tree. Provider tests separately cross the leaf-split boundary on real B2 and S3-compatible HTTP storage.

## Publication retries under read traffic

The first format-3 multi-process B2 run exposed write starvation from maintenance read barriers: every barrier changed the register revision while a writer uploaded its path. `PublicationRetry.tla` was added before implementing bounded retries that compare both the original root and original collection fence. The model includes uploads, read barriers, publication, list/read/fence/delete interleavings, and abandoned attempts. With one publisher, one collector, two attempts, and four revision advances, it explores 44,354 distinct states. Omitting the fence comparison produces a deleted-manifest counterexample. The Rust tests pause a publisher, advance read barriers, and separately change a root or run collection to verify the retry boundary. This is a safety result within those bounds, not a general fairness/liveness proof.

Idle page collection now skips publication when it has neither deletions nor a cursor change. That is a stuttering step in the existing collection model; no object is removed without a successful fence.

## Publication rebase across unrelated writes

`PublicationRebase.tla` extends the retry argument to changes in other databases. It models a staged publisher, a competing write to its target, unrelated database updates, stale raw observations, read barriers, list/read/fence/delete collection, and abandoned attempts. With four revision advances it checks 195,642 distinct states (827,904 generated, depth 24). A rebase must compare the target pointer and original collection fence and preserve every unrelated entry from the root used for its next conditional write. Three negative configurations independently omit these checks: they expose a deleted published manifest, an overwritten target write, and a lost unrelated write.

The original implementation retained an eight-attempt bound and rebuilt paths after definite precondition failure. Its tests covered competing target writes, unrelated writes, collection, and receipt recovery. The publication combiner below now owns this retry boundary and extends it to durable proposals and uncertain outcomes. The rebase model remains a useful independent check of the target/fence/merge rules. It abstracts index allocation into the root transition; composition, arbitrary-count safety, progress/fairness, and Rust refinement remain obligations.

## Sealed observation groups and immutable caching

`ObservationBatch.tla` was checked before implementing grouped read barriers. It models three readers, two unique write versions, one batch, stale observations, delayed responses after successful validation, cancellation, and abandoned work. All 25,194 distinct states (71,381 generated, depth 13) satisfy `ObservationWithinInvocation`: each returned snapshot's version falls within that reader's own invocation/response interval. The negative control admits readers after sealing and returns a pre-invocation snapshot. One batch suffices to expose this bug; arbitrary batches, a composition/refinement proof, and liveness remain separate obligations.

`observation::Observations` seals at most 64 members before calling the existing `Authority::read_fresh`. Another 64 observations may queue. A deterministic test delays a successful barrier's acknowledgement, acknowledges a competing write, then starts another reader: only a second validation may serve that reader. Another test fills the queue, cancels members, injects a failed barrier, and verifies subsequent recovery. No result is shared with a request admitted after sealing.

Remote immutable manifest bytes now share the existing bounded cache. Their unique object name and expected digest come from the freshly validated directory. Memory and disk tests cache a 300 KiB manifest, enforce a smaller caller read bound on a cache hit, replace and remotely collect that manifest, and verify a later request reads the replacement despite the stale bytes remaining cached. Directory cache hits avoid rewriting the same disk file. These cache changes add no authority transition; missing old bytes still restart the complete read, and mutable local heads remain uncached.

Automatic maintenance also shares its initial manifest between compaction and no-op collection. The initial snapshot may justify only a stuttering step: no deletion, cursor update, or publication. A race test pauses after listing, commits a new value, resumes the no-op pass, then verifies the next pass collects only the obsolete object. A publication test checks that empty and one-page idle databases perform exactly one barrier and no directory rewrite. Actual deletion retains the modeled list/read/fence/delete sequence unchanged.


## Cross-replica publication combining

`PublicationCombining.tla` was checked before implementing durable proposals. It models two fresh jobs, two database targets, two helpers, and two authority revision advances in the default check. Jobs can use genuine stale observations, upload, submit, disappear with an unknown outcome, receive success, or be definitely aborted before submission. Helpers select independent targets and race a conditional group publication. Original unbatched publishers, read barriers, collection candidate snapshots, fence publication, manifest/proposal deletion, and abandonment are included. A committed proposal may be cleaned up without advancing the fence; already loaded proposals remain subject to the original target comparison.

The default two-revision instance now permits helpers to select retained
metadata from any previously submitted proposal, even after proposal deletion.
This overapproximates the runtime's one bounded discovery batch per invocation;
every selection still rechecks the target and original collection fence. It
checks **1,736,213 distinct states** (15,818,936 generated, depth 25) in 60 seconds
on the development host. Six fault controls and the superseded-receipt coverage
control still produce their expected counterexamples. A one-revision coverage
check restricts receipt delivery to when a helper is already busy. Its one-job,
one-database instance reaches that interleaving in 582 states (depth 9). `Ack`
already permits this independently of helper admission; no new
protocol state or safety relaxation is needed. This does not prove Rust,
latency, fairness or whole-deployment behavior.

Publication retries retain at most 64 loaded proposal records. A definite bucket
comparison conflict no longer repeats listing and proposal downloads, or the
duplicate authority read and target lookup after reusing that batch. Current
heads, collection fences and manifest availability are rechecked before each
fresh directory rewrite and exact comparison. If a definite conflict changed
only register metadata, retries retain the completed directory and selected
pointers while the exact directory root and original fence remain unchanged.
They preserve the new metadata and compare its exact revision. A changed root
requires selection and path copying again; a changed fence refuses publication.
No uncertain PUT is replayed. `Refresh` models this same-heads/same-fence step
without adding protocol state. A one-helper fault configuration omits the
directory equality and detects lost unrelated writes. The regression forces two conflicts and checks
one LIST, one proposal GET and one directory upload, with both acknowledged and
applied-but-lost final responses. The previous code uploads three directory
pages. Omitting the root comparison fails the strengthened competing-directory
regression. Proposals arriving after discovery can be helped by a later invocation.
The existing suite covers competing writes, collected selections, abandoned
origins, bounded batches and subsequent progress. Fixed diagnostic fields split
initial work, local helper queueing, preparation and comparison duration without
logging database names, keys, values or provider errors.

Queued publications can now observe that exact receipt without taking the helper
slot. After each one-second delay, at most eight attempts check the original
proposal identity; only one such probe may be in flight per shared connection.
Busy probe admission skips that attempt. These read-only checks never list
proposals, upload objects, compare the authority or grant serving permission.
A missing/changed head, failed read or collected directory cannot supply a
receipt. The caller keeps its FIFO helper position; admission cancels an in-flight
probe, and cancellation leaves no detached polling task. Once the probe budget
is exhausted, ordinary helper admission and its uncertainty rules still apply.
There is no fairness or cloud-latency guarantee for these optional checks.

Two virtual-time regressions cover another replica publishing the queued proposal
while the local helper is stalled, including a lost publication reply. They also
check the eight-probe bound, absent/different pointers, a failed read, one-probe
concurrency and helper admission cancelling a stalled observation. The original
semaphore-only path fails the receipt regression. `queued_receipt` diagnostics
record no comparison attempt; their queue duration includes observation work.
The uncontended helper path starts no probe.

Publication receipts now return the directory's exact published pointer, including
when another helper confirmed it. They do not require a subsequent metadata
read. The premature-receipt control rejects unconfirmed success. A two-job,
one-database, one-helper coverage check reaches a successful receipt whose target
has since changed. This does not grant serving authority: `RootAcquisition`
binds the delayed result to its invocation, and response permission remains
separate. Rust regressions block all metadata reads after a successful CAS and
race a newer publication against delayed success delivery; stale tokens must
still fail their next comparison. Lost publication replies remain uncertain.

The historical three-revision run checked all 13,049,720 symmetry-reduced states (84,918,974 generated, depth 29) against the seven checked invariants: types, referenced-manifest existence, original target comparison, unrelated-write preservation, no duplicate publication, acknowledged history, and no commit after a definite abort. Symmetry renames only jobs among jobs and helpers among helpers. A separate run without symmetry also passed: 42,716,288 states, 279,275,713 generated, depth 29. Reproduce that larger run with `PublicationCombiningNoSymmetry.cfg`. TLC uses probabilistic state fingerprints; these runs are finite model-checking evidence, not a mechanized proof. Four intentionally unsafe configurations omit the collection fence, expected target, current-root merge, or post-submission uncertainty rule.

| Model boundary | Rust boundary |
| --- | --- |
| Upload / Submit | `publication::proposal::Published::publish` uploads a fresh manifest, then a bounded immutable proposal |
| Select | Bounded listing, strict proposal namespace/schema checks, target/fence comparison, and manifest existence/digest checks |
| Refresh | Retain the completed directory across a metadata-only conflict; recheck the original fence and exact base root before comparing fresh metadata |
| Publish | `Directory::insert_many` completes all final reachable uploads, then `Authority::replace` compares the observed authority version |
| Ack / Unknown | Success requires the selected unique identity or an acknowledged group CAS; every other outcome after proposal submission remains uncertain |
| List / Fence / Delete | `Published::collect_manifests` lists all three namespaces before fencing and deletes only authorized candidates |
| DeleteProposal after commit | Bounded optional cleanup queue; failure leaves work for the durable collector |

Deterministic tests abandon an originating task, let another replica publish it, lose a proposal-upload response, lose a whole group's CAS acknowledgement, race a same-target writer, and collect an entire prepared selection before its CAS. Recovery preserves the original request body and receipts. Malformed proposals cannot authorize a write. A 65-proposal test verifies the 64-target group bound and cursor wrap for deferred work. Three separate replica instances race both collectors. These tests do not depend on sharing one helper semaphore.

The bulk directory implementation reuses the single-entry tree algorithm in a bounded staging overlay. Tests show that a 64-entry leaf batch uploads one final page, malformed batches reject before I/O, and 64 mixed updates/inserts across a 4,096-entry three-level tree preserve the original root, ordering, pagination, witness reachability, and cache-free reads after reclamation. Intermediate paths are never uploaded or inserted into the cache. One helper per process limits staging to 64 MiB and 1,600 pages; remote fetches, final uploads, and optional proposal deletions each have concurrency eight. These are implementation bounds, not an allocator/RSS proof.

The model abstracts B+tree allocation, data-page collection, and API request identities. Composing those layers, proving arbitrary-count safety/liveness, and refining Rust and provider behavior remain open. The unchanged format number additionally requires mixed-version qualification; a model of an abstract old publisher alone does not qualify an old binary.


## Durable authorization lifecycle

`AuthorizationLifecycle.tla` was checked before implementing durable authorization. The configured instance has two operations, two fresh token identities, and three manifest-version advances. It includes policy activation, old-server format refusal, individual revocation, generation advancement, stale observations, a caller disappearing after preparing a write, and publication by another helper. Policy changes advance the manifest identity without changing the user data revision.

All 472,151 distinct states (874,110 generated, depth 12) satisfy type checks, policy preservation, authorization at the operation's linearization point, and read snapshots within invocation/response intervals. Four negative configurations expose authorization against an older snapshot, omission of the credential generation, comparison of only the user revision, and skipping read validation. The `*.checks` descriptor requires each corresponding counterexample.

The model abstracts one database incarnation and trusted signature verification. Keyring rollout, expiration/clock assumptions, deletion/recreation, administrative receipt recovery, data-page/directory composition, and implementation refinement remain obligations. Rust `Manifest::authorize` now checks the exact snapshot used by info, reads, writes, and receipt replay. `Engine::change_policy` publishes format 2 with an unchanged data revision and a fresh manifest identity; old engines reject that format. Tests exercise activation/revocation racing a prepared write, an abandoned proposal rejected by later helpers, a missing-page read retry, lost policy responses, bounded receipts, and malformed policies. These are implementation tests, not a proof of refinement or composition.


## Database creation identity

`DatabaseCreation.tla` checks the protected-creation design before implementation. Two callers choose among two request IDs and two initial configurations. Both may prepare while the name is absent; a helper may publish after the caller disappears. Later mutations advance the data revision while retaining the creation identity. Exact retries return the original result. With three manifest-version advances, all 1,177 distinct states (2,993 generated, depth 10) satisfy type checks, receipt retention, exact request/configuration identity, and original-result recovery. Four mandatory negative controls lose the receipt, ignore the configuration, accept any existing name, or return mutated current state instead of the original result.

This small model assumes the previously modeled exact absent-target publication and validated reads. It does not compose directory/collection behavior, model deletion/recreation, or prove digest/UUID properties or the Rust implementation. The permanent creation receipt must survive every data, policy, and maintenance update; a fresh observed exact receipt is the recovery witness after uncertainty. The Rust implementation stores `CreationReceipt` in every successor manifest and requires a matching ID and canonical initial-configuration digest to recover success. Initial policy and receipt publish atomically. Tests cover ambiguous acknowledgements, exact/competing creators, helper publication after abandonment, malformed receipts, collection, and retention after both bounded receipt windows roll over. Real B2 and versioned MinIO executable tests verify cold recovery and old-format refusal. These are implementation evidence, not a refinement proof.


## Pi Session ownership and durability

`PiSession.tla` precedes the indexed adapter implementation. Its two Sessions, two batches per call, and four database-version advances cover takeover, prepared/unknown publication, valid-prefix storage, ordered sequences, and post-publication receipts. All 5,055 distinct states (12,415 generated, depth nine) pass type, owner fencing, durable receipt, valid-prefix, and sequence invariants. Three negative controls find stale-owner publication, an early receipt, and an invalid suffix. This does not model record/index validation, document materialization, physical database publication, or full retry behavior; those remain composition and implementation obligations. The indexed adapter now compares authoritative owner/metadata and database revision on every operation, stages each valid prefix in one transaction, and emits receipts after publication. Independent HTTP tests cover takeover, a paused stale writer, receipt eviction after 130 unrelated writes, provider failure during validation, callback failure, and read restart. Record/index and document semantics are checked by all 32 upstream cases plus bounded-resource tests. This evidence does not discharge the composition/refinement obligations.


## Overlapping signing-key rollout

`SigningRotation.tla` checks two serving processes, two bounded issued tokens, two keys, and the expansion/promotion/retirement sequence described in [the key rotation design](design-signing-rotation.md). The positive configuration explores 1,189 states (4,360 generated, depth 14). Both negative controls fail the rolling-interoperability invariant: premature promotion and retirement of a still-live token's key. The model includes independent crashes, all-process loss, and restart from the current configuration. Serving partitioned processes must remain inside the completion barrier; that requirement is an assumption, not a Kubernetes fencing proof. Cryptography, clock skew, configuration delivery, offline issuers, and Rust verification are not established by this finite model.


## Nonrefundable immutable-upload credits

`UploadCredits.tla` checks two processes, two grants, three requests, an initial ceiling of two units raised up to four, and up to two upload attempts per charged request. The current model explores **11,006 states (41,209 generated, depth 17)** with crashes, all-process loss, missing acknowledgements, additional grants preserving existing rights, and delayed arrivals. Its stronger coverage invariant includes both charged units and all remaining usable rights. Five negative controls detect refunding dead-process credit, restoring spent credit, uploading before charging, and resetting allocation during replenishment or namespace initialization. Initialization precedes every upload, including ownership registrations. The Rust allocator applies the same boundary to bytes and attempts, replenishing only deficient dimensions. Built-in transports use one immutable PUT attempt per invocation. Tests cover concurrent replicas, zero-byte uploads, lost allocation/refill acknowledgements, retained credit after a lost additional grant, late arrivals, receipt eviction, administrator HTTP isolation, and proposal recovery after exhaustion. The finite model does not prove billing bounds, HTTP refinement, tenant isolation, migration, or composition with publication/collection. See [upload admission](design-upload-admission.md).

Catalog admission uses the target database key as the payer for protected creation; the internal catalog identity only determines encoded size and affected checkpoint paths. The existing `TenantAdmission` model now applies its cross-scope negative control to admission as well as construction claims, with unchanged safe bounds/state count. The Rust regression covers zero-budget creation, lost grant replies, funded-neighbor progress, a public database sharing the catalog name, refill after exhaustion, pending/committed retry reuse and credential revocation despite tenant exhaustion. Policy operations remain namespace-funded. Shared catalog construction claims and ledger metadata still need attribution.

The native connection now supports the same cumulative allocator through `Connection::with_upload_budget`. It conditionally initializes budgeted `owned-candidate-23` before any registration PUT. Lost initialization replies and reopen preserve the installed limits and allocation; unbudgeted and older namespaces refuse activation. Every registration, journal, table, checkpoint and catalog upload uses the wrapped object interface. Grants can invalidate a previously captured root comparison: that claim conflicts and a fresh incarnation retries using retained confirmed credit. Neither abandoned registrations nor lost connection-local rights are refunded. Metadata admission reserves space for the full receipt window, root and read probes within B2's ten fields.

Five focused native checks cover initialization loss, namespace/mode refusal, bootstrap conflicts, exhaustion before journal upload, refill/replay, lost allocation acknowledgements, delayed arrivals after replacement, and B2/S3 HTTP comparisons. The existing native HTTP fixtures now enable budgeting, including uncached checkpoint reads, cancellation and automatic checkpoint retries. The TCP lifecycle test exercises administrator-only budget status and idempotent ceiling increases. The existing allocator and its earlier regressions are reused; there is no second accounting algorithm. The immutable-upload ceiling remains namespace-wide; separate database write admission is described below. Non-upload control traffic remains outside the allowance. Automatic reclamation is described below. Shared maintenance attribution, sustained cloud qualification and the host-timing CLI gate remain open.

`native_http_refill_resumes_unsent_journal_without_replacing_the_owner` exhausts the allowance on registrations and database ledger initialization, leaves a creation waiting without issuing its journal PUT, then funds the ledger through the administrator route. Creation completes on the same owner with exactly one data PUT (1.01 seconds locally). A private admission witness returns the original upload handle only before the object provider was called; its buffer reservation and position survive. Existing ambiguous/missing-response tests require that no retry handle is returned. The worker retries at most one unsent position per second and does not issue resolving GETs for it. This is a stutter before `Upload` in the existing journal model; its bounds and negative controls remain unchanged.

`TenantAdmission.tla` directly checks two names, one grant and accepted write per name, one owner replacement, three ordered construction attempts, lost replies and explicit refills. **10,402 distinct states (39,228 generated, depth 17; under one second)** preserve same-name durable claims, combined admission/reserve coverage, allocation below ceilings and stale-owner exclusion. All six negative controls fail: unfunded uploads, cross-scope debit, omitted prepayment, recreated admission rights, refunded claims and stale owners. It replaces the previous two-instance wrapper without adding another maintained model. Atomic directory replacement and name binding remain assumptions. The physical `UploadCredits` model remains separate; neither checks byte sizing, arbitrary deployments, complete attribution, fairness, liveness or Rust refinement.

The Rust reserve regression interleaves a multi-tenant claim with a refill, loses a committed reply, refuses an underfunded group without partial debit, replaces the owner, consumes retained unissued reserve and then requires an explicit refill. It also checks that retry spending reduces unused admission rights. The existing cancellation/virtual-time fixture now checks durable charges for nine attempts and no new claim when resolving staged publication. A collection-origin race must refresh control before retrying. Eight Rust mutations detect spent-reserve reuse, missing admission-rights reduction, stale-owner claims, physical overruns, a catalog-name collision, missing control refresh, unsafe metadata rebasing and large-group self-conflict. A 70-tenant group crosses physical-grant and directory-batch boundaries, then races collection of its unpublished ledger objects. Full-image sizing tests enforce the actual upload allowance, including lost PUT replies.

Tenant ledger initialization, refill, admission grants and maintenance claims share one publication path using `PublicationRetry`: **44,354 distinct states, 117,886 generated, depth 21**, with its required unsafe-rebase counterexample. Its head is the tenant-directory root, and every budgeted metadata sweep advances its fence. Ordinary control updates may change the nonce without changing either. Unbudgeted local collection omits the new fence field, preserving its existing format. Byte charging, identity checks and the Rust implementation remain separate obligations.

The metadata retry regression pauses initialization, refill and grant publication across an unrelated read barrier. Each operation retains its original ledger and directory objects: six objects for the three operations, without retry copies. Permanently stale comparisons stop within eight attempts without more uploads; a changed format fails closed. Existing competing-ledger, lost-response and collection races still pass. Four Rust negative controls independently force reuploads or omit the directory, metadata-fence or format check. This reduces retry amplification; administrative uploads remain namespace-funded and per-tenant attribution is still open.

Budgeted native format `owned-candidate-23` charges five times the authenticated data-object size of each base table that a transaction can replace, plus bounded new manifest/split work and the existing leaf and ancestor bytes, deduplicated across affected paths with nine temporary references. Manifest seeks cover the database's metadata/receipt interval and every mutation key, charging each routed table once. This includes unchanged neighboring rows, expired receipts, replacements and tombstones. New generations and gap inserts pay for the table they displace. Appends after all base tables pay for the rightmost manifest path without copying table rows. Sorted later append ranges need no additional traversal. Missing or corrupt path evidence fails admission before journal assignment. Distinct writes can pay repeatedly; pending/committed/identical same-batch retries reuse admission. Pricing reads no values or table indexes and uses the existing bounded manifest cache. The old recorded-data-plus-index allowance is removed because reblocking can enlarge the index.

For a checkpoint row with key length `k >= 19` and value length `v >= 0`, singleton data/index encoding is `106 + 3*k + v <= 5*(14 + k + v)`. A data object already includes all its row encodings and block headers. Its fivefold size therefore bounds any repacking of retained rows, even splitting each into its own table. Direct new metadata, receipt and mutation rows separately pay their singleton encoding, `row_bytes + 92 + 2 * composite_key_bytes`. This is conservative admission, not exact usage or provider billing. It adds no I/O, allocation, stored metadata or new table encoding. The algebra is separate from the finite credit/assignment model, which abstracts byte amounts. Unbudgeted storage retains `owned-candidate-15`; older budgeted policies are refused before mutation.

The adversarial eight-block fixture starts with short boundary keys around unchanged 1 KiB keys. One small insertion moves those large keys onto block boundaries, producing **533,678** table bytes against the old **526,021** allowance. The allowance is **2,819,918** bytes including new rows and manifest growth: safe but deliberately loose. Cold verification preserves all rows and receipts. The shared-table fixture covers neighbors, gaps, duplicate routes, dirty writes, expiry and missing pricing evidence. New table boundaries and split propagation use the [manifest growth and packing bounds](design-upload-admission.md#native-serving). Full-image tests count actual uploaded bytes, including node headers and newly created manifests, for minimum/maximum fields, deletion markers, row/byte chunk limits, and multi-level splits with maximum-length keys. The finite credit model does not establish this sizing argument or implementation refinement.

The 183-table fixture prices three affected tables and their paths with the same four cold manifest reads and no table reads. Append pricing reads only the root and rightmost leaf, then makes no provider reads when warm. Removing the rightmost leaf fails closed. Real TCP tests refuse one byte below the new allowance before allocation/PUT and preserve neighboring writes and exact retries. Maximum-field, singleton and multi-table encoding tests remain in place. Administrative catalog/ledger writes remain namespace-funded. Tenant grants now separately prepay durable construction reserve; every retry or recovered construction claims fresh allowance before uploading. The physical namespace allocator still applies to every PUT.

Name-bound immutable ledgers share the existing directory and namespace allocator. Tests cover concurrent allocation, lost acknowledgements, eviction/reopen without restored credit, independent byte/attempt exhaustion, mixed batches, exact retries and neighbor progress. Checkpoint publication revalidates stale comparisons and reuses staged objects only while their collection generation remains valid. Real TCP tests check administrator isolation, exact funding/refusal boundaries, replenishment and continuation without a rejected journal position. Current results and cloud outcomes are recorded in [verification results](verification-results.md#v0180-durable-maintenance-funding-2026-11-10).

Native serving automatically sweeps **budget/catalog directory metadata, checkpoint-covered journals, obsolete table/manifest objects and S3 authority versions** once per minute, outside the owner lock, with at most one sweep in flight and a 30-second timeout. Each sweep lists at most 128 candidates in each of the shared manifest, directory-page, journal and checkpoint-object prefixes before a confirmed nonce-changing comparison. The existing directory collector performs witness-path reachability tests and physical version deletion. Durable cursors survive process loss; uncertain comparisons delete nothing. No immutable upload or refundable credit is involved. Current ledger entries are never removed, so old spent balances cannot become new allowance. Missing protected paths fail closed. The worker invokes the provider’s existing control-history maintenance even with no tenant metadata; S3 retains the current version and skips mutable `null` versions. The virtual-time regression covers both empty and populated namespaces, with bounded task settling instead of polling against a stopped virtual timer.

The [native HTTP provider qualification](evidence/native-journal-collection-2026-11-10.json) passes on real B2 and versioned MinIO at clean commit `670beab`: exhausted-tenant refusal without another PUT, neighbor progress, idempotent refill, automatic metadata/journal collection before and after takeover, and two complete cold recoveries of balances, values and retry receipts. The first sweep reduces five journal objects to one uncovered frame; after takeover and another checkpoint, no journal objects remain. Both checkpoint-backed and recovered-suffix paths serve 100 warmed point-and-scan HTTP reads with zero authority calls and zero object GETs. Initial cloud runs exposed obsolete deletion allowlists in both adapters; one shared canonical-key validator and wire regressions replaced them. All disposable resources from failed and successful runs were removed. The fixture uses a logical clock and stops the old server before takeover; it does not qualify physical timing, AWS S3 or sustained performance.

A paused publisher test collects its staged metadata before releasing its old comparison; the allocator rebuilds before obtaining rights. Lost collection responses delete nothing. Exhausted namespace credit still permits cleanup without touching tables or resetting either ledger. A 134-name test resumes paginated sweeps through new collector instances, retaining all live counters. Both B2/S3 HTTP fixtures retain allocated credit after physical metadata collection. A virtual-time HTTP test exercises the automatic worker and preserved data reads. The duplicate manifest/index deletion loops were replaced by one shared directory collector. The common `PublicationSafety` model/proof is reused, not duplicated: 35 TLAPS obligations and its missing-fence control pass, plus all three TLC controls. The finite regression is reduced from three page identities to two (26 seconds to 2 seconds locally); the theorem remains unbounded. Complete Rust refinement, durable reader/backup retention and sustained cleanup progress remain open; native table collection is described below.

The post-takeover regression exposed a runtime violation of the publication proof’s assumptions: the native collector protected only the budget directory, although recovery catalogs share its `manifests/` and `indexes/` prefixes. It deleted live catalog pages and prevented cold replay. The collector now protects the union of both roots from the same compared authority. The test failed before the fix and passes across three owners, including a catalog without tenant ledgers, matching tenant/catalog entry names, collection during a paused catalog publication, and missing/corrupt catalog refusal. Native format `owned-candidate-9` excludes earlier unsafe collectors; no in-place migration is provided.

`PublicationSafety` already assumes complete coverage of live references. Its new omitted-protection negative control demonstrates why that assumption is necessary; the unchanged safe instance explores **175,546 distinct states (810,459 generated, depth 16)** in two seconds. All three negative controls find the expected violation. The existing unbounded abstract theorem still discharges 35 obligations, with its missing-fence proof control rejected. These checks do not prove Rust refinement. Full local validation passes 274 Rust tests, formatting and strict clippy; the opt-in/cloud cases and release capacity regression run separately. The existing collector and cloud fixture were extended, with no parallel collection implementation. Native table/manifest reclamation is described below; catalog-header reclamation remains open.

## Data collection without immutable uploads

`GlobalCollection.tla` models original engine observations, fresh uploaded pages, retained references from previous manifests, durable proposals, helper publication, process disappearance/restart, and list/read/conditional-fence/delete sweeps. One configuration uses two processes, two jobs, two databases, one collector, and three revision advances: **1,620,524 distinct states (6,374,776 generated, depth 27)**. A second uses two concurrent collectors, two jobs/processes, one database, and two advances: **6,442,262 states (32,586,881 generated, depth 28)**. Three negative controls detect omitting the engine's original fence, omitting the collector fence, and listing after fencing. Delayed uploads and deletion are included; finite exploration and fingerprinting still carry their usual limits.

Format-5 `Published::get` carries the original collection fence in the engine's version token. `publish` compares it before proposal submission; helpers retain their existing proposal-fence comparison. `collect_data` lists at most 128 candidates, observes and conditionally fences the directory, validates protected manifests, and deletes only absent listed references. It never invokes immutable PUT. Missing indexes/manifests and invalid content fail closed. Tests pause an uploaded writer across collection, invalidate an exhausted proposal, race two writers with two collectors, retain earlier live pages, restart between cursor pages, and preserve data after uncertain fences. A temporary Rust negative control removing the origin comparison fails its regression. Format 4 retains its earlier behavior and is never silently upgraded. The new observation format requires fresh initialization; provider qualification, unbounded proof, model composition, and implementation refinement are distinct obligations. See [the collector design](design-budgetless-collection.md).

## Logical export and restore

`LogicalBackup.tla` models ordered scans at an observed revision or from an immutable retained image (catalog metadata and values together), interrupted/incomplete export, complete archive publication, protected destination ownership, prefix verification, delayed restore chunks, missing responses, and restart. Its two-key/two-value configuration with three delayed requests and two source advances explores **6,888 distinct states, 40,320 generated, depth 16**. Four required negative controls detect mixed revisions, truncation, foreign destination takeover, and missing restore revision checks.

`backup::export` checks every scan revision and final source identity before completing a checksum footer and publishing its private file. `backup::archive::Verified` privately spools and validates all records before any restore request. `backup::restore` recovers a creation identity derived from archive digest and target name, verifies previously committed chunks at one target revision, and continues deterministic expected-revision writes before verifying the entire result. Tests include actual HTTP interruptions, committed service errors, receipt eviction, archive replacement after validation, quota/encoding bounds, source mutation, existing destinations, and CLI export/inspect/restore.

The model assumes atomic ordinary database operations, collision-free identity/integrity checks, and an isolated destination. It abstracts the HTTP, cryptographic, and filesystem layers and does not establish retained snapshots, PITR, scheduling, or a composed unbounded recovery proof. The [operator procedure](recovery.md) describes the partial-destination and concurrent-source-write boundaries.

## Unbounded page reachability

`PublicationSafetyProof.tla` proves the abstract page-reachability invariant for arbitrary jobs, collectors, page identities, and execution length: 35 strict TLAPS obligations pass. A temporary missing-fence mutation fails the proof. The same core model passes a 175,546-state TLC regression with two jobs, two collectors and two page identities, and both missing-origin and missing-collector fences produce live-object-loss traces. The [proof scope and reproducible procedure](proof-publication.md) enumerate provider/freshness assumptions, trusted tools, and the outstanding composition/refinement obligations.

## Pinned-owner Harness takeover

`OwnershipClaim.tla` models a claimant that pins the first observed owner identity, retries definite revision conflicts, and checks every fresh snapshot before publishing. Same-owner progress, release, a competing new owner, unknown acknowledgements, and backoff interleavings are explicit. A takeover must never silently retarget a competing owner. The matching JavaScript code is `openIndexedPiStorage` in `clients/javascript/pi-indexed.mjs`; `indexed.test.mjs` injects both same-owner conflicts and a competitor appearing after conflict resolution.

The configured instance explores **735 distinct states**, 2,001 generated, at depth 11, with two other owners and five revision advances. Both unsafe configurations must violate `PinnedTarget`: permitting retargeting, or omitting revision comparison. A separate implementation mutation removes the post-backoff check and must fail the competing-owner regression. This finite model neither proves liveness nor composes all database and Harness behavior. Reproduce with `scripts/check-model.sh OwnershipClaim`; the [Harness evidence](evidence/pi-harness-2026-11-09.json) records source hashes and cloud outcomes.


## JSON transaction retry boundary

`TransactionClient.tla` precedes the JSON SDK implementation. It models one logical operation, successful publication with a lost response, exact receipt replay, competing revision advances and bounded receipt eviction. Once a commit outcome becomes uncertain, a later conflict cannot authorize recomputing the operation. The client must return an unresolved result instead. This prevents a late retry from turning one logical invocation into two database effects.

The configured instance permits competing advances below revision three and explores **47 distinct states**, 56 generated, at depth eight. Two negative controls detect recomputation after uncertainty and treating an explicitly expired receipt as a retryable conflict, including after client restart. Both violate `AtMostOneEffect`. Run `scripts/check-model.sh TransactionClient`. The model abstracts callback execution, HTTP classifications, request identity, physical publication and server receipt guarantees; it is not a full SDK refinement or liveness proof. Real HTTP tests cover identical retry bytes after a lost acknowledgement, recomputation after a definite conflict, and refusal to recompute after receipt loss with or without a preceding local uncertainty flag. The JSON SDK and Pi adapters distinguish the `conflict` code from other HTTP 409 errors.


## Journal prefix safety

`Journal.tla` precedes the immutable journal implementation in `src/journal/`. `Prepare` allocates fresh positions, `Upload` makes their immutable bytes durable, `Observe` establishes positive durability knowledge, and `Consume` advances only through consecutive known positions. These correspond to `Writer::prepare`, the single-use `PreparedUpload::send`, `Writer::complete`/`resolve`, and `Writer::pop_durable`. Missing or failed responses never establish absence. Recovery scans one position at a time and fixes a prefix before successor activation; late uploads cannot extend that decision. `Reader` implements discovery and replay, but conditional selection and serving permission remain caller obligations.

The TLC-only constraint permits three prepared positions. All **4,134 distinct states** (18,455 generated, depth 26) pass, with an optimistic fingerprint collision estimate of 3.2e-12. Six mandatory controls expose false durability knowledge, consumption gaps, early recovery, stale negative reads, late acknowledgement and reopened selection. Run `scripts/check-model.sh Journal`.

`JournalProof.tla` removes the exploration bound: all **88 strict TLAPS obligations** prove the inductive invariant and its consequences for arbitrary log lengths and execution lengths within one handoff. The theorem establishes consumed-prefix durability, preservation of acknowledged work in the selected prefix, and selection immutability. Removing the contiguous-consumption rule must fail the corresponding induction step. Run `scripts/check-proof.sh Journal`; it uses the same pinned tools and fresh proof caches as the [publication proof](proof-publication.md). Woodpecker requires both checks in independent cache stages.

The proof assumes immutable unique identities, durable objects and fresh per-key observations, an external boundary ending old acknowledgement permission, and atomic durable selection. It does not establish that boundary, arbitrary ownership epochs, delegation, clock behavior, transactions, request deduplication, resource admission, collection, liveness or Rust refinement. The actual serving engine is unchanged. The older handoff-only model is retired; its [historical evidence](evidence/wal-handoff-candidate-2026-11-09.json) retains its source commit.

Thirteen journal tests cover out-of-order completion, unknown outcomes, cancellation and retained-frame budgets, bounded parsing, corruption, exact replay and local reopen; a separate collector regression establishes namespace isolation. Real B2 and versioned MinIO checks each recover 512 records after reconnect, resolve three lost responses without repeating PUT, and exclude a late suffix after persisted selection. These use the existing conditional publication layer as a fixture, not a replacement ownership controller. Source hashes, bounds, provider results and cleanup are in [the component evidence](evidence/journal-2026-11-09.json).

## Candidate revocable writer permits

`spec/candidates/RevocableOwner.tla` checks one timing-independent handoff: exactly-once positive permit creation before publication, a fresh permit GET after a response group is sealed and its journal prefix is durable, revocation before recovery, and immutable selection before successor service. No clock or failure timer appears. Old successful replies can finish late; they cannot admit later requests. Missing permission refuses service.

With two old journal positions and one successor write, TLC checks **856 distinct states**, 1,690 generated, depth 15, in under one second (optimistic fingerprint collision estimate 3.9e-14). Seven required controls detect recreation, premature publication, validation before upload, acknowledgement gaps, stale positive GETs, late group admission and early recovery. Two separate reachability checks require a read reply *after* the successor write and an upload *after* successor activation. Run `scripts/check-candidate.sh RevocableOwner`; Woodpecker checks it in its own cached stage. [Source/configuration/log hashes](evidence/revocable-owner-2026-11-11.json).

Assumptions: one selected successor with exact conditional acquisition and fresh identities, durable immutable journal objects, fresh point observations, and durable prefix selection. The permit PUT must have only one physical attempt and complete positively before publication; a successful GET cannot be reused. Provider consistency must forbid stale positive reads initiated after confirmed revocation. No production provider contract is established by this model. Repeated competing takeovers, discovering all unresolved ancestors, placement, collection, resource admission, KV/receipt semantics, liveness and Rust refinement remain unproved here. The native runtime now implements this boundary, as described below. One ordinary provider GET per sealed group remains a latency/cost floor; removing shared conditional writes is not a zero-I/O-read claim.

### Permit provider experiment

At `4f450dc`, the shared fixture passes on real B2 (15.36 seconds) and versioned MinIO (0.36 seconds). It creates 32 unique positive objects with one PUT each, measures fresh GETs at concurrency 1/4/16, deletes every permit physically, and checks absence 97 times through original, independent and reopened connections. One journal frame is durable before the held positive reply; recovery retains that prefix, while a later upload cannot gain new validation and is excluded from replay. There are zero logical authority reads/replacements/barriers during the measured phases and handoff fixture. The result uses existing transports and journal code; it adds no runtime permit format or controller.

| B2 GET concurrency | Median | Maximum | Validations/second |
| --- | --- | --- | --- |
| 1 | 35.17 ms | 1,042.08 ms | 7.14 |
| 4 | 32.10 ms | 252.46 ms | 77.73 |
| 16 | 66.91 ms | 111.31 ms | 200.33 |

These are 32 samples per phase from the development host, including bounded SDK GET retries; neither client geography nor regional SLO is established. The fixture holds a completed positive reply locally across deletion rather than delaying it inside the provider. It does not exercise competing control claims or collectors. It demonstrates a transport primitive and its current cost, not database throughput, sustained capacity, a consistency proof or AWS qualification. The one-second outlier matters: removing metadata contention does not eliminate provider tail latency.

Both disposable buckets, the scoped B2 key and the MinIO container were removed. The test physically removed 32 permits per provider; teardown removed two remaining journal versions per provider. [Evidence, all samples and cleanup](evidence/revocable-owner-2026-11-11.json).

Use scoped credentials and an explicitly disposable `durable-db-test-*` bucket; the caller must remove all versions, bucket and workload credentials even on failure:

```sh
DURABLE_DB_DISPOSABLE=1 DURABLE_DB_TEST_PROVIDER=b2 \
DURABLE_DB_TEST_BUCKET=durable-db-test-UNIQUE \
DURABLE_DB_TEST_CONTEXT='Describe actual client and provider placement' \
DURABLE_DB_PROVIDER_FLOOR_REPORT=/tmp/permit-report.json \
cargo test --locked --test provider_floor -- --ignored --exact revocable_permit_floor --nocapture
```

Use `s3` with the same fixture for the S3 binding. It caps uploads at 34 objects: 32 small permits and two journal frames. Do not run both ignored provider-floor tests concurrently in the same process: they share process-wide metric counters. Real-provider observations support the expected behavior but do not strengthen the documented contract. [B2 deletion](https://www.backblaze.com/apidocs/b2-delete-file-version) and [download semantics](https://www.backblaze.com/docs/cloud-storage-download-files-with-the-native-api) describe removal and missing-file responses; an explicit guarantee against every post-revocation stale positive remains a qualification obligation. [AWS consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel) covers object deletion, but the AWS deployment is not tested here.

## Candidate serving permission and log acknowledgement

The timed `OwnedWal` experiment is retired in favor of the revocable-permit candidate. Its standalone model/configurations and cached CI stage were removed; [historical evidence](evidence/owned-wal-candidate-2026-11-09.json) retains the 570,280-state result, seven controls, source commit and timing assumptions. Timed root/partition models still describe the current experimental controller and are retained until its replacement is integrated. The restricted-profile and VM failure evidence below remains available as a fallback, not a production deployment restriction.

## Candidate permission delegation

Historical timed candidate, retired from release CI in v0.19.0. Shorter child leases must end before their parent permission. The argument assumes continuously advancing clocks and justified error bounds. The source and [checked bounds, controls and logs](evidence/delegated-grant-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh DelegatedGrant`.

## Candidate root renewal and repeated takeover

Historical timed candidate, retired from release CI in v0.19.0. Exact conditional renewal, confirmation-anchored drains and child containment were checked for repeated roots. `RootControlProof` discharged 153 strict obligations under its timing contract. The source and [checked bounds, controls and logs](evidence/root-control-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh RootControl` and `scripts/check-proof.sh RootControl`.

## Candidate repeated root and journal recovery

Historical timed candidate, retired from release CI in v0.19.0. The composition retains acknowledged prefixes through repeated timed ownership changes. `RootJournalProof` discharged 1,061 strict obligations; the external permission boundary still assumes qualified clocks. The source and [checked bounds, controls and logs](evidence/root-journal-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh RootJournal` and `scripts/check-proof.sh RootJournal`.

## Candidate delegated partition movement

Historical timed candidate, retired from release CI in v0.19.0. Freezing an assignment and draining old grants prevents overlapping delegated owners under the root timing contract. `PartitionControlProof` discharged 687 strict obligations. The source and [checked bounds, controls and logs](evidence/partition-control-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh PartitionControl` and `scripts/check-proof.sh PartitionControl`.

## Candidate placement requests and uncertain replies

Historical timed candidate, retired from release CI in v0.19.0. Captured comparisons, late application and uncertain replies must not recreate a locally invalidated assignment. This was a timed-delegation candidate, not implemented placement. The source and [checked bounds, controls and logs](evidence/placement-requests-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh PlacementRequests`.

## Candidate placement requests across root replacement

Historical timed candidate, retired from release CI in v0.19.0. A replacement coordinator must complete interrupted movement without extending stale child authority. The candidate assumes the parent/child timing contract. The source and [checked bounds, controls and logs](evidence/placement-takeover-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh PlacementTakeover`.

## Control request session

`src/providers/control.rs` implements the request-handling subset of the preceding placement models. `Connection::control()` always supplies the common nonce-stamped register. Sessions reserve a fresh invocation before network execution; each prepared request captures its comparison and payload and can execute only once. The session permits one current invocation. Abandoning it clears local confirmation but cannot prevent later provider application. Only a matching current completion can confirm metadata; raw observations, stale replies and completions from another session cannot do so. A conflict describes that invocation alone and does not resolve an earlier uncertain operation.

Metadata is limited to 64 fields and 8 KiB of encoded JSON, with 128 bytes and a field reserved where needed for the register's mutation identity. Raw key/value bytes are bounded before serialization; escaped JSON size is then checked too. These are component bounds, not a process memory or queue admission guarantee. Callers can retain observations and prepared requests and must separately bound them. An oversized successful response remains uncertain because its mutation may already have applied.

Seven component tests exercise late application/replies, lost responses followed by conflicts, restart/session separation, metadata limits and invocation exhaustion. A real local HTTP test sends native B2 requests, cancels the caller after the provider accepts the request, and keeps provider processing alive independently. It checks both schedules: the abandoned update applies before revalidation, or revalidation advances the version first and the old update is rejected. The fixture preserves B2's identical-metadata shortcut, so the shared nonce wrapper is part of the test.

Run `cargo test --locked --lib providers::` through kache. All 132 local Rust tests passed, including the large-directory regression. Woodpecker pipeline 54 passed the complete required suite at commit `8807835`, and public evidence matched that commit byte for byte. [Evidence](evidence/control-session-2026-11-09.json) records the implementation, tests and qualification. This is not a Rust refinement proof or a serving controller. The caller still owes root permission, affected-placement invalidation, drain completion, bounded outstanding work and interpretation of the confirmed metadata. No application read path or clock contract changes here.

## Candidate root requests and revalidation

Historical timed candidate, retired from release CI in v0.19.0. Invocation matching and uncertain fence/activation recovery were composed with timed permission. `RootRequestsProof` discharged 983 strict obligations. The current shared request session retains invocation matching but grants no timed authority. The source and [checked bounds, controls and logs](evidence/root-requests-proof-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh RootRequests` and `scripts/check-proof.sh RootRequests`.

## Candidate delegated journal recovery

Historical timed candidate, retired from release CI in v0.19.0. Delegated writer replacement preserves selected journal prefixes under the root/partition timing contract. This is abstract proof evidence, not a shipped partition engine. The source and [checked bounds, controls and logs](evidence/partition-journal-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh PartitionJournal` and `scripts/check-proof.sh PartitionJournal`.

## Candidate delayed control application

Historical timed candidate, retired from release CI in v0.19.0. Late control requests can apply after the caller loses permission. Coverage checks require those executions rather than assuming timeouts cancel provider work. The source and [checked bounds, controls and logs](evidence/late-control-candidate-2026-11-09.json) remain fallback evidence; they do not establish the current permit protocol or qualify the existing hosts. Reproduce manually with `scripts/check-candidate.sh LateControlCoverage`.

## Transaction validation component

`TransactionValidation.tla` precedes the internal Rust validator in `src/transactions/`. It orders accepted transactions against one consistent committed snapshot, checks complete point/range read dependencies against all accepted predecessors, retains strict whole-database revision conditions, and limits visibility to the durable prefix. Its conflict index records mutations including deletion; reclamation advances a snapshot floor and never removes pending conflict metadata.

The minimal finite instance has two transactions, two keys and a two-transaction retained window; the reclamation coverage control uses a one-transaction window. Transactions can read arbitrary key sets, scan the modeled interval, or require the whole-database revision. They can write empty or multi-key sets. It explores **24,577 distinct states**, 59,778 generated, depth 10. The former three-transaction instance took 26 seconds locally; this instance completes in under a second while preserving every negative and reachability control. This is a finite regression, not an unbounded serializability or implementation-refinement proof. Five controls must detect checking only committed predecessors, checking only existing scan keys, accepting expired snapshots, exposing speculative revisions, and weakening the whole-database condition. Two reachability traces require disjoint transactions to be accepted before either becomes durable, and acceptance to continue after reclaiming a full window.

Run `scripts/check-model.sh TransactionValidation` and `cargo test --locked --lib transactions::validation`. Woodpecker requires the complete model descriptor and the Rust tests. Twelve Rust tests cover conflicts, phantoms, range endpoints, incarnation changes, revision exhaustion, byte/count/work limits and 1,024 generated histories checked against a serial reference. [Evidence](evidence/transaction-validation-2026-11-09.json) records inputs, bounds and results.

The caller must supply every read dependency and mutated key, bind tokens to authenticated database snapshots, resolve existing request receipts before validation, serialize evaluation and admission with acceptance, and establish contiguous journal durability before advancing the committed view. Snapshot expiry must not cause the client to recompute a transaction whose earlier outcome is uncertain; the existing `TransactionClient` model checks that retry rule separately, and composition remains required. The model's complete version history is ghost state, not runtime storage. Byte/work limits can reject additional requests; the finite model checks the transaction-count limit, not allocator behavior or CPU cost. Ownership, values, receipts, authorization, journal failure/recovery composition, tenant fairness and the versioned HTTP/client API remain outside this component. The native CLI uses this validator; integration does not extend the scope of this component model.

## Candidate clock error margins

Historical fallback arithmetic: `GrantBoundsProof` discharges 21 strict TLAPS obligations, including per-sample error, root exclusion and rounded child containment. Three unsafe controls remove a timestamp error term, drain margin or child margin. These are unbounded arithmetic lemmas under a clock contract; they do not establish that contract on any host. Reproduce manually with `scripts/check-proof.sh GrantBounds`. This is no longer a release CI stage. [Original proof evidence](evidence/grant-bounds-2026-11-09.json), [later clock experiments](evidence/vm-clock-2026-11-10.json).

## Ownership interval component

The interval runtime, checked timeline types, native `BootClock` adapter and their tests were removed in v0.19.0. Their historical source is commit `6688d47`. The [component evidence](evidence/ownership-timing-2026-11-09.json) records checked arithmetic, delayed responses, invalid clocks and overflow; the [VM evidence](evidence/vm-clock-2026-11-10.json) records why those assumptions were not qualified on ordinary hosts. Native serving now uses permits or the exclusive local guard.

### ClockBound counter and disruption experiment

`scripts/check-clockbound.py` reuses the disposable KVM harness and builds the pinned upstream ClockBound client through kache, outside the database dependency graph. The stock guest receives a synthetic V3 calibration with a locally estimated counter period, 1 ms initial error and 1,000 ppm period uncertainty. This isolates counter/disruption behavior; it does not qualify that calibration, an NTP service, or the daemon. No host clock or configuration is changed.

Baseline, three-second QMP pause and container freeze return trusted intervals overlapping the host's elapsed-time brackets. A short ACPI suspend does not: the marker remains unchanged and the client reports `Synchronized`. The first owner-visible sample is deliberately delayed until its midpoint is nondecreasing. Its elapsed interval is **−1.31 to 2.73 ms**, while the external bracket starts at **61.15 ms** and ends at **10.49 seconds**. These ranges do not overlap. Host brackets include serial/QMP latency; no exact cross-machine timestamp alignment is assumed. Snapshot restore changes the marker and yields `Disrupted`; the simultaneous unguarded client remains trusted with an invalid interval, demonstrating the control's sensitivity.

The counterexample rejects this stock guest as a ClockBound-backed serving platform even with the current backwards-reading check. It does not change the `GrantBoundsProof` assumptions or prove a defect on supported EC2 hardware. ClockBound is not a database dependency, and the experiment grants no serving authority. The [recorded source, binary, kernel and observations](evidence/vm-clock-2026-11-10.json) make this rejection reproducible:

```sh
# Source the documented kache environment first; requires Docker and /dev/kvm.
python3 scripts/check-clockbound.py --output /tmp/clockbound.json
```

Exit zero means the expected counterexample and positive/negative controls were reproduced, **not** that a clock was qualified. Temporary sources, guests and files are removed; compiler artifacts remain cached. The probe targets Linux x86_64. Repeat platform qualification if the kernel, hypervisor or clock source changes.

### Reported clock error prerequisites

`ReportedSampleBounds` proves that timestamps each within `A` of a continuous reference clock satisfy the existing elapsed-measurement contract `[t - E, 2t + E]` with `E = 2A`. No rate scaling is needed. Both endpoint errors count; the negative control `E = A` fails. The proof assumes the per-sample bound is true. It does not establish it for any physical device. A source must include maximum calibration error, accumulated counter-rate error, sampling and rounding within `A` on **every accepted read**, with a coherent counter/calibration sample. Backwards readings retire the timeline; a valid timestamp does not authorize a restored or cloned incarnation.

The existing VM harness now reads calibration capabilities before longer fault campaigns. The [VMClock v1 specification](https://uapi-group.org/specifications/specs/vmclock/) makes maximum time/rate error and VM generation optional. Missing validity flags produce `null`, never a false zero-error claim; estimated errors do not replace maximum bounds. The probe requires a supported counter, TAI time, synchronized or free-running status, a nonzero counter period, both maximum-error fields and a VM generation counter for this proposed source. The generation counter detects cloning independently of the disruption marker. A platform with a separate incarnation mechanism would need separate qualification. Even a complete header reports `qualified_for_serving: false`: this diagnostic does not read the counter, implement the clock, or certify the hypervisor's guarantees.

The actual QEMU 10.1.5/KVM device reports `counter_id = 255`, `clock_status = 0`, `flags = 0`, and no maximum-error or generation fields. The development host and all three physical cluster nodes have no VMClock device. The cluster nodes have `i40e` PTP devices; their presence alone proves neither synchronization nor conservative bounds. **No available platform passed the prerequisites for this VMClock binding.** This rules out extending the marker-only fixture into a production clock; it does not rule out other calibrated sources. [Raw header, inventory, proof results and scope](evidence/vm-clock-2026-11-10.json).

```sh
python3 scripts/test-vm-clock.py
python3 scripts/check-vm-clock.py --capabilities --output /tmp/vmclock.json
python3 scripts/check-vm-clock.py --device /dev/vmclock0 --output /tmp/host-clock.json
```

The disposable capability probe needs Docker and `/dev/kvm`, but no Rust build, no custom kernel and no clock-setting privileges. The direct device probe only reads the supplied file. Exit zero means inspection completed; inspect `requirements_present` and the recorded reasons, not exit status, for prerequisites. Three fast tests cover validity flags, zero/estimated bounds, unreliable status, missing generations, truncated layouts and in-progress updates. Run `python scripts/test-vm-clock.py` manually; this fallback probe and the timing proof are no longer release CI stages.


## Common provider register

Both provider connections wrap conditional metadata replacement with one shared mutation-identity layer. Every invocation stamps a fresh UUID into the existing reserved `durable-db-probe` field. This matters because B2 can accept identical live metadata as a no-op despite a stale comparison revision; checking only that its response revision exceeds the caller's old revision does not establish a fresh comparison. S3 uses the same layer. It adds no provider calls or metadata slots, leaves unknown outcomes uncertain and does not infer serving permission or metadata freshness from a raw observation.

`ProviderRegister.tla` explicitly models that no-op behavior, stale observations, concurrent attempts and delayed/lost responses. Its three-attempt/two-value instance checks **8,901 distinct states**, 21,289 generated, depth 10. The required identity-reuse mutation produces a falsely accepted stale comparison; a separate forbidden-state control reaches a correctly rejected stale update whose logical metadata equals the current value. The model assumes globally unique attempt identities and exact revision comparison for real mutations. It is bounded component checking, not a cryptographic, implementation, liveness or controller-composition proof. Run `scripts/check-model.sh ProviderRegister`; its separate Woodpecker stage requires all three checks.

The real HTTP fixtures reproduce B2's no-op acceptance and verify automatic identities, repeated logical writes, stale-payload rejection and a committed response lost without retry. The S3 fixtures preserve uncertainty across committed errors and redirects through the same common layer. Fresh publication initialization accepts the known startup/read-barrier markers while still rejecting unknown protocol metadata. The [qualification record](evidence/provider-register-2026-11-09.json) includes real B2 and versioned-MinIO comparisons, concurrent identical payloads, reconnect, journal compatibility, the initial fixture failure and cleanup. MinIO does not qualify AWS, and the journal fixture does not implement a serving controller.

## Candidate root acquisition

`RootAcquisition.tla` starts with an empty register and unordered candidate identities. A candidate binds once to one predecessor and the next epoch. Requests retain their captured comparisons after timeout; any historical metadata may be observed. Successful generations are contiguous, one identity wins each generation, and a winning identity cannot be reused in a later generation. Only the current invocation's successful response confirms local state.

The model includes fenced/open activation and phase-preserving validation. Two unrestricted instances check **117,741** distinct states with two candidates/four invocations (depth 17), and **30,349** with three candidates/three invocations (depth 13). Four invocations retain the claim/activation/takeover/stale-validation sequence; three suffice for the separate competing-candidate case. Disabling comparison fails `UniqueWinner`; retargeting fails `ClaimBinding`; inventing activation during validation fails `OpenWasActivated`. Three reachability controls find competing captures of one predecessor and recovery after a lost claim or activation response. No symmetry reduction or guided constraint is used. Run `scripts/check-candidate.sh RootAcquisition` (about twelve seconds locally including all controls).

This checks the acquisition discipline behind ordered winning epochs; it is not a refinement theorem connecting it to `RootRequests`. Activation abstracts completed predecessor revocation, checked separately by `RootRegistration`. This model excludes response permission, physical clocks, journals, scheduling, liveness and a proof of the implementation.

Historical host/VM qualification used the removed timed owner at commit `6688d47`. Process `SIGSTOP` passed on the development host and three existing Kubernetes hosts; freezing its clock was an unsafe control. That did not qualify VM time. A 3-second QMP pause advanced `kvm-clock` by only about 7 ms; restoring saved memory similarly defeated elapsed-time expiry. Short guest suspend also violated the assumed rate bounds with `tsc`: 120.2445 ms on the host corresponded to 335.391747 ms of guest boottime.

A restricted fallback required VMClock disruption detection, `tsc`, and a custom kernel with suspend/hibernation disabled. Twelve bounded scenarios passed, and restored-owner refusal passed a 23.60-second native takeover test including an unsafe control. This does not justify that deployment contract for the existing hosts. Conservative cross-host error bounds, hypervisor suspend and migration remain unqualified. [Exact configurations, failures, successful controls and source hashes](evidence/vm-clock-2026-11-10.json), [three-host process-pause evidence](evidence/native-http-hosts-2026-11-10.json).

The preserved harness requires a historical test binary built with kache at `6688d47`, plus Docker, `/dev/kvm`, `ldd` and `strip`:

```sh
scripts/check-vm-clock.py --test-binary /path/to/6688d47-test-binary --kernel nosleep --output /tmp/vm-clock.json
scripts/check-vm-clock.py --test-binary /path/to/6688d47-test-binary --kernel nosleep --owner --output /tmp/vm-owner.json
```

The capability probes above remain usable without that binary. No production clock profile is selected; the permit mechanism does not depend on these timing results.

## Candidate root controller

The native controller now implements [revocable writer permits](#candidate-revocable-writer-permits). Claim persists a write-once permit before publishing its identity. Activation permanently revokes all unresolved ancestors before recovery, stopping only at the inherited checkpoint owner, inclusive. A successful control confirmation does not grant reusable serving permission. Native results, errors derived from state, and exact receipt retries require fresh validation after their view/durable prefix is fixed; the HTTP worker validates one sealed group of ready replies at a time.

The collector reuses `PublicationSafety` for permits: list bounded candidates before its fresh conditional fence; protect the current root identity, including unfinished activation. Each unpublished claim captured its comparison before its permit PUT, so the sweep defeats late publication. Older ancestor permits may be revoked early without deleting journal registrations; takeover still confirms their absence before recovery. Delayed PUTs from never-published claims cannot gain authority. No tombstones, extra leases or separate cleanup algorithm are introduced.

Remote formats 23/23-unbudgeted reject timed writers; local format 15 is unchanged. Focused Rust checks cover delayed positive replies across takeover, recovered acknowledged prefixes, interrupted ancestor revocation, missing/corrupt permits, unknown creation without PUT replay, collected claim publication, and fork refusal. The existing native HTTP tests cover pending uploads, receipts, authorization, cancellation and checkpoints. These are implementation regressions, not a refinement proof or provider-consistency theorem.

The clock-independent native HTTP fixture passes on B2 and versioned MinIO at `50b1655`, keeping the old server alive during takeover and requiring its subsequent reads and exact receipt retries to fail. Two cold reopens preserve values, receipts, budgets and retained references. Automatic collection and retained export/restoration pass. One hundred warmed HTTP point-and-scan reads use one GET each and zero authority calls; B2 median/p99 are 25.50/80.61 ms. These sequential debug observations do not qualify sustained load, Kubernetes placement or AWS. The local released-binary upgrade/rollback drill also passes. [Evidence and earlier fixture failure](evidence/native-permits-2026-11-11.json).

The timed runtime and renewal worker are deleted. Fifteen historical clock/delegation model/proof stages have been removed from the release build; their source, pinned evidence and manual commands remain for fallback research. Automatic cloud election/routing and partition placement are now integrated. Bounded failover availability and broad production qualification remain open.

## Durable root registrations

`RootRegistration.tla` extends exact conditional acquisition with immutable registration upload, positive knowledge and provider-send events. Every winning journal stays reachable through persisted predecessor links, including abandoned claims that apply later. The extended candidate also tracks a **separate** write-once permit per registration: deleting permission must not destroy the chain needed to discover earlier writers. Registration and permit creation are abstracted as one completion here; `RevocableOwner` separately checks the physical PUT/publication ordering and sealed response groups.

Two/three competing candidates and three control invocations check **34,061**/**281,563** distinct states, 110,381/1,032,703 generated, depths 23/27, in one/seven seconds. Four unsafe controls detect premature publication, lost predecessor links, revoking only the immediately preceding writer and stopping at an unfinished checkpoint owner. Two coverage controls require a three-generation chain retaining two acknowledged-journal markers and completion by a third claimant after the first finished revocation but the second did not. The latter witnesses the interrupted-takeover case; it is not a liveness theorem. No symmetry reduction or guided constraint is used. Run `scripts/check-candidate.sh RootRegistration`. [Current source/configuration/log hashes](evidence/native-permits-2026-11-11.json) and [earlier registration evidence](evidence/root-registration-2026-11-09.json) retain their respective scopes.

Revocations now run at most four at a time within one activation, replacing the serial wait. The model already permits arbitrary completion order; `FinishRevocation` still requires every necessary permit to be absent. A focused Rust regression covers eight predecessors, holds the last deletion before publication, injects a lost deletion reply and cancels in-flight activation, then checks recovery by a new owner. It fails against the serial implementation at its one-second progress bound and passes in 0.02 seconds with the bounded schedule. The four-request resource bound is checked in Rust, not modeled as a timing guarantee.

Revocation completion requires every unresolved ancestor's permit to be absent. The traversal may stop at a checkpoint owner that already completed ancestor revocation, including that owner's permit. A new negative control stops at an unfinished owner and violates the invariant. The invariant checks all earlier winning identities, not merely the observed chain, so an omitted ancestor is detected. A historical successful claimant may revoke its own immutable ancestors after a later takeover; it cannot recreate permission or delete the later claimant's permit. Unique identities, exact comparisons, permanent deletion and fresh observations remain assumptions. This does not yet compose journal selection, transaction results, collection, placement or Rust serving.

The registration qualification used `owned-candidate-2`; the current format adds the prefix catalog described below. It uploads a checksummed registration before its conditional claim, resolves an unknown upload only through an exact positive read, and sends no metadata mutation if the upload remains unresolved. The old candidate format is refused. The journal identity derives from the controller's fresh incarnation; its writer can be taken once after activation. Recovery streams records of at most 234 bytes, requires exact identity/namespace/encoding binding and decreasing parent epochs, and never interprets a missing or corrupt record as completed history.

Four additional tests cover positive resolution versus an upload that arrives after a different candidate wins; dropping all controller/writer/read state and retaining only the provider's current register and objects; every-byte corruption plus checksum-valid semantic mutations; and both namespace-size boundaries. Cold discovery finds three earlier journals and their immutable frames without listing, using a fresh controller. It does not claim that their provisional prefixes were durably selected. The S3 HTTP fixture now loses an applied registration PUT response, checks exactly one PUT per registration and traverses the chain through the real binding. B2 retains both cancellation-order cases with registration preceding publication. [Evidence](evidence/root-registration-2026-11-09.json).

The model assumes unique immutable identities and exact register comparisons. Its acknowledgement markers overapproximate eligibility and only assert discoverability. It does not prove data-byte durability, selected-prefix preservation, permission/clock composition, implementation refinement or liveness. The native controller now creates and revokes permits; the timed runtime is removed. Checkpoint/recovery and native HTTP integration are described below. Native physical uploads use the allocator described above; registration collection remains open.

## Durable prefix catalog

`RootCatalog.tla` models captured conditional requests, immutable catalog uploads, positive knowledge, provider send, delayed application, timeouts and replies separately. A claim must inherit all selected prefixes. A complete scan can refresh its comparison from an observation only if the catalog is unchanged; the selection CAS itself validates that observation. Exact comparison prevents a stale publication from dropping or replacing a decision. Late journal uploads remain possible after selection.

Recovery groups at most eight complete, unselected journals from the same
confirmed catalog into one publication. Every prefix upload and the shared
index/header must complete before the exact conditional write. Mixed snapshots,
incomplete or duplicate journals and oversized batches are rejected before
upload. Single-prefix callers use the same implementation. Direct confirmation
retains the immutable ancestry cursor and loads the new catalog. An unconfirmed
publication reads the same owner's record. If the full recovery snapshot is
unchanged, the controller retains the completed scans and ancestry cursor and
captures that observation before fresh uploads. The next selection CAS validates
it, removing the separate validation CAS. A changed catalog, checkpoint or
collection generation still requires exact validation and fresh discovery; an
observed successor refuses the attempt. The same four-retry budget bounds fresh
observations and further validations. Repeated uncertain writes can require fresh
catalog uploads on each retry; none can confirm a view without a successful CAS.
Only at most eight completed prefixes are retained; frame payloads are dropped.
Every attempt creates fresh catalog objects before its captured comparison, so
neither an unknown PUT nor an already selected cut is replayed.

The selection retry regression also fails on the v0.24.10 loop: a failed
comparison plus a successful retry requires three conditional writes, versus two
with publication validating the unchanged observation. Failed catalog PUTs now
require only the eventual selection write, versus validation plus selection.
The counts exclude the fixture's independent administrative mutation. Fresh
advisory metadata is preserved; observations and prepared requests leave recovery
unconfirmed. Races with collection and takeover still lose the exact comparison.
The TCP regression preserves the original commit receipt and value after an
uncertain selection, and the live predecessor refuses reads. Five failed
selection attempts exhaust the unchanged retry limit without confirming a catalog.
No new model states or persistent format are introduced; the existing bounded
StartSeal action already admits direct retries against unchanged observed cuts.
Its new unsafe-refresh control detects losing the snapshot comparison.

A focused regression fails against v0.24.4: one completed one-frame scan is read
again after a control conflict, producing five GETs when a late frame arrives.
The replacement uses the original two GETs and preserves that completed cut.
The same check covers an unknown comparison and lost catalog PUT replies, both
applied and delayed. A metadata-only sweep can remove the failed attempt's
objects; the retry stages new ones. A data sweep advances the recovery generation
and requires a new scan. Existing lost-publication, takeover and mixed-snapshot
checks remain in place; thirteen catalog checks finish in 0.52 seconds.

The eight-journal regression reduces catalog publications from eight to one,
with ten immutable PUTs and at most nine registration GETs. Seventeen journals
require three publications, 23 PUTs and at most eighteen registration GETs.
Tests recover every selected frame, cross an index split, lose a two-journal
publication reply, deliver late journal bytes, race takeover and fail uploads.
This removes per-journal catalog/index publication; it does not bound retained
ancestry, eliminate placement contention or establish a cloud latency speedup.

The model now captures a set of completed scans from one source catalog in each
seal and retains that completed scan across attempts. Routine checks use two
epochs, three invocations and one frame (**455,553** states, depth 33; 10 seconds
locally), plus three epochs and three invocations with empty journals
(**173,635** states, depth 27; 5 seconds). The latter includes
multi-journal publication; the former covers nonempty-prefix preservation.
Six mandatory unsafe controls detect missing comparison, dropped inheritance,
reselection, refresh across a changed catalog, publication before durable upload
and discovery before drain.
Four coverage controls witness a delayed selection, inherited selection, a
two-journal batch and retry of the same completed scan. The model abstracts out
collection; the existing Rust snapshot comparison and sweep regression guard
that boundary. The eight-entry implementation bound and mixed-snapshot validation
are checked in Rust. These bounded instances do not prove Rust, collection
composition or all nonempty batch interleavings.

Six fault/coverage controls use smaller bounds while retaining their same expected
violations: empty journals suffice for comparison, inheritance, early publication,
late application and scan-retry controls; early publication and late application
need only two invocations. Discovery-before-drain still uses a frame, with two
invocations. Together these six checks explore 113,591 distinct states instead of
1,119,253. Positive safety bounds and the nonempty reselection control are unchanged.

The root record carries an immutable catalog identity and rejects incompatible formats. Catalogs reuse the existing bounded-page B+tree, with a checksummed header of at most 4 KiB and 112-byte prefixes. `recovery()` requires confirmed activation and drain. Opaque scans bind to that owner and catalog; only a completed unselected scan can be published. The controller captures the exact comparison before any immutable upload. An upload failure prevents publication; a lost publication reply requires exact validation or a winning claim to recover the durable decision. Existing selections replay only their fixed prefix. Missing or corrupt referenced objects never become absent entries.

The original loopback qualification used the earlier `owned-candidate-3`
format. Its B2 binding loses an applied selection reply; the S3 binding reads
back journal, index, prefix and catalog bytes and excludes a late second frame.
That [historical evidence](evidence/root-catalog-2026-11-09.json) is not cloud or
current-release qualification. Current native checks and cloud campaigns are
recorded in [verification results](verification-results.md).

The model assumes ordered winning epochs, durable immutable bytes and exact register comparisons. Drain completion abstracts the ending of every earlier acknowledgement interval. The separate acquisition, registration, timing and journal results are not a complete composition or Rust-refinement theorem. Later sections describe transaction/receipt recovery, checkpoints and native HTTP integration. Collection, physical upload admission, election and placement are outside this component model. History traversal still grows with ownership changes.

The same change raises only the disposable JavaScript test server's request budget. An isolated receipt-eviction test fell from **37.35 s to 2.44 s** locally; the full JavaScript suite passed 98 tests with one opt-in process-control case skipped, plus four web-app storage tests. No assertions or failure scenarios were removed. These single-run development timings measure avoided admission waits, not database throughput.

## Ordered catalog replay

`CatalogReplay.tla` checks the stream produced from a complete immutable catalog. Reads must remain inside each selected prefix, journals must be consumed in increasing epoch order, and completion requires the exact expected sequence. Late uploads beyond a selected cut remain possible. Three journals with zero to two selected frames, including all 27 initial cut combinations, with a one-frame disposable cache check **11,459** distinct states (88,318 generated, depth 12). Positive validated reads may populate the cache, eviction may clear it, and replay retains the selected-cut checks. The cache never represents absence or a failed read. Unsafe skip, extension and reverse-order controls fail the required invariant; a coverage control reaches nonempty completed history. Run `scripts/check-candidate.sh CatalogReplay`.

The Rust cursor checks the catalog count against the current root epoch before returning frames, then validates canonical consecutive epoch keys, selected identities and journal hash chains while streaming. It pages at most 64 keys and reuses one frame budget across journals. The cursor owns its frame and exposes a borrowed view, and the underlying reader also refuses a reset while a frame reservation remains live. Thus a journal boundary cannot bypass the reader's memory limit. This does not bound allocations a caller makes by copying records or constructing multiple cursors.

Four root tests cover incomplete catalogs, ordered frames across ownership changes, late-frame exclusion, completion after permission expires, 130 mixed empty/nonempty journals across index pages, missing/corrupt frame recovery and checksum-valid bad key sets with the correct count. A journal test retains a frame across end-of-log and verifies that resetting cannot allocate another slot until the frame is released. The B2 and S3 HTTP fixtures also exercise the ordered replay API. [Evidence](evidence/catalog-replay-2026-11-09.json).

Small journal frames fetched and validated during selection now share the existing memory LRU with index data. Discovery always reads the provider; only selected replay uses remembered positive reads. Ordinary insertion and disk promotion cannot establish journal provenance. The unchanged 512 KiB entry cap, configured shared byte limit and 4,096-entry ceiling apply; larger frames bypass caching. Replay still decodes each frame, checks identity, predecessor and selected-tail digests, and reserves one full frame before lookup. Eviction and restart fall back to storage. This cache does not authorize serving or replace final ownership validation.

The 8- and 17-journal regression requires two GETs per discovered journal (frame plus end probe), then zero additional frame GETs during selected traversal and ordered replay. It still refuses serving after revocation. Journal checks cover eviction, cold cache, ordinary insertion, disk promotion, absent/failed/corrupt reads, invalid chains, wrong selected digests, oversized frames and outstanding-frame limits. The model abstracts immutable positive bytes; it does not model their encoding, disk corruption, byte accounting, collection or serving permission. Those remain implementation tests and separate protocol obligations. Removing the in-memory provenance guard makes the insertion/disk regression fail. All 364 routine Rust tests and clippy pass; the 20 opt-in provider/load tests remain separate qualification.

The model assumes complete immutable selected catalogs and durable selected frames; it does not establish those properties itself or model the storage tree, physical clocks, transaction evaluation, receipts or serving authority. The cursor reconstructs a byte stream, not an application-visible database view. Its `completed()` flag describes that snapshot only; a final current permission check and transaction/view reconstruction remain mandatory. This adds no serving-engine or production-readiness claim.

## Transaction records and private recovery

`TransactionRecovery.tla` models an unpublished value/revision/receipt projection of selected transaction records. A target may fail after partially changing its private state; failure cannot return a completed view. Receipt-retention floors are part of the immutable history, including choices to retain more than the minimum or discard earlier receipts. Two databases, two request identities, one key per database, two values, and up to three records check **2,114** states with a one-receipt bound and **4,002** with a two-receipt bound, both to depth 8. Four unsafe controls detect dropped receipts, a stale revision, a forgotten retention floor and premature exposure. A coverage control reaches nonempty completion after receipt eviction. Run `scripts/check-model.sh TransactionRecovery`.

The Rust codec bounds and checks the complete record before application, borrows key/value bytes, rejects noncanonical mutations and includes a database generation and original-request fingerprint. Recovery verifies contiguous database revisions, monotone retention floors and retained request identity. Receipt lookup resolves a matching earlier result before checking snapshot age; an absent receipt below the retained floor returns expired rather than fresh. Retention is recorded per transaction, with a 65,536-revision allocation bound; the recovery process cannot silently adopt a different retention policy. This does not select a production retention period or bound the sum of all resident database state.

Eleven new tests cover every-byte corruption/truncation, checksum-valid semantic corruption, encoded limits, receipt expiry and windows larger than 128, generation isolation, revision gaps and repeated requests, selected history across owner changes, late uploads, private projection failures, missing selected frames, rejection of an advanced cursor, cancellation and completion after ownership expiry. The S3 HTTP fixture now restores a value and its original receipt after uncertain control publication, excluding a later old-writer upload. B2's HTTP control fixture restores its empty selected history through the same runner; its immutable objects remain in memory. [Evidence](evidence/transaction-recovery-2026-11-09.json).

The model assumes valid selected history and a target that applies complete records and does not expose private state. It does not prove arbitrary `Target` implementations, serialization/refinement, source selection, evaluation of transactions, creation/policy recovery, live snapshot publication, physical clocks, admission or liveness. These replay tests use an in-memory projection. Later sections cover native checkpoint recovery, live group commit and routing; the full production failure campaign remains open.


## Live native transactions

Checkpoint reconstruction retries now use owner-local exponential backoff (1, 2, 4, 8, 16, then 30 seconds between starts). The gate is armed before I/O, including cancellation, and applies to explicit low-level maintenance calls as well as the serving worker. Exact validation of staged publication bypasses it without another data upload; successful publication resets it. Existing Rust tests cover deadline boundaries under paused Tokio time, seven failed attempts, 700 immediate calls, cancellation after an applied PUT, collected staging, success/reset and continued cold recovery. A real TCP burst checks new-write refusal, exact retry success and unchanged tenant allocation while the provider fails; idle maintenance resumes after recovery. Four unsafe Rust variants fail, and the unpaced variant also fails the TCP check. This is pacing, not ownership timing or per-tenant retry accounting. The checkpoint model treats waiting as stuttering. Its current instance checks 144,288 states (386,208 generated, depth 13), with nine fault controls. [Milestone evidence](evidence/checkpoint-retry-pacing-2026-11-10.json).

`transactions::live` connects the native projection, accepted-write validator, immutable journal and root permission check. Admission reserves journal and aggregate index capacity before accepting a revision. One upload groups up to 128 transactions, including multiple transactions for one database. Only contiguous durable frames advance the readable view. Conflicts depending on pending writes remain pending too. Request fingerprints include the original snapshot, read conditions and mutations; pending and retained committed receipts resolve before validation. Final read/result checks issue fresh permit GETs after sealing results. A separate async request gate serializes exact metadata validation, final recovery binding and checkpoint publication; it never holds the short root mutex across I/O. This prevents a renewal from abandoning another live caller’s checkpoint invocation. Regression tests pause checkpoint publication while renewal waits, cancel an in-flight renewal, and recover a lost renewal reply without blocking ordinary reads. The existing checkpoint/request models retain their cancellation and exact-comparison rules.

`LiveTransactions.tla` checks **19,220 distinct states**, 104,871 generated, depth 21, with two request identities, two accepted writes and one checkpoint. Nine fault controls detect publication across a gap, pending reads, premature conflict responses, missing final permission, duplicate request acceptance, admission despite a failed precondition, authorization against a policy preceding an accepted revocation, replies after committed revocation, and mutation of a checkpoint cut during staging. A reachability control requires reads during staging. Conditions and a single revocation are abstract; Rust tests cover value comparison, request hashing, credential generations and individual revocation. Existing `TransactionValidation`, `Journal`, `TransactionRecovery` and ownership models retain their respective conflict, byte, recovery and revocation obligations; this is a bounded component check, not a proof of the Rust implementation or arbitrary deployment schedules. The obsolete logical-file prototype and its larger model have been removed.

`Owner::read_request` now uses the existing public request/response types and shared validation. It evaluates multi-key reads and forward/reverse scan pages against one committed view, including prefix/start/end/after bounds, lookahead cursors, empty values, tombstones, expected revisions and the 1 MiB response bound. Cold generation reads do not materialize receipts or validator state. The parity test compares these responses against the existing HTTP engine in memory, from checkpoints and with recent writes over checkpoints, including Unicode prefix boundaries; the authority-expiry test now uses the same wire evaluator. The shared HTTP adapter below now uses this evaluator. The superseded 128-row native scan wrapper has been removed.

Run `cargo test --locked --lib live_` and `scripts/check-model.sh LiveTransactions`. Integration tests cover grouped commits, pending duplicates/conflicts, reversed completions, stable reads, deletion, receipt expiry, aggregate capacity before acceptance, selected recovery after owner loss, generation isolation and excluded late uploads. The real S3 HTTP fixture loses an applied upload reply, resolves it by exact GET and releases both ordered batches; ordinary data operations make no control updates. It then triggers a checkpoint, loses the applied publication reply, confirms it by exact revalidation and reads values back through HTTP ranges. This is transport/contract validation, not a cloud performance measurement.

`Owner::prepare_requests` evaluates up to 128 public commit requests (4 MiB of aggregate wire input) through the same native admission queue. `prepare_request` is its single-request wrapper. It validates the complete body before storage I/O and hashes the original wire fields, database and generation independently of ephemeral snapshot tokens. Pending and committed retries resolve before revision/value checks; duplicate write keys retain their final value. `commit_result` releases success or rejection only after its required durable prefix and a final permission check. Tests cover pending duplicates, altered check values, future/stale revisions, empty/deleted values, checkpoint and takeover retries, generation isolation and receipt expiry. The HTTP provider fixture verifies that two databases share one immutable PUT, loses its acknowledgement, resolves both results and retries after checkpoint publication. Batches mix new writes, retained replies and conflicts while preserving input order; invalid batch input accepts nothing. Checkpoint maintenance runs before evaluation and after publication, so warming a database cannot invalidate its freshly evaluated snapshot.

The shared decoder removes the older engine's repeated parsing and moves malformed-key/base64 refusal ahead of manifest reads. An expired native receipt returns `receipt_expired`, not an ordinary retryable conflict; clients retain the original request for reconciliation. The native HTTP adapter and cloud CLI use this evaluator.

Native protected creation and access policy now live as bounded rows in an internal database generation, using the same journal, immutable index and checkpoints. The shared creation-receipt and policy code also serves the existing engine. Pending creation cannot authorize data access. A pending policy mutation blocks further writes to that database; its publication follows all previously accepted writes. Reads and commit replies use the committed policy, and returned data tickets bind the database generation. Individual revocation and credential rotation preserve the data revision. Permanent creation receipts and bounded policy receipts retain their original results across checkpoints and takeover. The internal generation cannot be reached through a public database lookup, even for a user database with the catalog's name.

Two focused lifecycle tests take about **0.01 seconds after compilation**. They cover creation visibility and exact retries, competing intents, scope/write/legacy-token refusal, catalog isolation, out-of-order data/policy uploads, durable revocation before receipt replay, generation rotation, checkpointing and takeover. The S3 HTTP fixture loses applied creation and revocation replies and resolves both through exact journal reads; revocation blocks new admission while unresolved. This extends the existing creation/authorization models and shared Rust implementation rather than adding another storage format or authorization engine.

The native library no longer takes a clock contract. The shared HTTP adapter and cloud CLI serve recovered native owners with automatic placement. Full lifecycle qualification, scheduled retention and complete tenant maintenance attribution remain open; checkpoint retention and automatic physical collection are covered below. The production-readiness gate is unchanged.


`api::Native` connects these authorized methods to the existing router and middleware. A bounded worker owns admission, publication and periodic checkpoint retries. Journal PUT tasks run outside the owner lock; dropping an HTTP request drops its reply, not accepted journal work. Unknown PUT replies are resolved by exact GET without another PUT. A final owner check gates responses; a revoked or failed worker cannot report ready. The old `Native::renewing` scheduler and interval tests are removed. No renewal task grants or extends native permission.

The worker sends validated replies before starting checkpoints. Funding and immutable staging borrow the frozen owner through a shared lock, allowing point reads, scans and information requests on the same committed view. New admission waits. Publication, confirmation and projection replacement retain exclusive access, so existing readers finish before collection can retire the old tables. The staged job binds the owner identity and fully durable journal prefix; retry pacing is installed before any I/O, and uncertain publication remains attached to the owner. Tenant funding uses the same cached allocator so forfeited admission credit cannot survive in a second grant. The implicit foreground maintenance helper and all six call sites are removed; there is one checkpoint implementation, no cloned projection and no additional worker.

The real TCP regression pauses an immutable table upload, requires point/scan/info responses and the preceding commit acknowledgement within one second, then checks receipt replay after publication. Its second case revokes the permit while staging and requires HTTP 503. The previous exclusive path fails the read deadline. Existing cancellation, lost-publication, collection and charged-retry regressions remain. These checks isolate staging; publication I/O, admission warming and provider latency can still delay reads. The exact static artifact recovers all 676 acknowledgements in the B2 rollout fixture; direct gaps of 74.41 / 83.96 seconds still miss 60 seconds. Publication and routing delays remain open; see the [measured results](verification-results.md). `LiveTransactions` abstracts client disconnects as stuttering steps that cannot undo acceptance, upload or publication.

Run `cargo test --locked --lib root::tests::http`. A real TCP test covers protected creation/retry, policy-bound token issuance, read/write and cross-database scope, logical quota refusal, checkpoint-backed uncached reads, revocation before receipt replay and expired-owner refusal. A paused-provider test exercises concurrent HTTP reads, independently completed uploads behind a journal gap, bounded duplicate admission, cancellation of an accepted request and exact resolution after a lost PUT reply. It verifies no new control mutations for ordinary reads/writes, no repeat PUT, and no repeated cold catalog reads while a reply is pending. Additional regressions cancel a request during cold admission, expire permission during a paused upload, and recover a failed checkpoint with no further traffic. These four HTTP tests take about 0.21 seconds after compilation. These are contract/failure tests, not a cloud benchmark or host/VM timing qualification. Native collection explicitly returns 501. The budget endpoints now use the shared durable allocator when the native connection enables it. HTTP cross-request batching and the native CLI are now integrated, as described below.


The unchanged JavaScript client, both Pi 1.5.0 adapters and Harness tests also run against this native router through `native_http_javascript_contract`: **99 passed, one skipped** in about 28 seconds locally. This includes all 64 upstream adapter cases and 11 SDK cases; the remaining cases cover transport, adapter failures/resources and Harness workflows. The skipped all-API-process-loss case requires external process control and is not established by this fixture. It uses an in-memory provider, no byte cache, checkpoints every 64 frames, a 128-revision receipt window, explicit 512 MiB per-database admission funding and a bounded 20,000-unit request budget to avoid refill delays. The eviction test now asserts the original receipt actually returns 409 instead of silently passing with a longer window. Woodpecker runs the existing suite against both serving paths using the same source-built test executable; there is no extra router or production clock adapter. This is client compatibility evidence, not a cloud performance result.

The same pause/takeover executable and all four native HTTP regressions also pass in restricted pods on three Kubernetes hosts (Linux 6.8.0-149-generic, `tsc`, one CPU and 512 MiB per pod). The pause case, including its frozen-clock negative control, takes 9.37–9.44 seconds per host including execution transport. The initial minimal container lacked CA certificates and failed before exercising the pause scenario; the corrected fixture supplied system roots without changing timing. All temporary resources were deleted. This broadens host process-pause evidence, not VM/suspend or independent-clock qualification. [Configuration, failures, hashes and results](evidence/native-http-hosts-2026-11-10.json).

## Bounded immutable range transport

The shared provider boundary and local-directory binding now support exact nonempty ranges up to 8 MiB against a recorded full object size. Two local tests cover byte boundaries, size/path checks, and real S3 HTTP responses that ignore Range, change its offset/total size, truncate/extend its body or report a missing object. Invalid requests are rejected before HTTP. The full Rust suite passes 196 tests with ten opt-in cases ignored; the release capacity check still passes 10,005 databases.

The existing upload-credit wrapper preserves this range interface. A real HTTP regression spends the entire byte/attempt allowance on one 16 KiB object, refuses the next PUT, and reads a 64-byte slice with zero additional authority observations or mutations. Wrong object sizes and invalid ranges remain errors. Run `cargo test --locked --lib exhausted_upload_credit` (0.05 seconds after compilation). This fixes wrapper compatibility; native physical-budget initialization and per-tenant accounting remain open.

The opt-in provider fixture reads five verified slices of a 16 MiB object on B2 and versioned MinIO, without any register calls. A first B2 attempt failed during upload and produced no observed versions during cleanup; its cause was not classified. A separate fresh-bucket run passed. No uncertain PUT was retried. Both failed and successful logs are retained, and all buckets, versions and temporary keys were removed. [Evidence and complete samples](evidence/storage-ranges-2026-11-09.json).

This is a transport contract check, with no change to the publication state machine and no new formal-proof claim. Byte integrity and immutable identity remain assumptions; range/length checks alone are not a checksum. It does not yet connect large-table references, add a cache or establish whole-process memory bounds. Debug timing samples are not a production benchmark; MinIO is not AWS.


## External immutable files

`storage::immutable::Blob` returns an opaque identity, size and SHA-256 digest only after a known-successful upload to a fresh `tables/` name. Unknown uploads return no capability and are never retried under that identity. Complete reads verify the digest; exact partial reads rely on the native table's authenticated blocks. Native checkpoints publish these references. Physical deletion remains outside this primitive.

`ExternalFiles.tla` checks fresh uploads, unknown completions, delayed arrivals and atomic publication of object groups with **2,197 distinct states** (three physical names, two values, two logical names). Data and index uploads can complete in either order; a table reference requires both successful replies. Negative controls detect early reference publication and identity reuse; coverage controls reach a late unreferenced upload and a published two-object group. The table regression requires overlapping PUTs and checks either lost reply and cancellation after both objects arrive. It fails against serial uploads with a simulated timeout; no unknown result returns a table reference. Run `scripts/check-model.sh ExternalFiles`. The assumptions are atomic immutable storage and unique identities, not a Rust refinement, provider or collector proof. The retired third-party index experiment and its redundant reports have been removed; current native table/checkpoint evidence follows.

## Native sorted tables

`src/index/table` implements an independent Rust table format, without another database dependency or a second WAL. One immutable data object contains revisioned sorted blocks; a separate immutable index records their exact extents, first/last key-and-revision bounds, row counts and SHA-256 digests. The 152-byte reference includes both object identities, sizes and digests. Opening validates the complete bounded index against that reference; each data range is verified before decoding. Publication, physical charging and reference retention remain caller obligations. The generic immutable-object primitive now lives in `storage::immutable`.

`SortedTable.tla` checks sparse lookup across every nonempty subset of six ordered revisioned rows, arbitrary block boundaries, three query keys, four snapshot revisions and corrupt reads: **16,188 distinct states**, 18,263 generated, depth 10. Negative controls detect seeking by key alone when versions span blocks, and treating corrupt data as absence. Rows represent values or deletion markers; the reader must return the visible row to its merge caller. This finite lookup model assumes validated metadata and checksum detection; it is not a cryptographic, Rust-refinement, merge or serving proof.

Seven native-table tests cover versions spanning blocks, tombstones versus empty values, cold reconstruction, maximum keys/values on a local directory, unknown upload outcomes, malformed references/indexes, interrupted cursors and corrupt remote blocks, disposable disk-cache recovery, and 24 randomized comparisons with a revisioned reference map. Clippy passes with warnings denied. The full suite and the 10,005-database release capacity check remain green. [Exact source, counts and logs](evidence/native-tables-2026-11-09.json).

The same opt-in fixture passes on actual B2 and versioned MinIO: 1,024 keys, 3,072 stored versions, an ordered scan at revision two with 205 tombstones, and reopening without caches. After opening, a cold point lookup fetches exactly one data block; 2,000 repeated cached point reads make zero additional object calls. Both use the existing 256 KiB memory-cache budget and make zero shared-register calls. Debug timings are retained as integration samples, not latency/capacity SLOs or comparisons with the retired index. MinIO is not AWS. Both dedicated buckets and all versions were deleted, and the B2 workload key was revoked.

The format bounds data at 64 MiB, the index at 8 MiB and each block at 128 KiB/4,096 rows, with at most 1,024 blocks. Those bounds do not establish whole-process RSS, tenant admission, bounded history or a complete database-size limit. Table construction buffers its output, and active cursors/index metadata require separate aggregate admission. The merge layer below composes table reads; checkpoint publication and recovery are described below; general compaction and durable reader/backup retention remain open. No table lookup itself establishes current ownership or a globally retained snapshot.

## Native table merging

`TableMerge.tla` checks every input assignment for two sources, two keys and two positive revisions, including values, deletion markers, absence and conflicting versions: **22,512 distinct states**, 46,274 generated, depth 10 (about one second locally). The model requires every source frontier before emitting the next key in either direction at its highest revision. Failed reads preserve the unknown frontier; contradictory winning versions stop the scan. Negative controls detect choosing the older version, dropping deletion markers before merging, treating failed reads as end-of-source, and selecting the wrong reverse frontier. Inputs assume each source already selected its highest visible row at the same snapshot; the model does not establish source-set completeness, retention, authority or Rust refinement.

Both directions use the same table, memory, merge and checkpoint cursors. Reverse pages seek from their exclusive upper bound and retain the highest visible revision across block boundaries; they do not materialize and reverse an entire range. A cold reverse page in the 183-table fixture needs at most six range reads after opening its database.

The Rust layer uses a bounded heap for ordered scans and at most eight concurrent point-read requests. It permits 64 sources per read, a work bound that future manifest selection/compaction must maintain, not a database-count cap. It returns deletions as rows and rejects contradictions only at the highest visible revision, independently of source-completion order. Scan I/O failures and cancellation preserve loaded heads and the unread source; a contradictory winning row permanently retires that cursor.

Seven tests cover overlapping histories and binary ordering, empty values and deletion, contradictions and duplicate versions, interrupted/cancelled reads before and after output, block-boundary retry, empty/over-budget source sets, bounded I/O concurrency, and 32 randomized comparisons of point/range reads with a union reference map. The existing full suite includes HTTP regressions; no serving behavior is changed. [Source and validation evidence](evidence/table-merge-2026-11-09.json). This merge layer does not itself publish checkpoints, compact files or grant serving authority. Aggregate cursor/index memory and fairness remain controller admission obligations.

## Native memory and selected recovery

`src/index/memory` retains rows by binary key and descending revision, including deletion markers and empty values. Admission checks the entire evaluated commit against version and payload-byte limits before any row or revision changes. A borrowed cursor prevents mutation during a scan or flush. The table merge accepts this cursor alongside immutable tables, uses the same snapshot for every source and retains the highest visible revision. A newer memory version can supersede contradictory older table versions; a contradiction at the winning revision still fails.

`src/index/recovery::Projection` implements the selected-journal recovery target. It keeps revisions and retry receipts with each database generation and accounts for all resident generations together. The runner consumes and discards the private projection after any decode, I/O, admission or application error. Receipt eviction releases receipt capacity without discarding retained row versions. Recreated names have independent histories and receipts. Point/range queries reject revisions ahead of the completed database history. Flushing those rows to a table preserves value history but does not publish metadata or authorize removal of the journal.

`MemoryIndex.tla` checks two keys, deletion/empty/nonempty values, up to three commits, a three-version capacity and four query revisions: **5,706 distinct states**, 62,353 generated, depth 5, with a one-live-key/two-byte logical quota. Exact live-key/byte counters use the previous value for replacements and distinguish empty values from deletions. Four negative controls detect partial mutation on rejection, returning current values for an older snapshot, incorrect counter deltas and bypassed quota admission. The selected replay/receipt model is rerun unchanged. These finite component models do not prove their Rust composition, source-set completeness, physical timing, allocator behavior or the serving protocol.

The Rust checks cover admission failure before mutation, revision gaps, historical point/range reads, 32 randomized reference histories, flushing and merging with tables, and contradictions hidden by newer memory rows. The owner-replacement test now uses the native projection in place of its test-only value map, checks receipts and excluded late old-owner uploads, and flushes/reopens the result without caches. Additional tests exhaust every aggregate limit, isolate recreated generations, verify receipt accounting through eviction and recover **10,005 database generations** through the actual selected-journal cursor. This is a component capacity check, not concurrent serving capacity. The real-provider fixture also passes on B2 and versioned MinIO with two cache-free overlapping tables: 1,025 merged rows, 147 newer deletions, four historical point snapshots and zero shared-register calls. It tests table merging, not native transaction recovery on those providers or AWS. Both buckets and all four versions in each were removed, the B2 key was revoked, and the MinIO fixture stopped. [Exact source and validation evidence](evidence/native-recovery-2026-11-09.json).

Default limits per recovery target are 16,384 resident database generations, 262,144 retained row versions, 64 MiB of key/value payload, 65,536 receipts and 16 MiB of receipt payload. They are configurable worker admission limits, not bucket-wide database caps. Version/receipt counts bound collection entries; payload counters exclude allocator and collection overhead, borrowed journal frames, temporary receipt advancement, returned read buffers and concurrent targets. Whole-process admission remains open. Without checkpoints, complete retained history must fit these bounds. The checkpoint/recent-journal path below retains only suffix values and touched database receipt metadata in those resident budgets. No HTTP serving API, physical collector or runtime ownership contract is changed.

Native authorized writes enforce the limits recorded at creation before journal admission. Pending and committed retries resolve first. Quota evaluation reads only changed keys, adjusts the committed live-key/key-plus-value byte totals, and checks the final map; deleting missing keys and repeated write keys cannot distort accounting. Whole-database revision validation prevents two pending requests from consuming the same remaining allowance. Mixed batches return quota failures without rejecting valid neighboring databases or uploading rejected writes.

The projection updates these counters atomically with published records. Checkpoint metadata stores them alongside the revision and receipt floor; full-image verification compares them against actual values. Lazy reads use authenticated metadata without scanning the database. Tests cover exact bounds, overwrite growth/shrinkage, empty values, deletion, duplicate mutations, grouped conflicts/retries, checkpoint rewrites, and fresh-owner recovery. The S3 HTTP fixture checks quota rejection and retry without new object uploads. `MemoryIndex` above checks the bounded accounting/admission rule; Rust tests check checkpoint/recovery composition. These logical quotas do not bound retained physical objects, upload costs or egress.

## Native checkpoint collection

Native table collection, introduced in `owned-candidate-12` and retained by both current native formats, extends the existing collector to obsolete data, indexes and manifest nodes. Each sweep lists at most 128 table identities before an exact authority comparison. The existing manifest walker protects references reachable from the published checkpoint without fetching table indexes or values. Corruption, a missing manifest, a failed comparison or an uncertain fence grants no table-deletion permission. Collection creates no immutable object and refunds no upload credit. Its cursor survives collector replacement.

Before deleting any table candidate, the collector advances a persisted generation in its existing metadata field. Checkpoint staging captures that generation before upload. Publication cannot adopt a later generation after revalidation: the live owner discards invalidated staging and rebuilds with fresh identities, while resolving an already-published image without reuploading it. Metadata/journal-only sweeps leave the generation unchanged. Earlier native formats are rejected; use a fresh namespace.

`PublicationSafety` already models this list-before-fence protocol and bounded retained/fresh references. The unchanged safe model checks 175,546 states (810,459 generated, depth 16) in two seconds; a fourth negative control demonstrates the failure caused by adopting a collector’s generation. The existing abstract proof remains separate from Rust refinement. Focused Rust tests cover a paused checkpoint, collected staging, rebuild, two sweep pages and fresh collectors, corruption, cold values/receipts and nonrefundable credits. The multi-level reference fixture protects every table and manifest using four manifest reads and no data/index reads.

Protection traverses the current manifest until all candidates are protected or traversal completes; memory and deletion batch sizes are bounded, but total manifest work grows with checkpoint size. The existing worker deadline is 30 seconds, so large-image progress and contention require qualification. Raw checkpoint handles do not pin history. Native serving requires final ownership checks; durable reader/backup pins, compaction, registration/catalog-header cleanup, shared cost attribution and physical timing remain open. Real B2 and versioned MinIO pass automatic collection and two cold reopens, retaining exactly the published five table/index/manifest objects, values, receipts and budgets. [Source, prior failures, checks and cleanup evidence](evidence/native-table-collection-2026-11-10.json). This does not establish production readiness or a proof of the implementation.

## Native checkpoints

The native authority stores the complete checkpoint descriptor in the authority record's existing 1 KiB reservation. There is no checkpoint header object to upload, fetch, cache or collect. The descriptor retains the exact journal cut and authenticated manifest reference; table/manifest uploads still complete before conditional publication. `Snapshot::new` is synchronous and performs zero object reads. Unchanged checkpoint rewrites now make zero immutable uploads; the mixed changed-range fixture needs five instead of six. The 10,005-generation fixture uses 17 objects instead of 18. Maximal descriptor counts and control integers fit the reserved field without adding a B2 metadata slot or changing the namespace budget reserve. Malformed/oversized descriptors and prior native formats are refused. Old candidate namespaces need a fresh namespace; no migration is implemented.

The bounded checkpoint model already stores the descriptor in its atomic register; its upload action abstracts the complete table/manifest image. The same 65,664 states and nine negative controls pass without additional model state. Local validation passes 276 Rust tests, formatting and strict clippy; the separate release-capacity regression remains in CI. Real B2 and versioned MinIO pass the native HTTP fixture with the inline descriptor: six table/index/manifest objects for two checkpoints, automatic journal collection, two complete cold reopens, exact values/receipts and unchanged nonrefundable budget allocations. The fixture now proves a sweep occurred after setup and retains the recovery catalog after takeover. [Evidence](evidence/inline-checkpoints-2026-11-10.json) records the two preceding fixture failures as well as the passing runs and complete resource cleanup. This removes header-object garbage at its source. Table/manifest reclamation is described above; durable reader retention, full tenant cost attribution and physical ownership timing remain open.

The native maintenance loop now reclaims earlier registered ownership journals and the checkpoint-covered prefix of its named journal in batches of at most 128 candidates, using the existing list-before-fence cursor and physical deletion transport. It authenticates the published checkpoint cut, retains its suffix, and makes no immutable PUT or credit refund. The former manual-deletion fixture is replaced by real collection across three owners, including a paused unpublished checkpoint, lost publication/fence replies, exhausted byte allowance, malformed inline checkpoint descriptors, tombstones, empty values and receipt recovery. The focused regression passes in 0.01 seconds; exact journal-key parsing rejects malformed names. The collector moved from the upload allocator to root ownership; the duplicate catalog-protection decoder was removed. Table/manifest collection is described above; registration and catalog-header reclamation remain open. A second regression covers 130 frames across two sweep pages, new collector instances, missing/corrupt registrations, late old uploads, another incarnation at the cut’s epoch, a foreign partition, and cold recovery by a fourth owner. The 128 retained receipts resolve exactly; older receipts remain expired. Both tests finish in 0.03 seconds. A temporary unsafe `<=` epoch comparison fails the protected-journal assertion; the correct strict comparison passes.

The live owner now schedules checkpoints after a configurable frame count (default 256), including replayed suffix frames across takeovers, or earlier under resident pressure. New admission pauses until pending uploads drain. Publication binds the complete committed projection to the writer's exact prefix; a confirmed result rebases the projection onto the lazy image and expires old snapshot tokens. Retained receipts still resolve original requests. Cancellation preserves an attempted publication until exact revalidation determines whether it won; retries reuse the staged image only while its collection generation remains valid. Checkpoint construction freezes mutations but shares the committed projection with readers during funding and immutable uploads. Publication and projection replacement remain exclusive. Unchanged tables are reused; manifest traversal, changed-table work and publication still need latency qualification. The automatic maintenance task reclaims the published checkpoint’s covered journal prefix; obsolete table/manifest objects are collected; registration and catalog-header objects remain retained.

The integrated tests exercise repeated scheduled checkpoints, receipt expiry, deletion, empty generations, a lost publication reply, cancellation after applied publication without reuploading, authority expiry during an immutable value read, and admission draining with unresolved uploads. A takeover fixture runs the automatic collector’s sweep and verifies discovery and replay read only the remaining suffix.

`index::recovery::Recovering` keeps a private projection at complete selected-journal frame boundaries. Each bounded advance consumes that state, so cancellation or failure cannot expose or reuse a partial projection. Staging records every database generation's revision, live-key/byte totals and receipt-retention floor, all retained request IDs/fingerprints/revisions, and each key's latest value or deletion. Older value snapshots are rejected after restoration; the recorded retry window remains intact.

Checkpoint tables are shared across databases. Composite keys contain the database name, generation and record kind; receipt keys use request IDs, and user values use application keys. A table chunk holds at most 4,096 rows or 4 MiB of encoded rows before table/block headers. A bulk-built manifest records ordered upper key bounds with at most 64 references per node, an eight-level height bound and a 144 KiB node-read bound. Construction and traversal retain bounded per-level buffers. The manifest supports the lazy read path described below. `Recovering` now retains the checkpoint as a lazy backing image and reconstructs only the selected suffix, as described below. The checkpoint descriptor is now inline in the authority record, with no header upload, object read or header garbage. Immutable tables and manifest nodes use fresh identities and authenticated references.

`Root::publish_checkpoint` compares the staging controller's exact recovery snapshot before conditionally publishing its inline descriptor after all referenced objects are uploaded. New claims preserve that reference. An uncertain or abandoned comparison requires exact revalidation, and a stale staged result cannot replace a newer checkpoint. Takeover discovers from an inherited checkpoint even when its writer has no selected end yet. Recovery then verifies the cut's selected identity and prefix and checks the remaining journal hash chain through that end. A checkpoint can lie within a selected journal. The ancestry walk stops at its owner instead of revisiting earlier registrations. Unknown uploads produce no publishable handle; neither construction nor recovery deletes physical objects.

`Checkpoint.tla` checks all **216** three-record histories over two keys and three values, two owners, one unrelated funding update, upload-before-publication, a freshly observed or delayed captured conditional update, exact-cut restoration and selected suffix application: **144,288 distinct states**, 386,208 generated, depth 13. Nine negative controls detect early publication, a mismatched cut, omitted receipts, omitted values, a comparison bypass, exposing incomplete recovery, selecting a takeover suffix below an inherited checkpoint, collecting from an unpublished cut, and collecting its later suffix. The safe instance completes in one second. It abstracts selected epochs into an ordered history and assumes monotone checkpoint publication, durable immutable images and atomic comparisons. This is not a Rust-refinement, manifest-format, physical-clock, checkpoint-object collection, whole-protocol or liveness proof. The existing transaction/receipt model is rerun separately; composition remains an obligation.

Thirteen additional Rust checks cover lost replies at every upload stage, cancellation after manifest upload, incomplete receipts/counts, corrupt/missing pages, malformed routing bounds, failed-reader retirement, maximum keys and manifest levels, local-directory reopening, checkpoint/claim races, abandoned publication, stale cuts, selected-chain mismatch and missing suffixes. Tests remove covered frames and older registration records to establish that recovery does not read them. Repeated checkpoints recover 140 revisions with a two-version resident budget, retaining the declared 128-receipt window. **10,005 database generations** fit eight shared tables plus one manifest node (17 physical objects), and reconstruct after the journals are removed. This is component capacity, not serving concurrency. The initial checkpoint validation passed **233** tests with 12 opt-in cases ignored and the existing release-capacity check reserved for CI.

The initial eager-recovery fixture passed on B2 and versioned MinIO: 1,024 databases, 24 original selected frames, a checkpoint at frame 16, sixteen covered frames removed, and nine suffix frames replayed across a fresh connection/controller. All 6,400 receipts and 3,328 retained value versions reconstruct; late old-owner data is excluded. Reconstruction uses 27 object GETs and zero control-register calls. Subsequent in-memory verification makes zero object calls. Debug timings are integration samples, not SLOs or throughput measurements. Writers stop before takeover and clocks are injected: live stale-owner fencing, physical timing, Kubernetes and AWS are not qualified by this fixture. Both buckets and every physical version were removed and the temporary B2 key revoked. [Exact source, test, model, provider and cleanup evidence](evidence/native-checkpoints-2026-11-09.json).

Production obligations remain: qualifying the native HTTP runtime, complete tenant cost isolation, tenant-isolated retained readers, backup scheduling and broad collection qualification. Resident suffix values and touched database receipt windows must fit configured recovery limits. Payload/count budgets are not whole-process RSS bounds. Registrations remain retained; current checkpoints are protected by the collector described above. The earlier cloud fixture above used deliberate removal; the current prefix collector has separate regression evidence.

## Lazy checkpoint reads

`checkpoint::Snapshot` uses the inline descriptor and routes through inclusive manifest upper bounds to a requested database generation, value or request receipt. It does not load all database metadata, values or receipts. A point query reads at most one routed table index and one value block after the manifest path; an ordered range visits only its candidate tables and retains one table cursor at a time. Tombstones and empty values remain distinct. Receipt lookup checks the recorded retention window and fingerprint before considering the request's snapshot age.

Manifest nodes, eligible table indexes and data blocks share the bounded memory/disk cache. Cache keys include authenticated content digests; corrupt disk entries become misses and authoritative read failures remain errors. The existing 512 KiB cache-entry ceiling still applies, independently of bounded object reads. A failed or canceled checkpoint scan is permanently retired and must restart; it cannot skip the interrupted table. Root identity capture and staged readback expose no serving permission. Lazy reads rely on the complete checkpoint produced by the builder and validate the objects they visit, rather than rechecking every row/count/receipt in the image on every query.

`CheckpointRead.tla` checks all fifteen nonempty subsets of four ordered keys, four split positions and six queries, including exact table maxima and gaps: **1,440 distinct/generated states**, depth three. Two negative controls detect an exclusive upper-bound comparison and treating a failed read as absence. Complete ordered bounds and immutable bytes are assumed; the model does not prove the concrete tree or cache, scans, serialization, receipt completeness, Rust refinement, ownership or liveness.

Six Rust tests exercise 183 tables across multiple manifest nodes and three database generations, all key lookups and gaps, range boundaries, receipt expiry/conflicts, bounded cold I/O, warmed reads, cancellation, corrupt disk cache, and missing/corrupt authoritative manifest, index and data objects. A warmed mixed query repeats without provider calls. The existing full suite continues to cover HTTP behavior, which is unchanged.

The initial lazy-read B2 and versioned MinIO fixture checked all 1,024 checkpointed databases before eager recovery. Opening the checkpoint and looking up one value and one receipt takes **four object GETs**; 1,000 warmed iterations of point lookup, receipt resolution and ordered scan take **zero provider calls**. All lazy reads make zero control-register calls. The configured 4 MiB cache accounts for 412,470 bytes after the full fixture. The original nine-frame suffix recovery also passes. Both dedicated buckets, all versions and the temporary B2 key are removed afterward. [Source, checks and provider evidence](evidence/checkpoint-reads-2026-11-09.json).

These are debug integration samples, not latency/throughput SLOs. Checkpoint reads represent only the recorded cut, excluding later selected/live journals. The integration below combines it with a bounded selected recent layer. The native owner now supplies final serving checks and checkpoint scheduling. Process-wide admission, scheduled retention and large-image collection qualification remain open. Injected clocks and stopped writers do not qualify a live deployment, Kubernetes or AWS.

## Checkpoint and recent journal composition

`Recovering::begin_cached` retains an authenticated checkpoint as the base and resumes its exact selected cut. It loads metadata and the complete retained receipt window only when a suffix record touches a database generation. Only suffix value versions enter the resident index. An empty suffix therefore requires no resident database/value/receipt entries, regardless of the database count in the checkpoint. The controller supplies a bounded shared cache; `begin` uses no cache. Limits still apply to touched databases, their receipt windows and all recent versions, and do not bound aggregate process RSS.

`Projection::view` combines that base with the selected recent layer. A visible memory value or tombstone answers a point lookup directly; otherwise the view reads the routed checkpoint block. Scans merge a checkpoint cursor with the recent memory cursor at one supported revision. Untouched database receipts stay paged. Touched receipt windows resolve entirely from restored metadata, including expiry and fingerprint conflicts. The read view is available only after successful complete recovery and still grants no serving authority.

Checkpoint construction walks the authenticated manifest and reuses unchanged tables without opening their indexes, reading their values or uploading either object. The interval after the previous manifest upper bound through the current inclusive upper bound conservatively identifies tables that may need replacement; gaps can cause extra work. A table is rebuilt if it overlaps a recent value change or the metadata/receipt interval of a database with committed changes. The latter includes expired receipts absent from the replacement window. Changed rows merge in order into bounded chunks; databases inserted before or after the old image remain ordered. The authenticated base row total carries forward: subtract each replaced table’s validated row count, then add emitted rows. Manifest table counts and the bounds of opened tables are checked. Retained indexes are validated when read, not by this rewrite. Disordered bounds across manifest leaves are rejected before querying resident ranges; removing that guard reproduces a range-lookup panic. The builder assumes the authenticated old image is complete, just as lazy reads do; `Snapshot::verify` remains the explicit complete-image audit, including live usage counts. No objects are deleted.

A focused seven-table fixture reuses all seven tables and their manifest with **zero uploads** and **one manifest read**, with no index or value reads, when unchanged. It first warms receipt metadata without committing: that regression reproduced four uploads before the fix. Checkpoint dirtiness now compares committed revision with the base revision, so retries and refusals do not force table rewrites. When no journal frame has advanced beyond the published checkpoint and no upload/publication is pending, live maintenance now discards clean receipt metadata locally instead of constructing a checkpoint. The live wire-admission test repeats exact retries and stale-revision refusals across that cache threshold, with **zero immutable uploads and zero authority mutations**, and verifies that resident metadata is cleared. Pending publication, recovered suffix frames, newer live frames, missing state or changed revisions prevent this shortcut. It never deletes provider objects or refunds credit. A mixed-table regression changes a neighbor while retaining the clean database’s complete receipt window. Boundary updates, deletion, receipt expiry and new databases preserve three old tables and need **five uploads**. The fixture deliberately creates small tables to exercise reuse; its upload count is not a comparison with full-image packing. It verifies every final value, live usage, retry receipts and the old snapshot, and checks cancellation. A missing retained index still fails explicit full-image verification; a missing replacement index aborts the rewrite. These are deterministic fixture I/O counts, not cloud latency or throughput claims. The superseded full-image rewrite loop and duplicate full-scan accounting were removed. Fragmentation, compaction scheduling and scheduled/tenant-isolated retention remain open; table/manifest collection is described above.

The 183-table cold fixture reduces an unchanged checkpoint from **187 object reads to 4 manifest reads**, with the previous implementation checked as a failing control. A deletion exactly at a manifest upper bound reads **10 objects** (four manifest nodes and three table index/data pairs), touches no objects belonging to the two idle database generations, preserves all 540 value/deletion results and retains the correct retry windows. An intentionally exclusive upper-bound comparison fails this regression. The existing row totals are retained and adjusted only for replaced tables; no extra per-table metadata, cache or storage-format change is needed. This removes repeated index reads, not full-manifest traversal or the need for tenant maintenance accounting.

Manifest reconstruction compares each candidate's complete encoded length and SHA-256 digest, including child identities and bounds. It now finishes the current base group before adopting its successor's reuse candidate. Pending data rows flush at that boundary too. Overflowing rewrite groups split evenly instead of leaving a full node and a one-entry tail; fresh bulk builds remain densely packed. The reader and builder each retain at most nine candidate references, with at most 65 pending entries per builder level. There is no additional provider read, persisted metadata, format change or orphan adoption.

Inserting at the front of a 4,096-table manifest needs **five uploads instead of 68**. At 8,193 tables, a prefix insertion needs five, a deletion three and removing a whole leaf two. Unchanged reconstruction uploads nothing, including a second reconstruction of the newly split tree. Forward/reverse order and every descriptor are checked across empty trees, leaf/root splits and collapses. Repeating 128 prefix insertions into 64 tables keeps height one; full-plus-one splits fail that bound. An applied-but-lost or cancelled boundary flush returns no new root and leaves the old manifest readable.

The serving projection regression merges the final three tables of a leaf into one. It makes **four uploads instead of six** (one data/index pair, one leaf and its root), retains the two untouched sibling leaves, reads the same ten objects, and verifies all 540 values/deletions. Restoring the previous grouping or omitting the data-buffer flush fails the upload regressions. [Milestone evidence](evidence/manifest-boundaries-2026-11-10.json) records the checks. These are deterministic I/O counts, not a cloud latency/throughput result. Full-manifest traversal, sparse-node compaction, retry/recovery attribution and physical timing qualification remain open.

The updated checkpoint/recovery fixture passes on **B2 and versioned MinIO** at `f6337e1`: 1,024 databases, nine selected suffix frames, complete value/receipt verification, zero control calls during reconstruction, and fresh reopen without resident values or receipts. The first B2 attempt failed during checkpoint rewriting before publication; the earlier fixture suppressed the underlying error, so its cause remains unclassified. A fresh B2 run passed. Both attempts and cleanup are recorded; the initial failure is not treated as a pass. The fixture now retains static invariant diagnostics without printing provider details, and the local reuse test adds an applied-but-lost manifest response. All dedicated buckets, physical versions, test keys and the disposable MinIO container were removed. These are debug composition checks with injected clocks, not production performance or AWS qualification. [Run evidence](evidence/checkpoint-reuse-2026-11-10.json).

`CheckpointOverlay.tla` checks 1,728 three-record histories over two databases, two keys and three values, with all three checkpoint cuts: **31,104 distinct states (34,560 generated)**, depth five. Eight negative controls catch discarded suffix tombstones, omitted base receipts for touched databases, early recovery exposure, rewriting only the recent layer, reusing a changed table, reusing a changed manifest, discarding a selected suffix during cache eviction and excluding an interval’s upper boundary. These extensions add no distinct states to the safe instance; it abstracts subtree equality, not hashing or the Rust encoding. Reuse is checked with ordered table bounds that cross a database boundary. It assumes a complete immutable base and selected history. Unique request identities stand in for receipts; retention/fingerprint behavior has separate tests and models. It does not prove Rust refinement, physical timing, publication/ownership composition, collection or liveness.

The combined test rewrites over 256 KiB of checkpoint values with a **32-byte resident value budget**, preserves an untouched generation, compares historical point/range reads, exercises four admission failures, and checks expired receipts after rewriting. Receipt restoration rejects missing/duplicate history, enforces count/byte admission and restarts cleanly after failure/cancellation. The existing **10,005-generation** check now reopens with every resident limit set to one and verifies all generations with **zero resident database/value/receipt entries**. Those limits are work budgets, not database-count caps. The local suite passes **241 tests**, with 12 opt-in cases ignored and the release capacity regression reserved for CI.

B2 and versioned MinIO repeat the 1,024-database campaign, including nine selected suffix frames. Recovery retains **2,304 recent versions**, excluding the 1,024 checkpoint values, and **6,400 receipts** for the touched databases. It makes 27 object GETs and no control-register calls. A rewritten checkpoint reopens with four object GETs and zero resident database/value/receipt entries; all 1,024 generations are verified again. The B2 bucket's 65 versions and MinIO bucket's 79 versions were removed, both buckets deleted and the temporary B2 key revoked. [Source, tests, model and provider evidence](evidence/checkpoint-overlay-2026-11-09.json).

The campaign uses debug builds, injected clocks and stopped writers. It qualifies data-path composition, not live serving, performance SLOs, Kubernetes behavior or AWS. Current recovery still requires its selected suffix and touched receipt metadata to fit admitted memory. Live admission, contiguous durability publication, fresh permit validation and scheduled checkpoints now use this same projection. Native cloud CLI and placement are integrated; whole-process resource and production qualification remain open.

## Native point-read CPU cost

The table reader now validates a block through borrowed rows and copies only the point lookup's winning row. Previously it allocated/copy-filled every row before selecting one. Scans use the same validator and still return their full decoded block. The format, hashes, row ordering and first/last bounds are unchanged; even a successful point hit must validate the rest of its block. A new adversarial test reauthenticates a malformed later row and confirms that neither point lookup nor scan returns the earlier valid row.

The opt-in release measurement uses 4,096 keys, 20,000 deterministic lookups per value size, one task and a warmed 32 MiB byte cache on an AMD Ryzen AI MAX+ 395 host. Every seventh key is a tombstone. The authoritative fixture is in memory and both runs make zero provider GETs. The before/after runs use the identical measurement source and record source hashes.

| Value bytes | Before mean | After mean | Mean CPU-time reduction |
| --- | ---: | ---: | ---: |
| 0 | 172.3 µs | 16.5 µs | 10.4× |
| 32 | 132.8 µs | 8.8 µs | 15.1× |
| 4,096 | 4.0 µs | 0.93 µs | 4.3× |

Smaller values pack more rows into each block, so complete block validation does more work per lookup. These are single-host component measurements, with no HTTP, ownership, provider latency or concurrent workload. They are not database throughput or latency SLOs. Eight table tests and the existing SortedTable model/negative controls pass; the full regression suite and B2/MinIO checkpoint campaign also pass. [Exact measurements, source and validation](evidence/index-read-cost-2026-11-09.json). Reproduce the release measurement with the [development command](development.md#work-on-one-component).

## Independent partition control

The partition adapter reuses the shared publication directory rather than a new CAS algorithm. `spec/PartitionDirectory.md` maps its target and unrelated control records onto `PublicationRebase`; `PublicationSafety` and `PublicationCombining` cover directory collection and delayed proposal publication. Native root/permit ordering remains unchanged. This mapping is not a composed proof of placement or Rust.

Four focused Rust tests cover independent claims and takeover, cross-partition collection isolation, scoped range reads/list cursors/physical deletion, invalid paths, 66 partition entries across a directory split, ceilings preserved on reopen, exhausted shared admission, incompatible legacy opens, stale comparisons and lost publication replies. They complete in 0.64 seconds after compilation. Two deliberate Rust faults—omitting the object scope and replacing the captured comparison with a fresh one—are detected by running tests; the restored source passes again. `PublicationRebase` checks 195,642 distinct states at depth 24; its three broken variants still expose lost unrelated updates, overwritten target updates and collection races.

Automatic membership, election, HTTP routing, database mapping, partition removal, fairness, shared maintenance reserves and end-to-end cluster qualification are outside this adapter. Ordinary native response validation still depends on the provider's fresh-read-after-revocation contract.

At `00637a4`, the native HTTP partition fixture passes on B2 in 52.98 seconds and versioned MinIO in 0.99 seconds. It also verifies that a scoped GET and a projected control read count once at the actual provider boundary. Three live servers cover two partitions, concurrent writes, distinct values, checkpoint recovery, exact retries, old-owner read/retry refusal after takeover, continued progress in the unmoved partition and shared metadata collection. Both final disposable buckets, all 88 remaining physical versions, the B2 workload key and MinIO container were removed; the first run’s two buckets, key, container and 90 versions were also removed. This is explicit library placement, not automatic election/routing or an autoscaling/SLO claim. [Evidence](evidence/partition-directory-2026-11-11.json).

## Owner discovery and forwarding

`Root::advertise` binds an optional socket address to a fresh ownership incarnation in its existing control record. An open observation is a route hint, never serving permission. `api::Forward` caches one route for an explicitly selected partition and sends one authenticated HTTP attempt. The receiving backend checks the incarnation before dispatch; ordinary policy checks and fresh writer-permit validation remain mandatory. Tenant claims keep their expiry, policy generation and revocation identity. Administrator forwarding uses a signed 60-second token, never the signing-key bearer. The forwarder refuses redirects, disables transport retries/proxy inheritance, bounds concurrency and bytes, and clears only the failed cache instance. Readiness indicates the forwarding process's admission state, not owner availability. Deployment still needs a trusted private peer network or transport mesh and a shared signing-key ring.

`OwnerRouting` checks **728 distinct states, 3,197 generated, depth 11**. It covers address reuse, stale discovery, pinned destination identity, one forwarding attempt, synchronous route invalidation on cancellation and retirement after an idle permit observation confirms absence. Nine negative controls detect omitted identity checks, replay after a lost reply, reselecting after admission, retained busy/cancelled routes, readiness substituted for permission, failure to retire an idle revoked owner, retirement from a preference change alone, and retirement on an unavailable response-validation read. A failed fresh check suppresses the state-derived reply with HTTP 503 while retaining a locally healthy worker. Confirmed absent permission or an already failed local view still retires it. Two real TCP regressions fail against the previous implementation, then pass for rejected commands and sealed durable commits; identical retries preserve the incarnation and never resubmit a durable journal. The initial state now includes a confirmed claimant before activation; preference may discard neither its recovery nor the later serving backend. The availability subcase assumes a healthy process remains in membership; failure and explicit removal may still stop service. Run `scripts/check-model.sh OwnerRouting`. Errors stutter in the probe model; eventual observation and scheduling remain liveness assumptions. The existing `RevocableOwner` argument supplies response/permit ordering. These separate bounds do not establish composed placement correctness or Rust refinement.

Four TCP/root regressions complete in 0.10 seconds after compilation. They cover live takeover, exact receipt replay after a lost response, wrong-partition address reuse, token revocation, cached discovery, delayed failures racing cache replacement, redirect refusal, overload and oversized responses. Two deliberate Rust faults omit the destination check or clear a refreshed route on a late old failure; both are detected. An authentication regression checks unchanged tenant claims across signing-key rotation and rejects expired claims. The common journal-selection loop replaces duplicate local-startup and cloud-fixture implementations.

At `80841e5`, the expanded four-server/two-partition fixture passes on B2 in **51.42 seconds** and versioned MinIO in **1.48 seconds**. Create/write/read requests traverse the gateway; a cached stale route returns 503 after live takeover, and the caller's explicit identical retry discovers the successor and returns its retained receipt. The unmoved partition keeps serving. Shared ceilings, checkpoint recovery, collection and physical metric checks still pass. Both disposable buckets, all 88 remaining physical versions, the temporary B2 key and the MinIO container were removed. These are debug functional runs, not latency/throughput benchmarks, AWS qualification or automatic placement. [Evidence](evidence/owner-routing-2026-11-11.json).

## Storage-path owner challenges

Before replacing an advertised owner excluded by local peer health, the controller
publishes or joins an incarnation-bound random challenge in the existing partition
metadata. It waits 15 seconds for that same owner and nonce to remain unanswered;
only then does it attempt the ordinary exact conditional claim. A response, changed
incarnation/nonce or uncertain challenge publication defers takeover. A completed
wait survives conflicts, provider errors and cancelled acquisitions only while
the same owner and nonce remain. One bounded owner/nonce/deadline descriptor per
partition records it. Pending waits release their acquisition and resident slots;
the ordinary three-second scheduler revisits them. Four active acquisitions remain
the limit. No sleeping future or one-second polling loop holds up later partitions.
Known healthy peers can still rebalance; reuse of this process's address takes the
normal fenced recovery path directly. Confirmed acquisition continues while this
process remains in membership, even after preference changes; an unconfirmed
candidate still yields immediately. Actual takeover can fence either stage.
No extra settling interval delays reachable-owner moves.

Each confirmed claimant polls once every three seconds, with one request in flight,
through activation, recovery and serving. It conditionally clears only its own
challenge. Removing the local placement stops this responder even when an in-flight request
retains the old backend. A retained incumbent that prefers another process stops
answering, allowing that successor to take over even with asymmetric peer reachability.
Ordinary owner publication, collection, budget operations and read barriers
preserve the hint; the responder is the sole answer path. An already submitted
answer may still complete after preference changes. No extra storage object or
collector is introduced. The opaque `ddb-challenge` field binds two canonical UUIDs;
older readers preserve it, so mixed-version placement convergence is not qualified.
The remote metadata budget reserves its space. Local-directory serving is unchanged.

A timeout is a failure suspicion, never serving authority or a clock-error bound.
Pauses, delayed storage or inaccurate clocks may still cause a safe false takeover.
Revocation, journal recovery and fresh response permits remain mandatory. Stable
placement requires timely storage responses and scheduling. API pods unable to
reach a partition's owner may still refuse requests during a permanent peer split.

`OwnerChallenge.tla` checks one incumbent, two challengers, one challenge round and
four metadata writes, with one shutdown withdrawal captured at the initial
revision: **3,968 distinct states, 24,897 generated, depth 14**. Seven fault
controls break comparison, coalescing, waiting, owner-only acknowledgement,
retention of a completed wait across retries, the withdrawal comparison and
the distinction between a returning member and a fallback member.
Three reachability controls exercise a live answer, replacement after a stop,
and same-member replacement without a challenge wait after withdrawal. The
responder stops before withdrawal, matching the shutdown order. A stale withdrawal
cannot clear a successor’s route. Captured requests can
apply after owner death or lost replies. This finite policy check assumes the
existing exact register/permit protocol; it is neither Rust refinement nor a proof
of arbitrary-round liveness. RootAcquisition and OwnerRouting remain separate
checks of acquisition and response authority.

The CLI now closes admission, aborts and joins the cluster scheduler and
maintenance task, retires every local resident before storage I/O, and
conditionally withdraws the final resident routes. Four
hints can run concurrently within the CLI's five-second deadline. Each hint
checks the old incarnation and captures the exact metadata comparison; it neither
activates that incarnation nor revokes its permit. The existing owner-bound
`ddb-challenge` nonce holds a UUIDv8 fingerprint of the departing stable member
when its route is absent. This is an advisory identifier, not a credential. That
member's replacement skips the wait; other members join the hint and wait 15
seconds before fallback. No new metadata bytes are reserved. If no withdrawal
becomes durable, replacements retain the ordinary challenge path. Legacy hints
without a target remain readable. Lost replies and cancelled hints cannot
authorize service. No format, object class or extra background worker is added.

Three real HTTP regressions keep the old backend alive: same-member replacement
within five simulated seconds, delayed fallback when that member stays absent,
and a returning member that prevents the temporary fallback claim. Each checks
exactly one new owner epoch, actual permit fencing and identical receipt replay.
They pass in 0.12 seconds. Removing the target lets the temporary member claim;
ignoring the target makes the returning member miss its deadline. Both negative
controls fail as required. This replaces the unconditional-withdrawal test and
the rejected experiment's broad challenge/progress-credit tests.
A 16-partition shutdown regression blocks all metadata reads, expires the drain
deadline, and requires every local resident to be retired. It detects the
previous I/O-dependent retirement order in 0.11 seconds.
The root regression covers refused writes, lost replies, stale comparisons and
wrong incarnations. These are bounded policy and contract checks, not a
production rollout SLO or a proof of the Rust implementation.

The paused-time Rust regression holds two disjoint membership views for a minute,
checks unchanged owner incarnations, stops one controller without a DNS change,
and verifies recovery and fencing of a retained old backend. Root tests cover
coalesced/uncertain challenges, stale comparisons, activating-owner responses,
non-owner metadata work, malformed hints and old-writer preservation. An injected
provider failure after the wait requires replacement within eight seconds after
service returns; discarding the completed wait makes this check fail in 0.13
seconds of real time. A second paused-time regression replaces one process owning
all 16 partitions and requires all owners within 25 simulated seconds. The old
four-slot sleeping path fails in 0.24 seconds of real time; deferring idle waits
passes alongside the four challenge regressions in 0.37 seconds. This scheduling
change implements the existing model's separate Join/Wait/Prepare actions; the
then-current 3,518 states and controls passed. It is not a cloud latency guarantee.
The first B2 campaign below preserves acknowledgements and
avoids claims during peer isolation, but misses the cold-recovery target. The
corrected v0.23.11 campaign recovers all 558 acknowledgements with zero claims
in either hold and all three targets met. v0.23.12 repeats peer isolation with 578
recovered acknowledgements and all three targets met, then recovers all 1,968
acknowledgements in the broader fault campaign. Six of twelve broader timing
targets miss; continuous split availability remains unqualified. See
[measurements and exact binaries](verification-results.md).

## Membership-driven native placement

`api::Cluster` persists a versioned layout in the existing directory-root envelope, before the first partition entry. Different partition counts and populated manual directories are refused. Publication/rebase preserves this layout from the successful comparison. SHA-256 database mapping never uses membership; rendezvous placement changes the preferred process while preserving partition IDs and object scopes. Counts are 1..1,024 partitions and at most 256 canonical peer addresses, independent of database count. The byte cache is shared. Aggregate resident index/validation allowances and journal bytes are now divided across the fixed layout; see the owner lifetime accounting below.

Members now carry a bounded logical ID separately from their socket address.
Rendezvous scores use only that ID; the existing ownership incarnation and route
continue to use the transport address and storage permission. The health probe
carries the ID in a header, requiring no additional request or worker. Older peers
without a header retain address identity. Duplicate observed IDs are excluded
without removing unrelated peers; direct invalid controller snapshots fail closed.
The Kubernetes base uses stable StatefulSet names and disposable caches without
PVCs. This changes scheduling only, not database mapping or the authority format.
A focused real-HTTP regression replaces one logical member three times, verifies
recovered values and identical retry receipts, fences its retained predecessors,
and checks that the other owner's incarnation stays unchanged. Address permutation
checks cover 1,024 partitions; duplicate identity, malformed ID and probe tests
cover discovery boundaries. The exact static executable completes B2 scaling, two consecutive StatefulSet
rollouts and cold recovery with all 833 acknowledgements recovered. Both rollout
timing targets still miss; mixed-version upgrades, broader failures and
production availability remain unqualified.

A watch channel supplies membership snapshots. A single task retires local backends when this process leaves membership or local health detects retirement, and maintains at most four acquisitions. These share the existing aggregate resident allowance; the per-request scratch and transport overhead still require whole-process memory qualification. A preference change alone keeps the incumbent serving until actual revocation; its resident reservation remains held. Each completion immediately admits the next eligible partition through a rotating cursor; one stalled operation cannot hold up the remaining slots. Failed acquisitions wait for the existing three-second retry tick or a membership update. Controller health checks perform no provider I/O or blocking owner-lock acquisition. Each native worker separately observes its permit at most once every three seconds, with one observation in flight and no owner lock held over I/O. Confirmed absence retires the owner; errors leave authority unchanged. This adds a background GET per cloud partition, never a conditional update. It is independent of the fresh permit validation required for each response. Local-directory guards need no object request. Membership changes cancel unconfirmed acquisitions whose preferred peer changes. After a confirmed claim, the incumbent finishes recovery and installs its backend while it remains a member, even if preference moves elsewhere. A subsequent takeover still fences its permit; explicit removal still cancels acquisition. Unchanged partitions keep their progress. Closing membership cancels recovery and retires local backends. Membership and readiness schedule work, never authorize responses.

Each fresh acquisition must confirm its exact claim and activation, select revoked predecessor journals and privately recover before entering the serving map. Unknown claim/activation outcomes validate the same incarnation; only a definite claim conflict permits a fresh candidate. Journal selection allows four retries: unchanged observed recovery snapshots let publication perform the exact validation while retaining completed scans; changed snapshots require explicit validation before restarting discovery. A successor immediately ends that attempt. Individual provider requests retain their timeouts; whole recovery has no restart timer. Resident reservations cover overlapping recovery, serving and still-referenced retired owners. HTTP checks the requested incarnation against the same backend retained through dispatch. Cross-database tenant scope is rejected before selection, and forwarding remains one hop without hidden request replay.

The scheduler regression pauses one of eight acquisitions and requires the other seven to become available, including an actual HTTP create on a queued partition. The membership regression retains a blocked recovery while another partition changes preference, preserves the healthy incumbent until takeover, then removes/restores the local node. Both fail against the previous scheduler. All twelve controller tests and 325 enabled Rust checks pass, along with formatting and strict clippy. The unchanged residency state space checks 19 states and both negative controls; layout checks 56 states and both controls. The former batch drain, whole-pass cancellation and eight-probe limit are removed. This changes scheduling only, not serving authority or the storage/wire format. The exact v0.23.5 executable passes the complete B2 campaign with 1,350 recovered writes; [phase durations and limitations](verification-results.md#v0235-independent-recovery-scheduling-2026-11-12) separate the improvements from slower initial setup.

`ClusterLayout` includes empty membership and logical identity across endpoint replacement: **112 distinct states, 793 generated, depth 7**. Three negative controls detect erased layout metadata, membership-dependent database mapping and address-dependent placement. The placement subcase abstracts a fixed logical ranking over two members; it does not verify SHA-256, discovery agreement or production liveness. `OwnerRouting` checks the pinned backend with its added rebind fault. The existing `RootRegistration`, `RevocableOwner` and `PublicationRebase` checks and controls pass unchanged. These establish separate bounded properties under their recorded assumptions; they are not a composed proof of the Rust controller or liveness under permanently inconsistent membership.

Six controller/TCP checks pass in 0.29 seconds and three layout lifecycle/race checks in 0.03 seconds. A separate fixed-vector test pins the persisted hash contract. They cover one-to-two-to-one placement, a still-live removed owner, preservation of the unmoved owner, total compute replacement, values and receipts, lost control replies, membership closure, invalid configuration before mutation, incompatible layouts, publication staged before layout installation and backend replacement during HTTP admission. Five deliberately broken Rust variants erase the layout, accept a mismatched layout, reselect after admission, restart at a failed first partition or clear readiness during a routine probe; all are detected. All 310 enabled Rust checks, strict clippy and formatting pass.

The manual partition cloud fixture is replaced by the membership-driven fixture, retaining provider operation accounting, checkpoints, receipts, shared collection and upload ceiling checks. Independent node transports/caches pass on B2 in 115.46 seconds and versioned MinIO in 1.67 seconds; the preceding B2 HTTP 500 failure remains recorded. All disposable buckets, keys and containers were removed. [Exact scope, failed attempt, source hashes and cleanup](evidence/cluster-placement-2026-11-11.json). The following sections add DNS discovery, CLI cutover and aggregate resident admission. Sustained provider/host failure qualification and full composition remain open. Stable membership, provider progress and sufficient upload credit are necessary for eventual availability; membership disagreement can cause safe but unavailable takeover churn.

## Resident-owner accounting

Cluster configuration supplies aggregate index/validation counts and payload allowances, and aggregate journal bytes. Fixed equal shares preserve those bounds when all partitions move to one process. Receipt windows, checkpoint thresholds and maximum queued frame counts remain per partition. Too-small shares fail before layout installation or acquisition. A process admits at most one layout-count of recovering, serving or retired owners combined; a reservation follows owner references and spawned I/O, including completed but unconsumed results. Removing a routing entry alone cannot refund it. Backpressure occurs before ownership I/O.

The 19-state `OwnerResidency` model checks two slots and three contenders (46 generated states, depth 5), with controls for unchecked acquisition and premature release. This is a bounded lifetime argument, not a complete memory or Rust proof. The existing index, validator and journal bounds remain component obligations. Allocator overhead, HTTP/provider buffers and checkpoint scratch require separate peak-RSS qualification; the cache retains its independent configured bound.

A regression retains both old backends through removal, refuses replacement without provider calls, then releases them individually and verifies progress resumes within the same fixed budget. Invalid aggregate shares are refused before mutation. Seven focused controller checks pass in 0.30 seconds; strict clippy passes, and a deliberately premature-release Rust variant is detected. The cloud CLI uses these limits; whole-process memory qualification remains separate.

## DNS membership

`api::Membership` supplies either a single-process snapshot or observations of a headless service, using the advertised socket's port and IP family. DNS candidates are canonicalized and bounded at 256. One resolver lookup runs at a time and retains the last candidates on failure. A separate HTTP monitor continues checking those candidates even while resolution stalls. New peers must return HTTP 200 from `/health/live`; an established peer survives one missed observation and is removed after a second. A successful observation readmits it. Readiness is deliberately not probed, so recovery and initial discovery cannot block each other.

Probe rounds run every three seconds without overlap, with at most sixteen concurrent one-second requests and no idle connection pool, proxy or redirects. Large candidate sets can take longer than the polling interval. A changed DNS snapshot cancels the superseded probes without counting cancellation as failure. Dropping the lifetime handle closes both workers. These are scheduling hints, not a death certificate, lease or serving capability; false suspicions can cause extra takeovers, and liveness still requires eventually stable, mutually reachable views. Every data response retains the durable permit check.

Five focused monitor checks pass in 0.03 seconds. They cover bounds/configuration, stalled DNS without overlapping resolutions, HTTP-live bootstrap while readiness refuses, one-miss hysteresis with unchanged candidates, cancellation and shutdown. A real TCP regression replaces an HTTP-stalled peer while its DNS address remains, retains the old backend and checks durable refusal, replays the original receipt, then rejoins the same address and checks the router's stale-incarnation refusal followed by an identical client retry. Bypassing the health filter makes this regression fail. All 328 enabled Rust checks, formatting and strict clippy pass. `ClusterLayout` retains 56 states and two negative controls; `OwnerRouting` retains 110 states and five controls. These checks do not prove the Rust implementation or permanent-partition liveness.

The cloud harness now pauses all six old database processes in a disposable namespace, keeps their DNS records, and adds a seventh process. Every writer must progress while the old process IDs/start ticks remain stopped and the pod restart counts remain unchanged. It then resumes the old processes and continues the existing fault campaign. Only the fixture adds an unprivileged same-UID control sidecar and disables liveness restart; production manifests are unchanged. The process-control helper passes a local stop/refusal/resume check.

Raw partition control observations do not issue the common data-read barrier:
they supply stale hints, while exact comparisons and permit validation establish
authority. The same bounded immutable-manifest reader serves both paths. The
regression requires zero authority writes for observation, then rejects a
revoked predecessor's comparison and serving permission.

Cancelled forwarding retires only its own cached route instance, without replay
or invalidating a replacement. Background permit observation retires an idle
revoked owner so membership rejoin can recover even when requests enter through
another process. TCP regressions cover cancellation and dead-peer recovery; the split-view
regression separately checks placement and retained-owner fencing.

The current batched journal-selection path completes two B2 peer-isolation
campaigns with all 553 / 709 acknowledgements recovered and all six recovery
targets met. Only one writer progresses during each two-way hold: peer-local
failure views can still schedule competing claims. Broader recovery targets,
continuous partition availability and physical-host loss remain open. The
[verification results](verification-results.md) retain successful and failed
campaigns, exact images, configurations and timing limits. The headless service
continues publishing not-ready endpoints for bootstrap; see the
[Kubernetes DNS contract](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/).

## Native cloud CLI

The B2/S3 CLI now constructs the same native partition controller as the library. A fixed layout defaults to 16 partitions; advertised pod IPs and headless-service DNS supply placement. Initial namespace byte/attempt ceilings are mandatory. The resident allowance defaults to 128 MiB of component payload/count budgets across the entire layout, separate from the cache and whole-process RSS. Invalid combinations fail before provider access.

Partition metadata initialization runs within background acquisition, after reserving resident capacity. Opening HTTP does not require all partitions or immutable upload credit. A focused TCP regression starts a fresh zero-credit namespace, reads and replenishes its shared ceiling, verifies exact refill replay, then creates and writes a database. The controller's eight focused checks pass in 0.31 seconds. Restoring eager partition initialization makes the zero-credit regression fail before HTTP opens. The CLI cloud fixture additionally exercises real child processes, abrupt termination, exhausted-credit recovery, incompatible-layout refusal, cold values and receipt recovery. Provider results are recorded separately below.

At `0afabc0`, the actual debug executable passes the disposable B2 fixture in 64.72 seconds and versioned MinIO in 1.49 seconds (measured inside the workload, excluding process teardown). Each provider starts from zero credit, refills through HTTP, survives two abrupt process replacements, preserves values/exact receipts and refuses an incompatible partition count. The initial attempt exposed invalid default resident scaling before provider access; capping component limits fixes it. Both attempts removed all four buckets, two B2 keys, two containers and 309 remaining physical versions. [Source, configuration, failures and cleanup](evidence/native-cloud-cli-2026-11-11.json). This single-process fixture does not qualify DNS placement, AWS, RSS or an SLO.

The first native Kubernetes campaign failed its scale-to-three readiness deadline and exhausted all four writers' identical-request retries. The diagnostic without active writers and with maintenance delayed reached three ready replicas but still showed repeated whole-recovery deadline cancellations. Both disposable namespaces, buckets and keys were removed. Two corrections follow: overload before destination identity validation now invalidates that route without replay, and reconciliation lets a progressing recovery finish while canceling on changed membership. The TCP routing regression fails on the original cache rule; five routing checks pass in 0.10 seconds. A paused-time acquisition regression keeps one partition paused while another becomes available, spans 50 additional seconds and unchanged membership, then checks cancellation and new placement. Restoring the 30-second cutoff makes it fail. These local fixes require a fresh full campaign; the failed evidence is retained.

Shared directory maintenance is scheduled once per interval on the peer preferred for partition zero, independently of readiness; each native owner collects only its own partition. The two-node paused-time regression checks one sweep, reassignment and empty membership. `PublicationSafety` still checks concurrent collectors (175,546 distinct states, four negative controls); membership is a cost hint, never a cleanup lease. Default sixteen-partition B2 startup also has an explicit disposable CLI fixture: the initial candidate exceeded its 240-second deadline, while versioned MinIO completed in 2.026 seconds. After shared cleanup scheduling was corrected, the same default-layout fixture passed B2 in 126.628 seconds and MinIO in 2.024 seconds. The revised recovery-timeout negative control still fails its focused regression; all 319 enabled Rust checks, fmt and clippy pass. Native Kubernetes scaling remains an open qualification gate.

The native worker now retires after an uncertain journal PUT resolves absent,
allowing cluster reacquisition instead of indefinite polling. A two-partition
HTTP regression returns 503, reacquires, commits an identical retry once, then
injects the old delayed PUT and reopens again with the same receipt and value.
Restoring the old polling loop fails within two seconds. The existing
`RevocableOwner` model checks 856 states and nine controls, including delayed
reply and late-upload traces; absence still never proves a write aborted. CLI
qualification bounds a whole identical-retry sequence to 120 seconds so a
stalled request cannot consume seven minutes of diagnostic iteration.

## Recovery under collection and membership changes

Journal-selection uncertainty previously discarded the owner and added a new
incarnation to recover. Selection now uses the existing exact-validation protocol
to confirm the same owner, with four additional validation attempts across the
whole selection. It never repeats an uncertain immutable PUT. Confirmed cuts are
reloaded before discovery continues; a different owner is refused. A lost-reply
regression checks both late journal bytes and takeover during validation. Nine
catalog checks originally passed in 0.70 seconds. That historical check used
393,513 states with two epochs and 102,525 with three epochs, plus seven
negative/coverage controls. The current scan-reuse behavior and bounds are in
[Durable prefix catalog](#durable-prefix-catalog). Neither bounded check proves
controller liveness or implementation correctness.

The controller now compares local placement before canceling an acquisition pass.
Peer arrivals that leave its assignments unchanged preserve pending recovery.
Actual assignment changes, invalid membership and channel closure still cancel.
The paused-time regression adds an unrelated peer, observes that the paused I/O
survives, then removes the local process and requires cancellation/reacquisition.
The original whole-membership cancellation and immediate journal-selection
abandonment each fail their regression. This removes two restart paths without
adding a new owner protocol, retrying uncertain writes or relaxing cloud deadlines.

An idle recovered owner now schedules one checkpoint through the existing
maintenance worker, even if its selected journals contain no frames. The durable
cut advances to that owner, so later healthy handoffs stop at that checkpoint
instead of rescanning every empty ancestor. Ordinary foreground reads do not
require this opportunistic checkpoint; staged publication and capacity-required
maintenance retain their existing admission rules. Publication still needs the
normal exact comparison and funding. Repeated crashes, unavailable storage or
exhausted credit can still postpone the cut; this is not an unconditional recovery
bound. A seven-handoff regression checks zero-frame cuts, a lost publication reply,
a late old upload and no repeated checkpoint when idle. The `RootRegistration`
checks retain their completed-owner cutoff rule (34,061 and 281,563 states, six
negative/coverage controls); zero-frame data recovery is checked in Rust.

The first recovery-fix campaign (`b51c42a`, before idle checkpoints) passed one,
three and six replicas, a 120-second requested steady phase and forced pod loss.
It still exceeded the 240-second rollout deadline: 1,067 acknowledged writes,
1,064 completed readbacks and no client failure in the captured snapshot. This
was a temporary debug image and did not reach final recovered-history verification,
provider interruption, scale-down or cold replacement. The namespace, bucket,
scoped key, 875 physical versions and temporary image manifest were removed.
[Configuration and exact scope](evidence/native-recovery-2026-11-11.json).

With idle checkpoints (`664ee9b`), a second fresh B2/Kubernetes campaign again
passed one/three/six readiness, the steady phase and forced pod loss, but failed
rolling replacement's 240-second deadline and one client's 180-second identical
retry deadline. Its final snapshot has 1,107 acknowledgements and readbacks; no
final recovered-history verification was reached. Scale-up took longer, so this
is not evidence of a cloud speedup. Both attempts retain the same configuration
and deadline. The second namespace, bucket, scoped key, 482 physical versions and
temporary image manifest were removed. All 322 enabled Rust checks, formatting
and strict clippy pass locally. The follow-up below resolves uncertain claim/activation outcomes in place;
measuring whole handoff latency, the full rollout and later phases remain open.


## Same-owner acquisition recovery

The native controller now validates an uncertain claim or activation on its
original identity. An exact successful comparison can confirm an open owner;
a confirmed fence requires fresh activation against that comparison. A raw
observation grants neither. Four extra validation attempts are shared across
claim and activation; a successor or exhausted budget stops acquisition. This
reuses journal selection's validation loop and never repeats claim registration
or permit creation. A definite claim conflict still allows a fresh candidate.

Rust fault tests cover lost responses before/after activation applies, claim
response loss, successor takeover during observation and persistent failure.
The existing real HTTP controller regression also requires epoch one after the
lost claim response: serving cannot silently switch to a new owner. The updated
acquisition model and the inherited registration checks retain every negative
control. These are component safety/recovery checks, not a cloud progress result.

Acquisition logs include fixed stage names and elapsed milliseconds from each
attempt; these diagnostics never grant ownership. The existing Kubernetes
harness captures bounded, allowlisted samples at phase boundaries and on failure,
so rolling deletion does not erase every earlier successful handoff measurement.

The new B2/Kubernetes campaign (`a275944`, debug image) still misses the
240-second rolling deadline and two client retry deadlines after 1,044 observed
acknowledgements/readbacks. Successful sampled acquisitions span 12–86 seconds.
The run does not reach final history verification or later fault phases; its
namespace, bucket, key and 611 physical versions were removed. See the
[configuration and bounded timing evidence](evidence/native-recovery-2026-11-11.json).


## Checkpoint origin recovery

Checkpoint publication now reads a fresh comparison after funding and staging,
checks the complete recovery origin, and validates that comparison with the
checkpoint CAS itself. The uncontended funded regression uses two replacements
(funding and publication), down from four (including a stale attempt and separate
validation). Observations grant no permission or confirmation. A conflict, lost
reply or cancelled publication still uses exact validation before reusing bytes;
a changed collection fence discards staging. A cancelled prior control validation
is abandoned under the same request gate before preparing the checkpoint.

The regression checks durable funding and the original receipt. Existing live
cancellation and collected-staging checks exercise the new path; an additional
check covers stale observations, retained metadata changed after staging, an
applied-but-lost checkpoint reply and a successor observed by an old owner.
`Checkpoint` includes one unrelated register update before or after preparation;
its nine unsafe controls still fail. The origin collection boundary remains in
`GlobalCollection` and the Rust races, not an expanded combined model. These
checks do not establish deployment latency, liveness or Rust refinement.

A lost or cancelled control refresh clears local confirmation; a live permit can
still authorize a sealed response. Checkpoint reconstruction now retries exact
metadata validation under the serialized control lock when its origin is unknown.
It then reads the confirmed origin and uses the existing conditional publication.
This follows RootAcquisition's existing `Start(validate)` transition without a
prior confirmation; no new serving grant or ownership mechanism is introduced.
The existing Checkpoint model abstracts successful refresh as stuttering.

The lost-refresh regression and real HTTP funding-then-refresh failure both fail
on the old implementation. The HTTP check requires idle maintenance to resume,
a subsequent write to succeed and the previous receipt to replay. The bounded
models retain their state counts and negative controls. Timing, complete liveness
and correspondence to the cloud stall remain separate qualification obligations.

## Control admission under data saturation

Authenticated administrator operational requests and metrics capabilities share two reserved HTTP slots
in addition to the configured tenant pool. Principal/rate limits, body limits,
timeouts and cancellation guards still apply. Pool selection follows validated
capability type and matched operational routes; tenants cannot select the reserve.
Administrator data requests keep the data pool and a separate principal allowance.
The extended HTTP fixture holds all eight data slots across four tenants, checks
budget/monitoring access, fills both control slots and checks cancellation. It
fails with 429 against the old shared gate and passes in 0.01 seconds afterward.
This is bounded local overload protection checked in Rust; it neither changes
the durable protocol nor proves end-to-end administrative availability during
provider contention. The existing RequestBudget model still covers refill and
eviction, not HTTP scheduling.

## Bounded remote observations

The failed B2 runs are retained in the recovery evidence. Local shared-partition
coverage drives two tenants through 400 HTTP commits with concurrent collection;
it does not reproduce the cloud stall. Checkpoint completion and deferral events identify only a fixed stage, owner
incarnation and whole-call elapsed time. They exclude no-op polls and warm-cache
discard. The cluster harness retains bounded events and metrics before cleanup;
a reported final stage does not measure that stage alone. Neither diagnostic changes serving authority or retries
an uncertain mutation.

B2 and S3 share a ten-second deadline for each object GET, bounded range and
listing, including retries and streamed bytes. Read retries have at most three
physical attempts; conditional writes and immutable uploads remain single-attempt. A timeout is an
error, never a missing permit or a successful comparison. These are scheduling
bounds checked when the process runs, not leases or real-time guarantees during
process suspension. Slow transfers can fail and require an explicit retry.

A real TCP fixture delivers headers but stalls all three response bodies. It
first fails on the old reader and then requires each caller to receive an error
when virtual time crosses the deadline. A second fixture checks bounded retries
for transient failures, one attempt for permanent refusals, and no redirects.
The existing lost-upload and redirect regressions continue to protect the
mutation boundary. Ownership, uncertainty and revocation rules are unchanged;
subsequent exact-executable B2/Kubernetes campaigns pass, including provider
interruption and peer-only isolation; recovery targets remain unmet in several
phases. See the [current results](verification-results.md).

## HTTP readiness during partition recovery

An observed scale-up marked every pod unready together because readiness required
all local placements to finish recovery. That removed the client Service's
endpoints, including administrative refill and routes to healthy partitions.
The controller now advertises routing readiness while it belongs to valid
membership, and withdraws it on removal or shutdown. Recovery and capacity
failures stay local to affected partitions; they do not hide the HTTP router.
No owner is admitted before recovery and every data response retains its fresh
permit check. The all-partitions readiness scan and recovery-driven flag resets
were deleted.

Native worker health now uses only local retirement state and a nonblocking
owner-lock check. A failed provider observation no longer replaces an idle owner,
and a busy checkpoint cannot hold up another partition's health check. This
does not grant response authority: every data result retains fresh permit
validation, and an observed revocation or uncertain journal retirement still
requires recovery. The provider-outage regression first failed against the old
probe, then checks unchanged owner identity and zero additional authority writes
while data requests fail closed. Existing HTTP checks cover a paused exclusive
admission and refusal after revocation. The earlier network-health probe test
was replaced; no parallel health mechanism or new model was added.

The existing TCP regression observes HTTP 200 from the probe while a claim is
paused, HTTP 503 for the unconfirmed database, then service on the original
confirmed identity. The zero-credit regression requires the ready administrator
endpoint before refill. Membership closure, paused recovery and resident-budget
checks remain. `OwnerRouting` includes independent readiness changes and a
negative control granting a response from readiness alone. The disposable CLI
startup fixture now requires authenticated absent-database responses from all
sixteen partitions, so the broader probe cannot make that check pass early.
The cloud observations below distinguish rollout completion from writer recovery.

Both updated CLI fixtures pass on versioned MinIO at `5a69e41`: all sixteen
partitions answer in 1.46 seconds, and zero-credit administrator refill plus
cold values/receipt recovery takes 5.11 seconds. The two buckets, 503 physical
versions and temporary container were removed. This is not AWS or Kubernetes
qualification. The attempted three-slot rollout experiment on the preceding
binary failed at pod-loss recovery before it exercised that setting; its
temporary configuration option was removed. The maintained harness retains the
original one-slot rollout and 240-second deadline.


At `cbf4a70`, the B2/Kubernetes debug campaign completes rolling replacement and
aggregate write progress in about 85 seconds with the original one-slot policy
and 240-second deadline. It later fails one client retry deadline during scale-down
after provider interruption. Its partial snapshot has 915 acknowledgements and
912 readbacks; it never reaches final recovered-history verification. All task
resources and 1,211 physical versions were removed. This is not full recovery or
a production latency/availability claim.

The old harness could advance a phase while one writer remained stalled. It now
requires two further verified readbacks from each of four writers before advancing,
including after the steady pilot. A temporary regression executes the actual
progress function against a ten-write single-writer advance: the previous function
accepts it, while the corrected function waits for the other writers' readbacks.
Timeout diagnostics identify the operation and last HTTP/refusal category without
recording arbitrary response text; both classification and redaction were checked.
Earlier campaigns retain their aggregate-only scope. The subsequent campaign
uses the verified v0.23.3 static release binary and the stronger barrier at
`5a143b5`. It completes 1→3→6→1, forced pod loss, rolling replacement, provider
interruption and total compute replacement. All **1,791** acknowledgements,
readbacks and recovered keys agree; the separate sequential-history checker
validates unique request identities, invocation order, contiguous revisions and
value hashes. Per-writer totals are 371, 452, 501 and 467.

The maintained workload also records request-attempt time, actual retry-backoff
time and HTTP status counts separately for each commit and its readback. These
bounded summaries use the workload's monotonic clock; they include no tokens,
keys, values or arbitrary server errors, and do not isolate server-side causes.
The existing TCP fixture injects a 503 followed by success and holds a later
read response, checking both timing categories and unchanged verified-readback
semantics. The two harness checks pass in 2.28 seconds. Earlier histories without
these optional diagnostic fields remain valid; the fields do not change the
history checker's acceptance or availability calculations.

Rolling recovery takes approximately 165 seconds, scale-down 177 seconds and
cold replacement 127 seconds between recorded phase boundaries, including
orchestration and required readbacks. These are not request latency SLOs or a
controlled comparison with debug builds. The original 180-second client retry
and 240-second rollout/progress bounds remain. All task resources, credentials,
the temporary image and 1,073 physical versions were removed. This is one short
B2 run, before the local-health patch; AWS, HPA, host loss, peak memory and
sustained-load qualification remain open. See the
[completed campaign evidence](evidence/native-recovery-2026-11-11.json).
