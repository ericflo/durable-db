# Verification

The design starts in `spec/Cluster.tla`. Do not equate model checking with a proof that the Rust binary is correct. The model abstracts implementation details; TLC explores finite configured instances. An [unbounded TLAPS proof](proof-publication.md) now establishes abstract data-page reachability during publication and collection. Complete protocol composition, liveness under stated fairness assumptions, and a checked refinement from implementation to model remain separate obligations.

## What the model represents

Two interchangeable nodes, two fresh write attempts, immutable objects, a manifest with retained page references, conditional publication, durability receipts, collection candidate snapshots and fences, arbitrary node restarts/scale-downs, scale-ups, and storage partitions. A node can disappear at any protocol boundary. Collectors and writers can interleave. Object names cannot be reused. The epoch bound is three; no symmetry reduction hides different node roles.

Checked invariants: referenced objects exist, acknowledgements refer to durable history, an attempt is not committed twice, the current manifest is the latest committed state, and basic state types hold. The abstract durable history is a specification witness, not a claim that the database retains all historical values.

`UnsafeGC.cfg` disables the collection fence. TLC must find a reference to a deleted object; the verification script treats failure to find that counterexample as a failed test. This checks that the model is sensitive to a real protocol bug.

## Reproduce

```sh
scripts/check-model.sh
```

The script pins TLC 1.7.4 by SHA-256 (`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`). It checks the hash on every run and records detailed output under `.tools/model-safe.log` and `.tools/model-unsafe.log`. CI runs the same command. Finite-state results and test summaries are recorded in [verification results](verification-results.md).

## Mapping to Rust

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
- Whole-host/control-plane faults, independent rack failure domains, long-duration soak, and AWS qualification. CPU-triggered HPA, drain, SIGKILL, injected kernel OOM, provider partition, and all-pod loss pass on a three-node kind cluster with real B2 and MinIO. A separate three-physical-host B2 campaign passes manual scaling, pod replacement, egress restriction, and cold recovery. Those hosts share one rack; host shutdown and rack loss have not been injected.
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

`PublicationCombining.tla` was checked before implementing durable proposals. It models two fresh jobs, two database targets, two helpers, and three authority revision advances. Jobs can use genuine stale observations, upload, submit, disappear with an unknown outcome, receive success, or be definitely aborted before submission. Helpers select independent targets and race a conditional group publication. Original unbatched publishers, read barriers, collection candidate snapshots, fence publication, manifest/proposal deletion, and abandonment are included. A committed proposal may be cleaned up without advancing the fence; already loaded proposals remain subject to the original target comparison.

All 13,049,720 symmetry-reduced states (84,918,974 generated, depth 29) satisfy the seven checked invariants: types, referenced-manifest existence, original target comparison, unrelated-write preservation, no duplicate publication, acknowledged history, and no commit after a definite abort. Symmetry renames only jobs among jobs and helpers among helpers. A separate run without symmetry also passed: 42,716,288 states, 279,275,713 generated, depth 29. Reproduce that larger run with `PublicationCombiningNoSymmetry.cfg`. TLC uses probabilistic state fingerprints; these runs are finite model-checking evidence, not a mechanized proof. Four intentionally unsafe configurations omit the collection fence, expected target, current-root merge, or post-submission uncertainty rule.

| Model boundary | Rust boundary |
| --- | --- |
| Upload / Submit | `publication::proposal::Published::publish` uploads a fresh manifest, then a bounded immutable proposal |
| Select | Bounded listing, strict proposal namespace/schema checks, target/fence comparison, and manifest existence/digest checks |
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

`UploadCredits.tla` checks two processes, two grants, three requests, an initial ceiling of two units raised up to four, and up to two upload attempts per charged request. The current model explores **11,002 states (41,197 generated, depth 16)** with crashes, all-process loss, missing acknowledgements, additional grants preserving existing rights, and delayed arrivals. Its stronger coverage invariant includes both charged units and all remaining usable rights. Four negative controls detect refunding dead-process credit, restoring spent credit, uploading before charging, and resetting allocation during replenishment. The Rust allocator applies the same boundary to bytes and attempts, replenishing only deficient dimensions. Built-in transports use one immutable PUT attempt per invocation. Tests cover concurrent replicas, zero-byte uploads, lost allocation/refill acknowledgements, retained credit after a lost additional grant, late arrivals, receipt eviction, administrator HTTP isolation, and proposal recovery after exhaustion. The finite model does not prove billing bounds, HTTP refinement, tenant isolation, migration, or composition with publication/collection. See [upload admission](design-upload-admission.md).

## Data collection without immutable uploads

`GlobalCollection.tla` models original engine observations, fresh uploaded pages, retained references from previous manifests, durable proposals, helper publication, process disappearance/restart, and list/read/conditional-fence/delete sweeps. One configuration uses two processes, two jobs, two databases, one collector, and three revision advances: **1,620,524 distinct states (6,374,776 generated, depth 27)**. A second uses two concurrent collectors, two jobs/processes, one database, and two advances: **6,442,262 states (32,586,881 generated, depth 28)**. Three negative controls detect omitting the engine's original fence, omitting the collector fence, and listing after fencing. Delayed uploads and deletion are included; finite exploration and fingerprinting still carry their usual limits.

Format-5 `Published::get` carries the original collection fence in the engine's version token. `publish` compares it before proposal submission; helpers retain their existing proposal-fence comparison. `collect_data` lists at most 128 candidates, observes and conditionally fences the directory, validates protected manifests, and deletes only absent listed references. It never invokes immutable PUT. Missing indexes/manifests and invalid content fail closed. Tests pause an uploaded writer across collection, invalidate an exhausted proposal, race two writers with two collectors, retain earlier live pages, restart between cursor pages, and preserve data after uncertain fences. A temporary Rust negative control removing the origin comparison fails its regression. Format 4 retains its earlier behavior and is never silently upgraded. The new observation format requires fresh initialization; provider qualification, unbounded proof, model composition, and implementation refinement are distinct obligations. See [the collector design](design-budgetless-collection.md).

## Logical export and restore

`LogicalBackup.tla` models ordered scans at an observed revision, interrupted/incomplete export, complete archive publication, protected destination ownership, prefix verification, delayed restore chunks, missing responses, and restart. Its two-key/two-value configuration with three delayed requests and two source advances explores **6,888 distinct states, 40,064 generated, depth 16**. Four required negative controls detect mixed revisions, truncation, foreign destination takeover, and missing restore revision checks.

`backup::export` checks every scan revision and final source identity before completing a checksum footer and publishing its private file. `backup::archive::Verified` privately spools and validates all records before any restore request. `backup::restore` recovers a creation identity derived from archive digest and target name, verifies previously committed chunks at one target revision, and continues deterministic expected-revision writes before verifying the entire result. Tests include actual HTTP interruptions, committed service errors, receipt eviction, archive replacement after validation, quota/encoding bounds, source mutation, existing destinations, and CLI export/inspect/restore.

The model assumes atomic ordinary database operations, collision-free identity/integrity checks, and an isolated destination. It abstracts the HTTP, cryptographic, and filesystem layers and does not establish retained snapshots, PITR, scheduling, or a composed unbounded recovery proof. The [operator procedure](recovery.md) describes the partial-destination and concurrent-source-write boundaries.

## Unbounded page reachability

`PublicationSafetyProof.tla` proves the abstract page-reachability invariant for arbitrary jobs, collectors, page identities, and execution length: 35 strict TLAPS obligations pass. A temporary missing-fence mutation fails the proof. The same core model passes a 2,346,968-state TLC regression, and both missing-origin and missing-collector fences produce live-object-loss traces. The [proof scope and reproducible procedure](proof-publication.md) enumerate provider/freshness assumptions, trusted tools, and the outstanding composition/refinement obligations.

## Pinned-owner Harness takeover

`OwnershipClaim.tla` models a claimant that pins the first observed owner identity, retries definite revision conflicts, and checks every fresh snapshot before publishing. Same-owner progress, release, a competing new owner, unknown acknowledgements, and backoff interleavings are explicit. A takeover must never silently retarget a competing owner. The matching JavaScript code is `openIndexedPiStorage` in `clients/javascript/pi-indexed.mjs`; `indexed.test.mjs` injects both same-owner conflicts and a competitor appearing after conflict resolution.

The configured instance explores **735 distinct states**, 2,001 generated, at depth 11, with two other owners and five revision advances. Both unsafe configurations must violate `PinnedTarget`: permitting retargeting, or omitting revision comparison. A separate implementation mutation removes the post-backoff check and must fail the competing-owner regression. This finite model neither proves liveness nor composes all database and Harness behavior. Reproduce with `scripts/check-model.sh OwnershipClaim`; the [Harness evidence](evidence/pi-harness-2026-11-09.json) records source hashes and cloud outcomes.


## JSON transaction retry boundary

`TransactionClient.tla` precedes the JSON SDK implementation. It models one logical operation, successful publication with a lost response, exact receipt replay, competing revision advances and bounded receipt eviction. Once a commit outcome becomes uncertain, a later conflict cannot authorize recomputing the operation. The client must return an unresolved result instead. This prevents a late retry from turning one logical invocation into two database effects.

The configured instance permits competing advances below revision three and explores **41 distinct states**, 48 generated, at depth eight. The unsafe control recomputes after uncertainty and violates `AtMostOneEffect`. Run `scripts/check-model.sh TransactionClient`. The model abstracts callback execution, HTTP classifications, request identity, physical publication and server receipt guarantees; it is not a full SDK refinement or liveness proof. Real HTTP tests cover identical retry bytes after a lost acknowledgement, recomputation after a definite conflict, and refusal to recompute after uncertainty followed by receipt loss.


## Candidate log handoff

`spec/candidates/WalHandoff.tla` is research for the replacement serving/storage path. Its three-position, single-handoff instance explores 1,214 distinct states (3,720 generated, depth 21). The successor reads positions individually, may crash/restart, and durably seals a contiguous prefix after old acknowledgement permission ends. Late uploads cannot reopen that prefix. Five controls fail the exact acknowledgement-preservation or seal-immutability invariant stated in `WalHandoff.checks`.

The model assumes fresh negative point reads, immutable batch identities, atomic durable sealing and an external boundary that ends old acknowledgement permission. It does not establish that boundary or compose the clock/serving model, general membership, request deduplication, transactions, retention, collection or admission. This candidate has its own cached Woodpecker check; the implemented engine remains unchanged. See the [design and provider requirements](design-owned-partitions.md) and [source/log evidence](evidence/wal-handoff-candidate-2026-11-09.json).

## Candidate serving permission and log acknowledgement

`spec/candidates/OwnedWal.tla` combines finite local serving grants with immutable log uploads, contiguous acknowledgement, takeover drain and prefix recovery. Old-writer acknowledgement checks its grant after upload; it has no atomic global-owner check. Old reads check the grant against a history that distinguishes old and new batches. The successor seals a prefix before activating and can acknowledge one new durable batch. The instance has two old log positions, two real-time transitions, one-unit grants and independent clock rates of one or two units per transition. It checks **570,280 distinct states**, 2,709,529 generated, depth 28, with an optimistic fingerprint collision estimate of 6.6e-8.

Old-process pause/resume and loss, delayed grants/uploads, simultaneous absence, and successor crashes/restarts before activation are included. A successor restart can change the clock origin and restarts any unfinished drain. Seven controls detect skipped drain, stopped paused clocks, expiry at delayed response delivery, reusing a former process's timestamp, checking permission only before upload, acknowledgement gaps, and stale negative reads. The standard checker passes every exact expected invariant failure in `OwnedWal.checks`; Woodpecker gives this research model its own cached stage.

This is one bounded handoff, with the successor remaining the final owner after activation. It assumes bounded-rate continuously advancing elapsed clocks, fresh negative object reads, immutable batch identities and atomic durable fence/drain/seal records. It does not prove platform timing, arbitrary ownership epochs, a scalable delegation hierarchy, KV/transaction semantics, request deduplication, compaction, retention, collection, admission or liveness. Runtime assumptions remain unchanged. [Evidence](evidence/owned-wal-candidate-2026-11-09.json) records source/configuration/log hashes and the complete bounds; the [candidate design](design-owned-partitions.md) records the remaining implementation gate.
