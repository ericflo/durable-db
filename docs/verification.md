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


## Journal prefix safety

`Journal.tla` precedes the immutable journal implementation in `src/journal/`. `Prepare` allocates fresh positions, `Upload` makes their immutable bytes durable, `Observe` establishes positive durability knowledge, and `Consume` advances only through consecutive known positions. These correspond to `Writer::prepare`, the single-use `PreparedUpload::send`, `Writer::complete`/`resolve`, and `Writer::pop_durable`. Missing or failed responses never establish absence. Recovery scans one position at a time and fixes a prefix before successor activation; late uploads cannot extend that decision. `Reader` implements discovery and replay, but conditional selection and serving permission remain caller obligations.

The TLC-only constraint permits three prepared positions. All **4,134 distinct states** (18,455 generated, depth 26) pass, with an optimistic fingerprint collision estimate of 3.2e-12. Six mandatory controls expose false durability knowledge, consumption gaps, early recovery, stale negative reads, late acknowledgement and reopened selection. Run `scripts/check-model.sh Journal`.

`JournalProof.tla` removes the exploration bound: all **88 strict TLAPS obligations** prove the inductive invariant and its consequences for arbitrary log lengths and execution lengths within one handoff. The theorem establishes consumed-prefix durability, preservation of acknowledged work in the selected prefix, and selection immutability. Removing the contiguous-consumption rule must fail the corresponding induction step. Run `scripts/check-proof.sh Journal`; it uses the same pinned tools and fresh proof caches as the [publication proof](proof-publication.md). Woodpecker requires both checks in independent cache stages.

The proof assumes immutable unique identities, durable objects and fresh per-key observations, an external boundary ending old acknowledgement permission, and atomic durable selection. It does not establish that boundary, arbitrary ownership epochs, delegation, clock behavior, transactions, request deduplication, resource admission, collection, liveness or Rust refinement. The actual serving engine is unchanged. The older handoff-only model is retired; its [historical evidence](evidence/wal-handoff-candidate-2026-11-09.json) retains its source commit.

Thirteen journal tests cover out-of-order completion, unknown outcomes, cancellation and retained-frame budgets, bounded parsing, corruption, exact replay and local reopen; a separate collector regression establishes namespace isolation. Real B2 and versioned MinIO checks each recover 512 records after reconnect, resolve three lost responses without repeating PUT, and exclude a late suffix after persisted selection. These use the existing conditional publication layer as a fixture, not a replacement ownership controller. Source hashes, bounds, provider results and cleanup are in [the component evidence](evidence/journal-2026-11-09.json).

## Candidate serving permission and log acknowledgement

`spec/candidates/OwnedWal.tla` combines finite local serving grants with immutable log uploads, contiguous acknowledgement, takeover drain and prefix recovery. Old-writer acknowledgement checks its grant after upload; it has no atomic global-owner check. Old reads check the grant against a history that distinguishes old and new batches. The successor seals a prefix before activating and can acknowledge one new durable batch. The instance has two old log positions, two real-time transitions, one-unit grants and independent clock rates of one or two units per transition. It checks **570,280 distinct states**, 2,709,529 generated, depth 28, with an optimistic fingerprint collision estimate of 6.6e-8.

Old-process pause/resume and loss, delayed grants/uploads, simultaneous absence, and successor crashes/restarts before activation are included. A successor restart can change the clock origin and restarts any unfinished drain. Seven controls detect skipped drain, stopped paused clocks, expiry at delayed response delivery, reusing a former process's timestamp, checking permission only before upload, acknowledgement gaps, and stale negative reads. The standard checker passes every exact expected invariant failure in `OwnedWal.checks`; Woodpecker gives this research model its own cached stage.

This is one bounded handoff, with the successor remaining the final owner after activation. It assumes bounded-rate continuously advancing elapsed clocks, fresh negative object reads, immutable batch identities and atomic durable fence/drain/seal records. It does not prove platform timing, arbitrary ownership epochs, a scalable delegation hierarchy, KV/transaction semantics, request deduplication, compaction, retention, collection, admission or liveness. Runtime assumptions remain unchanged. [Evidence](evidence/owned-wal-candidate-2026-11-09.json) records source/configuration/log hashes and the complete bounds; the [candidate design](design-owned-partitions.md) records the remaining implementation gate.

## Candidate permission delegation

`spec/candidates/DelegatedGrant.tla` checks that a child grant cannot outlive its coordinator's already acquired root permission. With two children, one grant request per child, a two-unit root interval, two time transitions and independent clock increments of one or two units, it explores **1,258,368 states**, 6,754,337 generated, depth 15. The child duration is at most half the parent's remaining local interval and starts at the child's request timestamp. Delayed replies, pauses, permanent crashes and a successor's fence/drain are included.

Five controls detect unscaled delegation, expiry starting at delayed delivery, stopped paused root/child clocks and skipped drain. The optimistic fingerprint collision estimate is 3.7e-7. The standard checker and all controls pass in 17.551 seconds locally; CI gives the research candidate a separate cached stage. [Evidence](evidence/delegated-grant-candidate-2026-11-09.json) records exact source/configuration/log hashes.

This is a component model, not proof of the whole hierarchy or its composition with `OwnedWal`. Root acquisition/renewal, repeated child renewal, reassignment, restart, message/resource bounds, log recovery and liveness are excluded. It assumes atomic grant/fence operations, valid parent permission, fixed assignments and bounded-rate clocks that advance during pauses. Platform rate/error bounds and a runtime timing contract are not established. The [design](design-owned-partitions.md#renewal-without-a-provider-call-per-partition) explains the capacity motivation and remaining gates.


## Candidate root renewal and repeated takeover

`spec/candidates/RootControl.tla` checks exact conditional root renewal, delayed replies anchored to request start, draining from fence observation, conditional activation and child delegation. Renewals may recover an expired grant only while the conditional register still names that epoch. Losing an acquisition, renewal or activation response cannot create local permission. Processes may stop taking steps, resume after arbitrary modeled delays, or be replaced by a fresh incarnation. Incarnations are never reused.

The three-epoch instance explores **952,597 distinct states**, 7,115,167 generated, depth 37: three time advances, a one-unit grant bound, at most nine register revisions, one renewal request per epoch and no child requests. The second instance explores **3,417,821 states**, 13,990,263 generated, depth 39: two epochs, three time advances, a one-unit bound, seven register revisions, two renewal requests and two child requests per epoch. Their optimistic fingerprint collision estimates are 3.2e-7 and 2.0e-6. These are separate instances; they do not establish all combinations of three epochs and repeated delegation.

Five unsafe variants must detect overlapping roots or a child outliving its parent: late root expiry, renewal without the exact current-owner comparison, skipped drain, excessive child duration and child expiry starting at delivery. Two additional forbidden-state controls must find reachable traces: all three epochs activate in turn, and both a serving root and its child actually extend their deadlines. Request counts alone are insufficient coverage. Run `scripts/check-candidate.sh RootControl`; Woodpecker caches this component independently. [Evidence](evidence/root-control-candidate-2026-11-09.json) records exact inputs and checker logs.

`RootControlProof.tla` now proves the permission invariants for **arbitrary finite epoch counts, execution lengths and renewal counts** under the same abstract timing contract. All **153 strict TLAPS obligations** pass. The inductive strengthening bounds older grants and successful outstanding replies at every later fence, tracks request/drain ordering, and contains child permission within its parent. The two finite regressions also check this strengthening. Removing the current-owner condition from renewal fails the proof. Run `scripts/check-proof.sh RootControl`; its independent Woodpecker stage requires both results. This theorem establishes root exclusion, child containment and child exclusion; it does not establish data-history preservation or the timing contract itself.

Time here is an abstract upper-bound contract, **not a runtime shared clock or a proof of clock conversion**. The model requires root permission to end by a bound anchored to request start, child permission to remain inside its parent and a drain to cover every earlier grant after its fence is observed. The clock candidates explore possible implementations separately; their composition is not established. Acquisitions are ordered by fresh epoch, with conditional-renewal/fence races; a full election among many concurrent candidates is excluded. Assignment movement, concrete RPCs, uncertain-outcome resolution, log/data history, transactions, admission, collection, liveness and implementation refinement remain open. No runtime clock dependency or replacement serving engine is adopted.


## Candidate repeated root and journal recovery

`spec/candidates/RootJournal.tla` runs the full `RootControl` acquisition, renewal, drain and activation actions alongside immutable journal uploads, positive durability observations, contiguous consumption, permission-checked acknowledgements, recovery and durable prefix selection. The root itself owns one journal in this component. A replacement must recover every earlier epoch, reuse existing selections and load the complete selected history before serving. A conditional selection checks the current owner and that the old epoch is unselected, then advances the same authority revision used by renewals. Interrupted scans may be forgotten; selected history remains durable. Uploads prepared by an earlier process may arrive after selection.

The two-epoch/two-batch instance checks **12,474 distinct states**, 81,914 generated, depth 46: two time advances, one-unit grants, at most eight authority revisions, one renewal per epoch and no child requests. The three-epoch/one-batch instance checks **2,230,369 states**, 18,114,865 generated, depth 57: three time advances, twelve revisions and the same grant/renewal bounds. Their optimistic fingerprint collision estimates are 4.7e-11 and 1.9e-6. The invariants include the combined proof's inductive strengthening, durable acknowledged prefixes, immutable selections, complete inherited history, and inclusion of earlier acknowledgements in a valid owner's serving view. The two instances do not cover every combination of repeated epochs and longer logs.

Six required broken variants fail: acknowledgement without permission, consumption across a gap, recovery before drain, stale negative object reads, replacing an existing selection, and serving before all earlier epochs are selected. A separate forbidden-state coverage check produces a trace with all three owners ready, the first two having acknowledged a batch, and the third retaining both exact selected prefixes. This is reachability evidence, not a liveness proof. Run `scripts/check-candidate.sh RootJournal`; Woodpecker requires both instances and all seven controls in an independent cache stage. [Evidence](evidence/root-journal-candidate-2026-11-09.json) records the bounds, inputs and logs.

`RootJournalProof.tla` proves `RepeatedHistorySafety` for **arbitrary finite epoch counts, unbounded log lengths and execution lengths**. All **1,047 strict TLAPS obligations** pass. Its inductive invariant connects permission exclusion to acknowledgement, completed recovery scans and immutable selections: once a later owner is active, an earlier owner cannot acknowledge beyond the prefix recovered for it. The positive finite configurations also check this strengthening. Removing the acknowledgement permission check fails the induction step. Run `scripts/check-proof.sh RootJournal`; it first checks the imported root proof and its negative control, then checks the complete composition proof with fresh fingerprints and a focused negative control. Woodpecker reuses the exact verified `proof-root-control` dependency before requiring `proof-root-journal`.

The theorem assumes an **abstract elapsed-time bound**, fresh immutable object observations, and atomic durable conditional selection with exact selection reads. It does not establish a wire implementation of that selection or infer fresh metadata from an ordinary bucket read. Pause/loss is delaying or omitting actions; replacement uses a fresh epoch. Delegated partition owners, reassignment, arbitrary elections, clock conversion/platform guarantees, transactions, receipts, admission, collection, implementation refinement and liveness remain open. In particular, objects are never reclaimed in this model. No runtime clock dependency or replacement controller is adopted.

## Candidate delegated partition movement

`PartitionControl.tla` interleaves the actual root protocol with partition generations, owner grant requests, delayed/lost replies, durable freeze decisions and activation. A move freezes renewal of the previous generation while grants for other partitions continue. The same coordinator drains the previous permission before activating the replacement. If the coordinator dies during a move, a later root can finish it after its own global drain; it never interprets its predecessor's local clock origin. Freeze and activation advance the same conditional-register revision as root renewal. Grant issuance and delivery do not mutate that register.

Four positive instances check root permission, owner containment and exclusion of different generations for the same partition:

| Instance | Bounds | Distinct states |
| --- | --- | ---: |
| Independent movement | One root, two partitions, two generations, three time advances, two root renewals, eight revisions | 488,465 |
| Repeated movement | One root, one partition, three generations, four time advances, three root renewals, nine revisions | 134,007 |
| Renewal before movement | One root, one partition, two generations, four time advances, three root renewals, nine revisions | 212,562 |
| Interrupted movement | Two roots, one partition, two generations, three time advances, one renewal per root, eight revisions | 844,280 |

The renewal instance permits two owner requests per root/partition/generation; the others permit one. All disable the root component's separate child requests. They generate 3,093,949, 697,614, 1,014,980 and 5,556,328 states, at depths 32, 33, 34 and 32. Optimistic fingerprint collision estimates are 6.9e-8, 4.1e-9, 9.2e-9 and 2.2e-7. A configured generation denotes a never-reused owner incarnation; initial placement is fixed. These are separate finite instances, not a proof of their Cartesian product or arbitrary cluster sizes.

Three unsafe controls must fail: issuing grants for frozen/superseded assignments, skipping a partition drain, and starting owner expiry at reply delivery. Four reachability controls require a moved partition and an untouched partition to hold permission under the original coordinator, two successive moves reaching a third generation, an owner deadline actually extending before its replacement gets permission, and a later coordinator actually executing completion of an interrupted move. The last two use explicit history flags; merely observing an old completed move under a newer root is insufficient. Run `scripts/check-candidate.sh PartitionControl`; Woodpecker requires every check in a separate cached stage. [Evidence](evidence/partition-control-candidate-2026-11-09.json) records exact inputs and traces.

`PartitionControlProof.tla` now establishes root/owner containment and partition-generation exclusion for arbitrary finite root, partition and generation counts, with unbounded executions and renewal counts. All **486 strict TLAPS obligations** pass locally and in the exact-source proof stage on the CI builder. The inductive strengthening records pending grants, frozen-generation bounds and generations whose permission has permanently ended. It also proves that root transitions preserve the facts needed by delegated owners. The proof assumes the root timing contract; it does not prove it. `scripts/check-proof.sh PartitionControl` first checks the imported 153-obligation root proof and its negative control, then requires mutations of assignment checking, drain completion and reply-anchored expiry to fail. The drain mutation targets the changed drain lemma rather than assuming that lemma when checking its caller. CI inherits the exact checked root stage. Both the permission and journal positive instances now check this strengthening too.

This establishes unbounded abstract permission composition under the root timing abstraction. It assumes atomic durable placement updates and an issuer whose assignment view is synchronized with freeze decisions. The wire protocol must freeze issuance before an uncertain update and recover exact placement after root replacement; that refinement is not implemented here. Platform clocks, journal selection/data history, graceful early release, placement policy, routing, transactions, resource admission, collection and liveness remain open. The root/journal theorem does not automatically prove this delegated composition. No runtime clock dependency or replacement serving engine is adopted.

## Candidate delegated journal recovery

`PartitionJournal` runs the actual `PartitionControl` and `RootControl` transitions together with immutable journal uploads, permission-checked acknowledgements, recovery and conditional prefix selection. A journal belongs to a partition's writer generation, not to a coordinator epoch. A surviving writer can receive permission from a replacement coordinator and continue its existing journal. Replacing the writer itself must allocate a new generation; it cannot reopen the old generation for writing.

| Instance | Bounds | Distinct states |
| --- | --- | ---: |
| One movement | One root, one partition, two generations, two batches per generation, two time advances | 6,329 |
| Interrupted movement | Two roots, one partition, two generations, one batch per generation, two time advances | 177,371 |
| Independent histories | One root, two partitions, two generations, one batch per generation, two time advances | 328,091 |
| Repeated movement | One root, one partition, three generations, one batch per generation, three time advances | 108,304 |
| Surviving writer | Two roots, one partition, one generation, two batches, two time advances | 93,201 |

Seven required unsafe controls detect acknowledgement without permission, consumption across gaps, recovery before permission, stale negative reads, reselection, premature serving and a skipped partition drain. Five reachability controls require inherited acknowledged history after movement, interrupted movement completed by a replacement root, simultaneous serving histories on moved and untouched partitions, two successive moves retaining both predecessor histories, and new acknowledgements by the same writer under two different roots. The last uses a ghost set recording the root authorizing each actual acknowledgement; a coordinator change alone does not satisfy it. These controls establish reachable scenarios, not liveness.

Run `scripts/check-candidate.sh PartitionJournal`. Woodpecker requires every instance and control in a separately cached stage. The [evidence](evidence/partition-journal-candidate-2026-11-09.json) records all configuration bounds, generated states, depths and fingerprint estimates. The finite instances remain bounded regressions. `PartitionJournalProof.tla` separately proves `DelegatedHistorySafety` for arbitrary finite root, partition and writer-generation counts, with unbounded log lengths, renewals and execution lengths. All **785 strict TLAPS obligations** pass locally and in the combined proof stage on the CI builder. The proof imports the actual partition/root permission theorems; it does not assume a separate ownership implementation. Its strengthening relates every prepared, serving, scanning or selected generation to the effective assignment, and prevents an ended generation from acknowledging beyond its recovered prefix.

Run `scripts/check-proof.sh PartitionJournal`. The command checks the 486-obligation partition proof and its 153-obligation root dependency first, including their controls. The journal proof requires six mutations to fail: acknowledgement without permission, consumption across a gap, early scanning, stale negative reads, reselection and premature serving. Its finite positive configurations check the strengthening and every original invariant, with duplicate clauses removed; the proof includes the equivalence of those conjunctions. CI inherits the exact checked permission stages before checking this composition.

The model assumes the root elapsed-time contract, atomic durable placement changes with synchronized issuance, fresh immutable object observations, conditional immutable prefix selection and exact validated reads of selected prefixes. A selected prefix survives losing a scan or coordinator. Delayed uploads remain possible after movement. Pauses and process loss delay or omit actions; a replacement writer is a fresh generation. Transaction evaluation, request receipts, admission, reclamation, runtime/platform refinement and liveness remain open. No clock-based serving code is adopted by this result.

## Candidate clock error margins

`spec/candidates/GrantBoundsProof.tla` proves four arithmetic lemmas with **19 strict TLAPS obligations**: an upper bound on root permission, exclusion after a successor drain, the rounded child-duration bound, and containment of a delivered child's permission within its parent. There is no finite bound on the nonnegative integer durations or error allowance in these lemmas. This is an arithmetic proof under a clock contract, not an unbounded proof of the ownership protocol.

The contract normalizes clock rates so a reading over `t` reference units lies in `[t - E, 2t + E]`; readings are nondecreasing. The additive `E` bounds the error of the whole interval measurement, including sampling and quantization. A root's expiry starts at its request; a successor's drain starts after observing its fence. Waiting `2L + 3E` on the successor's clock excludes an old root whose grant length is at most `L`. A child's duration is `floor(max(0, R - 3E) / 2)` from its own request start, where `R` is the parent's remaining interval observed when issuing the reply. Delivery delay cannot extend permission; a zero-duration child cannot serve.

Run `scripts/check-proof.sh GrantBounds`. The checker uses the same pinned TLAPS toolchain and strict fresh-cache procedure as the other proofs. Reducing the drain margin and omitting the child margin must each fail their corresponding obligation. Woodpecker requires the positive proof and both controls in a separate cached stage. [Evidence](evidence/grant-bounds-2026-11-09.json) records source/checker hashes and results.

The proof does not establish rate/error bounds for Linux, virtual machines, suspend, migration or restored process snapshots. Choosing and enforcing a platform contract, integer-unit conversion, Rust overflow/rounding behavior, root/delegation/log composition, repeated ownership safety and liveness remain obligations. The earlier finite clock candidates use zero measurement error; this arithmetic result does not automatically refine those models or the `RootControl` abstraction. Runtime assumptions remain unchanged.


## Common provider register

Both provider connections wrap conditional metadata replacement with one shared mutation-identity layer. Every invocation stamps a fresh UUID into the existing reserved `durable-db-probe` field. This matters because B2 can accept identical live metadata as a no-op despite a stale comparison revision; checking only that its response revision exceeds the caller's old revision does not establish a fresh comparison. S3 uses the same layer. It adds no provider calls or metadata slots, leaves unknown outcomes uncertain and does not infer serving permission or metadata freshness from a raw observation.

`ProviderRegister.tla` explicitly models that no-op behavior, stale observations, concurrent attempts and delayed/lost responses. Its three-attempt/two-value instance checks **8,901 distinct states**, 21,289 generated, depth 10. The required identity-reuse mutation produces a falsely accepted stale comparison; a separate forbidden-state control reaches a correctly rejected stale update whose logical metadata equals the current value. The model assumes globally unique attempt identities and exact revision comparison for real mutations. It is bounded component checking, not a cryptographic, implementation, liveness or controller-composition proof. Run `scripts/check-model.sh ProviderRegister`; its separate Woodpecker stage requires all three checks.

The real HTTP fixtures reproduce B2's no-op acceptance and verify automatic identities, repeated logical writes, stale-payload rejection and a committed response lost without retry. The S3 fixtures preserve uncertainty across committed errors and redirects through the same common layer. Fresh publication initialization accepts the known startup/read-barrier markers while still rejecting unknown protocol metadata. The [qualification record](evidence/provider-register-2026-11-09.json) includes real B2 and versioned-MinIO comparisons, concurrent identical payloads, reconnect, journal compatibility, the initial fixture failure and cleanup. MinIO does not qualify AWS, and the journal fixture does not implement a serving controller.
