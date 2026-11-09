# Verification

The design starts in `spec/Cluster.tla`. Do not equate model checking with a proof that the Rust binary is correct. The model abstracts implementation details; TLC explores finite configured instances. Safety for unbounded clusters, liveness under stated fairness assumptions, and a checked refinement from implementation to model are separate obligations.

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

The JavaScript adapter runs Pi 1.5.0's own storage conformance suite over the HTTP service. The adapter is explicitly a bounded reference implementation; its passing tests do not establish the performance of a future indexed adapter.

The production hardening tests add malformed manifests, foreign object references, invalid accounting, page boundary/length mismatches, duplicate map entries, trailing bytes, and memory-cache digest mismatches. Rejected manifests must not allow reads, writes, compaction, or collection to proceed. A streaming-body HTTP test holds a database's full concurrency allowance, verifies that another database and the administrator can still work, and checks cancellation releases the allowance. These changes validate inputs and admission; they add no publication or collector transitions to the existing models. They do not prove arbitrary corruption recovery or strict global resource isolation.

## Remaining proof and qualification obligations

- A mechanized inductive proof for arbitrary node/object counts; TLC's finite bounds are not that proof.
- Liveness under eventual provider availability, fair admission, and bounded competing maintenance. Availability is impossible during arbitrary permanent partitions or when all nodes remain down.
- Implementation/model refinement, filesystem and network cancellation, malformed/corrupt object behavior, allocator/resource bounds, cryptographic assumptions, and schema upgrades.
- CPU-load-triggered Kubernetes HPA, node-drain and network-partition testing, long-duration soak, and AWS qualification. HPA bound changes and forced pod replacement have passed on B2. MinIO qualification is not AWS or B2 qualification.
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

The implementation retains its eight-attempt bound. `Published::cas` rebuilds a directory path only after definite precondition failure; errors remain uncertain outcomes handled through receipts. Deterministic tests pause before each of two publication attempts, interleave multiple unrelated writes or collection, and recover a lost acknowledgement through a fresh uncached engine. They also assert that rebasing does not upload another copy of the staged manifest. The model abstracts index allocation into the root transition; composition with the directory reachability model, arbitrary-count safety, progress/fairness, and Rust refinement remain obligations. This result does not establish a throughput target.

## Sealed observation groups and immutable caching

`ObservationBatch.tla` was checked before implementing grouped read barriers. It models three readers, two unique write versions, one batch, stale observations, delayed responses after successful validation, cancellation, and abandoned work. All 25,194 distinct states (71,381 generated, depth 13) satisfy `ObservationWithinInvocation`: each returned snapshot's version falls within that reader's own invocation/response interval. The negative control admits readers after sealing and returns a pre-invocation snapshot. One batch suffices to expose this bug; arbitrary batches, a composition/refinement proof, and liveness remain separate obligations.

`observation::Observations` seals at most 64 members before calling the existing `Authority::read_fresh`. Another 64 observations may queue. A deterministic test delays a successful barrier's acknowledgement, acknowledges a competing write, then starts another reader: only a second validation may serve that reader. Another test fills the queue, cancels members, injects a failed barrier, and verifies subsequent recovery. No result is shared with a request admitted after sealing.

Remote immutable manifest bytes now share the existing bounded cache. Their unique object name and expected digest come from the freshly validated directory. Memory and disk tests cache a 300 KiB manifest, enforce a smaller caller read bound on a cache hit, replace and remotely collect that manifest, and verify a later request reads the replacement despite the stale bytes remaining cached. Directory cache hits avoid rewriting the same disk file. These cache changes add no authority transition; missing old bytes still restart the complete read, and mutable local heads remain uncached.
