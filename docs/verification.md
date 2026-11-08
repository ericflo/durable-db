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

## Remaining proof and qualification obligations

- A mechanized inductive proof for arbitrary node/object counts; TLC's finite bounds are not that proof.
- Liveness under eventual provider availability, fair admission, and bounded competing maintenance. Availability is impossible during arbitrary permanent partitions or when all nodes remain down.
- Implementation/model refinement, filesystem and network cancellation, malformed/corrupt object behavior, allocator/resource bounds, cryptographic assumptions, and schema upgrades.
- Controlled Kubernetes HPA/drain/kill testing, long-duration soak, and AWS qualification. MinIO qualification is not AWS or B2 qualification.
- Independent security review and strict cluster-wide physical-storage/spend admission.

The repository must keep these limitations visible until the corresponding evidence exists. “Proven in all situations” is not an accurate description of this initial release.

## B2 authority refinement

`Bucket.tla` models the immutable manifest layer selected by bucket metadata. `publication::Published::cas` implements Upload→Publish; `collect_manifests` implements List→Read→Fence→Delete. Native `ifRevisionIs` must implement atomic revision comparison; bucket listings may be stale because `Authority::read_fresh` validates them through a conditional update. Physical file deletion removes versions, not markers. A successful head read supplies the unique manifest identity as the engine version. A bucket revision conflict is safe even when another database caused it.

The shared publication adapter tests exercise injected revision authority, paused publication, lost acknowledgments, two-layer collection, empty-cache recovery, and bounded metadata admission. Real-provider qualification and a mechanized composition proof remain separate obligations.

The publication model now applies to both providers. B2 revision numbers and S3 opaque object versions implement the same compare-and-publish boundary. `ReadBarrier.tla` checks stale directory observations with two readers, three values, and four revision advances. Its negative control skips validation and returns a stale snapshot. Provider-specific wire tests complement these abstractions; no bounded model run proves the whole implementation.

## Persistent directory

`Directory.tla` models immutable leaves and ordered, nonoverlapping branch ranges, including reuse of existing nodes under new roots. With four keys and five allocated nodes, it checks that a node is reachable from a root exactly when that node appears on its witness-key search path. The negative control uses the wrong witness and must violate `WitnessReachability`. The model does not establish Rust B+tree balancing, checksums, or publication composition by itself.

The Rust stress test creates 10,005 databases in permuted order, forces a three-level tree, paginates every entry (including prefix/suffix ordering edge cases), updates an existing entry, sweeps obsolete index pages, and reads every database from a fresh uncached backend. Deterministic publication/collection races and lost acknowledgements also run through the tree. Provider tests separately cross the leaf-split boundary on real B2 and S3-compatible HTTP storage.
