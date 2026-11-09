# Architecture reassessment

**Decision: replace the current shared per-request authority and publication path before production. The performance criticism remains accurate; it is not merely an outdated website description.** The existing engine remains a reference implementation and a source of tested API, authorization, recovery, provider and fault-injection components.

## Requirements remain unchanged

The target is one easily installed Rust executable, automatic Kubernetes scale-up and scale-down to one node, fast transactional ordered storage, and B2 or S3 as the durable source of truth. The same storage algorithm must work on both providers. Local directories support laptop development. Memory and disk caches stay bounded and disposable. Many tenants must share storage safely, with automatic maintenance, durable admission and recovery. Protocol changes start with explicit models. Source remains private, Apache-2.0 licensed, with direct main commits, Woodpecker/kache builds and public documentation.

Replaceable compute does not require every request to reconstruct authority independently. Treating those as equivalent was an architectural overconstraint. A running node may maintain useful state if ownership, handoff, recovery and resource bounds make that state safely replaceable.

## What is wrong with the current path

`Authority::read_fresh` reads the shared bucket authority and conditionally replaces it with a new read fence. Even a cache-hit application read participates in this serialized control path. Read grouping amortizes calls for a sealed group; it does not eliminate this dependency. Reads, writes and collectors can conflict on the same authority revision.

A write prepares immutable pages, manifests, directory changes and sometimes publication proposals. The shared directory removes the former database-count cap but does not create independent write throughput. The tested 100,005 directory entries establish a directory correctness property; they do not establish 100,005 concurrently useful databases. Database size limits also follow the initial bounded manifest design, not a demonstrated production requirement.

The [recorded B2 combiner experiment](verification-results.md#publication-combining-qualification) completed four-writer phases at 1.051 successful operations per second with second-scale latency. Its lifetime metrics averaged 1.20 selected members per group. Those older measurements are not a current regional SLO, but the underlying shared read/update path is still present. Passing more recovery tests cannot establish a suitable operating envelope for it.

The formal work verifies specific safety properties under stated assumptions. It does not show that the architecture meets the performance brief. Correctness and performance are separate acceptance requirements.

## Direction to evaluate

The candidate is a cluster of owned storage partitions, with many logical databases per partition, a batched immutable write-ahead log, immutable sorted runs, and bounded active indexes/caches. Ownership should be acquired and transferred at meaningful boundaries. Ordinary reads should use a validated serving authority and the local committed view, fetching immutable data on cache misses. Maintenance should compact and collect bounded ranges in the background.

This direction has four requirements before implementation can replace the existing engine:

1. **Safe read authority.** An old owner must not serve a stale successful read after a replacement commits a newer value. Write fencing alone does not solve this. The model must cover network partitions, pauses, delayed messages, one-node operation, membership changes and complete cold restart. Merely saying “use Raft,” “cache the head” or “use a lease” leaves essential assumptions unresolved. Any clock/lease or external-coordinator requirement must be explicit and assessed against the original deployment contract.
2. **Durable batched acknowledgement.** Every acknowledged transaction must be recoverable from B2/S3 after all compute and caches disappear. Persisting only to RAM or local disks and uploading later changes the durability contract and is not the default design. A WAL upload also needs an unambiguous recoverable commit decision; an uploaded but unpublished object is not sufficient.
3. **Independent capacity.** Thousands of logical databases must not each need a bucket or permanent process. Partitioning must actually reduce serialized work; putting more names behind the same per-request conditional register does not do that. Any global control path needs an explicit call budget, batching bound, recovery procedure and measured ceiling.
4. **Bounded storage work.** A multilevel index or log-structured engine should avoid a complete bounded manifest becoming the practical database-size ceiling. Admission, compaction, retained snapshots and reclamation must compose with the publication protocol; workloads must not create unbounded memory or cleanup debt.

B2's documented `b2_update_bucket(ifRevisionIs)` gives a conditional control operation. Its existence does not imply an inexpensive data-path primitive or that another object's operations provide equivalent semantics. Provider guarantees must be checked separately from successful experiments. The design must retain the common B2/S3 contract.

Object-store durability imposes a real write-latency floor. Group commit can amortize calls across many transactions and remove contention; it cannot make an acknowledged B2-durable write arrive before its required object-store persistence. We must measure that floor and the extra protocol cost separately. Fast cache-hit reads should not inherit it by construction.

## Next acceptance experiment

Build a small vertical prototype before adding more production features to it. Declare the workload, client/server/provider placement, partition count, payload sizes, cache bounds and durability mode. Record raw provider latency, end-to-end p50/p95/p99, throughput, failures, queueing, provider calls/bytes per transaction, and per-tenant progress.

The first benchmark should exercise warm reads without object-store authority updates, batched durable writes, and concurrent independent databases. Then inject owner loss and a partition with an old owner still running, shrink to one, remove all compute, and recover acknowledged history from the provider. A performance result without those semantics is not an acceptable replacement. A correctness result with the present order-of-one-operation-per-second behavior is not acceptable either.

Use small deterministic model instances and targeted tests during development. Run relevant models and negative controls after protocol changes; retain the complete cached Woodpecker gates for main. Run the expensive cloud matrix and long soaks after the candidate clears its performance gate. The existing one-hour memory pilot is baseline instrumentation evidence, not a reason to retain the old architecture.

## Serving-authority experiment

A research-only `ServingLease` model explores local reads with explicitly bounded-rate clocks, delayed grants, pauses, takeover and process loss. Its small instance passes 4,884,220 states. Removing takeover drain, allowing a paused clock to stop, or starting expiry at delayed response delivery each produces a stale-read counterexample. [Candidate evidence](evidence/serving-authority-candidate-2026-11-09.json) records exact bounds, assumptions and source hashes.

This is **not an adopted protocol**. It deliberately explores a timing assumption that the current runtime rules prohibit. Supporting that assumption on the deployment platforms would need a separate architecture decision and justification. The model's commit operation still atomically checks authoritative ownership; it does not establish a WAL-only write path or independent partition throughput. The experiment identifies both a possible way to remove the per-read object-store barrier and a concrete condition under which that way fails. Runtime reads remain unchanged.

## Log handoff and raw provider measurements

The [owned-partition design](design-owned-partitions.md) now spells out the acknowledgement and recovery boundary: immutable batch identities, a contiguous durable prefix, expiration of old acknowledgement permission, and a durable seal before a successor serves. Its stepwise recovery model passes 1,214 states and rejects five unsafe variants. A subsequent [combined serving/log candidate](evidence/owned-wal-candidate-2026-11-09.json) checks 570,280 states and seven unsafe variants for one handoff, including replacement on a different clock origin. It removes the abstract global-owner check from old-writer acknowledgement. Arbitrary epochs, delegated renewal, the deployment timing contract and full protocol composition remain open; no replacement runtime is adopted.

A bounded raw-provider experiment passes on B2 and versioned MinIO, with all 194 objects verified after reconnecting and both buckets cleaned up. B2 64 KiB uploads reach 23.67 objects/second at concurrency 16, with median/p95 latency of 387.7/1,100.5 ms. This is raw object throughput, not database transaction throughput or a production SLO. It supports independent batched publication while exposing the durable-write latency floor. Full measurements, failures, exact bounds and reproduction are in the design and linked evidence.

## Public API and examples

The client should expose understandable types and operation boundaries, with readable modules and API documentation. Examples under `examples/` should exercise ordinary server-side web application needs: settings, paginated records, and atomic changes across related records. They must demonstrate scoped credentials, conflicts and uncertain outcomes without hiding the database's guarantees. Pi integration remains an important acceptance workload, alongside general application storage.

## Sources

- [Backblaze conditional bucket update](https://www.backblaze.com/apidocs/b2-update-bucket).
- [Current shared authority and read path](architecture.md).
- [Measured performance and qualification history](verification-results.md).
