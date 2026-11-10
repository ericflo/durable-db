# Roadmap and release gates

**The next milestone is a usable replacement serving engine.** The current shared read barrier and publication path have not met the performance brief. More qualification of that path cannot close the architectural gap.

The [production readiness plan](production-readiness.md) owns the release checklist and evidence requirements. This roadmap gives the implementation order without maintaining a second checklist.

## Build and measure the replacement

Compose the [owned-partition design](design-owned-partitions.md) into an executable path: local reads under valid ownership, independent immutable log batches, bounded transaction validation, and recovery of every acknowledged write. Preserve one B2/S3 algorithm and the object-store durability boundary.

The journal, conflict validator and root controller are implemented components. Root registrations preserve journal identities across total controller loss. The immutable B+tree catalog preserves selected recovery prefixes across uncertain replies and repeated claims. Replay streams selected history in order, and typed transaction records restore values, revisions and retry receipts into an unpublished target. Fresh immutable objects, journaled references and bounded range reads are implemented and checked on B2 and versioned MinIO.

The value index is native Rust. Immutable sorted tables provide sparse block indexes, integrity checks and bounded range reads. Table/memory merging preserves snapshot revisions, deletion markers and retryable I/O failures. Selected journal replay restores values, revisions and retry receipts across owner replacement, with 10,005 database generations checked. The SlateDB experiment is retired; its evidence is historical.

Durable checkpoints now bind shared native tables, database revisions and retained retry receipts to an exact selected-journal cut. Their paged manifest has ordered key bounds, and receipts are indexed by request identity. Recovery verifies the selected suffix and stops ancestry traversal at the checkpoint. B2 and versioned MinIO pass fresh-controller recovery with covered journal frames removed. Lazy checkpoint reads now seek directly to the relevant table for points, ordered ranges and receipts, with bounded immutable caching. B2/MinIO checks verify zero provider calls for the warmed working set. These remain component qualifications under injected clocks and stopped writers.

Next, make checkpoints the backing view for the live index, retaining a bounded recent memory layer, and compose transaction admission, journal durability and current serving authority. Add continuous checkpoint scheduling on live journals and reference-aware maintenance. The current recovery projection must fit its configured resident limits; the lazy checkpoint reader still needs to replace eager loading in suffix recovery and join the recent memory layer. Platform timing, lifecycle/placement recovery, routing, whole-process admission and whole-protocol verification remain open. These components do not yet replace the HTTP engine.

The first end-to-end qualification must include stale owners that remain alive, movement, scale-down to one and complete compute loss. Measure warm reads, batched durable writes, many active databases and disjoint-key transactions within one database. Publish latency, failures, retries, fairness, provider calls and cost on B2, then AWS S3.

## Qualify a hosted preview

Integrate per-tenant physical admission, fair request and bandwidth budgets, authorization, lifecycle and bounded automatic maintenance. Unknown or delayed uploads must not create free spend rights. Add multilevel indexes, retained snapshots and recoverable compaction without making local storage authoritative.

Run Pi workflows and general web application workloads through the actual replacement API and typed client. Extend the existing Kubernetes and physical-host fault campaigns to that engine, including host loss, provider throttling, mixed versions, recovery and sustained resource bounds. Keep laptop setup and installation as primary acceptance tests.

## Release for production

Close every remaining [production gate](production-readiness.md): backup/PITR and restore drills, sustained load with injected failures, compatible upgrades, operational alerts, signed artifacts and advertised-platform installation. Resolve independent protocol and security review findings before changing the experimental status.

Keep the private source Apache-2.0 licensed and maintain the public documentation through Woodpecker. Open the source repository when its owner requests it.

Cross-database transactions, arbitrary SQL, row-level authorization and automatic conflict-free merging are outside the initial contract.
