# Roadmap and release gates

**The next milestone is a usable replacement serving engine.** The current shared read barrier and publication path have not met the performance brief. More qualification of that path cannot close the architectural gap.

The [production readiness plan](production-readiness.md) owns the release checklist and evidence requirements. This roadmap gives the implementation order without maintaining a second checklist.

## Build and measure the replacement

Compose the [owned-partition design](design-owned-partitions.md) into an executable path: local reads under valid ownership, independent immutable log batches, bounded transaction validation, and recovery of every acknowledged write. Preserve one B2/S3 algorithm and the object-store durability boundary.

The journal, conflict validator and root controller are implemented components. Root registrations preserve journal identities across total controller loss. The immutable prefix catalog preserves recovery decisions through uncertain replies and repeated claims, using the existing B+tree. Complete selected history can now be replayed oldest first with bounded frame and index-page memory. The controller combines one-time candidate claims, invocation-bound replies and checked intervals; B2 and S3 HTTP fault fixtures exercise the same implementation. Platform timing, transaction/placement recovery, routing, bounded historical reclamation and whole-protocol verification remain open. These components do not yet replace the HTTP engine.

The first end-to-end qualification must include stale owners that remain alive, movement, scale-down to one and complete compute loss. Measure warm reads, batched durable writes, many active databases and disjoint-key transactions within one database. Publish latency, failures, retries, fairness, provider calls and cost on B2, then AWS S3.

## Qualify a hosted preview

Integrate per-tenant physical admission, fair request and bandwidth budgets, authorization, lifecycle and bounded automatic maintenance. Unknown or delayed uploads must not create free spend rights. Add multilevel indexes, retained snapshots and recoverable compaction without making local storage authoritative.

Run Pi workflows and general web application workloads through the actual replacement API and typed client. Extend the existing Kubernetes and physical-host fault campaigns to that engine, including host loss, provider throttling, mixed versions, recovery and sustained resource bounds. Keep laptop setup and installation as primary acceptance tests.

## Release for production

Close every remaining [production gate](production-readiness.md): backup/PITR and restore drills, sustained load with injected failures, compatible upgrades, operational alerts, signed artifacts and advertised-platform installation. Resolve independent protocol and security review findings before changing the experimental status.

Keep the private source Apache-2.0 licensed and maintain the public documentation through Woodpecker. Open the source repository when its owner requests it.

Cross-database transactions, arbitrary SQL, row-level authorization and automatic conflict-free merging are outside the initial contract.
