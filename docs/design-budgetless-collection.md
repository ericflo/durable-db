# Data-page collection without upload credit

**Implemented for fresh format-5 cloud namespaces; production qualification remains open.** The format-4 prototype stops immutable PUTs at exhaustion, but its per-database page GC needs an uploaded fence manifest. Format 5 adds a collector using only the existing authority comparison and physical deletion. Compaction remains a funded upload operation; deferring compaction does not prevent this new collector from reclaiming unreachable pages.

## Fence the complete write attempt

A format-5 manifest observation captures both the database pointer and the current global collection fence before the engine prepares pages. Before submitting its immutable manifest/proposal, publication compares that original fence with the authority. A changed fence is a definite conflict only before proposal submission. Once a proposal exists, the existing unknown-outcome and helper rules remain in force; every helper compares the proposal's original fence at publication.

Capturing the fence only when submitting the proposal is unsafe. A writer could upload an unreferenced page, pause, have that page collected, and then adopt the collector's new fence while publishing a manifest that refers to the removed page. `GlobalCollection.tla` includes this as a required negative control. Format 5 makes the stronger observation token mandatory. Formats 3 and 4 retain their earlier collector behavior.

## Bounded shared sweep

List at most 128 data-page candidates from a persistent global cursor, then observe the authoritative directory root. Conditionally replace the same authority revision, preserving the root and upload ledger, advancing the global collection fence, and saving the next cursor. This step uploads no immutable object. If the comparison fails or its response is lost, do not delete from that uncertain sweep.

For each candidate database, read its manifest through the protected directory snapshot and validate both its content digest and manifest structure. Delete only listed pages absent from that manifest. A missing directory node or manifest is an error that abandons the affected work, never evidence that all its pages are garbage. A genuinely absent database entry may protect no pages: subsequent creation starts empty, and any later data upload has a fresh name and occurs after the listing.

Every subsequent publication either began after this fence with a new page name, carries forward a live protected reference, or must fail its original pointer/fence comparison. A delayed upload may become an orphan after deletion/listing; later cursor cycles reclaim it. No deletion refunds cumulative upload credit. Concurrent collectors may use different protected roots; each must independently satisfy list-before-observe-before-conditional-fence and validated reachability.

The model includes writer/process loss, retained pages from previous manifests, helper publication after the origin disappears, delayed upload/deletion, and concurrent collectors. Negative controls omit the origin fence, omit the collector fence, or list after fencing. Implementation tests cover missing indexes/manifests, a valid digest over invalid manifest content, two writers racing both collectors, cursor continuation after restart, uncertain fences, and exhausted proposals. A temporary Rust negative control omitting the origin-fence comparison fails the paused-writer regression; the correct comparison is restored and passes. Composition with the credit allocator, a checked refinement, and unbounded/liveness proofs remain open.

## Compatibility obligation

The format-4 prototype uses `upload_format: 4` and captures only pointer identity. It cannot safely coexist with this collector. New budgeted namespaces initialize format 5, with `upload_format: 5`; format-3 and format-4 binaries refuse the new authority before serving data. Current code can still open a format-4 namespace, enforce its existing upload ledger, and use its original funded page collector. Startup never changes it to format 5, even when budget flags are provided. No in-place migration or downgrade is supported. Qualified export/restore tooling is required before promising an upgrade of existing data.

The ordinary cluster maintenance loop invokes the format-5 data sweep. It scans one global cursor in batches of 128, independent of database count. This sweep still incurs control, listing, reading, and deletion costs, which are outside the immutable-upload ceiling. Slow providers, perpetual contention, or missing protected metadata can delay progress. The short cloud qualification does not establish a cleanup-latency SLO or an upper bound on every provider charge.
