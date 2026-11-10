# Owned partitions and durable log batches

**Status: candidate design, not the runtime engine.** Replace the shared per-request authority path before production. The existing engine remains the compatibility and correctness reference while this design is evaluated.

## Why the current engine must change

Replaceable compute does not require every request to reconstruct authority independently. Treating those as equivalent was an architectural overconstraint. A running node can retain useful state when ownership, handoff, recovery and resource bounds make that state safely replaceable.

`Authority::read_fresh` conditionally updates the shared bucket authority even for a cache-hit read. Grouping amortizes this operation but does not remove it. Writes and collectors also contend on the same authority revision. The directory removes the former database-count cap without creating independent write capacity: its 100,005-entry correctness test is not evidence of 100,005 concurrently useful databases.

The [B2 combiner measurement](verification-results.md#publication-combining-qualification) completed four-writer phases at 1.051 operations/second with second-scale latency. The later [one-hour baseline](evidence/physical-memory-hour-2026-11-09.json) also remains far below the intended operating envelope. Safety checks cannot establish performance suitability. The replacement must meet both requirements.

The current [engine protocol](architecture.md) remains the compatibility reference while the replacement is built. Provider bindings, API behavior, authorization and adversarial fixtures remain useful; the shared per-request publication path will be retired when its replacement passes the same behavioral checks and the new performance gates.

## Preserve the original contract

| Requirement | Direction |
| --- | --- |
| One easily installed Rust binary | Serving, routing, coordination and maintenance are roles of the same binary; no separately installed coordinator is selected |
| Kubernetes scaling down to one | Place owned partitions on available processes; transfer ownership explicitly; recover after complete compute loss |
| Fast application storage | Local reads under valid serving authority; batch accepted transactions into immutable log objects; independent partitions publish independently |
| B2/S3 as source of truth | Acknowledge only after the relevant log prefix is durable in object storage; retain one storage algorithm over both bindings |
| Bounded disposable caches | Memory indexes, read caches and optional disk caches have explicit budgets; object storage can reconstruct them |
| TLA+ from the start | Model serving authority, log handoff, delayed uploads, admission and collection before adopting the protocol; compose the properties and state assumptions |
| Many untrusted applications | Many logical databases per partition; per-tenant logical and physical admission, fair queues and bounded work |
| Automatic maintenance | Partition-owned compaction and retention-aware reclamation; recovery cannot depend on maintenance having completed |
| Laptop development | Keep single-process local-directory mode; use the same transaction and log semantics where applicable |
| Open-source quality and delivery | Apache-2.0, private source until requested otherwise, direct main commits, readable SDK/examples, public docs, kache and Woodpecker |

This design does not weaken acknowledgement to mean RAM replication or a future upload. Optional asynchronous durability would be a different contract and is not part of this proposal.

## Separate control from data

A slow conditional control register is useful for creating a new ownership epoch, publishing a durable handoff decision and changing placement. It should not be on every application read or every independent database write.

A partition contains many databases and has one current writer incarnation. Its owner maintains the committed view and groups accepted transactions into a bounded log batch. Other processes route requests to that owner. The number of databases is not the number of buckets, partitions or permanently assigned processes. Placement and partition-count limits must be measured and documented; this does not reintroduce a fixed small database cap.

One conditional register is still a potential control bottleneck. A candidate coordinator can batch placement and serving-authority renewal, while owners publish log batches independently. The coordination role must run in the database binary and recover from object storage. Delegated ownership and journal recovery now have abstract composition proofs; their timing and provider-request implementation is **not yet established**. A single conditional update per partition per write would leave the important bottleneck intact.

### Renewal without a provider call per partition

The coordinator can renew one durable root permission, then grant shorter permission to active owners over the cluster network. An owner may serve many partitions and each partition may contain many databases. Renewal demand should therefore follow active owners, not the total database population. The one-node case combines these roles in one process.

For a coordinator with `R` local clock units remaining, and clock rates bounded between `r_min` and `r_max`, a child interval may be no longer than `floor(R * r_min / r_max)`, anchored at the child's **request start**. At issuance, the parent cannot expire sooner than `R / r_max` real time units. A child interval of that size ends within `R / r_max` real units of its earlier request start. It therefore cannot outlive the parent interval. Implementing this argument requires explicit clock-resolution and measurement-error margins as well as the rate bound; those margins are not yet selected.

The `DelegatedGrant` candidate checks this containment with two owners, rates of one or two units per step, one fixed root grant, delayed replies, pauses/crashes and a fenced successor. Its **1,258,368-state** instance passes five required negative controls. Giving a child all the parent's remaining time, ignoring the rate ratio, already fails. See [the bounded evidence](evidence/delegated-grant-candidate-2026-11-09.json).

This would make provider renewal calls constant per root-renewal interval; owner grant messages still scale with the number of active owners. Placement changes, durable seals and admission would add control work. Grant messages need bounded batching and backpressure. Assignments must remain fixed for each granted interval; movement must exclude the previous owner before a replacement serves. The isolated grant model does not cover those operations; the root, partition and journal compositions below extend that coverage under explicit abstract contracts. None establishes a throughput ceiling. Neither a renewal interval nor a production partition count is selected.

A successor first fences further renewal by the old root. It starts a drain after confirming that fence, renews its own interval while draining, then conditionally activates. Delayed replies retain their request-start deadline. Partition movement follows the same rule at a smaller scope: stop grants for the affected generation before freezing it, let its old permission expire, then activate the replacement. Other partitions keep receiving grants. A replacement coordinator uses its own completed root drain; it never adopts a predecessor's clock reading.

[Root permission](verification.md#candidate-root-renewal-and-repeated-takeover) and [partition permission](verification.md#candidate-delegated-partition-movement) have abstract composition proofs. Their deployment timing contract and controller implementation remain open.

### Delayed control updates

A timeout does not cancel a provider operation. A freeze, activation or prefix selection may apply after the caller has stopped waiting or its permission has expired. Such late application does not authorize serving, issuing grants or acknowledging transactions.

Each request must capture an exact comparison, payload and invocation identity before sending. Only its matching current response can confirm a view. Raw metadata reads may be stale; an uncertain operation requires exact revalidation. A conflict describes that invocation alone and cannot prove that an earlier abandoned operation never applied.

An already earned root interval survives a renewal attempt, but starting or timing out that attempt cannot extend it. A newly confirmed fence starts a drain at confirmation. Recovering an uncertain activation instead uses the drain completed before that activation was sent. Placement has an additional obligation: invalidate the affected local assignment before sending its freeze, and never undo that invalidation merely because the request timed out.

`providers::Connection::control()` implements the request lifecycle in Rust, including invocation matching, bounded metadata and explicit uncertainty. It uses the same nonce-stamped conditional register for B2 and S3. Its real HTTP tests keep provider processing alive after caller cancellation. It does not interpret root intervals or placement, and confirmed metadata records a historical successful comparison rather than continuing permission.

The [verification record](verification.md#control-request-session) separates this implementation from the [root request model](verification.md#candidate-root-requests-and-revalidation), [placement uncertainty](verification.md#candidate-placement-requests-and-uncertain-replies), [placement during takeover](verification.md#candidate-placement-requests-across-root-replacement) and [late application coverage](verification.md#candidate-delayed-control-application). These components must still be composed with journal selection, physical timing and the serving engine.

## Transaction concurrency

Independent partitions address contention across databases. The current API also compares a whole-database `expected_revision`, so transactions on unrelated keys within one database can still conflict. Moving that API onto a faster journal does not remove this second bottleneck. The performance qualification must include both many active databases and disjoint-key writers in one database.

The replacement transaction layer should validate bounded key and range read sets against one consistent snapshot. Range checks must detect phantoms, including inserts and deletions; reading several keys must not mix snapshots. The existing whole-database revision condition retains its strict meaning for existing callers. More precise conditions require an explicit versioned API and client support. The internal validation component below has a preceding model; the HTTP API still uses its existing revision condition.

An owner can order accepted transactions within a batch and validate each against its accepted predecessors. Client-visible reads and acknowledgements must follow the contiguous durable prefix; speculative changes cannot escape through a read. Snapshot retention, expired snapshots, request receipts, quota checks and bounded conflict metadata belong in that protocol. A benchmark must report retries and fairness as well as aggregate throughput, so batching cannot hide starvation or repeated recomputation by individual clients.

### Bounded transaction validation

`src/transactions/validation.rs` now implements an internal conflict window for one database. It validates point keys, ordered ranges and the strict whole-database revision condition against every accepted predecessor, including transactions awaiting durability. Disjoint keys can share a snapshot. Range checks cover absent keys; insertions, deletions and no-op writes all record their mutated keys. The owner must capture the token and values from the same durable view, serialize evaluation/admission with acceptance, and advance that view only through the journal's contiguous durable prefix.

Snapshot tokens belong to one validator incarnation. Recovery or movement creates a fresh instance and expires old tokens. Conflict metadata can be reclaimed only for committed revisions; a snapshot older than the retained history expires instead of passing an incomplete check. Pending writes cannot be discarded to make capacity. Errors leave the validator unchanged by that invocation; they do not resolve an earlier request. Receipt recovery must precede validation, and snapshot expiry cannot justify recomputing a transaction after an uncertain result. The existing whole-database condition still conflicts with every intervening transaction, even one whose only change is a receipt.

Defaults retain at most 4,096 transactions, 65,536 key references and 4 MiB of key bytes counted across those references. Repeated keys within a transaction are deduplicated; repeated keys across transactions remain charged. Each request has at most 128 read terms and 128 mutation keys, with 1,024 bytes per key. Range validation inspects at most 4,096 index entries across a transaction before returning a work-limit error. If its snapshot already matches the accepted revision, range validation needs no index scan. These bounds cover this component's key storage and container cardinality, not whole-process RSS or tenant admission.

A preceding finite model and adversarial Rust tests cover read dependencies, phantoms, pending predecessors, history reclamation and disjoint transaction acceptance. This component does not store values or implement authenticated wire tokens, receipts, tenant quotas, journal reservation or ownership. It is not used by the HTTP engine yet. [Verification and remaining composition obligations](verification.md#transaction-validation-component).

## Immutable batches and acknowledgement

Assign each batch a fresh identity from its partition, unique writer epoch and never-reused sequence position. Encode accepted transactions, request identities, resulting database revisions and the preceding log digest. The process sends at most one PUT for that identity. An unknown PUT outcome stalls that sequence; it may be resolved by reading and checking the exact object. It must not allocate a replacement transaction identity or send different bytes under the same name.

Uploads may be pipelined, but acknowledgement advances only through a contiguous durable prefix. A completed upload at position three cannot justify acknowledging it while position two is unresolved. Before acknowledgement, the writer must also verify its serving permission. Merely having started the upload while permission was valid is insufficient. If permission expires after a PUT might have landed, the response is uncertain, not a definite conflict or rollback: recovery may still retain that batch.

This creates a possible one-immutable-PUT path per batch, with no shared authority update per transaction. The journal implements the immutable byte path, hash-chain validation and prefix replay. Request receipts, transaction evaluation and ownership must compose with it before this becomes a serving engine.

### Journal implementation boundary

The first Rust component is a bounded immutable journal in `src/journal/`. Its objects live under `journal/`, outside the existing page collector's `blocks/` namespace. A writer always receives a fresh random epoch and never reopens an old epoch for writing. Frames bind the partition, epoch, sequence, previous digest, length-delimited records and a SHA-256 digest. Decoding validates lengths before allocating record views.

A prepared upload is single-use. Lost or cancelled responses retain an unresolved position; resolution performs a point GET and compares the exact bytes, never a second PUT. A missing result remains uncertain because an upload can still arrive. Completed positions become consumable only in contiguous order. A count and byte budget bound frames held by the queue, uploads and consumers, including unresolved work. Record views borrow their frame, so a small retained record cannot silently pin an unaccounted whole-frame allocation. Each replay reader permits one live frame, reserving its buffer before GET; the caller releases it before requesting another. A frame is at most 4 MiB, a record at most 1 MiB, and a frame contains at most 1,024 records; these are internal journal bounds, not a revised public API contract. Resolving one uncertain writer position needs one additional read buffer of at most 4 MiB; record-offset tables and provider transport buffers add bounded overhead. These component limits do not establish a process-wide RSS or tenant admission bound.

Discovery reads one frame at a time and stops at the first fresh missing position. That result is provisional until the ownership controller durably selects it after ending old acknowledgement permission. Replay of a selected prefix requires every frame and the exact terminal digest, and never extends the prefix when late uploads appear. Provider errors and corruption fail closed.

The journal reports durability, not transaction success or serving permission. It does not implement ownership, seal publication, transaction evaluation, admission or collection. Those remain controller/engine obligations. The earlier handoff models preceded this component. `Journal.tla` now models durability knowledge and ordered consumption as well as recovery; its unbounded abstract proof assumes an external end to old acknowledgement permission and atomic prefix selection. `OwnedWal` explores one realization of that boundary. Executing the journal does not establish the complete serving protocol or adopt a clock dependency.

The B2 and S3 bindings now return the same `providers::Connection` without initializing the publication engine. It exposes measured object operations and the conditional metadata register separately. The journal can write and recover through these transports before any database format is initialized; the existing engine is an explicit consumer through `into_published`. Raw object CAS is not part of the portable B2 contract, and a metadata observation is not serving permission. A repeated provider check recovered 512 records on each backend, resolved three lost replies and excluded a late suffix; its batch paths made zero authority calls. The S3 run used versioned MinIO, not AWS. The fixture still explicitly uses the existing publication engine to persist its prefix selection.

## Handoff fixes an exact old prefix

A successor must:

1. Fence new grants to the old incarnation and establish that all old acknowledgement permission has ended.
2. Recover the old log's contiguous durable prefix. Deterministic epoch/sequence names permit ordered point reads; discovery need not rely on one atomic snapshot of a paginated listing.
3. Durably publish an immutable handoff decision containing that exact prefix before serving or appending a successor epoch.
4. On restart, recover the existing decision. Never extend an already accepted old prefix merely because another old upload appeared later.

A late upload beyond the sealed prefix is unacknowledged garbage. Including it opportunistically after new work has begun could change the history underneath the replacement. An old upload included before the decision may represent a pending operation with a lost response; normal request-identity recovery must resolve that uncertainty.

The [journal proof](verification.md#journal-prefix-safety) establishes prefix durability, acknowledgement preservation and selection immutability under the controller/provider contracts. [Root recovery](verification.md#candidate-repeated-root-and-journal-recovery) and [delegated recovery](verification.md#candidate-delegated-journal-recovery) then compose these rules with repeated ownership changes. A journal belongs to a writer generation, independently of the coordinator's epoch: a surviving writer may continue under a new coordinator, while a replacement writer must use a fresh generation. The composed abstract proofs do not yet establish wire uncertainty, platform timing, transactions, admission or the Rust controller.

The model assumes fresh per-key reads: a negative result must not conceal an upload completed before that read began. [AWS documents strong object read-after-write consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel); the B2 visibility contract is described below. Transport errors and unresolved responses must never count as end-of-log. The provider experiments exercise successful reconnect/read/list behavior and journal replay; they cannot prove the provider's consistency contract or the absence of stale negative reads in every failure mode.

The provider control interface now stamps a fresh mutation identity on every conditional replacement, using the same code for B2 and S3. This handles B2's identical-metadata no-op behavior without requiring the future controller to remember a wire-level precaution. It does not turn raw observations into fresh permission or anchor grant expiry; those remain controller obligations. See [the contract and qualification](verification.md#common-provider-register).

The recovery scan requires a narrower freshness contract than a general metadata read: after an immutable upload succeeds, a later point read must find those exact bytes or report an error, rather than incorrectly reporting absence. Backblaze describes confirmed uploads as immediately visible in its [storage architecture explanation](https://www.backblaze.com/blog/design-thinking-b2-apis-the-hidden-costs-of-s3-compatibility/), and its [file documentation](https://www.backblaze.com/docs/cloud-storage-files) says uploads can be downloaded immediately. We rely on that object-visibility contract for direct, uncached recovery reads. It does not establish fresh bucket metadata, atomic multi-object snapshots, external CDN behavior or cross-region replica freshness. An uncertain upload may still arrive after a scan; the selected-prefix protocol handles that case separately.

## Serving authority remains the central decision

Write fencing alone does not prevent a paused old owner from answering stale reads. A peer quorum alone also does not explain how an isolated former single-node deployment is excluded after complete replacement.

The [independently clocked handoff model](verification.md#candidate-serving-permission-and-log-acknowledgement) checks permission after upload, delayed grants, process pauses/loss, successor restart with a different clock origin, prefix selection and recovery for one handoff. The repeated permission and journal proofs extend different parts of that argument under abstract timing contracts. Neither component proves a deployment platform's clock behavior.

The repository's current no-clock correctness rule was an implementation constraint, not a requirement in the original product brief. It remains in force for runtime code. Adopting elapsed-time authority would require a documented architecture decision, justified platform/timing assumptions, a composed model and fault tests. No clock-based runtime change is made by these experiments. No external coordinator is selected as a substitute.

Clock selection alone is not that timing contract. Linux documents that `CLOCK_MONOTONIC` stops during suspend and `CLOCK_BOOTTIME` includes suspend, making the latter a candidate for this requirement. The kernel's virtualization documentation separately describes timekeeping and migration difficulties. These sources do not establish our required rate/error bounds across every virtual machine or migration. A deployment contract and failure tests must address those cases before runtime adoption. [Linux timekeeping](https://docs.kernel.org/core-api/timekeeping.html), [KVM timekeeping](https://www.kernel.org/doc/html/latest/virt/kvm/x86/timekeeping.html).

QEMU specifically documents that a paused or saved-and-restored guest can miss the elapsed gap and need its system time corrected. That is a reason to qualify VM pauses separately from ordinary process pauses; it is not evidence that a guest clock satisfies the lease bound. Restoring a live process snapshot cannot preserve serving permission without a fresh incarnation and recovery. No such platform guarantee is assumed by the current engine. [QEMU guest time](https://www.qemu.org/docs/master/interop/qemu-ga-ref.html#command-guest-set-time).

Finite clock precision also needs a margin. The candidate arithmetic now assumes that a clock's reading over an interval of `t` reference units lies between `t - E` and `2t + E`, with nondecreasing readings; `E` includes the error of the complete elapsed-time measurement. Under that contract, a root grant of length `L` requires a successor drain of at least `2L + 3E` on its own clock. A child may receive at most `floor(max(0, R - 3E) / 2)` units from a parent's remaining interval `R`, anchored to the child's request. Zero duration permits no service. `GrantBoundsProof.tla` proves these arithmetic implications with 19 obligations and two failing margin-removal controls. It does not establish that a deployment satisfies the assumed bounds, select `E` or `L`, or prove the complete controller. See [the proof scope](verification.md#candidate-clock-error-margins).

The [Rust interval component](verification.md#ownership-interval-component) implements checked arithmetic, request-anchored expiry and conservative drains with separate process-clock identities. Failed or backwards readings permanently retire a timeline. It supplies no system clock and grants no authority by itself; platform qualification and controller composition remain necessary.

The timing profile must be part of the durable, versioned cluster contract. Every participant must reject a mismatched profile. A rolling configuration change cannot shorten the drain while grants issued under a longer lifetime or larger error allowance may still exist; changing the profile needs an explicit transition that excludes those grants.

## Root controller and bootstrap

`src/ownership/root.rs` connects the common provider session to the checked interval component. A fresh UUID binds once to an observed predecessor and its checked successor epoch. Competing candidates may capture the same predecessor; exact publication selects one. An uncertain claim is recovered by validating that same incarnation and epoch, never by retargeting its identity. The acquisition model starts with an empty register and includes claims that apply after caller timeout.

The first matching confirmation starts a full drain. Renewals preserve an existing active interval while in flight, but only the current successful invocation extends it from its captured start. Activation does not extend the interval. Lost activation replies can be recovered by exact validation using the earlier completed drain. Expired, foreign or abandoned replies cannot manufacture permission. Observation futures own their state so provider waits need not hold the controller lock.

The experimental `owned-candidate-3` format binds the namespace, incarnation, epoch, phase, full timing profile and recovery catalog. Unknown fields/formats, legacy publication metadata, mismatched profiles, exhaustion and invalid confirmations fail closed. Provider-specific transports run the same controller; the B2 ten-field metadata bound includes room for the shared mutation identity. Existing non-reserved metadata is preserved.

This implementation has no system clock adapter, automatic election/renewal driver or placement scheduler. The controller's permission check must follow recovery, durable-prefix verification and preparation of a consistent data view. The acquisition model, interval proof and earlier root proofs are component evidence; no theorem yet proves their complete Rust composition. The [verification record](verification.md#candidate-root-controller) includes controlled-clock tests and loopback B2/S3 HTTP faults, not a new cloud benchmark or platform qualification.

## Durable journal registration

A root cannot overwrite its predecessor's only journal identity and rely on process memory to recover it. Before its conditional claim, the controller persists an immutable registration naming its own journal and the predecessor registration. The comparison and predecessor are captured before the upload. A lost upload response requires a positive exact-byte read; an absent or failed read leaves publication unattempted. Each registration has one PUT attempt. Unknown or losing uploads may remain unreferenced and do not create refund rights.

The current format requires this discipline and rejects the earlier unregistered candidate format. The root's fresh incarnation also names its reserved coordinator journal. `take_journal` opens that writer only once under a live interval; it cannot reset an old log's sequence. A new controller follows the current registration's immutable parent chain to discover previous journals after all prior controllers have disappeared. Missing, corrupt, wrong-namespace or incompatible-profile registrations fail recovery without advancing its cursor. Records are checksummed and at most 234 bytes; traversal keeps fixed bookkeeping, while callers must bound retained results.

The [registration model and tests](verification.md#durable-root-registrations) establish this discovery property. The catalog below adds durable prefix selection. Recovery still walks the chain; its I/O grows with ownership history. The registration namespace has no collector or physical admission yet. Those are implementation gates, not properties inferred from the existing publication collector.

## Persisted recovery decisions

The root record carries an immutable catalog identity. Each successful claim inherits the catalog from its compared predecessor. The catalog reuses the existing bounded-page B+tree to map registered journal identities to exact prefixes; it is not limited by the number of metadata fields. Its checksummed header is at most 4 KiB, directory pages at most 64 KiB, and each prefix is 112 bytes.

`recovery()` starts only after confirmed activation and drain. It returns an owning cursor that can wait for storage outside the controller lock. Previous journals arrive newest first. Existing selections use exact-prefix replay, which cannot include later uploads. An absent index entry permits discovery; a missing or corrupt referenced object is an error, never an absent entry. A completed unselected scan is opaque and bound to the controller that started it.

`select_prefix()` requires live permission and the same confirmed catalog. Renewal can advance the provider comparison while a scan runs, provided the catalog is unchanged. The operation captures that comparison before uploading its prefix, copied index path and fresh catalog header. Only successful uploads allow the conditional register mutation. Upload errors leave possible orphans and send no publication. A cancelled or timed-out publication may still apply; the next exact validation or winning claim recovers the canonical decision. A changed catalog requires a fresh recovery snapshot.

`replay()` reads the complete selected catalog in increasing epoch and frame order. It checks the expected journal count before returning frames, checks canonical consecutive epoch keys while paging, and validates each selected hash chain. A cursor retains at most 64 queued keys and one frame budget, reused between journals. The cursor owns its frame and returns a borrowed view; callers must release that borrow before advancing. Its reader cannot obtain a second allocation slot by crossing a journal boundary. Missing or corrupt history does not advance the failed read or complete recovery. Replay may finish after an ownership interval expires: callers must reconstruct a private view and check permission after completion before exposing it. [Ordered-replay evidence](verification.md#ordered-catalog-replay) covers this byte-stream contract.

This implements recovery decisions for coordinator journals. The transaction projection below consumes that history, but the serving API still needs an indexed value view, creation and authorization state, placement and a final permission check. The catalog has no collection, checkpoint truncation or physical upload admission. Uncollected history and recovery work grow with ownership changes; callers must bound retained cursors and frames. The [catalog model and fault tests](verification.md#durable-prefix-catalog) state the checked scope.

### Transaction records and private recovery

`transactions::record` encodes evaluated mutations with their database name and generation, resulting revision, original snapshot revision, request identity and fingerprint. A generation prevents a recreated database name from inheriting an earlier database's receipts. Mutations are unique and ordered; deletion and an empty value have distinct encodings. Recovery applies these results without rerunning application code or evaluating old reads against newer values.

Each record also fixes the receipt-retention floor. Recovery follows this durable decision instead of choosing a window from its current configuration. The floor cannot regress. Retained matching receipts resolve before snapshot-age checks; a missing receipt from a snapshot below the floor is **expired**, not proof that the operation failed. The internal format permits a window of up to 65,536 revisions rather than fixing retention at the current HTTP engine's 128 receipts. This is a bounded format/recovery capability, not a selected production policy or a guarantee that every active database keeps that many receipts in RAM. Request lookup uses an ordered index; eviction follows revision order.

`transactions::recovery::restore` consumes a fresh complete-history cursor and an unpublished target. It validates contiguous per-database revisions and receipt identities before passing evaluated mutations and the next metadata state to the target. Missing or invalid history, target errors and cancellation discard the private target; an already advanced cursor cannot be used to omit an earlier part of history. Only completed replay returns the target. A target implementation must keep partial state private and install all supplied values and metadata; the trait cannot establish those obligations for an arbitrary implementation.

Records are at most 1 MiB, with 128 mutations, 1,024-byte keys and 64 KiB values. Decoding borrows keys and values from the journal frame and allocates at most 128 views. Per-database receipt state has bounded cardinality; whole-process admission, database residency and index memory remain target/serving obligations. Tests use a disposable in-memory projection, including two databases across owner replacement, late uploads, interrupted projection and real S3 HTTP object operations. No production index, checkpoint starting position, database lifecycle, live group-commit driver or serving API is implemented by this component. [Verification](verification.md#transaction-records-and-private-recovery).

### Native value index

The replacement index is being implemented in durable-db's Rust code. The SlateDB evaluation is retired; its standalone programs, adapter and separate dependency lockfile have been removed. It was never a dependency of the shipped server. The [historical evaluation](evidence/external-files-2026-11-09.json) remains useful evidence about the common object transport, selected-journal reconstruction and the value of block caching. It does not qualify the native index.

Use one durable transaction journal. Accepted changes enter a bounded ordered memory view; only the contiguous durable prefix is readable or acknowledged, and every response requires current serving authority. Flush that view into immutable sorted tables with sparse block indexes and checksummed range reads. Point lookups should fetch the relevant blocks, while ordered scans merge bounded cursors. Retain revisioned values and deletion markers needed by supported snapshots. Compaction must preserve visibility before any physical bytes can be reclaimed.

A durable manifest/checkpoint must name the exact table set and replay position. Upload data before publishing its references. Recovery must verify that checkpoint and replay the selected suffix; it cannot rebuild from whatever files a bucket listing happens to return. Cancelled or uncertain uploads retain their physical charges. Snapshot retention, compaction and collection need a shared reference protocol. These are implementation requirements, not capabilities already provided by a native index.

The existing immutable-object primitive supports objects up to 64 MiB and responses up to 8 MiB. Successful uploads return an opaque reference with a fresh identity, size and SHA-256 digest; unknown uploads return no reference and are not retried. Complete bounded reads verify the digest. The native table reader now verifies block metadata through a separately checksummed index object, then checks each bounded data block against its recorded digest. The provider bindings, journal, replay cursor and transaction validator remain reusable without another database engine.

`src/index/table` implements the first native table format. Records sort by binary key ascending and revision descending, preserving deletions separately from empty values. The sparse index searches both key and revision, including when one key's versions span blocks. Point lookup reads one candidate block; a cursor visits one validated block at a time and emits the latest visible row per key, including tombstones for a future merge. Failed block reads remain errors and do not advance the cursor. The existing bounded memory/disk cache holds immutable blocks and refetches corrupt disk entries.

Each build creates a data object and an index object, followed by a 152-byte reference available for later manifest publication. Data is limited to 64 MiB, with at most 1,024 blocks, a 64 KiB target and 128 KiB maximum block size. An index is limited to 8 MiB. Internal keys allow 2 KiB to accommodate database/generation prefixes around 1 KiB application keys; values remain at most 64 KiB. These are file/record bounds, not database-count or total database-size limits. Builders retain their output buffers; index metadata, active cursors, cache and concurrent builders still require aggregate admission. The table reader alone supplies no serving authority, flush/checkpoint protocol, merge, compaction or collector. [Native table verification](verification.md#native-sorted-tables).

The conditional logical-file component remains independently checked: conditions inspect accepted predecessors, including pending writes, while reads expose only the contiguous durable prefix. Failed comparisons against pending changes wait for that prefix before becoming definite conflicts. Its `DDBFILE2` format, 16,384-file/64-MiB resident namespace defaults and fixed-size external references are component bounds, not database-count limits or a selected native table layout. Whole-process caches, buffers and physical admission remain controller obligations. No physical collector is enabled for these candidate objects. [Component verification](verification.md#conditional-logical-files).

## What the provider measurements establish

The bounded experiment uses the existing Rust provider bindings, two warmup objects, 32 measured operations per phase, 1 KiB and 64 KiB values, and concurrency 1, 4 and 16. Both B2 and a versioned MinIO fixture verify all 194 objects after reconnecting and match their ordered listing. Measured data operations add zero authority reads, conditional authority replacements or read barriers. Both dedicated buckets are removed afterward.

| B2 64 KiB immutable uploads | Median | p95 | Successful objects/second |
| --- | ---: | ---: | ---: |
| Concurrency 1 | 267.9 ms | 753.8 ms | 3.57 |
| Concurrency 4 | 347.2 ms | 623.9 ms | 10.30 |
| Concurrency 16 | 387.7 ms | 1,100.5 ms | 23.67 |

These are raw object operations from the development host to B2 `us-west-002`, not database transactions or a regional production SLO. Thirty-two samples do not establish tail behavior. Larger concurrency raises throughput while increasing latency in this run. The [raw evidence](evidence/provider-floor-2026-11-09.json) retains every latency and failure count, including the loopback MinIO results; MinIO is not AWS qualification.

This demonstrates useful provider concurrency and supports investigating batched independent publication. It does not establish the throughput of a replacement database. For a lightly loaded database, an individual durable write still waits for object storage; Rust and batching cannot remove that floor. Warm reads should avoid it.

## Next implementation gate

Establish an implementable timing contract, compose the root's durable registrations and prefix catalog with transaction recovery and placement, then complete the smallest executable serving path with bounded memory and caches. Delegated permissions and journal history now have unbounded abstract composition proofs. Clock conversion, transactions and resource accounting still need composition. Use the same HTTP transaction semantics and repeat the same workload on B2 and S3-compatible storage. Measure provider latency and end-to-end overhead separately.

The first vertical run must combine warm reads without object-store control calls, independent databases, group commit, owner loss with the old process still alive, scale-down to one, total compute loss and recovery of every acknowledgement. Do not qualify throughput using a mock ownership boundary and describe it as a safe replacement. Only after this gate should the design expand to multilevel indexes, retained snapshots, compaction, physical admission, lifecycle APIs and the full production failure matrix.

## Reproduce the raw provider experiment

Use the normal kache toolchain configuration and an explicitly dedicated disposable bucket whose name starts with `durable-db-test-`. Set the existing B2 or S3 credential environment privately, then:

```sh
export DURABLE_DB_DISPOSABLE=1
export DURABLE_DB_TEST_PROVIDER=b2 # or s3
export DURABLE_DB_TEST_BUCKET=durable-db-test-UNIQUE
export DURABLE_DB_TEST_CONTEXT='Describe client placement and provider region'
export DURABLE_DB_PROVIDER_FLOOR_REPORT="$PWD/provider-floor.json"
cargo test --locked --test provider_floor -- --ignored --nocapture
```

The experiment caps immutable data PUTs at 194 and their payload at 6,391,808 bytes. Provider initialization, reads, listing, verification and caller cleanup are additional operations. It writes partial reports between phases and never logs provider error bodies or credentials. The caller must remove the dedicated bucket and all physical versions after either success or failure. The checked-in record includes the cleanup results for both actual runs.


### Historical third-party index integration

Before retirement, actual B2 and versioned MinIO ran the same SlateDB/owned-file integration. The recorded fixture wrote 1,024 keys with 1 KiB values under 16 prefixes, checked ordered scans and captured snapshots, flushed a 1,100,152-byte table to an external object, and reconstructed the index twice from selected journals. Updates and deletion survive reopening. Four discarded journal-upload completions resolve by exact GET; a malformed old manifest uploaded after selection is excluded. Application operations make zero shared-register calls.

After cold recovery populated the table-block cache, 2,000 further reads took **78.77 ms on B2** and **117.31 ms on loopback MinIO**, with zero additional index-store calls. Thirty-two independent updates admitted with concurrency 16 shared one WAL upload, becoming durable in **399.24 ms on B2** and **14.42 ms on MinIO**. These are small debug-build integration samples, not capacity claims, percentile SLOs or a production-service comparison. [Historical external-table results](evidence/external-files-2026-11-09.json) retain all batch samples, cache settings, cold read counts and resource scope. The [earlier inline-only fixture](evidence/owned-index-2026-11-09.json) used smaller values and remains historical evidence, not a like-for-like benchmark.

That evaluation stopped writers before handoff and used deterministic injected clocks. It supplied no HTTP serving API, lifecycle/authorization recovery, transaction receipt composition or production clock adapter; compaction and collection were disabled. Its source is preserved in the recorded commits. Neither those cloud fixtures nor their timings qualify the native replacement engine, production readiness or AWS.

### Bounded reads for large immutable tables

`Storage::get_range` now reads an exact nonempty slice, with an 8 MiB response limit and the full object size supplied by the durable reference. The shared B2/S3 transport checks the returned offset, total size and body length before exposing bytes; ignored Range requests, missing objects, truncated/oversized bodies and changed sizes fail. There is no whole-object fallback. The local-directory implementation seeks and reads the same bounded slice. Both use the existing object-read metric without object-name labels.

HTTP fault tests cover these checks. Actual B2 and versioned MinIO each verify five ranges of a 16 MiB immutable object, including its first/last byte, interior blocks and the full 8 MiB allowance. The first B2 fixture failed during upload, before reads; the failure is retained and unclassified. An independent fresh-bucket run passed, with no retry of the uncertain original PUT. [Evidence](evidence/storage-ranges-2026-11-09.json).

This transport primitive assumes a fresh immutable identity and correct provider bytes; length/range validation alone does not supply a content checksum. The native table reader uses it with checksummed index references and per-block digests. The conditional-file component also uses it with journaled references and full-blob digests for complete bounded reads. Future collection must protect every retained reference.
