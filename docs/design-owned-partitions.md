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

`RootControl` now checks repeated renewal and takeover separately from clock conversion. An exact conditional root renewal must still belong to the current epoch; a delayed reply is anchored to its request start. A successor starts draining after observing its fence, renews its own grant while draining, and conditionally activates before delegating. Losing a response never manufactures permission. One instance checks three successive epochs; another checks repeated root and child grants during a takeover. Required reachability traces include all three owners activating and actual extensions of both a serving root's and its child's deadlines. An unbounded abstract proof now establishes root exclusion and child containment/exclusion for arbitrary finite epoch counts and arbitrary renewals; all 153 obligations pass, and a stale-owner renewal mutation fails. The model assumes an elapsed-time upper bound supplied by a correct clock conversion. It does not prove that conversion, assignment movement, log handoff or the runtime controller. See [scope and evidence](verification.md#candidate-root-renewal-and-repeated-takeover).

`PartitionControl` now adds bounded permission checks for independent partition movement, two successive moves and coordinator replacement during an interrupted move. A freeze stops grants for the old generation; other partitions continue receiving grants. The same coordinator waits out old permission, while a successor relies on its own completed root drain. Three unsafe variants and four reachability controls are required. Its unbounded abstract permission proof passes 486 obligations, with required mutations for assignment, drain and delayed-reply errors. This component establishes permission composition under atomic placement updates; `PartitionJournal` below composes these permissions with durable journal history. The wire implementation remains open. See [the movement scope](verification.md#candidate-delegated-partition-movement).

### Delayed control updates

A control request can finish at the provider after its caller's serving permission expires. Expiry alone cannot cancel an in-flight request. The partition models therefore allow an active coordinator's freeze, move completion and completed-prefix selection to apply late, provided the authoritative coordinator identity still matches. These transitions cannot issue grants or acknowledge transactions. The permission and history proofs cover the wider transitions without changing their invariants.

Four additional coverage traces require actual late applications with old acknowledged data: freeze and move completion after root expiry, selection after root expiry, and selection after writer expiry while the root remains valid. The traces retain the original safety invariants. Run `scripts/check-candidate.sh LateControlCoverage`.

This is still an atomic application model. The wire refinement must capture exact conditional versions, local assignment views, request start, delayed application, lost replies and restart. A coordinator must stop local grant issuance before sending a freeze; it cannot undo that freeze merely because a request timed out. Its same-root drain starts after confirming the durable freeze. A later coordinator uses its own completed root drain. A selected prefix remains immutable even if its successful response is lost. These are implementation obligations, not claims that the current controller implements them.

## Transaction concurrency

Independent partitions address contention across databases. The current API also compares a whole-database `expected_revision`, so transactions on unrelated keys within one database can still conflict. Moving that API onto a faster journal does not remove this second bottleneck. The performance qualification must include both many active databases and disjoint-key writers in one database.

The replacement transaction layer should validate bounded key and range read sets against one consistent snapshot. Range checks must detect phantoms, including inserts and deletions; reading several keys must not mix snapshots. The existing whole-database revision condition retains its strict meaning for existing callers. More precise conditions require an explicit versioned API and client support, plus a serializability model before implementation. This is a design requirement, not an implemented API.

An owner can order accepted transactions within a batch and validate each against its accepted predecessors. Client-visible reads and acknowledgements must follow the contiguous durable prefix; speculative changes cannot escape through a read. Snapshot retention, expired snapshots, request receipts, quota checks and bounded conflict metadata belong in that protocol. A benchmark must report retries and fairness as well as aggregate throughput, so batching cannot hide starvation or repeated recomputation by individual clients.

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

`Journal.tla` checks this boundary with three prepared positions, arbitrary upload completion order, uncertain responses resolved through positive observations, contiguous consumption, stepwise recovery, process loss and controller restart. Its **4,134-state** instance passes six negative controls. `JournalProof.tla` proves abstract prefix durability, acknowledgement preservation and seal immutability for unbounded log lengths under the stated controller/provider assumptions; all **88 proof obligations** pass. Removing contiguous consumption makes the proof fail. The old handoff-only model has been retired. See [the component evidence](evidence/journal-2026-11-09.json) and [proof scope](verification.md#journal-prefix-safety).

The model assumes fresh per-key reads: a negative result must not conceal an upload completed before that read began. Transport errors and unresolved responses must never count as end-of-log. The provider experiments exercise successful reconnect/read/list behavior and journal replay; they cannot prove the provider's consistency contract or the absence of stale negative reads in every failure mode.

`RootJournal` now composes the root renewal/takeover actions with journal recovery for repeated epochs. The two-owner/two-batch instance checks 12,474 states; the three-owner/one-batch instance checks 2,230,369. Six broken variants fail, and a separate trace reaches a third serving owner retaining both predecessors' acknowledged prefixes. Catalog selection advances the same authority revision as renewal. Its unbounded abstract composition theorem now passes all 1,047 obligations for arbitrary finite epoch counts and unbounded histories, with a failing acknowledgement-permission mutation. The timing and conditional-selection contracts remain assumptions; delegated partition movement, clock conversion and the runtime controller remain outside the theorem. See [the composition scope](verification.md#candidate-repeated-root-and-journal-recovery).

`PartitionJournal` now composes those journal rules with delegated partition movement. Five bounded instances cover movement, interrupted movement, two independent histories, successive moves, and a surviving writer continuing its journal under a replacement coordinator. Seven unsafe variants and five reachability controls are mandatory. Writer generation is independent of coordinator epoch: coordinator replacement alone need not force every surviving partition owner to start a new log, while replacing an owner must allocate a fresh generation. An unbounded abstract composition proof now passes 785 obligations under the stated timing and storage contracts. It preserves acknowledged history across arbitrary repeated moves and coordinator replacements; it does not establish the runtime implementation. See [the exact scope](verification.md#candidate-delegated-journal-recovery).

The provider contract is a separate obligation from an experiment: [AWS documents strong object read-after-write consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel), while [Backblaze describes confirmed file writes as immediately visible](https://www.backblaze.com/blog/design-thinking-b2-apis-the-hidden-costs-of-s3-compatibility/). Neither statement should be generalized to fresh bucket-metadata observations. The candidate's exact negative-read and error-classification assumptions must stay explicit.

The provider control interface now stamps a fresh mutation identity on every conditional replacement, using the same code for B2 and S3. This handles B2's identical-metadata no-op behavior without requiring the future controller to remember a wire-level precaution. It does not turn raw observations into fresh permission or anchor grant expiry; those remain controller obligations. See [the contract and qualification](verification.md#common-provider-register).

## Serving authority remains the central decision

Write fencing alone does not prevent a paused old owner from answering stale reads. A peer quorum alone also does not explain how an isolated former single-node deployment is excluded after complete replacement.

The `OwnedWal` candidate combines bounded-clock serving permission with log upload, acknowledgement and recovery for one handoff. The old writer checks permission **after** upload, without an atomic global-owner check on each acknowledgement. It covers delayed grants, old-process pauses/loss, successor replacement with a different clock origin, stepwise recovery, a durable seal and one new acknowledged batch. An unfinished drain restarts on the replacement's own clock; completed drain decisions and seals are abstract durable records. Its two-position, two-time-step instance passes **570,280 states** and all seven required negative controls. See [the exact assumptions, limits and evidence](evidence/owned-wal-candidate-2026-11-09.json).

This is initial bounded composition, not a complete cluster model. The successor remains the final owner after activation. Repeated epochs, many partitions, delegation, transaction/request semantics and liveness are still outside it. In particular, the abstract hierarchy does not establish the runtime controller or justify the deployment platform's clock behavior.

The repository's current no-clock correctness rule was an implementation constraint, not a requirement in the original product brief. It remains in force for runtime code. Adopting elapsed-time authority would require a documented architecture decision, justified platform/timing assumptions, a composed model and fault tests. No clock-based runtime change is made by these experiments. No external coordinator is selected as a substitute.

Clock selection alone is not that timing contract. Linux documents that `CLOCK_MONOTONIC` stops during suspend and `CLOCK_BOOTTIME` includes suspend, making the latter a candidate for this requirement. The kernel's virtualization documentation separately describes timekeeping and migration difficulties. These sources do not establish our required rate/error bounds across every virtual machine or migration. A deployment contract and failure tests must address those cases before runtime adoption. [Linux timekeeping](https://docs.kernel.org/core-api/timekeeping.html), [KVM timekeeping](https://www.kernel.org/doc/html/latest/virt/kvm/x86/timekeeping.html).

QEMU specifically documents that a paused or saved-and-restored guest can miss the elapsed gap and need its system time corrected. That is a reason to qualify VM pauses separately from ordinary process pauses; it is not evidence that a guest clock satisfies the lease bound. Restoring a live process snapshot cannot preserve serving permission without a fresh incarnation and recovery. No such platform guarantee is assumed by the current engine. [QEMU guest time](https://www.qemu.org/docs/master/interop/qemu-ga-ref.html#command-guest-set-time).

Finite clock precision also needs a margin. The candidate arithmetic now assumes that a clock's reading over an interval of `t` reference units lies between `t - E` and `2t + E`, with nondecreasing readings; `E` includes the error of the complete elapsed-time measurement. Under that contract, a root grant of length `L` requires a successor drain of at least `2L + 3E` on its own clock. A child may receive at most `floor(max(0, R - 3E) / 2)` units from a parent's remaining interval `R`, anchored to the child's request. Zero duration permits no service. `GrantBoundsProof.tla` proves these arithmetic implications with 19 obligations and two failing margin-removal controls. It does not establish that a deployment satisfies the assumed bounds, select `E` or `L`, or prove the complete controller. See [the proof scope](verification.md#candidate-clock-error-margins).

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

Establish an implementable timing contract and refine atomic placement/prefix selection into provider requests with uncertain replies, then implement the smallest executable path with bounded memory and caches. Delegated permissions and journal history now have unbounded abstract composition proofs. Clock conversion, transactions and resource accounting still need composition. Use the same HTTP transaction semantics and repeat the same workload on B2 and S3-compatible storage. Measure provider latency and end-to-end overhead separately.

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
