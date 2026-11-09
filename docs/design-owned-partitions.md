# Owned partitions and durable log batches

**Status: candidate design, not the runtime engine.** Replace the shared per-request authority path before production. The existing engine remains the compatibility and correctness reference while this design is evaluated. The [architecture reassessment](architecture-reassessment.md) explains why a change is necessary.

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

One conditional register is still a potential control bottleneck. A candidate coordinator can batch placement and serving-authority renewal, while owners publish log batches independently. The coordination role must run in the database binary and recover from object storage. The renewal hierarchy, delegation and failover protocol are **not yet modeled or selected**. A single conditional update per partition per write would leave the important bottleneck intact.

## Immutable batches and acknowledgement

Assign each batch a fresh identity from its partition, unique writer epoch and never-reused sequence position. Encode accepted transactions, request identities, resulting database revisions and the preceding log digest. The process sends at most one PUT for that identity. An unknown PUT outcome stalls that sequence; it may be resolved by reading and checking the exact object. It must not allocate a replacement transaction identity or send different bytes under the same name.

Uploads may be pipelined, but acknowledgement advances only through a contiguous durable prefix. A completed upload at position three cannot justify acknowledging it while position two is unresolved. Before acknowledgement, the writer must also verify its serving permission. Merely having started the upload while permission was valid is insufficient. If permission expires after a PUT might have landed, the response is uncertain, not a definite conflict or rollback: recovery may still retain that batch.

This creates a possible one-immutable-PUT path per batch, with no shared authority update per transaction. It is still a proposal: request receipts, transaction evaluation, hash-chain validation, partial recovery and the ownership protocol must compose before this is a valid implementation.

## Handoff fixes an exact old prefix

A successor must:

1. Fence new grants to the old incarnation and establish that all old acknowledgement permission has ended.
2. Recover the old log's contiguous durable prefix. Deterministic epoch/sequence names permit ordered point reads; discovery need not rely on one atomic snapshot of a paginated listing.
3. Durably publish an immutable handoff decision containing that exact prefix before serving or appending a successor epoch.
4. On restart, recover the existing decision. Never extend an already accepted old prefix merely because another old upload appeared later.

A late upload beyond the sealed prefix is unacknowledged garbage. Including it opportunistically after new work has begun could change the history underneath the replacement. An old upload included before the decision may represent a pending operation with a lost response; normal request-identity recovery must resolve that uncertainty.

`WalHandoff.tla` checks this boundary with three old log positions, arbitrary upload completion order, a stepwise recovery scan, old-process loss, successor crashes/restarts and a persistent seal. Its **1,214-state** instance preserves all acknowledged positions and the chosen seal. Five negative controls detect gaps in acknowledged prefixes, recovery before the drain boundary, stale negative reads, acknowledgement after the boundary, and reopening a seal. See [the experiment record](evidence/wal-handoff-candidate-2026-11-09.json).

The model assumes fresh per-key reads: a negative result must not conceal an upload completed before that read began. Transport errors and unresolved responses must never count as end-of-log. The raw provider experiment exercises successful reconnect/read/list behavior; it cannot prove the provider's consistency contract or the absence of stale negative reads in every failure mode.

The provider contract is a separate obligation from an experiment: [AWS documents strong object read-after-write consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel), while [Backblaze describes confirmed file writes as immediately visible](https://www.backblaze.com/blog/design-thinking-b2-apis-the-hidden-costs-of-s3-compatibility/). Neither statement should be generalized to fresh bucket-metadata observations. The candidate's exact negative-read and error-classification assumptions must stay explicit.

## Serving authority remains the central decision

Write fencing alone does not prevent a paused old owner from answering stale reads. A peer quorum alone also does not explain how an isolated former single-node deployment is excluded after complete replacement.

The existing `ServingLease` experiment makes one possible tradeoff explicit: a bounded-rate elapsed clock that continues advancing during process pauses, request-start-based expiry, and a takeover drain covering every old grant. It fails if a former owner's clock can stop while the replacement proceeds. Its commit operation still assumes atomic ownership checks.

The newer `OwnedWal` candidate combines that serving boundary with log upload, acknowledgement and recovery for one handoff. The old writer checks permission **after** upload, without an atomic global-owner check on each acknowledgement. It covers delayed grants, old-process pauses/loss, successor replacement with a different clock origin, stepwise recovery, a durable seal and one new acknowledged batch. An unfinished drain restarts on the replacement's own clock; completed drain decisions and seals are abstract durable records. Its two-position, two-time-step instance passes **570,280 states** and all seven required negative controls. See [the exact assumptions, limits and evidence](evidence/owned-wal-candidate-2026-11-09.json).

This is initial bounded composition, not a complete cluster model. The successor remains the final owner after activation. Repeated epochs, many partitions, delegation, transaction/request semantics and liveness are still outside it. In particular, it does not yet establish a scalable renewal hierarchy or justify the deployment platform's clock behavior.

The repository's current no-clock correctness rule was an implementation constraint, not a requirement in the original product brief. It remains in force for runtime code. Adopting elapsed-time authority would require a documented architecture decision, justified platform/timing assumptions, a composed model and fault tests. No clock-based runtime change is made by these experiments. No external coordinator is selected as a substitute.

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

Extend the initial serving/log model to the renewal hierarchy and repeated ownership changes, state an implementable timing contract, then implement the smallest executable path with bounded memory and caches. Use the same HTTP transaction semantics and repeat the same workload on B2 and S3-compatible storage. Measure provider latency and end-to-end overhead separately.

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
