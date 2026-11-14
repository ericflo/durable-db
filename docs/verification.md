# Verification

This page records what is claimed, what has been checked, how, and what has not. Bounded model checking is evidence about a model, not a proof of the Rust implementation or of every deployment.

## What is claimed

For one database, under the storage assumptions below:

1. **Durability and one history.** Every acknowledged transaction, together with the exact sequence of transactions before it, can be reconstructed from the bucket alone. All acknowledged histories are therefore prefixes of a single sequence: nothing acknowledged is lost, reordered or forked, whatever servers crash, pause, race or are replaced.
2. **Linearizable reads.** A `strong` read reflects every transaction acknowledged before the read began.
3. **Safe collection.** No server deletes an object that the current owner, or any later owner, needs.
4. **Final deletion.** Once any server has concluded that a database is deleted, no server acknowledges anything for it again. A deletion that did not complete leaves the database either intact or deleted, never in between.

## Assumptions

- **Storage** provides read-after-write consistency for PUT, DELETE, GET, HEAD and LIST, and never exposes a partial object. It may be arbitrarily slow, fail any request, and lose any response. A listing reports each key as it was at some moment during the listing, in key order.
- **Servers** stop without warning, pause for any length of time and resume, and run with arbitrary relative speeds. They do not run corrupted code or forge each other's objects: all servers of a deployment are trusted with the bucket.
- **Uploads** a server has sent may arrive late, more than once, and after the server has crashed.
- **Clocks** are not assumed to agree, or to be correct at all. Token expiry is the only place a clock is read, and it affects authorization only.
- **Membership** may be wrong or disagree between servers.

## The model

[`spec/DurableDb.tla`](../spec/DurableDb.tla) specifies creating a database, taking ownership, committing, linearizable reads, checkpointing, collection and deletion for one database, as atomic steps on a store of claims, manifests, frames and the tombstone. The store starts empty. Writers crash at any step. Any upload ever sent may arrive at any later time, any number of times, including after its object was collected.

Four invariants are checked in every reachable state:

| Invariant | Meaning |
| --- | --- |
| `Durable` | Every acknowledged history is a prefix of what a reader reconstructs from the store, unless the database was deleted. |
| `FreshReads` | No checked read missed a transaction acknowledged before it began. |
| `OwnerIntact` | The newest checked manifest of a writer whose claim still stands has not been deleted. |
| `Final` | Once a writer has concluded that the database is deleted, no writer is serving with a claim that stands. |

```sh
scripts/check-model.sh        # about three minutes; capped at 2 GiB heap, four workers and ten minutes a configuration
```

**Results** (TLC 2.19, 2026-11-14), each an exhaustive search with no violation:

| Configuration | Bounds | States generated | Distinct states | Depth |
| --- | --- | --- | --- | --- |
| `DurableDb` | 2 writers, epoch numbers up to 2, 2 transactions, no deletion | 27,663,022 | 3,585,296 | 61 |
| `Deletion` | 2 writers, epoch numbers up to 2, 1 transaction, deletion at any time | 23,717,105 | 4,110,075 | 67 |

The two configurations exist because deletion with two transactions is a much larger state space. `spec/DeepDeletion.cfg` is that combination; it is not part of the default run and has not been run to completion: one run was stopped after 24 minutes and 92 million distinct states with no violation found, which is not an exhaustive result.

The epoch number a writer chooses is unconstrained in the model (any unused number), so safety does not depend on the hint the implementation uses.

### Negative controls

Each protocol rule is a switch in the model. Turning one off must produce a counterexample; if it did not, the model would not be testing that rule. All eleven are refuted:

| Rule turned off | Configuration | Violated |
| --- | --- | --- |
| Acknowledge only after a claim check that began after the frame was stored | `UnsafeAckWithoutCheck` | `Durable` |
| Withdraw when a higher claim is listed | `UnsafeIgnoreHigherClaim` | `OwnerIntact` |
| Withdraw when a higher epoch already has a manifest | `UnsafeIgnoreHigherManifest` | `Durable` |
| Delete lower claims before reading manifests and frames | `UnsafeReadBeforeRevoke` | `Durable` |
| Collect only after a claim check that began after the manifest was stored | `UnsafeCollectWithoutCheck` | `Durable` |
| Never use a claim whose upload may still be in flight | `UnsafeRetriedClaim` | `OwnerIntact` |
| Never delete the own claim while an own manifest upload may still arrive | `UnsafeReleaseWhileSending` | `Durable` |
| After revoking, give the claim back only when a higher epoch outranks this one | `UnsafeGiveBackAfterRevoke` | `Durable` |
| Expose recovered state only after a claim check that follows the first manifest | `UnsafeServeBeforeCheck` | `Durable` |
| Write the tombstone only after a claim check that began after a manifest marked deleted was stored | `UnsafeTombstoneWithoutCheck` | `Final` |
| Remove a deleted database's objects only once the tombstone is stored | `UnsafePurgeBeforeTombstone` | `Final` |

### What the model leaves out

- **Tables** are folded into the manifest's state. Their collection follows the same rule as superseded manifests, which is modelled.
- **Transactions are unconditional.** Version checks, range checks and idempotency keys are not modelled. A refusal is treated by the implementation as a read: it is reported only after a claim check of its own.
- **Deletes are atomic steps.** A delete request that takes effect long after its sender gave up is not modelled. The argument that it is harmless is informal: a delete is sent only for an object the sender was entitled to remove for good, and the only object whose reappearance would matter, a claim, is uploaded once. The chaos test does inject such deletes.
- **Only safety** is checked. That some server eventually makes progress is not.
- **The bounds are small.** Two writers, two epochs and one or two transactions cover every interleaving of one takeover with commits, a flush, collection, a deletion and late uploads, not longer chains.
- **Nothing connects the model to the Rust code** except that the code is written to follow it step for step and the tests below.

## Tests of the implementation

| Test | What it does | Result |
| --- | --- | --- |
| `tests/chaos.rs` | Three nodes share a store that delays every request, drops 4%, applies another 4% while reporting failure, and applies 3% up to 1.5 s after reporting failure. Its purges remove one key at a time and can stop partway. Nodes crash without releasing, are released at random, and stale owners keep running. Checked: every acknowledged append is present at its revision; a compare-and-set counter equals the acknowledged increments; no linearizable read returns a revision older than a prior acknowledgement; a released database leaves no claim and no log. Then six databases are deleted under the same faults, by a node that was cut off and replaced, by nodes that stop after a few writes, and by repeated requests; no write is acknowledged once any client was told the database is gone, and only the tombstone remains. | 200 seeds pass (`CHAOS_SEEDS=200`, release build, 124 s). Three run on every `cargo test`. |
| `tests/interleavings.rs` | Chosen schedules, built by holding or failing single storage requests: a takeover that fails after revoking, a takeover during a deletion, an unconfirmed deletion, an interrupted cleanup, a lookup that overlaps a merge, a refusal that overlaps an uncommitted write, a database that was never created. | 11 pass. |
| `tests/engine.rs` | Transactions, checks, retries, crash recovery, takeover, stale owners, flush, merge, collection, quota, deletion, disk-cache corruption. | 12 pass. |
| `tests/storage.rs` | The S3 binding against a stand-in that keeps versions, pages its listings and verifies every signature: awkward keys, status handling, listings cut short, purges across page boundaries and interrupted. The directory store against the in-memory one. One order of directory names on all three. | 12 pass. |
| `tests/http.rs` | The documented API against a real server process, including a killed server. | 8 pass. |
| `tests/cluster.rs` | Up to three server processes on one store: routing to one owner, a killed owner, a graceful handover, a joining node, a contended counter while nodes are killed and restarted. | 5 pass. |
| `tests/bucket.rs` | On a real bucket: the storage contract including versions, delete markers and the order of directory names; read-after-write (8 workers × 40 rounds, each PUT and each DELETE followed at once by HEAD, GET and LIST); a purge of 1,041 versions across two listing pages while the deleted key that straddles them is polled; the engine with a takeover, a release and a deletion. | 4 pass on Backblaze B2. |
| `durable-db bench --workload ledger` | Transfers between accounts, each with a log entry in the same transaction; afterwards the balances must sum to the starting total and the log must hold exactly the acknowledged transfers. | 11,774 transfers verified on three servers sharing a directory, and 4,902 on three servers sharing a B2 bucket, each while one server was killed and one terminated. |
| `scripts/test-kubernetes.sh` | The shipped manifests on a three-node kind cluster backed by Backblaze B2. The ledger runs through the Service while the autoscaler adds pods under CPU load, a pod is force-deleted, and the autoscaler removes pods. | Scaled 2 → 3 → 2; 18,838 transfers in 240 s verified; no request failed. |

### What these checks found

The chaos test found three defects in the first implementation of this protocol, each within seconds. They are listed because they show what the checks are able to catch.

1. After a failed takeover the server deleted its claim although its manifest upload could still arrive. A lower epoch could then serve, and the late manifest would hide its acknowledged writes. The model forbade this step; the implementation had not followed it. It is now the `UnsafeReleaseWhileSending` control.
2. State recovered at takeover, including transactions the previous owner stored but never acknowledged, was visible before a claim check confirmed it. Now the `UnsafeServeBeforeCheck` control.
3. A stale owner could answer "conflict" from old state, so a client concluded that a transaction it had in fact committed was not applied. Refusals now wait for a claim check.

Release 2.0.0 was then reviewed in three parts (the engine, the storage layer, and the HTTP and tenant boundary) by automated reviewers instructed to break it and to prove each finding with a failing test. What they confirmed in the engine and the storage layer, all fixed since, each with a test that fails on the old code:

4. **Deletion was outside the protocol.** The owner checked its claim and then wrote the tombstone. A server that took over between the two served the database and acknowledged writes, and the tombstone then arrived and deleted it. Deletion is now a manifest settled by a claim check that follows it, modelled, with the `UnsafeTombstoneWithoutCheck` and `UnsafePurgeBeforeTombstone` controls.
5. **A takeover that failed after revoking gave its claim back.** The revoked epoch's first manifest could still arrive. A third server could then choose a number below it, serve, and have its acknowledged writes hidden when that manifest landed. Now the `UnsafeGiveBackAfterRevoke` control.
6. **A purge could make a revoked claim readable again** on a bucket that keeps versions, when a listing page ended between the claim's delete marker and the version under it. On Backblaze B2 the old code showed the key during the purge; `tests/bucket.rs` reproduces that arrangement.
7. **A listing page that held nothing but said there was more** ended the listing, so a claim could be missed at takeover. Whether a provider sends such a page was not established; the client no longer depends on it.
8. **Two directory stores on one directory could exchange temporary files** when they shared a process id, as two containers on one volume do, and acknowledge an object holding another's bytes.
9. A lookup that overlapped the owner's own merge reported the database lost and closed it. An inverted range stopped the writer. A refusal could report a conflict with a write that was never committed. A crafted table footer panicked instead of reporting damage. Keys that are not UTF-8, accepted only by the library interface, became unreadable once flushed; they are now refused.

### What has been run where

| Storage | Unit, engine, chaos | HTTP, cluster | Bucket tests | Benchmarks | Kubernetes |
| --- | --- | --- | --- | --- | --- |
| Memory | yes | | | | |
| S3 stand-in (in process) | storage tests only | | | | |
| Local directory | yes | yes | | yes | |
| Backblaze B2 (`us-west-002`) | | | yes | yes | yes (kind) |
| Amazon S3, R2, MinIO | | | not yet | not yet | not yet |

## Remaining obligations

- Run the bucket tests and benchmarks on Amazon S3 and at least one other S3-compatible store. The code path is shared with B2 except for the upload call, and is exercised against a stand-in written from the S3 documentation, but not against those services.
- A soak of many hours under load with periodic failures, on a real multi-node cluster rather than kind.
- Model conditional transactions with idempotent retries, and a delete request that arrives late.
- Run the deletion model with two transactions (`spec/DeepDeletion.cfg`) to completion.
- Check a liveness property: with one correct server and working storage, a request is eventually answered.
- A review by people outside the project. The reviews recorded above were automated and commissioned by the project itself.
- Behaviour when the bucket itself misbehaves beyond the assumptions (lost objects, stale listings) is undefined; block checksums detect corrupted table data only, and a manifest carries no checksum of its own.
