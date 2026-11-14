# Verification

This page records what is claimed, what has been checked, how, and what has not. Bounded model checking is evidence about a model, not a proof of the Rust implementation or of every deployment.

## What is claimed

For one database, under the storage assumptions below:

1. **Durability and one history.** Every acknowledged transaction, together with the exact sequence of transactions before it, can be reconstructed from the bucket alone. All acknowledged histories are therefore prefixes of a single sequence: nothing acknowledged is lost, reordered or forked, whatever servers crash, pause, race or are replaced.
2. **Linearizable reads.** A `strong` read reflects every transaction acknowledged before the read began.
3. **Safe collection.** No server deletes an object that the current owner, or any later owner, needs.

## Assumptions

- **Storage** provides read-after-write consistency for PUT, DELETE, GET, HEAD and LIST, and never exposes a partial object. It may be arbitrarily slow, fail any request, and lose any response.
- **Servers** stop without warning, pause for any length of time and resume, and run with arbitrary relative speeds. They do not run corrupted code or forge each other's objects: all servers of a deployment are trusted with the bucket.
- **Uploads** a server has sent may arrive late, more than once, and after the server has crashed.
- **Clocks** are not assumed to agree, or to be correct at all. Token expiry is the only place a clock is read, and it affects authorization only.
- **Membership** may be wrong or disagree between servers.

## The model

[`spec/DurableDb.tla`](../spec/DurableDb.tla) specifies taking ownership, committing, linearizable reads, checkpointing and collection for one database, as atomic steps on a store of claims, manifests and frames. Writers crash at any step. Any upload ever sent may arrive at any later time, any number of times, including after its object was collected.

Three invariants are checked in every reachable state:

| Invariant | Meaning |
| --- | --- |
| `Durable` | Every acknowledged history is a prefix of what a reader reconstructs from the store. |
| `FreshReads` | No checked read missed a transaction acknowledged before it began. |
| `OwnerIntact` | The newest checked manifest of a writer whose claim still stands has not been deleted. |

```sh
scripts/check-model.sh        # about four minutes; capped at 2 GiB heap and four workers
```

**Result** (TLC 2.19, 2026-11-14): with 2 writers, epoch numbers up to 2 and 2 transactions, TLC explored the state space exhaustively: 57,978,436 states generated, 7,153,234 distinct, depth 62, no violation.

The epoch number a writer chooses is unconstrained in the model (any unused number), so safety does not depend on the hint the implementation uses.

### Negative controls

Each protocol rule is a switch in the model. Turning one off must produce a counterexample; if it did not, the model would not be testing that rule. All eight are refuted:

| Rule turned off | Configuration | Violated |
| --- | --- | --- |
| Acknowledge only after a claim check that began after the frame was stored | `UnsafeAckWithoutCheck` | `Durable` |
| Withdraw when a higher claim is listed | `UnsafeIgnoreHigherClaim` | `OwnerIntact` |
| Withdraw when a higher epoch already has a manifest | `UnsafeIgnoreHigherManifest` | `Durable` |
| Delete lower claims before reading manifests and frames | `UnsafeReadBeforeRevoke` | `Durable` |
| Collect only after a claim check that began after the manifest was stored | `UnsafeCollectWithoutCheck` | `Durable` |
| Never use a claim whose upload may still be in flight | `UnsafeRetriedClaim` | `OwnerIntact` |
| Never delete the own claim while an own manifest upload may still arrive | `UnsafeReleaseWhileSending` | `Durable` |
| Expose recovered state only after a claim check that follows the first manifest | `UnsafeServeBeforeCheck` | `Durable` |

### What the model leaves out

- **Tables** are folded into the manifest's state. Their collection follows the same rule as superseded manifests, which is modelled.
- **Transactions are unconditional.** Version checks, range checks and idempotency keys are not modelled. A refusal is treated by the implementation as a read: it is reported only after a claim check of its own.
- **Deleting a database** (the tombstone) is not modelled.
- **Only safety** is checked. That some server eventually makes progress is not.
- **The bounds are small.** Two writers, two epochs and two transactions cover every interleaving of one takeover with commits, a flush, collection and late uploads, not longer chains.
- **Nothing connects the model to the Rust code** except that the code is written to follow it step for step and the tests below.

## Tests of the implementation

| Test | What it does | Result |
| --- | --- | --- |
| `tests/chaos.rs` | Three nodes share a store that delays every request, drops 4%, and applies another 4% while reporting failure. Nodes crash without releasing, are released at random, and stale owners keep running. Checked: every acknowledged append is present at its revision; a compare-and-set counter equals the acknowledged increments; no linearizable read returns a revision older than a prior acknowledgement; a released database leaves no claim and no log. | 200 seeds pass (`CHAOS_SEEDS=200`, release build, 128 s). Three run on every `cargo test`. |
| `tests/engine.rs` | Transactions, checks, retries, crash recovery, takeover, stale owners, flush, merge, collection, quota, deletion, disk-cache corruption. | 12 pass. |
| `tests/http.rs` | The documented API against a real server process, including a killed server. | 8 pass. |
| `tests/cluster.rs` | Up to three server processes on one store: routing to one owner, a killed owner, a graceful handover, a joining node, a contended counter while nodes are killed and restarted. | 5 pass. |
| `tests/bucket.rs` | On a real bucket: the storage contract including versions and delete markers; read-after-write (8 workers × 40 rounds, each PUT and each DELETE followed at once by HEAD, GET and LIST); the engine with a takeover, a release and a deletion. | 3 pass on Backblaze B2. |
| `durable-db bench --workload ledger` | Transfers between accounts, each with a log entry in the same transaction; afterwards the balances must sum to the starting total and the log must hold exactly the acknowledged transfers. | 11,774 transfers verified on three local servers while one was killed and one terminated. |
| `scripts/test-kubernetes.sh` | The shipped manifests on a three-node kind cluster backed by Backblaze B2. The ledger runs through the Service while the autoscaler adds pods under CPU load, a pod is force-deleted, and the autoscaler removes pods. | Scaled 2 → 3 → 2; 18,838 transfers in 240 s verified; no request failed. |

### What these checks found

The chaos test found three defects in the first implementation of this protocol, each within seconds. They are listed because they show what the checks are able to catch.

1. After a failed takeover the server deleted its claim although its manifest upload could still arrive. A lower epoch could then serve, and the late manifest would hide its acknowledged writes. The model forbade this step; the implementation had not followed it. It is now the `UnsafeReleaseWhileSending` control.
2. State recovered at takeover, including transactions the previous owner stored but never acknowledged, was visible before a claim check confirmed it. Now the `UnsafeServeBeforeCheck` control.
3. A stale owner could answer "conflict" from old state, so a client concluded that a transaction it had in fact committed was not applied. Refusals now wait for a claim check.

### What has been run where

| Storage | Unit, engine, chaos | HTTP, cluster | Bucket tests | Benchmarks | Kubernetes |
| --- | --- | --- | --- | --- | --- |
| Memory | yes | | | | |
| Local directory | yes | yes | | yes | |
| Backblaze B2 (`us-west-002`) | | | yes | yes | yes (kind) |
| Amazon S3, R2, MinIO | | | not yet | not yet | not yet |

## Remaining obligations

- Run the bucket tests and benchmarks on Amazon S3 and at least one other S3-compatible store. The code path is shared with B2 except for the upload call, but it has not been exercised there.
- A soak of many hours under load with periodic failures, on a real multi-node cluster rather than kind.
- Model database deletion, and conditional transactions with idempotent retries.
- Check a liveness property: with one correct server and working storage, a request is eventually answered.
- An independent review of the protocol and of `src/db.rs`.
- Behaviour when the bucket itself misbehaves beyond the assumptions (lost objects, stale listings) is undefined; block checksums detect corrupted table data only.
