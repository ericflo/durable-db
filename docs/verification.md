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
| `tests/chaos.rs` | Three nodes share a store that delays every request, drops 4%, applies another 4% while reporting failure, and applies 3% up to 1.5 s after reporting failure. Its purges remove one key at a time and can stop partway. Nodes crash without releasing, are released at random, and stale owners keep running. Checked: every acknowledged append is present at its revision; a compare-and-set counter equals the acknowledged increments; no linearizable read returns a revision older than a prior acknowledgement; a released database leaves no claim and no log. Then six databases are deleted under the same faults, by a node that was cut off and replaced, by nodes that stop after a few writes, and by repeated requests; no write is acknowledged once any client was told the database is gone, and only the tombstone remains. | 200 seeds pass (`CHAOS_SEEDS=200`, release build, 105 s), and pass again in sixteen simultaneous runs each held to 40% of one CPU (`systemd-run --user --scope -p CPUQuota=40%`). Three run on every `cargo test`. A seed that has not finished after five minutes is reported by number and the run is stopped. |
| `tests/interleavings.rs` | Chosen schedules, built by holding or failing single storage requests: a takeover that fails after revoking, a takeover during a deletion, an unconfirmed deletion, an interrupted cleanup, a lookup that overlaps a merge, a refusal that overlaps an uncommitted write, a database that was never created, deletions while a flush is stuck, a flush that falls due during a merge, a database waiting for its turn to merge, a release during a merge, a merge that fails, an empty disk cache read all at once, a spare upload that the store declines. | 18 pass. |
| `tests/engine.rs` | Transactions, checks, retries, crash recovery, takeover, stale owners, flush, merge, collection, quota, deletion, disk-cache corruption, configuration changes made at once, a read that runs out of room, merges into tables of limited size, more table index than the cache holds, the keys many databases remember. | 17 pass. |
| `tests/storage.rs` | The S3 binding against a stand-in that keeps versions, pages its listings and verifies every signature: awkward keys, status handling, listings cut short, purges across page boundaries and interrupted. The directory store against the in-memory one. One order of directory names on all three. | 12 pass. |
| `tests/http.rs` | The documented API against a real server process, including a killed server. | 8 pass. |
| `tests/tenants.rs` | What one caller can do to the others, against servers whose memory budgets can be read: stalled bodies with and without a token, a database that fills its part of a budget, results that wait to be read, a caller that stops reading, results relayed by another server, requests of very many items, paths a client would rewrite, a forged relay header, silent connections at the connection limit, readiness probes in bulk, shutdown with a request in flight. | 14 pass, and each alone on 15% of a processor. |
| `tests/cluster.rs` | Up to three server processes on one store: routing to one owner, a killed owner, a graceful handover, a joining node, a contended counter while nodes are killed and restarted. | 5 pass. |
| `tests/bucket.rs` | On a real bucket: the storage contract including versions, delete markers and the order of directory names; read-after-write (8 workers × 12 rounds, each PUT and each DELETE followed at once by HEAD, GET and LIST); a purge of 1,041 versions across two listing pages while the deleted key that straddles them is polled; the engine with a takeover, a release and a deletion. | 4 pass on Backblaze B2, with native uploads and with PutObject only. A request that fails on its way is sent again, as the engine sends it (finding 29). |
| `durable-db bench --workload ledger` | Transfers between accounts, each with a log entry in the same transaction; afterwards the balances must sum to the starting total and the log must hold exactly the acknowledged transfers. | 11,774 transfers verified on three servers sharing a directory, and 4,902 on three servers sharing a B2 bucket, each while one server was killed and one terminated. |
| `scripts/soak-memory.sh` | One server with the flags of the Kubernetes manifest on a local directory, in a scope limited to 1 GiB without swap and four CPUs. Three loads of a minute each with values that do not compress: half commits of 16 KiB over 4 databases, commits over 256 databases, strong reads from 512 clients. The server is killed if it passes the limit. | 621 MiB at most with 2.1.1; 618 and 659 MiB with 2.1.0. No request failed; 0.4% of the first load's requests were told to wait, and 1.7% with 2.1.0. |
| `scripts/test-kubernetes.sh` | The shipped manifests on a three-node kind cluster backed by Backblaze B2. The ledger runs through the Service while the autoscaler adds pods under CPU load, a pod is force-deleted, and the autoscaler removes pods. | Three runs with 2.1.1 and one more with 2.1.0, each scaled 2 → 5 → 2 with a pod force-deleted: 22,756, 23,567 and 20,678 transfers in 240 s verified with 2.1.1, 18,250 with 2.1.0. Of 9 to 11 million session reads sent meanwhile without retries, 0 to 110 failed and 0 to 47 were told to wait. The reads were a sixth to a quarter fewer with 2.1.1 than with 2.1.0, for a reason not established ([performance](performance.md#on-kubernetes)). |
| `scripts/test-upgrade.sh` | Three servers of one release on one directory, replaced by the next one at a time while the ledger runs through all three; then one goes back to the old release and forward again, so that each version takes over databases the other wrote. | From 2.0.0 to 2.1.0: 171,474 transfers verified. From 2.1.0 to 2.1.1: 152,215. |

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

The same review of the HTTP and tenant boundary confirmed the following, also fixed, each with a test in `tests/tenants.rs`, `tests/engine.rs` or `tests/interleavings.rs`. Undoing any one of the fixes makes its test fail; that was checked for each.

10. **Memory was committed to callers who had not shown a token.** A request's body was budgeted and read before its token was checked, a request with no body was charged for the largest one, and nothing limited how long a body, a request head or an unread response could take. A few hundred connections that sent nothing could hold the whole budget and refuse every other caller. The token is now checked against the database in the path first; bodies and results are reserved for exactly, with a quarter of each budget per database; and every phase of a connection has a time limit.
11. **A read was limited after it had been put together.** One request could name thousands of scans over deleted keys still in memory, or thousands of large values, and have all of it gathered before being told it was too much. A request's arrays were likewise parsed in full before being counted. Both are now counted as they grow.
12. **A response nobody read stayed in memory** for as long as the connection did. It is now part of the result budget until it is written, and the connection is closed when the client takes nothing for 30 seconds.
13. **A stored value kept its whole request alive.** A small value sent in a large body pinned that body in memory until the next flush, uncounted. Values are now copied out of the request.
14. **Deletions were exempt from back-pressure**, so a database could fill memory with them while a flush was behind.
15. **Two configuration changes made at once could lose one**, for instance a token revocation. Changes are now applied in the writer, to the configuration as it stands.
16. **A relayed path could be read differently by the next server**, because the HTTP client normalizes `.` and `..` segments and backslashes. Access was still checked there, so no data crossed tenants; such paths are now refused by every server. **Any caller could mark a request as relayed**, which made a server take over a database the caller had a token for. The mark is now a proof made with the signing key.
17. Logs named databases. They now carry a tag derived from the name. A token lifetime near the largest number wrapped around. `/readyz`, which anyone may call, asked storage on every call.

The engine was then read for whatever it holds in memory that no flag limited, so that the memory a server needs could be stated. Found and fixed, each with a test that fails when the fix is undone:

18. **A merge built its whole result in memory**, and so needed as much memory as the database it merged. A merge now writes tables of 32 MiB, one at a time.
19. **Every table kept its index for as long as its database was open.** An index can be as large as its table. Indexes are now held by the cache, within `--cache-bytes`.
20. **The keys remembered for range checks were limited in number only**: 100,000 keys of the largest size are 112 MiB, for each open database that wrote that many. They are now limited in bytes, for a database and for the server.
21. **An empty disk cache fetched every table that was read, whole, all at once**: after a restart, as many tables in memory as were being read. Two are fetched at a time.
22. **A database waiting for its turn to merge stopped flushing**, so two long merges of other databases could make it refuse writes. **A merge held up its own database's flushes and its release** until it was done. A waiting database now goes on flushing, and a merge gives way between two of the tables it writes.

A server from before these changes ignores what a manifest says about runs and reads each table on its own, which gives the same answers.

23. The first version of the fix for 22 asked for a new turn as soon as a merge failed, and was given it at once: **a merge that could not reach storage was repeated without pause.** The 200 chaos seeds passed all the same. One run made while the machine was busy with other work did not end, and that was the only sign. It was reproduced by holding the test to a fraction of a CPU, found in a core dump, and fixed: a failed merge now waits for the database's next write. Two things follow. A chaos seed does not fix the whole schedule, because the engine reads the wall clock for its storage deadline and its idle flush while the test's storage runs on a simulated clock; a schedule can therefore depend on how fast the machine is. And a schedule that did not end used to look like a slow run; it is now stopped and named.

The limits were then tested instead of added up: one server with the flags of the Kubernetes manifest, held to 1 GiB, under three loads (`scripts/soak-memory.sh`).

24. **The server was killed at its memory limit although it kept every budget.** The build under test was linked with glibc, whose allocator keeps freed memory in an arena for each thread: 1,481 MiB under a load that needed 446 MiB. The static binary stayed inside the limit and showed the opposite fault: **the read figures published with release 2.0.0 could not be reproduced with the binary that release shipped.** They had been measured with a glibc build. The static binary is linked with musl, whose allocator takes one lock for all threads, and read a tenth as fast: 21,376 strong reads a second on a local directory where 194,523 were published. The binary now carries its own allocator, so that neither speed nor memory depends on how it is linked. mimalloc was tried first and was killed at the limit; jemalloc with eight arenas, returning freed memory within a second, stays at 618 to 659 MiB and reads as fast as the glibc build did. Every figure in [performance](performance.md) was measured again with the static binary, and that page says so.
25. **The load generator misreported back-pressure.** It gave up at the first `503` while loading its keys and counted a `503` during a run as an error. It now waits and tries again while loading, and reports refusals apart from failures. Its values were one repeated letter, which compresses to almost nothing, so a load of large values wrote small tables; `--random-values` writes text that does not compress.

26. **Two tests of the HTTP boundary passed where they were written and failed on the build machines**, whose processors run this suite about 3.5 times slower. Each held an answer of 24 MiB that nobody read and expected the server to keep it while the test did more work, against a server set to give up on such a caller after 0.4 or 1 second; on a slower processor the work took longer than that. So the commit of release 2.1.0 was never built by CI, and the website showed the documentation of 2.0.0 until this was fixed; the release's binary was built from that commit by the same `Dockerfile` on the development machine, where the tests passed. The failure was reproduced by giving one test 28% of a processor. The tests that look at what is held now use a server that never gives up, giving up has a test of its own in which nothing else is going on, and a wait in that file allows a minute. `scripts/test-slow.sh` runs every test alone on a fraction of a processor.

27. **On Backblaze B2 every commit sent one of its two uploads to a storage pod the server had never used**, and each further upload of a slow object went to another. A new pod costs a call for its upload URL, a name lookup and a TLS connection. One database commits about ten times a second, and ten new pods a second went unnoticed. 64 databases committing at once asked for 160 a second: measured from the desktop these figures come from, which is on Wi-Fi, one such upload in fourteen then failed after waiting five seconds for its connection and the others took 0.8 s at the median, while uploads to pods already in use took 76 ms. The pods the server kept were only those within 2.5 times the fastest, so there were never as many as there were uploads under way, and the uploads started because another was slow went to new pods as well. That is the slow row of release 2.1.0 and of 2.0.0, which the notes of 2.1.0 left unexplained. Of ten new pods tried one at a time, four stored 4 KiB in under 100 ms and six took 160 ms to a second, on a connection already open as much as on a new one.

    Since 2.1.1 the upload that must be sent takes the fastest pod known. A duplicate is the store's to decline (`put_spare` in `src/store/mod.rs`): on B2 it is sent on a known pod that first uploads are not expected to need, or on a new pod while fewer than eight are on trial, and otherwise not at all; one that is not sent has stored nothing and is asked for again. Pods are kept in order of speed unless they are slower than a new pod usually is. A test holds the first upload while the store declines every spare and checks that nothing is acknowledged, that the spares are sent once the store has room, and that a first upload that fails is sent again; unit tests check the pool's rules. Eight faults put into this one at a time each fail a test. [Performance](performance.md#backblaze-b2) has the measurements before and after.

28. **Two tests could be given the same port for their servers.** The HTTP and cluster tests asked the system for a free port, closed it, and started a server process on it; the system may give the same port to the next caller. It did once, while the binary of 2.1.1 was being built: one server could not listen, and its test sent its requests to another test's server, which answered 401. Test servers now take ports below the range the system picks from, each given out once in a process, and a server that stops while it starts fails its test by saying so. The two suites then ran twelve times each without a failure.

29. **The slowest commits in the Backblaze tables were stalls of the measuring desktop's own link.** The desktop is on Wi-Fi, and a ping to its router had replies delayed by up to 2.4 s about once a minute. A build that logged every request showed all connections stalling together in those moments: four uploads to four pods and a name lookup in one, a claim check and an unrelated upload in another. Of twelve runs made with the ping alongside, the one with a stall in the ping is the one with a 99th percentile of 1.6 s. A figure published with 2.0.0 and 2.1.0 for what racing uploads gain (a 99th percentile of 2.2 s without, 0.45 s with) was measured before this was known and is withdrawn; [performance](performance.md#backblaze-b2) gives what was measured since, with the releases run in turn.

    The same link failed two tests of `tests/bucket.rs`, once each: a purge of a thousand requests, and three reads of the read-after-write test in one moment, each with a request that could not be sent. Those tests unwrapped every request. A request that fails says nothing about what the bucket stores, so they now send it again as the engine does, and a read sent again still starts after the write it follows returned. The purge that was interrupted is purged again, and the deleted key must stay absent through both.

30. **A test of flushing and merging expected what a fast machine does.** `flushes_merges_and_collects` unwrapped 1,300 commits to a small database, waited 200 ms, and expected four tables at most. A database refuses a write while more than four flushes' worth is waiting, and a merge takes as long as the machine makes it: of 40 runs on a processor held to 40% of one core, three failed on a refused write and one on a fifth table. The fifth table is not only a matter of time. With the test waiting for the flush instead, five tables were still at rest in two runs of 160, which the merge rule allows: runs are merged from the newest while they are together at least half the size of the next, and up to twelve may remain. The pipeline of the commit that carried release 2.1.1 failed in its test step. Its log was not read, so which test failed there is not known; this is the one that failed when the suite was run ten times on a slowed processor, and `scripts/test-slow.sh`, which runs each test once, had passed it. The test now sends a refused write again, waits for the flush instead of for a time, and expects the rule's own bound. It then passed 150 runs at 40%.

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
- The memory soak for hours rather than minutes, and on a bucket, where an upload stays in memory for as long as the bucket takes to store it.
- Model conditional transactions with idempotent retries, and a delete request that arrives late.
- Run the deletion model with two transactions (`spec/DeepDeletion.cfg`) to completion.
- Check a liveness property: with one correct server and working storage, a request is eventually answered.
- Make the chaos test repeatable from its seed alone, by giving the engine the simulated clock for its deadlines, and run it routinely under CPU pressure as well as without.
- The rule for choosing storage pods on Backblaze B2 was measured from one desktop on Wi-Fi, for minutes at a time. A server in a data centre, and hours of load, have not been measured.
- A review by people outside the project. The reviews recorded above were automated and commissioned by the project itself.
- Behaviour when the bucket itself misbehaves beyond the assumptions (lost objects, stale listings) is undefined; block checksums detect corrupted table data only, and a manifest carries no checksum of its own.
