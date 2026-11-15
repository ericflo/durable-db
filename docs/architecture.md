# Architecture

durable-db stores each database as a set of immutable objects in a bucket. Servers hold nothing that matters: any server can be killed at any moment, and any other server can rebuild every database from the bucket alone. This page describes exactly how.

## What storage must provide

Six operations: **PUT**, **GET** (whole or a byte range), **HEAD**, **LIST** by prefix, **DELETE**, and a version-aware delete used only for cleanup: it lists the versions under a prefix and removes them by name, up to a thousand with one request where the store takes such a request and one at a time where it does not. A store that has no request that lists versions keeps none, and there the keys are listed and removed. Two guarantees:

1. **Read-after-write.** Once a PUT or DELETE has returned, every GET, HEAD and LIST that starts afterwards observes it.
2. **Whole objects.** A reader sees a complete object or none.

Nothing else. No conditional writes (`If-Match`, `If-None-Match`), no transactions, no locks, no leases, no synchronized clocks. Backblaze B2 rejects conditional writes outright, which is why the protocol is built without them; Amazon S3, Cloudflare R2, MinIO and a local directory satisfy the same contract, so all of them run the same code. [Verification](verification.md#what-has-been-run-where) says which of them it has been run on.

## What is in the bucket

For a database named `app`, under `<prefix>/db/app/`:

| Key | Object | Written |
| --- | --- | --- |
| `c/<epoch>` | **Claim.** The permit of the server currently allowed to write. | Once, when a server takes ownership. Deleted to revoke it. |
| `m/<epoch>/<generation>` | **Manifest.** A small JSON record: the list of tables that make up the state, the revision they contain, and how many frames they cover. | At takeover and after every flush or merge. |
| `l/<epoch>/<number>` | **Frame.** One group of committed transactions. | Once per group commit. |
| `t/<epoch>/<id>` | **Table.** Sorted, compressed, checksummed key-value blocks with an index. | By a flush or a merge. |
| `m/~` | **Tombstone.** The database was deleted. | Once, after the deletion is final. Never removed. |

An **epoch** is `<number>-<random>`, written in fixed width so that sorting keys sorts epochs. A server picks a new epoch every time it takes ownership of a database. Because the random part is fresh, every key below an epoch is written by exactly one process. That is the reason no write ever needs to be conditional: two servers can never write the same key.

Every object is immutable. An upload may be repeated, but always with identical bytes.

## Taking ownership

A server that wants to serve a database does this, in order:

1. **List** claims and manifests, and choose an epoch number one higher than any it sees.
2. **PUT its claim**, once. If the upload's outcome is unknown, the claim is abandoned and never used.
3. **List claims.** If a higher claim exists, another server is ahead: delete the own claim and stop. Otherwise **delete every lower claim**. This is revocation.
4. **List manifests.** If the tombstone exists, the database is deleted. If a manifest of a higher epoch exists, another server is ahead: delete the own claim and stop. Otherwise take the newest manifest of the highest lower epoch.
5. **Read** that manifest, then list and read the frames that follow it, keeping the unbroken run.
6. If frames were replayed, write them into a new table. **PUT the first manifest** of the new epoch.
7. **Check the claim** (HEAD). If it still exists, start serving.

The order matters. Revocation (3) happens before reading (4, 5), so nothing the previous owner acknowledges afterwards can be missed. The final check (7) happens after the manifest is stored, so everything recovered, including transactions the previous owner stored but never acknowledged, is permanent before anyone sees it.

A takeover gives its claim back in exactly those cases: a higher claim, a higher manifest, or the tombstone. Each leaves something in the bucket that outranks this epoch for good. A takeover that stops for any other reason after step 3, such as storage failing while it reads, leaves its claim where it is, as a crash would. It may have revoked an epoch whose first manifest is still on its way to the bucket. Its claim keeps every lower epoch from serving, and makes the next server choose a number above that manifest.

**Creating** a database is a takeover that finds no manifest: the first manifest describes an empty database, and the creation is acknowledged after the claim check that follows it. A request for a database that has no manifest is answered "not found" from the listing alone, without a claim.

A server that leaves gracefully flushes, writes a manifest marked `sealed`, and deletes its own claim. The next owner then has nobody to revoke and no frames to replay.

## Committing

One task per open database orders its transactions.

1. Transactions that arrive together are validated in order against the newest state: version checks, range checks, quota, and whether the idempotency key was already used.
2. The survivors are appended to an in-memory table as not-yet-visible versions and encoded as one **frame**.
3. The frame is **PUT** to `l/<epoch>/<n>`. On Backblaze a second, identical upload goes to another storage pod at the same moment when the server has an upload URL to spare, and the first to finish wins; elsewhere a second upload starts only if the first is slow.
4. When the frame and all earlier frames are stored, the server **checks its claim** with one HEAD.
5. If the claim exists, the transactions become visible and are acknowledged.

Why this is safe: suppose server A acknowledges a transaction and server B takes over. A's claim check found the claim present, so the check began before B's revocation finished. A's frame was stored before that check began, so before B's revocation finished, so before B listed A's frames. B therefore read the frame. Conversely, if B revoked A first, A's check finds no claim and A acknowledges nothing; the client is told the outcome is unknown and retries with its idempotency key.

A refusal is held to the same standard. "Conflict" is a statement about the current state, so a server reports it only after a claim check of its own. A stale server answers "ownership moved", never "conflict".

Throughput comes from grouping: every transaction that arrives while an upload is in flight joins the next frame. Two frames may upload at once. More would not help, because acknowledgement is in order and every commit would wait for the slowest of many uploads.

## Reading

Readers never block writers. A read takes, under one short lock, the acknowledged revision, the current list of tables and the matching entries of the in-memory table. It then looks through tables from newest to oldest.

- A **strong** read also waits for a claim check that began after the read arrived. If the claim still stands, no other server can have acknowledged anything this server has not seen, so the answer is linearizable. All reads waiting at the same moment share one HEAD.
- A **session** read skips the check. It returns a consistent snapshot at the revision the client asked for or newer. If the server has not reached that revision, it checks its claim, discovers it was replaced, and the request is routed to the new owner.

## Tables, flush and merge

Writes accumulate in memory and in frames. A **flush** writes the acknowledged in-memory entries as a new table, stores a manifest that lists it, checks the claim, switches readers to the new table list, and deletes the frames the manifest covers. It runs when unflushed data reaches 8 MiB or 512 frames, after 5 seconds without a commit, and at release.

Tables are merged so that the newest tables stay small and the count stays logarithmic: the newest tables are merged whenever together they are at least half the size of the next one. Deleted keys are dropped when a merge includes the oldest table.

A value that was written over or deleted stays in its table until a merge reaches that table, and the database's size counts it until then. Merging by size bounds that only loosely, and for deletions, which are a few bytes each, not at all. So a write that the database's quota has no room for asks for a merge of everything, provided there is more than one run or there are writes that are in no table yet. It is answered `503` while the merge is made, taken when the merge has made room, and answered `507` when it has not. A merge of everything that removed less than an eighth of what it merged is not made again for four times as long as it took, and a second at least, so that a database that is full of what can still be read is not rewritten for every write. Nothing but time ends that wait: if a deletion did, one deletion and one write, over and over, would have the database rewritten for each. A write with no room is answered `507` while it lasts, and `503` if something has been deleted since the merge.

A merge writes its result as a **run**: tables of at most 32 MiB whose key ranges follow one another, each uploaded before the next is begun. A run is read, weighed and merged as the single table it would otherwise be, so a merge holds about one table in memory however large the database is. Two merges run at once on a server. A database that waits for its turn goes on flushing. A merge under way flushes between two of its tables when writes fall due, and stops there when the database is being released. The tables of a merge that did not finish are in no manifest, and are collected. A merge that failed is tried again when the database is next written to.

A table is blocks of about 32 KiB, compressed with LZ4, each with a CRC, followed by an index of the last key of each block. A lookup reads the index, then one block. Small tables arrive whole in a single request.

Two caches hold table data, both bounded and both disposable:

- **Memory** (`--cache-bytes`): decoded blocks and table indexes, shared by all databases. When it is full it keeps what has been read more than once, so a scan of everything does not displace what is in use.
- **Disk** (`--cache-dir`, optional): whole table files, written when a table is made and otherwise fetched in the background, two at a time. A damaged file fails its checksum, is deleted and is fetched again.

## Collection

Objects are deleted by the owner, and only what the newest manifest no longer needs:

- frames covered by the manifest,
- older manifests of the same epoch,
- tables replaced by a merge,
- everything left by lower epochs that the current manifest does not reference.

The rule that makes this safe is the same as for acknowledgements: delete only after a claim check that began after the manifest was stored. If the check passes, any later owner will read that manifest or a newer one, and never needs what was deleted. If the owner has been replaced, the check fails and it deletes nothing.

On a versioned bucket such as B2, collection removes every version of a key, so no lifecycle rule is needed and nothing is billed after it is collected. A key's delete markers are removed only when none of its data versions remain, across listing pages too, so a deleted key never becomes readable again wherever the cleanup stops. This matters for one kind of object: a revoked claim that reappeared would let a stale owner resume. On a bucket that keeps no versions a removed key is gone, and there is nothing more to collect.

## Deleting a database

Deleting is a change of state like any other, and is settled the same way.

1. The owner stops accepting requests and **PUTs a manifest marked `deleted`**, in its own epoch.
2. It **checks its claim**. If the claim stands, the deletion is final: every later owner will read that manifest or a newer one of the same kind.
3. Only then does it **PUT the tombstone**, and only when the tombstone is stored does it remove everything else under the database's prefix.

A server that was replaced before step 1 completed is outranked like any stale owner: its manifest belongs to a lower epoch, its check fails, and it writes no tombstone, so nothing is left behind to take effect later. A server that takes over and recovers a manifest marked `deleted` finishes the job: it writes its own first manifest, also marked, checks its claim, then writes the tombstone and removes the rest.

A deletion that fails partway has an unknown outcome, like a commit that does. Repeating the request settles it: the next owner either finds the database deleted and completes the cleanup, or finds it intact and deletes it.

## Many servers

Servers are identical. Each knows the others through one DNS name that resolves to every ready server (a Kubernetes headless Service), refreshed every second.

- A request for a database that this server has open is served here.
- Otherwise the server computes which member should hold the database (rendezvous hashing over the member list) and relays the request to it. If that member cannot be reached, the server takes ownership itself.
- When membership changes, a server hands the databases that now belong to another member over to it: flush, sealed manifest, claim deleted. At most 16 per second, so a new member is not flooded.
- On a termination signal, a server reports not-ready, hands over every database, relays requests to its peers for a few seconds while the load balancer stops sending them, and exits.

Membership is only a routing preference. Two servers with different views of the membership may both try to serve a database; the claim protocol lets exactly one of them acknowledge anything, and the other relays or retries. No Kubernetes lease, pod-local lock, clock or cache is ever relied on for correctness.

## What happens when things fail

| Event | Outcome |
| --- | --- |
| A server is killed mid-commit. | Its unacknowledged clients see a connection error and retry. Another server revokes the dead server's claim and replays its frames. A transaction whose frame was stored is applied exactly once: the retry finds its idempotency key. |
| An upload's response is lost. | The identical bytes are uploaded again. |
| A paused server resumes after being replaced. | Its claim is gone. Its next check fails; it acknowledges nothing and deletes nothing. |
| Two servers claim at the same moment. | Both write claims. The lower one sees the higher and withdraws. |
| A takeover fails after it revoked the previous owner, or with its own manifest upload in flight. | The claim is left in place, as after a crash, so no lower epoch can serve until a higher one revokes it. Deleting it would let a manifest that arrives late hide newer acknowledged writes. |
| A deletion is interrupted. | Before the claim check, the next owner decides: a manifest marked `deleted` that reached the bucket is adopted, otherwise the database is intact. After it, the tombstone or that manifest makes every server finish the cleanup. |
| An upload that was given up on arrives later. | A frame, manifest or table is the same bytes under the same key, or belongs to an epoch that has been outranked. A claim that might arrive late is never used. |
| Storage is unreachable. | Commits and strong reads fail with `503` until it returns. Nothing is acknowledged that is not in the bucket. |
| Storage refuses requests: it is asked too much, or it is failing. | A request that is refused is sent again, as one that was lost is. After eight refusals in a row of one kind of request, with none taken between them, a server sends no more of that kind: they fail in the server, and commits and strong reads fail with `503`. After a second, or after as long as the store asked for, one request is sent, and when one is taken all are sent again. Reads, listings, changes, and on Backblaze B2 uploads and the calls that name a storage pod are each counted apart. A server does not keep itself under a rate: a store that takes some of what it is sent is sent all of it. |
| Storage is slow: a request is not answered within twenty seconds. | The request is given up and sent again, as one that was lost is, and is not counted as a refusal: every request goes on being sent. What waited for it fails if the store stays slow. A purge that fails leaves what it would have removed in the bucket and ends the collection it was part of; the next collection removes it. A collection that is waiting for the store gives way to a release or a deletion of its database, and a server's housekeeping waits for no release. |
| Every server is gone. | Start one. It opens each database from the bucket on first use. |
| A cache file is damaged. | Its checksum fails; it is deleted and fetched again. |
| A task inside a server panics. | That is a defect, and it is kept to what the task was for. A database's task closes that database, which is opened again from the bucket by the next request. A round of the server's housekeeping is lost and the next one runs a second later. A request is not answered. Each is in the log as an error or a panic. |
| A round of a server's housekeeping does not end. | None is known to since 2.1.6, in which a round no longer waits for storage. It is not ended, since what it holds is not known, and the log says every thirty seconds how long it has run. Until it ends the server follows no change of its peers, hands no database over and tends none. |

## Limits of this design

- **One writer per database at a time.** A database's write throughput comes from grouping transactions into frames, not from spreading it over servers. Spread load by using more databases.
- **A commit costs one upload and one HEAD.** That is the floor: about 80 ms on B2 from a distance, a few milliseconds on a local disk.
- **A strong read costs one HEAD**, shared among concurrent reads. Session reads cost none.
- **Transactions do not span databases.**
- **A deleted database's name cannot be reused.** The tombstone is permanent, so that a late upload from a server that owned the old database can never be mistaken for the new one.
- **Merging the oldest table rewrites the database**, each time it grows by half. Memory does not grow with the database, but the uploads do. The default quota is 1 GiB; larger databases have not been measured.
- **No historical reads.** Only the current state is kept.
