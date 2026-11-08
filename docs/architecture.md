# Architecture

## Authority and transaction boundary

One database is one transaction boundary. Each database has a logical head named `heads/<database>.json`. In cluster mode, a shared bounded directory maps that head to an immutable `manifests/<uuid>` file. It lists immutable sorted pages, logical usage, quotas, recent request receipts, a collection cursor, a user revision, and a fresh fence identifier. Pages live under `blocks/<database>.<uuid>`; the delimiter cannot occur in a database identifier.

Reads validate the authoritative directory through a conditional read barrier, then fetch the selected immutable manifest. They load only the pages needed for exact keys or ordered scans. Immutable page bytes can come from an optional disk cache or bounded memory cache and are verified with SHA-256. A fresh empty cache produces the same result. No full database hydration occurs at server startup.

The initial index is a bounded flat manifest of page ranges. It is intentionally simple to audit. A persistent multilevel index is a future format, not a claimed capability of this release.

## Commit protocol

1. Validate the directory, fetch the manifest, and retain its unique identity.
2. Resolve an existing identical request receipt before checking the expected user revision.
3. Check the revision, predicates, request bounds, and resulting logical quotas. Read and copy only touched pages; split them near 128 KiB.
4. Upload replacement pages under globally fresh names. Finish every upload before publication.
5. Recheck the database head identity, upload a fresh immutable manifest, and conditionally publish the updated directory. The manifest contains the incremented user revision and request receipt.
6. Acknowledge only after the conditional write succeeds.

The conditional directory publication is the commit's linearization point. A definite precondition failure is a conflict. A timeout or broken connection may have committed: do not delete candidate pages and do not invent a rollback. The client retries the identical request. Recent receipts recover the original result; an evicted receipt still cannot cause an identical request to reapply because its old expected revision fails. Request IDs must never be recycled for new work.

Two transactions based on the same manifest cannot both publish. Cluster reads linearize at the directory read barrier. Together with expected-revision writes this gives database-scoped optimistic serializable transactions, provided clients recompute after conflicts and use one revision throughout a read/modify/write operation.

## Collection without leases or clock assumptions

Each server periodically lists a bounded page of databases. For each database it performs bounded compaction and collection. Concurrent collectors are allowed; they may duplicate work but cannot gain authority through their pod identities.

The collector finishes listing candidate objects **before** fetching the manifest. It then conditionally replaces that manifest with the same logical state and a new fence. Only after that succeeds does it delete candidates absent from the protected manifest.

Why a fence is necessary: a paused writer might have uploaded an object that is not yet referenced. Without the fence, a collector could delete it just before the writer publishes it. The fence invalidates every writer based on the prior manifest. A later writer can carry forward only currently referenced pages or newly named uploads that were not in the completed candidate listing. Retries must allocate fresh object names; content-addressed name reuse would invalidate this argument.

Collectors can stop at any instruction. A failed deletion leaves garbage for a later sweep. The collection cursor is stored with the fence and wraps after the end of the range. Objects created after a listing are collected in a later sweep. Successful writes never rely on a collector running.

A reader that loses a page of an old snapshot restarts the **entire** read from a fresh manifest. It returns a conflict if an explicit expected revision has moved. No partial scan or missing value is returned as a successful substitute.

Compaction replaces one adjacent pair of small pages without changing the logical revision. Its publication uses the same conditional-write discipline. Neither clocks nor Kubernetes termination hooks participate in data safety.

## One publication implementation, small provider bindings

Both B2 and S3 run `publication::Published`. It owns directory initialization and limits, the read barrier, immutable manifest publication, manifest collection, and retry rules. The engine above it owns page transactions and page collection. No provider selects a separate database algorithm.

| Shared operation | B2 binding | S3 binding |
| --- | --- | --- |
| Observe directory | Native bucket info and revision | `authority.json` GET and opaque object version |
| Compare-and-publish | `b2_update_bucket(ifRevisionIs)` | `PutObject` with `If-Match` / `If-None-Match` |
| Immutable bytes | S3-compatible GET/PUT/list | S3 GET/PUT/list |
| Physical reclamation | Native list/delete file versions | S3 list/delete object versions |

The directory is serialized into one `durable-db-heads` value, with at most 32 database entries and 9,000 serialized metadata bytes. The common format respects B2's 50-byte metadata field-name ceiling without shortening database names. Its publication format is version 2; earlier unpublished development formats are rejected, not silently reinterpreted.

A raw directory observation may be stale. The common read barrier conditionally publishes unchanged head pointers with a new unique fence. Only success permits a read to use that snapshot. This avoids relying on fresh bucket listings and deliberately adds a metadata write to every manifest read. It also means metadata contention limits a bucket's performance. No read-lease optimization is claimed.

Manifest collection lists candidates, then conditionally fences the entire pointer set before deleting unreferenced manifests. The proof structure is the same as page collection, at the directory layer. S3 additionally retires obsolete physical versions of its control-register object using a list-before-barrier sequence. B2 manages bucket-metadata history internally.

Providers must implement atomic conditional publication and durable immutable uploads. The startup probe attempts a genuinely different stale update. Real B2 accepts an identical-metadata no-op with a stale revision; it rejects a conflicting replacement. Every protocol publication has fresh identities/fences, and no-op replay cannot introduce a different state.

General-purpose S3 is supported; unordered directory/Express listing semantics are not. S3 credentials need version-listing and version-deletion permissions even when the bucket is unversioned. Both versioned and unversioned MinIO buckets passed the same protocol tests. Real AWS qualification remains separate. See [B2 evidence and setup](b2.md).

Object lock and external retention rules can prevent reclamation. No external lifecycle rule or unrelated writer may delete live database files or alter the directory.

## Local mode

A lifetime OS lock admits one process per directory. Writes use a temporary file, file fsync, atomic rename, and directory fsync. The compare-and-replace lock remains held inside the blocking filesystem task even if its async waiter is cancelled. Crash-left temporary files are removed after acquiring the lifetime lock on reopen. Local mode is not supported on shared/network filesystems or across multiple servers.
