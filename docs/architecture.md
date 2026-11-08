# Architecture

## Authority and transaction boundary

One database is one transaction boundary. Each database has a small manifest at `heads/<database>.json`. It lists immutable sorted pages, logical usage, quotas, recent request receipts, a collection cursor, a user revision, and a fresh fence identifier. Pages live under `blocks/<database>.<uuid>`; the delimiter cannot occur in a database identifier.

Reads always fetch the authoritative manifest. They load only the pages needed for exact keys or ordered scans. Immutable page bytes can come from an optional disk cache or bounded memory cache and are verified with SHA-256. A fresh empty cache produces the same result. No full database hydration occurs at server startup.

The initial index is a bounded flat manifest of page ranges. It is intentionally simple to audit. A persistent multilevel index is a future format, not a claimed capability of this release.

## Commit protocol

1. Fetch the manifest and its opaque provider version/ETag.
2. Resolve an existing identical request receipt before checking the expected user revision.
3. Check the revision, predicates, request bounds, and resulting logical quotas. Read and copy only touched pages; split them near 128 KiB.
4. Upload replacement pages under globally fresh names. Finish every upload before publication.
5. Replace the manifest with `If-Match` against the version read in step 1. Increment the user revision and include the request receipt in that same write.
6. Acknowledge only after the conditional write succeeds.

The conditional manifest replacement is the commit's linearization point. A definite precondition failure is a conflict. A timeout or broken connection may have committed: do not delete candidate pages and do not invent a rollback. The client retries the identical request. Recent receipts recover the original result; an evicted receipt still cannot cause an identical request to reapply because its old expected revision fails. Request IDs must never be recycled for new work.

Two transactions based on the same manifest cannot both publish. Reads linearize at their manifest fetch. Together with expected-revision writes this gives database-scoped optimistic serializable transactions, provided clients recompute after conflicts and use one revision throughout a read/modify/write operation.

## Collection without leases or clock assumptions

Each server periodically lists a bounded page of databases. For each database it performs bounded compaction and collection. Concurrent collectors are allowed; they may duplicate work but cannot gain authority through their pod identities.

The collector finishes listing candidate objects **before** fetching the manifest. It then conditionally replaces that manifest with the same logical state and a new fence. Only after that succeeds does it delete candidates absent from the protected manifest.

Why a fence is necessary: a paused writer might have uploaded an object that is not yet referenced. Without the fence, a collector could delete it just before the writer publishes it. The fence invalidates every writer based on the prior manifest. A later writer can carry forward only currently referenced pages or newly named uploads that were not in the completed candidate listing. Retries must allocate fresh object names; content-addressed name reuse would invalidate this argument.

Collectors can stop at any instruction. A failed deletion leaves garbage for a later sweep. The collection cursor is stored with the fence and wraps after the end of the range. Objects created after a listing are collected in a later sweep. Successful writes never rely on a collector running.

A reader that loses a page of an old snapshot restarts the **entire** read from a fresh manifest. It returns a conflict if an explicit expected revision has moved. No partial scan or missing value is returned as a successful substitute.

Compaction replaces one adjacent pair of small pages without changing the logical revision. Its publication uses the same conditional-write discipline. Neither clocks nor Kubernetes termination hooks participate in data safety.

## Provider contract

Cluster mode requires fresh, strongly consistent GET, atomic conditional create and replace, immutable-object PUT, bounded paginated listing, and idempotent DELETE. General-purpose S3 buckets provide the required conditional-write primitives. Directory/Express buckets and providers with unordered listings are not supported by the initial collector.

The startup probe checks create-if-absent, a successful conditional update, rejection of a stale ETag, and fresh reads. A probe cannot prove a provider's behavior under every failure; the documented provider contract remains an assumption.

B2 uses a different authority primitive: native `b2_update_bucket` with `ifRevisionIs`, publishing pointers to immutable manifests through bucket metadata. This required backend is being implemented and modeled separately; see [B2 design](b2.md). It must not emulate CAS with a pod-local mutex. [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

Use a dedicated unversioned bucket, or configure expiration of noncurrent versions and delete markers. Ordinary DELETE on a versioned bucket does not reclaim all billed bytes. Object lock and external retention rules can also prevent reclamation. The service must not share a prefix with unrelated writers.

## Local mode

A lifetime OS lock admits one process per directory. Writes use a temporary file, file fsync, atomic rename, and directory fsync. The compare-and-replace lock remains held inside the blocking filesystem task even if its async waiter is cancelled. Crash-left temporary files are removed after acquiring the lifetime lock on reopen. Local mode is not supported on shared/network filesystems or across multiple servers.
