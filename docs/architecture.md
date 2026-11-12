# Architecture

Durable DB serves local directories and cloud buckets through the native journal, sorted-table index and checkpoint engine. In cloud mode, the bucket is authoritative and API processes are disposable. One process can own every partition; adding processes moves ownership while database mapping stays fixed. The service is experimental: the [production gates](production-readiness.md) still require broader correctness, performance and operational qualification.

## Placement and authority

An immutable layout maps database names to fixed partitions with domain-separated SHA-256. Partition count is not a database-count limit. A shared object-backed B+tree stores partition control records; its root fits in B2 bucket metadata or an S3 control object. The same publication algorithm runs on both providers. B2 supplies a conditional bucket revision; S3 supplies an object version comparison.

Kubernetes supplies peer addresses through a headless Service, including starting pods. Rendezvous ranking chooses a preferred owner for each partition. DNS snapshots can be stale or disagree: they schedule acquisition but grant no serving permission. Each acquisition uses a fresh incarnation, registers it durably, conditionally claims its partition, revokes unresolved predecessor permits and recovers the selected journal history before serving.

A permit is a write-once positive object. A successor permanently revokes predecessor permits before choosing their durable journal prefixes. Remote responses are sealed before a fresh, uncached permit GET. There is no lease, bounded-clock assumption or bucket-wide conditional write on the ordinary read path. A fresh object GET still has provider latency; this is not an in-memory latency claim. [Protocol details and assumptions](design-owned-partitions.md#revocable-writer-permits).

Requests arriving elsewhere are forwarded once to an advertised owner incarnation. The receiving API pins that backend through authorization and dispatch; address reuse cannot redirect a request to an unrelated incarnation. Tenant claims and policy checks remain intact. Unknown outcomes return to the caller for an identical retry, without hidden forwarding replay.

## Commit protocol

A transaction operates within one database and names the revision it read. Admission validates its scope, logical quotas, receipt identity and resource allowances before accepting a new revision. An immutable journal upload can group transactions; only a contiguous durable prefix becomes readable. Acknowledgement also requires current serving permission. A rejected or uncertain upload cannot create a successful receipt by observation alone.

An uncertain journal PUT is resolved by an exact GET. If its frame is present,
normal confirmation can continue; if absent, the worker retires so the cluster
can reacquire, revoke and select the old journal prefix. It never repeats the
uncertain physical PUT or treats absence as proof of abort. A late old frame
outside the selected prefix cannot enter recovered state. Local mode reports
unavailability and requires reopening its directory after this failure.

Receipts bind the original request body and return its original result on an identical retry. A conflicting body reusing the ID is refused. The cloud CLI retains a 128-commit retry window; callers must resolve uncertainty within that window. Transactions spanning databases are not supported.

Durable upload allowances cover immutable PUT attempts, including unsuccessful or uncertain attempts. The namespace ceiling covers data and metadata; tenant admission and maintenance reserves supply additional isolation. Volatile unused rights are stranded on process loss rather than recreated from observations. The namespace administrator can inspect and refill its ceiling before partition owners become ready. Full administrative/egress accounting remains a production gate.

## Index, checkpoints and memory

A bounded recent projection overlays an immutable checkpoint. Shared sorted tables contain values and receipts from multiple database generations; a bounded manifest routes point lookups and ordered scans. Checkpoint publication binds the image to an exact selected journal position. Recovery opens that image lazily and replays its remaining selected suffix before exposing an owner.

Authenticated immutable blocks share a bounded memory cache and optional disposable disk cache. Mutable authority and permit validation are not cache hits. Corrupt cached bytes are misses; missing or corrupt authoritative objects are errors.

Aggregate index/validation counts and payload allowances, and journal bytes, are divided across the fixed layout. A resident-owner reservation follows private recovery, requests, worker state and spawned I/O through retirement. Removing an owner from the routing map does not free its reservation. This bounds those components even during repeated handoffs. Cache, checkpoint scratch, HTTP/provider buffers and allocator overhead remain separate; the configured payload allowance is not a whole-process RSS ceiling.

## Collection without leases or clock assumptions

Each native worker collects its own partition. The peer preferred for partition zero schedules shared-directory cleanup once per interval, independently of readiness; disagreement can run concurrent collectors safely. Checkpoints let collectors reclaim covered journal frames and obsolete tables/manifests while retaining live checkpoint references and explicit retained images. B2 deletion removes physical file versions, rather than leaving billable hidden versions.

Collection lists candidates before obtaining its publication fence and deletes only listed, unreachable objects. Failed or ambiguous fences grant no deletion authority. A paused publisher must lose the relevant comparison; a restarted collector begins a fresh attempt. External lifecycle/retention rules must neither delete live objects nor prevent required reclamation. Registration/catalog-header reclamation and complete growth qualification remain open.

## Provider and local boundaries

Cloud providers must supply durable immutable uploads, fresh permit reads after revocation and the documented conditional control operations. A startup probe checks stale-write exclusion; it cannot establish the complete provider contract. Raw partition-control observations may be stale and are only hints; claims and activation require exact comparisons. Directory operations that need a fresh snapshot, including public upload-budget status, retain the shared conditional barrier. Ordinary database reads validate their writer permit instead.

Conditional mutations and immutable uploads have one physical attempt per invocation. Redirects and hidden transport retries are disabled so a committed update with a lost reply cannot become a reported definite conflict. Read-only provider observations retain bounded retries. [B2 binding and qualification](b2.md).

Local mode holds a lifetime OS lock over one directory and uses fsync plus atomic publication. Blocking mutations retain that lock even if their async waiter is canceled. It needs no ownership clock or remote permit GET. Shared/network filesystems and cloning live process memory are unsupported. [Format compatibility and migration](recovery.md#local-format-upgrades).

The [verification record](verification.md) distinguishes bounded models, abstract proofs, implementation tests and deployment evidence. These components are not yet a complete composed proof of Rust or every deployment.
