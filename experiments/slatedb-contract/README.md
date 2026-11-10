# SlateDB integration experiments

These isolated programs evaluate SlateDB 0.18.1 as the replacement value index. They share a pinned dependency lockfile and use the repository's [kache setup](../../docs/development.md). They do not change the server's Rust 1.90 toolchain or add SlateDB to its dependencies. Upstream requires Rust 1.91.1 for this evaluation because it calls `Duration::from_mins`, unavailable on 1.90.

```sh
rustup toolchain install 1.91.1 --profile minimal
```

## Snapshot contract

```sh
cargo +1.91.1 run --locked \
  --manifest-path experiments/slatedb-contract/Cargo.toml \
  --bin durable-db-slatedb-contract
```

The 30-second in-memory fixture checks stable captured snapshots, pending durability handles, 2,000 warm reads without object calls, table flush and reopen. It records conditional WAL/manifest operations. Startup call counts can vary with background scheduling. Keep captured snapshots private until their corresponding durability handles complete and current ownership permits a response.

## Owned journals on B2 and S3

The `owned_index` binary connects SlateDB's native WAL and manifests to `owned_files`. Both providers run the same adapter and selected-journal recovery. Use a **fresh dedicated disposable bucket**: this initializes experimental root metadata, and B2's control register belongs to the bucket. Supply bucket-scoped B2 credentials through `B2_APPLICATION_KEY_ID`/`B2_APPLICATION_KEY`, or ordinary AWS environment credentials for S3. S3 needs bucket versioning. Never use an application bucket.

```sh
DURABLE_DB_DISPOSABLE=1 \
DURABLE_DB_TEST_PROVIDER=b2 \
DURABLE_DB_TEST_BUCKET=durable-db-test-your-bucket \
cargo +1.91.1 run --locked \
  --manifest-path experiments/slatedb-contract/Cargo.toml \
  --bin owned_index
```

For S3 use `DURABLE_DB_TEST_PROVIDER=s3`; a local fixture also needs `AWS_ENDPOINT`, `AWS_ALLOW_HTTP=true` and `AWS_REGION`. The program does not provision or delete buckets. The operator must remove **all object versions**, delete the dedicated bucket and revoke its temporary key afterward.

The bounded fixture:

- Writes 1,024 keys with 1 KiB values under 16 ordered prefixes in eight batches; checks stable snapshots, ordered scans and 2,000 warm reads. A table larger than 512 KiB is stored externally.
- Admits 32 independent updates with concurrency 16, then makes the group durable with one logical WAL upload. Earlier snapshots retain their original values.
- Discards every fifth accepted mutation's upload completion and resolves the uncertain outcome with an exact provider GET.
- Selects the old journal prefix, then uploads an old writer's malformed replacement manifest. Reopening must exclude it.
- Discards each old index, namespace and block cache, rebuilds from selected remote journals twice, and verifies original values, updates and deletion. After cold table reads populate the cache, another 2,000 reads must make no index-store calls.
- Checks that the application phase issues zero shared-register calls. Admission has a 512-mutation/32-MiB fixture ceiling, plus one explicitly prepared late record and bounded controller metadata. A 300-second timeout bounds execution.

Its JSON output includes all eight batch latency samples, the grouped-write duration, warm-read total and logical operation counts. These are **debug-build integration timings**, not a sustained-throughput or p99 qualification. MinIO is an S3-compatible fixture, not AWS evidence.

The adapter keeps files up to 256 KiB inline and uploads larger files as immutable objects, up to the core 64 MiB limit and the fixture's smaller aggregate upload budget. Successful uploads produce 56-byte references published through the journal. Range responses are capped at 8 MiB; larger individual read requests fail. An 8 MiB weighted Moka block cache is recreated on each reopen. These settings do not establish whole-process RSS bounds. The `DDBFILE2` record rejects the earlier inline-only encoding; use fresh buckets.

The adapter has no multipart/copy/delimited listing or persisted modification times, and explicitly disables compaction and garbage collection. Unsupported semantics return errors. It stops writers before handoff and uses deterministic injected clocks; it does not qualify live stale-owner fencing, physical timing, Kubernetes, tenant isolation, request receipts or a serving API. Checkpoints, aggregate admission, the complete serving path and safe collection remain production requirements. See the [design and evidence](../../docs/design-owned-partitions.md#reusing-an-existing-value-index).

The adapter regression test cancels an accepted create's caller while its upload is paused. A conflicting create must wait for durability before returning `AlreadyExists`; a range read and ETag check then verify the committed bytes.

```sh
cargo +1.91.1 test --locked \
  --manifest-path experiments/slatedb-contract/Cargo.toml --bin owned_index
```
