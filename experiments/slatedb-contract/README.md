# SlateDB snapshot contract probe

This isolated experiment evaluates SlateDB 0.18.1 as a possible value index. It is not a durable-db backend or a production dependency. It uses an in-memory object store and makes no B2, S3, autoscaling, resource-bound or throughput claim.

From the repository root, use the normal [kache setup](../../docs/development.md), then:

```sh
rustup toolchain install 1.91.1 --profile minimal
cargo +1.91.1 run --locked --manifest-path experiments/slatedb-contract/Cargo.toml
```

The upstream release uses `Duration::from_mins`, which fails to compile on durable-db's Rust 1.90. This experiment requires Rust 1.91.1; it does not change the main project's toolchain. Its lockfile is independent of the server's dependencies.

The probe checks that pending writes have stable, distinct snapshots; durability handles wait for a WAL flush; 2,000 warm snapshot reads make no additional object calls; and snapshots survive a table flush while reopening restores the latest value. It traces provider operation classes to expose conditional WAL and manifest writes. Total startup/reopen call counts can vary with background scheduling and are not a benchmark. A 30-second timeout bounds the fixture.

Keep snapshots private until their corresponding durability handles complete. The probe does not establish serving authority, B2-safe metadata, failure recovery, compaction/collection safety or bounded memory. See the [integration assessment](../../docs/design-owned-partitions.md#reusing-an-existing-value-index) before extending or adopting it.
