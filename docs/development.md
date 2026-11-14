# Development

## Build

The Rust toolchain is pinned in `rust-toolchain.toml`. Every compiler invocation goes through [kache](https://github.com/kunobi-ninja/kache), a build cache, as configured in `.cargo/config.toml`.

```sh
scripts/install-kache.sh              # installs the pinned kache into .tools/bin
export PATH="$PWD/.tools/bin:$PATH" KACHE_CONFIG="$PWD/.kache.toml"
cargo build --release                 # target/release/durable-db
```

The binary carries its own allocator, jemalloc, on every platform but Windows, so building it needs `make` and a C compiler. `src/main.rs` says why, and [performance](performance.md#the-allocator) has the measurements.

## Check

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                            # about 30 seconds
scripts/check-model.sh                # the TLA+ model and its negative controls
scripts/test-clients.sh               # the TypeScript SDK, pi-durable adapter and example
scripts/soak-memory.sh                # a server under the manifest's memory limit, loaded for three minutes
```

| Suite | What it covers |
| --- | --- |
| `cargo test --lib` | Table format, in-memory table, key layout, tokens, routing, request parsing. |
| `cargo test --test engine` | The engine on in-memory and directory storage: transactions, checks, retries, recovery, takeover, flush, merge, collection, quota, deletion, the disk cache, and what a database may hold in memory. |
| `cargo test --test chaos` | Three nodes on storage that delays, drops, half-applies and belatedly applies requests, with crashes and zombie owners. Checks that nothing acknowledged is lost, nothing is applied twice, linearizable reads never go backwards, and a deleted database stays deleted. `CHAOS_SEEDS=50` runs more schedules; `CHAOS_FIRST=n` starts at seed `n`. |
| `cargo test --test interleavings` | Single schedules that a random test seldom finds, built by holding or failing chosen storage requests. |
| `cargo test --test storage` | The S3 binding against an in-process stand-in that keeps versions, pages its listings and verifies signatures; the directory store against the in-memory one. |
| `cargo test --test http` | The documented HTTP API against a real server process. |
| `cargo test --test tenants` | What one caller can do to the others: memory held by bodies, results and connections, request shapes, relayed requests, shutdown. |
| `cargo test --test cluster` | Up to three server processes on one store: routing, a killed owner, a graceful handover, a joining node, a contended counter. |
| `cargo test --test bucket` | The storage contract, read-after-write, and the engine on a real bucket. Skipped unless configured, see below. |

To run the bucket tests, point them at a bucket you can afford to write to. Each test uses its own random prefix and removes what it wrote.

```sh
export DDB_S3_ENDPOINT=https://s3.us-west-002.backblazeb2.com DDB_S3_BUCKET=my-test-bucket
export DDB_S3_ACCESS_KEY_ID=... DDB_S3_SECRET_ACCESS_KEY=...
cargo test --test bucket
```

`scripts/check-model.sh` runs TLC with a 2 GiB heap, four workers and a time limit, in Docker when no local Java is installed. TLC sizes itself from the whole machine if left alone; do not remove the caps. All configurations take about three minutes. `scripts/check-model.sh DeepDeletion` runs the one configuration that is left out because it takes far longer.

`scripts/soak-memory.sh` runs one server with the flags of the Kubernetes manifest in a systemd scope limited to 1 GiB without swap and four CPUs, loads it in three ways with values that do not compress, and fails if the server is killed. It needs about 4 GiB of disk and `DURABLE_DB` set to the binary to test; test the static binary, because the allocator's behaviour depends on the C library it is linked with.

## Layout

```
src/store/      the storage contract and its three bindings: memory, directory, S3-compatible
src/layout.rs   object names, manifests, frames
src/table.rs    immutable sorted tables
src/mem.rs      the in-memory table of unflushed writes
src/io.rs       retries, the block cache, the disk cache
src/db.rs       one database: taking ownership, commits, reads, flush, merge, collection
src/node.rs     the databases one server holds, and its limits
src/cluster.rs  membership, routing, relaying
src/auth.rs     tokens
src/http.rs     the HTTP API
src/server.rs   accepting connections, and what one may cost before it has shown a token
src/bench.rs    the load generator and the ledger check
src/main.rs     the command line
spec/           the TLA+ model of src/db.rs
clients/        the TypeScript SDK and the pi-durable storage adapter
examples/       a small web application
deploy/         Kubernetes manifests
site/           the website generator
```

`src/db.rs` implements `spec/DurableDb.tla`; a change to the protocol starts in the model. [Verification](verification.md) records what is checked and what is not.

## Releases and CI

Woodpecker builds every push with the repository's `Dockerfile`: the model check, the Rust and client tests, a static `x86_64-unknown-linux-musl` binary, the website, and a `scratch` runtime image. A release is that binary and its checksum, built from the tagged commit and attached to the tag. There are no GitHub Actions workflows.

Benchmarks quoted anywhere in this repository are reproduced by the commands in [performance](performance.md).
