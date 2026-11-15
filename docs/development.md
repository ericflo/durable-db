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
cargo test                            # about 40 seconds
scripts/check-model.sh                # the TLA+ model and its negative controls
scripts/test-clients.sh               # the TypeScript SDK, pi-durable adapter and example
scripts/soak-memory.sh                # a server under the manifest's memory limit, loaded for three minutes
scripts/test-slow.sh                  # every test alone on 28% of a processor, as on a slow build machine
scripts/test-stores.sh                # the bucket tests and a ledger on MinIO, RustFS and Garage in containers
scripts/test-refusals.sh              # what a server sends a store that refuses every change for twenty seconds
scripts/soak-kills.sh                 # a ledger through three servers while one is stopped every ten seconds
scripts/soak-bucket.sh                # 256 databases written for twenty minutes on MinIO, then deleted
scripts/soak-size.sh                  # one database filled to 4 GiB, opened by a new server, read, written over, deleted
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

`scripts/test-stores.sh` does that for three S3 servers it starts in containers on this machine, each held to 1 GiB and two CPUs: a community build of MinIO and RustFS, each with a bucket that keeps versions and one that does not, and Garage, which keeps no versions and has no request that lists them. On each store it then runs a ledger through three servers while one is killed, one is stopped and one is killed again. It needs Docker and a release build, and takes about seven minutes.

`scripts/soak-kills.sh` runs a ledger through three servers on one directory for ten minutes (`SOAK_SECONDS`), stops one of them every ten seconds (`PAUSE`), three times in four with a kill, and starts it again. It fails if the ledger does not verify or a server panics, and prints the most memory and open files any server had. `DURABLE_DB` names the binary.

`scripts/test-refusals.sh` runs one server on MinIO behind an nginx that it tells to refuse every change for the middle third of a ledger, and prints how many requests the store refused, how soon a change was taken afterwards, and whether the ledger verified. `DURABLE_DB_BIN` names the binary, so that two releases can be compared. It needs Docker and takes about a minute and a half. The commit rate it prints is not a measurement: MinIO on a desktop's disk gave 420 to 1,030 commits a second to the same binary from one run to the next.

`scripts/soak-bucket.sh` runs one server with the flags of the Kubernetes manifest, held to 1 GiB, on MinIO in a container: 256 databases (`SOAK_DATABASES`) are written for twenty minutes (`SOAK_SECONDS`) and then deleted one after another. It prints what memory did, how many purges failed, how the deletions went, and how often the server held requests back from the store, and fails if the server is killed, panics, or says that a round of its housekeeping has not ended. It is the run that found that a store slow to list was taken to refuse reads. `DURABLE_DB` names the binary. It needs Docker and about half an hour.

`scripts/soak-size.sh` fills one database to 4 GiB (`SOAK_GIB`) of 4 KiB values through one server with the flags of the Kubernetes manifest, held to 1 GiB and four CPUs, on a local directory, with `SOAK_STORE=minio` on MinIO in a container, or with `SOAK_STORE=bucket` on the bucket that the `DDB_S3_` variables of the bucket tests name. It prints how fast the fill was and how its rate changed, how much was sent to storage for it, the most memory the server used, how long a new server with an empty cache took to open the database after the first was killed, a minute (`SOAK_SECONDS`) of strong reads from that server and then another, since the first are of what it has in neither cache, a minute of scans, each of the three with how much the server read from storage for it and in how many requests, two minutes of writes over what is there, how long a write then waits when the quota is set under what the database holds, which is how long a merge of all of it takes, how long a third server took to open the database after the second was stopped and not killed, how long the deletion took, and how many requests to storage failed. It fails if a server is killed or panics, or if that write is refused. It needs four times the size in free disk; 4 GiB take about a quarter of an hour, 16 GiB about three quarters. On a local directory the script reads the directory itself before the reads and the scans, so that the kernel has it in memory and the directory answers at once: the kernel counts what a server reads of it against the server's limit of 1 GiB and would keep little of it, and the loads would then measure the machine's disk and differ several times over between runs. That needs as much free memory as the database is large. On a bucket every read of what is in neither cache is a request, and the reads send as many at once as they have clients: `SOAK_READ_CLIENTS` gives them fewer than the fill has. `SOAK_CACHE_MIB` gives the servers a disk cache other than the manifest's 1 GiB. `DURABLE_DB` names the binary.

The load generator has what that script uses. `bench --workload fill` writes every one of `--keys` keys once, in an order spread over them, and ends when it has; a write that is answered `503` is made again. `--max-bytes` gives the databases it creates a quota. `--database name` uses databases called `name-0`, `name-1` and so on, which may be there already, loads nothing into them before a read workload and leaves them in place, so that one run can read what another wrote.

`scripts/check-model.sh` runs TLC with a 2 GiB heap, four workers and a time limit, in Docker when no local Java is installed. TLC sizes itself from the whole machine if left alone; do not remove the caps. All configurations take about three minutes. `TLC_TIMEOUT=3600 scripts/check-model.sh DeepDeletion` runs the one configuration that is left out, because it takes nineteen minutes.

`scripts/soak-memory.sh` runs one server with the flags of the Kubernetes manifest in a systemd scope limited to 1 GiB without swap and four CPUs, loads it in three ways with values that do not compress, and fails if the server is killed. Last it kills the server itself in the middle of a load over 256 databases and starts it again, and reports the memory of the second server apart. It needs about 4 GiB of disk and `DURABLE_DB` set to the binary to test; test the static binary, because the allocator's behaviour depends on the C library it is linked with.

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

Woodpecker builds every push with the repository's `Dockerfile`: the model check, the Rust and client tests, a static `x86_64-unknown-linux-musl` binary, the website, and a `scratch` runtime image. A release is that binary and its checksum, built from the tagged commit and attached to the tag. There are no GitHub Actions workflows. The build server's pages ask for a login, so the log of a build is not public; `docker build .` runs the same checks and makes the same binary on any machine with Docker.

Benchmarks quoted anywhere in this repository are reproduced by the commands in [performance](performance.md).
