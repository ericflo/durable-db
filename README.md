# durable-db

**Transactional storage. Replaceable compute. One binary.**

durable-db is an ordered key-value database written in Rust. Keep authoritative data in object storage, run interchangeable API servers, and use bounded memory and optional local disk to make immutable reads cheaper. On a laptop, a local directory replaces the object store.

**Status: experimental, v0.1.0.** The initial protocol is modeled in TLA+ and checked with TLC. The Rust implementation has adversarial and integration tests. These are evidence with explicit bounds—not a proof of the entire implementation. Read [verification](docs/verification.md) before putting important data here.

| What you get | How it works |
| --- | --- |
| Atomic transactions | Immutable sorted pages, published through one conditional manifest write |
| Ordered scans | UTF-8 byte ordering, prefix/range filters, forward/reverse cursors |
| Disposable servers | Every request reads the authoritative manifest; caches never decide ownership |
| Local development | One executable, one directory, no additional service |
| Tenant boundaries | Database-scoped signed tokens, exact logical quotas, bounded requests and concurrency |
| Automatic maintenance | Cooperative fenced garbage collection and incremental page compaction |
| A checkable design | TLA+ specification, intentional broken-model counterexample, failure-injection tests |

## Start locally

The initial source checkout is available now. Prebuilt releases are being prepared; build the current development version with Rust 1.90 and the pinned kache tools:

```sh
git clone git@github.com:ericflo/durable-db.git
cd durable-db
scripts/install-kache.sh
export PATH="$PWD/.tools/bin:$PATH"
export KACHE_CONFIG="$PWD/.kache.toml"
cargo build --locked --release
./target/release/durable-db serve --data ./data
```

In the examples below, put `target/release/durable-db` on your `PATH` or copy the single executable to your preferred location.

The default address is `127.0.0.1:7654`. First startup generates `data/admin.key` with owner-only file permissions. Keep that signing key private. Generate an administrator token in another terminal:

```sh
export DDB_TOKEN=$(durable-db token --key-file ./data/admin.key --admin)
curl http://127.0.0.1:7654/v1/databases/demo \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"max_bytes":67108864,"max_keys":100000}'

curl http://127.0.0.1:7654/v1/databases/demo/commit \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"expected_revision":0,"request_id":"first-write","writes":[{"key":"hello","value":"d29ybGQ="}]}'

curl http://127.0.0.1:7654/v1/databases/demo/read \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"keys":["hello"]}'
```

Values are base64-encoded arbitrary bytes: `d29ybGQ=` is `world`. Keys are UTF-8 strings. A read returns a revision; supply it when committing changes. A `409` requires a fresh read and recomputation. Retry an uncertain write with the **identical** request ID, revision, and body.

## Scale without moving the database

```text
                  ┌── API pod · bounded cache ──┐
clients ─ service ├── API pod · bounded cache ──┼── immutable pages + CAS manifest
                  └── API pod · bounded cache ──┘          object storage
```

Pods need the same bucket, prefix, and signing key. They need no peer discovery, quorum membership changes, attached data volume, or distributed lease. Removing every pod stops availability but leaves the database in object storage. Starting one pod restores service.

```sh
export AWS_REGION=us-east-1
# Configure AWS credentials through workload identity or the provider's environment.
durable-db serve --listen 0.0.0.0:7654 --bucket my-private-bucket \
  --admin-key-file /run/secrets/admin.key --memory-cache-mib 64
```

S3-compatible providers must guarantee atomic `If-Match`/`If-None-Match` writes and strongly consistent reads. Startup probes these capabilities and refuses to weaken the protocol. **B2 is a required backend and is under active implementation.** Its native bucket revision update (`ifRevisionIs`) offers a separate publication primitive; the B2 design will use immutable objects plus revision-checked bucket metadata, rather than assume S3 conditional overwrite support. The current executable supports local directories and S3. See [B2 design](docs/b2.md).

Autoscaling improves aggregate capacity across databases. Writes to one database serialize at its manifest. Object-store round trips set the write-latency floor; this is not an in-memory database with asynchronous backup. The initial format also caps a database at 256 MiB and 2,048 pages. [Limits and semantics](docs/api.md).

## Read more

- [Architecture and storage protocol](docs/architecture.md)
- [HTTP API and consistency](docs/api.md)
- [Kubernetes, operations, and recovery](docs/deployment.md)
- [Security and resource isolation](docs/security.md)
- [Formal model and verification evidence](docs/verification.md)
- [Client and pi-durable integration](docs/clients.md)
- [Development and Woodpecker CI](docs/development.md)
- [Roadmap and release gates](docs/roadmap.md)

## Build and test

Rust is pinned in `rust-toolchain.toml`. All compiler invocations use [kache](https://github.com/kunobi-ninja/kache); keep its client and engine together.

```sh
scripts/install-kache.sh
export PATH="$PWD/.tools/bin:$PATH"
export KACHE_CONFIG="$PWD/.kache.toml"
cargo build --locked --release
cargo test --locked --all-targets
scripts/check-model.sh             # Java 11+; checksum-pinned TLC
cd clients/javascript && npm ci && npm test # HTTP conformance requires test server env
```

CI, release builds, and documentation publication are being configured in **Woodpecker**. No GitHub Actions workflows are used. Apache-2.0 licensed. Contributions will open when the repository becomes public.
