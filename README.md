# durable-db

**Transactional storage. Replaceable compute. One binary.**

durable-db is an ordered key-value database written in Rust. Keep authoritative data in object storage, run interchangeable API servers, and use bounded memory and optional local disk to make immutable reads cheaper. On a laptop, a local directory replaces the object store.

**Status: experimental, v0.1.0.** The protocol is modeled in TLA+ and checked with TLC, with an [unbounded proof of abstract page-reachability safety](docs/proof-publication.md). The Rust implementation has adversarial and integration tests. Full protocol composition and implementation refinement remain open. Read [verification](docs/verification.md) before putting important data here.

| What you get | How it works |
| --- | --- |
| Atomic transactions | Immutable sorted pages and manifests, published through one conditional directory update |
| Ordered scans | UTF-8 byte ordering, prefix/range filters, forward/reverse cursors |
| Disposable servers | Every request reads the authoritative manifest; caches never decide ownership |
| Local development | One executable, one directory, no additional service |
| Tenant boundaries | Database-scoped signed tokens, exact logical quotas, bounded requests and concurrency |
| Automatic maintenance | Cooperative fenced garbage collection and incremental page compaction |
| A checkable design | TLA+ specification, intentional broken-model counterexample, failure-injection tests |

The current shared read/publication path does not meet the performance brief and will be replaced before production. The [owned-partition design](docs/design-owned-partitions.md) uses locally served reads, independent durable log batches and a native Rust index. Its live serving controller and replacement transaction API remain under development.

The replacement index now recovers an immutable checkpoint plus a bounded recent journal layer. Values stay in shared sorted tables, reads fetch the relevant blocks, and bounded memory/disk caches are disposable. Checkpoints bind values and retry receipts to an exact journal cut. B2 and MinIO fixtures check recovery, generation isolation and cached reads without per-read control updates; these component results are recorded in [verification](docs/verification.md).

## Start locally

[v0.1.0](https://github.com/ericflo/durable-db/releases/tag/v0.1.0) includes a statically linked **Linux x86_64** executable. No Rust toolchain or additional database service is needed. While the repository is private, authenticate `gh` with an account that has repository access.

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
durable-db serve --data ./data
```

The installer verifies the release checksum and installs one file into `~/.local/bin`. Set `DURABLE_DB_INSTALL_DIR` to choose another directory. For other platforms, [build from source](#build-from-source).

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

Current source builds add [idempotent protected creation and durable token revocation](docs/api.md#create-a-protected-database), [expiring administrator capabilities and overlapping signing keys](docs/design-signing-rotation.md), and [consistent export and resumable restore](docs/recovery.md). New provisioning uses PUT with a persistent creation request ID, then mints revocable database credentials. These features postdate the v0.1.0 installer example and require upgraded servers.

Fresh cloud namespaces can also enable [cumulative immutable-upload admission](docs/design-upload-admission.md). Its byte and attempt ceilings survive restarts and scaling, and administrators can raise them without resetting spent credit. This shared ceiling is one part of resource protection; format 5 also reclaims unreachable pages without upload credit. Per-tenant spend isolation and sustained-load qualification remain production gates.

## Application code

The TypeScript SDK keeps JSON encoding, revisions and transaction retries inside the client:

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token });
await db.set('settings', { theme: 'dark' });

await db.transaction(async (tx) => {
  const count = (await tx.get<number>('views')) ?? 0;
  tx.set('views', count + 1);
});
```

The package is linked locally while this repository is private. The [task-board example](examples/web-app/README.md) provides setup, a short storage module, Hono routes and tests. See the [client guide](clients/javascript/README.md) for pagination, bounds and unresolved outcomes. Current source builds are required.

## Scale without moving the database

```text
                  ┌── API pod · bounded cache ──┐
clients ─ service ├── API pod · bounded cache ──┼── immutable pages + shared directory
                  └── API pod · bounded cache ──┘          object storage
```

Pods need the same bucket and prefix, and compatible signing keys for the current rotation phase. They need no peer discovery, quorum membership changes, attached data volume, or distributed lease. Removing every pod stops availability but leaves the database in object storage. Starting one pod restores service.

```sh
export AWS_REGION=us-east-1
# Configure AWS credentials through workload identity or the provider's environment.
durable-db serve --listen 0.0.0.0:7654 --bucket my-private-bucket \
  --admin-key-file /run/secrets/admin.key --memory-cache-mib 64
```

S3-compatible providers must supply atomic `If-Match`/`If-None-Match` writes, durable immutable uploads, and ordered listings. Reads validate authority observations through a conditional barrier. Startup probes stale-write rejection; that probe does not establish the entire provider contract. **B2 and S3 use one publication implementation.** For B2, run with `--b2-bucket NAME` and `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY`. It publishes immutable manifests through revision-checked bucket metadata (`ifRevisionIs`) and physically deletes obsolete file versions. An immutable B+tree directory stores database entries outside bucket metadata; database count is no longer limited by metadata capacity. Initial real-B2 and versioned/unversioned S3-compatible tests pass; broader cloud qualification continues; see [B2 design and limits](docs/b2.md).

Pods add request concurrency, but directory publication serializes within a bucket. The first B2 contention benchmark exhausted write retries with four active writers; publication progress and performance remain a [production blocker](docs/verification-results.md#b2-contention-baseline-release-gate-failed). Object-store round trips set the write-latency floor. The common directory uses 64-entry index pages and one bounded root pointer; lookup and publication copy a logarithmic path. The initial format also caps a database at 256 MiB and 2,048 pages. [Limits and semantics](docs/api.md).

## Read more

- [Architecture and storage protocol](docs/architecture.md)
- [HTTP API and consistency](docs/api.md)
- [Kubernetes, operations, and recovery](docs/deployment.md)
- [Security and resource isolation](docs/security.md)
- [Formal model and verification evidence](docs/verification.md)
- [Client and pi-durable integration](docs/clients.md)
- [TypeScript web application example](examples/web-app/README.md)
- [Development and Woodpecker CI](docs/development.md)
- [Public documentation website](https://ericflo.github.io/durable-db-site/)
- [Production readiness and release gates](docs/production-readiness.md)
- [Roadmap](docs/roadmap.md)

## Build from source

Rust is pinned in `rust-toolchain.toml`. All compiler invocations use [kache](https://github.com/kunobi-ninja/kache); keep its client and engine together.

```sh
git clone git@github.com:ericflo/durable-db.git
cd durable-db
scripts/install-kache.sh
export PATH="$PWD/.tools/bin:$PATH"
export KACHE_CONFIG="$PWD/.kache.toml"
cargo build --locked --release
```

The executable is `target/release/durable-db`. The [development guide](docs/development.md) covers focused checks, the complete Rust/model/client suite and provider fixtures. Run the large directory capacity regression in release mode, as CI does; a debug sweep makes ordinary iteration unnecessarily slow.

CI, static binary builds, container builds, and documentation publication use **Woodpecker**. No GitHub Actions workflows are used. Apache-2.0 licensed. Contributions will open when the repository becomes public.
