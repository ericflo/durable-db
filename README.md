# durable-db

**Transactional key-value storage that lives in your bucket.**

durable-db is one Rust binary that serves atomic transactions and ordered scans over HTTP. S3 or Backblaze B2 holds the authoritative data. API servers keep only disposable state, so you can add, replace or stop all of them without moving the database. On a laptop, a local directory replaces the bucket.

> [!IMPORTANT]
> **Latest release: v1.0.6.** Production scope: one application or organization trust domain per deployment, with B2 or a local directory. Shared hosting of mutually untrusted tenants, AWS qualification and scheduled backups are deferred. Read the [supported scope](docs/production-readiness.md) and [verification limits](docs/verification.md).

[Quick start](#quick-start) · [Application code](#application-code) · [How it works](#how-it-works) · [Run on S3 or B2](#run-on-s3-or-b2) · [Contract and limits](#contract-and-limits) · [Status](#status) · [Verification](#verification) · [Documentation](#documentation) · [Website](https://ericflo.github.io/durable-db-site/)

## Quick start

No bucket, container or extra service is needed. Releases ship a static **Linux x86_64** binary; other platforms can [build from source](#build-from-source). While the repository is private, authenticate the `gh` CLI with an account that has access.

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
durable-db serve --data ./data
```

The installer verifies the release checksum and installs one file in `~/.local/bin`; set `DURABLE_DB_INSTALL_DIR` to choose another directory. The server listens on `127.0.0.1:7654`. The first start creates `data/admin.key`, readable only by you, which signs administrator tokens. Keep it private.

In another terminal, create a database, commit and read:

```sh
export DDB_TOKEN=$(durable-db token --key-file ./data/admin.key --admin)
curl -X PUT http://127.0.0.1:7654/v1/databases/demo \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"request_id":"create-demo","limits":{"max_bytes":67108864,"max_keys":100000}}'

curl http://127.0.0.1:7654/v1/databases/demo/commit \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"expected_revision":0,"request_id":"first-write","writes":[{"key":"hello","value":"d29ybGQ="}]}'

curl http://127.0.0.1:7654/v1/databases/demo/read \
  -H "Authorization: Bearer $DDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"keys":["hello"]}'
```

Keys are UTF-8 strings; values are base64-encoded bytes, so `d29ybGQ=` is `world`. A read returns a revision; send it with your next commit. A `409` means another commit won: read again and recompute. If a commit's response is lost, resend the **identical** request ID, revision and body.

Production provisioning mints revocable, database-scoped tokens after creation. See the [HTTP API](docs/api.md) for snapshots, scans, token revocation, errors and limits.

## Application code

The TypeScript SDK keeps JSON encoding, revisions and retries inside the client:

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token });

// One atomic read-modify-write. A conflict reruns the
// callback; a lost response retries the identical commit.
await db.transaction(async (tx) => {
  const task = await tx.get<Task>(`tasks/${id}`);
  if (!task || task.done) return;
  const done = (await tx.get<number>('stats/done')) ?? 0;
  tx.set(`tasks/${id}`, { ...task, done: true });
  tx.set('stats/done', done + 1);
});

// Ordered scans, a page at a time.
const { items, cursor } = await db.list<Task>({ prefix: 'tasks/', limit: 20 });
```

The package is linked from this repository while it is private; use the client from the same release as your server. The [client guide](clients/javascript/README.md) covers pagination, bounds and unresolved outcomes. The [task-board example](examples/web-app/README.md) adds setup, a short storage module, Hono routes and tests. Keep tokens on the application server, never in a browser bundle.

## How it works

```text
your app ──HTTPS──▶ API server (bounded cache) ──┐
                    API server (bounded cache) ──┼──▶ S3 or B2 bucket: the source of truth
                    0 to N servers, replaceable ─┘    - immutable, uniquely named data objects
                                                      - an authority record changed only by
                                                        conditional writes
```

- **Immutable journals, owned partitions.** Each partition orders transactions and uploads immutable journal frames. Ownership and checkpoints publish through conditional writes (`If-Match` on S3, `ifRevisionIs` on B2). Responses validate a fresh writer permit; competing commits from one database revision cannot both succeed.
- **Caches can be lost.** Servers keep bounded memory and disk caches and rebuild everything else from object storage. Cached bytes are verified by SHA-256. Losing a cache costs time, never data.
- **Uncertain writes have an answer.** A stored receipt returns the original result to an identical retry, and the old revision check stops a second application.
- **No clocks or leases in cleanup.** Garbage collection lists candidates, fences the state with a conditional write, and only then deletes. Pod-local locks, clocks, caches and Kubernetes leases never decide what is safe to delete.

On a laptop, the same API runs over a local directory held by an exclusive OS lock, with automatic checkpoints, cleanup and crash recovery. The [architecture](docs/architecture.md) describes the full protocol.

## Run on S3 or B2

Every server gets the same bucket, prefix and administrator signing key. Cloud serving uses native owned partitions. Kubernetes supplies membership through a headless Service; no attached volume, separate coordinator or clock-based lease is required. Removing every server stops availability but leaves the database in the bucket; starting one restores service.

```sh
# S3: credentials come from workload identity or the provider's environment.
export AWS_REGION=us-east-1
durable-db serve --bucket my-private-bucket \
  --admin-key-file /run/secrets/admin.key \
  --upload-budget-bytes 1073741824 --upload-budget-attempts 100000

# B2: a dedicated bucket and a scoped application key.
export B2_APPLICATION_KEY_ID=... B2_APPLICATION_KEY=...
durable-db serve --b2-bucket my-private-bucket \
  --admin-key-file /run/secrets/admin.key \
  --upload-budget-bytes 1073741824 --upload-budget-attempts 100000
```

These commands run one cloud process on loopback. For multiple processes, use the Kubernetes manifests or configure `--discovery-service` and `--advertise-ip`. Keep the partition layout fixed. Earlier cloud formats require export with the previous binary and restore into a fresh namespace.

S3-compatible providers must supply atomic `If-Match`/`If-None-Match` writes, durable immutable uploads and ordered listings. A startup probe refuses providers that accept stale conditional writes, but it cannot establish the whole provider contract. B2 and S3 share one publication implementation. Upgrade every server before enabling protected databases or a new signing keyring.

> [!IMPORTANT]
> The native cloud path removes bucket-wide conditional publication from ordinary data operations. Reads still require a fresh permit GET. Its measured operating limits are documented below; historical shared-engine benchmarks do not describe this replacement.

[Deployment](docs/deployment.md) covers Kubernetes manifests in `deploy/kubernetes/` (stable pod identities and disposable caches), request budgets and key rotation. [B2](docs/b2.md) and [upload admission](docs/design-upload-admission.md) cover provider details and durable spending limits.

## Contract and limits

The 1.0 contract applies within the [supported deployment scope](docs/production-readiness.md):

- **Serializable transactions within a database.** Every commit names the revision it read; if anything committed since, it gets `409` and the client recomputes.
- **Durable acknowledgements.** An acknowledged write must survive the loss of every API server and every cache.
- **Ordered reads.** Keys sort by UTF-8 bytes. Scans take a prefix, a range, a cursor and a direction, and every key and scan in one request reads one snapshot.
- **Exact retries.** Receipts for the last 128 commits return the original result to an identical retry.
- **Limits that refuse instead of truncating.** Quotas, request sizes, caches and per-principal request budgets reject work before anything is published.
- **Out of scope:** transactions that span databases, and long-running historical snapshots. Use one database per tenant or application.

| Resource | Initial limit |
| --- | --- |
| Database | Up to 256 MiB and 1,000,000 keys |
| Key | 1–1,024 UTF-8 bytes |
| Value | 64 KiB |
| Request | 1 MiB and 128 operations |
| Scan page | Up to 1,000 entries |
| Read result | 1 MiB |
| Request deadline | 30 seconds |

[All limits and error codes](docs/api.md#errors-and-bounds).

## Status

| State | Today |
| --- | --- |
| **Supported in 1.0** | Local-directory and B2 serving for one trust domain. Atomic transactions, ordered scans, scoped/revocable credentials, logical quotas, bounded admission, automatic checkpoints/collection, the TypeScript KV SDK, and manual export/verified restore. |
| **Available, unqualified** | The common S3 binding on AWS. Pi 1.5.0 adapters have native HTTP conformance and Harness tests; broader cloud Harness/upgrade qualification remains open. |
| **Deferred** | Mutually untrusted tenant hosting and complete cost/fairness isolation. Scheduled backups/PITR. Availability, throughput and p99 SLOs. A 72-hour soak, independent review, broader upgrades and additional binary platforms. |

Use one dedicated bucket per deployment, private peer networking and TLS at
application ingress. Fund upload allowances, set process/replica limits and
keep independently protected backups. v1.0.6 recovered **385/385** acknowledged
writes in its scale-down run, with a **41.31s** largest readback gap. Its separate
32-worker, 1 KiB B2 load baseline completed all operations at **9.34 writes/s**
and **141.38 reads/s**. Write tails reached **53.18s**; a larger-value run hit
cumulative funding limits. Write performance and conservative maintenance
precharging need improvement. These are short fixtures, not SLOs.
See the [measured results](docs/verification-results.md),
[operating scope and deferrals](docs/production-readiness.md), and [recovery procedure](docs/recovery.md).

## Verification

Protocol changes start as TLA+ models, which ship with deliberately broken variants that must produce counterexamples. An [unbounded TLAPS proof](docs/proof-publication.md) covers abstract page reachability. The Rust implementation faces adversarial and failure-injection tests against real B2, MinIO and Kubernetes, including SIGKILL, OOM, provider partitions and removal of every server.

Bounded model checking is not a proof of the Rust implementation or of every deployment. Full protocol composition and implementation refinement remain open; the [verification record](docs/verification.md) lists every assumption, checked bound and remaining obligation.

## Documentation

| Section | Documents |
| --- | --- |
| **Start** | [Deployment](docs/deployment.md) · [HTTP API](docs/api.md) · [Clients](docs/clients.md) · [Web app example](docs/web-app.md) |
| **Understand** | [Architecture](docs/architecture.md) · [B2 protocol](docs/b2.md) · [Security](docs/security.md) |
| **Operate** | [Backup and restore](docs/recovery.md) · [Observability](docs/observability.md) · [Development](docs/development.md) |
| **Verify** | [Production readiness](docs/production-readiness.md) · [Verification](docs/verification.md) · [Verification results](docs/verification-results.md) · [Publication proof](docs/proof-publication.md) |
| **Design notes** | [Owned partitions](docs/design-owned-partitions.md) · [Upload admission](docs/design-upload-admission.md) · [Budgetless collection](docs/design-budgetless-collection.md) · [Signing key rotation](docs/design-signing-rotation.md) |

The same documents are published at [ericflo.github.io/durable-db-site](https://ericflo.github.io/durable-db-site/).

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

The executable is `target/release/durable-db`. The [development guide](docs/development.md) covers focused checks, the complete Rust, model and client suite, and provider fixtures. Run the large directory capacity regression in release mode, as CI does.

CI, static binaries, container images and documentation publication use **Woodpecker**; no GitHub Actions workflows are used. Licensed under Apache-2.0. Contributions will open when the repository becomes public.
