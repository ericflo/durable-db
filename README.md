# durable-db

**A transactional database whose only state is a bucket.**

`durable-db` is one static Rust binary (~8 MB) that serves strictly serializable transactions and ordered JSON scans over HTTP. Everything lives as immutable objects in Backblaze B2, an S3-compatible bucket, or a local directory. Give every user, tenant, or agent its own isolated database—an idle one is 146 bytes in the bucket. Servers hold no durable state: add them, remove them, or kill every one, and nothing is lost.

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
durable-db serve --data ./data                    # leave running; use another terminal below

export TOKEN=$(durable-db create demo)
curl -X PUT localhost:7654/v1/databases/demo/keys/greeting -H "Authorization: Bearer $TOKEN" -d '"hello"'
curl localhost:7654/v1/databases/demo/keys/greeting -H "Authorization: Bearer $TOKEN"     # "hello"
```

> **New here?** Watch the [14-minute narrated explainer](https://durable-db.neuralcolumn.com/) (also embedded with chapters and a quality switcher on the [website](https://ericflo.github.io/durable-db-site/#watch)) covering per-agent databases, the claim protocol, fault injection, and group commit.

[Website](https://ericflo.github.io/durable-db-site/) · [Quick start](#quick-start) · [How it works](#how-it-works) · [Performance](#performance) · [Kubernetes](#on-kubernetes) · [Verification](#verification) · [Limits](#limits-and-status) · [Docs](#documentation)

## Why

- **Scales like a stateless web server.** A Kubernetes `Deployment` with no persistent volumes and an ordinary autoscaler. No consensus cluster, no leader-election service, no leases, no synchronized clocks.
- **Needs almost nothing from storage.** Only `PUT`, `GET`, `HEAD`, `LIST`, and `DELETE` with read-after-write consistency. No conditional writes (`If-Match`, `If-None-Match`), which Backblaze B2 rejects—so B2 and S3 run the same algorithm.
- **Built for many isolated databases.** Create a thousand databases in seconds, each with its own signed token, byte quota, and request rate limit. Idle databases cost zero server memory and one 146-byte manifest in the bucket.
- **Small enough to read.** Under 14,000 lines of Rust, one TLA+ model, and a protocol you can hold in your head: immutable objects, one writer per database holding a claim, and a claim check before every acknowledgement.

## Quick start

```sh
durable-db serve --data ./data          # listens on 127.0.0.1:7654
durable-db create my-app                # prints a write token for the new database
```

### TypeScript SDK

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url: 'http://127.0.0.1:7654', database: 'my-app', token });

await db.set('users/ada', { name: 'Ada', logins: 1 });
const ada = await db.get<{ name: string }>('users/ada');

// Read, decide, and write as one atomic step. If anything read changes first,
// nothing is written and the callback runs again.
await db.transaction(async (tx) => {
  const from = (await tx.get<number>('accounts/a')) ?? 0;
  const to = (await tx.get<number>('accounts/b')) ?? 0;
  if (from < 30) throw new Error('insufficient funds');
  tx.set('accounts/a', from - 30);
  tx.set('accounts/b', to + 30);
});

// Ordered prefix scan: one page, or iterate everything.
const page = await db.list({ prefix: 'users/', limit: 20 });
for await (const user of db.scan({ prefix: 'users/' })) console.log(user.key);
```

### Plain HTTP

```sh
curl localhost:7654/v1/databases/my-app/commit -H "Authorization: Bearer $TOKEN" -d '{
  "checks": [{"key": "accounts/a", "version": 7}],
  "writes": [{"key": "accounts/a", "value": {"balance": 70}},
             {"key": "accounts/b", "value": {"balance": 130}}],
  "idempotency_key": "transfer-7f3a"
}'
```

- [TypeScript client](clients/javascript/README.md) — zero runtime dependencies, automatic retries, revision tracking, and typed errors.
- [Task board example](examples/web-app/README.md) — a Hono web app with a 44-line storage module, four routes, and tests.
- [pi-durable storage adapter](clients/javascript/README.md#pi-durable-storage) — ships in the same package for `pi-durable` 1.6.1 and passes its complete storage conformance suite.

## What you get

- **Atomic transactions** — strictly serializable within a database, conditional on key versions and key ranges.
- **Ordered scans** — UTF-8 byte order by prefix or range, forward or reverse, cursor-paginated, all keys and scans in one request at one revision.
- **Durable acknowledgements** — a commit is acknowledged only after its bytes are stored in the bucket (or flushed to disk locally).
- **Two read levels** — `strong` (default) is linearizable at one storage round trip, shared across concurrent reads; `session` answers from memory and never goes backwards or misses your own writes.
- **Safe retries** — repeat a commit with the same `idempotency_key` and it applies once (remembered for 100,000 revisions).
- **Multi-tenant isolation** — per-database tokens verified before reading request bodies, byte quotas, request rate limits, and one-call revocation.
- **Self-cleaning storage** — flushing, compaction, and batch deletion of obsolete objects and versions are handled automatically by the owning server.

## How it works

```text
your app ──HTTP──▶ any server ──▶ the server holding the database ──▶ bucket
                   (stateless)     (in memory, with a claim object)    (all the state)
```

1. **Every object is immutable.** A database is frames of committed transactions, sorted LZ4-compressed tables, small manifests, a claim, and (if deleted) a tombstone. Every key lives under an epoch name with fresh random digits, so two servers never write the same key and no write needs to be conditional.
2. **One server at a time holds a database.** To take ownership, a server lists claims, picks a higher epoch, and `PUT`s its own claim. If a higher claim exists, it withdraws.
3. **A successor revokes before it reads.** The new owner deletes every lower claim first, and only then lists and reads the newest manifest and trailing frames. Nothing the prior owner acknowledges afterwards can be missed.
4. **Every acknowledgement waits for a `HEAD` on the claim.** A commit is one frame upload followed by one `HEAD`. If the claim still stands, the frame was stored before any successor began reading. If not, nothing is acknowledged and the client retries safely with its idempotency key.
5. **The same check guards reads, cleanup, and deletion.** Linearizable reads, garbage collection, and final database deletion each wait for a claim check begun after the state they depend on was stored.
6. **Servers only route.** Peers discover each other via DNS and relay requests to the database's owner. If they disagree, the claim decides; membership is never a correctness dependency.

Read [Architecture](docs/architecture.md) for every step and failure mode, or watch the [14-minute explainer](https://durable-db.neuralcolumn.com/).

## Performance

Measured 2026-11-14 with `durable-db bench` (release `2.1.1` static binary, 256-byte JSON values, one server on a 32-core desktop with consumer NVMe; Backblaze B2 `us-west-002` reached over residential Wi-Fi, 22 ms away). See [docs/performance.md](docs/performance.md) for full tables, large-database runs, and reproduction commands.

| Workload | Local directory, fsync on | Backblaze B2, 22 ms over Wi-Fi |
| --- | ---: | ---: |
| One commit (1 client, median) | 6.0 ms | 77 ms |
| Commits/s, 256 clients on one database | 16,541 | 1,881 |
| Commits/s, 64 clients over 64 databases | 4,936 | 534 |
| Strong reads/s, 256 clients | 378,213 | 6,480 (37 ms p50) |
| Session reads/s, 32 clients | 297,289 | 276,223 (0.10 ms p50) |

- **Storage latency is the floor; group commit drives throughput.** Every transaction arriving while an upload is in flight joins the next frame, so throughput rises with concurrency while median latency stays flat.
- **B2 variance and pod racing.** B2 upload times vary ~2× between hours (118 ms earlier the same day) and far more across storage pods. `durable-db` ranks pods by observed speed, routes primary uploads to the fastest free pod, and races an identical upload when a pod can be spared.
- **Many concurrent databases share nothing.** 64 databases committing together require 64 concurrent uploads; in that run, 1% of commits exceeded 500 ms.
- **Memory stays bounded as databases grow.** Measured with release `2.1.8` under a 1 GiB limit, servers peaked at 232–534 MiB for databases up to 16 GiB, and reopened a 16 GiB database after a kill in 50 ms locally ([details](docs/performance.md#a-large-database)).

## On a bucket

```sh
openssl rand -hex 32 > admin.key && chmod 600 admin.key
export AWS_ACCESS_KEY_ID=<keyID> AWS_SECRET_ACCESS_KEY=<applicationKey>
durable-db serve --bucket my-bucket --endpoint https://s3.us-west-002.backblazeb2.com --key-file admin.key
```

Run as many servers as you want against the same bucket and signing key. See [Deployment](docs/deployment.md) for B2, S3-compatible stores, configuration flags, tenant limits, and Prometheus metrics.

## On Kubernetes

```sh
kubectl -n durable-db create secret generic durable-db-keys --from-file=admin.key
kubectl -n durable-db create secret generic durable-db-storage --from-literal=DURABLE_DB_BUCKET=... # and endpoint, credentials
kubectl -n durable-db apply -k deploy/kubernetes
```

[`deploy/kubernetes`](deploy/kubernetes) ships a stateless `Deployment` (no persistent volumes; disposable `emptyDir` disk cache), two `Service`s, a `HorizontalPodAutoscaler`, and a `PodDisruptionBudget`. Stopping pods hand databases over gracefully; killed pods lose nothing.

`scripts/test-kubernetes.sh` verifies this on a three-node `kind` cluster backed by Backblaze B2: across **18 recorded runs**, the autoscaler scaled `2 → 5 → 2` while one pod was force-deleted mid-load, and every ledger balanced (**12,026 to 25,965 transfers**, varying with bucket latency at that hour). Alongside each run, 9–13 million unretried session reads ran with **0 to 110** failures across the forced kill.

## Verification

- **TLA+ model (`spec/DurableDb.tla`).** Exhaustive TLC checks at 2 writers and 2 epochs with zero invariant violations: **3.59M** distinct states (`DurableDb`), **4.11M** (`Deletion`), **67.1M** (`DeepDeletion`, 2 transactions + deletion), **659k** (`Retries`, idempotent retries and check refusals), and **967k** (`LateDeletes`, collection deletes arriving arbitrarily late). All **13 protocol rules** have negative controls in TLC; turning off any rule produces a counterexample.
- **Fault injection & kill soaks.** Three nodes on a store that delays, drops, half-applies, and late-applies requests with crashes and stale owners pass **200 random seeds** (`tests/chaos.rs`), alongside **74 targeted interleaving tests** (`tests/interleavings.rs`) and a 30-minute kill soak (**157 stops, 118 kills, 1,939,925 ledger transfers balanced**).
- **Real servers & buckets.** HTTP, multi-node cluster, and bucket contract suites on Backblaze B2, plus containerized MinIO, RustFS, and Garage, and the 18 Kubernetes failover runs above.

> Bounded model checking is evidence about a model—not a proof of the Rust implementation or of every deployment. See [Verification](docs/verification.md) for every assumption, what the model leaves out, all 47 findings caught by tests and reviews, and remaining obligations.

## Limits and status

Release `2.0.0` was a complete rewrite; every release from `2.0.0` through `2.1.12` shares the same storage format. **Upgrade from any release earlier than `2.1.4`** (servers with >20 open databases could stall background housekeeping; [finding 36](docs/verification.md#what-these-checks-found)). The read figures originally published with `2.0.0` were measured against a glibc build rather than the shipped static binary; [docs/performance.md](docs/performance.md) records the correction.

### By design

- **One writer per database at a time.** Scale write throughput across more databases, not more servers per database.
- **Single-database transactions, current state only.** Transactions do not span databases; there are no historical reads or point-in-time restore.
- **Deleted database names cannot be reused.** A permanent tombstone prevents late uploads from a prior owner from ever reviving deleted data.
- **Full rewrites on growth and quota pressure.** Merging the oldest table rewrites the database each time it grows by half, and once more if it hits its quota holding overwritten data; writes wait while that merge runs. Default quota is 1 GiB; databases up to 16 GiB (local), 4 GiB (MinIO), and 1 GiB (B2) have been measured ([details](docs/performance.md#a-large-database)).
- **Run count and merge turns.** A point lookup checks one table per run, newest first. Writes wait with `503` while a merge would leave >24 runs. A server runs two merges at a time; when more databases need merges, they share turns one table at a time, extending individual merges on slow links ([details](docs/performance.md#several-databases-merging-at-once)).
- **Disk cache sizing.** Reads missing both caches cost one bucket request per run searched. Where reads must stay fast or egress is billed, size `--cache-dir-bytes` to the databases a server serves—and 2× for zero-download merges ([details](docs/performance.md#sizing-the-disk-cache)).

### Not done yet

- **Only Backblaze B2 exercised among cloud stores.** Amazon S3, Google Cloud Storage, and Cloudflare R2 share the S3 code path (except B2's native upload) but have not been run live; MinIO, RustFS, and Garage pass in single-host containers.
- **No self-imposed provider rate cap.** Backblaze documents a default limit of 500 req/s per account; pushing past it for 20 minutes triggered 5.5 minutes of `503` refusals. Since `2.1.5` servers back off after 8 consecutive refusals of a request kind (tested on stand-ins and a proxy, not yet observed live on B2).
- **B2 measured only over residential Wi-Fi.** Every B2 figure comes from one desktop whose Wi-Fi stalled for 1–2 s about once a minute; a server in a data centre on a wired link has not been measured.
- **No multi-hour soak on a real multi-node cluster.** Memory stayed level across 10-minute local loads and 20-minute and 1-hour MinIO runs of 256 databases, and was still rising within configured flags after 5–11 minutes on B2—not yet measured for many hours.
- **Collection lag on very slow stores.** Purges wait up to 2 minutes per request; if a store takes longer, collection falls behind until it recovers ([finding 47](docs/verification.md#what-these-checks-found)).
- **Linux x86_64 prebuilt binary only.** Other platforms [build from source](docs/development.md).
- **No independent external review yet.** Reviews so far were automated and commissioned by the project.

<details>
<summary>Release history (2.0.0 – 2.1.12)</summary>

- **2.1.12** — Purge requests wait up to 2 minutes (from 20 s) so slow stores don't fail every purge; collection runs outside database maintenance so slow stores don't block flushes; TLA+ models late collection deletes ([finding 47](docs/verification.md#what-these-checks-found)).
- **2.1.11** — Queued databases take only their granted merge turn; new-owner leftover sweeps yield to release/deletion; TLA+ models idempotent retry deduplication (`Once`) and check refusals.
- **2.1.10** — Competing merges share turns one table at a time so small databases stay fast alongside large merges; release/deletion no longer wait for in-flight merge table uploads ([finding 46](docs/verification.md#what-these-checks-found)).
- **2.1.9** — Back-pressures writes with `503` while a merge would leave >24 runs; merges survive transient store failures without restarting from scratch ([finding 45](docs/verification.md#what-these-checks-found)).
- **2.1.8** — Fixes disk-cache thrashing when databases exceed cache size (tables fetched only after 1/16 read, never evicting active tables; multi-block read-ahead for scans/merges) and keeps run counts bounded during long merges ([findings 43–44](docs/verification.md#what-these-checks-found)).
- **2.1.7** — Databases hitting quota while holding overwritten/deleted data now compact before returning `507` ([finding 41](docs/verification.md#what-these-checks-found)); first >1 GiB measurements ([performance](docs/performance.md#a-large-database)).
- **2.1.6** — Slow listings no longer trigger read back-off ([finding 39](docs/verification.md#what-these-checks-found)); slow stores no longer block deletion, release, or housekeeping behind collection ([finding 40](docs/verification.md#what-these-checks-found)).
- **2.1.5** — Backs off to one probe request at a time after 8 consecutive store refusals of a request kind ([finding 38](docs/verification.md#what-these-checks-found)).
- **2.1.4** — Fixes housekeeping stall with >20 open databases ([finding 36](docs/verification.md#what-these-checks-found)); adjusts two listing requests for Amazon S3 and Google Cloud Storage compatibility.
- **2.1.3** — Supports unversioned S3 stores without version-listing APIs (such as Garage).
- **2.1.2** — Batch-deletes up to 1,000 object versions per purge request instead of one per version ([finding 32](docs/verification.md#what-these-checks-found)).
- **2.1.1** — Ranks and reuses fast Backblaze B2 storage pods instead of opening new pods on every commit ([finding 27](docs/verification.md#what-these-checks-found)).
- **2.1.0** — Fixes all findings from the `2.0.0` adversarial review, embeds jemalloc in the static binary, and enforces per-caller and per-database memory budgets.
- **2.0.0** — Complete rewrite around the immutable-object claim protocol.

</details>

## Documentation

| Document | Contents |
| --- | --- |
| [HTTP API](docs/api.md) | Endpoints, consistency, errors, and limits |
| [TypeScript client](clients/javascript/README.md) | The SDK and the `pi-durable` storage adapter |
| [Example](examples/web-app/README.md) | A task board on Hono |
| [Deployment](docs/deployment.md) | One machine, a bucket, Kubernetes, tenants, metrics |
| [Architecture](docs/architecture.md) | The protocol, step by step |
| [Performance](docs/performance.md) | Measurements and how to reproduce them |
| [Verification](docs/verification.md) | The model, the tests, and what remains |
| [Development](docs/development.md) | Building, testing, layout |

The same pages are published at [ericflo.github.io/durable-db-site](https://ericflo.github.io/durable-db-site/).

Apache-2.0. To report a vulnerability privately, see [SECURITY.md](SECURITY.md).
