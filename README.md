# durable-db

**A transactional database whose only state is a bucket.**

durable-db is one static binary that serves atomic transactions and ordered scans over HTTP. Everything it stores lives in a Backblaze B2 or S3 bucket, or in a directory on your laptop. Servers keep nothing that matters, so you run them like a stateless web service: add them, remove them, or kill all of them, and the database is still there.

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
durable-db serve --data ./data                    # leave running; use another terminal below

export TOKEN=$(durable-db create demo)
curl -X PUT localhost:7654/v1/databases/demo/keys/greeting -H "Authorization: Bearer $TOKEN" -d '"hello"'
curl localhost:7654/v1/databases/demo/keys/greeting -H "Authorization: Bearer $TOKEN"     # "hello"
```

[Quick start](#quick-start) · [How it works](#how-it-works) · [Performance](#performance) · [Kubernetes](#on-kubernetes) · [Verification](#verification) · [Limits](#limits-and-status) · [Docs](#documentation)

## Why

- **It scales like a web server.** A Kubernetes Deployment with no volumes and an ordinary autoscaler. No consensus cluster, no leader election service, no leases, no synchronized clocks.
- **It needs almost nothing from storage.** PUT, GET, LIST and DELETE with read-after-write consistency. No conditional writes, which Backblaze B2 does not have, so B2 and S3 run the same algorithm.
- **It is small enough to read.** About 10,000 lines of Rust, one TLA+ model, and a protocol you can hold in your head: immutable objects, one writer per database holding a claim, and a check of that claim before every acknowledgement.

## Quick start

```sh
durable-db serve --data ./data          # listens on 127.0.0.1:7654
durable-db create my-app                # prints a token for the new database
```

From TypeScript:

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url: 'http://127.0.0.1:7654', database: 'my-app', token });

await db.set('users/ada', { name: 'Ada', logins: 1 });
const ada = await db.get<{ name: string }>('users/ada');

// Read, decide and write as one atomic step. If anything the callback read
// changes first, nothing is written and the callback runs again.
await db.transaction(async (tx) => {
  const from = (await tx.get<number>('accounts/a')) ?? 0;
  const to = (await tx.get<number>('accounts/b')) ?? 0;
  if (from < 30) throw new Error('insufficient funds');
  tx.set('accounts/a', from - 30);
  tx.set('accounts/b', to + 30);
});

// Ordered keys: one page, or everything.
const page = await db.list({ prefix: 'users/', limit: 20 });
for await (const user of db.scan({ prefix: 'users/' })) console.log(user.key);
```

Or plain HTTP, with JSON values and no encoding step:

```sh
curl localhost:7654/v1/databases/my-app/commit -H "Authorization: Bearer $TOKEN" -d '{
  "checks": [{"key": "accounts/a", "version": 7}],
  "writes": [{"key": "accounts/a", "value": {"balance": 70}},
             {"key": "accounts/b", "value": {"balance": 130}}],
  "idempotency_key": "transfer-7f3a"
}'
```

The [TypeScript client](clients/javascript/README.md) retries safely, tracks revisions and types errors. The [task board example](examples/web-app/README.md) is a 44-line storage module, four routes and tests. The same package contains a storage adapter for [pi-durable](clients/javascript/README.md#pi-durable-storage) 1.5.2 that passes its complete storage conformance suite.

## What you get

- **Atomic transactions** within a database, strictly serializable, conditional on the versions of the keys and ranges you read.
- **Ordered scans** by prefix or range, forwards or backwards, paginated.
- **Durable acknowledgements.** A commit is acknowledged only once it is in the bucket (or flushed to disk, locally).
- **Two read levels.** `strong` reads are linearizable and cost one storage round trip, shared by concurrent reads. `session` reads come from memory and still never go backwards or miss your own writes.
- **Safe retries.** Repeat a commit with the same idempotency key and it applies once.
- **Many tenants.** One deployment serves many databases, each with its own tokens, storage quota and request budget.
- **Nothing to operate.** Flushing, merging and deleting obsolete objects are done by whichever server holds a database.

## How it works

```
your app ──HTTP──▶ any server ──▶ the server holding the database ──▶ bucket
                   (stateless)     (in memory, with a claim object)    (all the state)
```

1. **A database is a set of immutable objects**: frames of committed transactions, sorted tables, and small manifests that list them. Every object is written by exactly one server under a name only it uses, so no write is ever conditional.
2. **One server at a time holds a database.** It proves it with a *claim* object. Another server takes over by deleting that claim, and only then reads what the previous holder wrote.
3. **Every acknowledgement waits for a claim check.** A commit is one upload, then one HEAD to confirm the claim still exists. If it does, no one has taken over, and no one can have missed the upload. If it does not, nothing is acknowledged.
4. **The same check guards reads and cleanup.** A linearizable read and a deletion of old objects each wait for a claim check too.
5. **Servers only route.** They find each other through DNS and relay each request to the server that should hold the database. If they disagree, the claim decides; membership is never trusted for correctness.

[Architecture](docs/architecture.md) explains each step and what happens when servers crash, stall or race.

## Performance

Measured with the built-in `durable-db bench`, 256-byte values, one server. [Full tables, conditions and how to reproduce them.](docs/performance.md)

| | Local directory, fsync on | Backblaze B2, 22 ms away over Wi-Fi |
| --- | ---: | ---: |
| One commit | 6.0 ms | 77 ms |
| Commits/s, 256 clients on one database | 16,541 | 1,881 |
| Commits/s, 64 clients over 64 databases | 4,936 | 534 |
| Strong reads/s, 256 clients | 378,213 | 6,480 (37 ms each) |
| Session reads/s, 32 clients | 297,289 | 276,223 (0.10 ms each) |

On a bucket, a commit costs one upload and one HEAD, so the provider's upload time is the floor. B2's upload time varies by a factor of two between hours and far more between its storage pods; durable-db keeps the pods that prove fast, sends each upload to the fastest one that is free, and races an identical upload on another pod when one can be spared. Many clients share each upload, which is why throughput rises with load while latency does not. Many databases committing at once share nothing: 64 of them need 64 uploads at a time, and with them one commit in a hundred took over half a second.

## On a bucket

```sh
openssl rand -hex 32 > admin.key
export AWS_ACCESS_KEY_ID=<keyID> AWS_SECRET_ACCESS_KEY=<applicationKey>
durable-db serve --bucket my-bucket --endpoint https://s3.us-west-002.backblazeb2.com --key-file admin.key
```

Run as many servers as you like with the same bucket and key file. [Deployment](docs/deployment.md) covers B2, S3, configuration, tenants and metrics.

## On Kubernetes

```sh
kubectl -n durable-db create secret generic durable-db-keys --from-file=admin.key
kubectl -n durable-db create secret generic durable-db-storage --from-literal=DURABLE_DB_BUCKET=... # and endpoint, credentials
kubectl -n durable-db apply -k deploy/kubernetes
```

A Deployment, two Services, an autoscaler and a disruption budget. A pod that is told to stop hands its databases over before it exits; a pod that is killed loses nothing. `scripts/test-kubernetes.sh` checks this on a kind cluster against your bucket: in the last recorded run the autoscaler went from 2 to 5 pods and back, one pod was force-deleted, and a ledger of 20,678 transfers balanced exactly. A read load without retries ran alongside: 110 of its 9 million reads failed, with a pod deleted under them.

## Verification

- **A TLA+ model** of creating a database, taking ownership, committing, linearizable reads, checkpoints, collection and deleting the database. TLC explores it exhaustively at two writers and two epochs: 3.59 million states with two transactions, and 4.11 million with deletion and one transaction. Each of the eleven protocol rules has a negative control: turning the rule off must, and does, produce a counterexample.
- **Fault injection.** Three nodes on storage that delays, drops, half-applies and belatedly applies requests, with crashes and stale owners; 200 random schedules pass. This test found three real bugs in the first implementation. An adversarial review of release 2.0.0 found more, in deletion, in a failed takeover and in cleanup on a versioned bucket. All are fixed, each with a model control or a test that fails on the old code.
- **Real servers and a real bucket.** The HTTP contract, multi-node takeover and handover, the storage contract on B2, and the Kubernetes run above.

Bounded model checking is not a proof of the implementation. [Verification](docs/verification.md) lists the assumptions, what is checked, and what is not.

## Limits and status

Release 2.0.0 was a complete rewrite; earlier releases used a different design and storage format. Release 2.1.0 keeps the format, fixes what a review of 2.0.0 found, and limits what one caller or one database can cost a server. Release 2.1.1 changes how uploads are spread over Backblaze B2's storage pods, which was what made many databases committing at once slow there. The read figures published with 2.0.0 were not those of the binary it shipped: [performance](docs/performance.md) has the correction.

- One server writes a database at a time. Spread load over databases, not over servers per database.
- Transactions do not span databases. There are no historical reads and no point-in-time restore.
- A deleted database's name cannot be reused.
- A database is rewritten in full each time it grows by half. The default quota is 1 GiB; larger databases have not been measured.
- Among cloud stores only Backblaze B2 has been exercised. Amazon S3 and others share the code but are untested.
- Every B2 figure was measured from one desktop over Wi-Fi, a link that itself stalled for a second or two about once a minute; the slowest commits in those tables are such moments, and every commit that shares a stalled request waits with it. A server in a data centre, on a wired link, has not been measured.
- Memory was measured for minutes on a local directory, not for hours and not on a bucket. No soak of many hours on a real multi-node cluster yet, and no independent review.
- The prebuilt binary is Linux x86_64.

## Documentation

| | |
| --- | --- |
| [HTTP API](docs/api.md) | Endpoints, consistency, errors and limits |
| [TypeScript client](clients/javascript/README.md) | The SDK and the pi-durable storage adapter |
| [Example](examples/web-app/README.md) | A task board on Hono |
| [Deployment](docs/deployment.md) | One machine, a bucket, Kubernetes, tenants, metrics |
| [Architecture](docs/architecture.md) | The protocol, step by step |
| [Performance](docs/performance.md) | Measurements and how to reproduce them |
| [Verification](docs/verification.md) | The model, the tests, and what remains |
| [Development](docs/development.md) | Building, testing, layout |

The same pages are published at [ericflo.github.io/durable-db-site](https://ericflo.github.io/durable-db-site/).

Apache-2.0. To report a vulnerability privately, see [SECURITY.md](SECURITY.md).
