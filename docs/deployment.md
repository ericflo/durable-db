# Deployment

durable-db is one static binary. The same binary serves a laptop from a directory and a Kubernetes cluster from a bucket.

## Install

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
```

This downloads the release for Linux x86_64, verifies its checksum and puts `durable-db` in `~/.local/bin` (set `DURABLE_DB_INSTALL_DIR` to change that). On other platforms, [build from source](development.md).

## On one machine

```sh
durable-db serve --data ./data
```

The server listens on `127.0.0.1:7654` and stores everything in `./data`. The first start creates `./data/admin.key`, the signing key for tokens, readable only by you.

```sh
durable-db create my-app      # creates the database, prints a write token
```

Writes are flushed to disk before they are acknowledged. `--no-fsync` skips that for tests and throwaway data.

Several processes may serve the same directory; they coordinate exactly as servers on a bucket do. A directory on a network filesystem is not supported.

## On a bucket

Use one bucket (or one prefix) per deployment and nothing else in it. Do not put lifecycle rules, object lock or replication deletes on it: durable-db removes what it no longer needs, including old versions on a versioned bucket.

Every server of a deployment needs the same signing key:

```sh
openssl rand -hex 32 > admin.key && chmod 600 admin.key
```

### Backblaze B2

Create a private bucket and an application key restricted to it with the capabilities `listBuckets`, `listFiles`, `readFiles`, `writeFiles` and `deleteFiles`.

```sh
export AWS_ACCESS_KEY_ID=<keyID> AWS_SECRET_ACCESS_KEY=<applicationKey>
durable-db serve \
  --bucket my-bucket \
  --endpoint https://s3.us-west-002.backblazeb2.com \
  --key-file admin.key
```

The endpoint is shown on the bucket's page. `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` are accepted in place of the AWS variable names.

Backblaze limits the requests of an account, over all of its buckets: its documentation gives a new account 500 a second for uploads and downloads, and raises that on request. A commit is two to three requests. durable-db keeps itself under no rate, and a benchmark here that ran at six times that figure had every upload refused for five and a half minutes ([performance](performance.md#backblaze-b2)). Keep load tests in an account of their own. Since 2.1.5 a server that is refused holds back: after eight refusals in a row of one kind of request it sends one such request at a time, up to four seconds apart or as far apart as Backblaze asks, until one is taken ([architecture](architecture.md#what-happens-when-things-fail)). That spares the account, and what the server does meanwhile is fail commits. Run 2.1.6 or later: 2.1.5 also took a store that was slow to answer listings to be refusing reads, and did not send it the reads it would have answered ([finding 39](verification.md#what-these-checks-found)).

Reads, lists and deletes use B2's S3-compatible API. Uploads use B2's own upload call with the same key, because it measured two to three times faster than PutObject through B2's S3 gateway; `--s3-uploads-only` turns that off. Each small object is uploaded to the fastest storage pod the server knows, and a second, identical upload races it on another pod when one can be spared, since B2 uploads are free and their latency varies widely between pods. [Performance](performance.md#backblaze-b2) says how pods are chosen.

### Amazon S3 and other S3-compatible storage

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
durable-db serve --bucket my-bucket --endpoint https://s3.us-east-1.amazonaws.com --key-file admin.key
```

For other providers pass `--region` as well. The server signs requests with static credentials from the environment and addresses the bucket path-style. The bucket may keep versions or not. A store that has no request that lists versions, such as Garage, is taken to keep none: what is collected there is removed by key, and the server says so once in its log.

The credentials must be allowed to read, write, list and delete objects, and to list and delete their versions. By Amazon's documentation that is `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:DeleteObjectVersion` on the objects, and `s3:ListBucket` and `s3:ListBucketVersions` on the bucket, whether or not the bucket keeps versions. A server that lacks one of them stops at its storage check and names the request that was refused.

Among cloud providers only Backblaze B2 has been exercised by the test suite and benchmarks so far. Three S3 servers that you can run yourself have been, each on one machine: a community build of MinIO, RustFS and Garage. See [verification](verification.md#what-has-been-run-where).

Amazon S3, Google Cloud Storage and Cloudflare R2 have not been run. What is known of them without an account:

- **Amazon S3** answers listings in the shapes durable-db reads.
- **Google Cloud Storage**, through its S3-compatible endpoint with HMAC keys, does too, since release 2.1.4 asks it to. Turn off soft delete on the bucket: Google's documentation gives it as on for a new bucket, and with it everything durable-db removes is kept, and billed, for seven days more.
- **Cloudflare R2** keeps no versions by its documentation, and is expected to be handled as Garage is.

[Finding 35](verification.md#what-these-checks-found) says what was asked of each and what was not.

At startup the server writes, reads, lists and deletes one probe object and refuses to start if storage does not behave as required.

## On Kubernetes

The manifests in [`deploy/kubernetes`](../deploy/kubernetes) run durable-db as a Deployment with no persistent volume, a Service for applications, a headless Service through which servers find each other, a CPU autoscaler and a disruption budget.

```sh
kubectl create namespace durable-db
kubectl -n durable-db create secret generic durable-db-keys --from-file=admin.key
kubectl -n durable-db create secret generic durable-db-storage \
  --from-literal=DURABLE_DB_BUCKET=my-bucket \
  --from-literal=DURABLE_DB_ENDPOINT=https://s3.us-west-002.backblazeb2.com \
  --from-literal=AWS_ACCESS_KEY_ID=<keyID> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<applicationKey>

cd deploy/kubernetes
kustomize edit set image durable-db=<registry>/durable-db@sha256:<digest>
kubectl -n durable-db apply -k .
```

Applications connect to `http://durable-db.durable-db.svc:7654`. Put TLS and any public exposure in front of it at your ingress. Servers talk to each other in plain HTTP inside the cluster: a relayed request carries its caller's token, which the receiving server verifies again, so the network between them must be private.

**Scaling.** Any number of replicas from one up is correct. When a pod becomes ready, the others hand it the databases that hash to it. When a pod is told to stop, it reports not-ready, hands every database over, relays stray requests to its peers for `--drain-seconds`, and exits. A pod that is killed outright loses nothing: the next request for one of its databases makes another pod take it over. Scaling to zero stops service and loses nothing; starting one pod restores it.

**Upgrades** are ordinary rolling updates. Servers of different versions can run side by side as long as they share a storage format; a release that changes the format will say so. Every release from 2.0.0 to 2.1.7 shares one, and `scripts/test-upgrade.sh` checks the upgrade between two binaries under a ledger ([verification](verification.md#tests-of-the-implementation)). While 2.0.0 serves beside a later version, the later server does not take the 2.0.0 server's word that a request was relayed and may relay it once more, and the 2.0.0 server does not know the mark that a later server puts on a database it is deleting, so it could take that database over and serve it until the deletion completes. Delete databases before or after an upgrade from 2.0.0, not during it. Between 2.1.0 and 2.1.1 neither applies.

**Resources.** What a server holds in memory is limited by its flags, not by the size or the number of its databases:

| What | At most |
| --- | --- |
| Table blocks and indexes | `--cache-bytes` |
| Unflushed writes | `--memtable-bytes`, and as much again while they are written to tables |
| Recently written keys, remembered for range checks | a quarter of `--memtable-bytes` |
| Request bodies | `--body-bytes` |
| Read results | `--result-bytes` |
| Merges | two at a time, one table of 32 MiB each |
| Tables being fetched for the disk cache | two at a time, 64 MiB each |
| Connections | about 40 KiB each until a request is admitted, `--max-connections` of them |

The manifest sets 192 MiB of cache, 96 MiB of unflushed writes, 128 MiB each for bodies and results, and 1,024 connections. With every line at its limit at once that is about 900 MiB, under a memory limit of 1 GiB. Change the limit and the flags together. These are the limits the code enforces; a budget can be passed by one request's worth before it is enforced, and a few kilobytes for each open database are not counted. Measured: a server with these flags, held to 1 GiB and four CPUs and loaded for three minutes on a local directory, used 621 MiB at most, and between 618 and 659 MiB in runs of the release before ([performance](performance.md#memory-under-load)). When flushes fall behind, a database answers `503` to writes until they have caught up, instead of holding more. Longer loads, and the same loads on a bucket, have not been measured. The cache volume is an `emptyDir`; losing it costs bucket requests, not data. The manifest gives it 1 GiB (`--cache-dir-bytes`) in a volume of 2 GiB. That suits databases that are together smaller than it. A server that reads databases larger than its disk cache fetches whole tables over and over in each other's place, which on a bucket is download at the speed of the link ([verification](verification.md#what-these-checks-found), finding 43, not fixed in this release): raise `--cache-dir-bytes` and the volume with the quotas you give out, so that the cache holds what a server reads. The disk cache costs no memory.

## Configuration

Every option is a flag and an environment variable.

| Flag | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` | `DURABLE_DB_LISTEN` | `127.0.0.1:7654` | Address to listen on. |
| `--data` | `DURABLE_DB_DATA` | `data` | Directory storage. Used when no bucket is given. |
| `--bucket` | `DURABLE_DB_BUCKET` | | Bucket storage. Needs `--endpoint`. |
| `--endpoint` | `DURABLE_DB_ENDPOINT` | | The bucket's S3 endpoint URL. |
| `--region` | `DURABLE_DB_REGION` | from endpoint | Signing region. |
| `--prefix` | `DURABLE_DB_PREFIX` | | Key prefix inside the bucket. |
| `--key-file` | `DURABLE_DB_KEY_FILE` | `<data>/admin.key` | Token signing key. Required with a bucket. |
| `--peers` | `DURABLE_DB_PEERS` | | DNS name resolving to every server, or a list of `host:port`. |
| `--advertise` | `DURABLE_DB_ADVERTISE` | | This server's `host:port` as peers reach it. Required with `--peers`. |
| `--cache-bytes` | `DURABLE_DB_CACHE_BYTES` | 256 MiB | Memory for cached table blocks and indexes. |
| `--cache-dir` | `DURABLE_DB_CACHE_DIR` | | Directory for a disk cache of tables. |
| `--cache-dir-bytes` | `DURABLE_DB_CACHE_DIR_BYTES` | 4 GiB | Size of the disk cache. |
| `--memtable-bytes` | `DURABLE_DB_MEMTABLE_BYTES` | 256 MiB | Unflushed writes across all databases before the largest are flushed. A quarter of it is also what all databases together may remember of recently written keys. |
| `--max-open` | `DURABLE_DB_MAX_OPEN` | 4096 | Databases kept open. |
| `--idle-seconds` | `DURABLE_DB_IDLE_SECONDS` | 300 | Release a database unused for this long. |
| `--requests-per-second` | `DURABLE_DB_REQUESTS_PER_SECOND` | 0 (unlimited) | Per-database request budget unless the database sets its own. |
| `--max-value-bytes` | `DURABLE_DB_MAX_VALUE_BYTES` | 4 MiB | Largest value accepted. |
| `--body-bytes` | `DURABLE_DB_BODY_BYTES` | 512 MiB | Memory for request bodies, across all requests. One database may use a quarter. |
| `--result-bytes` | `DURABLE_DB_RESULT_BYTES` | 512 MiB | Memory for read results being put together or waiting to be sent. One database may use a quarter; one read returns at most an eighth, up to 64 MiB. |
| `--max-connections` | `DURABLE_DB_MAX_CONNECTIONS` | 8192 | Connections open at once. Further ones wait to be accepted. |
| `--drain-seconds` | `DURABLE_DB_DRAIN_SECONDS` | 5 | Time to relay requests after a termination signal. |
| `--upload-attempts` | `DURABLE_DB_UPLOAD_ATTEMPTS` | 4 on B2, else 2 | Identical uploads that may race for one small object. On B2 a second starts with the first when the server has an upload URL to spare; elsewhere one starts. |
| `--hedge-ms` | `DURABLE_DB_HEDGE_MS` | 150 | How long the uploads of an object may take before one more is started beside them. |
| `--s3-uploads-only` | `DURABLE_DB_S3_UPLOADS_ONLY` | off | Use PutObject on Backblaze too. |
| `--no-fsync` | `DURABLE_DB_NO_FSYNC` | off | Directory storage only: do not flush before acknowledging. |

Credentials come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. `RUST_LOG` sets the log level (`info` by default). A line that contains `panicked` is a defect in durable-db, whatever else the server goes on doing: please report it. So is one that says a round of housekeeping `has not ended`. A warning that begins `the store refuses` says that the server has stopped sending one kind of request (reads, listings, changes, or on Backblaze uploads or calls of its API) because the store refused eight in a row, and carries the store's status, its error code and the wait it asked for; `the store takes` that kind `again` ends it.

## Tokens and tenants

One deployment serves many databases. Give each tenant, site or application its own database and its own token:

```sh
durable-db create tenant-42 --max-bytes 268435456        # prints a write token
curl -X POST $URL/v1/databases/tenant-42/tokens \
  -H "Authorization: Bearer $(durable-db token --admin)" \
  -d '{"access": "read", "ttl_seconds": 3600}'
```

What keeps one tenant from harming another:

- **Access.** A database token works on its database only. The token is verified, and matched against the database in the path, before a request's body is read and before storage or another server is asked anything. A request without a valid token for that database is refused from its head alone.
- **Storage.** `max_bytes` per database; writes that add data beyond it are refused.
- **Request rate.** `max_requests_per_second` per database, or `--requests-per-second` for all.
- **Request size.** Key, value, request and page limits are enforced before any work is done ([limits](api.md#limits)). The items of a request are counted while it is parsed, and a read is stopped when its result passes the limit, not after it has been put together.
- **Memory.** The cache of table blocks and indexes is bounded for the whole server. A database whose unflushed writes exceed four times the flush size gets `503` until a flush catches up, whether it is writing or deleting. Request bodies are reserved for in `--body-bytes` before they are read, and read results in `--result-bytes` as they grow and until the client has taken them; a result is counted twice while it is encoded, and results under 16 KiB are not counted. One database may hold a quarter of either budget. When its quarter or the whole is spent, its next request is answered `503` and the others carry on. At most two table merges run at once, each holding one table of 32 MiB; a database waiting for its turn is held up in nothing else. The keys a database remembers for range checks are limited to 8 MiB, and to a share of the server's budget when many databases write at once. A database that has to forget some has its older range checks refused with `409`, and their transactions read again.
- **Connections.** `--max-connections` at once; a connection has 75 seconds to send a request's head, a body 30 seconds plus one per 256 KiB, and a client that takes nothing of its response for 30 seconds is disconnected and the response dropped. These limits bound memory. They do not stop someone from occupying every connection: limit connections per client at your ingress.
- **Revocation.** `{"revoke_tokens": true}` voids every token of a database at once.

Tokens belong on your servers, not in browsers. To rotate the signing key, put the new key on the first line of the key file and keep the old one below it until tokens signed with it have expired; all servers must have the file before the new key is used.

What it does not do: per-tenant CPU accounting, or billing. A tenant inside its limits can still use its full request rate around the clock.

## Operations

`GET /metrics` exposes, in Prometheus text format:

| Metric | Meaning |
| --- | --- |
| `durable_db_request_seconds{kind}` | Latency histogram for `read`, `commit`, `admin` and `open` (taking ownership of a database). |
| `durable_db_request_failures_total{kind}` | Requests of that kind that failed. |
| `durable_db_storage_seconds{op}` | Latency histogram of storage calls: `put`, `get`, `exists`, `list`, `delete`, `purge`. |
| `durable_db_storage_failures_total{op}` | Storage calls that failed. |
| `durable_db_storage_sent_bytes_total`, `..._received_bytes_total` | Bytes uploaded and downloaded. |
| `durable_db_forwarded_requests_total` | Requests relayed to the owning server. |
| `durable_db_rejected_requests_total` | Requests refused with `503` because the server, or the database's part of it, had no room: requests in flight, request bodies, or read results. |
| `durable_db_request_body_bytes`, `durable_db_read_result_bytes` | Memory reserved right now for request bodies and for read results, against `--body-bytes` and `--result-bytes`. |
| `durable_db_open_databases` | Databases this server holds. |
| `durable_db_block_cache_bytes`, `..._hits_total`, `..._misses_total` | Memory cache use. |
| `durable_db_table_index_bytes` | Table indexes in memory, in the cache or in use by a read. |
| `durable_db_recent_key_bytes` | Recently written keys remembered for range checks, by all open databases. |

Watch storage latency and failures first: every commit is one upload and one HEAD, so they are the service's latency floor.

`durable-db bench` measures a running deployment, and `durable-db bench --workload ledger` checks it: it moves money between accounts while you add, remove or kill servers, then verifies that no transfer was lost or applied twice. See [performance](performance.md).

**Backups.** The bucket is the database, so protect the bucket: restrict its keys, and use your provider's replication to a second bucket or region if you need protection against losing it. durable-db keeps only the current state; it has no point-in-time restore, and deletions it performs are permanent.
