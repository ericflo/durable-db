# Deployment

durable-db is one static binary. The same binary serves a laptop from a directory and a Kubernetes cluster from a bucket.

## Install

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
```

This downloads the release for Linux x86_64, verifies its checksum and puts `durable-db` in `~/.local/bin` (set `DURABLE_DB_INSTALL_DIR` to change that). While the repository is private the script uses the `gh` CLI, which must be signed in to an account with access. On other platforms, [build from source](development.md).

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

Reads, lists and deletes use B2's S3-compatible API. Uploads use B2's own upload call with the same key, because it measured two to three times faster than PutObject through B2's S3 gateway; `--s3-uploads-only` turns that off. Two identical uploads race for each small object, since B2 uploads are free and their latency varies widely between storage pods.

### Amazon S3 and other S3-compatible storage

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
durable-db serve --bucket my-bucket --endpoint https://s3.us-east-1.amazonaws.com --key-file admin.key
```

For other providers pass `--region` as well. The server signs requests with static credentials from the environment and addresses the bucket path-style. Only Backblaze B2 has been exercised by the test suite and benchmarks so far; see [verification](verification.md#what-has-been-run-where).

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

Applications connect to `http://durable-db.durable-db.svc:7654`. Put TLS and any public exposure in front of it at your ingress; servers talk to each other in plain HTTP inside the cluster and trust the network they are on.

**Scaling.** Any number of replicas from one up is correct. When a pod becomes ready, the others hand it the databases that hash to it. When a pod is told to stop, it reports not-ready, hands every database over, relays stray requests to its peers for `--drain-seconds`, and exits. A pod that is killed outright loses nothing: the next request for one of its databases makes another pod take it over. Scaling to zero stops service and loses nothing; starting one pod restores it.

**Upgrades** are ordinary rolling updates. Servers of different versions can run side by side as long as they share a storage format; a release that changes the format will say so.

**Resources.** Memory use is bounded by `--cache-bytes` (table blocks) plus `--memtable-bytes` (unflushed writes) plus working memory for requests and merges. The manifest's 1 GiB limit fits its 256 MiB + 128 MiB settings. The cache volume is an `emptyDir`; losing it costs bucket requests, not data.

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
| `--cache-bytes` | `DURABLE_DB_CACHE_BYTES` | 256 MiB | Memory for cached table blocks. |
| `--cache-dir` | `DURABLE_DB_CACHE_DIR` | | Directory for a disk cache of tables. |
| `--cache-dir-bytes` | `DURABLE_DB_CACHE_DIR_BYTES` | 4 GiB | Size of the disk cache. |
| `--memtable-bytes` | `DURABLE_DB_MEMTABLE_BYTES` | 256 MiB | Unflushed writes across all databases before the largest are flushed. |
| `--max-open` | `DURABLE_DB_MAX_OPEN` | 4096 | Databases kept open. |
| `--idle-seconds` | `DURABLE_DB_IDLE_SECONDS` | 300 | Release a database unused for this long. |
| `--requests-per-second` | `DURABLE_DB_REQUESTS_PER_SECOND` | 0 (unlimited) | Per-database request budget unless the database sets its own. |
| `--max-value-bytes` | `DURABLE_DB_MAX_VALUE_BYTES` | 4 MiB | Largest value accepted. |
| `--drain-seconds` | `DURABLE_DB_DRAIN_SECONDS` | 5 | Time to relay requests after a termination signal. |
| `--upload-attempts` | `DURABLE_DB_UPLOAD_ATTEMPTS` | 2 | Identical uploads that may race for one small object. |
| `--hedge-ms` | `DURABLE_DB_HEDGE_MS` | 0 on B2, else 150 | Delay before each further racing upload. |
| `--s3-uploads-only` | `DURABLE_DB_S3_UPLOADS_ONLY` | off | Use PutObject on Backblaze too. |
| `--no-fsync` | `DURABLE_DB_NO_FSYNC` | off | Directory storage only: do not flush before acknowledging. |

Credentials come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. `RUST_LOG` sets the log level (`info` by default).

## Tokens and tenants

One deployment serves many databases. Give each tenant, site or application its own database and its own token:

```sh
durable-db create tenant-42 --max-bytes 268435456        # prints a write token
curl -X POST $URL/v1/databases/tenant-42/tokens \
  -H "Authorization: Bearer $(durable-db token --admin)" \
  -d '{"access": "read", "ttl_seconds": 3600}'
```

What keeps one tenant from harming another:

- **Access.** A database token works on its database only. Tokens are verified before anything touches storage, so requests without a valid token cost nothing but CPU.
- **Storage.** `max_bytes` per database; writes that add data beyond it are refused.
- **Request rate.** `max_requests_per_second` per database, or `--requests-per-second` for all.
- **Request size.** Key, value, transaction and page limits are enforced before any work is done ([limits](api.md#limits)).
- **Memory.** The block cache is bounded for the whole server. A database whose unflushed writes exceed four times the flush size gets `503` until a flush catches up. Request bodies in memory are budgeted across all requests (512 MiB), and one read returns at most 64 MiB. At most two table merges run at once.
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
| `durable_db_rejected_requests_total` | Requests refused because the server was at its request limit. |
| `durable_db_open_databases` | Databases this server holds. |
| `durable_db_block_cache_bytes`, `..._hits_total`, `..._misses_total` | Memory cache use. |

Watch storage latency and failures first: every commit is one upload and one HEAD, so they are the service's latency floor.

`durable-db bench` measures a running deployment, and `durable-db bench --workload ledger` checks it: it moves money between accounts while you add, remove or kill servers, then verifies that no transfer was lost or applied twice. See [performance](performance.md).

**Backups.** The bucket is the database, so protect the bucket: restrict its keys, and use your provider's replication to a second bucket or region if you need protection against losing it. durable-db keeps only the current state; it has no point-in-time restore, and deletions it performs are permanent.
