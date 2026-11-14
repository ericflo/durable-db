# Performance

Every number here was measured with `durable-db bench`, which is part of the binary, on 2026-11-14 with release 2.0.0. The load is closed-loop: each client sends one request and waits for the answer, so latency is what a caller observes and throughput is completed requests over wall time. Values are 256 bytes of JSON. Reproduce any table with:

```sh
scripts/benchmark.sh          # against DURABLE_DB_URL, using DURABLE_DB_KEY_FILE
```

## What a request costs

| Request | Storage work | Typical time |
| --- | --- | --- |
| Commit | One upload and one HEAD, shared by every transaction in the same group | The store's upload time plus a HEAD |
| Strong read | One HEAD, shared by every read waiting at that moment | The store's HEAD time |
| Session read | None | Memory speed |
| Opening a database | About six small requests and two uploads | A few hundred milliseconds on a bucket |

Throughput on one database comes from grouping: all transactions that arrive during an upload ride the next one. More clients mean larger groups, not more uploads.

## Local directory

One server, `durable-db serve --data`, writes flushed to disk before they are acknowledged. A 32-core desktop with a consumer NVMe drive on which one flush takes about 3 ms; a commit needs two (the file and its directory entry).

| Workload | Requests/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| Commit, 1 client | 173 | 5.9 ms | 11.0 ms |
| Commit, 32 clients, one database | 2,168 | 14.8 ms | 19.9 ms |
| Commit, 256 clients, one database | 17,166 | 14.9 ms | 20.7 ms |
| Commit, 64 clients over 64 databases | 5,310 | 12.0 ms | 15.1 ms |
| Commit with an idempotency key, 32 clients | 2,154 | 14.9 ms | 20.2 ms |
| Strong read, 32 clients | 194,523 | 0.14 ms | 0.44 ms |
| Strong read, 256 clients | 335,187 | 0.70 ms | 1.79 ms |
| Session read, 32 clients | 276,569 | 0.10 ms | 0.35 ms |
| Strong scan of 20 keys, 32 clients | 239,280 | 0.12 ms | 0.34 ms |
| Half reads, half commits, 64 clients over 8 databases | 8,603 | 0.49 ms | 20.8 ms |

Commit latency is the disk's flush time and does not grow with load; throughput grows with clients because each flush carries more transactions.

## Backblaze B2

One server on the same desktop, on a residential connection 22 ms from B2's `us-west-002` region, with default settings.

| Workload | Requests/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| Commit, 1 client | 7 | 118 ms | 396 ms |
| Commit, 32 clients, one database | 156 | 186 ms | 452 ms |
| Commit, 256 clients, one database | 866 | 289 ms | 532 ms |
| Commit, 64 clients over 64 databases | 314 | 162 ms | 791 ms |
| Commit with an idempotency key, 32 clients | 175 | 161 ms | 411 ms |
| Strong read, 32 clients | 690 | 43 ms | 165 ms |
| Strong read, 256 clients | 6,342 | 39 ms | 126 ms |
| Session read, 32 clients | 244,743 | 0.11 ms | 0.39 ms |
| Strong scan of 20 keys, 32 clients | 701 | 38 ms | 270 ms |
| Half reads, half commits, 64 clients over 8 databases | 601 | 76 ms | 361 ms |

**B2's own latency sets these numbers, and it varies.** During this run, individual uploads to B2 took 130 ms at the median and 6% took longer than a second; HEAD requests took about 30 ms. An hour earlier the same benchmark measured a single-client commit at 77 ms and 175 commits/s at 32 clients. Expect a factor of two between good and bad hours, and less latency from a server closer to the bucket.

Two things keep B2's slow moments from becoming yours:

- **Racing uploads.** B2 stores each upload on one of many storage pods, and pods differ: the same 4 KiB upload measured medians from 67 ms to over 900 ms on eight pods at one moment. durable-db remembers which pods were fast, starts two identical uploads at once (a known-fast pod and a new one), and adds another every 150 ms while none has finished. Uploads are free on B2. With a single upload per commit, the 32-client p99 in a bad period was 2.2 s; with racing it was 0.45 s.
- **B2's native upload call.** PutObject through B2's S3-compatible gateway measured 150 to 260 ms at the median for the same object; B2's own upload call measured 71 ms. Uploads use the native call; everything else uses the S3 API.

### Opening a database

A database is opened by the first request that needs it and stays open while it is used (five minutes idle by default).

| Situation | Time to first answer |
| --- | --- |
| Warm server, database was released cleanly | 0.25 to 0.45 s, 0.3 s typical |
| Warm server, creating a database | 0.2 to 0.45 s |
| Brand-new server process, database was released cleanly | 1.7 s (includes new connections and a B2 upload session) |
| Brand-new server process, previous owner was killed with 30 unflushed commits | 1.3 s |
| Server start to ready, including the storage check | 1.1 s |

### On Kubernetes

From `scripts/test-kubernetes.sh` on a three-node kind cluster on the same desktop, backed by the same B2 bucket, through the Service: 30,114 session reads/s from 192 clients with no errors while the autoscaler added a pod; a ledger of read-then-commit transfers from 16 clients over 8 databases ran at 78 transfers/s (p50 150 ms, p99 717 ms) for four minutes through a scale-out, a force-deleted pod and a scale-in. The slowest transfer, during the forced deletion, took 9.9 s.

## What it costs in requests

Uploads are free on B2; HEAD and GET are billed per call, LIST at a higher rate. Per request:

| Request | Billable calls |
| --- | --- |
| Commit | One HEAD per group of transactions; uploads (two to four racing on B2) |
| Strong read | One HEAD per group of concurrent reads, at most two groups in flight |
| Session read from cache | None |
| Flush (every 8 MiB, 512 groups, or 5 s of quiet) | Two uploads, one HEAD, two version listings, deletes |
| Opening a database | Six or seven listings (two of them a background sweep for leftovers), one GET, one HEAD, two uploads |
| An idle database | Nothing |

A database committing as fast as B2 allows from one location runs about ten groups a second, which is under a million HEADs a day. Check your provider's current price list for what that means on your bill.

## What is not measured yet

- Amazon S3 or any provider other than B2.
- A server in the same region as its bucket.
- Many hours of sustained load.
- Databases larger than a few hundred megabytes.
