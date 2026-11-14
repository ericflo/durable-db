# Performance

Every number here was measured with `durable-db bench`, which is part of the binary, on 2026-11-14 with the static binary of release 2.1.0 as the server and as the load generator. The load is closed-loop: each client sends one request and waits for the answer, so latency is what a caller observes and throughput is completed requests over wall time. Values are 256 bytes of JSON. Reproduce any table with:

```sh
scripts/benchmark.sh          # against DURABLE_DB_URL, using DURABLE_DB_KEY_FILE
```

**A correction to release 2.0.0.** The read figures published with it were measured with a build linked with glibc, not with the static binary the release shipped. That binary reads about a tenth as fast wherever a read does not wait for storage. Measured again with the release binary itself on a local directory: 21,376 strong reads/s from 32 clients where 194,523 were published, 18,305 from 256 clients where 335,187 were published, and 25,889 session reads/s where 276,569 were published. Its commits were within what one run differs from the next, and a request that waits for a bucket is limited by the bucket. [The allocator](#the-allocator) has the cause and what was done.

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
| Commit, 1 client | 186 | 5.2 ms | 10.6 ms |
| Commit, 32 clients, one database | 2,164 | 14.7 ms | 20.5 ms |
| Commit, 256 clients, one database | 17,067 | 14.9 ms | 21.3 ms |
| Commit, 64 clients over 64 databases | 5,488 | 12.0 ms | 14.2 ms |
| Commit with an idempotency key, 32 clients | 2,136 | 15.0 ms | 20.1 ms |
| Strong read, 32 clients | 253,402 | 0.11 ms | 0.38 ms |
| Strong read, 256 clients | 379,480 | 0.63 ms | 1.49 ms |
| Session read, 32 clients | 301,523 | 0.09 ms | 0.33 ms |
| Strong scan of 20 keys, 32 clients | 244,313 | 0.11 ms | 0.40 ms |
| Half reads, half commits, 64 clients over 8 databases | 7,812 | 7.9 ms | 21.5 ms |

Commit latency is the disk's flush time and does not grow with load; throughput grows with clients because each flush carries more transactions. The median of the last row lies between its reads, which take a tenth of a millisecond, and its commits, which take fifteen, and so moves from run to run: 0.5, 1.6 and 7.9 ms were measured. Commits from 256 clients also vary between runs, from 12,600 to 18,200 a second across the builds measured for this page.

## Backblaze B2

One server on the same desktop, on a residential connection 22 ms from B2's `us-west-002` region, with default settings.

| Workload | Requests/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| Commit, 1 client | 11 | 76 ms | 211 ms |
| Commit, 32 clients, one database | 174 | 174 ms | 565 ms |
| Commit, 256 clients, one database | 1,117 | 199 ms | 495 ms |
| Commit, 64 clients over 64 databases | 177 | 169 ms | 2,370 ms |
| Commit with an idempotency key, 32 clients | 181 | 165 ms | 382 ms |
| Strong read, 32 clients | 859 | 36 ms | 121 ms |
| Strong read, 256 clients | 6,610 | 38 ms | 116 ms |
| Session read, 32 clients | 263,257 | 0.10 ms | 0.38 ms |
| Strong scan of 20 keys, 32 clients | 869 | 35 ms | 153 ms |
| Half reads, half commits, 64 clients over 8 databases | 614 | 90 ms | 397 ms |

**B2's own latency sets these numbers, and it varies.** The table is one run. The run published with release 2.0.0, some hours earlier on the same day, measured 118 ms for a single-client commit where this one measured 76 ms, and 866 commits/s from 256 clients where this one measured 1,117. Expect a factor of two between good and bad hours, and less latency from a server closer to the bucket.

**Many databases committing at once is where B2 was slowest.** Clients of one database share an upload; 64 databases need 64 uploads at a time, each to a storage pod of its own. In this run half of the server's upload attempts took under 250 ms and a quarter took over a second, and one commit in a hundred waited 2.4 s. The earlier run measured 314 commits/s and a 99th percentile of 791 ms on this row. To tell the hour from the release, the two binaries were then run on this row in turn, four times each: 2.0.0 measured 228, 207, 131 and 221 commits/s, and 2.1.0 measured 247, 147, 221 and 111. They do not differ. Why B2 served many simultaneous uploads so much more slowly in this hour than in the earlier one was not established.

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

From `scripts/test-kubernetes.sh` on a three-node kind cluster on the same desktop, backed by the same B2 bucket, through the Service: 163,283 session reads/s from 192 clients for 80 seconds, during which the autoscaler went from two pods to five and one pod was force-deleted. Of those 13 million reads, 102 failed and 46 were told to wait; the load generator does not repeat a read, and a force-deleted pod breaks the connections it holds. A ledger of read-then-commit transfers from 16 clients over 8 databases ran at 87 transfers/s (p50 120 ms, p99 647 ms) for four minutes through the scale-out, the force-deleted pod and the scale-in, and verified. Its slowest transfer took 14.6 s.

With release 2.0.0 the same test measured 30,114 session reads/s and the autoscaler stopped at three pods: its binary read at the speed of one core ([the allocator](#the-allocator)).

## The allocator

The binary carries its own allocator, jemalloc, held to eight arenas and set to give freed memory back to the system within a second. Which allocator a server uses decided two things: how fast it reads, and whether it stays inside a memory limit. This source was built with each candidate and put through `scripts/benchmark.sh` on a local directory and through the soak described [below](#memory-under-load), on the desktop described above:

| Allocator | Strong reads/s, 32 clients | Most memory in the soak, limit 1,024 MiB |
| --- | ---: | --- |
| musl's own, in a static binary, as release 2.0.0 was built | 21,673 | 446 MiB |
| glibc's, in a dynamically linked build | 274,550 | killed at the limit |
| mimalloc, in a static binary | 269,154 | killed at the limit |
| jemalloc as configured, in a static binary: release 2.1.0 | 253,402 | 618 MiB, and 659 MiB in an earlier run |

musl's allocator takes one lock for all threads, so a server with it reads at the speed of one core. glibc's keeps freed memory in an arena for each thread: without a limit it held 1,481 MiB under the soak's first load, and 446 MiB when it was told to use one arena, which made its reads slow as well. The three fast ones are within what one run differs from the next. Commits did not differ by allocator beyond that, since a commit waits for storage.

To repeat the comparison, take the `ALLOCATOR` and `MALLOC_CONF` items out of `src/main.rs` or name another allocator there, build the static binary as the `Dockerfile` does, and run the two scripts with it.

## Memory under load

```sh
DURABLE_DB=./durable-db scripts/soak-memory.sh
```

One server with the flags of the Kubernetes manifest (192 MiB of cache, 96 MiB of unflushed writes, 128 MiB each for request bodies and results), on a local directory, in a systemd scope limited to 1 GiB of memory without swap and to four CPUs. It is killed if it passes the limit. Three loads of a minute each run one after the other against the same server, with values of random text, which does not compress:

| Load | Requests/s | p50 | p99 | Told to wait | Most memory so far |
| --- | ---: | ---: | ---: | ---: | ---: |
| Half reads, half commits of 16 KiB values, 64 clients over 4 databases | 5,439 | 1.6 ms | 72 ms | 5,640 | 544 MiB |
| Commits of 1 KiB values, 256 clients over 256 databases | 6,719 | 21 ms | 145 ms | 0 | 618 MiB |
| Strong reads of 4 KiB values, 512 clients over 8 databases of 20,000 keys | 70,859 | 5.9 ms | 31 ms | 0 | 618 MiB |

The server used 618 MiB at most, and 659 MiB in an earlier run of the same script. No request failed.

"Told to wait" counts answers of `503`. In the first load each of the four databases holds 160 MiB and is overwritten about four times in the minute, so tables are merged all the while; with four CPUs the flushes fall behind now and then, and the server refuses a write to that database instead of holding it in memory. That was 1.7% of the requests of that load. A client waits and sends the request again, as the SDK does.

This is three minutes on a local directory. It says that the limits in [deployment](deployment.md#on-kubernetes) hold under these loads with this allocator, not what many hours or a bucket would show; on a bucket an upload is in memory for as long as the bucket takes to store it.

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

- Why many simultaneous uploads to B2 are slow in some hours, and whether fewer racing uploads would serve many busy databases better.
- Amazon S3 or any provider other than B2.
- A server in the same region as its bucket.
- Many hours of sustained load.
- Databases larger than a few hundred megabytes.
