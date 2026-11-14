# Performance

Every number here was measured with `durable-db bench`, which is part of the binary, on 2026-11-14 with the static binary of release 2.1.1 as the server and as the load generator, except where a table names another release. The load is closed-loop: each client sends one request and waits for the answer, so latency is what a caller observes and throughput is completed requests over wall time. Values are 256 bytes of JSON. Reproduce any table with:

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
| Commit, 1 client | 161 | 6.0 ms | 11.0 ms |
| Commit, 32 clients, one database | 2,105 | 15.0 ms | 20.9 ms |
| Commit, 256 clients, one database | 16,541 | 15.3 ms | 22.0 ms |
| Commit, 64 clients over 64 databases | 4,936 | 12.0 ms | 31.3 ms |
| Commit with an idempotency key, 32 clients | 2,098 | 15.1 ms | 20.9 ms |
| Strong read, 32 clients | 245,847 | 0.11 ms | 0.38 ms |
| Strong read, 256 clients | 378,213 | 0.64 ms | 1.51 ms |
| Session read, 32 clients | 297,289 | 0.09 ms | 0.34 ms |
| Strong scan of 20 keys, 32 clients | 246,678 | 0.11 ms | 0.38 ms |
| Half reads, half commits, 64 clients over 8 databases | 7,988 | 0.43 ms | 21.7 ms |

Commit latency is the disk's flush time and does not grow with load; throughput grows with clients because each flush carries more transactions. The median of the last row lies between its reads, which take a tenth of a millisecond, and its commits, which take fifteen, and so moves from run to run: 0.4, 0.5, 1.6 and 7.9 ms were measured. Commits from 256 clients also vary between runs, from 12,600 to 18,200 a second across the builds measured for this page.

Nothing in release 2.1.1 changes what a local directory does, and two rows of this table differed from the run published with 2.1.0, which measured 186 commits/s from one client and a p99 of 14.2 ms on the fourth row. The two releases were therefore run on those rows in turn, on a new server and directory each time. One client, three rounds: 171, 177 and 166 commits/s with 2.1.0, and 180, 172 and 182 with 2.1.1. 64 databases, eight rounds: 5,177 to 5,340 commits/s with 2.1.0 and 5,282 to 5,536 with 2.1.1, apart from one run of 7,499 whose median was 7 ms instead of 12. The p99 was 15 to 16 ms in seven rounds with 2.1.0 and 25 ms in the eighth; with 2.1.1 it was 14 to 19 ms in five rounds and 23, 25 and 37 ms in the others. These rows are the drive's flush time, and it moves by that much from run to run.

## Backblaze B2

One server on the same desktop, which reaches the internet over Wi-Fi and a residential connection, 22 ms from B2's `us-west-002` region, with default settings.

| Workload | Requests/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| Commit, 1 client | 12 | 77 ms | 192 ms |
| Commit, 32 clients, one database | 183 | 128 ms | 2,730 ms |
| Commit, 256 clients, one database | 1,881 | 128 ms | 223 ms |
| Commit, 64 clients over 64 databases | 534 | 88 ms | 563 ms |
| Commit with an idempotency key, 32 clients | 217 | 117 ms | 918 ms |
| Strong read, 32 clients | 804 | 38 ms | 116 ms |
| Strong read, 256 clients | 6,480 | 37 ms | 166 ms |
| Session read, 32 clients | 276,223 | 0.10 ms | 0.36 ms |
| Strong scan of 20 keys, 32 clients | 845 | 37 ms | 91 ms |
| Half reads, half commits, 64 clients over 8 databases | 781 | 74 ms | 257 ms |

**B2's own latency sets these numbers, and it varies.** The table is one run. Earlier the same day the run published with release 2.0.0 measured 118 ms for a single-client commit where this one measured 77 ms. Expect a factor of two between good and bad hours, and less latency from a server closer to the bucket. Where this page compares two releases, they were run in turn within the same half hour.

**Many databases committing at once was where B2 was slowest, and the cause was in durable-db.** Clients of one database share an upload; 64 databases need 64 uploads at a time. A B2 upload URL names one storage pod and serves one upload at a time, so each of them needs a pod of its own. Up to release 2.1.0 every commit also sent one of its two racing uploads to a pod the server had never used. A new pod costs a call for its URL, a name lookup and a TLS connection, and 64 busy databases asked for about 160 new pods a second. From this desktop, one such upload in fourteen failed after waiting five seconds for a connection and the others took 0.8 s at the median, while uploads to pods already in use took 76 ms. Since 2.1.1 the upload a commit waits for goes to the fastest pod the server knows, and a duplicate is sent only where a pod can be spared ([verification](verification.md#what-these-checks-found), finding 27).

The two releases were run on this row in turn, three times each. Every time a new server was started and the row was run twice on it, ten seconds each:

| 64 clients over 64 databases | Release 2.1.0 | Release 2.1.1 |
| --- | --- | --- |
| Commits/s in a new server's first ten seconds | 187, 174, 229 | 427, 406, 374 |
| Commits/s in its next ten seconds | 192, 180, 207 | 500, 507, 504 |
| p99 of a commit, first ten seconds | 2.1, 2.3, 1.8 s | 1.1, 1.0, 0.8 s |
| p99 of a commit, next ten seconds | 2.0, 2.0, 1.8 s | 0.58, 0.58, 0.56 s |
| p99.9 of a commit, all six runs | 2.9 to 4.3 s | 1.6 to 3.6 s |
| Upload attempts that took over a second | 26%, 28%, 24% | 1%, 1%, 1% |
| Upload attempts that failed | 732 of 38,285 | 205 of 36,023 |

No commit failed in any run. A new server knows no pods, so its first seconds are its slowest: the first uploads of all 64 databases go to new pods at once. The attempts that still fail with 2.1.1 are of two kinds, and each is sent again or has another upload beside it: a pod answers at once that it cannot take the upload, which B2 does routinely and after which durable-db drops that pod, and an upload to a new pod gives up after five seconds without a connection. In a run that recorded them, four in five were of the first kind. To repeat a column, start a server of that release and run `durable-db bench --workload write --clients 64 --databases 64` against it twice.

This desktop keeps no cache of name lookups, so every new connection asks its provider's resolvers, and it does so over Wi-Fi. That made new pods dearer here than they may be for a server in a data centre, and it has not been measured from one.

**The slowest commits in these tables are stalls of the desktop's link, not of B2 and not of durable-db.** The desktop is on Wi-Fi. A ping to its own router, five a second for 80 seconds, had 12 of 394 replies delayed by more than half a second, the longest by 2.4 s. A build that logged every request showed what such a moment is to the server: in one, four uploads to four different pods and a name lookup all took over a second at once; in another, a claim check and an unrelated upload did. One request to the bucket carries every commit of its group, so a row of ten seconds with a stall in it reports the stall as its 99th percentile. In the table above that is the 2.7 s of the second row and the 0.9 s of the row with idempotency keys.

With one database the two releases were run in turn for 30 seconds each, three times, on a new server each time, with the same ping running alongside:

| One database, 30 seconds | Release 2.1.0 | Release 2.1.1 |
| --- | --- | --- |
| 1 client, commits/s | 11, 9, 11 | 13, 12, 12 |
| 1 client, median | 81, 93, 80 ms | 64, 76, 72 ms |
| 1 client, p99 | 194, 390, 217 ms | 198, 195, 201 ms |
| 32 clients, commits/s | 205, 199, 189 | 240, 247, 223 |
| 32 clients, median | 137, 143, 132 ms | 118, 117, 125 ms |
| 32 clients, p99 | 388, 521, 1,602 ms | 294, 275, 369 ms |

Eleven of the twelve runs had no ping reply later than half a second. The twelfth had eight, the longest 1.7 s, and it is the run whose p99 is 1.6 s. 2.1.1 is the faster with one database too: every other duplicate goes to the next fastest pod the server knows, where 2.1.0 sent the duplicate that starts with the upload to a new pod every time. To repeat a column, run `durable-db bench --workload write --clients 32 --seconds 30` against a new server of that release, and `ping -i 0.2` to the router beside it.

Two things keep B2's slow moments from becoming yours:

- **Known pods first, and a duplicate where one can be spared.** B2 stores each upload on one of many storage pods, and pods differ: the same 4 KiB upload measured medians from 67 ms to over 900 ms on eight pods at one moment, and of ten new pods tried one at a time four were fast. durable-db keeps the pods it has used in order of speed. The upload a commit waits for takes the fastest one that is free. An identical upload starts beside it on another pod, a slower known one or a new one, and one more every 150 ms while none has finished, up to four; the first to finish wins. At most eight new pods are on trial at a time, and when many databases commit at once and no pod can be spared, the duplicate is not sent. Uploads are free on B2. In five logged runs with one database, one upload in five finished on the duplicate and not on the upload it started beside.
- **B2's native upload call.** PutObject through B2's S3-compatible gateway measured 150 to 260 ms at the median for the same object; B2's own upload call measured 71 ms. Uploads use the native call; everything else uses the S3 API.

### Opening a database

A database is opened by the first request that needs it and stays open while it is used (five minutes idle by default). These times were measured with release 2.1.0.

| Situation | Time to first answer |
| --- | --- |
| Warm server, database was released cleanly | 0.25 to 0.45 s, 0.3 s typical |
| Warm server, creating a database | 0.2 to 0.45 s |
| Brand-new server process, database was released cleanly | 1.7 s (includes new connections and a B2 upload session) |
| Brand-new server process, previous owner was killed with 30 unflushed commits | 1.3 s |
| Server start to ready, including the storage check | 1.1 s |

### On Kubernetes

From `scripts/test-kubernetes.sh` on a three-node kind cluster on the same desktop, backed by the same B2 bucket, through the Service. 192 clients send session reads to four databases for 80 seconds, during which the autoscaler goes from two pods to five and one pod is force-deleted. Beside them a ledger of read-then-commit transfers from 16 clients over 8 databases runs for four minutes, through the scale-out, the force-deleted pod and the scale-in, and is then verified. Five runs, in the order they were made: the first was published with release 2.1.0, the others were made within forty minutes of each other.

| | 2.1.0 | 2.1.1 | 2.1.1 | 2.1.0 | 2.1.1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Session reads/s | 163,283 | 115,943 | 119,493 | 139,882 | 115,589 |
| Reads that failed | 102 | 0 | 86 | 102 | 110 |
| Reads told to wait | 46 | 0 | 47 | 26 | 22 |
| Ledger transfers/s | 87 | 95 | 98 | 76 | 84 |
| Transfer, p50 | 120 ms | 117 ms | 113 ms | 136 ms | 125 ms |
| Transfer, p99 | 647 ms | 606 ms | 611 ms | 706 ms | 687 ms |
| Slowest transfer | 14.6 s | 12.2 s | 25.4 s | 16.3 s | 16.6 s |

Every ledger verified. The load generator does not repeat a read, and a force-deleted pod breaks the connections it holds. Five pods were ready at most in each of the last three runs; the test used to report the first count it saw above two, which is all that is known of the first two.

**Release 2.1.1 read a sixth to a quarter less than 2.1.0 in this test, in every run, and the cause is not established.** Two things it is not. It is not the server's read path, which the release does not change: one server held to two CPUs and 1 GiB with the manifest's flags, on the same bucket, was given the same reads with the same ledger beside them, the two releases in turn. With 2.1.0 it read 166,199 and 166,497 times a second while the ledger made 100 and 101 transfers a second; with 2.1.1 it read 168,024 and 167,313 times a second while the ledger made 121 and 118. It is not how soon a new pod is ready: fifteen starts of each release on the bucket took 1.07 s and 1.02 s at the median from start to ready. What remains is how the pods share the work, and there a read costs more than it does on one server.

**A read that reaches a pod not holding its database is relayed to the one that does, and a relayed read costs two and a half times a direct one.** Two servers of 2.1.1, each held to two CPUs, were given the session reads of one database from 192 clients, all sent to the first server. In the three runs where the first server held the database it read 173,685, 178,573 and 178,865 times a second. In the five where the second held it and the first relayed, it read 64,966 to 71,776 times a second. Behind a Service a connection reaches the holder one time in as many as there are pods, so five pods read less in this test than one server does alone, and the figure moves with the share of reads that were relayed, which depends on when in the 80 seconds the new pods took databases over. Whether that share is what differs between the two releases has not been checked ([below](#what-is-not-measured-yet)). Each server counts the requests it relays in `durable_db_forwarded_requests_total`.

With release 2.0.0 the same test measured 30,114 session reads/s and the autoscaler stopped at three pods: its binary read at the speed of one core ([the allocator](#the-allocator)).

## The allocator

The binary carries its own allocator, jemalloc, held to eight arenas and set to give freed memory back to the system within a second. Which allocator a server uses decided two things: how fast it reads, and whether it stays inside a memory limit. The source of release 2.1.0 was built with each candidate and put through `scripts/benchmark.sh` on a local directory and through the soak described [below](#memory-under-load), on the desktop described above:

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
| Half reads, half commits of 16 KiB values, 64 clients over 4 databases | 5,903 | 3.3 ms | 55 ms | 1,264 | 568 MiB |
| Commits of 1 KiB values, 256 clients over 256 databases | 6,778 | 21 ms | 148 ms | 0 | 621 MiB |
| Strong reads of 4 KiB values, 512 clients over 8 databases of 20,000 keys | 74,686 | 5.7 ms | 28 ms | 0 | 621 MiB |

The server used 621 MiB at most. Release 2.1.0 used 618 MiB in one run of the same script and 659 MiB in another. No request failed.

"Told to wait" counts answers of `503`. In the first load each of the four databases holds 160 MiB and is overwritten about four times in the minute, so tables are merged all the while; with four CPUs the flushes fall behind now and then, and the server refuses a write to that database instead of holding it in memory. That was 0.4% of the requests of that load in this run, and 1.7% in the run published with 2.1.0. A client waits and sends the request again, as the SDK does.

This is three minutes on a local directory. It says that the limits in [deployment](deployment.md#on-kubernetes) hold under these loads with this allocator, not what many hours or a bucket would show; on a bucket an upload is in memory for as long as the bucket takes to store it.

## What it costs in requests

Uploads are free on B2; HEAD and GET are billed per call, LIST at a higher rate. Per request:

| Request | Billable calls |
| --- | --- |
| Commit | One HEAD per group of transactions; uploads (one to four on B2) |
| Strong read | One HEAD per group of concurrent reads, at most two groups in flight |
| Session read from cache | None |
| Flush (every 8 MiB, 512 groups, or 5 s of quiet) | Two uploads, one HEAD, two version listings, deletes |
| Opening a database | Six or seven listings (two of them a background sweep for leftovers), one GET, one HEAD, two uploads |
| An idle database | Nothing |

A database committing as fast as B2 allows from one location runs about ten groups a second, which is under a million HEADs a day. Check your provider's current price list for what that means on your bill.

## What is not measured yet

- B2 from a server in a data centre, on a wired link. Every B2 figure here was measured over Wi-Fi from one desktop, for minutes at a time, and that includes the measurements behind the choice of storage pods.
- More databases committing at once on B2 than a server knows fast pods for.
- Why the Kubernetes test read less with release 2.1.1 than with 2.1.0: whether more of its reads were relayed.
- Amazon S3 or any provider other than B2.
- A server in the same region as its bucket.
- Many hours of sustained load.
- Databases larger than a few hundred megabytes.
