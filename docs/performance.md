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

**B2's own latency sets these numbers, and it varies.** The table is one run. Earlier the same day the run published with release 2.0.0 measured 118 ms for a single-client commit where this one measured 77 ms. Expect a factor of two between good and bad hours, and less latency from a server closer to the bucket. One client's commit, at the median, on 14 November: 64 to 76 ms in the morning, Pacific time, 99 and 107 ms at about two in the afternoon, and 86 to 91 ms at four, when releases 2.1.1 and 2.1.3 were run twice each in turn for 30 seconds and measured alike, while one ping in ten to the desktop's router took longer than half a second. Where this page compares two releases, they were run in turn within the same half hour.

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

### When many databases collect at once

Collection lists the versions it may remove and removes them. Up to release 2.1.1 every version was removed with a request of its own, 32 at a time for each database. Three things make many databases collect in the same second: databases that stop being written together flush together five seconds later, a server that is stopped seals and collects every database it holds, and databases that began together reach their 512th group of commits together. With 64 databases the server then opened about a thousand connections to B2 at once and B2 reset some of them. A purge whose request was reset failed, and what it had not removed waited for the next collection. Nothing was lost by that, and nothing was reported: the failure was a line in the debug log that did not say what had failed.

Since 2.1.2 one request removes up to a thousand versions, which B2 answered in 0.25 s, and a request of a purge that is not answered is sent again twice before the purge fails ([verification](verification.md#what-these-checks-found), finding 32).

**64 databases written for ten seconds, left alone, and the server stopped 25 seconds later.** Two runs of each release in turn:

| | Release 2.1.1 | Release 2.1.2 |
| --- | --- | --- |
| Purges that failed | 26 of 158, 75 of 159 | 0 of 159, 0 of 156 |
| Most open files after the load (and under it) | 1,136 (382), 1,486 (353) | 248 (343), 258 (358) |
| Most memory after the load (and under it) | 154 (78), 145 (75) MiB | 97 (82), 91 (80) MiB |
| Stopping the server, all 64 databases released | 7.2, 8.6 s | 3.6, 6.1 s |

Four more runs of 2.1.1 made earlier the same day measured 35 to 67 failed purges of about 190, 1,171 to 1,297 open files and 136 to 169 MiB.

**64 clients over 64 databases for 100 seconds**, so that every database flushes and collects once while it is written:

| | Release 2.1.1 | Release 2.1.2 |
| --- | --- | --- |
| Commits/s | 631 | 669, 670 |
| p99 of a commit | 414 ms | 290, 284 ms |
| p99.9 of a commit | 1,954 ms | 692, 735 ms |
| Purges that failed | 156 of 629 | 0 of 645, 0 of 643 |
| Time spent in purges, all databases together | 931 s | 335, 286 s |

The three runs are in the order 2.1.2, 2.1.1, 2.1.2. Three runs of 2.1.1 made about an hour earlier measured 644, 651 and 643 commits/s with a p99 of 282, 268 and 298 ms, and 124 of 668, 162 of 621 and 154 of 647 failed purges: its failures are in every run, and its slower commits in the run above are not.

**256 databases, one client each, the same way**: ten seconds of writes on a new server, 25 seconds alone, then stopped. Two runs of each release in turn, each server held to 4,096 open files:

| | Release 2.1.1 | Release 2.1.2 |
| --- | --- | --- |
| Purges that failed | 127 of 586, 197 of 587 | 0 of 596, 0 of 589 |
| Most open files after the load (and under it) | 1,715 (1,352), 1,542 (1,291) | 870 (1,370), 926 (1,427) |
| Most memory after the load (and under it) | 267 (140), 216 (133) MiB | 196 (144), 210 (149) MiB |
| Stopping the server, all 256 databases released | 22.7, 19.1 s | 6.5, 14.5 s |
| Commits/s in those ten seconds | 971, 336 | 1,127, 996 |
| p99 of a commit | 2.3, 4.4 s | 2.3, 2.6 s |

The last two rows are a new server's first ten seconds with more databases than it knows storage pods for, which this page had not measured before. Committing is the same code in both releases, and the row moves by a factor of three: two earlier runs measured 293 commits/s with 2.1.1 and 1,130 with a build between the two releases. The memory a server holds after the load is the flush of every database at once, which the flags bound ([deployment](deployment.md#on-kubernetes)).

**256 databases for five minutes** is what a server does once it knows its storage pods. One server of release 2.1.4, held to 4 GiB and 4,096 open files, with one client writing to each of 256 databases for 300 seconds, at five in the afternoon, Pacific time; of 1,988 pings to the router meanwhile, none took longer than 0.25 s.

| | Release 2.1.4 |
| --- | --- |
| Commits/s over the five minutes | 1,665 |
| Commits/s in each minute | 1,783, 1,665, 1,414, 1,683, 1,756 |
| A commit: median, p99, p99.9, slowest | 115 ms, 610 ms, 1.8 s, 8.1 s |
| Requests that failed, or were told to wait | 0, 0 |
| Uploads, and those that failed and were sent again | 695,092 and 1,092 |
| Most memory | 566 MiB |
| Most open files | 1,751 |

That is six or seven commits a second for each database, where one client alone had seven to nine an hour earlier. A run of release 2.1.3 half an hour earlier measured 1,592 commits/s, between 1,319 and 1,738 in each minute, with a p99 of 666 ms and 515 MiB; that server stopped its housekeeping during the run ([verification](verification.md#what-these-checks-found), finding 36), which a commit does not wait for.

Two things in it are not good. Memory rose all through the load, from 139 MiB in its first seconds to 404 MiB as it ended, and the most, 566 MiB, was while the databases were being deleted. When it was over the cache of table blocks held 143 MiB of the 256 MiB it may hold (`--cache-bytes`), and unflushed writes may take as much again (`--memtable-bytes`), so the rise is within what the flags allow; five minutes do not show it stop. And when the benchmark ended it deleted its 256 databases at once, each of about 2,000 commits: 68 purges of those deletions failed and were made again, and every deletion was answered as done. During the load itself one purge in 2,953 failed.

**This rate is more than Backblaze gives a new account.** Each of these commits is an upload, often a second upload that races it, and a HEAD: about 4,000 requests a second. Backblaze's documentation gives a new account 500 a second for uploads and downloads. The same load run for twenty minutes, an hour later, averaged 1,333 commits/s for eleven and a half minutes (between 869 and 1,864 in each minute), and then Backblaze answered `503` to every upload for five and a half minutes, whether for that limit or for a fault of its own is not established. No commit was acknowledged in that time, each of the 256 clients had one commit answered with an error, and every database was closed and, when uploads were accepted again, opened again and written to for the rest of the run, at 915 to 982 commits/s. Memory was 683 MiB and still rising when the refusals began, and 932 MiB at most, when all 256 databases were opened again together. [Verification](verification.md#what-these-checks-found), finding 37, has the account. Ask Backblaze for a higher limit before you plan on a rate like this one. At two to three requests for each commit, 500 a second is about 200 commits a second across an account. Since 2.1.5 a server that is refused sends one request at a time until one is taken, where this one sent 800 uploads a second ([verification](verification.md#what-these-checks-found), finding 38); it still keeps itself under no rate.

To repeat it, start a server under `prlimit --nofile=4096:4096`, run `durable-db bench --workload write --clients 256 --databases 256 --seconds 300` against it, and read `durable_db_request_seconds_count{kind="commit"}`, the counts and failures of `durable_db_storage_seconds` for `put` and `purge`, the server's resident memory and `ls /proc/<pid>/fd | wc -l` every five seconds.

To repeat the first table, start a server, run `durable-db bench --workload write --clients 64 --databases 64 --keep` against it, and for the next half minute and while the server stops, sample `ls /proc/<pid>/fd | wc -l`, the resident memory of the server, and `durable_db_storage_failures_total{op="purge"}` and `durable_db_storage_seconds_count{op="purge"}` from `/metrics`. The third is the first with 256 clients and databases. The second table is the same load with `--seconds 100` and without `--keep`, and its figures are read from `/metrics` when `bench` exits, after it has deleted its databases.

A store that refuses the request for many versions gets a request for each version, as before, and the server says so once in its log. Among cloud stores only B2 has been exercised.

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

From `scripts/test-kubernetes.sh` on a three-node kind cluster on the same desktop, backed by the same B2 bucket, through the Service. 192 clients send session reads to four databases for 80 seconds, during which the autoscaler goes from two pods to five and one pod is force-deleted. Beside them a ledger of read-then-commit transfers from 16 clients over 8 databases runs for four minutes, through the scale-out, the force-deleted pod and the scale-in, and is then verified. Nine runs, each column in the order they were made; the first of 2.1.0 was published with that release, and the rest were made in turn on one afternoon.

| | Release 2.1.0, four runs | Release 2.1.1, five runs |
| --- | --- | --- |
| Session reads/s | 163,283; 139,882; 118,284; 159,921 | 115,943; 119,493; 115,589; 144,577; 130,334 |
| Reads that failed | 102; 102; 0; 106 | 0; 86; 110; 100; 0 |
| Reads told to wait | 46; 26; 0; 0 | 0; 47; 22; 0; 0 |
| Ledger transfers/s | 87; 76; 89; 88 | 95; 98; 84; 96; 97 |
| Transfer, p50 | 120; 136; 121; 124 ms | 117; 113; 125; 114; 115 ms |
| Transfer, p99 | 647; 706; 641; 611 ms | 606; 611; 687; 614; 602 ms |
| Slowest transfer | 14.6; 16.3; 8.4; 9.7 s | 12.2; 25.4; 16.6; 6.2; 6.5 s |

Every ledger verified. The load generator does not repeat a read, and a force-deleted pod breaks the connections it holds. Five pods were ready at most in every run that recorded it; the test used to report the first count it saw above two, which is all that is known of the first run of each release.

Three more runs were made at about two in the afternoon of the same day, Pacific time, when the bucket answered more slowly for both releases: one client's commit took 99 ms at the median with 2.1.1 and 107 ms with 2.1.2, against 64 to 76 ms when the one-database table of this page was measured.

| In this order | Release 2.1.2 | Release 2.1.1 | Release 2.1.2 |
| --- | --- | --- | --- |
| Session reads/s | 147,631 | 120,014 | 120,892 |
| Reads that failed | 110 | 0 | 99 |
| Reads told to wait | 13 | 0 | 62 |
| Ledger transfers/s | 50 | 56 | 63 |
| Transfer, p50 | 200 ms | 181 ms | 166 ms |
| Transfer, p99 | 1,240 ms | 1,097 ms | 923 ms |
| Slowest transfer | 10.2 s | 10.0 s | 12.8 s |

Every ledger verified: 12,026, 13,349 and 15,098 transfers. A transfer waits for the bucket, so the ledger's rate follows what the bucket takes at that hour; these three runs show no difference between the releases.

One run of 2.1.3, later that afternoon: 117,949 session reads a second, none failed and 48 told to wait; a ledger of 20,017 transfers verified at 83 a second, with a transfer taking 133 ms at the median, 650 ms at the 99th percentile and 9.8 s at most.

Two runs at half past five that afternoon, 2.1.4 and then 2.1.1: 121,813 and 161,925 session reads a second, of which none and 109 failed and 48 and 124 were told to wait; ledgers of 23,679 and 22,496 transfers verified, at 99 and 93 a second, with a transfer taking 115 ms at the median in both, 585 and 611 ms at the 99th percentile, and 12.0 and 13.0 s at most. The bucket was as fast again as in the morning, for both releases. The slower hour was about two in the afternoon, Pacific time. Earlier versions of this page called it the evening, which it was by UTC.

One run of 2.1.5 at half past seven that evening, Pacific time: 157,202 session reads a second, of which 105 failed and none were told to wait; a ledger of 22,528 transfers verified at 94 a second, with a transfer taking 118 ms at the median, 612 ms at the 99th percentile and 7.1 s at most.

One run of 2.1.6 the night after, at a quarter past one: 121,858 session reads a second, of which none failed and none were told to wait; a ledger of 22,360 transfers verified at 93 a second, with a transfer taking 122 ms at the median, 608 ms at the 99th percentile and 6.6 s at most.

One run of 2.1.7 the same morning, at twenty past six: 123,473 session reads a second, of which none failed and none were told to wait; a ledger of 25,965 transfers verified at 108 a second, with a transfer taking 112 ms at the median, 429 ms at the 99th percentile and 7.9 s at most.

**The read figure moves by a third from run to run, with either release.** Nine runs do not show a difference between the releases and do not rule one out. After the first five, every run of 2.1.1 lay below every run of 2.1.0, and the notes of release 2.1.1 said that the figure had gone the wrong way for a reason not established; the next four runs, two of each release, came out the other way round once and did not bear that out. What is established:

- A single server reads the same with both. One server held to two CPUs and 1 GiB with the manifest's flags, on the same bucket, was given the same reads with the same ledger beside them, the two releases in turn: 166,199 and 166,497 reads a second with 2.1.0 while the ledger made 100 and 101 transfers a second, 168,024 and 167,313 with 2.1.1 while the ledger made 121 and 118.
- A new pod is ready as soon: fifteen starts of each release on the bucket took 1.07 s and 1.02 s at the median from start to ready.
- **A read that reaches a pod not holding its database is relayed to the one that does, and a relayed read costs two and a half times a direct one.** Two servers of 2.1.1, each held to two CPUs, were given the session reads of one database from 192 clients, all sent to the first server. In the three runs where the first server held the database it read 173,685, 178,573 and 178,865 times a second. In the five where the second held it and the first relayed, it read 64,966 to 71,776 times a second. Behind a Service a connection reaches the holder one time in as many as there are pods, which is why five pods read less in this test than one server does alone. In the last four runs the test counted them: the pods still running when the reads ended had answered 9.5 to 12.2 million reads and relayed 5.7 to 8.9 million requests, between 55% and 73%. Each server counts what it relays in `durable_db_forwarded_requests_total`.

The share relayed does not by itself account for the spread: the run with the smallest share, 55%, read 130,334 times a second, and one with 73% read 159,921. Which pod is killed, what it held, and when in the 80 seconds the new pods take databases over differ in every run, and their effect has not been separated ([below](#what-is-not-measured-yet)).

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

One server with the flags of the Kubernetes manifest (192 MiB of cache, 96 MiB of unflushed writes, 128 MiB each for request bodies and results), on a local directory, in a systemd scope limited to 1 GiB of memory without swap and to four CPUs. It is killed if it passes the limit. Three loads of a minute each run one after the other against the same server, with values of random text, which does not compress. Then the second load runs once more, and halfway through it the script kills the server and starts it again, so that the server that takes its place opens all 256 databases at once, from what the first one stored, while they are written to:

| Load | Requests/s | p50 | p99 | Told to wait | Most memory so far |
| --- | ---: | ---: | ---: | ---: | ---: |
| Half reads, half commits of 16 KiB values, 64 clients over 4 databases | 3,954 | 2.0 ms | 120 ms | 5,518 | 516 MiB |
| Commits of 1 KiB values, 256 clients over 256 databases | 6,842 | 22 ms | 104 ms | 0 | 584 MiB |
| Strong reads of 4 KiB values, 512 clients over 8 databases of 20,000 keys | 56,983 | 6.7 ms | 36 ms | 0 | 584 MiB |
| The second load again, the server killed halfway and started again | 6,626 | 22 ms | 155 ms | 0 | 563 MiB, the second server |

The server, of release 2.1.6, used 584 MiB at most in this run, and the one that took its place 563 MiB. In two more runs of 2.1.6 they used 582 and 544 MiB, and 596 and 540 MiB; in two runs of 2.1.5 made between them, 583 and 530 MiB, and 578 and 575 MiB. In one run of 2.1.7, 580 and 541 MiB. No request failed in any of the first three loads. In the last, 6,459 requests failed in the moment between the kill and the next server's start, and 5,214 and 28,072 in the two other runs: the benchmark does not send a request again, as a client would.

Ten minutes of each of the first three loads (`SECONDS_EACH=600`) with release 2.1.5, the server's memory read every ten seconds: 3,169, 6,491 and 78,586 requests a second, no request failed, 825 of the first load's 1.9 million were told to wait, and 599 MiB at most. Memory was level within each load: 365 to 436 MiB through the first, 468 to 515 MiB through the second with no rise from its second minute to its tenth, and 373 MiB through the third. Release 2.1.4 used 582 and 590 MiB, 2.1.1 used 621 MiB and 2.1.0 used 618 and 659 MiB in runs of the same script; in those of 2.1.1 and 2.1.0 the server may have stopped keeping unflushed writes within their budget during the second load ([verification](verification.md#what-these-checks-found), finding 36).

The first load is not the same from one run to the next: 3,480 to 5,846 requests a second in four runs of 2.1.5, and 4,116 and 6,417 in two of 2.1.4, within two hours on the same desktop. On the night 2.1.6 was measured, six runs of 2.1.5 gave 2,763 to 4,306 and seven of 2.1.6, made between them, 2,633 to 3,954. The one whole run of 2.1.7 gave 1,826, the fewest of any run, straight after ten minutes of `scripts/soak-kills.sh`; three runs of this load alone an hour later, in turn with three of 2.1.6, gave 2,682 to 3,343 for 2.1.7 and 2,689 to 4,184 for 2.1.6. The second load moved by a few percent, and the third between 56,983 and 77,398.

"Told to wait" counts answers of `503`. In the first load each of the four databases holds 160 MiB and is overwritten about four times in the minute, so tables are merged all the while; with four CPUs the flushes fall behind now and then, and the server refuses a write to that database instead of holding it in memory. That was 5,518 of the requests of that load in this run, which is 2%. It is not the same from one run to the next either: between none and 16,021, which is 8%, in ten runs of 2.1.5, and between none and 10,270, which is 6%, in ten of 2.1.6, five of which had none. In four runs of 2.1.7 it was four requests in one and none in the others. In every run with more than 3,000 the median request took under 2 ms, and in every run with fewer than 1,300 it took over 3 ms. Neither run of 2.1.4 had any, the run published with 2.1.1 had 0.4%, and the one published with 2.1.0 had 1.7%. A client waits and sends the request again, as the SDK does.

The same server on a store, MinIO in a container on the same desktop held to 4 GiB and four CPUs, with 256 databases written for twenty minutes by one client each and then deleted one after another:

```sh
DURABLE_DB=./durable-db scripts/soak-bucket.sh
```

With 2.1.6: 2,110 commits a second, of which none failed; 582 MiB at most, and between 416 and 581 MiB from the third minute to the twentieth; the 256 deletions took 257 s. MinIO took longer than twenty seconds over some of its listings: 1,148 of 17,593 purges failed and were made again later, and the slowest deletion took 143 s. The commit rate is that of one MinIO process on the desktop's disk, and says nothing of a provider's bucket.

This is minutes, thirty at the most, on a local directory and on a store on the same desktop. It says that the limits in [deployment](deployment.md#on-kubernetes) hold under these loads with this allocator, not what many hours or a provider's bucket would show; on a bucket an upload is in memory for as long as the bucket takes to store it.

## A large database

```sh
DURABLE_DB=./durable-db scripts/soak-size.sh                    # 4 GiB on a local directory
DURABLE_DB=./durable-db SOAK_GIB=16 scripts/soak-size.sh
DURABLE_DB=./durable-db SOAK_STORE=minio scripts/soak-size.sh   # 4 GiB on MinIO in a container
DURABLE_DB=./durable-db SOAK_STORE=bucket SOAK_GIB=1 SOAK_READ_CLIENTS=8 SOAK_SECONDS=30 scripts/soak-size.sh
```

The last runs on the bucket that `DDB_S3_ENDPOINT`, `DDB_S3_BUCKET`, `DDB_S3_ACCESS_KEY_ID` and `DDB_S3_SECRET_ACCESS_KEY` name, here Backblaze B2 from the desktop and over the link described [above](#backblaze-b2).

These runs were made with release 2.1.8, one of each, one after another on the desktop, the night the release was made. One database is filled by 64 clients, each commit one key with a value of 4 KiB that does not compress, through one server with the flags of the Kubernetes manifest in a scope limited to 1 GiB of memory and four CPUs. Its cache in memory is 192 MiB and its disk cache 1 GiB, so the databases here are as large as the disk cache or larger. That server is killed. A second, with an empty cache, opens the database. It is read at keys taken at random for a minute and then for another, by as many clients as wrote it; scanned, twenty keys from a key taken at random, for a minute; and written over at keys taken at random for two. Then its quota is set a hundredth above what it held when it was filled, which is under what it holds by then, and one write is made, which waits for a merge of everything. The second server is stopped, a third opens the database, and it is deleted. On the bucket eight clients read and scan, for thirty seconds each, and the writes take a minute. With each load the script prints what the server read from its store for it, in how many requests, and how many runs of tables the database had then.

| | Local directory, 4 GiB | Local directory, 16 GiB | MinIO, 4 GiB | Backblaze B2, 1 GiB |
| --- | ---: | ---: | ---: | ---: |
| Commits a second while it was filled | 1,830 | 2,576 | 2,441 | 47 |
| p50 and p99 of a commit | 27 ms, 163 ms | 23 ms, 111 ms | 21 ms, 126 ms | 998 ms, 3.8 s |
| Commits that were told to wait | 0 | 0 | 0 | 306,742 |
| The fill took | 574 s | 1,628 s | 429 s | 5,539 s |
| Fewest and most commits in ten seconds of it | 10,717, 24,255 | 20,851, 29,502 | 18,038, 28,029 | 2, 7,623 |
| Tables it then held | 129 in 8 runs, of 4,030 MiB | 506 in 5 runs, of 16,124 MiB | 132 in 11 runs, of 4,030 MiB | 42 in 32 runs, of 1,007 MiB |
| Sent to storage for it | 28,452 MiB, 6.9 times what was written | 139,309 MiB, 8.5 times what was written | 27,394 MiB, 6.7 times what was written | 6,230 MiB, 6.1 times what was written |
| Read back from storage meanwhile | 3,609 MiB | 51,700 MiB | 5,443 MiB | 0 MiB |
| Most memory of that server | 414 MiB | 453 MiB | 427 MiB | 232 MiB |
| A new server opened it, after a kill, in | 91 ms | 50 ms | 108 ms | 18.8 s |
| Strong reads a second, at first | 36,109 (p50 1.2 ms, p99 29 ms) | 30,513 (p50 1.6 ms, p99 23 ms) | 6,191 (p50 9.6 ms, p99 27 ms) | 2 (p50 3.2 s, p99 16.9 s) |
| Read from storage for them | 81,045 MiB in 2,593,289 requests | 111,867 MiB in 3,603,376 requests | 13,714 MiB in 412,488 requests | 100 MiB in 1,029 requests |
| It then had | 127 tables in 2 runs, their indexes 8 MiB of its memory | 507 tables in 6 runs, their indexes 26 MiB of its memory | 133 tables in 12 runs, their indexes 6 MiB of its memory | 43 tables in 33 runs, their indexes 1 MiB of its memory |
| Strong reads a second, after that | 41,044 (p50 1.1 ms, p99 29 ms) | 31,280 (p50 1.6 ms, p99 23 ms) | 6,567 (p50 9.2 ms, p99 24 ms) | 3 (p50 2.3 s, p99 8.3 s) |
| Read from storage for them | 67,881 MiB in 2,209,974 requests | 113,280 MiB in 3,686,219 requests | 13,344 MiB in 434,431 requests | 167 MiB in 1,409 requests |
| Scans a second | 15,053 (p50 3.0 ms, p99 30 ms) | 13,397 (p50 3.8 ms, p99 26 ms) | 2,222 (p50 27 ms, p99 68 ms) | 2 (p50 4.5 s, p99 13.0 s) |
| Read from storage for them | 106,531 MiB in 2,368,508 requests | 158,419 MiB in 3,185,105 requests | 17,400 MiB in 446,033 requests | 116 MiB in 1,318 requests |
| Writes over it a second | 1,979 (p50 25 ms, p99 143 ms) | 2,509 (p50 23 ms, p99 102 ms) | 2,711 (p50 20 ms, p99 124 ms) | 50 (p50 1.2 s, p99 4.7 s) |
| Sent to storage for them | 5,431 MiB, 5.9 times what was written | 10,216 MiB, 8.7 times what was written | 11,375 MiB, 9.0 times what was written | 46 MiB, 3.9 times what was written |
| It then held | 4,888 MiB in 157 tables of 9 runs | 17,172 MiB in 538 tables of 5 runs | 5,145 MiB in 162 tables of 5 runs | 1,019 MiB in 43 tables of 33 runs |
| A write at a quota under that waited | 33.0 s | 83 s | 47.2 s | 1,227 s |
| Read from storage for that merge | 3,924 MiB | 16,225 MiB | 4,155 MiB | 683 MiB |
| and it then held | 4,029 MiB in 126 tables | 16,125 MiB in 504 tables | 4,030 MiB in 126 tables | 1,007 MiB in 32 tables |
| Most memory of the second server | 444 MiB | 433 MiB | 534 MiB | 373 MiB |
| A third server opened it, after a stop, in | 29 ms | 23 ms | 19 ms | 424 ms |
| It was deleted in | 39 ms | 46 ms | 41 ms | 636 ms |
| Requests to storage that failed and were made again | 0 | 0 | 0 | 247 |

The link to Backblaze carried less that night than for any figure above it on this page: a test upload of 32 MiB went at 0.5 MB a second and a download at 1.5 MB a second, and the server sent 6,230 MiB in the 92 minutes of the fill, 1.1 MiB a second, where 2.1.7's run had sent 4 MiB a second. That column says what a bucket costs when the link is the limit, and is not to be set beside the others or beside 2.1.7's.

**Memory does not grow with the database**, and neither does the time to open it. A server holds the tables' indexes and blocks in a cache of a fixed size, and to open a database it reads one manifest and what had not been flushed. The servers used between 232 and 534 MiB, under the limit of 1 GiB, at 1 GiB as at 16. After a kill the next server opened the database in 91 ms at 4 GiB and in 50 ms at 16 GiB on a local directory, and in 108 ms on MinIO. On Backblaze it took 18.8 s, which is what the killed server had not flushed, fetched and stored again over that link; after a stop, with nothing to store again, 0.4 s.

**A database is sent to its store several times over while it grows.** Tables are merged with those next in size, so each value is written again as its table grows: 6.9 times what was written at 4 GiB and 8.5 times at 16 GiB on a local directory, 6.7 times on MinIO, and 6.1 times at 1 GiB on Backblaze. Writing over what is there costs more for each byte, since the value that was written over is carried along until a merge reaches the oldest table. The slowest ten seconds of each fill are those in which the largest merges ran.

**A read of a key looks in one table of each run**, newest first, so how fast a database is read depends on how many runs it has. A run is the tables of one merge, or the table of one flush. A database with more than twelve merges the newest that are too many; one with fewer keeps what the rule of sizes leaves, which was between 5 and 11 runs at the end of the fills on a local directory and on MinIO. A server that opens a database after a kill merges what is due at once. Here the database of 4 GiB was read with 2 runs, at 36,109 and 41,044 reads a second; the one of 16 GiB with 6, at 30,513 and 31,280; the one on MinIO with 12. A database that is written while its server merges gains a run for every flush until that merge ends, and a merge of 16 GiB takes more than a minute on a local directory. With `SOAK_RUNS` the script writes over the database with every client, at keys taken at random, until it has that many runs, for ten minutes at most, and kills the server then:

```sh
DURABLE_DB=./durable-db SOAK_GIB=16 SOAK_RUNS=16 scripts/soak-size.sh
```

| | Killed when it was filled | Killed when it had 16 runs |
| --- | ---: | ---: |
| The fill took | 1,628 s | 1,937 s |
| It was then written over for | not at all | 571 s, with 16 runs at most |
| and killed with | 506 tables in 5 runs | 599 tables in 16 runs |
| Strong reads a second, at first | 30,513 (p50 1.6 ms, p99 23 ms) | 12,004 (p50 3.9 ms, p99 31 ms) |
| Read from storage for them | 111,867 MiB in 3,603,376 requests | 145,174 MiB in 4,444,789 requests |
| It then had | 507 tables in 6 runs, their indexes 26 MiB of its memory | 596 tables in 12 runs, their indexes 30 MiB of its memory |
| Strong reads a second, after that | 31,280 (p50 1.6 ms, p99 23 ms) | 18,255 (p50 1.4 ms, p99 32 ms) |
| Read from storage for them | 113,280 MiB in 3,686,219 requests | 128,652 MiB in 3,983,716 requests |
| Scans a second | 13,397 (p50 3.8 ms, p99 26 ms) | 18,853 (p50 2.7 ms, p99 26 ms) |
| Read from storage for them | 158,419 MiB in 3,185,105 requests | 180,963 MiB in 2,887,716 requests |
| A write at a quota under that waited | 83 s | 102 s |

Written over by 64 clients, the database of 16 GiB took 571 of the ten minutes to reach 16 runs: a merge gives way to merges of what is flushed behind it once a database has more than twelve. The server that opened it merged the newest that were too many at once and read 12,004 keys a second in the first minute, with 12 runs, while the rule of sizes merged the rest; in the second minute the database was one run, and it read 18,255 a second. The writes it was killed in were the same 64 clients' as the fill's, at keys taken at random, and the fill ends with every key written once: a database that is written over faster, or by more clients, or for longer, can be left with more.

A database whose merges cannot keep up with what is written to it gains runs for as long as that lasts. On Backblaze that night the link carried less than the fill's merges had to send, merges that met a connection reset failed and were begun again, and the database had 32 runs when its server was killed. Each read of a key the disk cache did not hold then asked the bucket for a block of up to 33 runs, one request after another: two and three reads a second, half of them taking 2.3 s or more. No write was told to wait for runs in 2.1.8; since 2.1.9 one is, while a merge would leave more than twenty-four ([below](#when-merges-fall-behind)).

**A read of what the disk cache does not hold costs a request to the store for a block, and since 2.1.8 no more than that.** A read whose table is not in the disk cache asks the store for the block that would hold its key, 32 KiB for a value of 4 KiB here, in each run it looks in. In the first minute of reads of the 4 GiB database on a local directory, the server answered 36,109 reads a second and asked its store 1.2 times for each, for 32 KiB each time: the cache held a quarter of the database. That is 1.3 GiB a second from the store, in proportion to the reads. Every release before this one read more, and not in proportion to anything: a read that missed also had its whole table fetched for the disk cache, in the place of other tables, which were then fetched in their turn ([verification](verification.md#what-these-checks-found), finding 43). Release 2.1.7 under the same script, on the same desktop, the same night, between the runs above:

| | 2.1.7 | 2.1.8 |
| --- | ---: | ---: |
| Commits a second while it was filled | 1,537 | 1,830 |
| Read back from storage meanwhile | 10,621 MiB | 3,609 MiB |
| Strong reads a second, at first | 15,060 (p50 2.1 ms, p99 55 ms) | 36,109 (p50 1.2 ms, p99 29 ms) |
| Read from storage for them | 83,282 MiB in 1,353,143 requests | 81,045 MiB in 2,593,289 requests |
| Scans a second | 6,318 (p50 5.0 ms, p99 92 ms) | 15,053 (p50 3.0 ms, p99 30 ms) |
| Read from storage for them | 76,354 MiB in 1,490,914 requests | 106,531 MiB in 2,368,508 requests |
| Writes over it a second | 1,565 (p50 26 ms, p99 314 ms) | 1,979 (p50 25 ms, p99 143 ms) |
| A write at a quota under that waited | 48.3 s | 33.0 s |
| Read from storage for that merge | 5,015 MiB | 3,924 MiB |

By the server's count of its requests, of the 83,282 MiB that 2.1.7 read from its store in the first minute of reads, some 41 GiB were the blocks its reads asked for and 40 GiB were whole tables, 21 of them a second. A local directory gave that at once; a bucket gives what the link carries and bills it as download. Release 2.1.8 fetches a table only when reads have had a sixteenth of it from the store, and only into room that is free or that no read has asked for in five minutes. A merge has none of its tables fetched, and what it stores does not put out what it has yet to read. So the fill read back a third of what it had, the reads and scans were more than twice as fast, and their slowest hundredth took about half the time or less.

**On a bucket, give a server a disk cache as large as the databases it reads.** The same database of 4 GiB with a disk cache of 8 GiB:

```sh
DURABLE_DB=./durable-db SOAK_CACHE_MIB=8192 scripts/soak-size.sh
```

| | Disk cache of 1 GiB | Disk cache of 8 GiB |
| --- | ---: | ---: |
| Read back from storage meanwhile | 3,609 MiB | 0 MiB |
| Strong reads a second, at first | 36,109 (p50 1.2 ms, p99 29 ms) | 17,306 (p50 2.4 ms, p99 36 ms) |
| Read from storage for them | 81,045 MiB in 2,593,289 requests | 13,450 MiB in 305,879 requests |
| It then had | 127 tables in 2 runs, their indexes 8 MiB of its memory | 132 tables in 10 runs, their indexes 6 MiB of its memory |
| Strong reads a second, after that | 41,044 (p50 1.1 ms, p99 29 ms) | 17,413 (p50 2.3 ms, p99 38 ms) |
| Read from storage for them | 67,881 MiB in 2,209,974 requests | 0 MiB in 0 requests |
| Scans a second | 15,053 (p50 3.0 ms, p99 30 ms) | 5,123 (p50 5.9 ms, p99 112 ms) |
| Read from storage for them | 106,531 MiB in 2,368,508 requests | 0 MiB in 0 requests |
| It then held | 4,888 MiB in 157 tables of 9 runs | 4,769 MiB in 154 tables of 8 runs |
| A write at a quota under that waited | 33.0 s | 53.8 s |
| Read from storage for that merge | 3,924 MiB | 0 MiB |

With a cache that holds the database, the server read 13,450 MiB from its store in the first minute of reads: the blocks its reads asked for while the tables were fetched whole, two at a time, each once reads had had a sixteenth of it. After that it asked its store for nothing: not for the second minute of reads, not for the scans, and not for the merge at the quota. With the cache of 1 GiB it went on asking for 32 KiB 37,000 to 43,000 times a second, which on a bucket is a request to the bucket each time, and download. On a local directory the run with the larger cache was not the faster one, and that says nothing of a bucket. The script holds the store's directory in memory, from outside the server's limit, so a read of the store costs less there than a read of the server's own disk, where the cache is; and the database of that run was read with 10 runs, where the other had merged down to 2. What carries over is the rows of what was read from the store.

**A write at the quota waits for as long as reading the database and writing it again takes.** That was 33 s at 4 GiB and 83 s at 16 GiB on a local directory, 148 and 207 MiB a second, and 47 s at 4 GiB on MinIO; 2.1.7 took 48 s for the same at 4 GiB the same night. With the disk cache of 8 GiB it took 54 s: every table a merge stores and the cache keeps is synced to the disk the cache is on, so that what is written there and not yet on the disk does not pass the server's memory limit (finding 44), and on a local directory that disk is the store's as well. On Backblaze the write waited 1,227 s for 1 GiB over that night's link, 0.8 MiB a second; 2.1.7 had waited 525 s over a link nearly four times as fast. A client that writes at a quota has to be prepared to ask again for that long. A database that holds nothing that was written over or deleted is not merged for its quota at all, and is refused at once ([API](api.md#databases)).

The local directory answers in a fraction of a millisecond and MinIO here is one process on the desktop that loads it, so the rates in those columns are not those of a provider's bucket. What ran beside what, on a desktop of 32 cores: the local runs one after another, beside the run on Backblaze, which waited on its link; during the first three local runs, those of 4 GiB of 2.1.8 and of 2.1.7, the test suite forty-two times and its engine tests forty, each held to between 0.4 and 2.3 processors; and during the second half of the run with `SOAK_RUNS`, `scripts/soak-bucket.sh` on MinIO, 256 databases written through another server.

### When merges fall behind

Release 2.1.9 tells a write to wait while a merge would leave more than twenty-four runs, and keeps a merge going through a store that fails for a while (finding 45). The script above, with 2.1.8 and then 2.1.9 the morning 2.1.9 was released, one run of each, on a local directory at 4 GiB and on Backblaze at 1 GiB; the runs on Backblaze beside the local ones and the release's other checks:

| | 2.1.8, local, 4 GiB | 2.1.9, local, 4 GiB | 2.1.8, Backblaze, 1 GiB | 2.1.9, Backblaze, 1 GiB |
| --- | ---: | ---: | ---: | ---: |
| Commits a second while it was filled | 2,365 | 2,437 | 160 | 215 |
| Commits that were told to wait | 0 | 0 | 9,097 | 3,370 |
| The fill took | 443 s | 430 s | 1,634 s | 1,218 s |
| Most runs while it was filled | 13 | 12 | 18 | 16 |
| Strong reads a second, at first | 28,952 (p50 1.7 ms, p99 22 ms) | 37,544 (p50 1.3 ms, p99 23 ms) | 17 (p50 344 ms, p99 3.0 s) | 18 (p50 334 ms, p99 2.1 s) |
| It then had | 129 tables in 5 runs, their indexes 6 MiB of its memory | 129 tables in 6 runs, their indexes 6 MiB of its memory | 37 tables in 18 runs, their indexes 1 MiB of its memory | 40 tables in 17 runs, their indexes 1 MiB of its memory |
| Strong reads a second, after that | 35,401 (p50 1.4 ms, p99 23 ms) | 37,429 (p50 1.3 ms, p99 23 ms) | 79 (p50 82 ms, p99 340 ms) | 73 (p50 76 ms, p99 540 ms) |
| Scans a second | 15,510 (p50 3.3 ms, p99 25 ms) | 15,874 (p50 3.2 ms, p99 25 ms) | 89 (p50 65 ms, p99 309 ms) | 82 (p50 57 ms, p99 448 ms) |
| Writes over it a second | 2,417 (p50 23 ms, p99 92 ms) | 2,319 (p50 23 ms, p99 111 ms) | 211 (p50 290 ms, p99 588 ms) | 225 (p50 276 ms, p99 457 ms) |
| A write at a quota under that waited | 48.4 s | 27.7 s | 475 s | 495 s |
| Requests to storage that failed and were made again | 0 | 0 | 76 | 43 |

The two releases are within what one run differs from the next. The reads of a database depend on how many runs it was read with, which depends on the moment the soak kills the server that filled it. The link to Backblaze carried 2.9 MB a second up before the runs and 4.5 MB a second after them, and the fill of 2.1.9 had the better part of it. Neither fill had more than 18 runs, below where 2.1.9 holds writes back, so these runs say that nothing else changed.

Four times the writers, to see whether they would pass the bound:

```sh
DURABLE_DB=./durable-db SOAK_STORE=bucket SOAK_GIB=1 SOAK_READ_CLIENTS=8 SOAK_SECONDS=30 SOAK_CLIENTS=256 scripts/soak-size.sh
```

| | 2.1.8 | 2.1.9 |
| --- | ---: | ---: |
| Commits a second while it was filled | 345 | 339 |
| Commits that were told to wait | 445,044 | 469,703 |
| The fill took | 761 s | 772 s |
| Most runs while it was filled | 20 | 18 |
| Strong reads a second, at first | 10 (p50 659 ms, p99 2.9 s) | 10 (p50 743 ms, p99 2.9 s) |
| It then had | 38 tables in 18 runs, their indexes 1 MiB of its memory | 39 tables in 19 runs, their indexes 1 MiB of its memory |
| Strong reads a second, after that | 77 (p50 74 ms, p99 602 ms) | 40 (p50 136 ms, p99 913 ms) |
| Scans a second | 68 (p50 84 ms, p99 416 ms) | 37 (p50 190 ms, p99 702 ms) |
| A write at a quota under that waited | 528 s | 282 s |
| Requests to storage that failed and were made again | 24 | 11 |

They did not. A database takes no more writes than it can flush, 32 MiB of them at most, and the writes of 256 clients were held to that, told to wait some 450,000 times, before the runs came to 24; the link carried 4.7 MB a second up that morning. The night before, over a link that carried 1.1 MiB a second, 2.1.8 had 32 runs with 64 clients ([above](#a-large-database)). The bound of 2.1.9 is shown by the tests of finding 45 and not by a run here; [over a slow link](#over-a-slow-link) it is set against other values. The reads after the first minute and the scans differ more than the fills. The server that opens the database begins a merge of the runs that are too many: 2.1.8's ended during its second minute of reads, and its scans found 12 runs; 2.1.9's had not ended when its scans did, and they found 19. Over a link that merge takes minutes, and when it ends is not the same from one run to the next.

### Several databases merging at once

A server merges two databases at a time. Release 2.1.9 let a merge hold its turn from its first table to its last, and a database that waited for a turn meanwhile went on taking writes and gained a run with every flush. Since 2.1.10 a merge gives its turn to a database that waits for one between two of its tables, and waits for a turn again (finding 46).

```sh
DURABLE_DB=./durable-db scripts/soak-tenants.sh
DURABLE_DB=./durable-db SOAK_LARGE=3 SOAK_GIB=2 scripts/soak-tenants.sh
```

The script runs one server with the flags of the Kubernetes manifest, in a scope of 1 GiB and four CPUs, on a local directory. Two databases are filled with 4 GiB each, values of 4 KiB that do not compress, and written over for half a minute. Then each is given a quota under what it holds, so that the next write to each waits while all of it is merged, and the two merges begin together and take both of the server's turns. Meanwhile a small database is written by 64 clients, which make a flush of it about once a second, and read at keys taken at random by 8, in rounds of five seconds. The runs below were made with 2.1.9 and then with the release binary of 2.1.10, one of each, one after another on the desktop, the afternoon of the release, with nothing else running.

| | 2.1.9 | 2.1.10 |
| --- | ---: | ---: |
| Each write at a quota waited | 47.5 s, 48.3 s | 52.5 s, 52.6 s |
| Read from storage and sent to it, until the last was taken | 14,690 MiB and 8,534 MiB | 12,225 MiB and 9,085 MiB |
| The small database: runs before | 2 | 3 |
| Most runs while the merges ran | 27 | 5 |
| Its reads a second, round by round while they ran | 45,580 to 151,317 | 114,316 to 158,932 |
| Its writes a second, round by round while they ran | 507 to 1,328 | 677 to 1,907 |
| Its writes told to wait | 0 | 0 |
| The round after: reads a second, runs at most | 133,845, 4 | 116,548, 5 |

With 2.1.9 the small database's merge waited for a turn until the first of the two others ended, after 47.5 seconds, and it went from 2 runs to 27; in the last three rounds before then it was read at 45,580 to 66,503 keys a second, less than half of what it had been read at first. With 2.1.10 it had 5 runs at most, and was read at 114,316 keys a second at the least. What that cost the two large merges: each took 52.5 seconds where they had taken 47.5 and 48.3, a tenth longer. Each gave its turn up at every table at which the small database waited for one, and when it went on read again what it had read ahead and the index of each table it was in. What was read from storage and sent to it is the server's count for all its databases together, the small one's merges among them. One run of each was made, and how much one run of this script differs from the next has not been measured.

Three large databases of 2 GiB, so that one of them waits for a turn as well:

| | 2.1.9 | 2.1.10 |
| --- | ---: | ---: |
| Each write at a quota waited | 42.7 s, 24.5 s, 25.3 s | 38.6 s, 37.7 s, 38.4 s |
| Read from storage and sent to it, until the last was taken | 8,670 MiB and 7,001 MiB | 7,342 MiB and 6,889 MiB |
| The small database: runs before | 2 | 2 |
| Most runs while the merges ran | 16 | 5 |
| Its reads a second, round by round while they ran | 117,243 to 149,258 | 136,398 to 153,472 |
| Its writes a second, round by round while they ran | 843 to 2,144 | 976 to 1,409 |
| Its writes told to wait | 0 | 0 |
| The round after: reads a second, runs at most | 104,604, 6 | 135,657, 4 |

With 2.1.9 two of them merged first, and their writes were taken after 24.5 and 25.3 seconds; the third waited for a turn, and was taken after 42.7. The small database had 16 runs by the time a turn came free. With 2.1.10 the three merged a table at a time, and their writes were taken after 37.7 to 38.6 seconds: the last four seconds sooner, the first two thirteen seconds later, and the three seven seconds later on average. The small database had 5 runs at most.

On a bucket a merge of a large database takes as long as its link takes to carry the database twice over, so a database that waited for a turn with 2.1.9 waited minutes, and what a merge reads again when it goes on is a request to the bucket each time, and download. Neither has been measured on a bucket.

### Over a slow link

```sh
DURABLE_DB=./durable-db SOAK_STORE=minio SOAK_LINK=8000000,16000000,40 SOAK_MIB=2048 SOAK_READ_CLIENTS=8 SOAK_SECONDS=30 scripts/soak-size.sh
```

`SOAK_LINK` puts `scripts/slow-link.py` between the server and MinIO: what the server sends is carried at 8 MB a second and what it is sent at 16, each shared by all its connections as one link's is, and each byte arrives 20 ms after it left. The Backblaze figures above were measured over a link that carried 0.5 to 4.7 MB a second up and was not the same from one run to the next; this one is.

**The bound on runs.** Since 2.1.9 a write waits while a merge would leave a database with more than twenty-four runs, the runs that merge takes counting as one (finding 45). The figure was chosen. The runs below set it against 12 and 48, with builds of 2.1.10 that differ from one another only in it: the factor of `max_runs` in `Db::crowded` in `src/db.rs`, 1, 2 as released, and 4, each built with `cargo build --release`. Two runs of 12 and of 24 and one of 48, one after another on the desktop, the evening after 2.1.10 was released, with nothing else running; a database of 2 GiB filled by 64 clients and read by 8.

| Writes wait while a merge would leave more than | 12, first | 12, second | 24, first | 24, second | 48 |
| --- | ---: | ---: | ---: | ---: | ---: |
| The fill took | 2167 s | 1984 s | 1921 s | 1925 s | 1944 s |
| Commits a second while it was filled | 242 (p50 252 ms, p99 312 ms) | 264 (p50 254 ms, p99 307 ms) | 273 (p50 253 ms, p99 315 ms) | 272 (p50 254 ms, p99 309 ms) | 270 (p50 254 ms, p99 317 ms) |
| Commits that were told to wait | 212,248 | 44,023 | 0 | 0 | 0 |
| Most runs while it was filled | 23 | 24 | 20 | 22 | 25 |
| It then had | 70 tables in 14 runs | 76 tables in 23 runs | 72 tables in 18 runs | 75 tables in 22 runs | 76 tables in 25 runs |
| Sent to storage while it was filled | 11,531 MiB, 5.6 times what was written | 10,593 MiB, 5.2 times what was written | 11,005 MiB, 5.4 times what was written | 10,444 MiB, 5.1 times what was written | 10,404 MiB, 5.1 times what was written |
| The reads and scans looked in | 15, 12, 12 runs | 24, 24, 24 runs | 19, 19, 19 runs | 23, 23, 23 runs | 26, 26, 26 runs |
| Strong reads a second, at first | 22 (p50 324 ms, p99 1.7 s) | 10 (p50 820 ms, p99 3.6 s) | 15 (p50 523 ms, p99 2.7 s) | 12 (p50 671 ms, p99 3.4 s) | 10 (p50 800 ms, p99 3.6 s) |
| Strong reads a second, after that | 86 (p50 68 ms, p99 282 ms) | 19 (p50 447 ms, p99 841 ms) | 32 (p50 258 ms, p99 542 ms) | 23 (p50 368 ms, p99 769 ms) | 22 (p50 379 ms, p99 818 ms) |
| Scans a second | 69 (p50 116 ms, p99 204 ms) | 20 (p50 381 ms, p99 743 ms) | 27 (p50 289 ms, p99 512 ms) | 19 (p50 440 ms, p99 676 ms) | 20 (p50 393 ms, p99 591 ms) |
| Writes over it a second | 262 (p50 255 ms, p99 268 ms) | 110 (p50 254 ms, p99 268 ms) | 265 (p50 252 ms, p99 261 ms) | 255 (p50 257 ms, p99 298 ms) | 295 (p50 206 ms, p99 280 ms) |
| Writes over it that were told to wait | 0 | 29,607 | 0 | 0 | 0 |
| Runs after them | 4 | 16 | 23 | 15 | 15 |
| A write at a quota under that waited | 511 s | 582 s | 520 s | 553 s | 565 s |
| Most memory while it was filled | 260 MiB | 268 MiB | 260 MiB | 276 MiB | 278 MiB |
| The link carried, up and down | 14,344 and 4,450 MiB | 13,770 and 4,053 MiB | 13,778 and 3,293 MiB | 13,495 and 3,785 MiB | 13,509 and 3,888 MiB |
| Requests to storage that failed | 0 | 0 | 0 | 0 | 0 |

Neither 24 nor 48 was reached: no write of those three runs was told to wait, and their fills took the same time, 1,921 to 1,944 s. Every commit waited for its log's upload over the link, a quarter of a second, which held the 64 clients to some 270 commits a second, and the merges kept up with that to within 20 to 25 runs. A bound of 12 was reached: 212,248 and 44,023 commits were told to wait, the fills took 12 and 3 per cent longer than those of 24 and 48, and it did not keep the runs down, 23 and 24 at most, since the runs a merge under way takes count as one. Each server that opened the database after the kill began a merge of the runs that were more than twelve; in the second run of 12 the writes over the database were told to wait while that merge ran, 29,607 times, and were taken at 110 a second where the others took 255 to 295. The reads and scans followed how many runs the database had when its server was killed, which the moment of the kill decides, and not the bound. A read of a key that neither cache holds asks the store for a block in each run, one after another, each a round trip over this link: with 15 runs and then 12, 22 and 86 reads a second; with 19 to 26 runs, 10 to 32. So twenty-four stays. Over this link it was not reached, and twelve held writes back without holding the runs down.

A write at the quota waited 511 to 582 s. That is more than one merge of the database takes over the link, about 350 s for reading 1.2 GiB at 16 MB a second and writing 2 GiB at 8, since it waits as well for the end of a merge that the writes over the database had begun.

## What it costs in requests

Uploads are free on B2; HEAD and GET are billed per call, LIST at a higher rate. Per request:

| Request | Billable calls |
| --- | --- |
| Commit | One HEAD per group of transactions; uploads (one to four on B2) |
| Strong read | One HEAD per group of concurrent reads, at most two groups in flight |
| Session read from cache | None |
| Flush (every 8 MiB, 512 groups, or 5 s of quiet) | Two uploads, one HEAD, two version listings, and a removal for each listing page that found something to remove |
| Opening a database | Six or seven listings (two of them a background sweep for leftovers), one GET, one HEAD, two uploads |
| An idle database | Nothing |

A database committing as fast as B2 allows from one location runs about ten groups a second, which is under a million HEADs a day. Check your provider's current price list for what that means on your bill.

## What is not measured yet

- B2 from a server in a data centre, on a wired link. Every B2 figure here was measured over Wi-Fi from one desktop, for minutes at a time, and that includes the measurements behind the choice of storage pods.
- More than 256 databases committing at once on B2, or 256 for longer than eleven minutes: Backblaze then refused the account. Memory was 683 MiB and still rising at that point, and where it stops has not been measured.
- A rate of requests that Backblaze accepts from an account for hours. Its documentation gives a new account 500 a second.
- What sets the read figure of the Kubernetes test, which moves by a third from run to run, beyond the share of reads that are relayed.
- Amazon S3 or any provider other than B2.
- A server in the same region as its bucket.
- Many hours of sustained load.
- Several databases merging at once on a bucket, where a merge takes minutes and what a merge that gave its turn up reads again is download ([above](#several-databases-merging-at-once)).
- A database larger than 1 GiB on a provider's bucket, or larger than 16 GiB anywhere. A database larger than its server's disk cache on a bucket, where what [a large database](#a-large-database) shows of the disk cache would be paid for.
