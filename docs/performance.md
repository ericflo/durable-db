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

One server with the flags of the Kubernetes manifest (192 MiB of cache, 96 MiB of unflushed writes, 128 MiB each for request bodies and results), on a local directory, in a systemd scope limited to 1 GiB of memory without swap and to four CPUs. It is killed if it passes the limit. Three loads of a minute each run one after the other against the same server, with values of random text, which does not compress:

| Load | Requests/s | p50 | p99 | Told to wait | Most memory so far |
| --- | ---: | ---: | ---: | ---: | ---: |
| Half reads, half commits of 16 KiB values, 64 clients over 4 databases | 3,480 | 11 ms | 131 ms | 2 | 507 MiB |
| Commits of 1 KiB values, 256 clients over 256 databases | 6,604 | 21 ms | 143 ms | 0 | 591 MiB |
| Strong reads of 4 KiB values, 512 clients over 8 databases of 20,000 keys | 73,440 | 5.8 ms | 29 ms | 0 | 591 MiB |

The server, of release 2.1.5, used 591 MiB at most in this run, and 584 and 589 MiB in two others. No request failed in any. Release 2.1.4 used 582 and 590 MiB, 2.1.1 used 621 MiB and 2.1.0 used 618 and 659 MiB in runs of the same script; in those of 2.1.1 and 2.1.0 the server may have stopped keeping unflushed writes within their budget during the second load ([verification](verification.md#what-these-checks-found), finding 36).

The first load is not the same from one run to the next: 3,480, 5,428 and 5,846 requests a second in the three runs of 2.1.5, and 4,116 and 6,417 in two of 2.1.4, an hour apart on the same desktop. The other two loads moved by a few percent.

"Told to wait" counts answers of `503`. In the first load each of the four databases holds 160 MiB and is overwritten about four times in the minute, so tables are merged all the while; with four CPUs the flushes fall behind now and then, and the server refuses a write to that database instead of holding it in memory. That was 2 of the requests of that load in this run, 511 and 1,616 in the two others of 2.1.5, which is up to 0.5%, none in either run of 2.1.4, 0.4% in the run published with 2.1.1, and 1.7% in the one published with 2.1.0. A client waits and sends the request again, as the SDK does.

This is three minutes on a local directory. It says that the limits in [deployment](deployment.md#on-kubernetes) hold under these loads with this allocator, not what many hours or a bucket would show; on a bucket an upload is in memory for as long as the bucket takes to store it.

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
- Databases larger than a few hundred megabytes.
