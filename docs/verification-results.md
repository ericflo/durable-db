# Verification results

Results are for the initial development checkout on 2026-11-08. Exact final commit IDs and updated model counts are added after the final gate. Commands and bounds are in [verification](verification.md).

| Check | Result |
| --- | --- |
| Initial singleton-root cluster model | 8,575,596 distinct states; no invariant violations |
| Shared-page cluster model | 36,252,536 distinct states; 208,784,241 generated; depth 32; no invariant violations |
| Shared directory publication model | 8,246,048 distinct states; 61,692,865 generated; depth 21; no invariant violations |
| B2 missing-fence negative control | Missing-manifest counterexample at depth 8 |
| Shared publication fault tests | Five tests passed, including both collectors, metadata preservation, slot admission, and lost response recovery |
| Unsafe collector negative control | Counterexample found: publish then delete a formerly unreferenced object |
| Rust engine/HTTP initial suite | 10 tests passed, including 24 randomized reference-map cases |
| pi-durable 1.5.0 reference adapter | All 32 upstream storage conformance tests passed over HTTP (65 seconds) |
| Documentation site | Desktop/mobile rendering, no mobile overflow, 0→6 pod illustration controls passed |
| Independent HTTP processes | 1→3→1 servers, SIGKILL, all processes removed and reopened without caches; all 36 writes verified |
| Real S3-compatible endpoint | Shared directory protocol on both versioned and unversioned MinIO: writes, receipts, both collection layers, and cache-free reopen passed |
| Real B2 endpoint | Dedicated bucket and scoped key: five commits, replay receipts, physical reclamation, and fresh-client/cache-free recovery passed |
| Read barrier model | 39,332 distinct states; 276,019 generated; depth 13; negative control detects stale reads |
| Native HTTP wire test | Stale observation rejected by read barrier; exact version deletion verified |

Compaction preservation and process scaling tests also pass; build artifact and Kubernetes qualification checks continue. No cloud latency numbers or Kubernetes production qualification are claimed from local tests.

## Initial local latency baseline

One release-build run on an AMD Ryzen AI Max+ 395 laptop, local filesystem with fsync, 1 KiB values, sequential operations. Other development work was active. This is an initial microbenchmark, not cloud performance or a throughput claim.

| Operation | Samples | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Durable local write | 200 | 13.962 ms | 22.998 ms | 25.913 ms |
| Cached local key read | 2,000 | 0.313 ms | 0.346 ms | 0.486 ms |

Reproduce with `cargo run --locked --release --example bench` using the pinned kache wrapper. The results expose an optimization target: synchronous publication and page rewriting currently dominate small writes. No B2/S3 latency numbers have been measured yet.
