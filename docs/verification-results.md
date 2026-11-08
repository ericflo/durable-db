# Verification results

Results are for the initial development checkout on 2026-11-08. Exact final commit IDs and updated model counts are added after the final gate. Commands and bounds are in [verification](verification.md).

| Check | Result |
| --- | --- |
| Initial singleton-root cluster model | 8,575,596 distinct states; no invariant violations |
| Shared-page cluster model | 36,252,536 distinct states; 208,784,241 generated; depth 32; no invariant violations |
| Unsafe collector negative control | Counterexample found: publish then delete a formerly unreferenced object |
| Rust engine/HTTP initial suite | 10 tests passed, including 24 randomized reference-map cases |
| Real S3-compatible endpoint | Disposable MinIO: conditional writes, retry receipts, collection, and cache-free reopen passed |

The B2 publication model, compaction tests, client conformance, process scaling test, formatting/lint gate, and build artifact checks are tracked during development. No cloud latency numbers or Kubernetes production qualification are claimed from local tests.
