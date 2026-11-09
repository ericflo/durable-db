# An unbounded publication and collection safety proof

`PublicationSafetyProof.tla` proves `Spec => []LiveExists`: every page referenced by the authoritative logical root exists, throughout the abstract protocol. TLAPS checks all **35 proof obligations** with fresh fingerprints and strict failure handling. The theorem has **no finite bound on jobs, collectors, page identities, fence advances, or execution length**. It is a proof of this abstract invariant under the assumptions below. It is not yet a checked refinement of the Rust implementation or a proof of the complete database contract.

## Why the invariant survives

A job captures the collection fence when it observes its source references, before preparing fresh objects. A collector lists candidates before conditionally advancing that fence and protecting the current root. A publication must still carry its original fence. References carried forward from the observed root remain protected from older collectors; freshly allocated identities cannot occur in those older candidate lists. A collector that starts later invalidates the earlier job before deleting its unprotected objects.

The inductive invariant records these obligations for live roots and pending jobs. Initialization establishes it. Separate lemmas prove preservation for preparation, upload, publication, candidate listing, fencing, deletion, and collector completion. A temporal induction step covers arbitrary repetitions and stuttering. Pending jobs may be published by any helper; their originating process is not an authority. Adding/removing processes does not change authority in this abstraction. A provider mutation still takes its modeled action when it succeeds; losing the reply itself stutters, and callers or helpers may be delayed arbitrarily. Permanent outages may prevent progress; this is a safety theorem.

The model deliberately permits stale candidate lists drawn from all previously allocated identities and arbitrary retained references when publishing a new root. It omits target-head comparisons, thereby allowing more publication schedules than the implementation. This makes it useful for page reachability across rebasing and concurrent databases, but unsuitable by itself for proving transaction linearizability, authorization, receipt semantics, or absence of unintended logical updates.

## Assumptions and refinement obligations

- Authority replacement serializes publication and collection fences. The chosen root and fence correspond to the successful conditional replacement; an uncertain fence never licenses deletion in the implementation.
- Object identities are globally fresh and never reused, including after process loss. `used` is a proof variable recording this history. UUID and digest collision resistance remain explicit implementation assumptions.
- A fence identity is never reused. The proof uses an increasing natural number as an abstract representation of fresh fence identities; it does not rely on a clock or a pod-local counter.
- Successful immutable uploads remain readable until a protocol deletion. Provider failure can delay actions. Loss of already-acknowledged provider data, operator tampering, and coherent malicious replacement of checksummed metadata are outside the theorem.
- The collector's protected references are those of its captured root. Missing or invalid directory/manifest content must stop collection. Corruption cannot be interpreted as an empty protected set.
- The Rust, HTTP transport, directory, manifest, authorization, quota, and metadata-collection layers still require a composed refinement argument. The proof does not establish their correctness merely because they are called by the same server.

`Published::get` supplies the original fence observation; `proposal::publish` checks it before submitting new metadata; proposal helpers retain their original fence. `collect_data` implements candidate listing, exact authority replacement, protected-manifest validation, and deletion. The adversarial tests and real B2/S3-compatible campaigns exercise this mapping, but are not a machine-checked implementation refinement.

## Reproduce and challenge the result

```sh
scripts/check-proof.sh
scripts/check-model.sh PublicationSafety
```

The proof script uses [TLAPS](https://github.com/tlaplus/tlapm) snapshot `c8ddc44a80b621989f96a9ee2c371301c52b109f`, from upstream asset ID `721898594`. Its SHA-256 is `fa6d537fb62f8526e7dd0d716c88e3d18931cc5c6b5d9ba1949afdb8096fb758`. The Linux x86_64 bundle needs glibc 2.38 or newer; CI uses a separate pinned Ubuntu 24.04 verification stage. The Rust build and static database runtime retain their existing toolchains. `TLAPM` can select a separately installed copy of that checker revision.

The upstream release tag rolls, so installation uses its fixed asset ID and validates exact bytes. The verified archive and upstream license are retained in the private Woodpecker build artifacts under `verification-tools/`. If upstream retires the asset, `TLAPS_ARCHIVE` can supply that preserved archive with the same checksum. The installer never silently upgrades the checker. No checker is required to run the database binary.

The command requires `--strict --nofp`; cached proof fingerprints cannot substitute for this run. It then modifies a temporary model to omit the collector's fence advance and requires the inductive proof to fail. That unsafe version fails **one of 35 obligations**. This tests the verification gate as well as the positive proof. The checker, its translation, bundled Z3 4.8.9/other proof backends, and the logical libraries are part of the trusted computing base. This run does not claim independent proof-certificate replay in another proof assistant. [TLAPS documentation](https://proofs.tlapl.us/doc/web/content/Documentation/Tutorial/A_simple_proof.html) explains the inductive proof structure.

TLC separately checks the same core model with two jobs, two collectors, three page identities, and at most two fence advances: **2,346,968 distinct states, 14,032,827 generated, depth 16**. Omitting the origin fence or collector fence produces a live-object-loss counterexample. Those finite checks are regression and negative-control evidence; the separate TLAPS theorem supplies the unbounded abstract result.

Liveness, complete protocol composition, implementation refinement, retained backups/PITR, global cost isolation, and independent review remain production obligations.
