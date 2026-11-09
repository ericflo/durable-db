# Development

Use the pinned Rust toolchain in `rust-toolchain.toml`. All Rust builds use [kache](https://github.com/kunobi-ninja/kache), including local tests. Source builds also need a C toolchain and CMake for AWS-LC; the installed static executable needs neither. Java 11+ runs TLC, and Node 24 runs the client, examples and documentation tools.

```sh
scripts/install-kache.sh
export PATH="$PWD/.tools/bin:$PATH"
export KACHE_CONFIG="$PWD/.kache.toml"

cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets -- --skip ten_thousand_databases_split_page_recover_and_collect
cargo test --locked --release --test publication ten_thousand_databases_split_page_recover_and_collect
scripts/check-model.sh
scripts/check-proof.sh
```

The two Rust test commands match Woodpecker: run the capacity regression in release mode instead of paying for its large directory sweep in debug mode. Both commands are required for the complete suite.

The model command checks every current-engine model and its required controls. The proof command above checks abstract publication safety; journal and research proofs have separate commands below. These checks do not establish that the Rust implementation refines the models. See [verification](verification.md) for each theorem's assumptions and scope.

## Work on one component

Run the checks affected by a change while iterating. Woodpecker retains the complete required suite.

| Change | Focused checks |
| --- | --- |
| Transaction validator | `cargo test --locked --lib transactions::validation` and `scripts/check-model.sh TransactionValidation` |
| Rust journal | `cargo test --locked --lib journal` |
| Common provider register | `cargo test --locked --lib providers::` and `scripts/check-model.sh ProviderRegister` |
| Control request session | `cargo test --locked --lib providers::` and `scripts/check-candidate.sh PlacementRequests` |
| Ownership intervals | `cargo test --locked --lib ownership::timing` and `scripts/check-proof.sh GrantBounds` |
| Candidate root request lifecycle | `scripts/check-candidate.sh RootRequests` and `scripts/check-proof.sh RootRequests` |
| An implemented protocol model | `scripts/check-model.sh MODULE` |
| Journal prefix proof | `scripts/check-model.sh Journal` and `scripts/check-proof.sh Journal` |
| SDK or web-app example | `scripts/test-client.sh sdk.test.mjs` |
| Pi adapter or HTTP client transport | `scripts/test-client.sh` |
| Public documentation | `npm --prefix site ci` and `node site/build.mjs` |

Build the debug executable before running the client script. Its focused SDK mode still checks types, formatting, the application example and benchmark accounting. Its default mode runs the full client and Pi suite, including both adapters, the 32 upstream Pi cases, indexed-adapter fault/resource tests and real Harness workflows.

Research models live in `spec/candidates/`. They are separate from the implemented serving protocol and do not authorize runtime assumption changes.

```sh
scripts/check-candidate.sh PartitionControl
scripts/check-proof.sh PartitionControl
scripts/check-candidate.sh PartitionJournal
scripts/check-proof.sh PartitionJournal
```

The candidate checker also supports `OwnedWal`, `DelegatedGrant`, `RootControl`, `RootJournal`, `LateControlCoverage`, `PlacementRequests`, `PlacementTakeover` and `RootRequests`. The proof checker supports `PublicationSafety` (the default), `Journal`, `GrantBounds`, `RootControl`, `RootJournal`, `RootRequests`, `PartitionControl` and `PartitionJournal`. Imported proofs are checked before their dependents. See the [candidate catalog](https://github.com/ericflo/durable-db/tree/main/spec/candidates) and [owned-partition design](design-owned-partitions.md) for the purpose of each component.

## Formal check inputs

Each model's `.checks` descriptor declares every positive configuration, unsafe variant and reachability control. An unsafe variant must violate its named invariant. A reachability control forbids a desired scenario and must produce a counterexample reaching it; this is coverage, not a liveness proof. The candidate wrapper copies the maintained dependencies into a temporary directory and removes it on exit. Logs remain under `.tools/candidates/`.

Each proof's `.proof` descriptor declares:

```text
tag|log-name
dependency|ImportedModule
negative|control-name|focused-theorem|expected-action|sed-mutation
```

Dependencies are optional; every proof requires a negative control. An empty theorem field checks the entire mutated proof. Every positive proof uses strict checking and fresh fingerprints. A mutation must change its source and fail its expected obligation; a parser or tool-launch error cannot pass. The runner requires all declared controls to execute and keeps checker input separate from descriptor input.

Focus a mutation on the first lemma affected by the changed protocol. Checking only a caller could accidentally assume the broken lemma. Large solver queries should be split into explicit proof steps so checks remain reliable on the CI builder; do not hide failures by weakening a property or reusing old fingerprints.

Adding a proof needs a descriptor and its own Docker stage. It should not require changing the common runner or invalidating unrelated proofs. CI may skip repeated dependency execution only when the stage inherits the exact checked dependency. Local commands check dependencies by default.

## Real provider checks

Cloud checks are opt-in and require a dedicated disposable bucket. Never run them against application data. Configure credentials privately through the provider environment; do not put them in commands, reports or logs.

```sh
export DURABLE_DB_DISPOSABLE=1
export DURABLE_DB_TEST_PROVIDER=b2 # or s3
export DURABLE_DB_TEST_BUCKET=durable-db-test-UNIQUE
export DURABLE_DB_TEST_CONTEXT='Client placement and provider region'
export DURABLE_DB_REGISTER_REPORT="$PWD/register-report.json"
export DURABLE_DB_JOURNAL_REPORT="$PWD/journal-report.json"

cargo test --locked --test provider_register -- --ignored --exact unchanged_metadata_still_requires_an_exact_comparison
cargo test --locked --test journal_cloud -- --ignored --exact journal_reconnect_unknown_responses_and_late_uploads
```

The register fixture permits at most thirteen conditional attempts, including a bounded reconnect barrier. The journal fixture permits seventeen PUT attempts of at most 33 KiB each. Provisioning, reads, verification and cleanup are additional operations. After success or failure, remove every physical version, the bucket and its temporary credentials. MinIO exercises the S3 binding; it does not qualify AWS. The older publication fixture has its own environment contract in `tests/s3.rs`.

For component work, `Native::connect_storage` and `S3::connect_storage` return the same `providers::Connection`. Its `objects()` and `authority()` expose transports without initializing publication metadata. `into_published` explicitly opens the current engine. Each conditional register replacement receives a fresh mutation identity, including replacements with otherwise identical metadata. Raw connections do not grant serving permission or provide tenant admission. HTTP regressions check that immutable writes leave authority metadata untouched and that redirects or lost replies do not cause a second PUT.

## CI and publication

[Woodpecker](https://github.com/ericflo/durable-db/blob/main/.woodpecker.yaml) is the only CI system. GitHub Actions are disabled. Changes go directly to `main` while the repository is private. Successful main builds publish the public documentation with a repository-scoped SSH deploy key. A newer push cancels the previous running pipeline in this setup; finish local iteration before pushing and let the required run complete so verification and publication can finish.

The [Docker build](https://github.com/ericflo/durable-db/blob/main/Dockerfile) requires Rust formatting, lints and tests; the full client/Pi suite; every maintained model and its controls; and the publication, journal and candidate proofs. It builds the static Linux executable, runtime image and documentation artifacts. The 10,005-database regression retains every assertion and runs in release mode; the rest of the Rust suite runs in debug mode.

Rust, JavaScript, individual models and individual proofs have separate cache stages. Changing the client reuses the same source-built executable without repeating unchanged Rust work. A shared lock serializes formal-check heaps on the builder. Compiler stages stop their kache daemon on exit while preserving the compilation cache.

The pipeline archives its exact commit with canonical file permissions. Uncommitted files and checkout metadata are excluded, and the temporary context is removed on exit. File metadata matters to Docker cache reuse as well as content. [Docker cache invalidation](https://docs.docker.com/build/cache/invalidation/), [Git archive](https://git-scm.com/docs/git-archive).

## Dependency audits

```sh
scripts/audit-dependencies.sh
npm --prefix clients/javascript audit
npm --prefix examples/web-app audit
npm --prefix site audit
```

The Rust audit verifies the pinned cargo-audit binary, fetches current advisories, rejects vulnerabilities and warnings without exemptions, and records the advisory database commit. CI runs dependency audits outside Docker's layer cache, so new advisories can block an unchanged lockfile. A failed advisory fetch fails the gate. Audit success does not rule out undiscovered vulnerabilities.
