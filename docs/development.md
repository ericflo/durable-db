# Development

Install Rust using the pinned `rust-toolchain.toml`, then run `scripts/install-kache.sh`. Put `.tools/bin` on PATH and set `KACHE_CONFIG` to the absolute project `.kache.toml`. The Cargo configuration always invokes the kache wrapper.

Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked --all-targets`. Java 11+ is needed by `scripts/check-model.sh`; it downloads a checksum-pinned TLC and checks both the safe protocol and an intentionally broken collector.

Run `scripts/audit-dependencies.sh` on Linux x86_64. It verifies a pinned cargo-audit 0.22.2 binary, fetches the current RustSec advisory database, and rejects vulnerabilities and warnings without exemptions. The JSON report includes the advisory database commit. Run `npm --prefix clients/javascript audit` and `npm --prefix site audit` for JavaScript dependencies. Woodpecker runs these gates outside Docker's layer cache, so a newly reported advisory can block an unchanged lockfile. The audit tools failing to fetch data fails the gate. Audit success is not a guarantee that undiscovered vulnerabilities are absent.

The object-store client uses AWS-LC through rustls; source builds need a C toolchain and CMake. The static binary includes these runtime components and needs no compiler on the user's machine. CI installs build prerequisites in its tools image.

The network S3 test is opt-in; see `tests/s3.rs` for environment variables. Use a disposable bucket/prefix. The JavaScript package contains a bounded reference pi-durable 1.5.0 adapter and HTTP conformance tests; see [clients](clients.md).

Woodpecker builds the static Linux executable and runtime image with kache, checks Rust formatting/lints/tests, runs all five TLA+ models and negative controls, and runs the 32-case Pi HTTP conformance suite. After successful main builds it publishes the public documentation through a repository-scoped SSH deploy key. The pipeline is in `.woodpecker.yaml`; build steps are in `Dockerfile`. Run `scripts/test-client.sh` locally after building the debug executable. GitHub Actions are disabled. Changes go directly to main while this private project is developed.
