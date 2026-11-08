# Development

Install Rust using the pinned `rust-toolchain.toml`, then run `scripts/install-kache.sh`. Put `.tools/bin` on PATH and set `KACHE_CONFIG` to the absolute project `.kache.toml`. The Cargo configuration always invokes the kache wrapper.

Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked --all-targets`. Java 11+ is needed by `scripts/check-model.sh`; it downloads a checksum-pinned TLC and checks both the safe protocol and an intentionally broken collector.

The network S3 test is opt-in; see `tests/s3.rs` for environment variables. Use a disposable bucket/prefix. The JavaScript package contains a bounded reference pi-durable 1.5.0 adapter and HTTP conformance tests; see [clients](clients.md).

CI and release automation are being configured in Woodpecker. GitHub Actions are disabled. Changes go directly to main while this private project is developed.
