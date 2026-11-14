# syntax=docker/dockerfile:1
# One build verifies and produces everything: the model check, the Rust and
# client tests, a static binary, the website, and a scratch runtime image.
FROM docker.io/library/node:24-bookworm-slim@sha256:c0a93797ee78814ae6d32fcca2561fdbbe423149c3f5389aad32d86e9fd85bd4 AS node

FROM docker.io/library/rust:1.90-bookworm@sha256:3f6e6f8d8725a65a2db964bb828850f888d430c68784d661f753144e5d787207 AS tools
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl && rustup component add clippy rustfmt
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
WORKDIR /src
COPY scripts/install-kache.sh scripts/install-kache.sh
RUN scripts/install-kache.sh /usr/local/bin
ENV KACHE_CONFIG=/src/.kache.toml KACHE_CACHE_DIR=/cache/kache KACHE_RUNTIME_DIR=/tmp/kache-runtime CARGO_TARGET_DIR=/cache/target

# The protocol model and its negative controls. Bounded: see scripts/check-model.sh.
FROM tools AS model
COPY spec spec
COPY scripts/check-model.sh scripts/check-model.sh
RUN scripts/check-model.sh && mkdir -p /checks && touch /checks/model.passed

FROM tools AS check
COPY Cargo.toml Cargo.lock rust-toolchain.toml .kache.toml ./
COPY .cargo .cargo
COPY src src
COPY tests tests
RUN --mount=type=cache,id=durable-db-cargo,target=/usr/local/cargo/registry \
    --mount=type=cache,id=durable-db-kache,target=/cache/kache,sharing=locked \
    --mount=type=cache,id=durable-db-target,target=/cache/target,sharing=locked \
    trap 'kache daemon stop >/dev/null 2>&1 || true' EXIT; \
    cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings \
    && cargo test --locked \
    && cp /cache/target/debug/durable-db /usr/local/bin/durable-db

# The SDK, the pi-durable adapter and the example, against the server just built.
FROM check AS client-check
COPY clients clients
COPY examples examples
COPY scripts/test-clients.sh scripts/test-clients.sh
RUN DURABLE_DB_BINARY=/usr/local/bin/durable-db scripts/test-clients.sh && mkdir -p /checks && touch /checks/clients.passed

FROM check AS build
RUN --mount=type=cache,id=durable-db-cargo,target=/usr/local/cargo/registry \
    --mount=type=cache,id=durable-db-kache,target=/cache/kache,sharing=locked \
    --mount=type=cache,id=durable-db-target,target=/cache/target,sharing=locked \
    trap 'kache daemon stop >/dev/null 2>&1 || true' EXIT; \
    cargo build --locked --release --target x86_64-unknown-linux-musl \
    && mkdir -p /out && cp /cache/target/x86_64-unknown-linux-musl/release/durable-db /out/durable-db \
    && /out/durable-db --version

FROM tools AS site
COPY site site
COPY docs docs
COPY clients/javascript/README.md clients/javascript/README.md
COPY LICENSE Cargo.toml ./
COPY scripts/install.sh scripts/install.sh
RUN npm --prefix site ci --no-audit --no-fund && npm --prefix site run build

FROM build AS assembled
COPY --from=model /checks/model.passed /out/checks/model.passed
COPY --from=client-check /checks/clients.passed /out/checks/clients.passed
COPY --from=site /src/.site /out/site
COPY LICENSE /out/LICENSE
RUN cd /out && tar czf durable-db-linux-amd64.tar.gz durable-db LICENSE \
    && sha256sum durable-db-linux-amd64.tar.gz > SHA256SUMS

FROM scratch AS artifacts
COPY --from=assembled /out/ /

# The runtime image is the static binary and nothing else. TLS roots are
# compiled in.
FROM scratch AS runtime
COPY --from=assembled /out/durable-db /durable-db
USER 65532:65532
EXPOSE 7654
ENTRYPOINT ["/durable-db"]
CMD ["serve", "--listen", "0.0.0.0:7654", "--data", "/data"]
