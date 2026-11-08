# syntax=docker/dockerfile:1.7
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
FROM tools AS model
COPY spec spec
COPY scripts/check-model.sh scripts/check-model.sh
RUN scripts/check-model.sh && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM tools AS build
COPY Cargo.toml Cargo.lock rust-toolchain.toml .kache.toml ./
COPY .cargo .cargo
COPY src src
COPY tests tests
COPY examples examples
COPY clients clients
COPY scripts/test-client.sh scripts/test-client.sh
RUN --mount=type=cache,id=durable-db-cargo,target=/usr/local/cargo/registry \
    --mount=type=cache,id=durable-db-kache,target=/cache/kache,sharing=locked \
    --mount=type=cache,id=durable-db-target,target=/cache/target,sharing=locked \
    cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked --all-targets \
    && DURABLE_DB_TEST_BINARY=/cache/target/debug/durable-db scripts/test-client.sh \
    && cargo build --locked --release --target x86_64-unknown-linux-musl \
    && mkdir -p /out && cp /cache/target/x86_64-unknown-linux-musl/release/durable-db /out/durable-db \
    && /out/durable-db --version && kache stats
FROM build AS assembled
COPY --from=model /checks /out/checks
COPY site site
COPY docs docs
COPY LICENSE README.md ./
RUN npm --prefix site ci --no-audit --no-fund && npm --prefix site run build \
    && cp -a .site /out/site && cp LICENSE /out/site/LICENSE \
    && cp LICENSE README.md /out/ && cd /out \
    && tar czf durable-db-linux-amd64.tar.gz durable-db LICENSE \
    && sha256sum durable-db-linux-amd64.tar.gz > SHA256SUMS
FROM scratch AS artifacts
COPY --from=assembled /out/ /
FROM scratch AS runtime
COPY --from=assembled /out/durable-db /durable-db
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532
EXPOSE 7654
ENTRYPOINT ["/durable-db"]
CMD ["serve", "--listen", "0.0.0.0:7654", "--data", "/data"]
