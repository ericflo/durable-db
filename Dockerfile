# syntax=docker/dockerfile:1.7
FROM docker.io/library/node:24-bookworm-slim@sha256:c0a93797ee78814ae6d32fcca2561fdbbe423149c3f5389aad32d86e9fd85bd4 AS node
FROM quay.io/prometheus/prometheus:v3.16.0-distroless@sha256:9764044e09eb2c9e1abdb12d0f0188d4efb7384b0262d3c080f0fc59045c89ba AS alert-check
WORKDIR /rules
COPY deploy/prometheus .
RUN ["/bin/promtool", "test", "rules", "alerts.test.yml"]
FROM docker.io/library/rust:1.90-bookworm@sha256:3f6e6f8d8725a65a2db964bb828850f888d430c68784d661f753144e5d787207 AS tools
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools cmake openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl && rustup component add clippy rustfmt
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
WORKDIR /src
COPY scripts/install-kache.sh scripts/install-kache.sh
RUN scripts/install-kache.sh /usr/local/bin
ENV KACHE_CONFIG=/src/.kache.toml KACHE_CACHE_DIR=/cache/kache KACHE_RUNTIME_DIR=/tmp/kache-runtime CARGO_TARGET_DIR=/cache/target
# Cache each model independently. The shared lock bounds concurrent TLC heaps;
# Rust may build alongside one checker without exhausting the builder's RAM.
FROM tools AS model-tools
COPY scripts/check-model.sh scripts/check-model.sh
RUN scripts/check-model.sh prepare

FROM model-tools AS model-cluster
COPY spec/Cluster.tla spec/Cluster.checks spec/Cluster.cfg spec/UnsafeGC.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh Cluster && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-bucket
COPY spec/Bucket.tla spec/Bucket.checks spec/Bucket.cfg spec/UnsafeBucket.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh Bucket && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-readbarrier
COPY spec/ReadBarrier.tla spec/ReadBarrier.checks spec/ReadBarrier.cfg spec/UnsafeRead.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh ReadBarrier && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-directory
COPY spec/Directory.tla spec/Directory.checks spec/Directory.cfg spec/UnsafeDirectory.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh Directory && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-publicationretry
COPY spec/PublicationRetry.tla spec/PublicationRetry.checks spec/PublicationRetry.cfg spec/UnsafePublicationRetry.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PublicationRetry && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-publicationrebase
COPY spec/PublicationRebase.tla spec/PublicationRebase.checks spec/PublicationRebase.cfg spec/UnsafeRebaseFence.cfg spec/UnsafeRebaseTarget.cfg spec/UnsafeRebaseMerge.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PublicationRebase && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-observationbatch
COPY spec/ObservationBatch.tla spec/ObservationBatch.checks spec/ObservationBatch.cfg spec/UnsafeObservationBatch.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh ObservationBatch && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-publicationcombining
COPY spec/PublicationCombining.tla spec/PublicationCombining.checks spec/PublicationCombining.cfg spec/UnsafeCombiningFence.cfg spec/UnsafeCombiningTarget.cfg spec/UnsafeCombiningMerge.cfg spec/UnsafeCombiningAbort.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PublicationCombining && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-authorizationlifecycle
COPY spec/AuthorizationLifecycle.tla spec/AuthorizationLifecycle.checks spec/AuthorizationLifecycle.cfg spec/UnsafeStaleAuthorization.cfg spec/UnsafeGeneration.cfg spec/UnsafeUserRevision.cfg spec/UnsafeAuthorizationRead.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh AuthorizationLifecycle && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM scratch AS model
COPY --from=model-cluster /checks/ /checks/
COPY --from=model-bucket /checks/ /checks/
COPY --from=model-readbarrier /checks/ /checks/
COPY --from=model-directory /checks/ /checks/
COPY --from=model-publicationretry /checks/ /checks/
COPY --from=model-publicationrebase /checks/ /checks/
COPY --from=model-observationbatch /checks/ /checks/
COPY --from=model-publicationcombining /checks/ /checks/
COPY --from=model-authorizationlifecycle /checks/ /checks/

FROM tools AS build
COPY Cargo.toml Cargo.lock rust-toolchain.toml .kache.toml ./
COPY .cargo .cargo
COPY src src
COPY tests tests
COPY examples examples
COPY clients clients
COPY scripts/test-client.sh scripts/test-client.sh
COPY scripts/benchmark-http.mjs scripts/benchmark-http.mjs
COPY scripts/test-benchmark.mjs scripts/test-benchmark.mjs
RUN --mount=type=cache,id=durable-db-cargo,target=/usr/local/cargo/registry \
    --mount=type=cache,id=durable-db-kache,target=/cache/kache,sharing=locked \
    --mount=type=cache,id=durable-db-target,target=/cache/target,sharing=locked \
    cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings \
    && cargo test --locked --all-targets -- --skip ten_thousand_databases_split_page_recover_and_collect \
    && cargo test --locked --release --test publication ten_thousand_databases_split_page_recover_and_collect \
    && DURABLE_DB_TEST_BINARY=/cache/target/debug/durable-db scripts/test-client.sh \
    && cargo build --locked --release --target x86_64-unknown-linux-musl \
    && mkdir -p /out && cp /cache/target/x86_64-unknown-linux-musl/release/durable-db /out/durable-db \
    && /out/durable-db --version && kache stats
FROM build AS assembled
COPY --from=model /checks /out/checks
COPY --from=alert-check /rules /out/checks/prometheus
COPY site site
COPY docs docs
COPY deploy/prometheus deploy/prometheus
COPY scripts/install.sh scripts/install.sh
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
