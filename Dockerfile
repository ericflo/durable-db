# syntax=docker/dockerfile:1.7
FROM docker.io/library/node:24-bookworm-slim@sha256:c0a93797ee78814ae6d32fcca2561fdbbe423149c3f5389aad32d86e9fd85bd4 AS node
FROM docker.io/prom/prometheus:v3.16.0-distroless@sha256:e8b8fb0e336fe0affce6f9ea347b11d24975465fd3ba8119420186679c3efd5f AS alert-check
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

FROM model-tools AS model-provider-register
COPY spec/ProviderRegister.tla spec/ProviderRegister.checks spec/ProviderRegister*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh ProviderRegister && mkdir -p /checks && cp .tools/model-*.log /checks/ \
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

FROM model-tools AS model-databasecreation
COPY spec/DatabaseCreation.tla spec/DatabaseCreation.checks spec/DatabaseCreation.cfg spec/UnsafeCreationReceipt.cfg spec/UnsafeCreationConfig.cfg spec/UnsafeCreationName.cfg spec/UnsafeCreationResult.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh DatabaseCreation && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-pisession
COPY spec/PiSession.tla spec/PiSession.checks spec/PiSession.cfg spec/UnsafePiOwner.cfg spec/UnsafePiReceipt.cfg spec/UnsafePiPrefix.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PiSession && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-signingrotation
COPY spec/SigningRotation.tla spec/SigningRotation.checks spec/SigningRotation.cfg spec/UnsafeSigningPromotion.cfg spec/UnsafeSigningRetirement.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh SigningRotation && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

# Research model: checked independently without adopting its runtime assumptions.
FROM model-tools AS model-candidate-delegatedgrant
COPY spec/candidates/DelegatedGrant.tla spec/candidates/DelegatedGrant.checks spec/candidates/DelegatedGrant*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh DelegatedGrant && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-ownedwal
COPY spec/candidates/OwnedWal.tla spec/candidates/OwnedWal.checks spec/candidates/OwnedWal*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh OwnedWal && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-journal
COPY spec/Journal.tla spec/Journal.checks spec/Journal.cfg spec/UnsafeJournal*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh Journal && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-rootcontrol
COPY spec/candidates/RootControl.tla spec/candidates/RootControl.checks spec/candidates/RootControl*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootControl && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-transactionclient
COPY spec/TransactionClient.tla spec/TransactionClient.checks spec/TransactionClient.cfg spec/UnsafeTransactionRetry.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh TransactionClient && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-transactionvalidation
COPY spec/TransactionValidation.tla spec/TransactionValidation.checks spec/TransactionValidation*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh TransactionValidation && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-transactionrecovery
COPY spec/TransactionRecovery.tla spec/TransactionRecovery.checks spec/TransactionRecovery*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh TransactionRecovery && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-livetransactions
COPY spec/LiveTransactions.tla spec/LiveTransactions.checks spec/LiveTransactions*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh LiveTransactions && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-externalfiles
COPY spec/ExternalFiles.tla spec/ExternalFiles.checks spec/ExternalFiles*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh ExternalFiles && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-sortedtable
COPY spec/SortedTable.tla spec/SortedTable.checks spec/SortedTable*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh SortedTable && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-tablemerge
COPY spec/TableMerge.tla spec/TableMerge.checks spec/TableMerge*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh TableMerge && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-memoryindex
COPY spec/MemoryIndex.tla spec/MemoryIndex.checks spec/MemoryIndex*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh MemoryIndex && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-checkpoint
COPY spec/Checkpoint.tla spec/Checkpoint.checks spec/Checkpoint.cfg spec/CheckpointUnsafe*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh Checkpoint && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-checkpointread
COPY spec/CheckpointRead.tla spec/CheckpointRead.checks spec/CheckpointRead*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh CheckpointRead && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-checkpointoverlay
COPY spec/CheckpointOverlay.tla spec/CheckpointOverlay.checks spec/CheckpointOverlay*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh CheckpointOverlay && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-ownershipclaim
COPY spec/OwnershipClaim.tla spec/OwnershipClaim.checks spec/OwnershipClaim.cfg spec/UnsafeClaimRetarget.cfg spec/UnsafeClaimComparison.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh OwnershipClaim && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-uploadcredits
COPY spec/UploadCredits.tla spec/UploadCredits.checks spec/UploadCredits.cfg spec/UnsafeUploadRefund.cfg spec/UnsafeUploadRestore.cfg spec/UnsafeUploadBeforeCharge.cfg spec/UnsafeUploadRefillReset.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh UploadCredits && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-requestbudget
COPY spec/RequestBudget.tla spec/RequestBudget.checks spec/RequestBudget*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RequestBudget && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-globalcollection
COPY spec/GlobalCollection.tla spec/GlobalCollection.checks spec/GlobalCollection.cfg spec/GlobalCollectionConcurrent.cfg spec/UnsafeGlobalOrigin.cfg spec/UnsafeGlobalFence.cfg spec/UnsafeGlobalLateList.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh GlobalCollection && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-logicalbackup
COPY spec/LogicalBackup.tla spec/LogicalBackup.checks spec/LogicalBackup.cfg spec/UnsafeBackupScan.cfg spec/UnsafeBackupTruncated.cfg spec/UnsafeBackupForeign.cfg spec/UnsafeBackupReplay.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh LogicalBackup && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-publicationsafety
COPY spec/PublicationSafety.tla spec/PublicationSafety.checks spec/PublicationSafety.cfg spec/UnsafeProofOrigin.cfg spec/UnsafeProofFence.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PublicationSafety && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

# The pinned TLAPS binary requires glibc >= 2.38. It is a verification tool only;
# the database keeps the existing Rust toolchain and static musl runtime.
FROM docker.io/library/ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852 AS proof-tools
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 zlib1g && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/install-tlaps.sh scripts/install-tlaps.sh
RUN scripts/install-tlaps.sh /opt/proof-tools
ENV TLAPM=/opt/proof-tools/tlapm/bin/tlapm
FROM proof-tools AS proof
COPY scripts/check-proof.sh scripts/check-proof.sh
COPY spec/PublicationSafety.tla spec/PublicationSafetyProof.tla spec/PublicationSafety.proof spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-proof.sh && mkdir -p /checks && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-tools AS proof-journal
COPY scripts/check-proof.sh scripts/check-proof.sh
COPY spec/Journal.tla spec/JournalProof.tla spec/Journal.proof spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-proof.sh Journal && mkdir -p /checks && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-tools AS proof-grant-bounds
COPY scripts/check-proof.sh scripts/check-proof.sh
COPY spec/candidates/GrantBounds.tla spec/candidates/GrantBoundsProof.tla spec/candidates/GrantBounds.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-proof.sh GrantBounds && mkdir -p /checks && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-tools AS proof-root-control
COPY scripts/check-proof.sh scripts/check-proof.sh
COPY spec/candidates/RootControl.tla spec/candidates/RootControlProof.tla spec/candidates/RootControl.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-proof.sh RootControl && mkdir -p /checks && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-tools AS proof-root-requests
COPY scripts/check-proof.sh scripts/check-proof.sh
COPY spec/candidates/RootRequests.tla spec/candidates/RootRequestsProof.tla spec/candidates/RootRequests.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-proof.sh RootRequests && mkdir -p /checks && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-root-control AS proof-root-journal
COPY spec/candidates/RootJournal.tla spec/candidates/RootJournalProof.tla spec/candidates/RootJournal.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    DURABLE_DB_PROOF_DEPENDENCIES_CHECKED=1 scripts/check-proof.sh RootJournal \
    && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-root-control AS proof-partition-control
COPY spec/candidates/PartitionControl.tla spec/candidates/PartitionControlProof.tla spec/candidates/PartitionControl.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    DURABLE_DB_PROOF_DEPENDENCIES_CHECKED=1 scripts/check-proof.sh PartitionControl \
    && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM proof-partition-control AS proof-partition-journal
COPY spec/candidates/PartitionJournal.tla spec/candidates/PartitionJournalProof.tla spec/candidates/PartitionJournal.proof spec/candidates/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    DURABLE_DB_PROOF_DEPENDENCIES_CHECKED=1 scripts/check-proof.sh PartitionJournal \
    && cp .tools/proof-*.log /checks/ && touch /checks/passed

FROM model-tools AS model-candidate-rootjournal
COPY spec/candidates/RootControl.tla spec/candidates/RootJournal.tla spec/candidates/RootJournal.checks spec/candidates/RootJournal*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootJournal && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-partitioncontrol
COPY spec/candidates/RootControl.tla spec/candidates/PartitionControl.tla spec/candidates/PartitionControl.checks spec/candidates/PartitionControl*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PartitionControl && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-partitionjournal
COPY spec/candidates/RootControl.tla spec/candidates/PartitionControl.tla spec/candidates/PartitionJournal.tla spec/candidates/PartitionJournal.checks spec/candidates/PartitionJournal*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PartitionJournal && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-latecontrol
COPY spec/candidates/RootControl.tla spec/candidates/PartitionControl.tla spec/candidates/PartitionJournal.tla spec/candidates/LateControlCoverage.tla spec/candidates/LateControlCoverage.checks spec/candidates/LateControl*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh LateControlCoverage && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-placementrequests
COPY spec/candidates/PlacementRequests.tla spec/candidates/PlacementRequests.checks spec/candidates/PlacementRequests*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PlacementRequests && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-placementtakeover
COPY spec/candidates/RootControl.tla spec/candidates/PlacementTakeover.tla spec/candidates/PlacementTakeover.checks spec/candidates/PlacementTakeover*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh PlacementTakeover && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-rootrequests
COPY spec/candidates/RootRequests.tla spec/candidates/RootRequests.checks spec/candidates/RootRequests*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootRequests && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-rootacquisition
COPY spec/candidates/RootAcquisition.tla spec/candidates/RootAcquisition.checks spec/candidates/RootAcquisition*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootAcquisition && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-rootregistration
COPY spec/candidates/RootAcquisition.tla spec/candidates/RootRegistration.tla spec/candidates/RootRegistration.checks spec/candidates/RootRegistration*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootRegistration && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-rootcatalog
COPY spec/candidates/RootCatalog.tla spec/candidates/RootCatalog.checks spec/candidates/RootCatalog*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh RootCatalog && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM model-tools AS model-candidate-catalogreplay
COPY spec/candidates/CatalogReplay.tla spec/candidates/CatalogReplay.checks spec/candidates/CatalogReplay*.cfg spec/
RUN --mount=type=cache,id=durable-db-model-lock,target=/model-lock,sharing=locked \
    scripts/check-model.sh CatalogReplay && mkdir -p /checks && cp .tools/model-*.log /checks/ \
    && rm -rf .tools/tlc-* && touch /checks/passed

FROM scratch AS model
COPY --from=model-candidate-catalogreplay /checks/ /checks/
COPY --from=model-candidate-rootcatalog /checks/ /checks/
COPY --from=model-candidate-rootregistration /checks/ /checks/
COPY --from=model-candidate-rootacquisition /checks/ /checks/
COPY --from=proof-root-requests /checks/ /checks/
COPY --from=model-candidate-rootrequests /checks/ /checks/
COPY --from=model-candidate-placementtakeover /checks/ /checks/
COPY --from=model-candidate-placementrequests /checks/ /checks/
COPY --from=model-transactionvalidation /checks/ /checks/
COPY --from=model-transactionrecovery /checks/ /checks/
COPY --from=model-livetransactions /checks/ /checks/
COPY --from=model-externalfiles /checks/ /checks/
COPY --from=model-sortedtable /checks/ /checks/
COPY --from=model-tablemerge /checks/ /checks/
COPY --from=model-memoryindex /checks/ /checks/
COPY --from=model-checkpoint /checks/ /checks/
COPY --from=model-checkpointread /checks/ /checks/
COPY --from=model-checkpointoverlay /checks/ /checks/
COPY --from=model-candidate-latecontrol /checks/ /checks/
COPY --from=proof-partition-journal /checks/ /checks/
COPY --from=proof-partition-control /checks/ /checks/
COPY --from=model-candidate-partitionjournal /checks/ /checks/
COPY --from=model-candidate-partitioncontrol /checks/ /checks/
COPY --from=model-provider-register /checks/ /checks/
COPY --from=proof-root-journal /checks/ /checks/
COPY --from=model-candidate-rootjournal /checks/ /checks/
COPY --from=proof-root-control /checks/ /checks/
COPY --from=proof-grant-bounds /checks/ /checks/
COPY --from=model-candidate-rootcontrol /checks/ /checks/
COPY --from=model-candidate-delegatedgrant /checks/ /checks/
COPY --from=model-candidate-ownedwal /checks/ /checks/
COPY --from=model-journal /checks/ /checks/
COPY --from=proof-journal /checks/ /checks/
COPY --from=model-transactionclient /checks/ /checks/
COPY --from=model-ownershipclaim /checks/ /checks/
COPY --from=proof /checks/ /checks/
COPY --from=model-publicationsafety /checks/ /checks/
COPY --from=model-logicalbackup /checks/ /checks/
COPY --from=model-globalcollection /checks/ /checks/
COPY --from=model-cluster /checks/ /checks/
COPY --from=model-bucket /checks/ /checks/
COPY --from=model-readbarrier /checks/ /checks/
COPY --from=model-directory /checks/ /checks/
COPY --from=model-publicationretry /checks/ /checks/
COPY --from=model-publicationrebase /checks/ /checks/
COPY --from=model-observationbatch /checks/ /checks/
COPY --from=model-publicationcombining /checks/ /checks/
COPY --from=model-authorizationlifecycle /checks/ /checks/
COPY --from=model-databasecreation /checks/ /checks/
COPY --from=model-pisession /checks/ /checks/
COPY --from=model-signingrotation /checks/ /checks/
COPY --from=model-uploadcredits /checks/ /checks/
COPY --from=model-requestbudget /checks/ /checks/

FROM tools AS build
COPY Cargo.toml Cargo.lock rust-toolchain.toml .kache.toml ./
COPY .cargo .cargo
COPY src src
COPY tests tests
# Rust examples belong to the Rust check stage; application examples stay below.
COPY examples/*.rs examples/
RUN --mount=type=cache,id=durable-db-cargo,target=/usr/local/cargo/registry \
    --mount=type=cache,id=durable-db-kache,target=/cache/kache,sharing=locked \
    --mount=type=cache,id=durable-db-target,target=/cache/target,sharing=locked \
    trap 'kache daemon stop >/dev/null 2>&1 || true' EXIT; \
    cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings \
    && cargo test --locked --all-targets -- --skip ten_thousand_databases_split_page_recover_and_collect \
    && cargo test --locked --release --test publication ten_thousand_databases_split_page_recover_and_collect \
    && cargo build --locked --release --target x86_64-unknown-linux-musl \
    && mkdir -p /out /test-tools && cp /cache/target/debug/durable-db /test-tools/durable-db \
    && cp /cache/target/x86_64-unknown-linux-musl/release/durable-db /out/durable-db \
    && /out/durable-db --version && kache stats
# Application-only edits must not invalidate the Rust checks/build layer.
# This stage still tests against the executable built from this exact source.
FROM build AS client-check
COPY examples examples
COPY clients clients
COPY scripts/test-client.sh scripts/test-client.sh
COPY scripts/benchmark-http.mjs scripts/benchmark-http.mjs
COPY scripts/test-benchmark.mjs scripts/test-benchmark.mjs
RUN DURABLE_DB_TEST_BINARY=/test-tools/durable-db scripts/test-client.sh \
    && mkdir -p /checks && touch /checks/javascript.passed

FROM build AS assembled
COPY --from=client-check /checks/javascript.passed /out/checks/javascript.passed
COPY --from=proof-tools /opt/proof-tools/toolchain.tar.gz /out/verification-tools/tlapm-c8ddc44-linux-x86_64.tar.gz
COPY --from=proof-tools /opt/proof-tools/tlapm/doc/tlapm/LICENSE /out/verification-tools/LICENSE.tlapm
COPY --from=model /checks /out/checks
COPY --from=alert-check /rules /out/checks/prometheus
COPY site site
COPY docs docs
COPY deploy/prometheus deploy/prometheus
COPY scripts/install.sh scripts/install.sh
COPY scripts/publish-docs.sh scripts/test-publish-docs.mjs scripts/github_known_hosts scripts/
COPY LICENSE README.md ./
RUN node --test scripts/test-publish-docs.mjs \
    && npm --prefix site ci --no-audit --no-fund && npm --prefix site run build \
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
