# Signing keys and rolling rotation

v0.5.0 supports bounded overlapping verification keys and an expiring administrator capability; the original v0.1.0 binary predates them. It does not make a pod's key file a cluster consensus authority. Durable per-database token revocation remains in the database manifest.

The key file accepts the existing 32-byte hex secret or a strict JSON version-1 keyring containing one active hex key, at most three previous/next verification keys, and an explicit `allow_legacy_admin` boolean. Keys must be unique. All files are read once at process startup. Every key can verify tenant, monitoring, and new administrator capabilities; only the active key issues them. Existing tenant token encodings remain readable. The verifier tries at most four HMAC keys, without accepting caller-selected algorithms or unbounded key IDs.

New administrator capabilities carry only an expiration and a domain-separated HMAC, prefixed `ddb_control_`. They do not disclose the signing key. `token --admin` issues this format and honors `--ttl-seconds` (default one hour, maximum 30 days). The old `ddb_admin_` format contains the signing key itself. It remains accepted for a raw legacy hex file to preserve upgrades, and only when explicitly enabled in a JSON keyring. It must never be given to a tenant. Disabling the old envelope cannot undo a leaked signing key: an actor holding that key can forge other capabilities. Remove the compromised key and rotate database generations as appropriate.

For a planned rotation from A to B:

1. Distribute a keyring with A active and B verification-only. Restart every serving process and verify completion, including replacement pods and independent/offline issuers. All serving replicas must understand keyrings and the new administrator envelope before clients switch to that envelope.
2. Distribute B active with A verification-only. Restart every process and update offline issuers. Only after all old issuers have stopped does the last possible A token lifetime begin. Include clock skew and in-flight request/drain time in the waiting period. New signed capabilities are bounded to 30 days; a deployment using shorter lifetimes may use its enforced bound. Legacy administrator envelopes never expire and must be explicitly migrated or disabled; their holders possess the key and must also be treated as offline issuers.
3. After every A capability has expired or its holder has been migrated, distribute B alone and complete another restart. This ends acceptance of A on all serving replicas.

A process still serving with old configuration is part of the rollout, even if its control-plane connection is partitioned. A Kubernetes rollout status alone cannot prove such a process is fenced from clients. Restore from an old Secret or start an old offline issuer and the assumptions no longer hold. Emergency retirement may deliberately reject still-valid tokens; document that availability loss instead of treating it as a successful graceful rotation. There is no claim of immediate cluster-wide retirement while stale serving processes remain.

`SigningRotation.tla` models the three rollout phases, bounded issued capabilities and expiration, independent process restarts, all-process loss, and the required completion barriers. The invariant requires every live issued token to verify on every serving process. Negative controls promote B before distribution or retire A while a live A token remains. This is a finite abstract model of the operator procedure, not cryptographic verification, clock synchronization, Kubernetes fencing, or a proof of the Rust implementation. Those are explicit implementation and deployment obligations.


## Preparing the configuration

Keep key files on the operator side. These commands create new files with mode 0600 on Unix and refuse to overwrite an existing path. `keyring` accepts raw hex key files, validates duplicates and the four-key bound, and never prints key material.

```sh
durable-db keygen --output next.key
durable-db keyring --active-key-file current.key --verify-key-file next.key --output expanded.json
# Complete the first rollout with expanded.json mounted as --admin-key-file.
durable-db keyring --active-key-file next.key --verify-key-file current.key --output promoted.json
# Complete the second rollout; update offline issuers; wait for old capabilities.
durable-db keyring --active-key-file next.key --output retired.json
# Complete the retirement rollout with retired.json.
```

Use `--allow-legacy-admin` only while explicitly migrating old control clients. A legacy binary cannot read a JSON keyring or verify `ddb_control_` tokens. First upgrade every serving binary using the existing hex file and legacy control credential; then migrate control clients and start the keyring procedure. Mixed-version qualification scripts mint their bootstrap administrator credential with the old binary for this reason. New capabilities can be generated offline without printing them into logs:

```sh
umask 077
durable-db token --key-file promoted.json --admin --ttl-seconds 900 > admin.token
durable-db token --key-file promoted.json --metrics --ttl-seconds 3600 > metrics.token
```

Mount the keyring using the existing `--admin-key-file`/`DURABLE_DB_ADMIN_KEY_FILE` setting. A Secret update alone does not reload a running server. Use a completed rolling restart for each phase and keep autoscaled replacement pods on the same phase. Validate that old serving processes and offline issuers are gone before recording promotion or retirement as complete. Monitor rejection rates during each transition; retain the previous configuration for availability rollback only while its key remains trusted.
