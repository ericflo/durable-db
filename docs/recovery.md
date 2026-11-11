# Logical backup and restore

The binary provides `durable-db backup export`, `inspect`, and `restore`. An export contains the keys and values of one database revision, its original logical quotas, and source identity. Restore creates a new protected database with a fresh authorization incarnation and verifies every value. It does not restore credentials, policy history, transaction receipts, or the original revision number.

This is an operator-driven recovery tool. Scheduled retained snapshots, point-in-time recovery, independent backup storage, and recurring restore drills remain production gates.

## Native checkpoint retention

The native Rust operator API now provides one durable backup slot per namespace through `Control::retain_checkpoint`, `retained_checkpoint` and `release_checkpoint(id)`. Retaining pins the already published checkpoint and its exact journal cut; it does not force a checkpoint or include newer journal entries. Use the returned reference with `checkpoint::Snapshot` for immutable readback. The slot covers every database in the shared checkpoint and requires an administrator-owned control handle. Tenant HTTP tokens cannot create, inspect or release it; there is no public HTTP or CLI retention endpoint yet.

Retain succeeds only after `Outcome::Confirmed`. On an uncertain reply, validate with `retained_checkpoint()` and compare its identity with the tentative result. Do not blindly recreate a request. Release compares the identity, refuses a different slot, and is idempotent when the slot is empty. Administrators must coordinate release with backup readers: retaining a descriptor in memory does not extend protection after release. There is no automatic expiry or clock dependency.

The collector protects both the current checkpoint and the retained checkpoint. Retention survives ownership takeover and cold reopening; publication preserves a concurrently changed slot without adopting a newer collection generation. Released objects become eligible for ordinary automatic collection. The slot uses at most 1 KiB of authority metadata and no immutable uploads, and cannot accumulate arbitrary historical images. It is a namespace backup primitive, not tenant-isolated snapshots, a backup archive, multiple restore points, scheduled retention or point-in-time recovery. Copies outside the namespace and recurring restore drills remain necessary production work.

Binaries before v0.12.0 reject the added authority field while a slot is occupied, preventing an older collector from deleting the image. Release it with the new runtime before downgrading. Namespaces without an occupied slot retain their existing format. [Verification](verification.md#native-checkpoint-retention) records the protocol mapping and tests.

## Local format upgrades

v0.16.0 retains the local storage format used by v0.9.0 through v0.15.0. These directories reopen directly, preserving values, revisions and receipts. No export is required between these versions. Budgeted native serving now requires `owned-candidate-20` and a fresh namespace; it refuses v0.15.0 and earlier accounting policies. There is no in-place budgeted-native migration. This does not change the cloud CLI publication format.

v0.9.0 switched local serving to the native journal and index. It refuses an older directory instead of interpreting it as an empty native store. The cloud CLI format is unchanged.

1. Keep the previous binary and directory. Pause application writes and use that binary to serve the old directory and export each database using the commands below.
2. Stop the old server. Start v0.9.0 with a **new** `--data` directory and restore each archive. Keep the old directory and exports until verification completes.
3. Mint new scoped credentials and switch application routing only after every restore succeeds. Restore preserves keys, values and logical quotas; authorization incarnations, receipts and revision numbers are new.

For a migration from pre-v0.9.0 storage, rollback uses the previous binary and untouched old directory. Writes accepted by the new server are absent from that old directory; export and reconcile them before routing applications back. Never run a pre-v0.9.0 binary against a native directory or replace its authority file. Missing authority in a nonempty native directory is refused.

## Export a consistent revision

Use a database-scoped read capability in a private token file. The API origin must use HTTPS, except that HTTP loopback is accepted for local development. Redirects are refused; credentials are never forwarded to another origin.

```sh
durable-db backup export \
  --url https://database.example.com \
  --token-file /run/secrets/source-read-token \
  --database application \
  --output application.jsonl

durable-db backup inspect --input application.jsonl
```

Every ordered scan requests the same revision. Page size decreases when large values exceed the API response limit. Collection may proceed concurrently, but a concurrent data write can invalidate the export; retry the entire export or pause application writes for the backup window. This is not a retained snapshot that guarantees completion under continuous writes. A final identity/revision check also refuses a changed source.

The output is published only after complete validation, file synchronization, and a non-overwriting rename/link operation; the parent directory is synchronized on Unix. Temporary files are mode 0600 and removed on ordinary errors. An existing output is never replaced. Abrupt process/host failure can leave an incomplete temporary file; it is not a completed backup. The final newline-delimited JSON contains plaintext data: place it in access-controlled, encrypted backup storage under independent retention and credentials.

Inspection reads a private copy, validates record order, UTF-8 key bounds, canonical base64, value limits, quotas, exact logical counts, a required SHA-256 footer, and absence of trailing records. Files are capped at 1 GiB, records at 100 KiB, and in-memory chunks at 512 KiB/128 pairs. The checksum detects corruption; it does not authenticate an archive against an attacker who can replace it and recompute the checksum. Use independently protected backup metadata/signatures for that threat model.

## Restore and resume

Keep the destination isolated from application clients throughout restore and verification. Do not issue tenant credentials or switch application routing until the command succeeds. The destination is a new protected database, and the tool never mints application capabilities. Administrators or offline issuers can still access it; the procedure assumes they do not modify it concurrently.

```sh
durable-db backup restore \
  --url https://recovery.example.com \
  --token-file /run/secrets/recovery-admin-token \
  --database application-recovered \
  --input application.jsonl
```

Restore validates the whole archive in a private spool before making any HTTP request. Later changes to the supplied file cannot change that validated stream. A deterministic creation identity binds the archive digest and destination name. An unrelated existing database is refused. The same archive and destination recover the original protected creation receipt, even after the data receipt window has passed.

Writes use deterministic chunks, exact expected revisions, and stable request IDs. Transient failures and commit-fence conflicts retry the identical request within a bounded recovery window; creation conflicts and read snapshot conflicts stop immediately. After interruption, rerun the identical command. Before continuing, restore verifies every previously committed chunk and its exact byte/key totals. Divergent or additional data stops recovery. A failed restore may leave a partial, operator-only destination; it does not claim completion or roll back acknowledged chunks. Retrying a different archive against that name fails the creation identity check.

After writing, restore rereads every value at the final target revision and checks total usage and incarnation. The JSON result records source revision, archive digest, destination info, and verified key count. Only then should the operator mint fresh scoped credentials and update routing. Old source tokens remain invalid for the new incarnation. This is not an atomic replacement of an existing live database.

## Verification boundary

`LogicalBackup.tla` was checked before implementation: two keys/values, three delayed restore requests, two source revision advances, 6,888 states (40,064 generated, depth 16). Four negative controls detect mixed-revision export, accepting truncation, taking over an unrelated destination, and applying a delayed chunk without its revision comparison. The abstraction assumes atomic source reads/commits, monotonic revisions, fresh destination ownership, an isolated restore destination, and an integrity-checked immutable archive. It does not model SHA-256 internals, filesystem durability, TLS, scheduling, or retained object-store roots.

Implementation qualification and cloud restore evidence are tracked in the [verification record](verification-results.md). Backup storage retention, encryption-key recovery, cross-region/provider disaster drills, ongoing scheduling, and recovery-time objectives require separate qualification.
