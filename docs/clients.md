# Clients and pi-durable

`clients/javascript/client.mjs` is a small fetch-based client. It keeps revision and request-ID semantics explicit. The initial API works with any HTTP client.

```js
import { DurableDb, encode } from './client.mjs';
const db = new DurableDb({ url, database: 'demo', token });
const snapshot = await db.read({ keys: ['counter'] });
const operation = {
  expected_revision: snapshot.revision,
  request_id: crypto.randomUUID(),
  writes: [{ key: 'message', value: encode('Hello') }],
};
await db.commit(operation);
// On an unknown transport outcome, retry operation unchanged.
```

For provisioning with a current source build, persist a creation intent and call `await db.createProtected({request_id, limits})` using administrator credentials. Retry that same object after an unknown outcome. It returns `{info, replayed}` with the original creation info. `db.create(limits)` remains the legacy format-1 operation for compatibility. `db.changeAccess({request_id, expected_policy_revision, change})` exposes the administrative [policy API](api.md#durable-token-revocation) with explicit retry identity. Use `await db.issueToken({write: true, ttl_seconds: 3600})` for a database-scoped application credential. Protected databases return its `token_id` alongside the bearer token for later revocation. Tenant applications receive this scoped token instead of the provisioning credential.

## Pi 1.5.0 indexed adapter

Current source builds include `clients/javascript/pi-indexed.mjs` and its `indexed/` modules:

```js
import { openIndexedPiStorage } from './pi-indexed.mjs';
const storage = await openIndexedPiStorage(db);
// Pass storage to Harness.open(storage, options, context).
// Close it after the Harness stops issuing operations.
```

Use a protected database and a scoped application token. The default prefix is `pi-indexed/`; choose another ASCII prefix explicitly when sharing a database among trusted applications. Only the adapter may write within its prefix. Database credentials authorize the entire database, so an untrusted application needs a separate database, not merely a separate prefix.

Conversations, entries, tasks, submissions, and documents have direct records and ordered indexes. Opening reads one metadata value and claims a fresh owner; it does not reconstruct a journal. Reads validate current ownership and use one database revision across all lookups. Fork-aware entry scans clip ancestor segments at their fork points. Document lookups use live-address and historical-incarnation indexes; rewindable content finds a base and applies its following deltas. Latest-only content is stored separately from document metadata, so scanning records does not read large values.

One Session owns each prefix. After a crashed host, `openIndexedPiStorage(db, {takeover: true})` explicitly replaces it. Old owners cannot publish, serve fresh reads, or release the new owner's claim. API pods remain interchangeable; this owner belongs to the Harness Session. Host scheduling and external tool effects still need their own fencing protocol. Normal close drains admitted operations before releasing ownership.

A commit stages the valid prefix of the supplied batches and atomically writes its records, indexes, document content, and metadata. Receipts follow durable publication. Unknown responses retry the identical request; the adapter's last-commit nonce can resolve publication after the database's 128-receipt window rolls over. An unresolved provider or ownership error fails the adapter until reopen. A semantic refusal changes none of the refused batch while retaining earlier valid batches in that call. A callback throwing after publication does not undo the durable data.

| Bound | Default / behavior |
| --- | --- |
| Serialized record or content | 512 KiB; large values use at most sixteen chunks in the same transaction |
| Commit | Database's 1 MiB JSON and 128 operations, including indexes, chunk deletions, metadata, and checks; oversized staging rejects before publication |
| Snapshot read work | 8 MiB accounted encoded payload, 4,096 keys/scan results, and 256 logical HTTP reads; each transient retry is separately bounded |
| Fork ancestry | 256 conversations; cycles reject |
| Rewindable document replay | At most 1,024 deltas and 2 MiB serialized base-plus-delta content; another delta requires a checkpoint before exceeding the bound |
| Scan results | 1–1,000 entries; sparse residual filters may exhaust the scan budget and require a narrower query |

The read, ancestry, scan, and delta bounds can be reduced with `maxReadBytes`, `maxRequests`, `maxScanned`, `maxAncestry`, and `maxDeltas`. These bound encoded work, not exact JavaScript heap/RSS. Rewindable history remains stored for as-of reads and consumes the database quota. Latest-only replacement and retirement remove obsolete content chunks atomically. There is no automatic conversion from the reference journal; use a fresh prefix/database for the indexed adapter. The adapter and schema are experimental, pinned to Pi 1.5.0, and still subject to cloud/upgrade qualification and independent review.

## Pi 1.5.0 reference adapter

```js
import { openReferencePiStorage } from './pi-reference.mjs';
const storage = await openReferencePiStorage(db);
// Pass storage to Harness.open(storage, options, context).
```

This adapter verifies end-to-end storage semantics using Pi's `MemoryStorage` as an execution reference and durable-db as authority. Commit batches are chunked into bounded KV values and atomically written with their metadata. Pi receives durability receipts only after the database commits. A storage owner token fences old Sessions; normal open refuses an existing owner. After a crashed owner, explicitly pass `takeover: true`; the old owner can no longer persist changes.

The adapter replays a log into memory and stages a detached reference state before publication. Its default log budget is 8 MiB. It is suitable for conformance, small examples, and validating crash/reopen behavior. It is **not** the final production storage adapter: open/staging work grows with history, and the log budget eventually stops writes. The database engine itself does not use this full-history design.

The indexed adapter above implements direct ordered records and indexes. Keep the reference adapter for semantic comparison and fixtures; it remains unsuitable for histories that exceed its bounded replay budget.
