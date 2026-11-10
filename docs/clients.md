# Clients and pi-durable

The TypeScript JSON API provides `get`, `set`, `delete`, ordered `list` and atomic `transaction` calls:

```ts
import { Database } from '@durable-db/client';

const db = new Database({ url, database: 'my-app', token: scopedApplicationToken });
await db.set('settings', { theme: 'dark' });
const settings = await db.get<{ theme: string }>('settings');

await db.transaction(async (tx) => {
  const count = (await tx.get<number>('views')) ?? 0;
  tx.set('views', count + 1);
});
```

The [web application example](web-app.md) uses this API for a task board, preferences and an atomic completion counter. The package is private; the source example links `@durable-db/client` locally and builds it with `npm ci`. Node.js 24 is the tested runtime. Type parameters describe application schemas and do not validate stored data at runtime.

## JSON API behavior

Missing keys return `undefined`. Values are plain finite JSON, bounded to 64 KiB and 64 nesting levels. Transactions allow 128 distinct read keys, 128 write keys and a 1 MiB encoded request; reads see earlier staged writes. `list` returns `{ items: [{ key, value }], cursor }`, with 1–1,000 records per page. Cursors bind prefix and revision; a write between pages returns 409 and requires restarting the listing.

The callback can rerun after a definite conflict, up to five attempts. Await every transaction read and keep external side effects outside the callback. Lost commit acknowledgements retry the identical request up to five times. If publication remains unresolved, `UncertainCommitError` exposes the original wire request for reconciliation; do not blindly repeat a non-idempotent operation. `RetryLimitError` reports exhausted retries without uncertain publication. Individual HTTP attempts time out after 35 seconds; these bounds do not impose a deadline on arbitrary callback code.

The SDK stores no hidden application receipts. Across separate application requests, use stable business identities or application idempotency records where needed. Credentials belong on the application server, never in a browser bundle.

## Wire API and provisioning

`DurableDb` is the lower-level binary wire client used by the Pi adapters and provisioning tools. It exposes `read`, `commit`, `encode`, explicit revisions and request IDs. Each call makes one HTTP attempt. Retry uncertain commits unchanged. Redirects are refused; connect directly to the final HTTPS origin.

For provisioning with a current source build, persist a creation intent and call `await db.createProtected({request_id, limits})` using administrator credentials. Retry that same object after an unknown outcome. It returns `{info, replayed}` with the original creation info. `db.create(limits)` remains the legacy format-1 operation for compatibility. `db.changeAccess({request_id, expected_policy_revision, change})` exposes the administrative [policy API](api.md#durable-token-revocation) with explicit retry identity. Use `await db.issueToken({write: true, ttl_seconds: 3600})` for a database-scoped application credential. Protected databases return its `token_id` alongside the bearer token for later revocation. Tenant applications receive this scoped token instead of the provisioning credential.

## Pi 1.5.0 indexed adapter

v0.7.0 includes `clients/javascript/pi-indexed.mjs` and its `indexed/` modules:

```js
import { DurableDb } from '@durable-db/client';
import { openIndexedPiStorage } from '@durable-db/client/pi-indexed';

const piDatabase = new DurableDb({ url, database: 'pi-app', token: scopedApplicationToken });
const storage = await openIndexedPiStorage(piDatabase);
// Pass storage to Harness.open(storage, options, context).
// Close it after the Harness stops issuing operations.
```

Use a protected database and a scoped application token. The default prefix is `pi-indexed/`; choose another ASCII prefix explicitly when sharing a database among trusted applications. Only the adapter may write within its prefix. Database credentials authorize the entire database, so an untrusted application needs a separate database, not merely a separate prefix.

Conversations, entries, tasks, submissions, and documents have direct records and ordered indexes. Opening reads one metadata value and claims a fresh owner; it does not reconstruct a journal. Reads validate current ownership and use one database revision across all lookups. Fork-aware entry scans clip ancestor segments at their fork points. Document lookups use live-address and historical-incarnation indexes; rewindable content finds a base and applies its following deltas. Latest-only content is stored separately from document metadata, so scanning records does not read large values.

One Session owns each prefix. After a crashed host, `openIndexedPiStorage(piDatabase, {takeover: true})` explicitly replaces it. Takeover pins the first observed owner: it can retry a definite fence conflict or that owner's ongoing writes, but refuses a different owner appearing during retry/backoff. Old owners cannot publish, serve fresh reads, or release the new owner's claim. API pods remain interchangeable; this owner belongs to the Harness Session. Host scheduling and external tool effects still need their own fencing protocol. Normal close drains admitted operations before releasing ownership.

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

## Running and recovering a Harness

Give the Harness the indexed adapter, a host-owned model collection and registry, and a Chord context:

```js
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { Harness } from '@earendil-works/pi-durable';
const context = BACKGROUND_CONTEXT;
const storage = await openIndexedPiStorage(piDatabase);
const harness = await Harness.open(storage, { models, registry }, context);
const root = await harness.root(context, { agent: { model: selectedModel } });
// Persist requestId in the host's admission record before submitting or retrying.
const submission = await root.submit({ type: 'input', content, requestId }, context);
const result = await submission.wait(context);
await harness.close(context);
```

`selectedModel` identifies `{provider, modelId}` from the host's configured model collection. A browser reconnect should reacquire its submission from the owning Harness, rather than automatically taking over storage. Moving or replacing database API pods requires no Session ownership change. Replacing the Harness host does: open with explicit `takeover: true`, reopen the Harness, and reacquire the submission by its stored ID or repeat the same request ID. A still-running old owner fails when it next tries to use storage.

Cancelling the context passed to `submission.wait()` cancels that wait only. The durable run continues. `submission.abort()` withdraws queued input; `root.abort(context)` durably aborts the active conversation's work and waits for it to settle. A normal Harness close leaves interrupted work recoverable. A killed host leaves its ownership claim behind, requiring the explicit takeover above.

Pi records a tool's intent before executing it. After a crash, `replay: 'safe'` permits re-execution; an unsafe tool instead produces an interruption result. Declaring replay safe is an application responsibility. Database fencing prevents a stale host from storing its result, but cannot undo a payment, message, or other external side effect that the tool already performed. Such tools need an idempotency key or a fence enforced by that external service.

`harness.test.mjs` runs the actual Pi 1.5.0 Harness with its deterministic faux model, including lost commit acknowledgements, request deduplication after reopen, wait cancellation, queued withdrawal, durable abort, competing live owners, and SIGKILL during model requests and both replay-safe and unsafe tools. `scripts/qualify-pi-harness.py` runs those cases through three cache-free API processes and adds complete API-process removal and cold replacement while a tool waits. It requires an explicitly disposable provider bucket and private credential file; the caller removes that bucket after the campaign. Neither fixture sends requests to a paid model provider or claims exactly-once external tool execution.

## Pi 1.5.0 reference adapter

```js
import { openReferencePiStorage } from '@durable-db/client/pi-reference';
const storage = await openReferencePiStorage(piDatabase);
// Pass storage to Harness.open(storage, options, context).
```

This adapter verifies end-to-end storage semantics using Pi's `MemoryStorage` as an execution reference and durable-db as authority. Commit batches are chunked into bounded KV values and atomically written with their metadata. Pi receives durability receipts only after the database commits. A storage owner token fences old Sessions; normal open refuses an existing owner. After a crashed owner, explicitly pass `takeover: true`; the old owner can no longer persist changes.

The adapter replays a log into memory and stages a detached reference state before publication. Its default log budget is 8 MiB. It is suitable for conformance, small examples, and validating crash/reopen behavior. It is **not** the final production storage adapter: open/staging work grows with history, and the log budget eventually stops writes. The database engine itself does not use this full-history design.

The indexed adapter above implements direct ordered records and indexes. Keep the reference adapter for semantic comparison and fixtures; it remains unsuitable for histories that exceed its bounded replay budget.
