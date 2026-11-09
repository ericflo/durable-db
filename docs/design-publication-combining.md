# Combining pending publications: working design

**Status: implemented candidate under qualification; not a production-readiness claim.** The B2 profile at `d889213` measured 65 path rebases and 322 immutable PUT calls for a run with 44 successful commits including setup. Local read grouping does not address this repeated upload work. The next experiment combines publications from different replicas into one directory update while retaining the same B2/S3 authority boundary.

## Proposed transitions

1. The engine builds its transaction from a freshly validated database manifest and uploads fresh data pages as today.
2. Publication observes the directory, checks the expected database pointer, and captures the global collection fence. It uploads a fresh immutable manifest under its existing database namespace.
3. It uploads a small immutable proposal: database, expected old pointer (or absence), new manifest pointer/digest, and original global fence. Proposal fields and object namespaces have strict size/schema bounds. Object names are never reused.
4. Any replica servicing a write can help. It lists a bounded page of proposals, includes its own proposal explicitly, observes a directory version, and selects at most one eligible proposal per database. Eligibility requires matching the original global fence and expected database pointer. Missing or malformed objects cannot justify publication. Manifest bytes must exist and match their digest before selection is published.
5. The helper builds one directory update containing all selected changes, preserving every unrelated entry. A bulk path-copy operation should upload each changed leaf/branch once. Sequentially calling the current single-entry insertion would retain much of the amplification.
6. One authority CAS publishes the entire selection. A definite comparison mismatch permits rebuilding against another observation, with every proposal revalidated. Unknown outcomes remain unknown. A pod-local semaphore may reduce duplicate helper work, but safety must hold with arbitrary concurrent helpers and after every pod disappears.
7. A caller can acknowledge success when its own successful helper CAS included its manifest, or when a genuine authority observation selects that unique manifest. The latter can be a historical observation: it proves publication occurred after this call created its fresh identity. Failed lookup or absence does not prove that publication never happened.
8. Existing manifest/index collection lists candidates before fencing. Extend that pass to a bounded proposal listing before the same fence. Every listed proposal's old fence is invalidated; deleting those proposals cannot enable a later publication. A helper that loaded a proposal before deletion still cannot cross the fence. Acknowledged current manifests remain protected exactly as today. Late uploads become garbage for a later sweep.
9. After evidence of successful publication, a helper can enqueue the selected proposals for bounded asynchronous deletion. This is an optional cost optimization: the durable collector still handles lost, full, or interrupted cleanup queues. A loaded committed proposal cannot replace its original target again because manifest identities are never reused. Cleanup never deletes a manifest or directory page, and acknowledgement does not wait for it.

## Uncertain outcomes are central

Before submitting a proposal, publication may return a definite conflict. Once proposal upload has begun, another replica may commit it even after the originating request times out, is cancelled, or loses its process. Failure to observe it in the current head is **not** proof of failure: it could have committed and then been superseded. A changed collection fence is also not proof that it never committed.

After submission, return success only with publication evidence; otherwise return an uncertain outcome and use the engine's existing identical-request receipt recovery. Never convert a timeout or a missing current pointer into definite abort. A client must preserve its request ID, expected revision, and body. An observed state without the write does not prevent it appearing later while the outcome remains unresolved. This is already possible with delayed network writes; helping makes the case explicit.

Receipt retention and old expected revisions still bound replay behavior. Reissuing one request may create several proposals with distinct object identities, but only one can replace the original expected pointer. Competing writes to the same database remain optimistic conflicts. No cross-database transaction API is implied by combining independent publications.

## Model and test requirements

Before implementation, model competing helpers, duplicate targets, genuine stale authority observations, arbitrary caller disappearance, delayed proposal visibility, original unbatched writers, and list/read/fence/delete collection. Check current-manifest existence, preservation of unrelated writes, target comparison, acknowledged publication history, and the prohibition on later commits after a definite abort. Negative controls must expose fence omission, target omission, stale-root overwrite, and treating an uncertain proposal as aborted.

The first model may abstract directory allocation and the engine's data-page layer, but those composition/refinement obligations must remain explicit. A bounded safety run does not prove progress, arbitrary-size safety, or implementation correctness. A full helper's selection and a collector's protected root can both become stale; the authority CAS, rather than clocks or pod identities, resolves those races.

Bulk directory tests must cover mixed inserts/updates across several leaves, root splits, unchanged subtree reuse, ordered pagination, witness reachability, and collection. Deterministic publication tests must pause at proposal upload, helper selection, before/after CAS, and before acknowledgement; remove the originating replica; inject unknown results; and recover through another uncached replica. Include old single-publication writers in compatibility testing before retaining the existing format version.

## Bounded work and qualification

Use bounded proposal pages, a maximum of 64 selected databases, bounded concurrent object fetches, and the existing 512 KiB manifest limit. The selection bound is a batch-size limit, not a database-count cap. Helpers must include their own proposal without scanning the whole queue. A durable proposal-collection cursor must wrap so abandoned and delayed proposals eventually receive another pass. A helper can stop at any instruction without making a published manifest depend on its local state.

Measure provider calls, bytes, conflicts, latency, and final state with maintenance active on the same B2 and S3 implementation. Retain every failure and uncertain result in the report. Then test process loss and scale changes with operation-history checks. Physical admission, backup retention, credential lifecycle, and independent protocol review remain separate production obligations; a throughput improvement cannot close them.
