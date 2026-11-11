# Independent native control records

The partition directory reuses `PublicationRebase`, `PublicationSafety` and
`PublicationCombining`; it does not add another publication algorithm. A
directory entry contains one complete native `Bucket` record. Its immutable
pointer is the comparison identity. The entry's revision is local to that
partition, never the shared bucket revision. Each replacement gets a fresh
mutation nonce through the existing `Register` wrapper.

`PublicationRebase.head` is the target partition's control pointer and `other`
represents unrelated partitions. `OtherWritesPreserved` and
`TargetWriteNotOverwritten` therefore guard independent control updates. A
partition collector changes its own entry, defeating a publisher that captured
an older partition observation before uploading. The shared directory collector
uses the existing global publication fence to protect directory pages and
control manifests. Neither collector treats an observation as permission.

After a successful entry CAS, the adapter reads the published entry to obtain
its exact comparison token. It confirms only identical revision and metadata.
An intervening change or failed read returns **uncertain**, never conflict or a
token reconstructed from the old observation. A proposal can publish after its
caller disappears, as already modeled by `PublicationCombining`.

For each partition, `RootRegistration` and `RevocableOwner` apply unchanged.
Object names are mapped injectively to `partitions/<eight lowercase hex digits>/`.
GET, range GET, PUT, LIST cursors and physical deletion all use that scope.
Lists return relative names only after checking the prefix. Garbage collection
and permit revocation cannot reach another partition's objects. Object CAS is
not used. A partition is never deleted or renumbered by this adapter.

Format 6 distinguishes this directory from legacy database manifests. It has
a mandatory shared upload ceiling covering control publication and every
partition upload. Each partition additionally uses the existing native ceiling
and tenant ledgers; opening it does not reset counters. The shared collector
collects control metadata only; each native collector owns its scoped data.

These are a refinement mapping and implementation obligations, not a composed
proof. Rust tests must cover independent concurrent ownership, one-partition
takeover, uncertain publication, stale comparison, scoped listing/deletion,
budget exhaustion and recovery. Membership, route selection, automatic election,
placement policy and eventual progress are outside this adapter.

## Owner addresses and forwarding

An optional canonical socket address is published in the same native control
record as the fresh incarnation. A new claim never inherits its predecessor's
address. Discovery exposes open records only, as routing hints; it does not
establish that recovery finished or that a permit remains live. No new mutable
object or collector is added. Address-free local/remote records remain readable;
old binaries reject the new optional wire field when it is present.

A forwarder caches one route and includes the expected incarnation in every
request. An HTTP backend refuses a mismatch before dispatch. The header grants
no authority: normal authentication, tenant policy and fresh permit validation
still apply. This also stops forwarding loops and cross-partition address reuse.
The forwarder preserves tenant identity, policy generation/token ID and expiry
when signing for the peer; it never converts tenant access to administrator
access. Administrators use a short-lived signed token, never the signing key.

There is one HTTP attempt, without redirects, transport retries or proxy
inheritance. Failure/503 invalidates only that exact cached route instance; a
late failure cannot clear a replacement. The original caller resolves uncertain
writes with an identical request. Cache and connection pools have bounded
lifetimes/counts, requests and responses have byte ceilings, and in-flight calls
are bounded. Peer addresses and signing keys are trusted operator configuration;
network confidentiality requires a trusted private network or transport mesh.

`OwnerRouting` checks address reuse, stale hints, incarnation checks and the
single-attempt contract. `RevocableOwner` supplies the serving-order argument.
Neither proves membership, election, placement policy or eventual progress.

## Stable mapping and membership-driven placement

Before any partition entry exists, an exact conditional update installs a
versioned layout inside the existing directory-root envelope. Its partition
count is immutable. An incompatible opener fails; there is no implicit
conversion of manually populated directories. Every later root rewrite keeps
the layout from its successful comparison, including rebase after installation.
Older decoders refuse the new envelope field. No extra metadata slot is needed.
SHA-256 over a domain-separated database name chooses a stable partition;
membership never participates in this calculation. `ClusterLayout` checks this
boundary with two configurations and changing membership, plus erase/remap faults.

Rendezvous ranking of canonical peer addresses chooses a preferred node for each
partition. Membership is only a scheduling hint and may disagree, pause, omit
live nodes or be empty. It grants no authority. Each acquisition uses a fresh
root incarnation and the existing claim, revocation, journal selection and
private recovery path. Only a recovered owner enters the serving map. Unknown
control outcomes stop that attempt; a later attempt uses a new incarnation and
therefore discovers/revokes any abandoned winner through its registration chain.
These are the existing `RootAcquisition`, `RootRegistration`, `RevocableOwner`
and replay obligations, not a second ownership protocol.

Reconciliation is serialized per process. Each pass checks at most eight owned
partitions and advances a rotating cursor before I/O, including failed or
canceled attempts, so one failing partition cannot monopolize retries. Membership
updates and a retry timer trigger it. Timers affect progress
only. Removing a local placement drops its serving-map entry; already admitted
requests retain the exact backend they selected and still validate its permit.
An expected-incarnation header must be checked against that same pinned backend,
never one selected again after the check. The HTTP contract and tenant scopes
remain unchanged. All partition caches share one bounded byte cache. Aggregate index/validation
counts and payload bytes, and journal bytes, are divided by the fixed partition
count. Receipt-window, checkpoint and per-owner frame-count thresholds remain
per partition. Invalid shares fail before storage mutation. An acquisition first
reserves one of at most that many resident-owner slots; the reservation follows
private recovery, the native owner, requests and spawned I/O through retirement.
A removed map entry does not release it. Completed task results retain it until
consumed or dropped. Replacement waits if old references occupy all slots.
`OwnerResidency` checks this lifetime rule with two slots and three contenders,
including premature release and unchecked-acquisition faults. This bounds these
components, not allocator, SDK, checkpoint scratch or whole-process RSS; container
limits and measured peak-memory qualification remain required.

Safety does not require membership agreement. Eventual progress does: peers must
eventually receive the same healthy membership, provider operations must make
progress, upload credit must remain available, and the preferred node must keep
retrying. Permanent partitions and unlimited placement churn have no availability
promise. Membership discovery, Kubernetes qualification and CLI migration are
separate integration work; the library accepts explicit membership snapshots.

## Membership discovery

A headless Kubernetes Service supplies peer addresses through ordinary DNS. It
must publish not-ready endpoints so acquisition/readiness cannot deadlock with
discovery. All peers use the same service port and address family; duplicate
answers are canonicalized and more than 256 distinct peers are refused. The pod
IP is an advertised address, never a serving-authority credential.

Only one resolution may be outstanding. Lookup failure preserves the last valid
snapshot; a successful empty answer can remove all placements. A hung resolver
may delay placement, but cannot spawn an unbounded sequence of blocking lookups.
The lookup loop ends with its lifetime handle. Timers, DNS TTLs, omission, stale
answers and disagreement affect progress only. The existing layout and ownership
models admit arbitrary membership and empty sets; no DNS lease becomes authority.

## Cloud CLI cutover

The cloud CLI selects this controller for both B2 and S3; local mode keeps its
single-directory lock. Cloud startup requires a shared upload ceiling, validates
layout/resource/discovery configuration before provider mutation, and advertises
a reachable IP with the bound listener port. With DNS omitted it is single
process only. Kubernetes supplies the pod IP and headless-service name.

Partition connection construction performs no object initialization. The acquisition
loop initializes each preferred partition only after reserving its resident slot.
Fresh/partial namespaces at zero credit must still start administrative HTTP.

The shared upload ledger is the effective operator ceiling and remains available
through the administrative HTTP endpoint before owner readiness. Internal
partition ceilings use the format maximum so they cannot become lower,
unrefillable CLI limits; every physical upload still passes shared admission.
Fresh native directories are required: prior publication-engine namespaces are
refused, never implicitly rewritten. Export with the prior binary and restore
into a new namespace. Native owner workers collect their own partition objects. Shared directory
collection runs once per maintenance interval on the peer preferred for partition
zero, independently of owner readiness. This is a scheduling hint, not a collector
lease: disagreement can run concurrent collectors, which remain safe under the
existing list-before-fence protocol. Only one shared sweep may be in flight per
process. The old cloud CLI serving/maintenance branch is removed. Shutdown closes discovery and drains HTTP;
correctness does not depend on either action.

Acquisition scheduling completes one new owner per pass and immediately continues after success. Existing-owner health checks remain bounded at eight per pass. Provider request timeouts bound individual I/O; there is no fixed deadline for a complete recovery, which can legitimately need many requests. A changed membership snapshot or a closed feed cancels reconciliation; an identical snapshot does not. This avoids repeatedly abandoning a progressing recovery and amplifying its registration history. Cancellation remains safe under the existing acquisition protocol and resident reservations.

An uncertain journal PUT gets an exact resolving GET. A present matching frame
can complete normally. If that GET reports absence, the serving worker retires
instead of waiting forever for an object that may never arrive. This is not an
abort claim or permission to repeat the PUT. The cluster's normal acquisition
revokes the old permit and selects the old journal's durable prefix before
recovering. A late old PUT outside that prefix stays excluded, as checked by
`RevocableOwner`'s late-upload case and journal selection. Clients still retry
the identical request; only durable selected receipts can return success.
