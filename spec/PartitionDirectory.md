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
