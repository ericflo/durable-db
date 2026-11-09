# Durable immutable-upload admission

**Design in progress; not an implemented feature or a production billing guarantee.** Logical quotas and per-pod rate limits are already implemented. They do not bound cumulative orphan uploads across autoscaling, repeated cancellation, and delayed provider requests. This design starts with a cluster-wide immutable-upload ceiling and leaves per-tenant allocation and other provider costs as explicit additional work.

## Charge before sending

The shared publication authority can allocate bounded, nonrefundable upload credits with its existing exact comparison. Allocation burns the entire grant durably before a process can use any of it. A process consumes credits before invoking an immutable PUT. Charge for the maximum number of underlying upload attempts, including bounded SDK retries; a logical API call is not necessarily one provider request. Both payload bytes and upload-attempt counts need separate ceilings, including zero-byte objects. Immutable pages, manifests, directory nodes, and proposals must all pass through this boundary, including a helper publishing another replica's proposal.

Unused credits are lost when their process disappears. They are never restored from a cache, transferred to a replacement, or refunded because a lease expired, a pod died, or a listing did not find an object. A charged request can still reach the provider after those observations. A lost allocation acknowledgement may strand credits; uncertain recovery must never manufacture another usable grant. This conservative waste is visible operationally and traded for a hard upper bound on admitted immutable payload/attempt volume.

Deleting an object does not refund cumulative credits. Otherwise a delayed duplicate version or repeated create/delete workload could exceed the admitted volume. Operators explicitly increase the cumulative ceilings using revision-checked, idempotent administrative intent. A refill is not an automatic clock reset. The control plane must distinguish funded capacity, allocated credit, actual observed use, and logical live bytes; they are different quantities.

The intended authority record is a fixed-size global budget, independent of database count. Per-process grant state is disposable: losing it can only lose usable credit. It must not be checkpointed and replayed as fresh permission. The global allocator is common to B2 and S3, and must not require S3-only conditional immutable writes. The S3 control update's retry boundary has been corrected before attempting this design.

## Scope and format requirements

This ceiling covers immutable object payloads and upload attempts. It does **not** by itself cover authority-register traffic, reads/listings/deletes, request headers, TCP retransmission, provider accounting overhead, download bandwidth, or every component of the provider bill. Admission itself requires control requests; an exhausted-budget caller must not be promised that it can make unlimited free attempts. Gateway-level distributed request/egress/admin budgets and per-tenant isolation remain required. One tenant must not be allowed to consume a shared cluster ceiling merely because its logical data quota is small.

Enforcement needs a new publication format that older readers/writers refuse, including an incompatible authority-root envelope; a startup-only format check is insufficient for a previously running process. The first enforceable deployment should use a fresh dedicated namespace/bucket with no older in-flight uploads. Enabling a new counter over an old namespace cannot retroactively account for uploads that an old process sent before migration. A migration contract and export/restore tooling must precede any broader upgrade claim.

The implementation must bound SDK upload multiplicity, audit redirects/multipart behavior, keep all immutable-upload paths behind the allocator, and preserve unknown outcomes if budget exhaustion occurs after a durable proposal was submitted. Exhaustion must not be turned into a definite transaction abort. Collection must continue when upload credit is exhausted so already unreachable objects can be reclaimed.

## Initial model and remaining obligations

`UploadCredits.tla` models fixed capacity in abstract positive upload units, two processes with crashes/restarts, grants whose acknowledgements can be lost, charging for a bounded number of attempts, and arrivals after all originating processes disappear. The safety argument is that total charged units never exceed durable allocation, delivered units never exceed the charge, and allocation never exceeds capacity. Byte and attempt dimensions require the same componentwise checks in the implementation.

Negative controls refund a dead process's grant, restore a previously spent grant from local state, or allow arrival before charging. The model deliberately has no expiration-based refund or clock authority. It is a finite abstraction, not a proof of HTTP request counting, the provider's billing system, the global allocator's implementation, or composition with publication/collection. Administrative replenishment, per-tenant fairness, migration, SDK refinement, and bounded cleanup under exhaustion still need modeling and adversarial implementation tests.
