# Architecture experiments

These models explore possible replacements for the current serving protocol. They are not the implemented serving protocol and do not authorize changing runtime assumptions. The maintained journal component model and its unbounded abstract proof now live in `spec/Journal.tla` and `spec/JournalProof.tla`, replacing the former `WalHandoff` experiment. Run `scripts/check-model.sh Journal` and `scripts/check-proof.sh Journal` for that component.

## RootAcquisition

This component starts with an empty register and unordered competing candidate identities. Each identity binds once to an observed predecessor; claims capture an exact comparison and cannot retarget after timeout or conflict. Successful generations are contiguous, each has one winning identity, and a winning identity cannot reappear in another generation. Lost claims can apply after timeout and be recovered through exact validation. Raw observations may be any historical version.

Run `scripts/check-candidate.sh RootAcquisition`. The two- and three-candidate positive instances each allow four invocations, with 89,685 and 697,663 distinct states. Removing comparisons or allowing retargeting must fail. Two reachability controls exercise competing captured predecessors and recovery after a timed-out claim actually applies. There is no symmetry reduction or guided constraint. This is a finite acquisition model, not a serving-permission, liveness or Rust-refinement proof. The root controller also relies on the separate `RootRequests`, `RootControl` and `GrantBounds` contracts; their composition with the implementation remains an obligation.

## CatalogReplay

Ordered replay from a complete immutable selected catalog, assuming catalog and frame durability. Three journals with zero to two selected frames check all 27 initial cut combinations and 2,303 distinct states. Required controls detect skipping a prefix, extending it with late uploads and reversing epoch order; a fourth witnesses nonempty completion. Run `scripts/check-candidate.sh CatalogReplay`.

This is a byte-stream property, not a proof of the catalog's durability, storage-tree implementation, transaction recovery, bounded execution time or serving permission. A completed replay may outlive its root interval and still needs a final permission check before exposing a reconstructed view.

## RootCatalog

Conditional prefix catalogs with separate scan, immutable upload, positive knowledge, provider send, apply, reply and timeout events. Claims inherit existing selections, and exact comparison rejects stale publications. Renewal may refresh a scan's comparison only while its catalog is unchanged. Old uploads may arrive after selection; scans before the ownership drain are forbidden.

Run `scripts/check-candidate.sh RootCatalog`. Two epochs, three invocations and two frames check 2,816,947 distinct states. Three epochs, three invocations and one frame check 4,196,373 states. Five unsafe controls exercise comparison, inheritance, reselection, premature publication and early scans; two coverage controls witness late application after timeout and inheritance across a third epoch. No symmetry reduction or guided constraint is used.

Drain completion abstracts the ending of every earlier acknowledgement interval. Ordered epochs and discoverable journals are assumptions, established separately by other components rather than composed into this model. It does not prove physical timing, complete database/receipt recovery, collection, bounded service time, liveness or Rust refinement.

## RootRegistration

This extends acquisition with immutable journal registrations, durable predecessor links, uncertain uploads, positive-read resolution and explicit provider sends. Publication requires knowledge that the registration is durable. Recovery follows only the current register and persisted parent links; it does not use a process's captured observations or caches. Every winning journal remains discoverable, including former winners that might still acknowledge work under the separate permission contract.

Run `scripts/check-candidate.sh RootRegistration`. The same model now adds a separate positive permit for each registration and revokes the complete ancestor chain before declaring revocation complete. Two/three candidates with three invocations check **34,061**/**281,563** states in one/seven seconds. Three unsafe controls reject early publication, dropped predecessor links and revoking only the immediate predecessor. Two coverage controls exercise a three-generation discovery chain and a third claimant completing revocation after an interrupted second claimant.

Registration and permit creation are one abstract completion here; the dedicated permit model checks their wire ordering. Exact comparisons, unique immutable identities, fresh observations and permanent revocation remain assumptions. This is bounded discovery/revocation composition, not journal selection, placement, cleanup, liveness or Rust refinement. [Current bounds and evidence](../../docs/verification.md#durable-root-registrations).

Run a focused candidate and all its mandatory negative controls from the repository root:

```sh
scripts/check-candidate.sh RevocableOwner
scripts/check-candidate.sh DelegatedGrant
```

The wrapper uses the existing checksum-pinned checker in a temporary directory and saves logs under `.tools/candidates/`. It does not rebuild Rust or rerun unrelated models. Java must be available on `PATH`, or set `JAVA` to its executable.

## RevocableOwner

Timing-independent positive permits replace the standalone timed `OwnedWal` experiment. One permit, one selected successor, two old journal positions and one new write check **856 distinct states**, 1,690 generated, depth 15. The safe run takes under one second. Seven negative controls detect permit recreation, publication before upload, validation before durability, acknowledgement gaps, stale positive GETs, requests joining after validation, and scanning before revocation. Two coverage controls require an old read reply after the new write and an upload after successor activation.

Run `scripts/check-candidate.sh RevocableOwner`. No clocks or timers occur in the model. It assumes fresh per-key observations, one physical permit PUT completed before publication, exact acquisition/fresh identities, immutable journals and durable selection. This is one bounded handoff, not proof of repeated root replacement, placement, KV semantics, admission, cleanup, liveness or Rust. [Scope and evidence](../../docs/verification.md#candidate-revocable-writer-permits).

## DelegatedGrant

This separate candidate tests a way to avoid a conditional provider call for every partition's renewal. A coordinator holds an already acquired root grant and delegates shorter permission to owners. Each owner's interval is anchored at its own request start. If the coordinator has `R` clock units remaining and the slowest clock advances at half the fastest rate, the delegated duration is at most `floor(R / 2)`. It must not simply copy the remaining duration or start a new interval on response delivery. The old coordinator can continue issuing these bounded grants during the successor's drain; none may survive activation.

The checked instance contains one fixed root grant of length two, two independently clocked children, one request/reply per child, two real-time transitions, arbitrary pauses and permanent crashes, and a successor that fences and drains before activation. The positive run explores **1,258,368 states**, 6,754,337 generated, depth 15, in seven seconds. Five required controls fail for giving the full unscaled remaining duration, delayed-response expiry, a stopped paused coordinator clock, a stopped paused child clock, and skipped drain. The optimistic fingerprint collision estimate is 3.7e-7. Standard checking plus controls takes 17.551 seconds locally.

The model checks that a valid child grant remains within the root permission and that none is valid after successor activation. `DelegatedGrant.checks` has a separate cached CI stage. Run `scripts/check-candidate.sh DelegatedGrant` for the positive instance and every unsafe variant. Hashes and limits are in `docs/evidence/delegated-grant-candidate-2026-11-09.json`.

This is the grant-containment component, **not** full hierarchical ownership. It assumes a valid root grant, atomic issuing and fence transitions, bounded-rate continuously advancing clocks, and unchanged assignment during a grant. It does not model root acquisition/renewal, repeated child renewals, reassignment, restarts, durable log composition, message/resource bounds, liveness or actual throughput. The deployment clock contract remains unresolved. No runtime clock dependency is adopted.

## RootControl

This component checks repeated renewal and takeover under an abstract elapsed-time upper bound. It includes exact conditional renewals, request-start expiry, fence-observation drain, conditional activation, lost replies and delegated grants. The three-epoch/no-child instance checks 952,597 states; the two-epoch/repeated-root-and-child instance checks 3,417,821. Five faulty variants detect overlapping roots or a child outliving its parent. Two coverage controls require actual successive activation and deadline extension.

Run `scripts/check-candidate.sh RootControl`. The same descriptor is required in its own Woodpecker stage. The last two descriptor entries are forbidden-state coverage checks, not unsafe protocol variants. See `docs/verification.md` and `docs/evidence/root-control-candidate-2026-11-09.json` for exact bounds, assumptions and hashes.

Run `scripts/check-proof.sh RootControl` for the unbounded abstract permission proof. Its 153 obligations establish root exclusion and child containment/exclusion for arbitrary finite epoch counts and arbitrary execution and renewal counts. The finite regressions also check its inductive strengthening. Removing the current-owner renewal condition must fail the proof; both checks run in an independent CI stage. The theorem assumes the abstract timing contract and does not establish journal safety or runtime refinement.

The abstract time coordinate is a specification device. The deployment clock conversion, arbitrary candidate elections, placement changes and journal composition are not established. Pauses and loss are represented by delaying or omitting an incarnation's actions; replacement uses a new epoch. The clock and WAL candidates retain distinct obligations and remain necessary research sources. This model does not change the runtime clock rule or implement a controller.

## RootJournal

This candidate composes `RootControl` with journal recovery across repeated epochs. It checks contiguous durability, acknowledgements under valid permission, fresh recovery reads, immutable selections and complete inherited history before serving. Selection also advances the shared authority revision, so it can conflict with renewal. The root owns the journal directly; delegated partition movement is outside this component.

Run `scripts/check-candidate.sh RootJournal`. The wrapper copies its `RootControl` dependency, checks two positive instances, requires six unsafe variants to fail and requires a repeated-history coverage trace. The two-epoch/two-batch instance explores 12,474 states; the three-epoch/one-batch instance explores 2,230,369. Coverage shows the third owner retaining both predecessors' acknowledged prefixes. Exact bounds, assumptions and remaining obligations are in `docs/verification.md` and `docs/evidence/root-journal-candidate-2026-11-09.json`.

Run `scripts/check-proof.sh RootJournal` for the unbounded abstract composition theorem. All 1,061 obligations pass for arbitrary finite epoch counts and unbounded histories; removing acknowledgement permission fails the changed induction step. The checker verifies the imported 153-obligation root proof and its negative control first. Both finite positive instances also check the combined inductive strengthening.

The abstract timing contract and atomic durable selection remain assumptions. The theorem does not establish delegated movement, transactions, admission, reclamation, liveness or clock/platform/Rust correctness. It does not establish the revocable-permit mechanism or the isolated grant/clock proofs.

## GrantBounds

`GrantBoundsProof.tla` is an unbounded arithmetic proof of grant/drain margins, not another finite serving model. It permits a factor-two rate range and an additive elapsed-measurement error `E`. A drain needs `2L + 3E`; a child's request-anchored duration is at most `floor(max(0, R - 3E) / 2)`. All 19 obligations pass. Removing either margin fails its proof.

Run `scripts/check-proof.sh GrantBounds`. The candidate has a separate cached proof stage. It does not prove a platform timing contract, implement a clock, or establish composition with the root/log models. See the maintained verification document for the exact assumptions and remaining obligations.

## PartitionControl

This component combines the root protocol with independent partition generations and owner grants. Durable freeze/activation shares the root's conditional register; ordinary owner grants do not update it. Same-root movement drains old grants, and a later coordinator can complete an interrupted move after its own global drain without reading the former coordinator's clock.

Run `scripts/check-candidate.sh PartitionControl`. Four positive instances cover two independent partitions, two successive moves, owner renewal before movement, and coordinator loss during movement. Their state counts are 2,383,804, 545,839, 391,634 and 1,256,474. Three faulty variants expose superseded-assignment grants, early activation and expiry anchored to delivery. Four coverage checks require independent permission, a third generation, actual owner deadline extension before movement, and completion of an interrupted move by a later root.

These finite checks exercise permission composition with an atomic placement/issuance boundary and the abstract root timing contract. They do not prove the clock contract, uncertainty/refinement of placement updates, data recovery, early release, routing, admission, collection, liveness or the Rust implementation. The exact bounds and remaining obligations are in `docs/verification.md`; no runtime clock rule changes.

Run `scripts/check-proof.sh PartitionControl` for its unbounded abstract permission theorem: 687 obligations plus assignment, drain and reply mutations. The command checks the root proof dependency first. The strengthening is also required by the positive permission and journal configurations. Arbitrary finite partition/generation counts are covered under the abstract timing and placement contracts; journal history, wire uncertainty and platform correctness are separate obligations.

## PartitionJournal

Run `scripts/check-candidate.sh PartitionJournal` for delegated journal composition. It imports the actual root and partition-control transitions, then checks immutable upload/observation, contiguous acknowledgement, recovery, immutable prefix selection and complete serving history. Writer generations are independent of root epochs: a surviving writer can continue under a replacement coordinator, but replacing the writer allocates a fresh generation.

Five positive instances cover one movement, interrupted movement, two independent partitions, repeated movement and coordinator replacement without writer replacement. Seven unsafe controls and five history-reachability controls are mandatory. See [the verification scope](../../docs/verification.md#candidate-delegated-journal-recovery) for the exact bounds, assumptions and results. These are finite regressions alongside the unbounded proof below; neither adopts a runtime controller.

Run `scripts/check-proof.sh PartitionJournal` for `DelegatedHistorySafety`: 785 obligations for arbitrary finite roots, partitions and writer generations, and unbounded histories/executions. The proof imports the maintained partition/root proofs and checks their dependencies first. Six journal mutations must fail. Positive model configurations also check the inductive strengthening; an equivalence theorem preserves every original invariant while avoiding duplicate checks. The timing contract, atomic placement/selection and fresh object observations remain assumptions. Wire uncertainty, platform/Rust refinement, transactions, resource admission, reclamation and liveness remain outside this result.

## LateControlCoverage

This wrapper records actual late control applications in the delegated journal model, with previously acknowledged history. Its four mandatory reachability checks cover freeze and move completion after coordinator expiry, prefix selection after coordinator expiry, and selection after writer expiry while the coordinator remains valid. All original safety invariants remain enabled; a trace must violate only its named forbidden-event condition.

Run `scripts/check-candidate.sh LateControlCoverage`. The wrapper does not change the underlying transitions. Those transitions now admit late control application while still requiring the active coordinator identity to match; service and grant issuance retain their permission guards. This broadens the abstract proof scope but does not implement request/version/reply refinement. Woodpecker requires all four traces.

## Placement request uncertainty

Run `scripts/check-candidate.sh PlacementRequests` for the one-coordinator request/reply component. Its 359,761-state instance allows stale metadata observations, timed-out writes that apply later and stale replies after movement. It requires four faulty variants and four actual-event reachability traces. Invocation IDs are never reused; an uncertain result cannot restore issuance; a confirmed frozen view starts a conservative drain. The provider register must compare exactly even for an otherwise unchanged payload.

This does not replace the root, permission or journal models: it does not yet compose root replacement, independent clocks, restart or prefix selection. See [the scope and bounds](../../docs/verification.md#candidate-placement-requests-and-uncertain-replies).

## Placement requests during coordinator replacement

Run `scripts/check-candidate.sh PlacementTakeover`. This composes the actual `RootControl` actions with placement request start, provider application, timeout, stale observations and delayed replies. Root and placement operations compare the same revision. The positive instance checks 3,379,987 states; four faulty variants and a recovery witness are required. The witness issues replacement-generation permission after a timed-out predecessor freeze actually applied.

The seed is an already-active first root, followed by one successor; it does not model arbitrary repeated replacement or establish an unbounded refinement. The original root timing/epoch contracts remain assumptions. There is no journal or runtime adoption. [Bounds and limitations](../../docs/verification.md#candidate-placement-requests-across-root-replacement).

## Root request uncertainty

Run `scripts/check-candidate.sh RootRequests`. The model captures root fence, renewal/revalidation and activation invocations, including timeout before provider application and overlapping abandoned requests. An existing permission interval survives an attempted renewal but cannot be extended by its timeout. Exact revalidation recovers an uncertain fence or activation; a newly confirmed fence starts a drain, while recovering activation uses the drain completed before its original request.

Two unrestricted instances check 2,319,263 states with two epochs/four requests and 109,007 with three epochs/three requests. Four faulty controls and six actual-event witnesses are mandatory. Guided action constraints only remove schedules; both positive instances remain unrestricted. The fence and activation traces require actual application after timeout. A six-request trace reaches three successive live roots without overlapping permission. Raw observations can be any historical version, selected when the invocation captures its comparison.

Run `scripts/check-proof.sh RootRequests` for the separate unbounded abstract theorem: 983 strict obligations establish root exclusion and monotone authoritative epochs for arbitrary finite epoch/request counts, positive integer lifetimes and execution lengths. All four safety-guard mutations must fail, each targeting its first affected helper. Both positive finite instances also check the inductive strengthening.

The theorem starts from an already-active first root and assumes ordered winning incarnations and abstract elapsed-time bounds. It is not a refinement of `RootControl`, a competing-candidate election proof, physical-clock qualification or a composition with child grants, placement requests and journal history. No runtime controller is adopted. [Bounds, assumptions and evidence](../../docs/verification.md#candidate-root-requests-and-revalidation).

## Retired experiments

The initial `ServingLease` model used atomic authority-checked commits. Its timed successor `OwnedWal` was also retired when revocable permits became the selected experiment. The [OwnedWal evidence](../../docs/evidence/owned-wal-candidate-2026-11-09.json) retains its exact source commit and bounds; the old standalone sources/configurations and CI stage are removed. Its [historical evidence](../../docs/evidence/serving-authority-candidate-2026-11-09.json) identifies the source commit; the obsolete source and direct-run configurations are removed. The former `WalHandoff` model was similarly replaced by `Journal`; its [evidence](../../docs/evidence/wal-handoff-candidate-2026-11-09.json) remains available.
