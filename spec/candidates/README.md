# Architecture experiments

These models explore possible replacements for the current serving protocol. They are not the implemented serving protocol and do not authorize changing runtime assumptions. The maintained journal component model and its unbounded abstract proof now live in `spec/Journal.tla` and `spec/JournalProof.tla`, replacing the former `WalHandoff` experiment. Run `scripts/check-model.sh Journal` and `scripts/check-proof.sh Journal` for that component.

Run a focused candidate and all its mandatory negative controls from the repository root:

```sh
scripts/check-candidate.sh OwnedWal
scripts/check-candidate.sh DelegatedGrant
```

The wrapper uses the existing checksum-pinned checker in a temporary directory and saves logs under `.tools/candidates/`. It does not rebuild Rust or rerun unrelated models. Java must be available on `PATH`, or set `JAVA` to its executable. `ServingLease` predates these descriptors; its direct command is below.

## ServingLease

Question: can a former owner return a stale successful read after another owner commits, if ordinary reads use a local view rather than an object-store conditional barrier?

The candidate grants a finite serving interval through an atomic conditional authority operation. Expiry is measured from the local clock at **request start**, not response delivery. Takeover first fences further grants and writes, then waits out every possible old grant before allowing the replacement to commit. A process discards its grant and local view on restart. Reads check their interval even when returning cached values.

This requires a **bounded-rate, continuously advancing monotonic clock**, including while application execution is paused. The instance models two real-time units per tick and each process clock advancing by one or two units. The replacement therefore waits twice the lease length on its own clock. If an old process's clock can stop while another process takes over, the safety result no longer holds.

That assumption conflicts with the current runtime rule in `AGENTS.md` that a clock must not be a cluster correctness dependency. This is research into the cost of that rule, not an adopted exception. Choosing this direction requires an explicit architecture decision and an implementable, justified timing contract. A finite state exploration cannot establish real clock behavior on arbitrary hardware, virtual machines, suspend/resume or migration.

### Checked instance

Two processes, one ownership change, one value advance, three time steps and a one-unit lease. Processes may pause, crash and restart; grants may be delayed across takeover. Both processes can be absent. The positive run explores 4,884,220 distinct states, 28,643,250 generated, depth 25, in 34 seconds. Three separate unsafe configurations violate `NoStaleRead`:

- `UnsafeSkipDrain.cfg`: replacement activates before old grants have expired.
- `UnsafePausedClock.cfg`: the former owner's clock stops during its pause.
- `UnsafeLateGrant.cfg`: a delayed grant starts a fresh interval when delivered.

The positive result uses TLC fingerprints; its reported optimistic collision estimate is 6.3e-6. It is bounded checking, not an unbounded proof.

### What this does not establish

`Commit` is still an abstract atomic operation that checks the current authoritative owner and epoch. This model does **not** show how to publish a WAL batch with only an immutable PUT, fence delayed uploads, or recover a complete log from B2/S3. It does not establish independent partition write capacity, index/compaction correctness, retention, garbage collection, admission, auth revocation, general membership or liveness. Only one value advance is modeled. Removing a per-read barrier is one necessary part of the architecture investigation.

### Reproduce

From the repository root, prepare the pinned TLC tool and run the candidate:

```sh
scripts/check-model.sh prepare
cd spec/candidates
java -XX:+UseParallelGC -Xmx2g -cp ../../.tools/tla2tools-1.7.4.jar tlc2.TLC \
  -workers 2 -metadir ../../.tools/candidate-serving-lease \
  -config ServingLease.cfg ServingLease.tla
```

Repeat with each unsafe configuration and a separate `-metadir`; each must exit with TLC status 12 and report `Invariant NoStaleRead is violated`. The recorded source and log hashes are in `docs/evidence/serving-authority-candidate-2026-11-09.json`.


## OwnedWal

This candidate combines finite serving permission with immutable uploads and recovery for **one ownership transfer**. Unlike `ServingLease`, an old writer's acknowledgement does not atomically inspect the authoritative owner or epoch. It checks its local grant after upload and acknowledges only the contiguous durable prefix. Reads check the same grant. The successor fences further grants, waits for all old permission to expire, probes the log, durably seals its prefix and then activates. It can durably acknowledge one new batch. Read comparisons distinguish the actual old/new history, not just equal numeric sequence lengths.

The instance uses two old log positions, two real-time transitions, one-unit grants, and independent clock advances of one or two units per transition. A successor waits two grant lengths on its own elapsed clock. Old-process pause/resume and permanent loss are included; successor crashes/restarts are included before activation. Both processes can be absent. A replacement can have a different clock origin: it restarts an unfinished drain interval on its own clock. Completed drain decisions and seals are abstract atomic durable control records. The final successor does not fail after activation, and a restarted old writer would be a new incarnation outside this instance.

The positive run checks **570,280 distinct states**, 2,709,529 generated, depth 28, in three seconds of TLC execution. The standard checker completes it and seven negative controls in 15.753 seconds. The controls require stale reads for skipped drain, stopped paused clocks, expiry anchored at delayed grant delivery, and reuse of a previous process's drain timestamp; they require lost acknowledged history for permission checked only at upload start, acknowledgement across gaps, and stale negative recovery reads. The optimistic TLC fingerprint collision estimate is 6.6e-8.

`OwnedWal.checks` records exact expectations and is run in a separate cached Woodpecker stage. Locally, use the same TLC command above with `-config OwnedWal.cfg OwnedWal.tla`, then each `OwnedWalUnsafe*.cfg`. Source/configuration and checker-log hashes are in `docs/evidence/owned-wal-candidate-2026-11-09.json`.

This closes a gap between the earlier serving and log models for this bounded instance. It does **not** establish arbitrary epochs, partition placement, delegated renewal, liveness, request receipts, KV/transaction semantics, index recovery, compaction, retention, collection, or admission. Bounded-rate clocks, fresh negative object reads, immutable identities, and atomic durable control records remain assumptions. The runtime clock rule is unchanged; no replacement runtime is adopted by this result.

## DelegatedGrant

This separate candidate tests a way to avoid a conditional provider call for every partition's renewal. A coordinator holds an already acquired root grant and delegates shorter permission to owners. Each owner's interval is anchored at its own request start. If the coordinator has `R` clock units remaining and the slowest clock advances at half the fastest rate, the delegated duration is at most `floor(R / 2)`. It must not simply copy the remaining duration or start a new interval on response delivery. The old coordinator can continue issuing these bounded grants during the successor's drain; none may survive activation.

The checked instance contains one fixed root grant of length two, two independently clocked children, one request/reply per child, two real-time transitions, arbitrary pauses and permanent crashes, and a successor that fences and drains before activation. The positive run explores **1,258,368 states**, 6,754,337 generated, depth 15, in seven seconds. Five required controls fail for giving the full unscaled remaining duration, delayed-response expiry, a stopped paused coordinator clock, a stopped paused child clock, and skipped drain. The optimistic fingerprint collision estimate is 3.7e-7. Standard checking plus controls takes 17.551 seconds locally.

The model checks that a valid child grant remains within the root permission and that none is valid after successor activation. `DelegatedGrant.checks` has a separate cached CI stage. Reproduce using `-config DelegatedGrant.cfg DelegatedGrant.tla` and each `DelegatedGrantUnsafe*.cfg` as above. Hashes and limits are in `docs/evidence/delegated-grant-candidate-2026-11-09.json`.

This is the grant-containment component, **not** full hierarchical ownership. It assumes a valid root grant, atomic issuing and fence transitions, bounded-rate continuously advancing clocks, and unchanged assignment during a grant. It does not model root acquisition/renewal, repeated child renewals, reassignment, restarts, durable log composition, message/resource bounds, liveness or actual throughput. The deployment clock contract remains unresolved. No runtime clock dependency is adopted.
