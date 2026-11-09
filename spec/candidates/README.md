# Architecture experiments

These models explore possible replacements for the current serving protocol. They are not the implemented protocol, are not included in the eighteen implementation-oriented CI models, and do not authorize changing runtime assumptions.

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


## WalHandoff

The candidate acknowledges only a contiguous durable prefix of an old epoch. After an externally established end to old acknowledgement permission, recovery probes deterministic sequence positions one at a time. Uploads may complete before, during or after this scan. The successor durably seals the chosen prefix before activation and retains that decision across controller crashes/restarts.

The three-position instance explores **1,214 distinct states**, 3,720 generated, depth 21. All five unsafe controls produce their expected invariant violation: acknowledging across a missing position, scanning before the old permission has ended, trusting a stale negative read, acknowledging after handoff, or reopening a durable seal when a late upload appears. `WalHandoff.checks` records the exact required failures. Woodpecker copies this candidate into an isolated checker stage and runs the existing pinned `check-model.sh`; it remains separate from models of implemented runtime behavior.

For a local run, prepare TLC as above, then use `-config WalHandoff.cfg WalHandoff.tla` in this directory. Repeat with the five unsafe configurations listed in the descriptor; each must exit 12 and identify the stated invariant. Source/log hashes are in `docs/evidence/wal-handoff-candidate-2026-11-09.json`.

This model **assumes** the `Drain` boundary prevents old acknowledgements and that negative point reads are fresh. It does not implement that boundary or compose the clock/serving model, prove a provider consistency contract, implement a database, or model transactions, request deduplication, compaction, collection or admission. Old-writer process loss and successor-controller restart are included; general membership and a new epoch's transaction execution are not. See `docs/design-owned-partitions.md` for the proposed protocol and remaining gates.
