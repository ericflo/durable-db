------------------------ MODULE TransactionValidation ------------------------
EXTENDS Naturals, Sequences, FiniteSets
\* One database's serial validation component, before journal/ownership
\* composition. A version identifies a mutation (put/delete/no-op), not a
\* value. Range reads include every key in the modeled interval, even absent
\* keys. `versions` is unbounded ghost history, not retained runtime storage.
\* Forget removes only committed conflict metadata. Snapshots older than the
\* floor expire instead of silently validating against incomplete history.
\* Empty and multi-key write sets exercise strict database revision conditions.
\* Input/byte/work admission may reject additional operations; this model
\* checks the transaction-count bound, not Rust heap or CPU consumption.
CONSTANTS Transactions, Keys, Capacity,
          UnsafeCommittedOnly, UnsafeExistingOnly, UnsafeExpired, UnsafeVisible, UnsafeDatabaseRevision
VARIABLES versions, accepted, durable, floor, changed, phase, snapshots,
          snapshotVersions, readKeys, rangeRead, wholeRead, serial, visible
vars == <<versions, accepted, durable, floor, changed, phase, snapshots,
          snapshotVersions, readKeys, rangeRead, wholeRead, serial, visible>>
Zero == [k \in Keys |-> 0]
Init == /\ versions = <<Zero>> /\ accepted = <<>> /\ durable = 0 /\ floor = 0
        /\ changed = Zero /\ phase = [t \in Transactions |-> "fresh"]
        /\ snapshots = [t \in Transactions |-> 0]
        /\ snapshotVersions = [t \in Transactions |-> Zero]
        /\ readKeys = [t \in Transactions |-> {}]
        /\ rangeRead = [t \in Transactions |-> FALSE]
        /\ wholeRead = [t \in Transactions |-> FALSE]
        /\ serial = TRUE /\ visible = 0
Open(t, reads, mode) ==
    /\ phase[t] = "fresh"
    /\ phase' = [phase EXCEPT ![t] = "open"]
    /\ snapshots' = [snapshots EXCEPT ![t] = durable]
    /\ snapshotVersions' = [snapshotVersions EXCEPT ![t] = versions[durable + 1]]
    /\ readKeys' = [readKeys EXCEPT ![t] = IF mode = "keys" THEN reads ELSE Keys]
    /\ rangeRead' = [rangeRead EXCEPT ![t] = (mode = "range")]
    /\ wholeRead' = [wholeRead EXCEPT ![t] = (mode = "database")]
    /\ UNCHANGED <<versions, accepted, durable, floor, changed, serial, visible>>
CheckedKeys(t) == IF UnsafeExistingOnly /\ rangeRead[t]
                 THEN {k \in readKeys[t]: snapshotVersions[t][k] > 0}
                 ELSE readKeys[t]
Latest(k) == IF UnsafeCommittedOnly THEN versions[durable + 1][k] ELSE changed[k]
Accept(t, writes) ==
    /\ phase[t] = "open" /\ Len(accepted) - floor < Capacity
    /\ (UnsafeExpired \/ snapshots[t] >= floor)
    /\ (UnsafeDatabaseRevision \/ ~wholeRead[t] \/ snapshots[t] = Len(accepted))
    /\ \A k \in CheckedKeys(t): Latest(k) <= snapshots[t]
    /\ LET revision == Len(accepted) + 1
       IN /\ accepted' = Append(accepted, t)
          /\ versions' = Append(versions, [k \in Keys |-> IF k \in writes THEN revision ELSE versions[Len(versions)][k]])
          /\ changed' = [k \in Keys |-> IF k \in writes THEN revision ELSE changed[k]]
    /\ phase' = [phase EXCEPT ![t] = "accepted"]
    /\ serial' = (serial /\ (~wholeRead[t] \/ snapshots[t] = Len(accepted))
                      /\ \A k \in readKeys[t]:
                         snapshotVersions[t][k] = versions[Len(versions)][k])
    /\ UNCHANGED <<durable, floor, snapshots, snapshotVersions, readKeys, rangeRead, wholeRead, visible>>
Publish == /\ durable < Len(accepted) /\ durable' = durable + 1
           /\ UNCHANGED <<versions, accepted, floor, changed, phase, snapshots,
                          snapshotVersions, readKeys, rangeRead, wholeRead, serial, visible>>
Forget == /\ floor < durable /\ floor' = floor + 1
          /\ changed' = [k \in Keys |-> IF changed[k] <= floor + 1 THEN 0 ELSE changed[k]]
          /\ UNCHANGED <<versions, accepted, durable, phase, snapshots,
                         snapshotVersions, readKeys, rangeRead, wholeRead, serial, visible>>
Read == /\ visible' = IF UnsafeVisible THEN Len(accepted) ELSE durable
        /\ UNCHANGED <<versions, accepted, durable, floor, changed, phase, snapshots,
                       snapshotVersions, readKeys, rangeRead, wholeRead, serial>>
Next == Publish \/ Forget \/ Read
        \/ (\E t \in Transactions: (\E reads \in SUBSET Keys, mode \in {"keys", "range", "database"}: Open(t, reads, mode))
                                   \/ (\E writes \in SUBSET Keys: Accept(t, writes)))
Spec == Init /\ [][Next]_vars
SerialValidation == serial
DurableVisibility == visible <= durable
WindowBound == Len(accepted) - floor <= Capacity
IndexMatches == \A k \in Keys:
    changed[k] = IF versions[Len(versions)][k] > floor THEN versions[Len(versions)][k] ELSE 0
NotConcurrentDisjoint == ~(Len(accepted) = 2 /\ durable = 0 /\
    \E a, b \in Transactions: a # b /\ phase[a] = "accepted" /\ phase[b] = "accepted"
      /\ readKeys[a] # {} /\ readKeys[b] # {} /\ readKeys[a] \cap readKeys[b] = {})
NotReclaimedWindow == ~(floor > 0 /\ Len(accepted) > Capacity)
=============================================================================
