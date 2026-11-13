------------------------- MODULE OwnerResidency -------------------------
EXTENDS Naturals, FiniteSets
CONSTANT Fault
ASSUME Fault \in {"None", "Acquire", "Retire"}
VARIABLE s
vars == <<s>>
Owners == {1, 2, 3}
Capacity == 2
Init == s = [live |-> {}, reserved |-> {}, retired |-> {}]
\* Reserve before acquisition/recovery, not just before entering the map.
\* Acquisitions may overlap: no live owner must finish before another starts.
\* Completed/canceled work frees its own slot; unrelated membership changes
\* need not retire an owner. A preference change can leave the incumbent live
\* until revoked; its reservation remains occupied throughout that overlap.
\* Requests may prioritize a queued partition. Acquire already chooses any
\* eligible owner; changing that order preserves this transition relation.
\* Priority is bounded advisory state, never another reservation or authority.
\* While requests await recovery, background acquisitions may use just one
\* slot, leaving room for requested retries. Restricting eligible Acquire
\* steps preserves these safety invariants; scheduling progress is tested in Rust.
\* Placement is scheduling, never serving authority.
Acquire(o) == /\ o \notin s.live
              /\ (Cardinality(s.reserved) < Capacity \/ Fault = "Acquire")
              /\ s' = [s EXCEPT !.live = @ \cup {o}, !.reserved = @ \cup {o}]
\* Routing retirement does not end requests, uploads or private worker state.
Retire(o) == /\ o \in s.live \ s.retired
             /\ s' = [s EXCEPT !.retired = @ \cup {o},
                 !.reserved = IF Fault = "Retire" THEN @ \ {o} ELSE @]
\* Finish abstracts the last reference, including canceled/finished I/O tasks.
Finish(o) == /\ o \in s.retired
             /\ s' = [s EXCEPT !.live = @ \ {o}, !.reserved = @ \ {o},
                                !.retired = @ \ {o}]
Next == \E o \in Owners: Acquire(o) \/ Retire(o) \/ Finish(o)
TypeOK == s \in [live: SUBSET Owners, reserved: SUBSET Owners, retired: SUBSET Owners]
Bounded == Cardinality(s.live) <= Capacity
Funded == s.live \subseteq s.reserved
Spec == Init /\ [][Next]_vars
=============================================================================
