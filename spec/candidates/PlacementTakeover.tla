------------------------- MODULE PlacementTakeover -------------------------
EXTENDS RootControl, Sequences
CONSTANTS Attempts, Generations, UnsafeCompare, UnsafeTimeout, UnsafeReply, UnsafeDrain

\* Placement request/reply composition with the actual RootControl actions.
\* ONE partition, an already-active initial root and ONE successor. Root
\* permission uses RootControl's abstract elapsed-time contract. No journal,
\* physical clock, arbitrary election, wire refinement theorem or liveness.
\* history, timedOut, applied and recovered are ghost state.
VARIABLES placement, history, view, observed, valid, active, requests, issued,
          localDrain, timedOut, applied, recovered
wireVars == <<placement, history, view, observed, valid, active, requests, issued,
              localDrain, timedOut, applied, recovered>>
allVars == <<vars, wireVars>>
RequestIds == 1..Attempts
Snapshot == [revision |-> authority.revision, owner |-> authority.owner,
             active |-> authority.active, generation |-> placement.generation,
             phase |-> placement.phase, mover |-> placement.mover]
Fresh == [status |-> "fresh", root |-> 1, kind |-> "validate", expected |-> 0,
          generation |-> 1, phase |-> "open", mover |-> 0]
\* Begin with the first coordinator already active. Its initial grant lasts
\* one Lease; the RootControl replacement/renewal actions run unchanged.
\* This seed follows fence at time zero, drain, renewal at time Lease and
\* activation: revisions one, two and three. Strong is checked from the seed.
\* The finite instance permits no further initial-root renewal; the successor
\* must execute its own fence, drain, renewal and activation before issuance.
WireInit ==
    /\ now = Lease
    /\ authority = [owner |-> 1, revision |-> 3, active |-> TRUE]
    /\ roots = [p \in Ids |-> [phase |-> IF p = 1 THEN "active" ELSE "fresh",
         pending |-> "none", requested |-> IF p = 1 THEN Lease ELSE 0,
         expected |-> IF p = 1 THEN 2 ELSE 0,
         expires |-> IF p = 1 THEN 2 * Lease ELSE 0, drain |-> 0,
         renewals |-> IF p = 1 THEN 1 ELSE 0, extended |-> FALSE]]
    /\ children = [p \in Ids |-> [pending |-> "none", requested |-> 0,
         duration |-> 0, expires |-> 0, requests |-> 0, extended |-> FALSE]]
    /\ placement = [generation |-> 1, phase |-> "open", mover |-> 0]
    /\ history = <<Snapshot>>
    /\ view = [p \in Ids |-> Snapshot] /\ observed = [p \in Ids |-> Snapshot]
    /\ valid = [p \in Ids |-> FALSE] /\ active = [p \in Ids |-> 0]
    /\ requests = [a \in RequestIds |-> Fresh]
    /\ issued = [p \in Ids |-> [g \in 1..Generations |-> 0]]
    /\ localDrain = [p \in Ids |-> 0]
    /\ timedOut = {} /\ applied = {} /\ recovered = FALSE
ControlStep ==
    /\ Next
    /\ history' = IF authority'.revision # authority.revision
                    THEN Append(history, [Snapshot EXCEPT !.revision = authority'.revision,
                         !.owner = authority'.owner, !.active = authority'.active])
                    ELSE history
    /\ UNCHANGED <<placement, view, observed, valid, active, requests, issued,
                    localDrain, timedOut, applied, recovered>>
NextId(a) == requests[a].status = "fresh"
            /\ \A b \in RequestIds: b < a => requests[b].status # "fresh"
\* Foreign/inactive observations are rejected without changing local state.
\* Their wire deliveries are stutters; all historical usable observations
\* remain possible, including versions preceding a freeze or renewal.
Observe(p, r) ==
    /\ roots[p].phase = "active" /\ active[p] = 0 /\ r \in 1..Len(history)
    /\ history[r].owner = p /\ history[r].active
    /\ observed' = [observed EXCEPT ![p] = history[r]]
    /\ UNCHANGED <<vars, placement, history, view, valid, active, requests, issued,
                   localDrain, timedOut, applied, recovered>>
Start(p, a, kind) ==
    /\ roots[p].phase = "active" /\ active[p] = 0 /\ NextId(a)
    /\ IF kind = "validate" THEN observed[p].owner = p /\ observed[p].active
       ELSE /\ valid[p]
            /\ IF kind = "freeze"
               THEN view[p].phase = "open" /\ view[p].generation < Generations
               ELSE view[p].phase = "frozen" /\
                    (UnsafeDrain \/ p > view[p].mover \/ now >= localDrain[p] + Lease)
    /\ LET source == IF kind = "validate" THEN observed[p] ELSE view[p]
       IN requests' = [requests EXCEPT ![a] =
             [status |-> "pending", root |-> p, kind |-> kind, expected |-> source.revision,
              generation |-> source.generation + (IF kind = "freeze" THEN 1 ELSE 0),
              mover |-> IF kind = "freeze" THEN p ELSE source.mover,
              phase |-> IF kind = "validate" THEN source.phase
                        ELSE IF kind = "freeze" THEN "frozen" ELSE "open"]]
    /\ active' = [active EXCEPT ![p] = a] /\ valid' = [valid EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<vars, placement, history, view, observed, issued, localDrain,
                   timedOut, applied, recovered>>
\* Every root or placement mutation shares the exact authority revision.
\* Expiry does not cancel an issued operation; an old coordinator's request
\* cannot apply after another coordinator's fence changes the register.
Apply(a) ==
    /\ requests[a].status = "pending"
    /\ LET success == UnsafeCompare \/ (requests[a].expected = authority.revision
                  /\ requests[a].root = authority.owner /\ authority.active)
       IN /\ authority' = IF success THEN [authority EXCEPT !.revision = @ + 1] ELSE authority
          /\ placement' = IF success THEN
                [generation |-> requests[a].generation, phase |-> requests[a].phase,
                 mover |-> requests[a].mover] ELSE placement
          /\ history' = IF success THEN Append(history,
                [revision |-> authority'.revision, owner |-> authority.owner,
                 active |-> authority.active, generation |-> placement'.generation,
                 phase |-> placement'.phase, mover |-> placement'.mover]) ELSE history
          /\ requests' = [requests EXCEPT ![a].status = IF success THEN "ok" ELSE "conflict"]
          /\ applied' = IF success THEN applied \cup {a} ELSE applied
    /\ UNCHANGED <<now, roots, children, view, observed, valid, active, issued,
                   localDrain, timedOut, recovered>>
Timeout(p, a) ==
    /\ active[p] = a /\ requests[a].status \in {"pending", "ok", "conflict"}
    /\ active' = [active EXCEPT ![p] = 0] /\ valid' = [valid EXCEPT ![p] = UnsafeTimeout]
    /\ timedOut' = timedOut \cup {a}
    /\ UNCHANGED <<vars, placement, history, view, observed, requests, issued,
                   localDrain, applied, recovered>>
Deliver(a) ==
    /\ requests[a].status \in {"ok", "conflict"}
    /\ LET p == requests[a].root IN
       /\ IF (UnsafeReply \/ active[p] = a) /\ requests[a].status = "ok"
          THEN /\ view' = [view EXCEPT ![p] = [revision |-> requests[a].expected + 1,
                       owner |-> p, active |-> TRUE, generation |-> requests[a].generation,
                       phase |-> requests[a].phase, mover |-> requests[a].mover]]
               /\ valid' = [valid EXCEPT ![p] = TRUE]
               /\ localDrain' = [localDrain EXCEPT ![p] = now]
          ELSE UNCHANGED <<view, valid, localDrain>>
       /\ active' = [active EXCEPT ![p] = IF @ = a THEN 0 ELSE @]
    /\ requests' = [requests EXCEPT ![a].status = "done"]
    /\ UNCHANGED <<vars, placement, history, observed, issued, timedOut, applied, recovered>>
\* The local view alone cannot issue permission after root replacement.
\* The root interval bounds every issued interval, including old replies.
Issue(p) ==
    /\ RootValid(p) /\ valid[p] /\ view[p].phase = "open"
    /\ issued' = [issued EXCEPT ![p][view[p].generation] = roots[p].expires]
    /\ recovered' = (recovered \/ (p > 1 /\ view[p].generation > 1 /\ issued[1][1] > 0
          /\ \E a \in timedOut \cap applied:
              requests[a].root = 1 /\ requests[a].kind = "freeze"
              /\ requests[a].generation = view[p].generation))
    /\ UNCHANGED <<vars, placement, history, view, observed, valid, active, requests,
                   localDrain, timedOut, applied>>
WireNext == ControlStep \/ (\E a \in RequestIds: Apply(a) \/ Deliver(a))
    \/ (\E p \in Ids: Issue(p) \/ (\E r \in 1..Len(history): Observe(p, r))
         \/ (\E a \in RequestIds: Timeout(p, a)
              \/ (\E kind \in {"validate", "freeze", "finish"}: Start(p, a, kind))))
WireSpec == WireInit /\ [][WireNext]_allVars
Metadata == [revision : Nat, owner : 0..Epochs, active : BOOLEAN,
             generation : 1..Generations, phase : {"open", "frozen"}, mover : 0..Epochs]
WireTypeOK ==
    /\ placement \in [generation : 1..Generations, phase : {"open", "frozen"}, mover : 0..Epochs]
    /\ history \in Seq(Metadata)
    /\ view \in [Ids -> Metadata] /\ observed \in [Ids -> Metadata]
    /\ requests \in [RequestIds ->
         [status : {"fresh", "pending", "ok", "conflict", "done"}, root : Ids,
          kind : {"validate", "freeze", "finish"}, expected : Nat,
          generation : 1..Generations, phase : {"open", "frozen"}, mover : 0..Epochs]]
    /\ Len(history) > 0
    /\ valid \in [Ids -> BOOLEAN] /\ active \in [Ids -> 0..Attempts]
    /\ issued \in [Ids -> [1..Generations -> Nat]] /\ localDrain \in [Ids -> Nat]
    /\ timedOut \subseteq RequestIds /\ applied \subseteq RequestIds /\ recovered \in BOOLEAN
ViewSafe == \A p \in Ids:
    (RootValid(p) /\ valid[p] /\ view[p].phase = "open") =>
        (view[p].generation = placement.generation /\ placement.phase = "open")
WithinRoot == \A p \in Ids, g \in 1..Generations: now < issued[p][g] => RootValid(p)
Exclusive == \A p, q \in Ids, g, h \in 1..Generations:
    (now < issued[p][g] /\ now < issued[q][h]) => p = q /\ g = h
GenerationOrder == \A i,j \in 1..Len(history):
    i <= j => history[i].generation <= history[j].generation
\* A guided recovery witness avoids repeating almost the entire positive
\* search merely to reach one required event. This ACTION_CONSTRAINT only
\* removes schedules from WireSpec; it cannot introduce new transitions.
\* The unrestricted positive configuration still explores all schedules.
RecoveryActions ==
    Start(1, 1, "validate") \/ Apply(1) \/ Deliver(1) \/ Issue(1)
    \/ Start(1, 2, "freeze") \/ Timeout(1, 2) \/ Apply(2)
    \/ (2 \in timedOut \cap applied /\ StartFence(2))
    \/ ApplyFence(2) \/ DeliverFence(2) \/ Tick
    \/ StartRenew(2) \/ ApplyRenew(2) \/ DeliverRenew(2)
    \/ StartActivate(2) \/ ApplyActivate(2) \/ DeliverActivate(2)
    \/ Observe(2, Len(history))
    \/ Start(2, 3, "validate") \/ Apply(3) \/ Deliver(3)
    \/ Start(2, 4, "finish") \/ Apply(4) \/ Deliver(4) \/ Issue(2)
NotRecovered == ~recovered
WireSmall == Small /\ roots[1].renewals = 1
=============================================================================
