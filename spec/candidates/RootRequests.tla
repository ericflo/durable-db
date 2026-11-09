--------------------------- MODULE RootRequests ---------------------------
EXTENDS Naturals, Sequences
CONSTANTS Attempts, Lease, MaxTime,
          UnsafeCompare, UnsafeTimeout, UnsafeLateExpiry, UnsafeDrain

\* Explicit root-control invocations, starting from a reachable active root.
\* This tests timeout BEFORE provider application, overlapping abandoned calls,
\* stale reads and revalidation after lost fence/activation replies. It retains
\* RootControl's ordered-epoch and abstract elapsed-time contracts. It does not
\* model competing election candidates, physical clocks or journal history.
VARIABLES history, roots, view, valid, active, requests, now,
          timedOut, applied, renewed, recovered, activationRecovered, lateRejected, lateReply
vars == <<history, roots, view, valid, active, requests, now,
          timedOut, applied, renewed, recovered, activationRecovered, lateRejected, lateReply>>
Ids == 1..Attempts
Owners == 1..2
Kinds == {"fence", "validate", "activate"}
Statuses == {"fresh", "pending", "ok", "conflict", "done"}
Metadata == [owner : Owners, revision : Nat, open : BOOLEAN]
Initial == [owner |-> 1, revision |-> 3, open |-> TRUE]
Current == history[Len(history)]
Max(a, b) == IF a >= b THEN a ELSE b
FreshRequest == [status |-> "fresh", kind |-> "validate", owner |-> 1,
    expected |-> 0, open |-> FALSE, started |-> 0, result |-> Initial]

Init ==
    /\ history = <<Initial>> /\ now = Lease
    /\ roots = [p \in Owners |->
          [phase |-> IF p = 1 THEN "active" ELSE "fresh",
           expires |-> IF p = 1 THEN 2 * Lease ELSE 0, drain |-> 0]]
    /\ view = [p \in Owners |-> Initial]
    /\ valid = [p \in Owners |-> p = 1]
    /\ active = [p \in Owners |-> 0]
    /\ requests = [a \in Ids |-> FreshRequest]
    /\ timedOut = {} /\ applied = {}
    /\ renewed = FALSE /\ recovered = FALSE /\ activationRecovered = FALSE
    /\ lateRejected = FALSE /\ lateReply = FALSE

RootValid(p) == roots[p].phase = "active" /\ now < roots[p].expires
NextId(a) == requests[a].status = "fresh"
             /\ \A b \in Ids: b < a => requests[b].status # "fresh"

\* Select any historical observation at invocation creation. Read timing has
\* no other effect: this retains stale reads and captured versions without
\* multiplying states for observational stuttering before the same request.
Sources(p, kind) == IF kind = "activate" THEN {view[p]}
                    ELSE {history[i] : i \in 1..Len(history)}

Start(p, a, kind, source) ==
    /\ active[p] = 0 /\ NextId(a)
    /\ CASE kind = "fence" ->
               roots[p].phase = "fresh" /\ p = source.owner + 1
         [] kind = "validate" -> source.owner = p
         [] OTHER -> /\ roots[p].phase = "draining" /\ valid[p]
                     /\ now < roots[p].expires
                     /\ (UnsafeDrain \/ now >= roots[p].drain + Lease)
    /\ requests' = [requests EXCEPT ![a] =
           [status |-> "pending", kind |-> kind, owner |-> p,
            expected |-> source.revision, started |-> now,
            open |-> IF kind = "fence" THEN FALSE
                     ELSE IF kind = "activate" THEN TRUE ELSE source.open,
            result |-> Initial]]
    /\ active' = [active EXCEPT ![p] = a]
    /\ valid' = [valid EXCEPT ![p] = FALSE]
    \* An existing root interval survives a renewal attempt, but is not extended
    \* by starting it. Placement invalidation is a separate controller obligation.
    /\ UNCHANGED <<history, roots, view, now, timedOut, applied,
                   renewed, recovered, activationRecovered, lateRejected, lateReply>>

Apply(a) ==
    /\ requests[a].status = "pending"
    /\ LET r == requests[a]
           success == UnsafeCompare \/ r.expected = Current.revision
           result == [owner |-> r.owner, revision |-> Current.revision + 1, open |-> r.open]
       IN /\ history' = IF success THEN Append(history, result) ELSE history
          /\ requests' = [requests EXCEPT
                ![a].status = IF success THEN "ok" ELSE "conflict",
                ![a].result = IF success THEN result ELSE Initial]
          /\ applied' = IF success THEN applied \cup {a} ELSE applied
          /\ lateRejected' = (lateRejected \/
                (a \in timedOut /\ r.owner = 1 /\ Current.owner = 2 /\ ~success))
    /\ UNCHANGED <<roots, view, valid, active, now, timedOut,
                   renewed, recovered, activationRecovered, lateReply>>

Timeout(p, a) ==
    /\ active[p] = a /\ requests[a].status \in {"pending", "ok", "conflict"}
    /\ active' = [active EXCEPT ![p] = 0]
    /\ valid' = [valid EXCEPT ![p] = FALSE]
    /\ roots' = IF UnsafeTimeout
                 THEN [roots EXCEPT ![p].expires = Max(@, requests[a].started + Lease)]
                 ELSE roots
    /\ timedOut' = timedOut \cup {a}
    /\ UNCHANGED <<history, view, requests, now, applied,
                   renewed, recovered, activationRecovered, lateRejected, lateReply>>

Deliver(a) ==
    /\ requests[a].status \in {"ok", "conflict"}
    /\ LET r == requests[a]
           p == r.owner
           success == active[p] = a /\ r.status = "ok"
           expiry == (IF UnsafeLateExpiry THEN now ELSE r.started) + Lease
           phase == IF roots[p].phase = "fresh" THEN "draining"
                    ELSE IF r.result.open THEN "active" ELSE roots[p].phase
       IN /\ IF success
             THEN /\ roots' = [roots EXCEPT ![p] =
                       [phase |-> phase,
                        expires |-> IF r.kind = "activate" THEN roots[p].expires
                                    ELSE Max(roots[p].expires, expiry),
                        drain |-> IF roots[p].phase = "fresh" THEN now ELSE roots[p].drain]]
                  /\ view' = [view EXCEPT ![p] = r.result]
                  /\ valid' = [valid EXCEPT ![p] = TRUE]
             ELSE UNCHANGED <<roots, view, valid>>
          /\ active' = [active EXCEPT ![p] = IF @ = a THEN 0 ELSE @]
          /\ renewed' = (renewed \/
                (success /\ p = 1 /\ r.kind = "validate" /\ RootValid(p)
                 /\ expiry > roots[p].expires
                 /\ \E b \in timedOut \cap applied:
                       requests[b].owner = p /\ requests[b].kind = "validate"))
          /\ recovered' = (recovered \/
                (success /\ p = 2 /\ phase = "active" /\ now < roots'[p].expires
                 /\ \E b \in timedOut \cap applied:
                       requests[b].owner = p /\ requests[b].kind = "fence"))
          /\ activationRecovered' = (activationRecovered \/
                (success /\ p = 2 /\ r.kind = "validate" /\ r.result.open
                 /\ roots[p].phase = "draining" /\ phase = "active"
                 /\ now < roots'[p].expires
                 /\ \E b \in timedOut \cap applied:
                       requests[b].owner = p /\ requests[b].kind = "activate"))
          /\ lateReply' = (lateReply \/
                (p = 1 /\ a \in timedOut /\ r.status = "ok" /\ active[p] # a /\ RootValid(2)))
    /\ requests' = [requests EXCEPT ![a].status = "done"]
    /\ UNCHANGED <<history, now, timedOut, applied, lateRejected>>

Tick ==
    /\ now < MaxTime /\ now' = now + 1
    /\ UNCHANGED <<history, roots, view, valid, active, requests,
                   timedOut, applied, renewed, recovered, activationRecovered, lateRejected, lateReply>>
Next == Tick \/ (\E a \in Ids: Apply(a) \/ Deliver(a))
        \/ (\E p \in Owners, a \in Ids:
               Timeout(p, a) \/ (\E kind \in Kinds: \E source \in Sources(p, kind):
                                     Start(p, a, kind, source)))
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ history \in Seq(Metadata) /\ Len(history) > 0 /\ now \in Nat
    /\ roots \in [Owners -> [phase : {"fresh", "draining", "active"}, expires : Nat, drain : Nat]]
    /\ view \in [Owners -> Metadata]
    /\ valid \in [Owners -> BOOLEAN] /\ active \in [Owners -> 0..Attempts]
    /\ requests \in [Ids -> [status : Statuses, kind : Kinds, owner : Owners,
                              expected : Nat, open : BOOLEAN, started : Nat, result : Metadata]]
    /\ timedOut \subseteq Ids /\ applied \subseteq Ids
    /\ renewed \in BOOLEAN /\ recovered \in BOOLEAN /\ activationRecovered \in BOOLEAN
    /\ lateRejected \in BOOLEAN /\ lateReply \in BOOLEAN
RootExclusive == \A p, q \in Owners: (RootValid(p) /\ RootValid(q)) => p = q
EpochOrder == \A i, j \in 1..Len(history): i <= j => history[i].owner <= history[j].owner
NotRenewed == ~renewed
NotRecovered == ~recovered
NotActivationRecovered == ~activationRecovered
NotLateRejected == ~lateRejected
NotLateReply == ~lateReply

\* Guided witnesses only REMOVE schedules from Spec; no new transitions.
\* All safety invariants remain enabled and the positive instance is unrestricted.
DrainTick == Tick /\ roots[2].phase = "draining"
RenewSuccessor(a) == Start(2, a, "validate", Current) /\ now = 2 * Lease
ActivateSuccessor(a) == Start(2, a, "activate", view[2])
RecoveryActions ==
    (Start(2, 1, "fence", Initial) /\ now = Lease) \/ Timeout(2, 1)
    \/ (Apply(1) /\ 1 \in timedOut) \/ Deliver(1)
    \/ Start(2, 2, "validate", Current) \/ Apply(2) \/ Deliver(2)
    \/ DrainTick \/ RenewSuccessor(3) \/ Apply(3) \/ Deliver(3)
    \/ ActivateSuccessor(4) \/ Apply(4) \/ Deliver(4)
ActivationRecoveryActions ==
    (Start(2, 1, "fence", Initial) /\ now = Lease) \/ Apply(1) \/ Deliver(1)
    \/ DrainTick \/ RenewSuccessor(2) \/ Apply(2) \/ Deliver(2)
    \/ ActivateSuccessor(3) \/ Timeout(2, 3)
    \/ (Apply(3) /\ 3 \in timedOut) \/ Deliver(3)
    \/ Start(2, 4, "validate", Current) \/ Apply(4) \/ Deliver(4)
DelayedReplyActions ==
    (Start(1, 1, "validate", Initial) /\ now = Lease) \/ Apply(1) \/ Timeout(1, 1)
    \/ (Start(2, 2, "fence", Current) /\ requests[1].status = "ok")
    \/ Apply(2) \/ Deliver(2) \/ DrainTick
    \/ RenewSuccessor(3) \/ Apply(3) \/ Deliver(3)
    \/ ActivateSuccessor(4) \/ Apply(4) \/ Deliver(4)
    \/ (Deliver(1) /\ RootValid(2))
TimeoutActions ==
    (Start(2, 1, "fence", Initial) /\ now = Lease) \/ Apply(1) \/ Deliver(1)
    \/ DrainTick \/ RenewSuccessor(2) \/ Apply(2) \/ Deliver(2)
    \/ ActivateSuccessor(3) \/ Apply(3) \/ Deliver(3)
    \/ (Start(1, 4, "validate", Initial) /\ RootValid(2))
    \/ Apply(4) \/ Timeout(1, 4) \/ Deliver(4)
=============================================================================
