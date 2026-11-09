------------------------- MODULE PlacementRequests -------------------------
EXTENDS Naturals, Sequences
CONSTANTS Attempts, Generations, Lease, MaxTime,
          UnsafeCompare, UnsafeTimeout, UnsafeReply, UnsafeDrain

\* Wire-level movement by ONE continuing coordinator, on ONE partition.
\* There is no root takeover, process restart, clock conversion, journal,
\* admission or liveness claim. Lease is an abstract grant-lifetime bound.
\* The provider register performs exact comparisons even for equal payloads:
\* the portable register wrapper supplies a fresh mutation nonce each time.
\* history, timedOut, applied and the four coverage flags are ghost state.
VARIABLES history, view, observed, valid, active, requests, issued, now, drain,
          timedOut, applied, lateFreeze, lateReply, staleCompare, resumed
vars == <<history, view, observed, valid, active, requests, issued, now, drain,
          timedOut, applied, lateFreeze, lateReply, staleCompare, resumed>>
Ids == 1..Attempts
Phases == {"open", "frozen"}
Kinds == {"validate", "freeze", "finish"}
Statuses == {"fresh", "pending", "ok", "conflict", "done"}
Metadata == [revision : Nat, generation : 1..Generations, phase : Phases]
Current == history[Len(history)]
Initial == [revision |-> 0, generation |-> 1, phase |-> "open"]
FreshRequest == [status |-> "fresh", kind |-> "validate", expected |-> 0,
                generation |-> 1, phase |-> "open", stale |-> FALSE]

Init ==
    /\ history = <<Initial>> /\ view = Initial /\ observed = Initial
    /\ valid = FALSE /\ active = 0
    /\ requests = [a \in Ids |-> FreshRequest]
    /\ issued = [g \in 1..Generations |-> 0] /\ now = 0 /\ drain = 0
    /\ timedOut = {} /\ applied = {}
    /\ lateFreeze = FALSE /\ lateReply = FALSE
    /\ staleCompare = FALSE /\ resumed = FALSE

\* Never reuse invocation identities, including after timeout. Ordering IDs
\* removes symmetric choices without imposing an order on provider completion.
NextId(a) == requests[a].status = "fresh"
             /\ \A b \in Ids: b < a => requests[b].status # "fresh"

\* An ordinary read may return ANY historical version. It cannot validate
\* the issuer's view, including at startup.
Observe(r) ==
    /\ active = 0 /\ r \in 1..Len(history)
    /\ observed' = history[r]
    /\ UNCHANGED <<history, view, valid, active, requests, issued, now, drain,
                   timedOut, applied, lateFreeze, lateReply, staleCompare, resumed>>

Start(a, kind) ==
    /\ active = 0 /\ NextId(a)
    /\ IF kind = "validate" THEN TRUE
       ELSE /\ valid
            /\ IF kind = "freeze"
               THEN view.phase = "open" /\ view.generation < Generations
               ELSE view.phase = "frozen" /\ (UnsafeDrain \/ now >= drain + Lease)
    /\ LET source == IF kind = "validate" THEN observed ELSE view
       IN requests' = [requests EXCEPT ![a] =
             [status |-> "pending", kind |-> kind, expected |-> source.revision,
              generation |-> source.generation + (IF kind = "freeze" THEN 1 ELSE 0),
              stale |-> kind = "validate" /\ source.revision < Current.revision,
              phase |-> IF kind = "validate" THEN source.phase
                        ELSE IF kind = "freeze" THEN "frozen" ELSE "open"]]
    \* Stop grant issuance BEFORE sending a request that can freeze placement.
    /\ active' = a /\ valid' = FALSE
    /\ UNCHANGED <<history, view, observed, issued, now, drain,
                   timedOut, applied, lateFreeze, lateReply, staleCompare, resumed>>

Apply(a) ==
    /\ requests[a].status = "pending"
    /\ LET success == UnsafeCompare \/ requests[a].expected = Current.revision
       IN /\ history' = IF success THEN Append(history,
                [revision |-> Current.revision + 1, generation |-> requests[a].generation,
                 phase |-> requests[a].phase]) ELSE history
          /\ requests' = [requests EXCEPT ![a].status = IF success THEN "ok" ELSE "conflict"]
          /\ lateFreeze' = (lateFreeze \/
                (success /\ requests[a].kind = "freeze" /\ a \in timedOut /\ issued[1] > 0))
          /\ applied' = IF success THEN applied \cup {a} ELSE applied
          /\ staleCompare' = (staleCompare \/ (~success /\ requests[a].stale))
    /\ UNCHANGED <<view, observed, valid, active, issued, now, drain, timedOut, lateReply, resumed>>

\* Timeout abandons the caller's invocation, not the provider's operation.
\* It cannot restore permission or discard the captured conditional payload.
Timeout(a) ==
    /\ active = a /\ requests[a].status \in {"pending", "ok", "conflict"}
    /\ active' = 0 /\ valid' = UnsafeTimeout
    /\ timedOut' = timedOut \cup {a}
    /\ UNCHANGED <<history, view, observed, requests, issued, now, drain, applied,
                   lateFreeze, lateReply, staleCompare, resumed>>

Deliver(a) ==
    /\ requests[a].status \in {"ok", "conflict"}
    /\ IF (UnsafeReply \/ active = a) /\ requests[a].status = "ok"
       THEN /\ view' = [revision |-> requests[a].expected + 1,
                        generation |-> requests[a].generation, phase |-> requests[a].phase]
            \* A confirmed frozen view starts a fresh, conservative drain.
            /\ valid' = TRUE /\ drain' = now
       ELSE UNCHANGED <<view, valid, drain>>
    /\ requests' = [requests EXCEPT ![a].status = "done"]
    /\ active' = IF active = a THEN 0 ELSE active
    /\ lateReply' = (lateReply \/ (a \in timedOut /\ active # a
          /\ requests[a].status = "ok" /\ requests[a].generation < view.generation
          /\ issued[requests[a].generation] > 0))
    /\ UNCHANGED <<history, observed, issued, now, timedOut, applied, lateFreeze, staleCompare, resumed>>

\* Grants use the validated local view, with no provider call per grant.
\* This abstracts a permission interval, not a network reply or a local clock.
Issue ==
    /\ valid /\ view.phase = "open"
    /\ issued' = [issued EXCEPT ![view.generation] = now + Lease]
    /\ resumed' = (resumed \/ (view.generation > 1 /\ issued[1] > 0
          /\ \E a \in timedOut \cap applied:
              requests[a].kind = "freeze" /\ requests[a].generation = view.generation))
    /\ UNCHANGED <<history, view, observed, valid, active, requests, now, drain,
                   timedOut, applied, lateFreeze, lateReply, staleCompare>>
Tick ==
    /\ now < MaxTime /\ now' = now + 1
    /\ UNCHANGED <<history, view, observed, valid, active, requests, issued, drain,
                   timedOut, applied, lateFreeze, lateReply, staleCompare, resumed>>
Next == Tick \/ Issue \/ (\E r \in 1..Len(history): Observe(r))
        \/ (\E a \in Ids: Apply(a) \/ Timeout(a) \/ Deliver(a)
              \/ (\E kind \in Kinds: Start(a, kind)))
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ history \in Seq(Metadata) /\ Len(history) > 0
    /\ view \in Metadata /\ observed \in Metadata /\ valid \in BOOLEAN
    /\ active \in 0..Attempts
    /\ requests \in [Ids -> [status : Statuses, kind : Kinds, expected : Nat,
                             generation : 1..Generations, phase : Phases, stale : BOOLEAN]]
    /\ issued \in [1..Generations -> Nat] /\ now \in Nat /\ drain \in Nat
    /\ timedOut \subseteq Ids /\ applied \subseteq Ids
    /\ lateFreeze \in BOOLEAN /\ lateReply \in BOOLEAN
    /\ staleCompare \in BOOLEAN /\ resumed \in BOOLEAN
ViewSafe == (valid /\ view.phase = "open") =>
             (view.generation = Current.generation /\ Current.phase = "open")
Exclusive == \A g, h \in 1..Generations:
    (now < issued[g] /\ now < issued[h]) => g = h
GenerationOrder == \A i, j \in 1..Len(history):
    i <= j => history[i].generation <= history[j].generation

\* Forbidden-state checks require actual events, not just pending requests.
NotLateFreeze == ~lateFreeze
NotLateReply == ~lateReply
NotStaleCompare == ~staleCompare
NotResumed == ~resumed
=============================================================================
