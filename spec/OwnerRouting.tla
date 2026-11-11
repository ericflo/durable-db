------------------------- MODULE OwnerRouting -------------------------
EXTENDS Naturals
CONSTANT Fault
ASSUME Fault \in {"None", "Identity", "Retry", "Rebind"}
VARIABLE s
vars == <<s>>
Owners == {"old", "new", "foreign"}
Init == s = [route |-> "old", endpoint |-> "old", permit |-> TRUE,
             cached |-> "none", expected |-> "none", phase |-> "idle",
             attempts |-> 0, served |-> "none", pinned |-> "none"]
\* Routing observations are hints, including stale ones after takeover.
Lookup == /\ s.cached = "none"
          /\ s' = [s EXCEPT !.cached = s.route]
Takeover == /\ s.route = "old"
            /\ s' = [s EXCEPT !.route = "new", !.permit = FALSE]
\* An address may be reused by a completely different partition/process.
Reuse == /\ s.endpoint = "old"
         /\ s' = [s EXCEPT !.endpoint = "foreign"]
Move == /\ s.route = "new" /\ s.endpoint = "old"
        /\ s' = [s EXCEPT !.endpoint = "new"]
Send == /\ s.phase = "idle" /\ s.cached # "none"
        /\ s' = [s EXCEPT !.expected = s.cached,
                          !.phase = "sent", !.attempts = @ + 1]
Select == /\ s.phase = "sent" /\ s.pinned = "none"
          /\ (Fault = "Identity" \/ s.expected = s.endpoint)
          /\ s' = [s EXCEPT !.pinned = s.endpoint]
Serve == /\ s.phase = "sent" /\ s.pinned # "none"
         /\ LET target == IF Fault = "Rebind" THEN s.endpoint ELSE s.pinned IN
             /\ (target # "old" \/ s.permit)
             /\ s' = [s EXCEPT !.phase = "done", !.served = target]
LoseReply == /\ s.phase = "sent"
             /\ s' = [s EXCEPT !.phase = IF Fault = "Retry" THEN "idle" ELSE "done"]
Next == Lookup \/ Takeover \/ Reuse \/ Move \/ Send \/ Select \/ Serve \/ LoseReply
TypeOK == s \in [route: {"old", "new"}, endpoint: Owners,
    permit: BOOLEAN, cached: Owners \cup {"none"}, expected: Owners \cup {"none"},
    phase: {"idle", "sent", "done"}, attempts: 0..2, served: Owners \cup {"none"}, pinned: Owners \cup {"none"}]
ExpectedOwner == s.served # "none" => s.served = s.expected
SingleAttempt == s.attempts <= 1
\* Snapshot permit validity at service; later revocation may follow a response.
\* The stronger read/ack ordering argument remains in RevocableOwner.
Spec == Init /\ [][Next]_vars
=======================================================================
