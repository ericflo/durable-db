------------------------- MODULE OwnerRouting -------------------------
EXTENDS Naturals
CONSTANT Fault
ASSUME Fault \in {"None", "Identity", "Retry", "Rebind", "BusyCache", "CancelCache", "Readiness", "Idle"}
VARIABLE s
vars == <<s>>
Owners == {"old", "new", "foreign"}
Init == s = [route |-> "old", endpoint |-> "old", permit |-> TRUE,
             cached |-> "none", expected |-> "none", phase |-> "idle",
             attempts |-> 0, served |-> "none", pinned |-> "none",
             ready |-> FALSE, authorized |-> FALSE, absent |-> FALSE, retired |-> FALSE]
\* HTTP readiness keeps routing and administration reachable during recovery.
\* It is independent of response authority, including for direct peer requests.
\* Local worker health can remain ready during provider errors or maintenance;
\* neither condition authorizes a response without the fresh permit check.
\* A false peer-failure suspicion may trigger takeover while the old process
\* lives. Takeover still revokes its permit; a health observation cannot serve.
Readiness == s' = [s EXCEPT !.ready = ~@]
\* A completed background permit observation can retire an idle local owner,
\* never authorize a response. Errors are stuttering; eventual observation and
\* scheduling are required for liveness and are not proved by this safety check.
Probe == s' = [s EXCEPT !.absent = ~s.permit,
                 !.retired = @ \/ (~s.permit /\ Fault # "Idle")]
IdleRetires == s.absent => s.retired
\* Routing observations are hints, including stale ones after takeover.
Lookup == /\ s.cached = "none" /\ s.phase \notin {"busy", "cancelled"}
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
             /\ (target # "old" \/ s.permit \/ (Fault = "Readiness" /\ s.ready))
             /\ s' = [s EXCEPT !.phase = "done", !.served = target,
                               !.authorized = target # "old" \/ s.permit]
LoseReply == /\ s.phase = "sent"
             /\ s' = [s EXCEPT !.phase = IF Fault = "Retry" THEN "idle" ELSE "done"]
\* Admission may reject before the destination checks its incarnation.
Busy == /\ s.phase = "sent" /\ s.pinned = "none"
        /\ s' = [s EXCEPT !.phase = "busy",
                   !.cached = IF Fault = "BusyCache" THEN @ ELSE "none"]
BusyInvalidates == s.phase = "busy" => s.cached = "none"
\* The caller can cancel before the peer deadline, without learning whether
\* its request applied. Retire that route hint; never replay the request here.
Cancel == /\ s.phase = "sent"
          /\ s' = [s EXCEPT !.phase = "cancelled",
                     !.cached = IF Fault = "CancelCache" THEN @ ELSE "none"]
CancelInvalidates == s.phase = "cancelled" => s.cached = "none"
Next == Probe \/ Readiness \/ Busy \/ Cancel \/ Lookup \/ Takeover \/ Reuse \/ Move \/ Send \/ Select \/ Serve \/ LoseReply
TypeOK == s \in [route: {"old", "new"}, endpoint: Owners,
    permit: BOOLEAN, cached: Owners \cup {"none"}, expected: Owners \cup {"none"},
    phase: {"idle", "sent", "done", "busy", "cancelled"}, attempts: 0..2, served: Owners \cup {"none"}, pinned: Owners \cup {"none"},
    ready: BOOLEAN, authorized: BOOLEAN, absent: BOOLEAN, retired: BOOLEAN]
ExpectedOwner == s.served # "none" => s.served = s.expected
AuthorizedResponse == s.served # "none" => s.authorized
SingleAttempt == s.attempts <= 1
\* Snapshot permit validity at service; later revocation may follow a response.
\* The stronger read/ack ordering argument remains in RevocableOwner.
Spec == Init /\ [][Next]_vars
=======================================================================
