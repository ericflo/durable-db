------------------------- MODULE OwnerRouting -------------------------
EXTENDS Naturals
CONSTANT Fault
ASSUME Fault \in {"None", "Identity", "Retry", "Rebind", "BusyCache", "CancelCache", "Readiness", "Idle", "Placement", "Unavailable"}
VARIABLE s
vars == <<s>>
Owners == {"old", "new", "foreign"}
Init == s = [route |-> "old", endpoint |-> "old", permit |-> FALSE, activated |-> FALSE,
             cached |-> "none", expected |-> "none", phase |-> "idle",
             attempts |-> 0, served |-> "none", pinned |-> "none",
             ready |-> FALSE, authorized |-> FALSE, absent |-> FALSE, retired |-> FALSE,
             preferred |-> TRUE]
\* Start after a confirmed claim, before activation/recovery completes. A healthy
\* process stays in membership while preference changes. Preserve that acquisition
\* as well as a serving incumbent until takeover; preference grants no authority.
\* Process removal/failure is outside this availability subcase.
Placement == s' = [s EXCEPT !.preferred = ~@,
                    !.retired = @ \/ Fault = "Placement"]
IncumbentAvailable == s.route = "old" => ~s.retired
Activate == /\ s.route = "old" /\ ~s.retired /\ ~s.activated
            /\ s' = [s EXCEPT !.permit = TRUE, !.activated = TRUE]
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
Probe == /\ s.activated
         /\ s' = [s EXCEPT !.absent = ~s.permit,
                 !.retired = @ \/ (~s.permit /\ Fault # "Idle")]
\* A failed fresh validation releases only a retryable transport error, never
\* a state-derived reply. It cannot infer revocation or discard a healthy owner.
ValidationFailure == /\ s.activated /\ s.phase = "sent" /\ s.pinned = "old"
                     /\ s' = [s EXCEPT !.phase = "done",
                               !.retired = @ \/ Fault = "Unavailable"]
IdleRetires == s.absent => s.retired
\* Routing observations are hints, including stale ones after takeover.
\* A cluster forwarder refuses its own advertised address before Send while
\* no local backend is installed. This transport refusal grants no authority;
\* omitted work is stuttering here. Socket identity and admission-slot usage
\* remain implementation obligations checked by the paused-recovery TCP test.
\* After an HTTP/1 unavailable response, retiring the client socket lets an
\* explicit retry reach another ingress through a connection-based Service.
\* This transport step stutters: it neither replays the operation nor revokes
\* an owner. Socket reuse and HTTP-version handling are Rust test obligations.
\* Expiring unacknowledged peer TCP traffic is another transport failure, not
\* evidence of revocation or non-commit. It takes LoseReply, never replays, and
\* leaves acknowledged requests their existing response deadline. Kernel TCP
\* timing and retained-connection packet loss are implementation obligations.
Lookup == /\ s.cached = "none" /\ s.phase \notin {"busy", "cancelled"}
          /\ s' = [s EXCEPT !.cached = s.route]
\* Excluding a peer may discard its cached route before the next attempt.
\* A fresh lookup may still name that peer; membership is neither a response
\* authority nor proof that a sent mutation aborted. In-flight attempts stay
\* pinned. Rust checks the membership trigger and the healthy-cache fast path.
\* A witnessed connect failure may defer another send to that exact route for
\* a short local interval. Each explicit retry still looks up the owner; a
\* changed incarnation/address bypasses the pause. The interval grants no
\* permission, does not infer an abort and cannot replay a mutation. Deferring
\* before Send stutters here; pacing, expiry and cache-instance isolation are
\* implementation obligations checked by the TCP regressions.
Forget == /\ s.phase = "idle" /\ s.cached # "none"
          /\ s' = [s EXCEPT !.cached = "none"]
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
Next == Activate \/ Placement \/ Probe \/ ValidationFailure \/ Readiness \/ Busy \/ Cancel \/ Lookup \/ Forget \/ Takeover \/ Reuse \/ Move \/ Send \/ Select \/ Serve \/ LoseReply
TypeOK == s \in [route: {"old", "new"}, endpoint: Owners,
    activated: BOOLEAN, permit: BOOLEAN, cached: Owners \cup {"none"}, expected: Owners \cup {"none"},
    phase: {"idle", "sent", "done", "busy", "cancelled"}, attempts: 0..2, served: Owners \cup {"none"}, pinned: Owners \cup {"none"},
    ready: BOOLEAN, authorized: BOOLEAN, absent: BOOLEAN, retired: BOOLEAN, preferred: BOOLEAN]
ExpectedOwner == s.served # "none" => s.served = s.expected
AuthorizedResponse == s.served # "none" => s.authorized
SingleAttempt == s.attempts <= 1
\* Snapshot permit validity at service; later revocation may follow a response.
\* The stronger read/ack ordering argument remains in RevocableOwner.
Spec == Init /\ [][Next]_vars
=======================================================================
