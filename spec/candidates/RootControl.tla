--------------------------- MODULE RootControl ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Epochs, Lease, MaxTime, MaxRevision, MaxRenewals, MaxChildRequests,
          UnsafeLateRoot, UnsafeStaleRenewal, UnsafeSkipDrain,
          UnsafeLongChild, UnsafeLateChild
VARIABLES now, authority, roots, children
vars == <<now, authority, roots, children>>
Ids == 1..Epochs
Max(a, b) == IF a >= b THEN a ELSE b

\* This component uses an abstract elapsed-time upper bound, not a runtime
\* shared clock. A proved clock conversion must establish that every root
\* grant ends by request-start + Lease, every child stays within its parent,
\* and a completed drain waits at least Lease after observing its fence.
\* Fresh monotonically numbered epochs stand for never-reused incarnations.
Init == /\ now = 0
        /\ authority = [owner |-> 0, revision |-> 0, active |-> FALSE]
        /\ roots = [p \in Ids |->
             [phase |-> "fresh", pending |-> "none", requested |-> 0,
              expected |-> 0, expires |-> 0, drain |-> 0, renewals |-> 0, extended |-> FALSE]]
        /\ children = [p \in Ids |->
             [pending |-> "none", requested |-> 0, duration |-> 0,
              expires |-> 0, requests |-> 0, extended |-> FALSE]]
RootValid(p) == roots[p].phase = "active" /\ now < roots[p].expires
ChildValid(p) == now < children[p].expires
RootReplyEnd(p) == (IF UnsafeLateRoot THEN now ELSE roots[p].requested) + Lease
ChildReplyEnd(p) == (IF UnsafeLateChild THEN now ELSE children[p].requested)
                   + children[p].duration
Tick == /\ now' = now + 1
        /\ UNCHANGED <<authority, roots, children>>

StartFence(p) == /\ p = authority.owner + 1 /\ roots[p].phase = "fresh"
                 /\ roots[p].pending = "none"
                 /\ roots' = [roots EXCEPT ![p].pending = "fence",
                      ![p].requested = now, ![p].expected = authority.revision]
                 /\ UNCHANGED <<now, authority, children>>
ApplyFence(p) == /\ roots[p].pending = "fence"
                 /\ IF roots[p].expected = authority.revision /\ p = authority.owner + 1
                    THEN /\ authority' = [owner |-> p,
                                revision |-> authority.revision + 1, active |-> FALSE]
                         /\ roots' = [roots EXCEPT ![p].pending = "reply-fence"]
                    ELSE /\ roots' = [roots EXCEPT ![p].pending = "none"]
                         /\ UNCHANGED authority
                 /\ UNCHANGED <<now, children>>
DeliverFence(p) == /\ roots[p].pending = "reply-fence"
                   /\ roots' = [roots EXCEPT ![p].pending = "none",
                        ![p].phase = "draining", ![p].drain = now,
                        ![p].expires = RootReplyEnd(p)]
                   /\ UNCHANGED <<now, authority, children>>

\* Renewals may start after expiry, but only an exact current-owner compare
\* may grant another interval. A delayed success belongs to its request start.
StartRenew(p) == /\ roots[p].phase \in {"draining", "active"}
                 /\ roots[p].pending = "none"
                 /\ roots' = [roots EXCEPT ![p].pending = "renew",
                      ![p].requested = now, ![p].expected = authority.revision,
                      ![p].renewals = @ + 1]
                 /\ UNCHANGED <<now, authority, children>>
ApplyRenew(p) == /\ roots[p].pending = "renew"
                 /\ IF UnsafeStaleRenewal \/
                       (authority.owner = p /\ roots[p].expected = authority.revision)
                    THEN /\ authority' = [authority EXCEPT !.revision = @ + 1]
                         /\ roots' = [roots EXCEPT ![p].pending = "reply-renew"]
                    ELSE /\ roots' = [roots EXCEPT ![p].pending = "none"]
                         /\ UNCHANGED authority
                 /\ UNCHANGED <<now, children>>
DeliverRenew(p) == /\ roots[p].pending = "reply-renew"
                   /\ roots' = [roots EXCEPT ![p].pending = "none",
                        ![p].expires = Max(@, RootReplyEnd(p)),
                        ![p].extended = @ \/ (roots[p].phase = "active"
                                              /\ RootReplyEnd(p) > roots[p].expires)]
                   /\ UNCHANGED <<now, authority, children>>

StartActivate(p) == /\ roots[p].phase = "draining" /\ roots[p].pending = "none"
                    /\ now < roots[p].expires
                    /\ (UnsafeSkipDrain \/ now >= roots[p].drain + Lease)
                    /\ roots' = [roots EXCEPT ![p].pending = "activate",
                         ![p].expected = authority.revision]
                    /\ UNCHANGED <<now, authority, children>>
ApplyActivate(p) == /\ roots[p].pending = "activate"
                    /\ IF authority.owner = p /\ roots[p].expected = authority.revision
                           /\ ~authority.active
                       THEN /\ authority' = [authority EXCEPT !.revision = @ + 1, !.active = TRUE]
                            /\ roots' = [roots EXCEPT ![p].pending = "reply-activate"]
                       ELSE /\ roots' = [roots EXCEPT ![p].pending = "none"]
                            /\ UNCHANGED authority
                    /\ UNCHANGED <<now, children>>
DeliverActivate(p) == /\ roots[p].pending = "reply-activate"
                      /\ roots' = [roots EXCEPT ![p].pending = "none", ![p].phase = "active"]
                      /\ UNCHANGED <<now, authority, children>>

\* Losing a response cannot manufacture permission. The conditional mutation
\* may already have happened, even if this incarnation never acts again.
LoseReply(p) == /\ roots[p].pending \in {"reply-fence", "reply-renew", "reply-activate"}
                /\ roots' = [roots EXCEPT ![p].pending = "none"]
                /\ UNCHANGED <<now, authority, children>>

RequestChild(p) == /\ children[p].pending = "none"
                   /\ children' = [children EXCEPT ![p].pending = "request",
                        ![p].requested = now, ![p].requests = @ + 1]
                   /\ UNCHANGED <<now, authority, roots>>
IssueChild(p) == /\ RootValid(p) /\ children[p].pending = "request"
                 /\ children' = [children EXCEPT ![p].pending = "reply",
                      ![p].duration = IF UnsafeLongChild THEN 2 * Lease
                                      ELSE roots[p].expires - now]
                 /\ UNCHANGED <<now, authority, roots>>
DeliverChild(p) == /\ children[p].pending = "reply"
                   /\ children' = [children EXCEPT ![p].pending = "none",
                        ![p].expires = Max(@, ChildReplyEnd(p)),
                        ![p].extended = @ \/ (children[p].expires > 0
                                               /\ ChildReplyEnd(p) > children[p].expires)]
                   /\ UNCHANGED <<now, authority, roots>>

Next == Tick \/ (\E p \in Ids:
           StartFence(p) \/ ApplyFence(p) \/ DeliverFence(p)
           \/ StartRenew(p) \/ ApplyRenew(p) \/ DeliverRenew(p)
           \/ StartActivate(p) \/ ApplyActivate(p) \/ DeliverActivate(p)
           \/ LoseReply(p) \/ RequestChild(p) \/ IssueChild(p) \/ DeliverChild(p))
Spec == Init /\ [][Next]_vars
TypeOK == /\ now \in Nat
          /\ authority \in [owner : 0..Epochs, revision : Nat, active : BOOLEAN]
          /\ roots \in [Ids ->
               [phase : {"fresh", "draining", "active"},
                pending : {"none", "fence", "renew", "activate", "reply-fence",
                           "reply-renew", "reply-activate"}, requested : Nat,
                expected : Nat, expires : Nat, drain : Nat, renewals : Nat, extended : BOOLEAN]]
          /\ children \in [Ids ->
               [pending : {"none", "request", "reply"}, requested : Nat,
                duration : Nat, expires : Nat, requests : Nat, extended : BOOLEAN]]
RootExclusive == \A p, q \in Ids: (RootValid(p) /\ RootValid(q)) => p = q
ChildWithinRoot == \A p \in Ids: ChildValid(p) => RootValid(p)
ChildExclusive == \A p, q \in Ids: (ChildValid(p) /\ ChildValid(q)) => p = q
Invariant == TypeOK /\ RootExclusive /\ ChildWithinRoot /\ ChildExclusive

\* Strengthening for induction. A later fence bounds every older outstanding
\* permission and successful-but-undelivered renewal, for arbitrary epochs.
ClockOrder == \A p \in Ids:
    /\ roots[p].requested <= now /\ roots[p].drain <= now
    /\ roots[p].expires <= now + Lease /\ children[p].requested <= now
RootState == \A p \in Ids:
    /\ (roots[p].phase = "fresh" => roots[p].expires = 0)
    /\ (roots[p].phase # "fresh" => p <= authority.owner)
    /\ (roots[p].pending \in {"fence", "reply-fence"} => roots[p].phase = "fresh")
    /\ (roots[p].pending = "reply-fence" => p <= authority.owner)
    /\ (roots[p].pending \in {"renew", "reply-renew"} => roots[p].phase # "fresh")
    /\ (roots[p].pending \in {"activate", "reply-activate"} =>
          roots[p].phase = "draining" /\ now >= roots[p].drain + Lease)
    /\ (roots[p].phase = "active" => now >= roots[p].drain + Lease)
FenceBounds == \A p, q \in Ids:
    (p < q /\ roots[q].phase # "fresh") =>
       /\ roots[p].expires <= roots[q].drain + Lease
       /\ (roots[p].pending \in {"reply-fence", "reply-renew"} =>
             roots[p].requested <= roots[q].drain)
ChildState == \A p \in Ids:
    /\ children[p].expires <= roots[p].expires
    /\ (children[p].expires > 0 => roots[p].phase = "active")
    /\ (children[p].pending = "reply" =>
          roots[p].phase = "active" /\
          children[p].requested + children[p].duration <= roots[p].expires)
Strong == TypeOK /\ ClockOrder /\ RootState /\ FenceBounds /\ ChildState
\* Coverage control: all three incarnations must be able to serve in one run.
NotAllActivated == ~\A p \in Ids: roots[p].phase = "active"
\* Ghost coverage flags record actual deadline extensions, not just requests.
NotRenewedChild == ~\E p \in Ids: roots[p].extended /\ children[p].extended
Small == /\ now <= MaxTime /\ authority.revision <= MaxRevision
         /\ \A p \in Ids: roots[p].renewals <= MaxRenewals
                        /\ children[p].requests <= MaxChildRequests
=============================================================================
