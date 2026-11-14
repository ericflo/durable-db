--------------------------- MODULE TenantAdmission ---------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Tenants, Attempts, UnsafeLateCheck, UnsafeCrossScope, UnsafeCopyOmission,
          UnsafeRestore, UnsafeRefund, UnsafeOwner, UnsafeReuseCopies
\* One grant funds four admission and four maintenance units. At most two
\* writes per tenant pay one direct unit each, plus one copy unit for each
\* distinct checkpoint base they dirty. Repeated writes can reuse that copy
\* prepayment only within the same tenant and base. Construction still claims
\* its FULL two-unit allowance; the discount never refunds durable funding. Each
\* retry/takeover must claim NEW durable units before I/O; finite credit does
\* not guarantee progress through unbounded storage failures. UploadCredits
\* separately checks volatile physical grants and late provider arrivals.
\* Admission uses the target tenant, including a protected creation stored
\* in the shared catalog. The catalog storage identity is never its payer.
\* Operator-funded policy mutations, byte sizing and catalog/ledger retry
\* funding remain outside this admission abstraction.
VARIABLES owner, nonce, ceiling, allocated, funded, spent, credit, granted,
          accepted, requested, issued, usable, arrived, stale, writes, paid, covered, base, reusable
None == [origin |-> 2, version |-> 0, tenants |-> {}]
Min(a,b) == IF a < b THEN a ELSE b
Other(t) == CHOOSE u \in Tenants: u # t
vars == <<owner, nonce, ceiling, allocated, funded, spent, credit, granted,
          accepted, requested, issued, usable, arrived, stale, writes, paid, covered, base, reusable>>
Init == /\ owner = 0 /\ nonce = 0
        /\ ceiling = [t \in Tenants |-> 8]
        /\ allocated = [t \in Tenants |-> 0] /\ funded = allocated
        /\ spent = allocated /\ credit = allocated /\ granted = {}
        /\ accepted = {} /\ requested = [a \in Attempts |-> None]
        /\ issued = [a \in Attempts |-> {}] /\ usable = {} /\ arrived = {}
        /\ stale = FALSE /\ writes = allocated /\ paid = allocated
        /\ covered = {} /\ base = 0 /\ reusable = TRUE
Grant(t,ack) ==
    /\ t \notin granted /\ granted' = granted \cup {t}
    /\ allocated' = [allocated EXCEPT ![t] = IF UnsafeCopyOmission THEN 4 ELSE 8]
    /\ funded' = [funded EXCEPT ![t] = 4]
    /\ credit' = [credit EXCEPT ![t] = IF ack THEN 4 ELSE 0]
    /\ nonce' = nonce + 1
    /\ UNCHANGED <<owner, ceiling, spent, accepted, requested, issued, usable, arrived, stale, writes, paid, covered, base, reusable>>
Accept(t) == LET payer == IF UnsafeCrossScope THEN Other(t) ELSE t
                 reused == IF UnsafeReuseCopies
                           THEN \E b \in 0..1: <<t,b>> \in covered
                           ELSE <<t,base>> \in covered
                 cost == 1 + IF reused /\ reusable THEN 0 ELSE 1
             IN /\ writes[t] < 2 /\ credit[payer] >= cost
                /\ credit' = [credit EXCEPT ![payer] = @ - cost]
                /\ paid' = [paid EXCEPT ![payer] = @ + cost]
                /\ writes' = [writes EXCEPT ![t] = @ + 1]
                /\ covered' = covered \cup {<<t,base>>}
                /\ accepted' = accepted \cup {t}
                /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, granted,
                             requested, issued, usable, arrived, stale, base, reusable>>
NewBase == /\ base = 0 /\ arrived # {} /\ base' = 1 /\ reusable' = TRUE
           /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, credit,
                           granted, accepted, requested, issued, usable, arrived, stale,
                           writes, paid, covered>>
Capture(a,o) == /\ a = 1 + Cardinality({b \in Attempts: issued[b] # {}})
                /\ issued[a] = {} /\ accepted # {} /\ o \in 0..owner
                /\ requested' = [requested EXCEPT ![a] =
                      [origin |-> o, version |-> nonce, tenants |-> accepted]]
                /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, credit,
                                granted, accepted, issued, usable, arrived, stale, writes, paid, covered, base, reusable>>
Claim(a,ack) ==
    /\ requested[a] # None /\ issued[a] = {}
    /\ requested[a].version = nonce
    /\ (requested[a].origin = owner \/ UnsafeOwner)
    /\ LET group == requested[a].tenants
           payers == IF UnsafeCrossScope THEN {Other(t): t \in group} ELSE group
           extra(t) == IF t \in payers /\ spent[t] + 2 > funded[t] THEN spent[t] + 2 - funded[t] ELSE 0
       IN /\ \A t \in payers: allocated[t] + extra(t) <= ceiling[t]
          /\ allocated' = [t \in Tenants |-> allocated[t] + extra(t)]
          /\ funded' = [t \in Tenants |-> funded[t] + extra(t)]
          /\ spent' = [t \in Tenants |-> spent[t] + IF t \in payers THEN 2 ELSE 0]
          /\ credit' = [t \in Tenants |-> Min(credit[t], funded'[t] - spent'[t])]
          /\ issued' = [issued EXCEPT ![a] = group]
    /\ requested' = [requested EXCEPT ![a] = None]
    /\ reusable' = FALSE
    /\ usable' = IF ack THEN usable \cup {a} ELSE usable
    /\ stale' = (stale \/ requested[a].origin # owner)
    /\ nonce' = nonce + 1
    /\ UNCHANGED <<owner, ceiling, granted, accepted, arrived, writes, paid, covered, base>>
Upload(a) == /\ a \notin arrived /\ (a \in usable \/ UnsafeLateCheck)
             /\ arrived' = arrived \cup {a}
             /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, credit,
                             granted, accepted, requested, issued, usable, stale, writes, paid, covered, base, reusable>>
Takeover == /\ reusable' = FALSE /\ owner = 0 /\ owner' = 1 /\ nonce' = nonce + 1
            /\ credit' = [t \in Tenants |-> IF UnsafeRestore /\ t \in granted THEN 4 ELSE 0]
            /\ spent' = IF UnsafeRefund THEN [t \in Tenants |-> 0] ELSE spent
            /\ UNCHANGED <<ceiling, allocated, funded, granted, accepted,
                            requested, issued, usable, arrived, stale, writes, paid, covered, base>>
Refill(t) == /\ ceiling[t] = 8 /\ ceiling' = [ceiling EXCEPT ![t] = 10]
             /\ nonce' = nonce + 1
             /\ UNCHANGED <<owner, allocated, funded, spent, credit, granted,
                             accepted, requested, issued, usable, arrived, stale, writes, paid, covered, base, reusable>>
Next == NewBase \/ Takeover \/ (\E t \in Tenants: Accept(t) \/ Refill(t) \/ (\E ack \in BOOLEAN: Grant(t,ack)))
        \/ (\E a \in Attempts: Upload(a) \/ (\E o \in 0..1: Capture(a,o))
             \/ (\E ack \in BOOLEAN: Claim(a,ack)))
ScopeCoverage == /\ ~stale /\ arrived \subseteq usable
                 /\ \A t \in Tenants:
                     /\ spent[t] = 2 * Cardinality({a \in Attempts: t \in issued[a]})
                     /\ credit[t] + spent[t] <= funded[t]
                     /\ funded[t] + credit[t] + paid[t] <= allocated[t]
                     /\ paid[t] >= writes[t] + Cardinality({b \in 0..1: <<t,b>> \in covered})
                     /\ allocated[t] <= ceiling[t]
                     /\ t \in accepted => t \in granted
NoReuse == \A t \in Tenants: writes[t] <= Cardinality({b \in 0..1: <<t,b>> \in covered})
Spec == Init /\ [][Next]_vars
=============================================================================
