--------------------------- MODULE TenantAdmission ---------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Tenants, Attempts, UnsafeLateCheck, UnsafeCrossScope, UnsafeCopyOmission,
          UnsafeRestore, UnsafeRefund, UnsafeOwner
\* One write grant per tenant: two admission units and two separately funded
\* maintenance units. One accepted write needs one construction unit. Each
\* retry/takeover must claim NEW durable units before I/O; finite credit does
\* not guarantee progress through unbounded storage failures. UploadCredits
\* separately checks volatile physical grants and late provider arrivals.
\* Admission uses the target tenant, including a protected creation stored
\* in the shared catalog. The catalog storage identity is never its payer.
\* Operator-funded policy mutations, byte sizing and catalog/ledger retry
\* funding remain outside this admission abstraction.
VARIABLES owner, nonce, ceiling, allocated, funded, spent, credit, granted,
          accepted, requested, issued, usable, arrived, stale
None == [origin |-> 2, version |-> 0, tenants |-> {}]
Min(a,b) == IF a < b THEN a ELSE b
Other(t) == CHOOSE u \in Tenants: u # t
vars == <<owner, nonce, ceiling, allocated, funded, spent, credit, granted,
          accepted, requested, issued, usable, arrived, stale>>
Init == /\ owner = 0 /\ nonce = 0
        /\ ceiling = [t \in Tenants |-> 4]
        /\ allocated = [t \in Tenants |-> 0] /\ funded = allocated
        /\ spent = allocated /\ credit = allocated /\ granted = {}
        /\ accepted = {} /\ requested = [a \in Attempts |-> None]
        /\ issued = [a \in Attempts |-> {}] /\ usable = {} /\ arrived = {}
        /\ stale = FALSE
Grant(t,ack) ==
    /\ t \notin granted /\ granted' = granted \cup {t}
    /\ allocated' = [allocated EXCEPT ![t] = IF UnsafeCopyOmission THEN 2 ELSE 4]
    /\ funded' = [funded EXCEPT ![t] = 2]
    /\ credit' = [credit EXCEPT ![t] = IF ack THEN 2 ELSE 0]
    /\ nonce' = nonce + 1
    /\ UNCHANGED <<owner, ceiling, spent, accepted, requested, issued, usable, arrived, stale>>
Accept(t) == LET payer == IF UnsafeCrossScope THEN Other(t) ELSE t
             IN /\ t \notin accepted /\ credit[payer] > 0
                /\ credit' = [credit EXCEPT ![payer] = @ - 1]
                /\ accepted' = accepted \cup {t}
                /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, granted,
                             requested, issued, usable, arrived, stale>>
Capture(a,o) == /\ a = 1 + Cardinality({b \in Attempts: issued[b] # {}})
                /\ issued[a] = {} /\ accepted # {} /\ o \in 0..owner
                /\ requested' = [requested EXCEPT ![a] =
                      [origin |-> o, version |-> nonce, tenants |-> accepted]]
                /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, credit,
                                granted, accepted, issued, usable, arrived, stale>>
Claim(a,ack) ==
    /\ requested[a] # None /\ issued[a] = {}
    /\ requested[a].version = nonce
    /\ (requested[a].origin = owner \/ UnsafeOwner)
    /\ LET group == requested[a].tenants
           payers == IF UnsafeCrossScope THEN {Other(t): t \in group} ELSE group
           extra(t) == IF t \in payers /\ spent[t] = funded[t] THEN 1 ELSE 0
       IN /\ \A t \in payers: allocated[t] + extra(t) <= ceiling[t]
          /\ allocated' = [t \in Tenants |-> allocated[t] + extra(t)]
          /\ funded' = [t \in Tenants |-> funded[t] + extra(t)]
          /\ spent' = [t \in Tenants |-> spent[t] + IF t \in payers THEN 1 ELSE 0]
          /\ credit' = [t \in Tenants |-> Min(credit[t], funded'[t] - spent'[t])]
          /\ issued' = [issued EXCEPT ![a] = group]
    /\ requested' = [requested EXCEPT ![a] = None]
    /\ usable' = IF ack THEN usable \cup {a} ELSE usable
    /\ stale' = (stale \/ requested[a].origin # owner)
    /\ nonce' = nonce + 1
    /\ UNCHANGED <<owner, ceiling, granted, accepted, arrived>>
Upload(a) == /\ a \notin arrived /\ (a \in usable \/ UnsafeLateCheck)
             /\ arrived' = arrived \cup {a}
             /\ UNCHANGED <<owner, nonce, ceiling, allocated, funded, spent, credit,
                             granted, accepted, requested, issued, usable, stale>>
Takeover == /\ owner = 0 /\ owner' = 1 /\ nonce' = nonce + 1
            /\ credit' = [t \in Tenants |-> IF UnsafeRestore /\ t \in granted THEN 2 ELSE 0]
            /\ spent' = IF UnsafeRefund THEN [t \in Tenants |-> 0] ELSE spent
            /\ UNCHANGED <<ceiling, allocated, funded, granted, accepted,
                            requested, issued, usable, arrived, stale>>
Refill(t) == /\ ceiling[t] = 4 /\ ceiling' = [ceiling EXCEPT ![t] = 6]
             /\ nonce' = nonce + 1
             /\ UNCHANGED <<owner, allocated, funded, spent, credit, granted,
                             accepted, requested, issued, usable, arrived, stale>>
Next == Takeover \/ (\E t \in Tenants: Accept(t) \/ Refill(t) \/ (\E ack \in BOOLEAN: Grant(t,ack)))
        \/ (\E a \in Attempts: Upload(a) \/ (\E o \in 0..1: Capture(a,o))
             \/ (\E ack \in BOOLEAN: Claim(a,ack)))
ScopeCoverage == /\ ~stale /\ arrived \subseteq usable
                 /\ \A t \in Tenants:
                     /\ spent[t] = Cardinality({a \in Attempts: t \in issued[a]})
                     /\ credit[t] + spent[t] <= funded[t]
                     /\ funded[t] + credit[t] + (IF t \in accepted THEN 1 ELSE 0) <= allocated[t]
                     /\ allocated[t] <= ceiling[t]
                     /\ t \in accepted => t \in granted
Spec == Init /\ [][Next]_vars
=============================================================================
