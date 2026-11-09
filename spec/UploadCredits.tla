---------------------------- MODULE UploadCredits ----------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Nodes, Grants, Operations, Capacity, MaxAttempts,
          UnsafeRefund, UnsafeRestore, UnsafeBeforeCharge
VARIABLES running, allocated, issued, owner, amount, current, remaining,
          charged, delivered, refunded
vars == <<running, allocated, issued, owner, amount, current, remaining,
          charged, delivered, refunded>>
None == "none"
ChargedUnits == Cardinality(charged) * MaxAttempts
DeliveredUnits == Cardinality({p \in Operations \X (1..MaxAttempts): p[2] <= delivered[p[1]]})

Init == /\ running = Nodes /\ allocated = 0 /\ issued = {}
        /\ owner = [g \in Grants |-> None] /\ amount = [g \in Grants |-> 0]
        /\ current = [n \in Nodes |-> None] /\ remaining = [n \in Nodes |-> 0]
        /\ charged = {} /\ delivered = [o \in Operations |-> 0] /\ refunded = {}

\* The authority spends the entire grant before any local credit is usable.
\* A lost acknowledgement can strand the grant; it cannot create more credit.
Reserve(n,g,units,ack) ==
    /\ n \in running /\ remaining[n] = 0 /\ g \notin issued
    /\ units \in 1..Capacity /\ allocated + units <= Capacity
    /\ allocated' = allocated + units /\ issued' = issued \cup {g}
    /\ owner' = [owner EXCEPT ![g] = n] /\ amount' = [amount EXCEPT ![g] = units]
    /\ current' = [current EXCEPT ![n] = IF ack THEN g ELSE None]
    /\ remaining' = [remaining EXCEPT ![n] = IF ack THEN units ELSE 0]
    /\ UNCHANGED <<running, charged, delivered, refunded>>
Charge(n,o) ==
    /\ n \in running /\ o \notin charged /\ remaining[n] >= MaxAttempts
    /\ remaining' = [remaining EXCEPT ![n] = @ - MaxAttempts]
    /\ charged' = charged \cup {o}
    /\ UNCHANGED <<running, allocated, issued, owner, amount, current, delivered, refunded>>
\* Arrival does not require a live process. These requests may already be in
\* the network when cancellation, scale-down, or complete process loss occurs.
Arrive(o) == /\ (o \in charged \/ UnsafeBeforeCharge)
             /\ delivered[o] < MaxAttempts
             /\ delivered' = [delivered EXCEPT ![o] = @+1]
             /\ UNCHANGED <<running, allocated, issued, owner, amount, current, remaining, charged, refunded>>
Crash(n) == /\ n \in running /\ running' = running \ {n}
            /\ remaining' = [remaining EXCEPT ![n] = 0]
            /\ UNCHANGED <<allocated, issued, owner, amount, current, charged, delivered, refunded>>
Restart(n) ==
    /\ n \notin running /\ running' = running \cup {n}
    /\ remaining' = [remaining EXCEPT ![n] =
          IF UnsafeRestore /\ current[n] # None THEN amount[current[n]] ELSE 0]
    /\ current' = [current EXCEPT ![n] = IF UnsafeRestore THEN @ ELSE None]
    /\ UNCHANGED <<allocated, issued, owner, amount, charged, delivered, refunded>>
\* An unsafe collector assumes a dead worker's credit can be refunded, even
\* though its charged uploads may still arrive after a replacement is started.
Refund(g) == /\ UnsafeRefund /\ g \in issued \ refunded /\ owner[g] \notin running
             /\ allocated >= amount[g]
             /\ allocated' = allocated - amount[g] /\ refunded' = refunded \cup {g}
             /\ UNCHANGED <<running, issued, owner, amount, current, remaining, charged, delivered>>

Next == (\E n \in Nodes: Crash(n) \/ Restart(n)
           \/ (\E o \in Operations: Charge(n,o))
           \/ (\E g \in Grants, u \in 1..Capacity, ack \in BOOLEAN: Reserve(n,g,u,ack)))
        \/ (\E o \in Operations: Arrive(o)) \/ (\E g \in Grants: Refund(g))
TypeOK == /\ running \subseteq Nodes /\ allocated \in 0..Capacity /\ issued \subseteq Grants
          /\ owner \in [Grants -> Nodes \cup {None}] /\ amount \in [Grants -> 0..Capacity]
          /\ current \in [Nodes -> Grants \cup {None}] /\ remaining \in [Nodes -> 0..Capacity]
          /\ charged \subseteq Operations /\ delivered \in [Operations -> 0..MaxAttempts]
          /\ refunded \subseteq issued
DurableCoverage == ChargedUnits <= allocated
DeliveryCovered == DeliveredUnits <= ChargedUnits
UploadBound == DeliveredUnits <= Capacity
Spec == Init /\ [][Next]_vars
=============================================================================
