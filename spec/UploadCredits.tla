---------------------------- MODULE UploadCredits ----------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Nodes, Grants, Operations, Capacity, InitialCapacity, MaxAttempts,
          UnsafeRefund, UnsafeRestore, UnsafeBeforeCharge, UnsafeRefillReset, UnsafeInitializeReset
VARIABLES initialized, running, allocated, ceiling, issued, owner, amount, current, remaining,
          charged, delivered, refunded
vars == <<initialized, running, allocated, ceiling, issued, owner, amount, current, remaining,
          charged, delivered, refunded>>
None == "none"
ChargedUnits == Cardinality(charged) * MaxAttempts
AvailableUnits == Cardinality(UNION {{<<n,u>>: u \in 1..remaining[n]}: n \in Nodes})
DeliveredUnits == Cardinality({p \in Operations \X (1..MaxAttempts): p[2] <= delivered[p[1]]})

Init == /\ initialized = FALSE
        /\ running = Nodes /\ allocated = 0 /\ ceiling = 0 /\ issued = {}
        /\ owner = [g \in Grants |-> None] /\ amount = [g \in Grants |-> 0]
        /\ current = [n \in Nodes |-> None] /\ remaining = [n \in Nodes |-> 0]
        /\ charged = {} /\ delivered = [o \in Operations |-> 0] /\ refunded = {}

\* Namespace bootstrap is conditional and precedes EVERY immutable upload,
\* including owner registrations. Reopening never resets an existing ledger.
Initialize == /\ (~initialized \/ UnsafeInitializeReset)
              /\ initialized' = TRUE /\ ceiling' = InitialCapacity /\ allocated' = 0
              /\ UNCHANGED <<running, issued, owner, amount, current, remaining,
                              charged, delivered, refunded>>

\* The authority spends the entire grant before any local credit is usable.
\* A lost acknowledgement can strand the grant; it cannot create more credit.
Reserve(n,g,units,ack) ==
    /\ initialized /\ n \in running /\ g \notin issued
    /\ units \in 1..Capacity /\ allocated + units <= ceiling
    /\ allocated' = allocated + units /\ issued' = issued \cup {g}
    /\ owner' = [owner EXCEPT ![g] = n] /\ amount' = [amount EXCEPT ![g] = units]
    /\ current' = [current EXCEPT ![n] = IF ack THEN g ELSE @]
    /\ remaining' = [remaining EXCEPT ![n] = IF ack THEN @ + units ELSE @]
    /\ UNCHANGED <<initialized, running, ceiling, charged, delivered, refunded>>
Charge(n,o) ==
    /\ n \in running /\ o \notin charged /\ remaining[n] >= MaxAttempts
    /\ remaining' = [remaining EXCEPT ![n] = @ - MaxAttempts]
    /\ charged' = charged \cup {o}
    /\ UNCHANGED <<initialized, running, allocated, ceiling, issued, owner, amount, current, delivered, refunded>>
\* Arrival does not require a live process. These requests may already be in
\* the network when cancellation, scale-down, or complete process loss occurs.
Arrive(o) == /\ (o \in charged \/ UnsafeBeforeCharge)
             /\ delivered[o] < MaxAttempts
             /\ delivered' = [delivered EXCEPT ![o] = @+1]
             /\ UNCHANGED <<initialized, running, allocated, ceiling, issued, owner, amount, current, remaining, charged, refunded>>
Crash(n) == /\ n \in running /\ running' = running \ {n}
            /\ remaining' = [remaining EXCEPT ![n] = 0]
            /\ UNCHANGED <<initialized, allocated, ceiling, issued, owner, amount, current, charged, delivered, refunded>>
Restart(n) ==
    /\ n \notin running /\ running' = running \cup {n}
    /\ remaining' = [remaining EXCEPT ![n] =
          IF UnsafeRestore /\ current[n] # None THEN amount[current[n]] ELSE 0]
    /\ current' = [current EXCEPT ![n] = IF UnsafeRestore THEN @ ELSE None]
    /\ UNCHANGED <<initialized, allocated, ceiling, issued, owner, amount, charged, delivered, refunded>>
\* An unsafe collector assumes a dead worker's credit can be refunded, even
\* though its charged uploads may still arrive after a replacement is started.
Refund(g) == /\ UnsafeRefund /\ g \in issued \ refunded /\ owner[g] \notin running
             /\ allocated >= amount[g]
             /\ allocated' = allocated - amount[g] /\ refunded' = refunded \cup {g}
             /\ UNCHANGED <<initialized, running, ceiling, issued, owner, amount, current, remaining, charged, delivered>>

\* An authorized absolute ceiling increase is a conditional authority update.
\* Old grants, pending arrivals, and all cumulative allocation survive it.
Replenish(newCeiling) ==
    /\ initialized /\ newCeiling \in (ceiling+1)..Capacity
    /\ ceiling' = newCeiling
    /\ allocated' = IF UnsafeRefillReset THEN 0 ELSE allocated
    /\ UNCHANGED <<initialized, running, issued, owner, amount, current, remaining, charged, delivered, refunded>>
\* Either dimension of a byte/attempt grant may run out first. Discarding
\* its unused tail loses rights; it does not reclaim durable allocation.
Discard(n) == /\ n \in running /\ remaining[n] > 0
              /\ remaining' = [remaining EXCEPT ![n] = 0]
              /\ current' = [current EXCEPT ![n] = None]
              /\ UNCHANGED <<initialized, running, allocated, ceiling, issued, owner, amount, charged, delivered, refunded>>

Next == Initialize \/ (\E n \in Nodes: Crash(n) \/ Restart(n) \/ Discard(n)
           \/ (\E o \in Operations: Charge(n,o))
           \/ (\E g \in Grants, u \in 1..Capacity, ack \in BOOLEAN: Reserve(n,g,u,ack)))
        \/ (\E o \in Operations: Arrive(o)) \/ (\E g \in Grants: Refund(g))
        \/ (\E c \in 1..Capacity: Replenish(c))
TypeOK == /\ initialized \in BOOLEAN
          /\ running \subseteq Nodes /\ allocated \in 0..ceiling /\ ceiling \in 0..Capacity
          /\ (initialized => ceiling >= InitialCapacity)
          /\ (~initialized => ceiling = 0) /\ issued \subseteq Grants
          /\ owner \in [Grants -> Nodes \cup {None}] /\ amount \in [Grants -> 0..Capacity]
          /\ current \in [Nodes -> Grants \cup {None}] /\ remaining \in [Nodes -> 0..Capacity]
          /\ charged \subseteq Operations /\ delivered \in [Operations -> 0..MaxAttempts]
          /\ refunded \subseteq issued
DurableCoverage == ChargedUnits + AvailableUnits <= allocated
DeliveryCovered == DeliveredUnits <= ChargedUnits
UploadBound == DeliveredUnits <= ceiling
Spec == Init /\ [][Next]_vars
=============================================================================
