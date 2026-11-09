------------------------ MODULE RequestBudget ------------------------
EXTENDS Integers

CONSTANTS Capacity, Rate, Idle, MaxTime, UnsafeSample, UnsafeEviction
ASSUME /\ Capacity >= 5 /\ Rate > 0 /\ Idle > 0 /\ MaxTime > Idle
       /\ UnsafeSample \in BOOLEAN /\ UnsafeEviction \in BOOLEAN

VARIABLES now, stamp, credit, present, reference
vars == <<now, stamp, credit, present, reference>>
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Refill(at) == Min(Capacity, credit + Max(0, at-stamp)*Rate)

Init == /\ now = 0 /\ stamp = 0 /\ credit = Capacity
        /\ present = TRUE /\ reference = Capacity

\* The reference bucket advances at processing time, independently of the
\* implementation's captured timestamp and entry eviction. No restarts or
\* multiple pods: these are explicitly per-process overload limits.
Tick == /\ now < MaxTime
        /\ now' = now+1
        /\ reference' = Min(Capacity, reference+Rate)
        /\ UNCHANGED <<stamp, credit, present>>

Charge(at, cost) ==
    /\ at \in 0..now
    /\ UnsafeSample \/ at = now
    /\ LET available == IF present THEN Refill(at) ELSE Capacity
           accepted == available >= cost
       IN /\ credit' = IF accepted THEN available-cost ELSE available
          /\ reference' = IF accepted THEN reference-cost ELSE reference
    /\ stamp' = at
    /\ present' = TRUE
    /\ UNCHANGED now

\* Forgetting a depleted budget must not mint another initial burst.
Evict == /\ present /\ now-stamp >= Idle
         /\ UnsafeEviction \/ Refill(now) = Capacity
         /\ present' = FALSE
         /\ UNCHANGED <<now, stamp, credit, reference>>

Next == Tick \/ Evict \/ \E at \in 0..now, cost \in {1, 5}: Charge(at, cost)
Spec == Init /\ [][Next]_vars
TypeOK == /\ now \in 0..MaxTime /\ stamp \in 0..now
          /\ credit \in 0..Capacity /\ present \in BOOLEAN
          /\ reference \in Int
NoFreeCredit == reference >= 0
=====================================================================
