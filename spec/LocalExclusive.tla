-------------------------- MODULE LocalExclusive --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Nodes, Operations, UnsafeDropGuard, UnsafeBeforeRecovery
VARIABLES held, handles, ready, io, used, recovered
vars == <<held, handles, ready, io, used, recovered>>
\* One local filesystem with exclusive open-description locks. Every worker
\* and pending syscall retains the guard. Process pause does not release it.
\* Nodes are fresh incarnations; copied process memory/shared network filesystems
\* are outside this contract. Journal durability/recovery is checked separately.
IOOwners == {p[1]: p \in io}
Init == /\ held = {} /\ handles = {} /\ ready = {} /\ io = {} /\ used = {} /\ recovered = {}
Open(n) == /\ held = {} /\ n \notin used
           /\ held' = {n} /\ handles' = {n} /\ used' = used \cup {n}
           /\ ready' = IF UnsafeBeforeRecovery THEN {n} ELSE ready
           /\ UNCHANGED <<io, recovered>>
Recover(n) == /\ n \in handles /\ n \notin ready
              /\ ready' = ready \cup {n} /\ recovered' = recovered \cup {n}
              /\ UNCHANGED <<held, handles, io, used>>
StartIO(n,o) == /\ n \in ready /\ <<n,o>> \notin io
                /\ io' = io \cup {<<n,o>>}
              /\ UNCHANGED <<held, handles, ready, used, recovered>>
Close(n) == /\ n \in handles
            /\ handles' = handles \ {n} /\ ready' = ready \ {n}
            /\ held' = IF n \in IOOwners /\ ~UnsafeDropGuard THEN held ELSE held \ {n}
            /\ UNCHANGED <<io, used, recovered>>
FinishIO(n,o) == /\ <<n,o>> \in io /\ io' = io \ {<<n,o>>}
                 /\ held' = IF n \in handles \/ (\E p \in io': p[1] = n) THEN held ELSE held \ {n}
                 /\ UNCHANGED <<handles, ready, used, recovered>>
\* A completed kernel process exit leaves no old computation able to issue I/O.
Crash(n) == /\ n \in held /\ held' = held \ {n}
            /\ handles' = handles \ {n} /\ ready' = ready \ {n} /\ io' = {p \in io: p[1] # n}
            /\ UNCHANGED <<used, recovered>>
Next == \E n \in Nodes: Open(n) \/ Recover(n) \/ Close(n) \/ Crash(n)
                       \/ (\E o \in Operations: StartIO(n,o) \/ FinishIO(n,o))
Exclusive == /\ Cardinality(held) <= 1
             /\ handles \cup ready \cup IOOwners \subseteq held
             /\ ready \subseteq handles \cap recovered
Spec == Init /\ [][Next]_vars
=============================================================================
