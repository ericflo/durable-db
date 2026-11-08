----------------------------- MODULE Directory -----------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Keys, MaxNodes, UnsafeWitness
VARIABLES built, entries, children, witness
vars == <<built, entries, children, witness>>
Ids == 1..MaxNodes
Minimum(s) == CHOOSE k \in s: \A j \in s: k <= j
Maximum(s) == CHOOSE k \in s: \A j \in s: k >= j
Init == /\ built = 0
        /\ entries = [n \in Ids |-> {}]
        /\ children = [n \in Ids |-> {}]
        /\ witness = [n \in Ids |-> Minimum(Keys)]
Leaf(ks) ==
  /\ built < MaxNodes /\ ks \subseteq Keys /\ ks # {} /\ Cardinality(ks) <= 2
  /\ built' = built+1
  /\ entries' = [entries EXCEPT ![built+1] = ks]
  /\ witness' = [witness EXCEPT ![built+1] = IF UnsafeWitness THEN Minimum(Keys) ELSE Minimum(ks)]
  /\ UNCHANGED children
Branch(cs) ==
  /\ built < MaxNodes /\ cs \subseteq 1..built /\ Cardinality(cs) = 2
  /\ \A a,b \in cs: a # b => (Maximum(entries[a]) < Minimum(entries[b]) \/ Maximum(entries[b]) < Minimum(entries[a]))
  /\ LET ks == UNION {entries[c] : c \in cs} IN
       /\ entries' = [entries EXCEPT ![built+1] = ks]
       /\ witness' = [witness EXCEPT ![built+1] = IF UnsafeWitness THEN Minimum(Keys) ELSE Minimum(ks)]
  /\ children' = [children EXCEPT ![built+1] = cs]
  /\ built' = built+1
RECURSIVE Reach(_)
Reach(n) == {n} \cup UNION {Reach(c) : c \in children[n]}
Route(n,k) ==
  LET before == {c \in children[n] : Minimum(entries[c]) <= k} IN
  IF before = {} THEN CHOOSE c \in children[n]: \A d \in children[n]: Minimum(entries[c]) <= Minimum(entries[d])
  ELSE CHOOSE c \in before: \A d \in before: Minimum(entries[c]) >= Minimum(entries[d])
RECURSIVE Path(_,_)
Path(n,k) == IF children[n] = {} THEN {n} ELSE {n} \cup Path(Route(n,k),k)
Next == (\E ks \in SUBSET Keys: Leaf(ks)) \/ (\E cs \in SUBSET (1..built): Branch(cs))
TypeOK == /\ built \in 0..MaxNodes /\ \A n \in 1..built: entries[n] # {} /\ entries[n] \subseteq Keys
WitnessReachability == \A root,node \in 1..built: (node \in Reach(root)) <=> (node \in Path(root,witness[node]))
Spec == Init /\ [][Next]_vars
=============================================================================
