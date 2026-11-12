-------------------------- MODULE ClusterLayout --------------------------
EXTENDS Naturals, FiniteSets
CONSTANT Fault
ASSUME Fault \in {"None", "Erase", "Members", "Address"}
VARIABLE s
vars == <<s>>
Nodes == {1, 2}
\* Logical scores are fixed; replacing an endpoint cannot change their order.
Preferred(m, endpoints) ==
    IF m = {} THEN 0 ELSE
    CHOOSE n \in m: \A other \in m:
        IF Fault = "Address" THEN endpoints[n] >= endpoints[other] ELSE n >= other
Init == s = [layout |-> 0, chosen |-> 0, data |-> FALSE,
             members |-> Nodes, opened |-> {}, mapped |-> 2,
             endpoints |-> [n \in Nodes |-> n], preferred |-> 2]
\* A layout is installed by exact comparison only while the directory is empty.
Configure(n) == /\ n \in Nodes /\ s.layout = 0 /\ ~s.data
                /\ s' = [s EXCEPT !.layout = n, !.chosen = n]
Open(n) == /\ n = s.layout /\ n # 0
           /\ s' = [s EXCEPT !.opened = @ \cup {n}]
\* Publication may rebase across layout installation. It preserves the layout
\* from its successful comparison, not from an earlier staging observation.
Publish == /\ ~s.data
           /\ s' = [s EXCEPT !.data = TRUE,
                       !.layout = IF Fault = "Erase" THEN 0 ELSE @]
\* DNS and fallible liveness probes may yield any subset, including empty.
\* These hints affect placement only; neither can remap a database.
Membership(m) == /\ m \in SUBSET Nodes
                 /\ s' = [s EXCEPT !.members = m,
                              !.preferred = Preferred(m, s.endpoints)]
\* Replacing node 1 changes only its address; node 2 remains untouched.
Replace == LET endpoints == [s.endpoints EXCEPT ![1] = IF @ = 1 THEN 3 ELSE 1]
           IN s' = [s EXCEPT !.endpoints = endpoints,
                              !.preferred = Preferred(s.members, endpoints)]
Map == /\ s.opened # {}
       /\ s' = [s EXCEPT !.mapped = IF Fault = "Members"
                   THEN IF s.members = {} THEN 0 ELSE 1 % Cardinality(s.members)
                   ELSE 1 % s.chosen]
Next == (\E n \in Nodes: Configure(n) \/ Open(n)) \/ Publish \/ Map \/ Replace
        \/ (\E m \in SUBSET Nodes: Membership(m))
TypeOK == s \in [layout: 0..2, chosen: 0..2, data: BOOLEAN,
                 members: SUBSET Nodes, opened: SUBSET Nodes, mapped: 0..2,
                 endpoints: [Nodes -> 1..3], preferred: 0..2]
ImmutableLayout == s.chosen # 0 => s.layout = s.chosen
CompatibleOpen == s.opened \subseteq {s.layout}
StableMapping == s.mapped # 2 => s.mapped = 1 % s.chosen
StablePlacement == s.preferred = IF s.members = {} THEN 0 ELSE
    CHOOSE n \in s.members: \A other \in s.members: n >= other
Spec == Init /\ [][Next]_vars
=============================================================================
