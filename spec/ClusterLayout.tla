-------------------------- MODULE ClusterLayout --------------------------
EXTENDS Naturals, FiniteSets
CONSTANT Fault
ASSUME Fault \in {"None", "Erase", "Members"}
VARIABLE s
vars == <<s>>
Nodes == {1, 2}
Init == s = [layout |-> 0, chosen |-> 0, data |-> FALSE,
             members |-> Nodes, opened |-> {}, mapped |-> 2]
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
Membership(m) == /\ m \in SUBSET Nodes
                 /\ s' = [s EXCEPT !.members = m]
Map == /\ s.opened # {}
       /\ s' = [s EXCEPT !.mapped = IF Fault = "Members"
                   THEN IF s.members = {} THEN 0 ELSE 1 % Cardinality(s.members)
                   ELSE 1 % s.chosen]
Next == (\E n \in Nodes: Configure(n) \/ Open(n)) \/ Publish \/ Map
        \/ (\E m \in SUBSET Nodes: Membership(m))
TypeOK == s \in [layout: 0..2, chosen: 0..2, data: BOOLEAN,
                 members: SUBSET Nodes, opened: SUBSET Nodes, mapped: 0..2]
ImmutableLayout == s.chosen # 0 => s.layout = s.chosen
CompatibleOpen == s.opened \subseteq {s.layout}
StableMapping == s.mapped # 2 => s.mapped = 1 % s.chosen
Spec == Init /\ [][Next]_vars
=============================================================================
