----------------------------- MODULE ExternalFiles -----------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Objects, Values, Names, UnsafeEarly, UnsafeReuse
VARIABLES issued, physical, known, references
vars == <<issued, physical, known, references>>
Init == /\ issued = {} /\ physical = {} /\ known = {} /\ references = {}
(* Each issued pair is a PUT to a fresh physical name. A lost response can
   remain unknown, and its bytes can arrive after logical deletion/handoff. *)
Issue(id, value) == /\ IF UnsafeReuse THEN TRUE ELSE ~\E p \in issued: p.id = id
                   /\ issued' = issued \cup {[id |-> id, value |-> value]}
                   /\ UNCHANGED <<physical, known, references>>
Arrive(p) == /\ p \in issued
             /\ physical' = {q \in physical: q.id # p.id} \cup {p}
             /\ UNCHANGED <<issued, known, references>>
Confirm(p) == /\ p \in physical
              /\ known' = known \cup {p}
              /\ UNCHANGED <<issued, physical, references>>
(* A table references data and index together. Uploads can complete in either
   order; losing a reply or cancelling the builder does not publish a group.
   The journal orders publication; abstracting its delay is conservative. *)
Publish(name, parts) == /\ parts # {}
                   /\ parts \subseteq (IF UnsafeEarly THEN issued ELSE known)
                   /\ references' = {r \in references: r.name # name} \cup {[name |-> name, objects |-> parts]}
                   /\ UNCHANGED <<issued, physical, known>>
Delete(name) == /\ references' = {r \in references: r.name # name}
               /\ UNCHANGED <<issued, physical, known>>
Next == (\E id \in Objects, value \in Values: Issue(id,value))
        \/ (\E p \in issued: Arrive(p) \/ Confirm(p))
        \/ (\E name \in Names, parts \in SUBSET issued: Publish(name,parts))
        \/ (\E name \in Names: Delete(name))
Spec == Init /\ [][Next]_vars
PublishedBytes == \A r \in references: r.objects \subseteq physical
UniquePhysical == \A p,q \in issued: p.id = q.id => p = q
NoUnknownPublish == \A r \in references: r.objects \subseteq known
NotLateUnreferenced == ~(physical # {} /\ known = {} /\ references = {})
NotPublishedPair == ~\E r \in references: Cardinality(r.objects) = 2
=============================================================================
