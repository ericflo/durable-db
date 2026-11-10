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
(* The logical namespace orders/commits this operation through its existing
   journal. Abstracting that delay here is conservative for reference safety. *)
Publish(name, p) == /\ p \in IF UnsafeEarly THEN issued ELSE known
                   /\ references' = {r \in references: r.name # name} \cup {[name |-> name, object |-> p]}
                   /\ UNCHANGED <<issued, physical, known>>
Delete(name) == /\ references' = {r \in references: r.name # name}
               /\ UNCHANGED <<issued, physical, known>>
Next == (\E id \in Objects, value \in Values: Issue(id,value))
        \/ (\E p \in issued: Arrive(p) \/ Confirm(p))
        \/ (\E name \in Names, p \in issued: Publish(name,p))
        \/ (\E name \in Names: Delete(name))
Spec == Init /\ [][Next]_vars
PublishedBytes == \A r \in references: r.object \in physical
UniquePhysical == \A p,q \in issued: p.id = q.id => p = q
NoUnknownPublish == \A r \in references: r.object \in known
NotLateUnreferenced == ~(physical # {} /\ known = {} /\ references = {})
=============================================================================
