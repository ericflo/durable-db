------------------------------ MODULE MemoryIndex ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS UnsafePartial, UnsafeLatest
Keys == {1,2}
Values == {0,1,2} \* 0 is a deletion, 1 is an empty value, 2 is a nonempty value.
Capacity == 3
VARIABLES history, rows, rejected, reply
vars == <<history,rows,rejected,reply>>
Max(S) == CHOOSE n \in S: \A m \in S:n >= m
Mutations == UNION {[domain -> Values]:domain \in SUBSET Keys}
Pairs(h) == {p \in (1..Len(h)) \X Keys:p[2] \in DOMAIN h[p[1]]}
Count(h) == Cardinality(Pairs(h))
Flatten(h) == {[key |-> p[2], revision |-> p[1], value |-> h[p[1]][p[2]]]:p \in Pairs(h)}
Visible(rs,k,s) == LET matches == {r \in rs:r.key = k /\ r.revision <= s}
                  IN IF matches = {} THEN {} ELSE
                     {r \in matches:r.revision = Max({v.revision:v \in matches})}
Init == /\ history = <<>> /\ rows = {} /\ rejected = FALSE /\ reply = <<>>
Commit(m) == /\ Len(history) < 3 /\ ~rejected
             /\ LET next == Append(history,m) IN
                IF Count(next) <= Capacity THEN
                  /\ history' = next /\ rows' = Flatten(next)
                  /\ UNCHANGED rejected
                ELSE
                  /\ history' = history /\ rejected' = TRUE
                  /\ rows' = IF UnsafePartial THEN Flatten(next) ELSE rows
             /\ reply' = <<>>
Read(k,s) == /\ reply' = <<k,s,Visible(rows,k,IF UnsafeLatest THEN Len(history) ELSE s)>>
             /\ UNCHANGED <<history,rows,rejected>>
Next == (\E m \in Mutations:Commit(m)) \/ (\E k \in Keys,s \in 0..3:Read(k,s))
Spec == Init /\ [][Next]_vars
Atomic == /\ rows = Flatten(history) /\ Cardinality(rows) <= Capacity
          /\ reply # <<>> => reply[3] = Visible(Flatten(history),reply[1],reply[2])
=============================================================================
