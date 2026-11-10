------------------------------ MODULE MemoryIndex ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS UnsafePartial, UnsafeLatest, UnsafeStatistics, UnsafeQuota
Keys == {1,2}
Values == {0,1,2} \* 0 is a deletion, 1 is an empty value, 2 is a nonempty value.
Capacity == 3
VARIABLES history, rows, rejected, reply, statistics
vars == <<history,rows,rejected,reply,statistics>>
Max(S) == CHOOSE n \in S: \A m \in S:n >= m
Mutations == UNION {[domain -> Values]:domain \in SUBSET Keys}
Pairs(h) == {p \in (1..Len(h)) \X Keys:p[2] \in DOMAIN h[p[1]]}
Count(h) == Cardinality(Pairs(h))
Flatten(h) == {[key |-> p[2], revision |-> p[1], value |-> h[p[1]][p[2]]]:p \in Pairs(h)}
Visible(rs,k,s) == LET matches == {r \in rs:r.key = k /\ r.revision <= s}
                  IN IF matches = {} THEN {} ELSE
                     {r \in matches:r.revision = Max({v.revision:v \in matches})}
Value(rs,k) == LET visible == Visible(rs,k,3)
               IN IF visible = {} THEN 0 ELSE (CHOOSE row \in visible: TRUE).value
Measure(rs) == [keys |-> Cardinality({k \in Keys: Value(rs,k) > 0}),
                bytes |-> Value(rs,1) + Value(rs,2)]
Size(m,k) == IF k \in DOMAIN m THEN m[k] ELSE 0
Delta(m) == LET before == {r \in rows:r.key \in DOMAIN m}
                old == IF UnsafeStatistics THEN [keys |-> 0, bytes |-> 0] ELSE Measure(before)
            IN [keys |-> statistics.keys + Cardinality({k \in DOMAIN m:m[k] > 0}) - old.keys,
                bytes |-> statistics.bytes + Size(m,1) + Size(m,2) - old.bytes]
Fits(s) == s.keys <= 1 /\ s.bytes <= 2
Init == /\ history = <<>> /\ rows = {} /\ rejected = FALSE /\ reply = <<>> /\ statistics = [keys |-> 0, bytes |-> 0]
Commit(m) == /\ Len(history) < 3 /\ ~rejected
             /\ LET next == Append(history,m) IN
                IF Count(next) <= Capacity /\ (UnsafeQuota \/ Fits(Delta(m))) THEN
                  /\ history' = next /\ rows' = Flatten(next) /\ statistics' = Delta(m)
                  /\ UNCHANGED rejected
                ELSE
                  /\ history' = history /\ rejected' = TRUE /\ UNCHANGED statistics
                  /\ rows' = IF UnsafePartial THEN Flatten(next) ELSE rows
             /\ reply' = <<>>
Read(k,s) == /\ reply' = <<k,s,Visible(rows,k,IF UnsafeLatest THEN Len(history) ELSE s)>>
             /\ UNCHANGED <<history,rows,rejected,statistics>>
Next == (\E m \in Mutations:Commit(m)) \/ (\E k \in Keys,s \in 0..3:Read(k,s))
Spec == Init /\ [][Next]_vars
Atomic == /\ rows = Flatten(history) /\ Cardinality(rows) <= Capacity
          /\ reply # <<>> => reply[3] = Visible(Flatten(history),reply[1],reply[2])
Statistics == statistics = Measure(rows)
Quota == Fits(statistics)
=============================================================================
