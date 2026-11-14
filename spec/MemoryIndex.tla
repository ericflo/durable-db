------------------------------ MODULE MemoryIndex ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS UnsafePartial, UnsafeLatest, UnsafeStatistics, UnsafeQuota
Keys == {1,2}
Values == {0,1,2} \* 0 is a deletion, 1 is an empty value, 2 is a nonempty value.
Capacity == 3
VARIABLES history, rows, rejected, reply, statistics, floor
vars == <<history,rows,rejected,reply,statistics,floor>>
Max(S) == CHOOSE n \in S: \A m \in S:n >= m
Mutations == UNION {[domain -> Values]:domain \in SUBSET Keys}
Pairs(h) == {p \in (1..Len(h)) \X Keys:p[2] \in DOMAIN h[p[1]]}
Flatten(h) == {[key |-> p[2], revision |-> p[1], value |-> h[p[1]][p[2]]]:p \in Pairs(h)}
Visible(rs,k,s) == LET matches == {r \in rs:r.key = k /\ r.revision <= s}
                  IN IF matches = {} THEN {} ELSE
                     {r \in matches:r.revision = Max({v.revision:v \in matches})}
Retained(h,f) == {r \in Flatten(h):r.revision > f} \cup
                 UNION {Visible(Flatten(h),k,f):k \in Keys}
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
Init == /\ floor = 0 /\ history = <<>> /\ rows = {} /\ rejected = FALSE /\ reply = <<>> /\ statistics = [keys |-> 0, bytes |-> 0]
Commit(m) == /\ Len(history) < 3 /\ ~rejected
             /\ LET next == Append(history,m)
                    appended == rows \cup {r \in Flatten(next):r.revision = Len(next)} IN
                IF Cardinality(appended) <= Capacity /\ (UnsafeQuota \/ Fits(Delta(m))) THEN
                  /\ history' = next /\ rows' = appended /\ statistics' = Delta(m)
                  /\ UNCHANGED rejected
                ELSE
                  /\ history' = history /\ rejected' = TRUE /\ UNCHANGED statistics
                  /\ rows' = IF UnsafePartial THEN appended ELSE rows
             /\ reply' = <<>> /\ UNCHANGED floor
Read(k,s) == /\ s >= floor
             /\ reply' = <<k,s,Visible(rows,k,IF UnsafeLatest THEN Len(history) ELSE s)>>
             /\ UNCHANGED <<history,rows,rejected,statistics,floor>>
\* Value reclamation changes neither durable history nor transaction metadata.
Compact(advanceFloor,keepDeletes) ==
    LET latest == Retained(history,Len(history)) IN
    /\ Cardinality(latest) < Cardinality(rows)
    /\ rows' = IF keepDeletes THEN latest ELSE {r \in latest:r.value # 0}
    /\ floor' = IF advanceFloor THEN Len(history) ELSE floor
    /\ UNCHANGED <<history,rejected,reply,statistics>>
NextWith(advanceFloor,keepDeletes) ==
    (\E m \in Mutations:Commit(m)) \/ (\E k \in Keys,s \in 0..3:Read(k,s))
    \/ Compact(advanceFloor,keepDeletes)
Spec == Init /\ [][NextWith(TRUE,TRUE)]_vars
UnsafeFloorSpec == Init /\ [][NextWith(FALSE,TRUE)]_vars
UnsafeDeleteSpec == Init /\ [][NextWith(TRUE,FALSE)]_vars
Atomic == /\ floor \in 0..Len(history)
          /\ rows = Retained(history,floor) /\ Cardinality(rows) <= Capacity
          /\ reply # <<>> => reply[3] = Visible(Flatten(history),reply[1],reply[2])
Statistics == statistics = Measure(rows)
Quota == Fits(statistics)
=============================================================================
