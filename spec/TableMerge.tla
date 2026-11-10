------------------------------ MODULE TableMerge ------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeOld, UnsafeDropDelete, UnsafeSkipError, UnsafeOrder
Sources == {1,2}
Keys == {1,2}
Absent == [revision |-> 0, value |-> "absent"]
Rows == {[revision |-> r, value |-> v]: r \in 1..2, v \in {"value","deleted"}}
Min(S) == CHOOSE n \in S: \A m \in S: n <= m
Max(S) == CHOOSE n \in S: \A m \in S: n >= m
VARIABLES input, heads, consumed, errors, output, failed, reverse
vars == <<input,heads,consumed,errors,output,failed,reverse>>
Rank(k) == IF reverse THEN 3-k ELSE k
Init == /\ reverse \in BOOLEAN
        /\ input \in [Sources -> [Keys -> Rows \cup {Absent}]]
        /\ heads = [s \in Sources |-> 0]
        /\ consumed = heads /\ errors = 0 /\ output = <<>> /\ failed = FALSE
(* Each immutable source already returns its highest visible row per key at
   one common snapshot. A failed/cancelled read does not consume that row. *)
Read(s,fault) == /\ ~failed /\ heads[s] = 0 /\ (~fault \/ errors = 0)
                 /\ IF fault THEN
                      /\ errors' = errors + 1
                      /\ heads' = IF UnsafeSkipError THEN [heads EXCEPT ![s] = 3] ELSE heads
                    ELSE
                      /\ LET keys == {k \in Keys: k > consumed[s] /\ input[s][Rank(k)] # Absent}
                         IN heads' = [heads EXCEPT ![s] = IF keys = {} THEN 3 ELSE Min(keys)]
                      /\ UNCHANGED errors
                 /\ UNCHANGED <<input,consumed,output,failed,reverse>>
Emit == /\ ~failed /\ \A s \in Sources: heads[s] # 0
        /\ LET key == IF UnsafeOrder /\ reverse THEN Max({heads[s]:s \in Sources}) ELSE Min({heads[s]:s \in Sources}) IN
           /\ key < 3
           /\ LET contenders == {input[s][Rank(key)]:s \in {s \in Sources: heads[s] = key}}
                  rows == IF UnsafeDropDelete THEN {r \in contenders: r.value # "deleted"} ELSE contenders
                  revisions == {r.revision:r \in rows}
                  revision == IF rows = {} THEN 0 ELSE IF UnsafeOld THEN Min(revisions) ELSE Max(revisions)
                  winners == {r \in rows:r.revision = revision}
              IN /\ failed' = (Cardinality(winners) > 1)
                 /\ output' = IF Cardinality(winners) = 1
                              THEN Append(output,[key |-> Rank(key), row |-> CHOOSE r \in winners:TRUE]) ELSE output
           /\ heads' = [s \in Sources |-> IF heads[s] = key THEN 0 ELSE heads[s]]
           /\ consumed' = [s \in Sources |-> IF heads[s] = key THEN key ELSE consumed[s]]
        /\ UNCHANGED <<input,errors,reverse>>
Next == (\E s \in Sources,fault \in BOOLEAN: Read(s,fault)) \/ Emit
Spec == Init /\ [][Next]_vars
Present == {k \in Keys: \E s \in Sources: input[s][k] # Absent}
Expected(k) == LET rows == {input[s][k]:s \in {s \in Sources:input[s][k] # Absent}}
                  revision == Max({r.revision:r \in rows})
              IN {r \in rows:r.revision = revision}
OutputKeys == {output[i].key:i \in 1..Len(output)}
Correct == /\ \A i \in 1..Len(output):
               /\ output[i].key \in Present
               /\ Expected(output[i].key) = {output[i].row}
               /\ \A k \in Present: Rank(k) < Rank(output[i].key) => k \in OutputKeys
           /\ \A i,j \in 1..Len(output): i < j => Rank(output[i].key) < Rank(output[j].key)
           /\ (~failed /\ \A s \in Sources: heads[s] = 3) => OutputKeys = Present
=============================================================================
