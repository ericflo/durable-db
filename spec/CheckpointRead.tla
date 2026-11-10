--------------------------- MODULE CheckpointRead ---------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeBoundary, UnsafeMissing
\* Complete ordered table bounds and immutable bytes are assumptions. This
\* isolates inclusive routing and the distinction between I/O failure/absence;
\* it does not model the concrete tree, cache, receipt windows or live serving.
Keys == {1,2,3,4}
VARIABLES stored, split, query, frontier, result, failed
vars == <<stored,split,query,frontier,result,failed>>
Min(S) == CHOOSE n \in S:\A m \in S:n <= m
Max(S) == CHOOSE n \in S:\A m \in S:n >= m
Init == /\ stored \in (SUBSET Keys) \ {{} }
        /\ split \in 1..4 /\ query \in 0..5
        /\ frontier = -1 /\ result = "pending" /\ failed = FALSE
Tables == {{k \in stored:k <= split},{k \in stored:k > split}} \ {{} }
Seek == /\ result = "pending" /\ frontier = -1
        /\ LET candidates == {t \in Tables: IF UnsafeBoundary THEN Max(t) > query ELSE Max(t) >= query}
           IN frontier' = IF candidates = {} THEN 0 ELSE Min({Max(t):t \in candidates})
        /\ UNCHANGED <<stored,split,query,result,failed>>
Read(fault) == /\ result = "pending" /\ frontier >= 0
              /\ failed' = fault
              /\ result' = (IF fault /\ ~UnsafeMissing THEN "error" ELSE
                   IF ~fault /\ frontier > 0 /\ query \in (CHOOSE t \in Tables:Max(t)=frontier)
                   THEN "present" ELSE "absent")
              /\ UNCHANGED <<stored,split,query,frontier>>
Next == Seek \/ (\E fault \in BOOLEAN:Read(fault))
Spec == Init /\ [][Next]_vars
Correct == result # "pending" =>
             (IF failed THEN result = "error" ELSE result = (IF query \in stored THEN "present" ELSE "absent"))
=============================================================================
