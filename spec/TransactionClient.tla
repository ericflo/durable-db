------------------------ MODULE TransactionClient ------------------------
EXTENDS Naturals
CONSTANT UnsafeRecomputeAfterUncertainty
VARIABLES revision, expected, receipt, effects, uncertain, phase
vars == <<revision, expected, receipt, effects, uncertain, phase>>
Init == /\ revision = 0 /\ expected = 0 /\ receipt = FALSE
        /\ effects = 0 /\ uncertain = FALSE /\ phase = "send"
Publish == /\ phase = "send" /\ revision = expected /\ ~receipt
           /\ revision' = revision + 1 /\ effects' = effects + 1 /\ receipt' = TRUE
           /\ phase' \in {"done", "lost"}
           /\ UNCHANGED <<expected, uncertain>>
Lose == /\ phase = "lost" /\ uncertain' = TRUE /\ phase' = "send"
        /\ UNCHANGED <<revision, expected, receipt, effects>>
Replay == /\ phase = "send" /\ receipt /\ phase' = "done"
          /\ UNCHANGED <<revision, expected, receipt, effects, uncertain>>
\* Other transactions can advance the revision and evict the bounded receipt.
Advance == /\ revision < 3 /\ revision' = revision + 1 /\ receipt' = FALSE
           /\ UNCHANGED <<expected, effects, uncertain, phase>>
Conflict == /\ phase = "send" /\ revision # expected /\ ~receipt
            /\ IF ~uncertain \/ UnsafeRecomputeAfterUncertainty
                 THEN /\ expected' = revision /\ UNCHANGED phase
                 ELSE /\ phase' = "unresolved" /\ UNCHANGED expected
            /\ UNCHANGED <<revision, receipt, effects, uncertain>>
Next == Publish \/ Lose \/ Replay \/ Advance \/ Conflict
AtMostOneEffect == effects <= 1
Spec == Init /\ [][Next]_vars
=============================================================================
