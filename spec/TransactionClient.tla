------------------------ MODULE TransactionClient ------------------------
EXTENDS Naturals
CONSTANTS UnsafeRecomputeAfterUncertainty, UnsafeExpired
VARIABLES revision, expected, receipt, effects, uncertain, phase, expired
vars == <<revision, expected, receipt, effects, uncertain, phase, expired>>
(* expired also starts from a persisted request after a prior commit, receipt
   eviction and client restart; the local uncertainty flag need not survive. *)
Init == /\ expired \in BOOLEAN /\ expected = 0 /\ receipt = FALSE
        /\ revision = IF expired THEN 1 ELSE 0
        /\ effects = IF expired THEN 1 ELSE 0
        /\ uncertain = FALSE /\ phase = "send"
Publish == /\ phase = "send" /\ revision = expected /\ ~receipt
           /\ revision' = revision + 1 /\ effects' = effects + 1 /\ receipt' = TRUE
           /\ phase' \in {"done", "lost"}
           /\ UNCHANGED <<expected, uncertain, expired>>
Lose == /\ phase = "lost" /\ uncertain' = TRUE /\ phase' = "send"
        /\ UNCHANGED <<revision, expected, receipt, effects, expired>>
Replay == /\ phase = "send" /\ receipt /\ phase' = "done"
          /\ UNCHANGED <<revision, expected, receipt, effects, uncertain, expired>>
\* Other transactions can advance the revision and evict the bounded receipt.
Advance == /\ revision < 3 /\ revision' = revision + 1 /\ receipt' = FALSE
           /\ UNCHANGED <<expected, effects, uncertain, phase, expired>>
Conflict == /\ phase = "send" /\ ~expired /\ revision # expected /\ ~receipt
            /\ IF ~uncertain \/ UnsafeRecomputeAfterUncertainty
                 THEN /\ expected' = revision /\ UNCHANGED phase
                 ELSE /\ phase' = "unresolved" /\ UNCHANGED expected
            /\ UNCHANGED <<revision, receipt, effects, uncertain, expired>>
Expired == /\ phase = "send" /\ expired
           /\ IF UnsafeExpired
                THEN /\ expected' = revision /\ expired' = FALSE /\ UNCHANGED phase
                ELSE /\ phase' = "unresolved" /\ UNCHANGED <<expected, expired>>
           /\ UNCHANGED <<revision, receipt, effects, uncertain>>
Next == Publish \/ Lose \/ Replay \/ Advance \/ Conflict \/ Expired
AtMostOneEffect == effects <= 1
Spec == Init /\ [][Next]_vars
=============================================================================
