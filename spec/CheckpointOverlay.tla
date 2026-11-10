-------------------------- MODULE CheckpointOverlay -------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeDeletion, UnsafeReceipt, UnsafeEarly, UnsafeRewrite, UnsafeReuse, UnsafeManifestReuse
Databases == 1..2
Keys == 1..2
Pairs == Databases \X Keys
Records == [db: Databases, key: Keys, value: 0..2]
\* Zero is deletion, -1 is absence. Request identities are unique frame
\* positions here; retention/fingerprint checks are modeled separately.
VARIABLES history, cut, applied, recent, touched, receipts, phase, rewritten
vars == <<history, cut, applied, recent, touched, receipts, phase, rewritten>>
RECURSIVE Fold(_)
Fold(n) == IF n = 0 THEN [p \in Pairs |-> -1]
           ELSE [Fold(n-1) EXCEPT ![<<history[n].db, history[n].key>>] = history[n].value]
ExpectedReceipts(db, n) == {i \in 1..n: history[i].db = db}
Base == Fold(cut)
Visible == [p \in Pairs |-> IF recent[p] = -1 THEN Base[p] ELSE recent[p]]
Init == /\ history \in [1..3 -> Records]
        /\ cut \in 1..3
        /\ applied = 0
        /\ recent = [p \in Pairs |-> -1]
        /\ touched = {}
        /\ receipts = [d \in Databases |-> {}]
        /\ phase = "recovering"
        /\ rewritten = {}
Advance ==
    /\ phase = "recovering" /\ cut + applied < 3
    /\ LET i == cut + applied + 1
           r == history[i]
           prior == IF r.db \in touched THEN receipts[r.db]
                    ELSE IF UnsafeReceipt THEN {} ELSE ExpectedReceipts(r.db, cut)
       IN /\ recent' = IF UnsafeDeletion /\ r.value = 0 THEN recent
                       ELSE [recent EXCEPT ![<<r.db,r.key>>] = r.value]
          /\ touched' = touched \cup {r.db}
          /\ receipts' = [receipts EXCEPT ![r.db] = prior \cup {i}]
    /\ applied' = applied + 1
    /\ phase' = IF UnsafeEarly THEN "ready" ELSE phase
    /\ UNCHANGED <<history, cut, rewritten>>
Finish == /\ phase = "recovering" /\ cut + applied = 3
          /\ phase' = "ready"
          /\ UNCHANGED <<history, cut, applied, recent, touched, receipts, rewritten>>
Fail == /\ phase = "recovering" /\ phase' = "failed"
        /\ UNCHANGED <<history, cut, applied, recent, touched, receipts, rewritten>>
\* Ordered tables cover positions 1..3 and 4; the first crosses a database
\* boundary. An unchanged table reuses its authenticated prior reference.
Position(p) == 2*(p[1]-1)+p[2]
SameTable(p,q) == (Position(p) <= 3) = (Position(q) <= 3)
Reuse(p) == UnsafeReuse \/ \A q \in Pairs: SameTable(p,q) => recent[q] = -1
Merged == [p \in Pairs |-> IF Reuse(p) THEN Base[p] ELSE Visible[p]]
\* An authenticated manifest reference can also be retained when its complete
\* immutable subtree is unchanged. Rust compares length and digest of the
\* encoded node, including every child identity, boundary and digest.
Manifest == IF UnsafeManifestReuse \/ Merged = Base THEN Base ELSE Merged
Rewrite == /\ phase = "ready" /\ rewritten = {}
           /\ rewritten' = {IF UnsafeRewrite THEN recent ELSE Manifest}
           /\ UNCHANGED <<history, cut, applied, recent, touched, receipts, phase>>
Next == Advance \/ Finish \/ Fail \/ Rewrite
Spec == Init /\ [][Next]_vars
Safe == /\ (phase = "ready" =>
              /\ cut + applied = 3
              /\ Visible = Fold(3)
              /\ \A d \in Databases:
                   (IF d \in touched THEN receipts[d] ELSE ExpectedReceipts(d,cut)) = ExpectedReceipts(d,3))
        /\ (rewritten # {} => rewritten = {Fold(3)})
=============================================================================
