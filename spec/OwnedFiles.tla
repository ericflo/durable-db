------------------------------ MODULE OwnedFiles ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Names, Values, MaxWrites, UnsafePending, UnsafeGap, UnsafeVersion, UnsafeReject
VARIABLES history, uploaded, published, view, nextVersion, recovering, cut, cursor, private, finished, rejected
vars == <<history, uploaded, published, view, nextVersion, recovering, cut, cursor, private, finished, rejected>>
Empty == [n \in Names |-> [version |-> 0, value |-> 0]]
Apply(m, r) == [m EXCEPT ![r.name] = [version |-> r.version, value |-> r.value]]
RECURSIVE Fold(_)
Fold(n) == IF n = 0 THEN Empty ELSE Apply(Fold(n-1), history[n])
Init == /\ history = <<>> /\ uploaded = {} /\ published = 0 /\ view = Empty
        /\ nextVersion = 1 /\ recovering = FALSE /\ cut = 0 /\ cursor = 0
        /\ private = Empty /\ finished = FALSE /\ rejected = {}
(* Values includes zero, representing deletion; absence has version zero. *)
Accept(name, value, expected) ==
    /\ ~recovering /\ Len(history) < MaxWrites
    /\ LET base == IF UnsafePending THEN view ELSE Fold(Len(history))
           version == IF value = 0 THEN 0 ELSE IF UnsafeVersion THEN 1 ELSE nextVersion
       IN /\ base[name].version = expected
          /\ history' = Append(history, [name |-> name, value |-> value,
                                          expected |-> expected, version |-> version])
    /\ nextVersion' = nextVersion + 1
    /\ UNCHANGED <<uploaded, published, view, recovering, cut, cursor, private, finished, rejected>>
Upload(i) == /\ i \in 1..Len(history) /\ uploaded' = uploaded \cup {i}
             /\ UNCHANGED <<history, published, view, nextVersion, recovering, cut, cursor, private, finished, rejected>>
Publish(n) == /\ ~recovering /\ n \in (published+1)..Len(history)
              /\ IF UnsafeGap THEN n \in uploaded ELSE 1..n \subseteq uploaded
              /\ published' = n /\ view' = Fold(n)
              /\ UNCHANGED <<history, uploaded, nextVersion, recovering, cut, cursor, private, finished, rejected>>
(* The selected-prefix protocol supplies an immutable complete cut. Its own
   fencing/drain assumptions are checked separately; late uploads may continue. *)
Select(n) == /\ ~recovering /\ n \in published..Len(history) /\ 1..n \subseteq uploaded
             /\ recovering' = TRUE /\ cut' = n
             /\ UNCHANGED <<history, uploaded, published, view, nextVersion, cursor, private, finished, rejected>>
Replay == /\ recovering /\ ~finished /\ cursor < cut
          /\ private' = Apply(private, history[cursor+1]) /\ cursor' = cursor+1
          /\ UNCHANGED <<history, uploaded, published, view, nextVersion, recovering, cut, finished, rejected>>
Finish == /\ recovering /\ cursor = cut /\ ~finished /\ finished' = TRUE
          /\ UNCHANGED <<history, uploaded, published, view, nextVersion, recovering, cut, cursor, private, rejected>>
Reject(name, expected) ==
    /\ ~recovering /\ rejected = {}
    /\ Fold(Len(history))[name].version # expected
    /\ rejected' = {[name |-> name, expected |-> expected, at |-> Len(history), answered |-> UnsafeReject]}
    /\ UNCHANGED <<history, uploaded, published, view, nextVersion, recovering, cut, cursor, private, finished>>
Answer == /\ rejected # {}
          /\ LET r == CHOOSE r \in rejected: TRUE
             IN /\ ~r.answered /\ published >= r.at
                /\ rejected' = {[r EXCEPT !.answered = TRUE]}
          /\ UNCHANGED <<history, uploaded, published, view, nextVersion, recovering, cut, cursor, private, finished>>
Next == (\E n \in Names, v \in Values, e \in 0..MaxWrites: Accept(n,v,e))
        \/ (\E i \in 1..MaxWrites: Upload(i) \/ Publish(i))
        \/ (\E n \in 0..MaxWrites: Select(n)) \/ Replay \/ Finish \/ Answer
        \/ (\E name \in Names, expected \in 0..MaxWrites: Reject(name, expected))
Spec == Init /\ [][Next]_vars
ConditionalOrder == \A i \in 1..Len(history): history[i].expected = Fold(i-1)[history[i].name].version
DurableView == view = Fold(published) /\ 1..published \subseteq uploaded
FreshVersions == \A i,j \in 1..Len(history):
    (i # j /\ history[i].version # 0 /\ history[j].version # 0) => history[i].version # history[j].version
RecoveredView == private = Fold(cursor) /\ (finished => cursor = cut /\ private = Fold(cut))
DefiniteConflict == \A r \in rejected: r.answered => 1..r.at \subseteq uploaded
NotRecoveredLate == ~(finished /\ cut > 0 /\ cut < Len(history) /\ Len(history) \in uploaded)
=============================================================================
