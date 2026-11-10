----------------------- MODULE TransactionRecovery -----------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Databases, Keys, Requests, Values, MaxRecords, ReceiptLimit,
          UnsafeReceipt, UnsafeRevision, UnsafeExpose, UnsafeFloor
VARIABLES history, view, cursor, failed, finished
vars == <<history, view, cursor, failed, finished>>
Empty == [d \in Databases |-> [revision |-> 0,
           values |-> [k \in Keys |-> 0], receipts |-> <<>>, floor |-> 0]]
Keep(s,floor) == SelectSeq(s, LAMBDA r: r.revision > floor)
Receipt(r) == [id |-> r.id, digest |-> r.digest, revision |-> r.revision]
Apply(v,r) == [v EXCEPT ![r.db] =
    [revision |-> r.revision,
     values |-> [v[r.db].values EXCEPT ![r.key] = r.value],
     receipts |-> Keep(Append(v[r.db].receipts, Receipt(r)),r.floor),
     floor |-> r.floor]]
RECURSIVE Fold(_)
Fold(n) == IF n = 0 THEN Empty ELSE Apply(Fold(n-1), history[n])
Present(rs,id) == \E i \in 1..Len(rs): rs[i].id = id
Init == /\ history = <<>> /\ view = Empty /\ cursor = 0
        /\ failed = FALSE /\ finished = FALSE
\* History represents already selected immutable records. This construction
\* enforces accepted revision order and receipt identity; durability, ownership,
\* transaction evaluation and catalog selection are external contracts.
AppendRecord(d,k,id,value,floor) ==
    /\ cursor = 0 /\ ~failed /\ ~finished /\ Len(history) < MaxRecords
    /\ ~Present(Fold(Len(history))[d].receipts,id)
    /\ floor \in Fold(Len(history))[d].floor..Fold(Len(history))[d].revision
    /\ Fold(Len(history))[d].revision+1-floor <= ReceiptLimit
    /\ history' = Append(history,
          [db |-> d, key |-> k, id |-> id, digest |-> value,
           value |-> value, revision |-> Fold(Len(history))[d].revision+1,
           floor |-> floor])
    /\ UNCHANGED <<view, cursor, failed, finished>>
Replay == /\ ~failed /\ ~finished /\ cursor < Len(history)
          /\ LET r == history[cursor+1]
                 next == Apply(view,r)
             IN view' = [next EXCEPT
                  ![r.db].receipts = IF UnsafeReceipt THEN view[r.db].receipts
                                    ELSE @,
                  ![r.db].revision = IF UnsafeRevision THEN view[r.db].revision
                                    ELSE @,
                  ![r.db].floor = IF UnsafeFloor THEN 0 ELSE @]
          /\ cursor' = cursor + 1
          /\ UNCHANGED <<history, failed, finished>>
\* A target may partially change private state before an error. The runner
\* must discard it; no failed or incomplete view can be returned to the caller.
Fail == /\ ~failed /\ ~finished /\ cursor < Len(history)
        /\ LET r == history[cursor+1]
           IN view' = [view EXCEPT ![r.db].values[r.key] = r.value]
        /\ failed' = TRUE
        /\ UNCHANGED <<history, cursor, finished>>
Finish == /\ ~finished
          /\ (cursor = Len(history) /\ ~failed) \/ UnsafeExpose
          /\ finished' = TRUE
          /\ UNCHANGED <<history, view, cursor, failed>>
Next == Replay \/ Fail \/ Finish \/
        (\E d \in Databases, k \in Keys, id \in Requests, value \in Values,
             floor \in 0..MaxRecords: AppendRecord(d,k,id,value,floor))
Spec == Init /\ [][Next]_vars
AtomicPrefix == (~failed) => view = Fold(cursor)
CompleteView == finished => (~failed /\ cursor = Len(history) /\ view = Fold(Len(history)))
NotCompleteNonempty == ~(finished /\ Len(history) > ReceiptLimit)
=============================================================================
