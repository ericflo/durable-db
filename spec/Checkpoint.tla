------------------------------ MODULE Checkpoint ------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeEarly, UnsafeCut, UnsafeReceipt, UnsafeCompare, UnsafeExpose, UnsafeValue
Rows == {[key |-> k,value |-> v]:k \in {1,2},v \in {0,1,2}}
VARIABLES History, owner, register, built, pending, bytes, published, start, cursor, view, done
Empty == [values |-> [k \in {1,2} |-> 0], receipts |-> {}]
RECURSIVE Fold(_)
Fold(n) == IF n = 0 THEN Empty ELSE
           [values |-> [Fold(n-1).values EXCEPT ![History[n].key] = History[n].value],
            receipts |-> 1..n]
vars == <<History,owner,register,built,pending,bytes,published,start,cursor,view,done>>
None == [cut |-> 0, state |-> Empty]
Init == /\ History \in [1..3 -> Rows]
        /\ owner = 1 /\ register = 0 /\ built = None /\ pending = <<>>
        /\ bytes = {} /\ published = None /\ start = -1 /\ cursor = 0
        /\ view = Empty /\ done = FALSE
Build(n) == /\ owner = 1 /\ built = None /\ n \in 1..3
            /\ built' = [cut |-> IF UnsafeCut THEN 3 ELSE n,
                           state |-> [Fold(n) EXCEPT !.receipts = IF UnsafeReceipt THEN {} ELSE @,
                                   !.values = IF UnsafeValue THEN Empty.values ELSE @]]
            /\ UNCHANGED <<owner,register,pending,bytes,published,start,cursor,view,done>>
Upload == /\ built # None /\ bytes' = bytes \cup {built}
          /\ UNCHANGED <<owner,register,built,pending,published,start,cursor,view,done>>
Prepare == /\ published = None /\ owner = 1 /\ built # None /\ pending = <<>> /\ (built \in bytes \/ UnsafeEarly)
           /\ pending' = <<owner,register,built>>
           /\ UNCHANGED <<owner,register,built,bytes,published,start,cursor,view,done>>
\* The captured comparison survives a lost reply; a later owner inherits the
\* durable reference only if publication won the register race.
Publish == /\ pending # <<>>
           /\ (pending[1] = owner /\ pending[2] = register) \/ UnsafeCompare
           /\ published' = pending[3] /\ register' = register + 1
           /\ pending' = <<>>
           /\ UNCHANGED <<owner,built,bytes,start,cursor,view,done>>
Claim == /\ owner = 1 /\ owner' = 2 /\ register' = register + 1
         /\ UNCHANGED <<built,pending,bytes,published,start,cursor,view,done>>
Begin == /\ owner = 2 /\ start = -1
         /\ start' = published.cut /\ cursor' = published.cut /\ view' = published.state
         /\ UNCHANGED <<owner,register,built,pending,bytes,published,done>>
Replay == /\ start >= 0 /\ cursor < 3 /\ ~done
          /\ LET next == cursor+1 IN
               view' = [values |-> [view.values EXCEPT ![History[next].key] = History[next].value],
                         receipts |-> view.receipts \cup {next}]
          /\ cursor' = cursor+1
          /\ UNCHANGED <<owner,register,built,pending,bytes,published,start,done>>
Finish == /\ start >= 0 /\ ~done /\ (cursor = 3 \/ UnsafeExpose)
          /\ done' = TRUE
          /\ UNCHANGED <<owner,register,built,pending,bytes,published,start,cursor,view>>
Next == /\ UNCHANGED History
        /\ ((\E n \in 1..3:Build(n)) \/ Upload \/ Prepare \/ Publish \/ Claim \/ Begin \/ Replay \/ Finish)
Spec == Init /\ [][Next]_vars
Safe == /\ published # None => (published \in bytes /\ published.state = Fold(published.cut))
        /\ start >= 0 => view = Fold(cursor)
        /\ done => (cursor = 3 /\ view = Fold(3))
        /\ start >= 0 => published.cut = start
=============================================================================
