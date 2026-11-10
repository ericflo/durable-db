------------------------------ MODULE Checkpoint ------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeEarly, UnsafeCut, UnsafeReceipt, UnsafeCompare, UnsafeExpose, UnsafeValue, UnsafeFloor
Rows == {[key |-> k,value |-> v]:k \in {1,2},v \in {0,1,2}}
VARIABLES History, owner, register, built, pending, bytes, published, start, cursor, view, done, selected, frames
Empty == [values |-> [k \in {1,2} |-> 0], receipts |-> {}]
RECURSIVE Fold(_)
Fold(n) == IF n = 0 THEN Empty ELSE
           [values |-> [Fold(n-1).values EXCEPT ![History[n].key] = History[n].value],
            receipts |-> 1..n]
vars == <<History,owner,register,built,pending,bytes,published,start,cursor,view,done,selected,frames>>
None == [cut |-> 0, state |-> Empty]
Init == /\ History \in [1..3 -> Rows]
        /\ owner = 1 /\ register = 0 /\ built = None /\ pending = <<>>
        /\ bytes = {} /\ published = None /\ start = -1 /\ cursor = 0
        /\ view = Empty /\ done = FALSE /\ selected = -1 /\ frames = 1..3
Build(n) == /\ owner = 1 /\ built = None /\ n \in 1..3
            /\ built' = [cut |-> IF UnsafeCut THEN 3 ELSE n,
                           state |-> [Fold(n) EXCEPT !.receipts = IF UnsafeReceipt THEN {} ELSE @,
                                   !.values = IF UnsafeValue THEN Empty.values ELSE @]]
            /\ UNCHANGED <<owner,register,pending,bytes,published,start,cursor,view,done,selected,frames>>
Upload == /\ built # None /\ bytes' = bytes \cup {built}
          /\ UNCHANGED <<owner,register,built,pending,published,start,cursor,view,done,selected,frames>>
Prepare == /\ published = None /\ owner = 1 /\ built # None /\ pending = <<>> /\ (built \in bytes \/ UnsafeEarly)
           /\ pending' = <<owner,register,built>>
           /\ UNCHANGED <<owner,register,built,bytes,published,start,cursor,view,done,selected,frames>>
\* The captured comparison survives a lost reply; a later owner inherits the
\* durable reference only if publication won the register race.
\* Local renewal and checkpoint callers serialize control invocations. Renewal
\* is a stuttering step for checkpoint data; cancellation preserves uncertainty.
Publish == /\ pending # <<>>
           /\ (pending[1] = owner /\ pending[2] = register) \/ UnsafeCompare
           /\ published' = pending[3] /\ register' = register + 1
           /\ pending' = <<>>
           /\ UNCHANGED <<owner,built,bytes,start,cursor,view,done,selected,frames>>
Claim == /\ owner = 1 /\ owner' = 2 /\ register' = register + 1
         /\ UNCHANGED <<built,pending,bytes,published,start,cursor,view,done,selected,frames>>
(* A live checkpoint is a minimum recoverable prefix even before the old
   journal has a selected end. Discovery starts at its authenticated cut. *)
Select(n) == /\ owner = 2 /\ selected = -1 /\ n \in 0..3
             /\ (UnsafeFloor \/ n >= published.cut)
             /\ selected' = n
             /\ UNCHANGED <<owner,register,built,pending,bytes,published,start,cursor,view,done,frames>>
Begin == /\ owner = 2 /\ start = -1 /\ selected >= 0
         /\ start' = published.cut /\ cursor' = published.cut /\ view' = published.state
         /\ UNCHANGED <<owner,register,built,pending,bytes,published,done,selected,frames>>
Replay == /\ start >= 0 /\ cursor < selected /\ ~done /\ cursor+1 \in frames
          /\ LET next == cursor+1 IN
               view' = [values |-> [view.values EXCEPT ![History[next].key] = History[next].value],
                         receipts |-> view.receipts \cup {next}]
          /\ cursor' = cursor+1
          /\ UNCHANGED <<owner,register,built,pending,bytes,published,start,done,selected,frames>>
Finish == /\ start >= 0 /\ ~done /\ (cursor = selected \/ UnsafeExpose)
          /\ done' = TRUE
          /\ UNCHANGED <<owner,register,built,pending,bytes,published,start,cursor,view,selected,frames>>
\* A committed checkpoint replaces only its covered journal prefix. Flattening
\* epochs assumes selected history and monotone checkpoint publication; object
\* naming, provider comparisons and Rust refinement have separate obligations.
\* A closed older epoch is wholly before the cut; late bytes outside its
\* selected prefix never enter History. Deleting them is a stuttering step.
Collect(cut) == /\ \E n \in frames: n <= cut /\ frames' = frames \ {n}
                /\ UNCHANGED <<History,owner,register,built,pending,bytes,published,start,cursor,view,done,selected>>
Next == /\ UNCHANGED History
        /\ ((\E n \in 1..3:Build(n)) \/ Upload \/ Prepare \/ Publish \/ Claim \/ (\E n \in 0..3: Select(n)) \/ Begin \/ Replay \/ Finish \/ Collect(published.cut))
Spec == Init /\ [][Next]_vars
UnsafeStagedCollectionSpec == Init /\ [][Next \/ Collect(built.cut)]_vars
UnsafeSuffixCollectionSpec == Init /\ [][Next \/ (published # None /\ Collect(published.cut+1))]_vars
Safe == /\ (published.cut+1)..3 \subseteq frames
        /\ published # None => (published \in bytes /\ published.state = Fold(published.cut))
        /\ start >= 0 => view = Fold(cursor)
        /\ done => (cursor = selected /\ view = Fold(selected))
        /\ start >= 0 => published.cut = start /\ published.cut <= selected
=============================================================================
