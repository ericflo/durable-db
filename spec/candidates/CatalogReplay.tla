------------------------- MODULE CatalogReplay -------------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Epochs, Frames, UnsafeSkip, UnsafeExtend, UnsafeOrder
VARIABLES selected, objects, epoch, position, replayed, complete
vars == <<selected, objects, epoch, position, replayed, complete>>
Owners == 1..Epochs
RECURSIVE Flatten(_)
Flatten(e) == IF e > Epochs THEN <<>> ELSE
    [n \in 1..selected[e] |-> <<e,n>>] \o Flatten(e+1)
Expected == Flatten(1)
Init == /\ selected \in [Owners -> 0..Frames]
        /\ objects = [e \in Owners |-> 1..selected[e]]
        /\ epoch = IF UnsafeOrder THEN Epochs ELSE 1
        /\ position = 0 /\ replayed = <<>> /\ complete = FALSE
\* Selected catalogs are complete and immutable (RootCatalog / RootJournal).
\* Late uploads may exist outside their cuts. Failed reads stutter: they cannot
\* advance a cursor or declare completion. This is not a serving-permission model.
LateUpload(e,n) == /\ n \in 1..Frames /\ n \notin objects[e]
                   /\ objects' = [objects EXCEPT ![e] = @ \cup {n}]
                   /\ UNCHANGED <<selected, epoch, position, replayed, complete>>
Read == /\ epoch \in Owners /\ ~complete
        /\ position + 1 \in objects[epoch]
        /\ position < selected[epoch] \/ UnsafeExtend
        /\ position' = position + 1
        /\ replayed' = Append(replayed, <<epoch, position+1>>)
        /\ UNCHANGED <<selected, objects, epoch, complete>>
Advance == /\ epoch \in Owners /\ ~complete
           /\ position = selected[epoch] \/ UnsafeSkip
           /\ epoch' = IF UnsafeOrder THEN epoch - 1 ELSE epoch + 1
           /\ position' = 0
           /\ UNCHANGED <<selected, objects, replayed, complete>>
Finish == /\ epoch \notin Owners /\ ~complete
          /\ complete' = TRUE
          /\ UNCHANGED <<selected, objects, epoch, position, replayed>>
Next == Read \/ Advance \/ Finish \/ (\E e \in Owners, n \in 1..Frames: LateUpload(e,n))
Spec == Init /\ [][Next]_vars
OrderedPrefix == /\ Len(replayed) <= Len(Expected)
                 /\ replayed = SubSeq(Expected,1,Len(replayed))
CompleteHistory == complete => replayed = Expected
NotCompleteNonempty == ~(complete /\ Len(Expected) > 0)
=======================================================================
