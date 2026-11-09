--------------------------- MODULE RootJournal ---------------------------
EXTENDS RootControl, Integers
CONSTANTS LogLength, UnsafeJournalPermission, UnsafeJournalGap,
          UnsafeJournalEarly, UnsafeJournalMissing, UnsafeJournalReseal,
          UnsafeJournalReady
VARIABLES planned, objects, known, consumed, acknowledged,
          cuts, originalCuts, scans, ready, views
dataVars == <<planned, objects, known, consumed, acknowledged,
              cuts, originalCuts, scans, ready, views>>
allVars == <<vars, dataVars>>
IdleScan == [epoch |-> 0, position |-> 0, finished |-> FALSE]
DataInit == /\ planned = [p \in Ids |-> 0]
            /\ objects = [p \in Ids |-> {}]
            /\ known = [p \in Ids |-> {}]
            /\ consumed = [p \in Ids |-> 0]
            /\ acknowledged = [p \in Ids |-> 0]
            /\ cuts = [p \in Ids |-> -1]
            /\ originalCuts = cuts
            /\ scans = [p \in Ids |-> IdleScan]
            /\ ready = [p \in Ids |-> FALSE]
            /\ views = [p \in Ids |-> [e \in Ids |-> -1]]

\* RootControl's full acquisition/renewal/drain protocol runs unchanged.
\* This component uses the root itself as one journal owner; delegated
\* partition movement and the clock conversion are separate obligations.
ControlStep == Next /\ UNCHANGED dataVars
PrepareBatch(p) == /\ RootValid(p) /\ ready[p]
                   /\ planned' = [planned EXCEPT ![p] = @ + 1]
                   /\ UNCHANGED <<vars, objects, known, consumed, acknowledged,
                                  cuts, originalCuts, scans, ready, views>>
UploadBatch(p, n) == /\ n \in 1..planned[p] /\ n \notin objects[p]
                    /\ objects' = [objects EXCEPT ![p] = @ \cup {n}]
                    /\ UNCHANGED <<vars, planned, known, consumed, acknowledged,
                                   cuts, originalCuts, scans, ready, views>>
ObserveBatch(p, n) == /\ n \in objects[p] /\ n \notin known[p]
                     /\ known' = [known EXCEPT ![p] = @ \cup {n}]
                     /\ UNCHANGED <<vars, planned, objects, consumed, acknowledged,
                                    cuts, originalCuts, scans, ready, views>>
ConsumeBatch(p, n) == /\ n \in known[p] /\ n > consumed[p]
                     /\ (UnsafeJournalGap \/ n = consumed[p] + 1)
                     /\ consumed' = [consumed EXCEPT ![p] = n]
                     /\ UNCHANGED <<vars, planned, objects, known, acknowledged,
                                    cuts, originalCuts, scans, ready, views>>
AcknowledgeBatch(p) == /\ ready[p] /\ consumed[p] > acknowledged[p]
                       /\ (UnsafeJournalPermission \/ RootValid(p))
                       /\ acknowledged' = [acknowledged EXCEPT ![p] = consumed[p]]
                       /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                      cuts, originalCuts, scans, ready, views>>

\* Recovery can stop or lose its scan at any point. A replacement has a
\* fresh epoch, reuses durable earlier cuts and scans every unselected epoch.
BeginRecovery(p, e) == /\ e < p /\ ~ready[p] /\ scans[p] = IdleScan
                      /\ (cuts[e] = -1 \/ UnsafeJournalReseal)
                      /\ (RootValid(p) \/
                           (UnsafeJournalEarly /\ roots[p].phase = "draining"))
                      /\ scans' = [scans EXCEPT ![p] =
                           [epoch |-> e, position |-> 0, finished |-> FALSE]]
                      /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                     acknowledged, cuts, originalCuts, ready, views>>
ReadNextBatch(p) == /\ scans[p].epoch # 0 /\ ~scans[p].finished
                    /\ scans[p].position + 1 \in objects[scans[p].epoch]
                    /\ scans' = [scans EXCEPT ![p].position = @ + 1]
                    /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                   acknowledged, cuts, originalCuts, ready, views>>
ReadMissingBatch(p) == /\ scans[p].epoch # 0 /\ ~scans[p].finished
                       /\ (UnsafeJournalMissing \/
                            scans[p].position + 1 \notin objects[scans[p].epoch])
                       /\ scans' = [scans EXCEPT ![p].finished = TRUE]
                       /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                      acknowledged, cuts, originalCuts, ready, views>>
ForgetRecovery(p) == /\ scans[p] # IdleScan
                     /\ scans' = [scans EXCEPT ![p] = IdleScan]
                     /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                    acknowledged, cuts, originalCuts, ready, views>>
SelectPrefix(p) ==
    /\ RootValid(p) /\ authority.owner = p /\ scans[p].finished
    /\ LET e == scans[p].epoch IN
         /\ (cuts[e] = -1 \/ UnsafeJournalReseal)
         /\ cuts' = [cuts EXCEPT ![e] = scans[p].position]
         /\ originalCuts' = [originalCuts EXCEPT
               ![e] = IF @ = -1 THEN scans[p].position ELSE @]
    /\ scans' = [scans EXCEPT ![p] = IdleScan]
    \* A conditional catalog publication advances the same provider register.
    \* Lost replies need no local state: the durable cut survives and is reused.
    /\ authority' = [authority EXCEPT !.revision = @ + 1]
    /\ UNCHANGED <<now, roots, children, planned, objects, known, consumed,
                   acknowledged, ready, views>>
ReadyToServe(p) == /\ RootValid(p) /\ ~ready[p]
                  /\ (UnsafeJournalReady \/ \A e \in 1..(p-1): cuts[e] >= 0)
                  /\ ready' = [ready EXCEPT ![p] = TRUE]
                  /\ views' = [views EXCEPT ![p] = cuts]
                  /\ UNCHANGED <<vars, planned, objects, known, consumed,
                                 acknowledged, cuts, originalCuts, scans>>
CombinedNext == ControlStep \/ (\E p \in Ids:
    PrepareBatch(p) \/ AcknowledgeBatch(p) \/ ReadNextBatch(p)
    \/ ReadMissingBatch(p) \/ ForgetRecovery(p) \/ SelectPrefix(p) \/ ReadyToServe(p)
    \/ (\E e \in Ids: BeginRecovery(p, e))
    \/ (\E n \in 1..planned[p]: UploadBatch(p, n) \/ ObserveBatch(p, n) \/ ConsumeBatch(p, n)))
CombinedSpec == Init /\ DataInit /\ [][CombinedNext]_allVars

DataTypeOK == /\ planned \in [Ids -> Nat]
              /\ objects \in [Ids -> SUBSET Nat] /\ known \in [Ids -> SUBSET Nat]
              /\ consumed \in [Ids -> Nat] /\ acknowledged \in [Ids -> Nat]
              /\ cuts \in [Ids -> Int] /\ originalCuts \in [Ids -> Int]
              /\ scans \in [Ids -> [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]]
              /\ ready \in [Ids -> BOOLEAN] /\ views \in [Ids -> [Ids -> Int]]
DurableKnowledge == \A p \in Ids:
    /\ objects[p] \subseteq 1..planned[p] /\ known[p] \subseteq objects[p]
    /\ 1..consumed[p] \subseteq objects[p]
    /\ 1..acknowledged[p] \subseteq objects[p]
SelectedHistory == \A p \in Ids: cuts[p] >= 0 =>
    /\ 1..cuts[p] \subseteq objects[p] /\ acknowledged[p] <= cuts[p]
ImmutableSelection == cuts = originalCuts
CompleteHistory == \A p \in Ids: ready[p] =>
    \A e \in 1..(p-1): views[p][e] >= 0 /\ views[p][e] = cuts[e]
ServingHistory == \A p \in Ids: (ready[p] /\ RootValid(p)) =>
    \A e \in Ids: acknowledged[e] <=
        IF e < p THEN views[p][e] ELSE IF e = p THEN consumed[p] ELSE 0
CombinedInvariant == Invariant /\ Strong /\ DataTypeOK /\ DurableKnowledge
                     /\ SelectedHistory /\ ImmutableSelection /\ CompleteHistory /\ ServingHistory
CombinedSmall == Small /\ \A p \in Ids: planned[p] <= LogLength
\* Reachability control: later owners reuse a selected nonempty earlier log.
NotRepeatedHistory == ~((\A p \in Ids: ready[p]) /\
                       (\A p \in 1..(Epochs-1): acknowledged[p] > 0))
=============================================================================
