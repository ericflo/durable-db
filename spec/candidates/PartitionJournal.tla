------------------------- MODULE PartitionJournal -------------------------
EXTENDS PartitionControl, Integers
CONSTANTS LogLength, UnsafeJournalPermission, UnsafeJournalGap,
          UnsafeJournalEarly, UnsafeJournalMissing, UnsafeJournalReseal,
          UnsafeJournalReady
VARIABLES planned, objects, known, consumed, acknowledged,
          cuts, originalCuts, scans, ready, views, acknowledgingRoots
historyVars == <<planned, objects, known, consumed, acknowledged,
                 cuts, originalCuts, scans, ready, views, acknowledgingRoots>>
combinedVars == <<allVars, historyVars>>
Journals == Partitions \X (1..Generations)
IdleScan == [generation |-> 0, position |-> 0, finished |-> FALSE]
JournalValid(j) == \E p \in Ids: OwnerValid(<<p, j[1], j[2]>>)
Older(j, g) == <<j[1], g>>
HistoryInit ==
    /\ planned = [j \in Journals |-> 0]
    /\ objects = [j \in Journals |-> {}]
    /\ known = [j \in Journals |-> {}]
    /\ consumed = [j \in Journals |-> 0]
    /\ acknowledged = [j \in Journals |-> 0]
    /\ cuts = [j \in Journals |-> -1]
    /\ originalCuts = cuts
    /\ acknowledgingRoots = [j \in Journals |-> {}]
    /\ scans = [j \in Journals |-> IdleScan]
    /\ ready = [j \in Journals |-> FALSE]
    /\ views = [j \in Journals |-> [g \in 1..Generations |-> -1]]

\* Run the actual root and placement protocols, including root replacement,
\* repeated grants, independent freezes and lost/delayed replies, unchanged.
\* A journal generation identifies one writer incarnation. Changing the root
\* does not restart that writer. Replacing a writer MUST advance its generation.
PermissionStep == PartitionNext /\ UNCHANGED historyVars
Prepare(j) == /\ JournalValid(j) /\ ready[j]
              /\ planned' = [planned EXCEPT ![j] = @ + 1]
              /\ UNCHANGED <<allVars, objects, known, consumed, acknowledged,
                             cuts, originalCuts, scans, ready, views, acknowledgingRoots>>
Upload(j, n) == /\ n \in 1..planned[j] /\ n \notin objects[j]
               /\ objects' = [objects EXCEPT ![j] = @ \cup {n}]
               /\ UNCHANGED <<allVars, planned, known, consumed, acknowledged,
                              cuts, originalCuts, scans, ready, views, acknowledgingRoots>>
Observe(j, n) == /\ n \in objects[j] /\ n \notin known[j]
                /\ known' = [known EXCEPT ![j] = @ \cup {n}]
                /\ UNCHANGED <<allVars, planned, objects, consumed, acknowledged,
                               cuts, originalCuts, scans, ready, views, acknowledgingRoots>>
Consume(j, n) == /\ n \in known[j] /\ n > consumed[j]
                /\ (UnsafeJournalGap \/ n = consumed[j] + 1)
                /\ consumed' = [consumed EXCEPT ![j] = n]
                /\ UNCHANGED <<allVars, planned, objects, known, acknowledged,
                               cuts, originalCuts, scans, ready, views, acknowledgingRoots>>
Acknowledge(j, p) == /\ ready[j] /\ consumed[j] > acknowledged[j]
                  /\ (UnsafeJournalPermission \/ OwnerValid(<<p, j[1], j[2]>>))
                  /\ acknowledged' = [acknowledged EXCEPT ![j] = consumed[j]]
                  /\ acknowledgingRoots' = [acknowledgingRoots EXCEPT ![j] = @ \cup {p}]
                  /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                                 cuts, originalCuts, scans, ready, views>>

\* Permission to a newer generation follows the placement drain. Scanning
\* merely because the placement is frozen is too early: an old owner may
\* still acknowledge. A scan can be lost; a durable selection cannot.
BeginScan(j, g) == /\ g < j[2] /\ ~ready[j] /\ scans[j] = IdleScan
                  /\ (cuts[Older(j, g)] = -1 \/ UnsafeJournalReseal)
                  /\ (JournalValid(j) \/
                       (UnsafeJournalEarly /\ placement[j[1]].generation = j[2]))
                  /\ scans' = [scans EXCEPT ![j] =
                       [generation |-> g, position |-> 0, finished |-> FALSE]]
                  /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                                 acknowledged, cuts, originalCuts, ready, views, acknowledgingRoots>>
ReadNext(j) == /\ scans[j].generation # 0 /\ ~scans[j].finished
              /\ scans[j].position + 1 \in objects[Older(j, scans[j].generation)]
              /\ scans' = [scans EXCEPT ![j].position = @ + 1]
              /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                             acknowledged, cuts, originalCuts, ready, views, acknowledgingRoots>>
ReadMissing(j) == /\ scans[j].generation # 0 /\ ~scans[j].finished
                 /\ (UnsafeJournalMissing \/ scans[j].position + 1
                       \notin objects[Older(j, scans[j].generation)])
                 /\ scans' = [scans EXCEPT ![j].finished = TRUE]
                 /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                                acknowledged, cuts, originalCuts, ready, views, acknowledgingRoots>>
ForgetScan(j) == /\ scans[j] # IdleScan
                /\ scans' = [scans EXCEPT ![j] = IdleScan]
                /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                               acknowledged, cuts, originalCuts, ready, views, acknowledgingRoots>>
\* The active coordinator conditionally selects an immutable prefix in the
\* shared authority. This abstracts request/reply uncertainty and exact reads
\* of previously selected prefixes; it does not add a CAS per data batch.
Select(j, p) ==
    /\ JournalValid(j) /\ RootValid(p) /\ authority.owner = p /\ scans[j].finished
    /\ LET old == Older(j, scans[j].generation) IN
         /\ (cuts[old] = -1 \/ UnsafeJournalReseal)
         /\ cuts' = [cuts EXCEPT ![old] = scans[j].position]
         /\ originalCuts' = [originalCuts EXCEPT
               ![old] = IF @ = -1 THEN scans[j].position ELSE @]
    /\ scans' = [scans EXCEPT ![j] = IdleScan]
    /\ authority' = [authority EXCEPT !.revision = @ + 1]
    /\ UNCHANGED <<now, roots, children, ownerVars, planned, objects, known,
                   consumed, acknowledged, ready, views, acknowledgingRoots>>
Ready(j) == /\ JournalValid(j) /\ ~ready[j]
            /\ (UnsafeJournalReady \/ \A g \in 1..(j[2]-1): cuts[Older(j, g)] >= 0)
            /\ ready' = [ready EXCEPT ![j] = TRUE]
            /\ views' = [views EXCEPT ![j] =
                 [g \in 1..Generations |-> cuts[Older(j, g)]]]
            /\ UNCHANGED <<allVars, planned, objects, known, consumed,
                           acknowledged, cuts, originalCuts, scans, acknowledgingRoots>>
HistoryNext == PermissionStep \/ (\E j \in Journals:
    Prepare(j) \/ ReadNext(j) \/ ReadMissing(j) \/ ForgetScan(j) \/ Ready(j)
    \/ (\E p \in Ids: Select(j, p) \/ Acknowledge(j, p))
    \/ (\E g \in 1..Generations: BeginScan(j, g))
    \/ (\E n \in 1..planned[j]: Upload(j, n) \/ Observe(j, n) \/ Consume(j, n)))
HistorySpec == Init /\ OwnerInit /\ HistoryInit /\ [][HistoryNext]_combinedVars
HistoryTypeOK ==
    /\ planned \in [Journals -> Nat]
    /\ objects \in [Journals -> SUBSET Nat] /\ known \in [Journals -> SUBSET Nat]
    /\ consumed \in [Journals -> Nat] /\ acknowledged \in [Journals -> Nat]
    /\ cuts \in [Journals -> Int] /\ originalCuts \in [Journals -> Int]
    /\ scans \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
    /\ acknowledgingRoots \in [Journals -> SUBSET Ids]
    /\ ready \in [Journals -> BOOLEAN] /\ views \in [Journals -> [1..Generations -> Int]]
DurableHistory == \A j \in Journals:
    /\ objects[j] \subseteq 1..planned[j] /\ known[j] \subseteq objects[j]
    /\ 1..consumed[j] \subseteq objects[j] /\ 1..acknowledged[j] \subseteq objects[j]
    /\ acknowledged[j] <= consumed[j] /\ consumed[j] <= planned[j]
SelectedHistory == \A j \in Journals: cuts[j] >= 0 =>
    /\ 1..cuts[j] \subseteq objects[j] /\ acknowledged[j] <= cuts[j]
ImmutableHistory == cuts = originalCuts
CompleteHistory == \A j \in Journals: ready[j] =>
    \A g \in 1..(j[2]-1): views[j][g] >= 0 /\ views[j][g] = cuts[Older(j, g)]
ServingHistory == \A j \in Journals: (ready[j] /\ JournalValid(j)) =>
    \A g \in 1..Generations: acknowledged[Older(j, g)] <=
        IF g < j[2] THEN views[j][g] ELSE IF g = j[2] THEN consumed[j] ELSE 0
HistoryInvariant == OwnerStrong /\ RootExclusive /\ ChildWithinRoot /\ ChildExclusive
                    /\ OwnerWithinRoot /\ PartitionExclusive
                    /\ HistoryTypeOK /\ DurableHistory /\ SelectedHistory
                    /\ ImmutableHistory /\ CompleteHistory /\ ServingHistory
HistorySmall == PartitionSmall /\ \A j \in Journals: planned[j] <= LogLength
NotMovedHistory == ~\E j \in Journals:
    j[2] = 2 /\ ready[j] /\ JournalValid(j) /\ acknowledged[Older(j, 1)] > 0
NotRepeatedHistory == ~\E j \in Journals:
    j[2] = 3 /\ ready[j] /\ JournalValid(j)
    /\ (\A g \in 1..2: acknowledged[Older(j, g)] > 0)
NotResumedHistory == ~\E j \in Journals:
    j[2] = 1 /\ {1, 2} \subseteq acknowledgingRoots[j]
    /\ OwnerValid(<<2, j[1], j[2]>>)
NotRecoveredHistory == ~(recoveredMove /\ ~NotMovedHistory)
NotIndependentHistory == ~\E a, b \in Journals:
    a[1] # b[1] /\ a[2] = 2 /\ b[2] = 1
    /\ ready[a] /\ JournalValid(a) /\ ready[b] /\ JournalValid(b)
    /\ acknowledged[Older(a, 1)] > 0 /\ acknowledged[b] > 0
=============================================================================
