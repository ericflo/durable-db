---------------------- MODULE PartitionJournalProof ----------------------
EXTENDS PartitionJournal, PartitionControlProof
ASSUME JournalParameters == /\ ~UnsafeJournalPermission /\ ~UnsafeJournalGap
       /\ ~UnsafeJournalEarly /\ ~UnsafeJournalMissing /\ ~UnsafeJournalReseal
       /\ ~UnsafeJournalReady

THEOREM PrefixExtension ==
  ASSUME NEW objects, NEW n \in Nat, 1..n \subseteq objects, n + 1 \in objects
  PROVE 1..(n + 1) \subseteq objects
<1>1. ASSUME NEW i \in 1..(n + 1)
      PROVE i \in objects
  <2>1. i \in 1..n \/ i = n + 1 BY SMT
  <2> QED BY SMT, <2>1
<1> QED BY SMT, <1>1
THEOREM MissingBoundsPrefix ==
  ASSUME NEW objects, NEW a \in Nat, NEW b \in Nat, 1..a \subseteq objects, b + 1 \notin objects
  PROVE a <= b
<1>1. ASSUME a > b
      PROVE FALSE
  <2>1. b + 1 \in Nat /\ 1 <= b + 1 /\ b + 1 <= a BY SMT, <1>1
  <2>2. b + 1 \in 1..a BY SMT, <2>1
  <2> QED BY SMT, <2>2
<1> QED BY SMT, <1>1
THEOREM UpdateRange ==
  ASSUME NEW domain, NEW range, NEW f \in [domain -> range],
         NEW key \in domain, NEW value \in range
  PROVE [f EXCEPT ![key] = value] \in [domain -> range]
  BY SMT
THEOREM SetInsertType ==
  ASSUME NEW universe, NEW members \in SUBSET universe, NEW element \in universe
  PROVE (members \cup {element}) \in SUBSET universe
  BY SMT
THEOREM IdleScanWellTyped ==
  IdleScan \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY SMT, MovementParameters DEF IdleScan
THEOREM JournalComponents ==
  ASSUME NEW j \in Journals
  PROVE j[1] \in Partitions /\ j[2] \in 1..Generations /\ j[2] \in Nat
  BY SMT, MovementParameters DEF Journals
THEOREM OlderJournal ==
  ASSUME NEW j \in Journals, NEW g \in 1..Generations
  PROVE Older(j, g) \in Journals
  BY SMT DEF Journals, Older
THEOREM HistoryArithmetic ==
  ASSUME HistoryStrong, NEW j \in Journals
  PROVE /\ planned[j] \in Nat /\ consumed[j] \in Nat /\ acknowledged[j] \in Nat
        /\ cuts[j] \in Int /\ scans[j].position \in Nat
        /\ scans[j].generation \in 0..Generations
        /\ scans[j].generation \in Nat /\ j[2] \in Nat
        /\ EffectiveGeneration(j[1]) \in Int
  BY SMT, MovementParameters, JournalComponents, PlacementArithmetic
    DEF HistoryStrong, HistoryTypeOK
THEOREM ScanJournal ==
  ASSUME HistoryStrong, NEW j \in Journals, scans[j] # IdleScan
  PROVE Older(j, scans[j].generation) \in Journals
        /\ scans[j].generation < j[2]
        /\ j[2] <= EffectiveGeneration(j[1])
<1>1. scans[j].generation \in 0..Generations /\ scans[j].generation > 0
      /\ scans[j].generation \in Nat
  BY SMT, HistoryArithmetic DEF HistoryStrong, HistoryOrder
<1>2. scans[j].generation \in 1..Generations
  BY SMT, MovementParameters, <1>1
<1> QED BY SMT, <1>2, OlderJournal DEF HistoryStrong, HistoryOrder
THEOREM ValidGeneration ==
  ASSUME OwnerStrong, NEW j \in Journals, JournalValid(j)
  PROVE j[2] = EffectiveGeneration(j[1])
<1>1. PICK p \in Ids: OwnerValid(<<p, j[1], j[2]>>)
  BY DEF JournalValid
<1>2. <<p, j[1], j[2]>> \in Owners
  BY SMT, <1>1, JournalComponents DEF Owners
<1>3. now \in Nat /\ grants[<<p, j[1], j[2]>>].expires \in Nat
      /\ j[2] \in Nat /\ EffectiveGeneration(j[1]) \in Int
  BY SMT, <1>2, OwnerArithmetic
<1>4. grants[<<p, j[1], j[2]>>].expires > 0
  BY SMT, <1>1, <1>3 DEF OwnerValid
<1>5. j[2] <= EffectiveGeneration(j[1])
  BY SMT, <1>2, <1>4 DEF OwnerStrong, NoFutureGrants
<1>6. ~(j[2] < EffectiveGeneration(j[1]))
  BY SMT, <1>1, <1>2, <1>3 DEF OwnerStrong, EndedGenerations, OwnerValid
<1> QED BY SMT, <1>3, <1>5, <1>6
THEOREM PermissionGenerationProgress ==
  ASSUME OwnerStrong, PartitionNext
  PROVE \A d \in Partitions: EffectiveGeneration(d)' >= EffectiveGeneration(d)
<1>1. (ControlStep \/ (\E o \in Owners:
        RequestOwner(o) \/ IssueOwner(o) \/ DeliverOwner(o) \/ LoseOwnerReply(o))) =>
      UNCHANGED placement
  BY DEF ControlStep, ownerVars, RequestOwner, IssueOwner, DeliverOwner, LoseOwnerReply
<1>2. UNCHANGED placement =>
      \A d \in Partitions: EffectiveGeneration(d)' >= EffectiveGeneration(d)
  BY SMT, PlacementArithmetic DEF EffectiveGeneration
<1>3. (\E p \in Ids, d \in Partitions: StartMove(p, d)) =>
      \A q \in Partitions: EffectiveGeneration(q)' >= EffectiveGeneration(q)
  BY SMT, PlacementArithmetic, StartMoveKeepsGeneration
<1>4. (\E p \in Ids, d \in Partitions: FinishMove(p, d)) =>
      \A q \in Partitions: EffectiveGeneration(q)' >= EffectiveGeneration(q)
  BY FinishMoveAdvancesGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4 DEF PartitionNext

THEOREM InitialHistoryStrong ==
  ASSUME Init, OwnerInit, HistoryInit
  PROVE HistoryStrong
<1>1. OwnerStrong BY InitialOwnersStrong
<1>2. HistoryTypeOK
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, HistoryTypeOK, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>3. DurableHistory
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, DurableHistory, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>4. SelectedHistory
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, SelectedHistory, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>5. ImmutableHistory
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, ImmutableHistory, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>6. CompleteHistory
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, CompleteHistory, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>7. HistoryOrder
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, HistoryOrder, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1>8. RecoveryState
  BY SMT, MovementParameters, JournalComponents DEF HistoryInit, RecoveryState, OwnerInit,
      EffectiveGeneration, Older, Journals, IdleScan
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM PermissionPreservesHistory ==
  ASSUME HistoryStrong, PermissionStep
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY PartitionNextStrong DEF HistoryStrong, PermissionStep
<1>2. \A d \in Partitions: EffectiveGeneration(d)' >= EffectiveGeneration(d)
  BY PermissionGenerationProgress DEF HistoryStrong, PermissionStep
<1>3. HistoryOrder'
  BY SMT, <1>1, <1>2, JournalComponents, HistoryArithmetic
    DEF HistoryStrong, OwnerStrong, OwnerTypeOK, EffectiveGeneration, PermissionStep, historyVars,
      HistoryOrder
<1> QED BY <1>1, <1>3 DEF HistoryStrong, PermissionStep, historyVars, HistoryTypeOK,
  DurableHistory, SelectedHistory, ImmutableHistory, CompleteHistory, RecoveryState, Older

THEOREM PreparePreservesHistory ==
  ASSUME NEW j \in Journals, HistoryStrong, Prepare(j)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Prepare
<1>2. HistoryTypeOK'
<2>v1. planned[j] + 1 \in Nat
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Prepare, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Prepare, IdleScan
<2>f1. planned' \in [Journals -> Nat]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Prepare, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF Prepare, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, PrefixExtension
    DEF Prepare, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, DurableHistory, Older, IdleScan
<1>4. SelectedHistory'
  BY DEF Prepare, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF Prepare, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF Prepare, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Prepare, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY DEF Prepare, HistoryStrong, RecoveryState, allVars, ownerVars, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM UploadPreservesHistory ==
  ASSUME NEW j \in Journals, NEW n \in 1..planned[j], HistoryStrong, Upload(j, n)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Upload
<1>2. HistoryTypeOK'
<2>v1. (objects[j] \cup {n}) \in SUBSET Nat
  <3>1. objects[j] \in SUBSET Nat
    BY SMT DEF HistoryStrong, HistoryTypeOK
  <3>2. n \in Nat
    BY SMT, HistoryArithmetic
  <3> QED BY SetInsertType, <3>1, <3>2
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Upload, IdleScan
<2>f1. objects' \in [Journals -> SUBSET Nat]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Upload, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF Upload, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Upload, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, DurableHistory, Older, IdleScan
<1>4. SelectedHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Upload, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, SelectedHistory, Older, IdleScan
<1>5. ImmutableHistory'
  BY DEF Upload, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF Upload, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY DEF Upload, HistoryStrong, HistoryOrder, allVars, ownerVars, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, ScanJournal
    DEF Upload, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ObservePreservesHistory ==
  ASSUME NEW j \in Journals, NEW n \in 1..planned[j], HistoryStrong, Observe(j, n)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Observe
<1>2. HistoryTypeOK'
<2>v1. (known[j] \cup {n}) \in SUBSET Nat
  <3>1. known[j] \in SUBSET Nat
    BY SMT DEF HistoryStrong, HistoryTypeOK
  <3>2. n \in Nat
    BY SMT, HistoryArithmetic
  <3> QED BY SetInsertType, <3>1, <3>2
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Observe, IdleScan
<2>f1. known' \in [Journals -> SUBSET Nat]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Observe, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF Observe, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Observe, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, DurableHistory, Older, IdleScan
<1>4. SelectedHistory'
  BY DEF Observe, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF Observe, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF Observe, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY DEF Observe, HistoryStrong, HistoryOrder, allVars, ownerVars, EffectiveGeneration
<1>8. RecoveryState'
  BY DEF Observe, HistoryStrong, RecoveryState, allVars, ownerVars, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ConsumePreservesHistory ==
  ASSUME NEW j \in Journals, NEW n \in 1..planned[j], HistoryStrong, Consume(j, n)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Consume
<1>2. HistoryTypeOK'
<2>v1. n \in Nat
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Consume, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Consume, IdleScan
<2>f1. consumed' \in [Journals -> Nat]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Consume, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF Consume, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, PrefixExtension
    DEF Consume, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, DurableHistory, Older, IdleScan
<1>4. SelectedHistory'
  BY DEF Consume, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF Consume, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF Consume, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY DEF Consume, HistoryStrong, HistoryOrder, allVars, ownerVars, EffectiveGeneration
<1>8. RecoveryState'
  BY DEF Consume, HistoryStrong, RecoveryState, allVars, ownerVars, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM AcknowledgePreservesHistory ==
  ASSUME NEW j \in Journals, NEW p \in Ids, HistoryStrong, Acknowledge(j, p)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Acknowledge
<1>2. HistoryTypeOK'
<2>v1. consumed[j] \in Nat
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Acknowledge, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Acknowledge, IdleScan
<2>f1. acknowledged' \in [Journals -> Nat]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Acknowledge, HistoryStrong, HistoryTypeOK
<2>v2. (acknowledgingRoots[j] \cup {p}) \in SUBSET Ids
  <3>1. acknowledgingRoots[j] \in SUBSET Ids
    BY SMT DEF HistoryStrong, HistoryTypeOK
  <3>2. p \in Ids
    BY SMT, HistoryArithmetic
  <3> QED BY SetInsertType, <3>1, <3>2
<2>k2. j \in Journals
  BY SMT, ScanJournal DEF Acknowledge, IdleScan
<2>f2. acknowledgingRoots' \in [Journals -> SUBSET Ids]
  BY SMT, <2>v2, <2>k2, UpdateRange
    DEF Acknowledge, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1, <2>f2 DEF Acknowledge, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Acknowledge, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, DurableHistory, Older,
      IdleScan, JournalValid, HistoryOrder
<1>4. SelectedHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, ValidGeneration
    DEF Acknowledge, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, SelectedHistory, Older,
      IdleScan, JournalValid, HistoryOrder
<1>5. ImmutableHistory'
  BY DEF Acknowledge, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF Acknowledge, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY DEF Acknowledge, HistoryStrong, HistoryOrder, allVars, ownerVars, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic,
      ValidGeneration, ScanJournal
    DEF Acknowledge, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, JournalValid, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM BeginScanPreservesHistory ==
  ASSUME NEW j \in Journals, NEW g \in 1..Generations, HistoryStrong, BeginScan(j, g)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, BeginScan
<1>2. HistoryTypeOK'
<2>v1. [generation |-> g, position |-> 0, finished |-> FALSE] \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF BeginScan, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF BeginScan, IdleScan
<2>f1. scans' \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF BeginScan, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF BeginScan, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF BeginScan, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY DEF BeginScan, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF BeginScan, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF BeginScan, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, ValidGeneration
    DEF BeginScan, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF BeginScan, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ReadNextPreservesHistory ==
  ASSUME NEW j \in Journals, HistoryStrong, ReadNext(j)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, ReadNext
<1>2. HistoryTypeOK'
<2>v1. [scans[j] EXCEPT !.position = @ + 1] \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF ReadNext, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF ReadNext, IdleScan
<2>f1. scans' \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF ReadNext, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF ReadNext, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF ReadNext, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY DEF ReadNext, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF ReadNext, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF ReadNext, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF ReadNext, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic,
      PrefixExtension, ScanJournal
    DEF ReadNext, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ReadMissingPreservesHistory ==
  ASSUME NEW j \in Journals, HistoryStrong, ReadMissing(j)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, ReadMissing
<1>2. HistoryTypeOK'
<2>v1. [scans[j] EXCEPT !.finished = TRUE] \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF ReadMissing, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF ReadMissing, IdleScan
<2>f1. scans' \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF ReadMissing, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF ReadMissing, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF ReadMissing, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY DEF ReadMissing, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF ReadMissing, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF ReadMissing, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF ReadMissing, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic,
      MissingBoundsPrefix, ScanJournal
    DEF ReadMissing, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ForgetScanPreservesHistory ==
  ASSUME NEW j \in Journals, HistoryStrong, ForgetScan(j)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, ForgetScan
<1>2. HistoryTypeOK'
<2>v1. IdleScan \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY IdleScanWellTyped
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF ForgetScan, IdleScan
<2>f1. scans' \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF ForgetScan, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1 DEF ForgetScan, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF ForgetScan, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY DEF ForgetScan, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF ForgetScan, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY DEF ForgetScan, HistoryStrong, CompleteHistory, allVars, ownerVars, EffectiveGeneration
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF ForgetScan, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF ForgetScan, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM SelectPreservesHistory ==
  ASSUME NEW j \in Journals, NEW p \in Ids, HistoryStrong, Select(j, p)
  PROVE HistoryStrong'
<1>1. OwnerStrong'
  <2>1. Strong' BY RevisionAdvanceStrong DEF HistoryStrong, OwnerStrong, Select
  <2>2. authority \in [owner : 0..Epochs, revision : Nat, active : BOOLEAN]
    BY DEF HistoryStrong, OwnerStrong, Strong, TypeOK
  <2>3. authority' = [authority EXCEPT !.revision = @ + 1]
    BY DEF Select
  <2>4. authority'.owner = authority.owner BY SMT, <2>2, <2>3
  <2> QED BY <2>1, <2>4 DEF Select, ownerVars, HistoryStrong, OwnerStrong, OwnerTypeOK,
    GrantState, PlacementState, NoFutureGrants, FrozenBounds, EndedGenerations, EffectiveGeneration
<1>2. HistoryTypeOK'
<2>v1. scans[j].position \in Int
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Select, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. Older(j, scans[j].generation) \in Journals
  BY SMT, ScanJournal DEF Select, IdleScan
<2>f1. cuts' \in [Journals -> Int]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Select, HistoryStrong, HistoryTypeOK
<2>v2. (IF originalCuts[Older(j, scans[j].generation)] = -1 THEN scans[j].position ELSE originalCuts[Older(j, scans[j].generation)]) \in Int
  BY SMT, <2>k1, <2>v1 DEF HistoryStrong, HistoryTypeOK
<2>k2. Older(j, scans[j].generation) \in Journals
  BY SMT, ScanJournal DEF Select, IdleScan
<2>f2. originalCuts' \in [Journals -> Int]
  BY SMT, <2>v2, <2>k2, UpdateRange
    DEF Select, HistoryStrong, HistoryTypeOK
<2>v3. IdleScan \in [generation : 0..Generations, position : Nat, finished : BOOLEAN]
  BY IdleScanWellTyped
<2>k3. j \in Journals
  BY SMT, ScanJournal DEF Select, IdleScan
<2>f3. scans' \in [Journals -> [generation : 0..Generations, position : Nat, finished : BOOLEAN]]
  BY SMT, <2>v3, <2>k3, UpdateRange
    DEF Select, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1, <2>f2, <2>f3 DEF Select, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF Select, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, ScanJournal
    DEF Select, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, SelectedHistory, Older,
      IdleScan, HistoryOrder, RecoveryState, CompleteHistory, ImmutableHistory
<1>5. ImmutableHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Select, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, ImmutableHistory, Older,
      IdleScan, HistoryOrder, RecoveryState, CompleteHistory
<1>6. CompleteHistory'
  <2>1. \A q \in Journals: \A g \in 1..(q[2]-1): ready[q] =>
      Older(q, g) # Older(j, scans[j].generation)
    BY SMT, JournalParameters DEF Select, HistoryStrong, CompleteHistory
  <2>2. \A q \in Journals: \A g \in 1..(q[2]-1): ready[q] =>
      cuts'[Older(q, g)] = cuts[Older(q, g)]
    BY SMT, <2>1, OlderJournal, JournalComponents, MovementParameters
      DEF Select, HistoryStrong, HistoryTypeOK
  <2> QED BY <2>2 DEF Select, HistoryStrong, CompleteHistory
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic,
      ValidGeneration, ScanJournal
    DEF Select, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older,
      IdleScan, RecoveryState, DurableHistory, EffectiveGeneration, CompleteHistory,
      ImmutableHistory
<1>8. RecoveryState'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Select, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, RecoveryState, Older,
      IdleScan, HistoryOrder, DurableHistory, EffectiveGeneration, CompleteHistory, ImmutableHistory
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM ReadyPreservesHistory ==
  ASSUME NEW j \in Journals, HistoryStrong, Ready(j)
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, Ready
<1>2. HistoryTypeOK'
<2>v1. TRUE \in BOOLEAN
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Ready, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k1. j \in Journals
  BY SMT, ScanJournal DEF Ready, IdleScan
<2>f1. ready' \in [Journals -> BOOLEAN]
  BY SMT, <2>v1, <2>k1, UpdateRange
    DEF Ready, HistoryStrong, HistoryTypeOK
<2>v2. [g \in 1..Generations |-> cuts[Older(j, g)]] \in [1..Generations -> Int]
  BY SMT, MovementParameters, JournalComponents, HistoryArithmetic, OlderJournal
    DEF Ready, HistoryStrong, HistoryTypeOK, IdleScan, Older, Journals, Ids
<2>k2. j \in Journals
  BY SMT, ScanJournal DEF Ready, IdleScan
<2>f2. views' \in [Journals -> [1..Generations -> Int]]
  BY SMT, <2>v2, <2>k2, UpdateRange
    DEF Ready, HistoryStrong, HistoryTypeOK
<2> QED BY <2>f1, <2>f2 DEF Ready, HistoryStrong, HistoryTypeOK
<1>3. DurableHistory'
  BY DEF Ready, HistoryStrong, DurableHistory, allVars, ownerVars, EffectiveGeneration
<1>4. SelectedHistory'
  BY DEF Ready, HistoryStrong, SelectedHistory, allVars, ownerVars, EffectiveGeneration
<1>5. ImmutableHistory'
  BY DEF Ready, HistoryStrong, ImmutableHistory, allVars, ownerVars, EffectiveGeneration
<1>6. CompleteHistory'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic
    DEF Ready, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, CompleteHistory, Older, IdleScan
<1>7. HistoryOrder'
  BY SMT, JournalParameters, MovementParameters, JournalComponents, HistoryArithmetic, ValidGeneration
    DEF Ready, allVars, vars, ownerVars, HistoryStrong, HistoryTypeOK, HistoryOrder, Older, IdleScan,
      RecoveryState, DurableHistory, EffectiveGeneration
<1>8. RecoveryState'
  BY DEF Ready, HistoryStrong, RecoveryState, allVars, ownerVars, EffectiveGeneration
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8 DEF HistoryStrong

THEOREM HistoryNextStrong == HistoryStrong /\ HistoryNext => HistoryStrong'
  BY PermissionPreservesHistory, PreparePreservesHistory, UploadPreservesHistory,
    ObservePreservesHistory, ConsumePreservesHistory, AcknowledgePreservesHistory,
    BeginScanPreservesHistory, ReadNextPreservesHistory, ReadMissingPreservesHistory,
    ForgetScanPreservesHistory, SelectPreservesHistory, ReadyPreservesHistory DEF HistoryNext
THEOREM StutteringHistory ==
  ASSUME HistoryStrong, UNCHANGED combinedVars
  PROVE HistoryStrong'
<1>1. OwnerStrong' BY StutteringOwners DEF HistoryStrong, combinedVars
<1> QED BY <1>1 DEF HistoryStrong, combinedVars, allVars, historyVars, ownerVars,
    HistoryTypeOK, DurableHistory, SelectedHistory, ImmutableHistory, CompleteHistory,
    HistoryOrder, RecoveryState, EffectiveGeneration
THEOREM SafeHistory ==
  ASSUME HistoryStrong
  PROVE HistoryInvariant
<1>1. RootExclusive /\ ChildWithinRoot /\ ChildExclusive
      /\ OwnerWithinRoot /\ PartitionExclusive
  BY StrongSafety, OwnerSafety DEF HistoryStrong, OwnerStrong
<1>2. ServingHistory
  <2>1. ASSUME NEW j \in Journals, ready[j], JournalValid(j), NEW g \in 1..Generations
        PROVE acknowledged[Older(j, g)] <=
          IF g < j[2] THEN views[j][g] ELSE IF g = j[2] THEN consumed[j] ELSE 0
    <3>1. j[2] = EffectiveGeneration(j[1]) BY <2>1, ValidGeneration DEF HistoryStrong
    <3>2. Older(j, g) \in Journals /\ g \in Nat /\ j[2] \in Nat
      BY SMT, OlderJournal, JournalComponents, MovementParameters
    <3>3. CASE g < j[2]
      BY SMT, <2>1, <3>2, <3>3 DEF HistoryStrong, CompleteHistory, SelectedHistory, HistoryTypeOK, Older
    <3>4. CASE g = j[2]
      BY SMT, <2>1, <3>2, <3>4 DEF HistoryStrong, DurableHistory, Journals, Older
    <3>5. CASE g > j[2]
      BY SMT, <2>1, <3>1, <3>2, <3>5, HistoryArithmetic
        DEF HistoryStrong, HistoryOrder, DurableHistory, Older, Journals
    <3> QED BY SMT, <3>2, <3>3, <3>4, <3>5
  <2> QED BY <2>1 DEF ServingHistory
<1> QED BY <1>1, <1>2 DEF HistoryStrong, HistoryInvariant
\* Check the strengthened state once in finite regressions while retaining
\* every original safety predicate.
THEOREM FiniteChecksEquivalent ==
  (HistoryInvariant /\ HistoryStrong) <=>
  (HistoryStrong /\ RootExclusive /\ ChildWithinRoot /\ ChildExclusive
   /\ OwnerWithinRoot /\ PartitionExclusive /\ ServingHistory)
  BY SMT DEF HistoryInvariant, HistoryStrong
THEOREM DelegatedHistorySafety == HistorySpec => []HistoryInvariant
<1>1. HistorySpec => []HistoryStrong
  BY PTL, InitialHistoryStrong, HistoryNextStrong, StutteringHistory DEF HistorySpec
<1> QED BY PTL, <1>1, SafeHistory
=============================================================================
