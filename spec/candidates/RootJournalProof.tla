------------------------- MODULE RootJournalProof -------------------------
EXTENDS RootJournal, RootControlProof
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

THEOREM EpochDomain ==
  ASSUME NEW n \in 0..Epochs
  PROVE n = 0 \/ n \in Ids
<1>1. n \in Int /\ 0 <= n /\ n <= Epochs BY SMT, Parameters
<1>2. n = 0 \/ 1 <= n BY SMT, <1>1
<1> QED BY SMT, <1>1, <1>2 DEF Ids

THEOREM EarlierInactive ==
  ASSUME NEW p \in Ids, NEW q \in Ids, Strong,
         p < q, roots[q].phase = "active"
  PROVE ~RootValid(p)
<1>1. now \in Nat /\ roots[p].expires \in Nat /\ roots[q].drain \in Nat
  BY SMT, Parameters DEF Strong, TypeOK, Ids
<1>2. roots[p].expires <= roots[q].drain + Lease
  BY SMT DEF Strong, FenceBounds
<1>3. now >= roots[q].drain + Lease
  BY SMT DEF Strong, RootState
<1> QED BY SMT, Parameters, <1>1, <1>2, <1>3 DEF RootValid

THEOREM TickKeepsActive ==
  ASSUME Strong, Tick
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF Tick, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM StartFenceKeepsActive ==
  ASSUME NEW q \in Ids, Strong, StartFence(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF StartFence, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM ApplyFenceKeepsActive ==
  ASSUME NEW q \in Ids, Strong, ApplyFence(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF ApplyFence, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM DeliverFenceKeepsActive ==
  ASSUME NEW q \in Ids, Strong, DeliverFence(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF DeliverFence, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM StartRenewKeepsActive ==
  ASSUME NEW q \in Ids, Strong, StartRenew(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF StartRenew, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM ApplyRenewKeepsActive ==
  ASSUME NEW q \in Ids, Strong, ApplyRenew(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF ApplyRenew, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM DeliverRenewKeepsActive ==
  ASSUME NEW q \in Ids, Strong, DeliverRenew(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF DeliverRenew, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM StartActivateKeepsActive ==
  ASSUME NEW q \in Ids, Strong, StartActivate(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF StartActivate, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM ApplyActivateKeepsActive ==
  ASSUME NEW q \in Ids, Strong, ApplyActivate(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF ApplyActivate, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM DeliverActivateKeepsActive ==
  ASSUME NEW q \in Ids, Strong, DeliverActivate(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF DeliverActivate, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM LoseReplyKeepsActive ==
  ASSUME NEW q \in Ids, Strong, LoseReply(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF LoseReply, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM RequestChildKeepsActive ==
  ASSUME NEW q \in Ids, Strong, RequestChild(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF RequestChild, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM IssueChildKeepsActive ==
  ASSUME NEW q \in Ids, Strong, IssueChild(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF IssueChild, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM DeliverChildKeepsActive ==
  ASSUME NEW q \in Ids, Strong, DeliverChild(q)
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY SMT, Parameters DEF DeliverChild, Strong, TypeOK, RootState, Ids, Max,
     RootReplyEnd, ChildReplyEnd, RootValid

THEOREM ActivePhasePersists ==
  ASSUME Strong, Next
  PROVE \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY TickKeepsActive, StartFenceKeepsActive, ApplyFenceKeepsActive, DeliverFenceKeepsActive,
      StartRenewKeepsActive, ApplyRenewKeepsActive, DeliverRenewKeepsActive, StartActivateKeepsActive,
      ApplyActivateKeepsActive, DeliverActivateKeepsActive, LoseReplyKeepsActive,
      RequestChildKeepsActive, IssueChildKeepsActive, DeliverChildKeepsActive DEF Next

THEOREM CompositeInitial == Init /\ DataInit => CompositeStrong
  BY SMT, Parameters, InitialStrong DEF DataInit, CompositeStrong, DataStrong, DataTypeOK,
      DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder, RecoveryState,
      IdleScan, Ids

THEOREM ControlPreservesData ==
  ASSUME CompositeStrong, ControlStep
  PROVE CompositeStrong'
<1>1. Strong' BY NextStrong DEF CompositeStrong, ControlStep
<1>2. \A p \in Ids: roots[p].phase = "active" => roots'[p].phase = "active"
  BY ActivePhasePersists DEF CompositeStrong, ControlStep
<1>3. DataStrong'
  BY SMT, <1>2 DEF CompositeStrong, ControlStep, dataVars, DataStrong, DataTypeOK, DurableKnowledge,
      SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder, RecoveryState, IdleScan, Ids
<1> QED BY <1>1, <1>3 DEF CompositeStrong

THEOREM PrepareBatchPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, PrepareBatch(p)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF PrepareBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>2. DataStrong'
  <2>1. DataTypeOK'
    <3>before. planned \in [Ids -> Nat] /\ planned[p] \in Nat
      BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
    <3>value. planned[p] + 1 \in Nat BY SMT, <3>before
    <3>planned. planned' \in [Ids -> Nat]
      BY SMT, UpdateRange, <3>before, <3>value DEF PrepareBatch
    <3> QED BY <3>planned DEF CompositeStrong, DataStrong, DataTypeOK, PrepareBatch, vars
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, PrepareBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM UploadBatchPreserves ==
  ASSUME NEW p \in Ids, NEW n \in 1..planned[p], CompositeStrong, UploadBatch(p, n)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF UploadBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>updated. objects' = [objects EXCEPT ![p] = @ \cup {n}]
  BY DEF UploadBatch
<1>before. objects \in [Ids -> SUBSET Nat]
           /\ \A q \in Ids: objects[q] \subseteq 1..planned[q]
  BY SMT DEF CompositeStrong, DataStrong, DataTypeOK, DurableKnowledge
<1>growth. \A q \in Ids: objects[q] \subseteq objects'[q]
                       /\ objects'[q] \subseteq 1..planned[q]
  <2>1. ASSUME NEW q \in Ids
        PROVE objects[q] \subseteq objects'[q] /\ objects'[q] \subseteq 1..planned[q]
    <3>1. CASE q = p
      BY SMT, <3>1, <1>updated, <1>before
    <3>2. CASE q # p
      BY SMT, <3>2, <1>updated, <1>before
    <3> QED BY <3>1, <3>2
  <2> QED BY <2>1
<1>domains. \A q \in Ids: scans[q].epoch = 0 \/ scans[q].epoch \in Ids
  <2>1. ASSUME NEW q \in Ids
        PROVE scans[q].epoch = 0 \/ scans[q].epoch \in Ids
    <3>1. scans[q] \in [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]
      BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
    <3>2. scans[q].epoch \in 0..Epochs BY SMT, <3>1
    <3> QED BY EpochDomain, <3>2
  <2> QED BY <2>1
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMT, Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong, UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMTT(15), Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong,
        UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong, UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong, UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong, UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong, UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMTT(15), Parameters, JournalParameters, <1>growth, <1>domains DEF vars, CompositeStrong,
        UploadBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ObserveBatchPreserves ==
  ASSUME NEW p \in Ids, NEW n \in 1..planned[p], CompositeStrong, ObserveBatch(p, n)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ObserveBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>range. planned[p] \in Nat /\ known[p] \subseteq Nat
  BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
<1>number. n \in Int /\ n >= 1 BY SMT, <1>range
<1>type. n \in Nat /\ known[p] \cup {n} \subseteq Nat
  BY SMT, <1>range, <1>number
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMTT(15), Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters, <1>type DEF vars, CompositeStrong, ObserveBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ConsumeBatchPreserves ==
  ASSUME NEW p \in Ids, NEW n \in 1..planned[p], CompositeStrong, ConsumeBatch(p, n)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ConsumeBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>type. consumed[p] \in Nat /\ n = consumed[p] + 1
  BY SMT, Parameters, JournalParameters DEF CompositeStrong, DataStrong,
       DataTypeOK, ConsumeBatch, Ids
<1>known. n \in objects[p] /\ 1..consumed[p] \subseteq objects[p]
  BY SMT DEF CompositeStrong, DataStrong, DurableKnowledge, ConsumeBatch
<1>prefix. 1..n \subseteq objects[p]
  BY SMT, PrefixExtension, <1>type, <1>known
<1>counters. acknowledged[p] \in Nat /\ consumed[p] \in Nat /\ planned[p] \in Nat
  BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
<1>before. acknowledged[p] <= consumed[p]
  BY SMT DEF CompositeStrong, DataStrong, DataOrder
<1>bounds. acknowledged[p] <= n /\ n <= planned[p]
  BY SMT, <1>counters, <1>before, <1>type
<1>write. consumed' = [consumed EXCEPT ![p] = n]
  BY DEF ConsumeBatch
<1>orders. \A q \in Ids: acknowledged[q] <= consumed'[q] /\ consumed'[q] <= planned[q]
  <2>1. ASSUME NEW q \in Ids
        PROVE acknowledged[q] <= consumed'[q] /\ consumed'[q] <= planned[q]
    <3>1. CASE q = p
      BY SMT, <3>1, <1>write, <1>bounds DEF CompositeStrong, DataStrong, DataTypeOK
    <3>2. CASE q # p
      BY SMT, <3>2, <1>write DEF CompositeStrong, DataStrong, DataTypeOK, DataOrder
    <3> QED BY <3>1, <3>2
  <2> QED BY <2>1
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMTT(15), Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMTT(15), Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, <1>orders DEF CompositeStrong, DataStrong, DataOrder, ConsumeBatch, vars
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters, <1>type, <1>known, <1>prefix, <1>bounds DEF vars,
        CompositeStrong, ConsumeBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM AcknowledgeBatchPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, AcknowledgeBatch(p)
  PROVE CompositeStrong'
<1>1. Strong' BY DEF CompositeStrong, AcknowledgeBatch, vars, Strong,
       TypeOK, ClockOrder, RootState, FenceBounds, ChildState
<1>2. Strong /\ RootValid(p)
  BY SMT, JournalParameters DEF CompositeStrong, AcknowledgeBatch
<1>later. \A q \in Ids: p < q => roots[q].phase # "active"
  <2>1. ASSUME NEW q \in Ids, p < q, roots[q].phase = "active"
        PROVE FALSE
    BY EarlierInactive, <1>2, <2>1
  <2> QED BY <2>1
<1>3. cuts[p] = -1
  <2>1. cuts[p] \in Int /\ cuts[p] >= -1
    BY SMT, Parameters DEF CompositeStrong, DataStrong, DataOrder, DataTypeOK, Ids
  <2>2. cuts[p] >= 0 => \E q \in Ids: p < q /\ roots[q].phase = "active"
    BY SMT DEF CompositeStrong, DataStrong, DataOrder
  <2>3. cuts[p] < 0 BY SMT, <2>1, <2>2, <1>later
  <2> QED BY SMT, <2>1, <2>3
<1>4. \A q \in Ids: scans[q].epoch # p
  <2>1. ASSUME NEW q \in Ids, scans[q].epoch = p
        PROVE FALSE
    <3>1. p > 0 BY SMT, Parameters DEF Ids
    <3>2. p < q /\ roots[q].phase = "active"
      BY SMT, <2>1, <3>1 DEF CompositeStrong, DataStrong, RecoveryState
    <3> QED BY SMT, <1>later, <3>2
  <2> QED BY <2>1
<1>5. DataTypeOK'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>6. DurableKnowledge'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>7. SelectedHistory'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>8. ImmutableSelection'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>9. CompleteHistory'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>10. DataOrder'
  BY SMT, Parameters, JournalParameters, <1>3, <1>4
    DEF CompositeStrong, DataStrong, AcknowledgeBatch, vars,
        DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder,
            RecoveryState, IdleScan, Ids, RootValid
<1>11. RecoveryState'
  <2>1. ASSUME NEW q \in Ids, scans[q].epoch # 0
        PROVE acknowledged'[scans[q].epoch] = acknowledged[scans[q].epoch]
    <3>range. scans[q].epoch \in 0..Epochs
      BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
    <3>domain. scans[q].epoch \in Ids
      BY SMT, EpochDomain, <2>1, <3>range
    <3> QED BY SMT, <1>4, <2>1, <3>domain
      DEF CompositeStrong, DataStrong, DataTypeOK, AcknowledgeBatch
  <2> QED BY SMT, <2>1
    DEF CompositeStrong, DataStrong, RecoveryState, AcknowledgeBatch, vars
<1> QED BY <1>1, <1>5, <1>6, <1>7, <1>8, <1>9, <1>10, <1>11 DEF CompositeStrong, DataStrong

THEOREM BeginRecoveryPreserves ==
  ASSUME NEW p \in Ids, NEW e \in Ids, CompositeStrong, BeginRecovery(p, e)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF BeginRecovery
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, BeginRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ReadNextBatchPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, ReadNextBatch(p)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ReadNextBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>scanType. scans[p] \in [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]
  BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
<1>nonzero. scans[p].epoch # 0 BY DEF ReadNextBatch
<1>domain. scans[p].epoch \in Ids /\ scans[p].position \in Nat
  <2>1. scans[p].epoch \in 0..Epochs /\ scans[p].position \in Nat
    BY SMT, <1>scanType
  <2> QED BY SMT, EpochDomain, <2>1, <1>nonzero

<1>old. 1..scans[p].position \subseteq objects[scans[p].epoch]
  BY SMT, <1>domain DEF CompositeStrong, DataStrong, RecoveryState, Ids
<1>next. scans[p].position + 1 \in objects[scans[p].epoch]
  BY DEF ReadNextBatch
<1>prefix. 1..(scans[p].position + 1) \subseteq objects[scans[p].epoch]
  BY SMT, PrefixExtension, <1>domain, <1>old, <1>next
<1>bound. scans[p].position + 1 <= planned[scans[p].epoch]
  BY SMT, <1>domain, <1>next DEF CompositeStrong, DataStrong, DurableKnowledge
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF vars,
        CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMTT(15), Parameters, JournalParameters, <1>domain, <1>old, <1>next, <1>prefix, <1>bound DEF
        vars, CompositeStrong, ReadNextBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ReadMissingBatchPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, ReadMissingBatch(p)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ReadMissingBatch
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>domain. scans[p].epoch \in Ids /\ scans[p].position \in Nat
  BY SMT, Parameters DEF CompositeStrong, DataStrong, DataTypeOK, ReadMissingBatch, Ids
<1>type. acknowledged[scans[p].epoch] \in Nat
  BY SMT, <1>domain DEF CompositeStrong, DataStrong, DataTypeOK
<1>prefix. 1..acknowledged[scans[p].epoch] \subseteq objects[scans[p].epoch]
  BY SMT, <1>domain DEF CompositeStrong, DataStrong, DurableKnowledge
<1>missing. scans[p].position + 1 \notin objects[scans[p].epoch]
  BY SMT, JournalParameters DEF ReadMissingBatch
<1>bound. acknowledged[scans[p].epoch] <= scans[p].position
  BY SMT, MissingBoundsPrefix, <1>domain, <1>type, <1>prefix, <1>missing
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF vars,
        CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMTT(15), Parameters, JournalParameters, <1>domain, <1>type, <1>prefix, <1>missing, <1>bound DEF
        vars, CompositeStrong, ReadMissingBatch,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ForgetRecoveryPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, ForgetRecovery(p)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ForgetRecovery
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>2. DataStrong'
  <2>1. DataTypeOK'
    <3>before. scans \in [Ids -> [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]]
      BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
    <3>value. IdleScan \in [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]
      BY SMT, Parameters DEF IdleScan
    <3>scans. scans' \in [Ids -> [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]]
      BY SMT, UpdateRange, <3>before, <3>value DEF ForgetRecovery
    <3> QED BY <3>scans DEF CompositeStrong, DataStrong, DataTypeOK, ForgetRecovery, vars
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ForgetRecovery,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM SelectPrefixPreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, SelectPrefix(p)
  PROVE CompositeStrong'
<1>frame. authority' = [authority EXCEPT !.revision = @ + 1]
          /\ UNCHANGED <<now, roots, children>>
  BY DEF SelectPrefix
<1>owner. authority'.owner = authority.owner
  BY SMT, <1>frame DEF CompositeStrong, Strong, TypeOK
<1>1. Strong'
  <2>1. TypeOK'
    BY SMT, Parameters, <1>frame DEF CompositeStrong, Strong, TypeOK, Ids
  <2>2. ClockOrder' BY <1>frame DEF CompositeStrong, Strong, ClockOrder
  <2>3. RootState' BY <1>frame, <1>owner DEF CompositeStrong, Strong, RootState
  <2>4. FenceBounds' BY <1>frame DEF CompositeStrong, Strong, FenceBounds
  <2>5. ChildState' BY <1>frame DEF CompositeStrong, Strong, ChildState
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5 DEF Strong
<1>finished. scans[p].finished BY DEF SelectPrefix
<1>nonzero. scans[p].epoch # 0
  BY SMT, <1>finished DEF CompositeStrong, DataStrong, RecoveryState, IdleScan
<1>scanType. scans[p] \in [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]
  BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
<1>domain. scans[p].epoch \in Ids /\ scans[p].position \in Nat
  <2>1. scans[p].epoch \in 0..Epochs /\ scans[p].position \in Nat
    BY SMT, <1>scanType
  <2> QED BY SMT, EpochDomain, <2>1, <1>nonzero

<1>idle. IdleScan \in [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]
  BY SMT, Parameters DEF IdleScan
<1>2. DataStrong'
  <2>1. DataTypeOK'
    <3>before. cuts \in [Ids -> Int] /\ originalCuts \in [Ids -> Int]
               /\ scans \in [Ids -> [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]]
      BY SMT DEF CompositeStrong, DataStrong, DataTypeOK
    <3>value. scans[p].position \in Int
      BY SMT, <1>domain
    <3>old. originalCuts[scans[p].epoch] \in Int
      BY SMT, <3>before, <1>domain
    <3>chosen. (IF originalCuts[scans[p].epoch] = -1 THEN scans[p].position
                ELSE originalCuts[scans[p].epoch]) \in Int
      BY SMT, <3>old, <3>value
    <3>cuts. cuts' \in [Ids -> Int]
      BY SMT, UpdateRange, <3>before, <3>value, <1>domain DEF SelectPrefix
    <3>original. originalCuts' \in [Ids -> Int]
      BY SMT, UpdateRange, <3>before, <3>chosen, <1>domain DEF SelectPrefix
    <3>scans. scans' \in [Ids -> [epoch : 0..Epochs, position : Nat, finished : BOOLEAN]]
      BY SMT, UpdateRange, <3>before, <1>idle DEF SelectPrefix
    <3> QED BY <3>cuts, <3>original, <3>scans
      DEF CompositeStrong, DataStrong, DataTypeOK, SelectPrefix, vars
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters, <1>finished, <1>nonzero, <1>domain DEF vars, CompositeStrong,
        SelectPrefix,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM ReadyToServePreserves ==
  ASSUME NEW p \in Ids, CompositeStrong, ReadyToServe(p)
  PROVE CompositeStrong'
<1>1. Strong'
  <2>1. UNCHANGED vars BY DEF ReadyToServe
  <2> QED BY StutteringStrong, <2>1 DEF CompositeStrong
<1>2. DataStrong'
  <2>1. DataTypeOK'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>2. DurableKnowledge'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>3. SelectedHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>4. ImmutableSelection'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>5. CompleteHistory'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>6. DataOrder'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2>7. RecoveryState'
    BY SMT, Parameters, JournalParameters DEF vars, CompositeStrong, ReadyToServe,
       DataStrong, DataTypeOK, DurableKnowledge, SelectedHistory, ImmutableSelection, CompleteHistory,
           DataOrder, RecoveryState, IdleScan, Ids, RootValid
  <2> QED BY <2>1, <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF DataStrong
<1> QED BY <1>1, <1>2 DEF CompositeStrong

THEOREM CompositeNext == CompositeStrong /\ CombinedNext => CompositeStrong'
  BY ControlPreservesData, PrepareBatchPreserves, UploadBatchPreserves, ObserveBatchPreserves,
      ConsumeBatchPreserves, AcknowledgeBatchPreserves, BeginRecoveryPreserves, ReadNextBatchPreserves,
      ReadMissingBatchPreserves, ForgetRecoveryPreserves, SelectPrefixPreserves, ReadyToServePreserves
      DEF CombinedNext

THEOREM CompositeStutter == CompositeStrong /\ UNCHANGED allVars => CompositeStrong'
  BY DEF allVars, vars, dataVars, CompositeStrong, DataStrong, DataTypeOK, DurableKnowledge,
      SelectedHistory, ImmutableSelection, CompleteHistory, DataOrder, RecoveryState, IdleScan, Ids,
      Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids

THEOREM CompositeSafety ==
  ASSUME CompositeStrong
  PROVE CombinedInvariant
<1>1. Invariant BY StrongSafety DEF CompositeStrong, Invariant, Strong
<1>2. ServingHistory
  <2>1. ASSUME NEW p \in Ids, ready[p], RootValid(p)
        PROVE \A e \in Ids: acknowledged[e] <=
          IF e < p THEN views[p][e] ELSE IF e = p THEN consumed[p] ELSE 0
    <3>1. ASSUME NEW e \in Ids
          PROVE acknowledged[e] <=
            IF e < p THEN views[p][e] ELSE IF e = p THEN consumed[p] ELSE 0
      <4>types. p \in Nat /\ e \in Nat /\ p >= 1 /\ e >= 1 /\ acknowledged[e] \in Nat
        BY SMT, Parameters DEF CompositeStrong, DataStrong, DataTypeOK, Ids
      <4>1. CASE e < p
        <5>1. views[p][e] = cuts[e] /\ cuts[e] >= 0
          BY SMT, <2>1, <4>1, <4>types DEF CompositeStrong, DataStrong, CompleteHistory
        <5>2. acknowledged[e] <= cuts[e]
          BY SMT, <5>1 DEF CompositeStrong, DataStrong, SelectedHistory
        <5> QED BY SMT, <4>1, <5>1, <5>2
      <4>2. CASE e = p
        BY SMT, <4>2 DEF CompositeStrong, DataStrong, DataOrder
      <4>3. CASE p < e
        <5>1. ASSUME acknowledged[e] > 0 PROVE FALSE
          <6>1. roots[e].phase = "active"
            BY SMT, <5>1 DEF CompositeStrong, DataStrong, DataOrder
          <6> QED BY EarlierInactive, <2>1, <4>3, <6>1 DEF CompositeStrong
        <5>2. acknowledged[e] = 0 BY SMT, <4>types, <5>1
        <5> QED BY SMT, <4>types, <4>3, <5>2
      <4> QED BY SMT, <4>types, <4>1, <4>2, <4>3
    <3> QED BY <3>1
  <2> QED BY <2>1 DEF ServingHistory
<1> QED BY <1>1, <1>2 DEF CombinedInvariant, CompositeStrong, DataStrong

THEOREM RepeatedHistorySafety == CombinedSpec => []CombinedInvariant
<1>1. CombinedSpec => []CompositeStrong
  BY PTL, CompositeInitial, CompositeNext, CompositeStutter DEF CombinedSpec
<1> QED BY PTL, <1>1, CompositeSafety
=============================================================================
