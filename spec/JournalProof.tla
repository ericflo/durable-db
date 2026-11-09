--------------------------- MODULE JournalProof ---------------------------
EXTENDS Journal, TLAPS

THEOREM PrefixExtension ==
  ASSUME NEW objects, NEW n \in Nat, 1..n \subseteq objects, n + 1 \in objects
  PROVE 1..(n + 1) \subseteq objects
<1>1. ASSUME NEW i \in 1..(n + 1)
      PROVE i \in objects
  <2>1. i \in 1..n \/ i = n + 1 BY SMT
  <2> QED BY SMT, <2>1
<1> QED BY SMT, <1>1

THEOREM PrefixRestriction ==
  ASSUME NEW objects, NEW a \in Nat, NEW b \in Nat, a <= b, 1..b \subseteq objects
  PROVE 1..a \subseteq objects
  BY SMT

THEOREM MissingBoundsPrefix ==
  ASSUME NEW objects, NEW a \in Nat, NEW b \in Nat, 1..a \subseteq objects, b + 1 \notin objects
  PROVE a <= b
<1>1. ASSUME a > b
      PROVE FALSE
  <2>1. b + 1 \in Nat /\ 1 <= b + 1 /\ b + 1 <= a BY SMT, <1>1
  <2>2. b + 1 \in 1..a BY SMT, <2>1
  <2> QED BY SMT, <2>2
<1> QED BY SMT, <1>1

THEOREM InitialInvariant == Init => Invariant
  BY SMT DEF Init, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM PreparePreserves ==
  ASSUME Invariant, Prepare
  PROVE Invariant'
  BY SMT DEF Prepare, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM UploadPreserves ==
  ASSUME NEW n, Invariant, Upload(n)
  PROVE Invariant'
  BY SMT DEF Upload, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ObservePreserves ==
  ASSUME NEW n, Invariant, Observe(n, TRUE)
  PROVE Invariant'
  BY SMT DEF Observe, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ConsumePreserves ==
  ASSUME NEW n, Invariant, Consume(n, TRUE)
  PROVE Invariant'
  BY SMT, PrefixExtension DEF Consume, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM AcknowledgePreserves ==
  ASSUME NEW n, Invariant, Acknowledge(n, TRUE)
  PROVE Invariant'
  BY SMT, PrefixRestriction DEF Acknowledge, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM FencePreserves ==
  ASSUME Invariant, Fence
  PROVE Invariant'
  BY SMT DEF Fence, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM DrainPreserves ==
  ASSUME Invariant, Drain
  PROVE Invariant'
  BY SMT DEF Drain, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM BeginScanPreserves ==
  ASSUME Invariant, BeginScan(FALSE)
  PROVE Invariant'
  BY SMT DEF BeginScan, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ReadNextPreserves ==
  ASSUME Invariant, ReadNext
  PROVE Invariant'
  BY SMT, PrefixExtension DEF ReadNext, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ReadMissingPreserves ==
  ASSUME Invariant, ReadMissing(TRUE)
  PROVE Invariant'
  BY SMT, MissingBoundsPrefix DEF ReadMissing, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM SealPreserves ==
  ASSUME Invariant, Seal
  PROVE Invariant'
  BY SMT DEF Seal, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ActivatePreserves ==
  ASSUME Invariant, Activate
  PROVE Invariant'
  BY SMT DEF Activate, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM ReopenPreserves ==
  ASSUME Invariant, Reopen(FALSE)
  PROVE Invariant'
  BY SMT DEF Reopen, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM CrashOldPreserves ==
  ASSUME Invariant, CrashOld
  PROVE Invariant'
  BY SMT DEF CrashOld, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM CrashNewPreserves ==
  ASSUME Invariant, CrashNew
  PROVE Invariant'
  BY SMT DEF CrashNew, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM RestartNewPreserves ==
  ASSUME Invariant, RestartNew
  PROVE Invariant'
  BY SMT DEF RestartNew, EmptyScan, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM NextInvariant == Invariant /\ Next => Invariant'
  BY PreparePreserves,
     UploadPreserves,
     ObservePreserves,
     ConsumePreserves,
     AcknowledgePreserves,
     FencePreserves,
     DrainPreserves,
     BeginScanPreserves,
     ReadNextPreserves,
     ReadMissingPreserves,
     SealPreserves,
     ActivatePreserves,
     ReopenPreserves,
     CrashOldPreserves,
     CrashNewPreserves,
     RestartNewPreserves DEF Next, Steps

THEOREM StutteringInvariant == Invariant /\ UNCHANGED vars => Invariant'
  BY DEF vars, Invariant, TypeOK, KnownDurable, ConsumedDurable, AcknowledgedDurable,
             ScannedDurable, FinishedCoversAcknowledgements, SelectedDurable,
             AcknowledgedPrefixPreserved, SealedPrefixImmutable, SealedPhase, ScanState

THEOREM Safety == Spec => [](ConsumedDurable /\ AcknowledgedPrefixPreserved /\ SealedPrefixImmutable)
<1>1. Spec => []Invariant
  BY PTL, InitialInvariant, NextInvariant, StutteringInvariant DEF Spec
<1>2. Invariant => ConsumedDurable /\ AcknowledgedPrefixPreserved /\ SealedPrefixImmutable
  BY DEF Invariant
<1> QED BY PTL, <1>1, <1>2
=============================================================================
