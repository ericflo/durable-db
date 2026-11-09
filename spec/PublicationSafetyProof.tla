-------------------- MODULE PublicationSafetyProof --------------------
EXTENDS PublicationSafety, TLAPS

THEOREM InitialInvariant == Init => Invariant
  BY SMT DEF Init, Invariant, TypeOK, EmptyJob, JobType, LiveExists,
             Allocated, Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM PreparePreserves ==
  ASSUME NEW j, NEW keep, NEW new, Invariant, Prepare(j, keep, new)
  PROVE Invariant'
  BY SMT DEF Prepare, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM UploadPreserves ==
  ASSUME NEW j, Invariant, Upload(j)
  PROVE Invariant'
  BY SMT DEF Upload, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM PublishPreserves ==
  ASSUME NEW j, NEW retained, Invariant, Publish(j, retained, TRUE)
  PROVE Invariant'
  BY SMT DEF Publish, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM ListPreserves ==
  ASSUME NEW c, NEW names, Invariant, List(c, names)
  PROVE Invariant'
  BY SMT DEF List, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM FencePreserves ==
  ASSUME NEW c, Invariant, Fence(c, TRUE)
  PROVE Invariant'
  BY SMT DEF Fence, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM DeletePreserves ==
  ASSUME NEW c, Invariant, Delete(c)
  PROVE Invariant'
  BY SMT DEF Delete, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM FinishPreserves ==
  ASSUME NEW c, Invariant, Finish(c)
  PROVE Invariant'
  BY SMT DEF Finish, Invariant, TypeOK, JobType, LiveExists, Allocated,
             Observed, LiveProtected, PendingProtected, Uploaded, Valid

THEOREM NextInvariant == Invariant /\ Next => Invariant'
  BY PreparePreserves, UploadPreserves, PublishPreserves, ListPreserves,
     FencePreserves, DeletePreserves, FinishPreserves DEF Next, Steps

THEOREM StutteringInvariant == Invariant /\ UNCHANGED vars => Invariant'
  BY DEF vars, Invariant, TypeOK, JobType, LiveExists, Allocated, Observed,
         LiveProtected, PendingProtected, Uploaded, Valid

THEOREM Safety == Spec => []LiveExists
<1>1. Spec => []Invariant
  BY PTL, InitialInvariant, NextInvariant, StutteringInvariant DEF Spec
<1>2. Invariant => LiveExists
  BY DEF Invariant
<1> QED BY PTL, <1>1, <1>2
=============================================================================
