----------------------- MODULE PartitionControlProof -----------------------
EXTENDS PartitionControl, RootControlProof, Integers
ASSUME MovementParameters == /\ Generations \in Nat /\ Generations > 0
       /\ ~UnsafeAssignment /\ ~UnsafeMoveDrain /\ ~UnsafeOwnerReply

THEOREM OwnerComponents ==
  ASSUME NEW o \in Owners
  PROVE o[1] \in Ids /\ o[2] \in Partitions /\ o[3] \in 1..Generations
  BY SMT DEF Owners
THEOREM OwnerArithmetic ==
  ASSUME OwnerStrong, NEW o \in Owners
  PROVE /\ o[1] \in Ids /\ o[2] \in Partitions /\ o[3] \in Nat
        /\ now \in Nat /\ grants[o].expires \in Nat
        /\ grants[o].requested \in Nat /\ grants[o].duration \in Nat
        /\ roots[o[1]].expires \in Nat
        /\ EffectiveGeneration(o[2]) \in Int
<1>1. o[1] \in Ids /\ o[2] \in Partitions /\ o[3] \in 1..Generations
  BY OwnerComponents
<1> QED BY SMT, Parameters, MovementParameters, <1>1
  DEF OwnerStrong, OwnerTypeOK, Strong, TypeOK, EffectiveGeneration, Ids
THEOREM PlacementArithmetic ==
  ASSUME OwnerStrong, NEW d \in Partitions
  PROVE /\ placement[d].generation \in Nat /\ placement[d].drain \in Nat
        /\ placement[d].root \in 0..Epochs
        /\ (placement[d].root > 0 => placement[d].root \in Ids)
        /\ EffectiveGeneration(d) \in Int
  BY SMT, Parameters, MovementParameters
    DEF OwnerStrong, OwnerTypeOK, EffectiveGeneration, Ids
THEOREM RevisionAdvanceStrong ==
  ASSUME Strong, authority' = [authority EXCEPT !.revision = @ + 1],
         UNCHANGED <<now, roots, children>>
  PROVE Strong'
<1>1. authority'.owner = authority.owner BY SMT DEF Strong, TypeOK
<1>2. TypeOK' BY SMT, Parameters DEF Strong, TypeOK, Ids
<1>3. ClockOrder' /\ FenceBounds' /\ ChildState'
  BY DEF Strong, ClockOrder, FenceBounds, ChildState
<1>4. RootState' BY <1>1 DEF Strong, RootState
<1> QED BY <1>2, <1>3, <1>4 DEF Strong
THEOREM StartMoveKeepsGeneration ==
  ASSUME OwnerStrong, NEW p \in Ids, NEW d \in Partitions, StartMove(p, d)
  PROVE \A q \in Partitions: EffectiveGeneration(q)' = EffectiveGeneration(q)
  BY SMT, Parameters, MovementParameters, PlacementArithmetic
    DEF OwnerStrong, OwnerTypeOK, StartMove, EffectiveGeneration
THEOREM FinishMoveDrain ==
  ASSUME OwnerStrong, NEW p \in Ids, NEW d \in Partitions, FinishMove(p, d)
  PROVE now >= placement[d].drain + Lease
<1>1. p \in Nat /\ now \in Nat /\ roots[p].drain \in Nat
      /\ placement[d].root \in Nat /\ placement[d].drain \in Nat
  BY SMT, Parameters, PlacementArithmetic DEF OwnerStrong, Strong, TypeOK, Ids
<1>2. p > placement[d].root => placement[d].drain <= roots[p].drain
  BY SMT DEF FinishMove, RootValid, OwnerStrong, PlacementState
<1>3. now >= roots[p].drain + Lease
  BY SMT DEF FinishMove, RootValid, OwnerStrong, Strong, RootState
<1> QED BY SMT, MovementParameters, Parameters, <1>1, <1>2, <1>3 DEF FinishMove

THEOREM AllGrantsBounded ==
  ASSUME OwnerStrong
  PROVE \A o \in Owners:
    grants[o].expires <= now + Lease
    /\ (grants[o].pending = "reply" =>
          grants[o].requested + grants[o].duration <= now + Lease)
  BY SMT, Parameters, OwnerArithmetic DEF OwnerStrong, Strong, ClockOrder, GrantState
THEOREM FinishMoveAdvancesGeneration ==
  ASSUME OwnerStrong, NEW p \in Ids, NEW d \in Partitions, FinishMove(p, d)
  PROVE \A q \in Partitions:
    EffectiveGeneration(q) <= EffectiveGeneration(q)' /\ EffectiveGeneration(q)' \in Int
  BY SMT, Parameters, MovementParameters, PlacementArithmetic
    DEF FinishMove, OwnerStrong, OwnerTypeOK, EffectiveGeneration

THEOREM InitialOwnersStrong ==
  ASSUME Init, OwnerInit
  PROVE OwnerStrong
<1>1. Strong BY InitialStrong
<1>2. OwnerTypeOK
  BY SMT, Parameters, MovementParameters DEF OwnerInit, OwnerTypeOK
<1>3. GrantState
  BY SMT, OwnerComponents DEF Init, OwnerInit, GrantState
<1>4. PlacementState
  BY SMT, Parameters DEF Init, OwnerInit, PlacementState
<1>5. NoFutureGrants BY SMT DEF OwnerInit, NoFutureGrants
<1>6. FrozenBounds
  BY SMT, Parameters, OwnerComponents DEF OwnerInit, FrozenBounds
<1>7. EndedGenerations BY SMT DEF Init, OwnerInit, EndedGenerations
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

\* Facts the delegated layer needs from root steps, proved once without
\* expanding every placement/grant predicate in every root action.
RootProgress ==
    /\ now' >= now /\ authority'.owner >= authority.owner
    /\ \A p \in Ids:
         /\ roots'[p].expires >= roots[p].expires
         /\ (roots[p].phase = "active" => roots'[p].phase = "active")
         /\ (roots'[p].phase # "fresh" =>
               (roots[p].phase # "fresh" /\ roots'[p].drain = roots[p].drain)
               \/ roots'[p].drain = now)

THEOREM TickProgress ==
  ASSUME Strong, Tick
  PROVE RootProgress
  BY SMT, Parameters DEF Tick, RootProgress, Strong, TypeOK, Ids

THEOREM StartFenceProgress ==
  ASSUME NEW p \in Ids, Strong, StartFence(p)
  PROVE RootProgress
  BY SMT, Parameters DEF StartFence, RootProgress, Strong, TypeOK, Ids

THEOREM ApplyFenceProgress ==
  ASSUME NEW p \in Ids, Strong, ApplyFence(p)
  PROVE RootProgress
  BY SMT, Parameters DEF ApplyFence, RootProgress, Strong, TypeOK, Ids

THEOREM DeliverFenceProgress ==
  ASSUME NEW p \in Ids, Strong, DeliverFence(p)
  PROVE RootProgress
  BY SMT, Parameters DEF DeliverFence, RootProgress, Strong, TypeOK, Ids, RootState, RootReplyEnd

THEOREM StartRenewProgress ==
  ASSUME NEW p \in Ids, Strong, StartRenew(p)
  PROVE RootProgress
  BY SMT, Parameters DEF StartRenew, RootProgress, Strong, TypeOK, Ids

THEOREM ApplyRenewProgress ==
  ASSUME NEW p \in Ids, Strong, ApplyRenew(p)
  PROVE RootProgress
  BY SMT, Parameters DEF ApplyRenew, RootProgress, Strong, TypeOK, Ids

THEOREM DeliverRenewProgress ==
  ASSUME NEW p \in Ids, Strong, DeliverRenew(p)
  PROVE RootProgress
  BY SMT, Parameters DEF DeliverRenew, RootProgress, Strong, TypeOK, Ids, RootReplyEnd, Max

THEOREM StartActivateProgress ==
  ASSUME NEW p \in Ids, Strong, StartActivate(p)
  PROVE RootProgress
  BY SMT, Parameters DEF StartActivate, RootProgress, Strong, TypeOK, Ids

THEOREM ApplyActivateProgress ==
  ASSUME NEW p \in Ids, Strong, ApplyActivate(p)
  PROVE RootProgress
  BY SMT, Parameters DEF ApplyActivate, RootProgress, Strong, TypeOK, Ids

THEOREM DeliverActivateProgress ==
  ASSUME NEW p \in Ids, Strong, DeliverActivate(p)
  PROVE RootProgress
  BY SMT, Parameters DEF DeliverActivate, RootProgress, Strong, TypeOK, Ids, RootState

THEOREM LoseReplyProgress ==
  ASSUME NEW p \in Ids, Strong, LoseReply(p)
  PROVE RootProgress
  BY SMT, Parameters DEF LoseReply, RootProgress, Strong, TypeOK, Ids

THEOREM RequestChildProgress ==
  ASSUME NEW p \in Ids, Strong, RequestChild(p)
  PROVE RootProgress
  BY SMT, Parameters DEF RequestChild, RootProgress, Strong, TypeOK, Ids

THEOREM IssueChildProgress ==
  ASSUME NEW p \in Ids, Strong, IssueChild(p)
  PROVE RootProgress
  BY SMT, Parameters DEF IssueChild, RootProgress, Strong, TypeOK, Ids

THEOREM DeliverChildProgress ==
  ASSUME NEW p \in Ids, Strong, DeliverChild(p)
  PROVE RootProgress
  BY SMT, Parameters DEF DeliverChild, RootProgress, Strong, TypeOK, Ids

THEOREM NextProgress == Strong /\ Next => RootProgress
  BY TickProgress, StartFenceProgress, ApplyFenceProgress, DeliverFenceProgress, StartRenewProgress,
     ApplyRenewProgress, DeliverRenewProgress, StartActivateProgress, ApplyActivateProgress,
     DeliverActivateProgress, LoseReplyProgress, RequestChildProgress, IssueChildProgress,
     DeliverChildProgress DEF Next
THEOREM ControlOwners ==
  ASSUME OwnerStrong, ControlStep
  PROVE OwnerStrong'
<1>1. Strong' BY NextStrong DEF OwnerStrong, ControlStep
<1>2. RootProgress BY NextProgress DEF OwnerStrong, ControlStep
<1>3. OwnerTypeOK' BY DEF OwnerStrong, OwnerTypeOK, ControlStep, ownerVars
<1>4. GrantState'
  BY SMT, <1>1, <1>2, OwnerArithmetic DEF OwnerStrong, GrantState, ControlStep, ownerVars, RootProgress,
     Strong, TypeOK, Ids
<1>5. PlacementState'
  BY SMT, <1>1, <1>2, PlacementArithmetic DEF OwnerStrong, PlacementState, ControlStep, ownerVars,
     RootProgress, Strong, TypeOK, Ids
<1>6. NoFutureGrants' /\ FrozenBounds'
  BY DEF OwnerStrong, NoFutureGrants, FrozenBounds, EffectiveGeneration, ControlStep, ownerVars
<1>7. EndedGenerations'
  BY SMT, <1>1, <1>2, OwnerArithmetic DEF OwnerStrong, EndedGenerations, EffectiveGeneration,
     ControlStep, ownerVars, RootProgress, Strong, TypeOK, Ids
<1> QED BY <1>1, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong
THEOREM RequestOwnerPreserves ==
  ASSUME NEW o \in Owners, OwnerStrong, RequestOwner(o)
  PROVE OwnerStrong'

<1>1. Strong' BY StutteringStrong DEF OwnerStrong, RequestOwner

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters DEF RequestOwner, vars, ownerVars, OwnerStrong, OwnerTypeOK,
     Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>3. GrantState'
  BY SMT, OwnerArithmetic DEF RequestOwner, vars, OwnerStrong, OwnerTypeOK, GrantState

<1>4. PlacementState'
  BY DEF RequestOwner, vars, OwnerStrong, PlacementState

<1>5. NoFutureGrants'
  BY SMT, Parameters, MovementParameters DEF RequestOwner, vars, ownerVars, OwnerStrong, NoFutureGrants,
     EffectiveGeneration, OwnerTypeOK, Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>6. FrozenBounds'
  BY SMT DEF RequestOwner, vars, OwnerStrong, OwnerTypeOK, FrozenBounds, EffectiveGeneration

<1>7. EndedGenerations'
  BY SMT DEF RequestOwner, vars, OwnerStrong, OwnerTypeOK, EndedGenerations, EffectiveGeneration

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM IssueOwnerPreserves ==
  ASSUME NEW o \in Owners, OwnerStrong, IssueOwner(o)
  PROVE OwnerStrong'

<1>1. Strong' BY StutteringStrong DEF OwnerStrong, IssueOwner

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters, OwnerComponents DEF IssueOwner, RootValid, vars, OwnerStrong,
     OwnerTypeOK, Strong, TypeOK, Owners, Ids

<1>3. GrantState'
  BY SMT, OwnerArithmetic DEF IssueOwner, vars, OwnerStrong, OwnerTypeOK, GrantState, RootValid

<1>4. PlacementState'
  BY DEF IssueOwner, vars, OwnerStrong, PlacementState

<1>5. NoFutureGrants'
  BY SMT, Parameters, MovementParameters DEF IssueOwner, vars, ownerVars, OwnerStrong, NoFutureGrants,
     EffectiveGeneration, OwnerTypeOK, Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>6. FrozenBounds'
  BY SMT, MovementParameters, OwnerArithmetic DEF IssueOwner, vars, OwnerStrong, OwnerTypeOK,
     FrozenBounds, EffectiveGeneration

<1>7. EndedGenerations'
  BY SMT, MovementParameters, OwnerArithmetic DEF IssueOwner, vars, OwnerStrong, OwnerTypeOK,
     EndedGenerations, EffectiveGeneration

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM DeliverOwnerPreserves ==
  ASSUME NEW o \in Owners, OwnerStrong, DeliverOwner(o)
  PROVE OwnerStrong'

<1>1. Strong' BY StutteringStrong DEF OwnerStrong, DeliverOwner

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters DEF DeliverOwner, vars, ownerVars, OwnerStrong, OwnerTypeOK,
     Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>3. GrantState'
  <2>1. Max(grants[o].expires, OwnerReplyEnd(o)) <= roots[o[1]].expires
    BY SMT, MovementParameters, OwnerArithmetic
      DEF DeliverOwner, OwnerStrong, GrantState, OwnerReplyEnd, Max
  <2> QED BY SMT, <2>1
    DEF DeliverOwner, vars, OwnerStrong, OwnerTypeOK, GrantState

<1>4. PlacementState'
  BY DEF DeliverOwner, vars, OwnerStrong, PlacementState

<1>5. NoFutureGrants'
  BY SMT, Parameters, MovementParameters DEF DeliverOwner, vars, ownerVars, OwnerStrong, NoFutureGrants,
     EffectiveGeneration, OwnerTypeOK, Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>6. FrozenBounds'
  <2>1. o[3] < placement[o[2]].generation =>
      Max(grants[o].expires, OwnerReplyEnd(o)) <= placement[o[2]].drain + Lease
    BY SMT, Parameters, MovementParameters, OwnerArithmetic, PlacementArithmetic
      DEF DeliverOwner, OwnerStrong, FrozenBounds, OwnerReplyEnd, Max
  <2> QED BY SMT, <2>1
    DEF DeliverOwner, vars, OwnerStrong, OwnerTypeOK, FrozenBounds

<1>7. EndedGenerations'
  <2>1. o[3] < EffectiveGeneration(o[2]) =>
      Max(grants[o].expires, OwnerReplyEnd(o)) <= now
    BY SMT, MovementParameters, OwnerArithmetic
      DEF DeliverOwner, OwnerStrong, EndedGenerations, OwnerReplyEnd, Max
  <2> QED BY SMT, <2>1
    DEF DeliverOwner, vars, OwnerStrong, OwnerTypeOK, EndedGenerations, EffectiveGeneration

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM LoseOwnerReplyPreserves ==
  ASSUME NEW o \in Owners, OwnerStrong, LoseOwnerReply(o)
  PROVE OwnerStrong'

<1>1. Strong' BY StutteringStrong DEF OwnerStrong, LoseOwnerReply

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters DEF LoseOwnerReply, vars, ownerVars, OwnerStrong, OwnerTypeOK,
     Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>3. GrantState'
  BY SMT DEF LoseOwnerReply, vars, OwnerStrong, OwnerTypeOK, GrantState

<1>4. PlacementState'
  BY DEF LoseOwnerReply, vars, OwnerStrong, PlacementState

<1>5. NoFutureGrants'
  BY SMT, Parameters, MovementParameters DEF LoseOwnerReply, vars, ownerVars, OwnerStrong,
     NoFutureGrants, EffectiveGeneration, OwnerTypeOK, Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>6. FrozenBounds'
  BY SMT DEF LoseOwnerReply, vars, OwnerStrong, OwnerTypeOK, FrozenBounds

<1>7. EndedGenerations'
  BY SMT DEF LoseOwnerReply, vars, OwnerStrong, OwnerTypeOK, EndedGenerations, EffectiveGeneration

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM StartMovePreserves ==
  ASSUME NEW p \in Ids, NEW d \in Partitions, OwnerStrong, StartMove(p, d)
  PROVE OwnerStrong'

<1>1. Strong' BY RevisionAdvanceStrong DEF OwnerStrong, StartMove

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters DEF StartMove, vars, ownerVars, OwnerStrong, OwnerTypeOK,
     Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>3. GrantState'
  BY DEF StartMove, OwnerStrong, GrantState

<1>4. PlacementState'
  <2>1. p \in Nat /\ p > 0 /\ roots[p].phase = "active" /\ authority.owner = p
    BY SMT, Parameters DEF StartMove, RootValid, Ids
  <2>2. \A q \in Ids: q > p => roots[q].phase = "fresh"
    BY SMT, Parameters, <2>1 DEF OwnerStrong, Strong, RootState, Ids
  <2>3. now \in Nat /\ authority'.owner = authority.owner
    BY SMT DEF StartMove, OwnerStrong, Strong, TypeOK
  <2>4. \A q \in Partitions: placement'[q].drain <= now'
    BY SMT, Parameters, MovementParameters, <2>1, <2>2, <2>3, PlacementArithmetic
      DEF StartMove, OwnerStrong, OwnerTypeOK, PlacementState
  <2>5. \A q \in Partitions: placement'[q].root <= authority'.owner
    BY SMT, Parameters, MovementParameters, <2>1, <2>2, <2>3, PlacementArithmetic
      DEF StartMove, OwnerStrong, OwnerTypeOK, PlacementState
  <2>6. \A q \in Partitions: placement'[q].generation = 1 =>
      placement'[q].root = 0 /\ placement'[q].phase = "open" /\ placement'[q].drain = 0
    BY SMT, Parameters, MovementParameters, <2>1, <2>2, <2>3, PlacementArithmetic
      DEF StartMove, OwnerStrong, OwnerTypeOK, PlacementState
  <2>7. \A q \in Partitions: placement'[q].generation > 1 =>
      placement'[q].root > 0 /\ roots'[placement'[q].root].phase = "active"
    BY SMT, Parameters, MovementParameters, <2>1, <2>2, <2>3, PlacementArithmetic
      DEF StartMove, OwnerStrong, OwnerTypeOK, PlacementState
  <2>8. \A q \in Partitions, r \in Ids:
      (r > placement'[q].root /\ roots'[r].phase # "fresh") => placement'[q].drain <= roots'[r].drain
    BY SMT, Parameters, MovementParameters, <2>1, <2>2, <2>3, PlacementArithmetic
      DEF StartMove, OwnerStrong, OwnerTypeOK, PlacementState
  <2> QED BY <2>4, <2>5, <2>6, <2>7, <2>8 DEF PlacementState

<1>5. NoFutureGrants'
  <2>1. \A q \in Partitions: EffectiveGeneration(q)' = EffectiveGeneration(q)
    BY StartMoveKeepsGeneration
  <2> QED BY SMT, <2>1, OwnerComponents DEF StartMove, OwnerStrong, NoFutureGrants

<1>6. FrozenBounds'
  BY SMT, AllGrantsBounded, OwnerComponents DEF StartMove, OwnerStrong, OwnerTypeOK, FrozenBounds

<1>7. EndedGenerations'
  <2>1. \A q \in Partitions: EffectiveGeneration(q)' = EffectiveGeneration(q)
    BY StartMoveKeepsGeneration
  <2> QED BY SMT, <2>1, OwnerComponents DEF StartMove, OwnerStrong, EndedGenerations

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM FinishMovePreserves ==
  ASSUME NEW p \in Ids, NEW d \in Partitions, OwnerStrong, FinishMove(p, d)
  PROVE OwnerStrong'

<1>1. Strong' BY RevisionAdvanceStrong DEF OwnerStrong, FinishMove

<1>2. OwnerTypeOK'
  BY SMT, Parameters, MovementParameters DEF FinishMove, vars, ownerVars, OwnerStrong, OwnerTypeOK,
     Strong, TypeOK, Owners, Ids, Max, OwnerReplyEnd

<1>3. GrantState'
  BY DEF FinishMove, OwnerStrong, GrantState

<1>4. PlacementState'
  BY SMT DEF FinishMove, OwnerStrong, PlacementState, OwnerTypeOK, Strong, TypeOK

<1>5. NoFutureGrants'
  <2>1. \A q \in Partitions:
      EffectiveGeneration(q) <= EffectiveGeneration(q)' /\ EffectiveGeneration(q)' \in Int
    BY FinishMoveAdvancesGeneration
  <2> QED BY SMT, <2>1, OwnerArithmetic
    DEF FinishMove, OwnerStrong, NoFutureGrants

<1>6. FrozenBounds'
  BY SMT, OwnerComponents DEF FinishMove, OwnerStrong, OwnerTypeOK, FrozenBounds

<1>7. EndedGenerations'
  <2>1. now >= placement[d].drain + Lease BY FinishMoveDrain
  <2>2. \A o \in Owners: (o[2] = d /\ o[3] < placement[d].generation) =>
      grants[o].expires <= now
      /\ (grants[o].pending = "reply" => grants[o].requested + grants[o].duration <= now)
    BY SMT, Parameters, <2>1, OwnerArithmetic, PlacementArithmetic
      DEF OwnerStrong, OwnerTypeOK, FrozenBounds
  <2> QED BY SMT, <2>2, OwnerComponents
    DEF FinishMove, OwnerStrong, OwnerTypeOK, EndedGenerations, EffectiveGeneration

<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7 DEF OwnerStrong

THEOREM PartitionNextStrong == OwnerStrong /\ PartitionNext => OwnerStrong'
  BY ControlOwners, RequestOwnerPreserves, IssueOwnerPreserves, DeliverOwnerPreserves,
     LoseOwnerReplyPreserves, StartMovePreserves, FinishMovePreserves DEF PartitionNext
THEOREM StutteringOwners == OwnerStrong /\ UNCHANGED allVars => OwnerStrong'
  BY DEF allVars, vars, ownerVars, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids,
     Max, RootValid, RootReplyEnd, ChildReplyEnd, OwnerStrong, OwnerTypeOK, GrantState, PlacementState,
     NoFutureGrants, FrozenBounds, EndedGenerations, EffectiveGeneration, Owners, OwnerReplyEnd
THEOREM OwnerSafety ==
  ASSUME OwnerStrong
  PROVE OwnerWithinRoot /\ PartitionExclusive
<1>1. \A o \in Owners: OwnerValid(o) => RootValid(o[1])
  <2>1. ASSUME NEW o \in Owners, OwnerValid(o)
        PROVE RootValid(o[1])
    <3>1. now \in Nat /\ grants[o].expires \in Nat /\ roots[o[1]].expires \in Nat
      BY OwnerArithmetic
    <3>2. grants[o].expires > 0 BY SMT, <3>1, <2>1 DEF OwnerValid
    <3> QED BY SMT, <3>1, <3>2, <2>1 DEF OwnerStrong, GrantState, OwnerValid, RootValid
  <2> QED BY <2>1
<1>2. \A o \in Owners: OwnerValid(o) => o[3] = EffectiveGeneration(o[2])
  <2>1. ASSUME NEW o \in Owners, OwnerValid(o)
        PROVE o[3] = EffectiveGeneration(o[2])
    <3>1. now \in Nat /\ grants[o].expires \in Nat /\ o[3] \in Nat
          /\ EffectiveGeneration(o[2]) \in Int
      BY OwnerArithmetic
    <3>2. grants[o].expires > 0 BY SMT, <3>1, <2>1 DEF OwnerValid
    <3>3. o[3] <= EffectiveGeneration(o[2])
      BY SMT, <3>2 DEF OwnerStrong, NoFutureGrants
    <3>4. ~(o[3] < EffectiveGeneration(o[2]))
      BY SMT, <3>1, <2>1 DEF OwnerStrong, EndedGenerations, OwnerValid
    <3> QED BY SMT, <3>1, <3>3, <3>4
  <2> QED BY <2>1
<1> QED BY SMT, <1>1, <1>2 DEF OwnerWithinRoot, PartitionExclusive
THEOREM PartitionSafety == PartitionSpec => []PartitionInvariant
<1>1. PartitionSpec => []OwnerStrong
  BY PTL, InitialOwnersStrong, PartitionNextStrong, StutteringOwners DEF PartitionSpec
<1>2. OwnerStrong => PartitionInvariant
  BY OwnerSafety, StrongSafety DEF OwnerStrong, PartitionInvariant
<1> QED BY PTL, <1>1, <1>2
=============================================================================
