------------------------- MODULE RootControlProof -------------------------
EXTENDS RootControl, TLAPS
ASSUME Parameters == /\ Epochs \in Nat /\ Lease \in Nat /\ Lease > 0
       /\ ~UnsafeLateRoot /\ ~UnsafeStaleRenewal /\ ~UnsafeSkipDrain
       /\ ~UnsafeLongChild /\ ~UnsafeLateChild

THEOREM InitialStrong == Init => Strong
  BY SMT, Parameters DEF
       Init, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM TickPreserves == Strong /\ Tick => Strong'
  BY SMT, Parameters DEF
       Tick, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM StartFencePreserves ==
  ASSUME NEW p \in Ids, Strong, StartFence(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       StartFence, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM ApplyFencePreserves ==
  ASSUME NEW p \in Ids, Strong, ApplyFence(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       ApplyFence, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM DeliverFencePreserves ==
  ASSUME NEW p \in Ids, Strong, DeliverFence(p)
  PROVE Strong'
<1>1. TypeOK'
  BY SMT, Parameters DEF DeliverFence, Strong, TypeOK, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>2. ClockOrder'
  BY SMT, Parameters DEF
       DeliverFence, Strong, TypeOK, ClockOrder, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>3. RootState'
  BY SMT, Parameters DEF
       DeliverFence, Strong, TypeOK, RootState, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>4. FenceBounds'
  BY SMT, Parameters DEF
       DeliverFence, Strong, TypeOK, ClockOrder, RootState, FenceBounds, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd
<1>5. ChildState'
  BY SMT, Parameters DEF
       DeliverFence, Strong, TypeOK, ClockOrder, RootState, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5 DEF Strong

THEOREM StartRenewPreserves ==
  ASSUME NEW p \in Ids, Strong, StartRenew(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       StartRenew, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM ApplyRenewPreserves ==
  ASSUME NEW p \in Ids, Strong, ApplyRenew(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       ApplyRenew, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM DeliverRenewPreserves ==
  ASSUME NEW p \in Ids, Strong, DeliverRenew(p)
  PROVE Strong'
<1>1. TypeOK'
  BY SMT, Parameters DEF DeliverRenew, Strong, TypeOK, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>2. ClockOrder'
  BY SMT, Parameters DEF
       DeliverRenew, Strong, TypeOK, ClockOrder, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>3. RootState'
  BY SMT, Parameters DEF
       DeliverRenew, Strong, TypeOK, RootState, Ids, Max, RootValid, RootReplyEnd, ChildReplyEnd
<1>4. FenceBounds'
  <2>1. \A q \in Ids:
      (p < q /\ roots[q].phase # "fresh") =>
         Max(roots[p].expires, RootReplyEnd(p)) <= roots[q].drain + Lease
    <3>1. roots[p].pending = "reply-renew" BY DEF DeliverRenew
    <3>2. ASSUME NEW q \in Ids, p < q, roots[q].phase # "fresh"
          PROVE Max(roots[p].expires, RootReplyEnd(p)) <= roots[q].drain + Lease
      <4>1. roots[p].expires \in Nat /\ roots[p].requested \in Nat /\ roots[q].drain \in Nat
        BY SMT, Parameters DEF Strong, TypeOK, Ids
      <4>2. roots[p].expires <= roots[q].drain + Lease /\ roots[p].requested <= roots[q].drain
        BY SMT, <3>1, <3>2 DEF Strong, FenceBounds
      <4> QED BY SMT, Parameters, <4>1, <4>2 DEF Max, RootReplyEnd
    <3> QED BY <3>2
  <2> QED BY SMT, Parameters, <2>1
    DEF DeliverRenew, Strong, TypeOK, FenceBounds, Ids, Max, RootReplyEnd

<1>5. ChildState'
  BY SMT, Parameters DEF
       DeliverRenew, Strong, TypeOK, ClockOrder, RootState, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5 DEF Strong

THEOREM StartActivatePreserves ==
  ASSUME NEW p \in Ids, Strong, StartActivate(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       StartActivate, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max,
       RootValid, RootReplyEnd, ChildReplyEnd

THEOREM ApplyActivatePreserves ==
  ASSUME NEW p \in Ids, Strong, ApplyActivate(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       ApplyActivate, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max,
       RootValid, RootReplyEnd, ChildReplyEnd

THEOREM DeliverActivatePreserves ==
  ASSUME NEW p \in Ids, Strong, DeliverActivate(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       DeliverActivate, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max,
       RootValid, RootReplyEnd, ChildReplyEnd

THEOREM LoseReplyPreserves ==
  ASSUME NEW p \in Ids, Strong, LoseReply(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       LoseReply, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM RequestChildPreserves ==
  ASSUME NEW p \in Ids, Strong, RequestChild(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       RequestChild, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM IssueChildPreserves ==
  ASSUME NEW p \in Ids, Strong, IssueChild(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       IssueChild, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM DeliverChildPreserves ==
  ASSUME NEW p \in Ids, Strong, DeliverChild(p)
  PROVE Strong'
  BY SMT, Parameters DEF
       DeliverChild, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM NextStrong == Strong /\ Next => Strong'
  BY TickPreserves,
     StartFencePreserves,
     ApplyFencePreserves,
     DeliverFencePreserves,
     StartRenewPreserves,
     ApplyRenewPreserves,
     DeliverRenewPreserves,
     StartActivatePreserves,
     ApplyActivatePreserves,
     DeliverActivatePreserves,
     LoseReplyPreserves,
     RequestChildPreserves,
     IssueChildPreserves,
     DeliverChildPreserves DEF Next

THEOREM StutteringStrong == Strong /\ UNCHANGED vars => Strong'
  BY DEF
       vars, Strong, TypeOK, ClockOrder, RootState, FenceBounds, ChildState, Ids, Max, RootValid,
       RootReplyEnd, ChildReplyEnd

THEOREM StrongSafety ==
  ASSUME Strong
  PROVE RootExclusive /\ ChildWithinRoot /\ ChildExclusive
<1>1. RootExclusive
  <2>1. ASSUME NEW p \in Ids, NEW q \in Ids, RootValid(p), RootValid(q)
        PROVE p = q
    <3>1. p \in Nat /\ q \in Nat /\ now \in Nat
          /\ roots[p].expires \in Nat /\ roots[q].expires \in Nat
          /\ roots[p].drain \in Nat /\ roots[q].drain \in Nat
      BY SMT, Parameters DEF Strong, TypeOK, Ids
    <3>2. p < q => FALSE
      BY SMT, Parameters, <2>1, <3>1 DEF Strong, FenceBounds, RootState, RootValid
    <3>3. q < p => FALSE
      BY SMT, Parameters, <2>1, <3>1 DEF Strong, FenceBounds, RootState, RootValid
    <3> QED BY SMT, <3>1, <3>2, <3>3
  <2> QED BY <2>1 DEF RootExclusive
<1>2. ChildWithinRoot
  BY SMT, Parameters DEF Strong, TypeOK, ChildState, Ids, ChildWithinRoot, RootValid, ChildValid
<1>3. ChildExclusive
  BY SMT, <1>1, <1>2 DEF ChildExclusive, RootExclusive, ChildWithinRoot
<1> QED BY <1>1, <1>2, <1>3

THEOREM Safety == Spec => [](RootExclusive /\ ChildWithinRoot /\ ChildExclusive)
<1>1. Spec => []Strong
  BY PTL, InitialStrong, NextStrong, StutteringStrong DEF Spec
<1> QED BY PTL, <1>1, StrongSafety
=============================================================================
