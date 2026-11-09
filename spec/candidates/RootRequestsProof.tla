------------------------- MODULE RootRequestsProof -------------------------
\* Root exclusion and monotone epochs for the explicit request lifecycle.
\* Ordered incarnation identities and abstract elapsed-time bounds are premises;
\* this is not a clock/platform or serving-engine refinement theorem.
EXTENDS RootRequests, SequenceTheorems, TLAPS
ASSUME Parameters == /\ Epochs \in Nat /\ Epochs >= 2
       /\ Attempts \in Nat /\ MaxTime \in Nat /\ Lease \in Nat /\ Lease > 0
       /\ ~UnsafeCompare /\ ~UnsafeTimeout /\ ~UnsafeLateExpiry /\ ~UnsafeDrain

RootRecord == [phase : {"fresh", "draining", "active"}, expires : Nat, drain : Nat]
RequestRecord == [status : Statuses, kind : Kinds, owner : Owners, expected : Nat,
                  open : BOOLEAN, started : Nat, result : Metadata]

THEOREM SequenceFacts ==
  ASSUME TypeOK
  PROVE /\ Len(history) \in Nat
        /\ history \in [1..Len(history) -> Metadata]
        /\ Current \in Metadata
  BY SMT, LenProperties DEF TypeOK, Current

THEOREM InitialStrong == Init => Strong
  BY SMT, Parameters, IsASeq DEF
       Init, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder,
       RootState, Captured, Applied, FenceBounds, Current, Initial,
       FreshRequest, Metadata, Owners, Ids, Kinds, Statuses

\* Keep the quantified state facts separate from transition arithmetic. These
\* are projections of Strong, not additional assumptions about reachable states.
THEOREM TypedRoot ==
  ASSUME Strong, NEW p \in Owners
  PROVE roots[p] \in RootRecord
  BY SMT DEF Strong, TypeOK, RootRecord

THEOREM TypedRequest ==
  ASSUME Strong, NEW a \in Ids
  PROVE requests[a] \in RequestRecord
  BY SMT DEF Strong, TypeOK, RequestRecord

THEOREM CapturedEntry ==
  ASSUME Strong, NEW a \in Ids, requests[a].status # "fresh"
  PROVE LET r == requests[a] IN
    /\ r.started <= now
    /\ r.expected \in 3..Current.revision
    /\ LET source == history[r.expected - 2] IN
          CASE r.kind = "fence" -> r.owner = source.owner + 1 /\ ~r.open
            [] r.kind = "validate" -> r.owner = source.owner /\ r.open = source.open
            [] OTHER -> r.owner = source.owner /\ r.open
    /\ (r.kind = "activate" => roots[r.owner].phase # "fresh"
                                  /\ r.started >= roots[r.owner].drain + Lease)
  BY DEF Strong, Captured

THEOREM OpenHistory ==
  ASSUME Strong, NEW i \in 1..Len(history), history[i].open
  PROVE roots[history[i].owner].phase # "fresh"
        /\ now >= roots[history[i].owner].drain + Lease
  BY DEF Strong, HistoryOK

THEOREM TickPreserves ==
  ASSUME Strong, Tick
  PROVE Strong'
<1>1. TypeOK'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, Current, Owners, Ids
<1>2. HistoryOK'
  <2>1. \A i \in 1..Len(history'): history'[i].revision = i + 2
    BY DEF Tick, Strong, HistoryOK
  <2>2. EpochOrder'
    BY DEF Tick, Strong, HistoryOK, EpochOrder
  <2>3. ASSUME NEW i \in 1..Len(history'), history'[i].open
        PROVE roots'[history'[i].owner].phase # "fresh"
              /\ now' >= roots'[history'[i].owner].drain + Lease
    <3>1. i \in 1..Len(history) /\ history[i].open /\ now \in Nat
          /\ roots' = roots /\ history' = history /\ now' = now + 1
      BY <2>3 DEF Tick, Strong, TypeOK
    <3>2. history[i].owner \in Owners
      BY SMT, SequenceFacts, <3>1 DEF Strong, TypeOK, Metadata
    <3> QED BY SMT, Parameters, OpenHistory, TypedRoot, <3>1, <3>2 DEF RootRecord
  <2> QED BY <2>1, <2>2, <2>3 DEF HistoryOK
<1>3. ViewOK'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, ViewOK, Current, Owners, Ids
<1>4. ClockOrder'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, ClockOrder, Current, Owners, Ids
<1>5. RootState'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, RootState, Current, Owners, Ids
<1>6. Captured'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, Captured, Current, Owners, Ids
<1>7. Applied'
  BY SMT, Parameters DEF Tick, Strong, TypeOK, Applied, Current, Owners, Ids
<1>8. FenceBounds'
  BY DEF Tick, Strong, FenceBounds
<1>9. now' >= Lease
  BY SMT, Parameters DEF Tick, Strong, TypeOK
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Strong

THEOREM SourceFacts ==
  ASSUME NEW p \in Owners, NEW kind \in Kinds, Strong,
         NEW source \in Sources(p, kind)
  PROVE /\ source \in Metadata
        /\ source.revision \in 3..Current.revision
        /\ source = history[source.revision - 2]
  BY SMT, SequenceFacts DEF Strong, TypeOK, Sources, ViewOK, HistoryOK, Current

THEOREM ReplacementType ==
  ASSUME NEW a \in Ids, TypeOK
  PROVE [owner |-> requests[a].owner, revision |-> Current.revision + 1,
         open |-> requests[a].open] \in Metadata
  BY SMT, Parameters, SequenceFacts DEF TypeOK, Metadata, Current, Ids

THEOREM ApplyShape ==
  ASSUME NEW a \in Ids, Strong, Apply(a)
  PROVE IF requests[a].expected = Current.revision
        THEN /\ history' \in Seq(Metadata)
             /\ Len(history') = Len(history) + 1
             /\ \A i \in 1..Len(history): history'[i] = history[i]
             /\ Current' = [owner |-> requests[a].owner,
                              revision |-> Current.revision + 1, open |-> requests[a].open]
        ELSE history' = history
  BY SMT, Parameters, SequenceFacts, ReplacementType, AppendProperties
     DEF Apply, Strong, TypeOK, Current

THEOREM AppliedEntry ==
  ASSUME Strong, NEW a \in applied
  PROVE /\ a \in Ids /\ requests[a].expected - 1 \in 1..Len(history)
        /\ requests[a].owner <= Current.owner /\ requests[a].started <= now
        /\ (requests[a].result.open =>
              roots[requests[a].owner].phase # "fresh"
              /\ now >= roots[requests[a].owner].drain + Lease)
<1>1. a \in Ids /\ requests[a].status # "fresh"
      /\ requests[a].expected - 1 \in 1..Len(history)
  BY SMT, SequenceFacts DEF Strong, TypeOK, Applied, Captured, HistoryOK, Current, Ids
<1>2. requests[a].owner <= Current.owner /\ requests[a].started <= now
  BY SMT, SequenceFacts, <1>1 DEF Strong, TypeOK, Applied, Captured, HistoryOK, EpochOrder, Current, Metadata
<1>3. requests[a].result.open =>
        roots[requests[a].owner].phase # "fresh"
        /\ now >= roots[requests[a].owner].drain + Lease
  BY SMT, <1>1 DEF Strong, TypeOK, Applied, HistoryOK
<1> QED BY <1>1, <1>2, <1>3

THEOREM ApplyFacts ==
  ASSUME NEW a \in Ids, Strong, Apply(a)
  PROVE /\ a \notin applied
        /\ Current'.owner \in Nat /\ Current.owner \in Nat
        /\ Current'.revision \in Nat /\ Current.revision \in Nat
        /\ Current'.owner >= Current.owner /\ Current'.revision >= Current.revision
        /\ (requests[a].expected = Current.revision => requests[a].owner >= Current.owner)
        /\ (requests[a].expected = Current.revision /\ requests[a].open =>
               roots[requests[a].owner].phase # "fresh"
               /\ now >= roots[requests[a].owner].drain + Lease)
<1>1. a \notin applied
  BY SMT DEF Apply, Strong, Applied
<1>2. requests[a].expected = Current.revision => history[requests[a].expected - 2] = Current
  BY SMT, SequenceFacts DEF Strong, TypeOK, HistoryOK, Current
<1>3. requests[a].expected = Current.revision => requests[a].owner >= Current.owner
  BY SMT, SequenceFacts, <1>2 DEF Apply, Strong, TypeOK, Captured, Current, Metadata, Kinds, Owners, Ids
<1>4. requests[a].expected = Current.revision /\ requests[a].open =>
         roots[requests[a].owner].phase # "fresh"
         /\ now >= roots[requests[a].owner].drain + Lease
  <2>1. ASSUME requests[a].expected = Current.revision, requests[a].open
        PROVE roots[requests[a].owner].phase # "fresh"
              /\ now >= roots[requests[a].owner].drain + Lease
    <3>1. requests[a].status # "fresh"
      BY DEF Apply
    <3>2. requests[a] \in RequestRecord /\ now \in Nat
      BY TypedRequest DEF Strong, TypeOK
    <3>3. requests[a].kind # "fence"
      BY SMT, CapturedEntry, <2>1, <3>1, <3>2 DEF RequestRecord
    <3>4. requests[a].kind = "activate" =>
             roots[requests[a].owner].phase # "fresh"
             /\ now >= roots[requests[a].owner].drain + Lease
      BY SMT, Parameters, CapturedEntry, TypedRoot, <3>1, <3>2 DEF RequestRecord, RootRecord
    <3>5. requests[a].kind = "validate" => requests[a].owner = Current.owner /\ Current.open
      BY SMT, CapturedEntry, <2>1, <1>2, <3>1, <3>2 DEF RequestRecord
    <3>6. roots[Current.owner].phase # "fresh" /\ now >= roots[Current.owner].drain + Lease
          \/ requests[a].kind # "validate"
      BY SMT, OpenHistory, SequenceFacts, <3>5 DEF Strong, TypeOK, Current
    <3> QED BY SMT, <3>2, <3>3, <3>4, <3>5, <3>6 DEF RequestRecord, Kinds
  <2> QED BY <2>1
<1>5. Current'.owner \in Nat /\ Current.owner \in Nat
      /\ Current'.revision \in Nat /\ Current.revision \in Nat
      /\ Current'.owner >= Current.owner /\ Current'.revision >= Current.revision
  BY SMT, Parameters, SequenceFacts, ApplyShape, <1>3 DEF Strong, TypeOK, Current, Metadata, Owners
<1> QED BY <1>1, <1>3, <1>4, <1>5

THEOREM RequestUpdateType ==
  ASSUME NEW r \in RequestRecord, NEW status \in Statuses, NEW result \in Metadata
  PROVE [r EXCEPT !.status = status, !.result = result] \in RequestRecord
  BY SMT DEF RequestRecord

THEOREM DeliverViewShape ==
  ASSUME NEW a \in Ids, Deliver(a)
  PROVE IF active[requests[a].owner] = a /\ requests[a].status = "ok"
        THEN view' = [view EXCEPT ![requests[a].owner] = requests[a].result]
        ELSE view' = view
  BY DEF Deliver

THEOREM StartType ==
  ASSUME NEW p \in Owners, NEW a \in Ids, NEW kind \in Kinds, Strong,
         NEW source \in Sources(p, kind), Start(p, a, kind, source)
  PROVE TypeOK'
<1>1. requests' \in [Ids -> RequestRecord]
  <2>1. a \in Ids /\ requests \in [Ids -> RequestRecord]
    BY SMT, Parameters DEF Strong, TypeOK, Metadata, RequestRecord, Owners, Ids
  <2>2. requests'[a] \in RequestRecord
    BY SMT, <2>1, Parameters, SequenceFacts, SourceFacts DEF Start, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, NextId, Initial, Sources
  <2>3. DOMAIN requests' = Ids
        /\ \A b \in Ids: b # a => requests'[b] = requests[b]
    BY SMT, <2>1 DEF Start
  <2> QED BY SMT, <2>1, <2>2, <2>3 DEF Start
<1>2. active' \in [Owners -> 0..Attempts]
  BY SMT, Parameters, SequenceFacts, SourceFacts DEF Start, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, NextId, Initial, Sources
<1>3. valid' \in [Owners -> BOOLEAN]
  BY SMT, Parameters, SequenceFacts, SourceFacts DEF Start, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, NextId, Initial, Sources
<1> QED BY SMT, <1>1, <1>2, <1>3, Parameters DEF Start, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, NextId, Initial, Sources

THEOREM ApplyType ==
  ASSUME NEW a \in Ids, Strong, Apply(a)
  PROVE TypeOK'
<1>1. history' \in Seq(Metadata) /\ Len(history') > 0
  BY SMT, Parameters, SequenceFacts, ReplacementType, ApplyShape DEF Apply, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Initial, Current
<1>2. requests' \in [Ids -> RequestRecord]
  <2>1. a \in Ids /\ requests \in [Ids -> RequestRecord]
    BY SMT, Parameters DEF Strong, TypeOK, Metadata, RequestRecord, Owners, Ids
  <2>2. requests'[a] \in RequestRecord
    <3>1. requests'[a] = [requests[a] EXCEPT
              !.status = IF UnsafeCompare \/ requests[a].expected = Current.revision
                         THEN "ok" ELSE "conflict",
              !.result = IF UnsafeCompare \/ requests[a].expected = Current.revision
                         THEN [owner |-> requests[a].owner, revision |-> Current.revision + 1,
                               open |-> requests[a].open] ELSE Initial]
      BY SMT, <2>1 DEF Apply
    <3>2. Initial \in Metadata
          /\ [owner |-> requests[a].owner, revision |-> Current.revision + 1,
               open |-> requests[a].open] \in Metadata
      BY SMT, Parameters, ReplacementType DEF Strong, TypeOK, Initial, Metadata, Owners
    <3> QED BY SMT, RequestUpdateType, <2>1, <3>1, <3>2 DEF Statuses
  <2>3. DOMAIN requests' = Ids
        /\ \A b \in Ids: b # a => requests'[b] = requests[b]
    BY SMT, <2>1 DEF Apply
  <2> QED BY SMT, <2>1, <2>2, <2>3 DEF Apply
<1>3. applied' \subseteq Ids
  BY SMT, Parameters, SequenceFacts, ReplacementType, ApplyShape DEF Apply, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Initial, Current
<1>4. lateRejected' \in BOOLEAN
  BY SMT, Parameters, SequenceFacts, ReplacementType, ApplyShape DEF Apply, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Initial, Current
<1> QED BY SMT, <1>1, <1>2, <1>3, <1>4, Parameters DEF Apply, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Initial, Current

THEOREM TimeoutType ==
  ASSUME NEW p \in Owners, NEW a \in Ids, Strong, Timeout(p, a)
  PROVE TypeOK'
<1>1. active' \in [Owners -> 0..Attempts]
  BY SMT, Parameters, SequenceFacts DEF Timeout, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses
<1>2. valid' \in [Owners -> BOOLEAN]
  BY SMT, Parameters, SequenceFacts DEF Timeout, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses
<1>3. timedOut' \subseteq Ids
  BY SMT, Parameters, SequenceFacts DEF Timeout, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses
<1> QED BY SMT, <1>1, <1>2, <1>3, Parameters DEF Timeout, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses

THEOREM DeliverType ==
  ASSUME NEW a \in Ids, Strong, Deliver(a)
  PROVE TypeOK'
<1>1. roots' \in [Owners -> RootRecord]
  BY SMT, Parameters, SequenceFacts DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid
<1>2. view' \in [Owners -> Metadata]
  <2>1. requests[a].owner \in Owners /\ view \in [Owners -> Metadata]
    BY SMT, Parameters DEF Strong, TypeOK, Metadata, RequestRecord, Owners, Ids
  <2>2. view'[requests[a].owner] \in Metadata
    <3>1. requests[a].result \in Metadata
      BY SMT DEF Strong, TypeOK
    <3> QED BY SMT, <2>1, <3>1, DeliverViewShape
  <2>3. DOMAIN view' = Owners
        /\ \A b \in Owners: b # requests[a].owner => view'[b] = view[b]
    BY SMT, <2>1 DEF Deliver
  <2> QED BY SMT, <2>1, <2>2, <2>3 DEF Deliver
<1>3. valid' \in [Owners -> BOOLEAN]
  BY SMT, Parameters, SequenceFacts DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid
<1>4. active' \in [Owners -> 0..Attempts]
  BY SMT, Parameters, SequenceFacts DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid
<1>5. requests' \in [Ids -> RequestRecord]
  <2>1. a \in Ids /\ requests \in [Ids -> RequestRecord]
    BY SMT, Parameters DEF Strong, TypeOK, Metadata, RequestRecord, Owners, Ids
  <2>2. requests'[a] \in RequestRecord
    BY SMT, <2>1, Parameters, SequenceFacts DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid
  <2>3. DOMAIN requests' = Ids
        /\ \A b \in Ids: b # a => requests'[b] = requests[b]
    BY SMT, <2>1 DEF Deliver
  <2> QED BY SMT, <2>1, <2>2, <2>3 DEF Deliver
<1>6. renewed' \in BOOLEAN /\ recovered' \in BOOLEAN /\ activationRecovered' \in BOOLEAN /\ lateReply' \in BOOLEAN
  BY SMT, Parameters, SequenceFacts DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid
<1> QED BY SMT, <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, Parameters DEF Deliver, Strong, TypeOK, Metadata, RequestRecord, RootRecord, Owners, Ids, Kinds, Statuses, Max, RootValid

THEOREM StartPreserves ==
  ASSUME NEW p \in Owners, NEW a \in Ids, NEW kind \in Kinds, Strong,
         NEW source \in Sources(p, kind), Start(p, a, kind, source)
  PROVE Strong'
<1>1. TypeOK'
  BY StartType
<1>2. HistoryOK'
  BY DEF Start, Strong, HistoryOK, EpochOrder
<1>3. ViewOK'
  BY DEF Start, Strong, ViewOK
<1>4. ClockOrder'
  BY DEF Start, Strong, ClockOrder
<1>5. RootState'
  BY DEF Start, Strong, RootState, Current
<1>6. Captured'
  BY SMT, Parameters, SequenceFacts, SourceFacts DEF Start, NextId, Sources, Strong, TypeOK, Captured, ViewOK, Current, Metadata, Owners, Ids, Kinds
<1>7. Applied'
  BY SMT, Parameters, SequenceFacts, SourceFacts DEF Start, NextId, Initial, Strong, TypeOK, Applied, Current, Owners, Ids
<1>8. FenceBounds'
  BY SMT, Parameters, SequenceFacts, SourceFacts DEF Start, NextId, Initial, Sources, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>9. now' >= Lease
  BY SMT DEF Start, Strong
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Strong


THEOREM ApplyRows ==
  ASSUME NEW a \in Ids, Strong, Apply(a)
  PROVE /\ roots' = roots /\ now' = now
        /\ requests'[a].owner = requests[a].owner
        /\ requests'[a].started = requests[a].started
        /\ requests'[a].expected = requests[a].expected
        /\ requests'[a].open = requests[a].open
        /\ requests'[a].kind = requests[a].kind
        /\ \A b \in Ids: b # a => requests'[b] = requests[b]
        /\ IF requests[a].expected = Current.revision
           THEN /\ requests'[a].status = "ok" /\ requests'[a].result = Current'
                /\ applied' = applied \cup {a}
           ELSE /\ requests'[a].status = "conflict" /\ applied' = applied
<1>1. roots' = roots /\ now' = now
  BY DEF Apply
<1>2. requests \in [Ids -> RequestRecord]
  BY DEF Strong, TypeOK, RequestRecord
<1>3. requests'[a].owner = requests[a].owner
      /\ requests'[a].started = requests[a].started
      /\ requests'[a].expected = requests[a].expected
      /\ requests'[a].open = requests[a].open
      /\ requests'[a].kind = requests[a].kind
  BY SMT, <1>2 DEF Apply, RequestRecord
<1>4. \A b \in Ids: b # a => requests'[b] = requests[b]
  BY SMT, <1>2 DEF Apply
<1>5. IF requests[a].expected = Current.revision
      THEN /\ requests'[a].status = "ok" /\ requests'[a].result = Current'
           /\ applied' = applied \cup {a}
      ELSE /\ requests'[a].status = "conflict" /\ applied' = applied
  BY SMT, Parameters, ApplyShape, <1>2 DEF Apply, RequestRecord
<1> QED BY <1>1, <1>3, <1>4, <1>5

THEOREM ApplyPreserves ==
  ASSUME NEW a \in Ids, Strong, Apply(a)
  PROVE Strong'
<1>1. TypeOK'
  BY ApplyType
<1>2. HistoryOK'
  <2>1. \A i \in 1..Len(history'): history'[i].revision = i + 2
    BY SMT, Parameters, SequenceFacts, ApplyShape DEF Apply, Strong, TypeOK, HistoryOK, Current, Metadata
  <2>2. EpochOrder'
    <3>1. ASSUME NEW i \in 1..Len(history'), NEW j \in 1..Len(history'), i <= j
          PROVE history'[i].owner <= history'[j].owner
      <4>1. Len(history) \in Nat /\ Len(history) > 0
            /\ i \in Nat /\ j \in Nat /\ i <= j
        BY SMT, SequenceFacts, <3>1 DEF Strong, TypeOK
      <4>2. CASE i <= Len(history) /\ j <= Len(history)
        BY SMT, ApplyShape, <3>1, <4>1, <4>2 DEF Strong, HistoryOK, EpochOrder
      <4>3. CASE j > Len(history)
        <5>1. j = Len(history') /\ history'[j] = Current'
              /\ history'[j].owner \in Nat /\ Current.owner \in Nat
          BY SMT, ApplyShape, ApplyFacts, <4>1, <4>3 DEF Current
        <5>2. i = j => history'[i].owner <= history'[j].owner
          BY SMT, <5>1
        <5>3. i < j => i \in 1..Len(history) /\ history'[i] = history[i]
          BY SMT, ApplyShape, <3>1, <4>1, <4>3
        <5>4. i < j => history'[i].owner <= Current.owner /\ history'[i].owner \in Nat
          BY SMT, Parameters, SequenceFacts, <5>3 DEF Strong, TypeOK, HistoryOK, EpochOrder, Current, Metadata, Owners
        <5> QED BY SMT, ApplyFacts, <4>1, <5>1, <5>2, <5>3, <5>4
      <4> QED BY SMT, <4>1, <4>2, <4>3
    <3> QED BY <3>1 DEF EpochOrder
  <2>3. \A i \in 1..Len(history'): history'[i].open =>
          roots'[history'[i].owner].phase # "fresh"
          /\ now' >= roots'[history'[i].owner].drain + Lease
    BY SMT, Parameters, SequenceFacts, ApplyShape, ApplyFacts DEF Apply, Strong, TypeOK, HistoryOK, Current, Metadata
  <2> QED BY <2>1, <2>2, <2>3 DEF HistoryOK
<1>3. ViewOK'
  <2>1. view' = view /\ roots' = roots
    BY DEF Apply
  <2>2. \A p \in Owners: \E i \in 1..Len(history'): view'[p] = history'[i]
    BY SMT, SequenceFacts, ApplyShape, <2>1 DEF Strong, TypeOK, ViewOK
  <2>3. \A p \in Owners: roots'[p].phase # "fresh" => view'[p].owner = p
    BY <2>1 DEF Strong, ViewOK
  <2> QED BY <2>2, <2>3 DEF ViewOK
<1>4. ClockOrder'
  BY DEF Apply, Strong, ClockOrder
<1>5. RootState'
  BY SMT, Parameters, ApplyFacts, ApplyRows DEF Strong, RootState, Owners
<1>6. Captured'
  <2>1. ASSUME NEW b \in Ids, requests'[b].status # "fresh"
        PROVE LET r == requests'[b] IN
          /\ r.started <= now'
          /\ r.expected \in 3..Current'.revision
          /\ LET source == history'[r.expected - 2] IN
                CASE r.kind = "fence" -> r.owner = source.owner + 1 /\ ~r.open
                  [] r.kind = "validate" -> r.owner = source.owner /\ r.open = source.open
                  [] OTHER -> r.owner = source.owner /\ r.open
          /\ (r.kind = "activate" => roots'[r.owner].phase # "fresh"
                                        /\ r.started >= roots'[r.owner].drain + Lease)
    <3>1. requests[b].status # "fresh"
      BY SMT, ApplyRows, <2>1 DEF Apply
    <3>2. requests[b].expected - 2 \in 1..Len(history)
          /\ history'[requests[b].expected - 2] = history[requests[b].expected - 2]
      BY SMT, Parameters, SequenceFacts, ApplyShape, CapturedEntry, <3>1
         DEF Strong, TypeOK, HistoryOK, Current
    <3>3. requests'[b].owner = requests[b].owner /\ requests'[b].started = requests[b].started
          /\ requests'[b].expected = requests[b].expected /\ requests'[b].open = requests[b].open
          /\ requests'[b].kind = requests[b].kind
      BY SMT, ApplyRows
    <3>4. requests'[b].started <= now'
      BY SMT, CapturedEntry, ApplyRows, <3>1, <3>3
    <3>5. requests'[b].expected \in 3..Current'.revision
      BY SMT, CapturedEntry, ApplyFacts, TypedRequest, <3>1, <3>3 DEF RequestRecord
    <3>6. LET r == requests'[b] IN LET source == history'[r.expected - 2] IN
            CASE r.kind = "fence" -> r.owner = source.owner + 1 /\ ~r.open
              [] r.kind = "validate" -> r.owner = source.owner /\ r.open = source.open
              [] OTHER -> r.owner = source.owner /\ r.open
      BY SMT, CapturedEntry, <3>1, <3>2, <3>3
    <3>7. requests'[b].kind = "activate" => roots'[requests'[b].owner].phase # "fresh"
                  /\ requests'[b].started >= roots'[requests'[b].owner].drain + Lease
      BY SMT, CapturedEntry, ApplyRows, <3>1, <3>3
    <3> QED BY <3>4, <3>5, <3>6, <3>7
  <2> QED BY <2>1 DEF Captured
<1>7. Applied'
  <2>1. \A b \in Ids: requests'[b].status = "ok" => b \in applied'
    BY SMT, ApplyRows DEF Strong, Applied
  <2>2. ASSUME NEW b \in applied'
        PROVE LET r == requests'[b] IN
          /\ r.status \in {"ok", "done"}
          /\ r.result = history'[r.expected - 1]
          /\ r.result.revision = r.expected + 1
          /\ r.result.owner = r.owner /\ r.result.open = r.open
          /\ r.result.revision <= Current'.revision
    <3>1. CASE b = a
      <4>1. requests[a].expected = Current.revision /\ b = a
        BY SMT, ApplyFacts, ApplyRows, <2>2, <3>1
      <4>2. requests'[b].result = Current' /\ requests'[b].status = "ok"
            /\ requests'[b].expected = Current.revision
            /\ Current' = [owner |-> requests[a].owner,
                           revision |-> Current.revision + 1, open |-> requests[a].open]
        BY SMT, ApplyShape, ApplyRows, <4>1
      <4>3. requests'[b].expected - 1 = Len(history')
        BY SMT, SequenceFacts, ApplyShape, <4>1, <4>2 DEF Strong, TypeOK, HistoryOK, Current
      <4>4. Current'.revision \in Nat
        BY SMT, Parameters, SequenceFacts, <4>2 DEF Strong, TypeOK, Current, Metadata
      <4> QED BY SMT, ApplyRows, <4>1, <4>2, <4>3, <4>4 DEF Current
    <3>2. CASE b # a
      BY SMT, Parameters, SequenceFacts, AppliedEntry, ApplyShape, ApplyFacts, ApplyRows, <2>2, <3>2 DEF Strong, TypeOK, Applied, Current, Metadata
    <3> QED BY <3>1, <3>2
  <2> QED BY <2>1, <2>2 DEF Applied
<1>8. FenceBounds'
  <2>1. \A p, q \in Owners: (p < q /\ roots'[q].phase # "fresh") =>
          roots'[p].expires <= roots'[q].drain + Lease
    BY SMT DEF Apply, Strong, FenceBounds
  <2>2. ASSUME NEW p \in Owners, NEW q \in Owners,
               p < q, roots'[q].phase # "fresh", NEW b \in applied', requests'[b].owner = p
        PROVE requests'[b].started <= roots'[q].drain
    <3>1. CASE b = a
      <4>1. requests[a].expected = Current.revision /\ b = a
        BY SMT, ApplyFacts, ApplyRows, <2>2, <3>1
      <4>2. requests[a].owner = p /\ p >= Current.owner
        BY SMT, ApplyFacts, ApplyRows, <2>2, <4>1
      <4>3. q <= Current.owner
        BY SMT, ApplyRows, <2>2 DEF Strong, RootState
      <4>4. p \in Nat /\ q \in Nat /\ Current.owner \in Nat /\ p < q
        BY SMT, Parameters, ApplyFacts, <2>2 DEF Owners
      <4> QED BY SMT, <4>2, <4>3, <4>4
    <3>2. CASE b # a
      <4>1. b \in applied /\ requests'[b] = requests[b] /\ roots' = roots
        BY SMT, ApplyRows, <2>2, <3>2 DEF Strong, TypeOK
      <4> QED BY SMT, <2>2, <4>1 DEF Strong, FenceBounds
    <3> QED BY <3>1, <3>2
  <2> QED BY <2>1, <2>2 DEF FenceBounds
<1>9. now' >= Lease
  BY SMT DEF Apply, Strong
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Strong


THEOREM TimeoutPreserves ==
  ASSUME NEW p \in Owners, NEW a \in Ids, Strong, Timeout(p, a)
  PROVE Strong'
<1>1. TypeOK'
  BY TimeoutType
<1>2. HistoryOK'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>3. ViewOK'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>4. ClockOrder'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>5. RootState'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>6. Captured'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>7. Applied'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>8. FenceBounds'
  BY SMT, Parameters, SequenceFacts DEF Timeout, Max, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>9. now' >= Lease
  BY SMT DEF Timeout, Strong
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Strong

THEOREM DeliverRows ==
  ASSUME NEW a \in Ids, Strong, Deliver(a)
  PROVE /\ now' = now /\ applied' = applied
        /\ \A b \in Ids: requests'[b].owner = requests[b].owner
                           /\ requests'[b].started = requests[b].started
        /\ \A p \in Owners: p # requests[a].owner => roots'[p] = roots[p]
        /\ \A p \in Owners: roots[p].phase # "fresh" => roots'[p].drain = roots[p].drain
        /\ \A p \in Owners: roots[p].phase = "fresh" /\ roots'[p].phase # "fresh" =>
              p = requests[a].owner /\ roots'[p].drain = now
<1>1. now' = now /\ applied' = applied
  BY DEF Deliver
<1>2. \A b \in Ids: requests'[b].owner = requests[b].owner
                     /\ requests'[b].started = requests[b].started
  BY SMT DEF Deliver, Strong, TypeOK
<1>3. \A p \in Owners: p # requests[a].owner => roots'[p] = roots[p]
  BY SMT DEF Deliver, Strong, TypeOK
<1>4. \A p \in Owners: roots[p].phase # "fresh" => roots'[p].drain = roots[p].drain
  BY SMT DEF Deliver, Strong, TypeOK
<1>5. \A p \in Owners: roots[p].phase = "fresh" /\ roots'[p].phase # "fresh" =>
            p = requests[a].owner /\ roots'[p].drain = now
  BY SMT DEF Deliver, Strong, TypeOK
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5

THEOREM DeliverPreserves ==
  ASSUME NEW a \in Ids, Strong, Deliver(a)
  PROVE Strong'
<1>1. TypeOK'
  BY DeliverType
<1>2. HistoryOK'
  BY SMT, Parameters, SequenceFacts DEF Deliver, Max, RootValid, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>3. ViewOK'
  BY SMT, Parameters, SequenceFacts, AppliedEntry DEF Deliver, Max, RootValid, Strong, TypeOK, ViewOK, Captured, Applied, HistoryOK, Current, Metadata, Owners, Ids
<1>4. ClockOrder'
  BY SMT, Parameters, SequenceFacts DEF Deliver, Max, RootValid, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>5. RootState'
  BY SMT, Parameters, SequenceFacts, AppliedEntry DEF Deliver, Max, RootValid, Strong, TypeOK, RootState, Captured, Applied, HistoryOK, EpochOrder, Current, Metadata, Owners, Ids
<1>6. Captured'
  BY SMT, Parameters, SequenceFacts DEF Deliver, Max, RootValid, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder, RootState, Captured, Applied, FenceBounds, Current, Metadata, Owners, Ids, Kinds, Statuses
<1>7. Applied'
  BY SMT, Parameters, SequenceFacts DEF Deliver, Max, RootValid, Strong, TypeOK, Applied, Current, Metadata, Owners, Ids
<1>8. FenceBounds'
  <2>1. ASSUME NEW p \in Owners, NEW q \in Owners, p < q, roots'[q].phase # "fresh"
        PROVE /\ roots'[p].expires <= roots'[q].drain + Lease
              /\ \A b \in applied': requests'[b].owner = p => requests'[b].started <= roots'[q].drain
    <3>1. p \in Nat /\ q \in Nat /\ p # q
          /\ roots[p].expires \in Nat /\ roots[q].drain \in Nat /\ now \in Nat
      BY SMT, Parameters, TypedRoot, <2>1 DEF Strong, TypeOK, RootRecord, Owners
    <3>2. CASE roots[q].phase = "fresh"
      <4>1. q = requests[a].owner /\ roots'[q].drain = now /\ roots'[p] = roots[p]
        BY SMT, DeliverRows, <2>1, <3>1, <3>2
      <4>2. roots'[p].expires <= roots'[q].drain + Lease
        BY SMT, <4>1 DEF Strong, ClockOrder
      <4>3. \A b \in applied': requests'[b].owner = p => requests'[b].started <= roots'[q].drain
        BY SMT, AppliedEntry, DeliverRows, <4>1
      <4> QED BY <4>2, <4>3
    <3>3. CASE roots[q].phase # "fresh"
      <4>1. roots'[q].drain = roots[q].drain
            /\ roots[p].expires <= roots[q].drain + Lease
        BY SMT, DeliverRows, <2>1, <3>3 DEF Strong, FenceBounds
      <4>2. requests[a].status = "ok" /\ requests[a].owner = p =>
              requests[a].started <= roots[q].drain
        BY SMT, <2>1, <3>3 DEF Strong, Applied, FenceBounds
      <4>3. roots'[p].expires <= roots'[q].drain + Lease
        BY SMT, Parameters, TypedRequest, <2>1, <3>1, <4>1, <4>2
           DEF Deliver, Max, RequestRecord, Strong, TypeOK
      <4>4. \A b \in applied': requests'[b].owner = p => requests'[b].started <= roots'[q].drain
        BY SMT, DeliverRows, <2>1, <3>3, <4>1 DEF Strong, TypeOK, FenceBounds
      <4> QED BY <4>3, <4>4
    <3> QED BY <3>2, <3>3
  <2> QED BY <2>1 DEF FenceBounds
<1>9. now' >= Lease
  BY SMT DEF Deliver, Strong
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Strong


THEOREM StrongImpliesExclusive ==
  ASSUME Strong
  PROVE RootExclusive
<1>1. ASSUME NEW p \in Owners, NEW q \in Owners, RootValid(p), RootValid(q)
      PROVE p = q
  <2>1. p \in Nat /\ q \in Nat /\ now \in Nat
        /\ roots[p].expires \in Nat /\ roots[q].expires \in Nat
        /\ roots[p].drain \in Nat /\ roots[q].drain \in Nat
    BY SMT, Parameters DEF Strong, TypeOK, Owners
  <2>2. p < q => FALSE
    BY SMT, Parameters, <1>1, <2>1 DEF Strong, FenceBounds, RootState, RootValid
  <2>3. q < p => FALSE
    BY SMT, Parameters, <1>1, <2>1 DEF Strong, FenceBounds, RootState, RootValid
  <2> QED BY SMT, <2>1, <2>2, <2>3
<1> QED BY <1>1 DEF RootExclusive
THEOREM NextStrong == Strong /\ Next => Strong'
  BY TickPreserves, StartPreserves, ApplyPreserves, TimeoutPreserves, DeliverPreserves DEF Next

THEOREM StutteringStrong == Strong /\ UNCHANGED vars => Strong'
  BY DEF vars, Strong, TypeOK, HistoryOK, EpochOrder, ViewOK, ClockOrder,
         RootState, Captured, Applied, FenceBounds, Current

THEOREM Safety == Spec => [](RootExclusive /\ EpochOrder)
<1>1. Spec => []Strong
  BY PTL, InitialStrong, NextStrong, StutteringStrong DEF Spec
<1>2. Strong => RootExclusive /\ EpochOrder
  BY StrongImpliesExclusive DEF Strong, HistoryOK
<1> QED BY PTL, <1>1, <1>2
=============================================================================
