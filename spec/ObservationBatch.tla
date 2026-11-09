------------------------ MODULE ObservationBatch ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Readers, MaxValue, UnsafeLateJoin
VARIABLES current, phase, lower, upper, answer, pending, members,
          stage, observed, validated
vars == <<current, phase, lower, upper, answer, pending, members,
          stage, observed, validated>>

\* Writes advance a unique logical version. Every returned observation must
\* fall within its own invocation/response interval. One batch is enough to
\* expose the unsafe reuse of an in-flight or completed read barrier.
Init == /\ current = 0
        /\ phase = [r \in Readers |-> "idle"]
        /\ lower = [r \in Readers |-> 0]
        /\ upper = [r \in Readers |-> 0]
        /\ answer = [r \in Readers |-> 0]
        /\ pending = {} /\ members = {} /\ stage = "gather"
        /\ observed = 0 /\ validated = 0

Invoke(r) == /\ phase[r] = "idle"
             /\ phase' = [phase EXCEPT ![r] = "waiting"]
             /\ lower' = [lower EXCEPT ![r] = current]
             /\ IF UnsafeLateJoin /\ stage \in {"sealed", "validated"}
                   THEN /\ members' = members \cup {r}
                        /\ UNCHANGED pending
                   ELSE /\ pending' = pending \cup {r}
                        /\ UNCHANGED members
             /\ UNCHANGED <<current, upper, answer, stage, observed, validated>>

Seal == /\ stage = "gather" /\ pending # {}
        /\ members' = pending /\ pending' = {} /\ stage' = "sealed"
        /\ UNCHANGED <<current, phase, lower, upper, answer, observed, validated>>

Observe(v) == /\ stage = "sealed" /\ v \in 0..current
              /\ observed' = v
              /\ UNCHANGED <<current, phase, lower, upper, answer,
                             pending, members, stage, validated>>

Validate == /\ stage = "sealed" /\ observed = current
            /\ validated' = current /\ stage' = "validated"
            /\ UNCHANGED <<current, phase, lower, upper, answer,
                           pending, members, observed>>

Return(r) == /\ stage = "validated" /\ r \in members
             /\ phase[r] = "waiting"
             /\ answer' = [answer EXCEPT ![r] = validated]
             /\ upper' = [upper EXCEPT ![r] = current]
             /\ phase' = [phase EXCEPT ![r] = "returned"]
             /\ UNCHANGED <<current, lower, pending, members, stage,
                            observed, validated>>

Cancel(r) == /\ phase[r] = "waiting"
             /\ phase' = [phase EXCEPT ![r] = "cancelled"]
             /\ UNCHANGED <<current, lower, upper, answer, pending,
                            members, stage, observed, validated>>

Abandon == /\ stage \in {"gather", "sealed", "validated"}
           /\ stage' = "abandoned"
           /\ UNCHANGED <<current, phase, lower, upper, answer,
                          pending, members, observed, validated>>

Write == /\ current < MaxValue /\ current' = current + 1
         /\ UNCHANGED <<phase, lower, upper, answer, pending,
                        members, stage, observed, validated>>

Next == Write \/ Seal \/ Validate \/ Abandon
        \/ (\E v \in 0..current: Observe(v))
        \/ (\E r \in Readers: Invoke(r) \/ Return(r) \/ Cancel(r))

TypeOK == /\ current \in 0..MaxValue
          /\ phase \in [Readers -> {"idle", "waiting", "returned", "cancelled"}]
          /\ lower \in [Readers -> 0..MaxValue]
          /\ upper \in [Readers -> 0..MaxValue]
          /\ answer \in [Readers -> 0..MaxValue]
          /\ pending \subseteq Readers /\ members \subseteq Readers
          /\ stage \in {"gather", "sealed", "validated", "abandoned"}
          /\ observed \in 0..MaxValue /\ validated \in 0..MaxValue

ObservationWithinInvocation == \A r \in Readers:
    phase[r] = "returned" => /\ lower[r] <= answer[r]
                             /\ answer[r] <= upper[r]
Spec == Init /\ [][Next]_vars
=============================================================================
