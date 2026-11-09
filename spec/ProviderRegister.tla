------------------------ MODULE ProviderRegister ------------------------
EXTENDS Naturals, TLC
CONSTANTS Attempts, Values, UnsafeReuse
VARIABLES revision, value, nonce, phase, seen, desired, matched,
          reply, result, returnedRevision, staleSameValue
vars == <<revision, value, nonce, phase, seen, desired, matched,
          reply, result, returnedRevision, staleSameValue>>
PayloadNonce(a) == IF UnsafeReuse THEN 0 ELSE a
Init == /\ revision = 0 /\ value = 0 /\ nonce = 0
        /\ phase = [a \in Attempts |-> "fresh"]
        /\ seen = [a \in Attempts |-> 0]
        /\ desired = [a \in Attempts |-> 0]
        /\ matched = [a \in Attempts |-> FALSE]
        /\ reply = [a \in Attempts |-> "none"]
        /\ result = [a \in Attempts |-> "none"]
        /\ returnedRevision = [a \in Attempts |-> 0]
        /\ staleSameValue = [a \in Attempts |-> FALSE]
\* Each identity denotes one invocation, not a reusable idempotency token.
\* Observations can be arbitrarily stale; payloads can be identical to live data.
Begin(a, r, v) == /\ phase[a] = "fresh" /\ r \in 0..revision
                  /\ phase' = [phase EXCEPT ![a] = "pending"]
                  /\ seen' = [seen EXCEPT ![a] = r]
                  /\ desired' = [desired EXCEPT ![a] = v]
                  /\ UNCHANGED <<revision, value, nonce, matched, reply, result,
                                 returnedRevision, staleSameValue>>
\* B2 may accept an identical-metadata no-op regardless of the comparison.
\* Otherwise it atomically changes metadata only at the exact revision.
Apply(a) ==
    /\ phase[a] = "pending"
    /\ LET same == desired[a] = value /\ PayloadNonce(a) = nonce
           success == same \/ seen[a] = revision
           change == success /\ ~same
       IN /\ revision' = IF change THEN revision + 1 ELSE revision
          /\ value' = IF change THEN desired[a] ELSE value
          /\ nonce' = IF change THEN PayloadNonce(a) ELSE nonce
          /\ reply' = [reply EXCEPT ![a] = IF success THEN "ok" ELSE "conflict"]
          /\ returnedRevision' = [returnedRevision EXCEPT ![a] = revision']
    /\ phase' = [phase EXCEPT ![a] = "reply"]
    /\ matched' = [matched EXCEPT ![a] = (seen[a] = revision)]
    /\ staleSameValue' = [staleSameValue EXCEPT
           ![a] = (seen[a] < revision /\ desired[a] = value)]
    /\ UNCHANGED <<seen, desired, result>>
Deliver(a) ==
    /\ phase[a] = "reply"
    /\ result' = [result EXCEPT ![a] =
         IF reply[a] = "conflict" THEN "conflict"
         ELSE IF returnedRevision[a] > seen[a] THEN "success" ELSE "unknown"]
    /\ phase' = [phase EXCEPT ![a] = "done"]
    /\ UNCHANGED <<revision, value, nonce, seen, desired, matched,
                   reply, returnedRevision, staleSameValue>>
LoseReply(a) == /\ phase[a] = "reply"
                /\ result' = [result EXCEPT ![a] = "unknown"]
                /\ phase' = [phase EXCEPT ![a] = "done"]
                /\ UNCHANGED <<revision, value, nonce, seen, desired, matched,
                               reply, returnedRevision, staleSameValue>>
Next == \E a \in Attempts:
    (\E r \in 0..revision, v \in Values: Begin(a, r, v))
    \/ Apply(a) \/ Deliver(a) \/ LoseReply(a)
ExactComparison == \A a \in Attempts: result[a] = "success" => matched[a]
NotStaleNoOpRejected == ~\E a \in Attempts:
    result[a] = "conflict" /\ staleSameValue[a]
Spec == Init /\ [][Next]_vars
=============================================================================
