----------------------------- MODULE PiSession -----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC
CONSTANTS Sessions, MaxVersion, MaxBatches,
          UnsafeOwnership, UnsafeEarlyReceipt, UnsafePrefix
VARIABLES owner, version, nextSeq, durable, attempts, delivered, badOwner
vars == <<owner, version, nextSeq, durable, attempts, delivered, badOwner>>
InitialAttempt == [phase |-> "idle", expected |-> 0, stored |-> 0, proposed |-> 0,
                   first |-> 0, published |-> FALSE]
Init == /\ owner = "none" /\ version = 0 /\ nextSeq = 1 /\ durable = <<>>
        /\ attempts = [s \in Sessions |-> InitialAttempt]
        /\ delivered = {} /\ badOwner = FALSE
Claim(s) == /\ version < MaxVersion /\ owner # s
            /\ owner' = s /\ version' = version+1
            /\ UNCHANGED <<nextSeq, durable, attempts, delivered, badOwner>>
Prepare(s,n,valid) ==
    /\ attempts[s].phase = "idle" /\ owner = s
    /\ n \in 1..MaxBatches /\ valid \in 0..n
    /\ attempts' = [attempts EXCEPT ![s] = [phase |-> "prepared", expected |-> version,
                     stored |-> valid, proposed |-> n, first |-> nextSeq, published |-> FALSE]]
    /\ UNCHANGED <<owner, version, nextSeq, durable, delivered, badOwner>>
Publish(s) ==
    /\ attempts[s].phase \in {"prepared", "unknown"} /\ version < MaxVersion
    /\ ~attempts[s].published
    /\ (UnsafeOwnership \/ (owner = s /\ attempts[s].expected = version))
    /\ LET count == IF UnsafePrefix THEN attempts[s].proposed ELSE attempts[s].stored
       IN /\ durable' = Append(durable, [session |-> s, first |-> nextSeq,
                                         count |-> count, valid |-> attempts[s].stored])
          /\ attempts' = [attempts EXCEPT ![s].published = TRUE, ![s].first = nextSeq]
          /\ nextSeq' = nextSeq + count
    /\ version' = version+1 /\ badOwner' = (badOwner \/ owner # s)
    /\ UNCHANGED <<owner, delivered>>
Unknown(s) == /\ attempts[s].phase = "prepared"
              /\ attempts' = [attempts EXCEPT ![s].phase = "unknown"]
              /\ UNCHANGED <<owner, version, nextSeq, durable, delivered, badOwner>>
Receive(s) ==
    /\ attempts[s].phase \in {"prepared", "unknown"}
    /\ (UnsafeEarlyReceipt \/ attempts[s].stored = 0 \/ attempts[s].published)
    /\ delivered' = IF attempts[s].stored = 0 THEN delivered ELSE delivered \cup {s}
    /\ attempts' = [attempts EXCEPT ![s].phase = "returned"]
    /\ UNCHANGED <<owner, version, nextSeq, durable, badOwner>>
Next == (\E s \in Sessions: Claim(s) \/ Publish(s) \/ Unknown(s) \/ Receive(s)
         \/ (\E n \in 1..MaxBatches: (\E valid \in 0..n: Prepare(s,n,valid))))
TypeOK == /\ owner \in Sessions \cup {"none"} /\ version \in 0..MaxVersion
          /\ nextSeq \in 1..(1+MaxVersion*MaxBatches)
          /\ durable \in Seq([session: Sessions, first: 1..(1+MaxVersion*MaxBatches),
                              count: 0..MaxBatches, valid: 0..MaxBatches])
          /\ attempts \in [Sessions -> [phase: {"idle","prepared","unknown","returned"},
              expected: 0..MaxVersion, stored: 0..MaxBatches, proposed: 0..MaxBatches,
              first: 0..(1+MaxVersion*MaxBatches), published: BOOLEAN]]
          /\ delivered \subseteq Sessions /\ badOwner \in BOOLEAN
FencedOwner == ~badOwner
ReceiptsAreDurable == \A s \in delivered: attempts[s].published
ValidPrefixOnly == \A i \in 1..Len(durable): durable[i].count <= durable[i].valid
UniqueSequences == \A i,j \in 1..Len(durable): i < j => durable[i].first + durable[i].count <= durable[j].first
Spec == Init /\ [][Next]_vars
=============================================================================
