--------------------------- MODULE ServingLease ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Nodes, First, Lease, MaxTick, MaxEpoch, MaxValue,
          UnsafeSkipDrain, UnsafePausedClock, UnsafeLateGrant
VARIABLES tick, clocks, online, paused, owner, epoch, mode, drainStart,
          value, view, expires, heldEpoch, requestEpoch, requestStart,
          pending, grantEpoch, grantStart, grantValue, badRead
vars == <<tick, clocks, online, paused, owner, epoch, mode, drainStart,
          value, view, expires, heldEpoch, requestEpoch, requestStart,
          pending, grantEpoch, grantStart, grantValue, badRead>>

\* Candidate only. A real-time step represents two units; EACH node's clock
\* must advance by one or two units, even while its process is paused.
\* This is an explicit bounded-rate clock assumption, not an implementation
\* guarantee and not a proof of the complete proposed storage architecture.
Init == /\ tick = 0 /\ clocks = [n \in Nodes |-> 0]
        /\ online = Nodes /\ paused = {}
        /\ owner = First /\ epoch = 0 /\ mode = "active"
        /\ drainStart = [n \in Nodes |-> 0]
        /\ value = 0 /\ view = [n \in Nodes |-> 0]
        /\ expires = [n \in Nodes |-> 0] /\ heldEpoch = [n \in Nodes |-> 0]
        /\ requestEpoch = [n \in Nodes |-> 0] /\ requestStart = [n \in Nodes |-> 0]
        /\ pending = [n \in Nodes |-> "none"]
        /\ grantEpoch = [n \in Nodes |-> 0] /\ grantStart = [n \in Nodes |-> 0]
        /\ grantValue = [n \in Nodes |-> 0] /\ badRead = FALSE
Runnable(n) == n \in online \ paused
LeaseValid(n) == /\ Runnable(n) /\ expires[n] > clocks[n]
Tick == /\ tick < MaxTick /\ tick' = tick + 1
        /\ \E speeds \in [Nodes -> {1, 2}]:
             clocks' = [n \in Nodes |-> clocks[n] +
               (IF UnsafePausedClock /\ n \in paused THEN 0 ELSE speeds[n])]
        /\ UNCHANGED <<online, paused, owner, epoch, mode, drainStart, value,
              view, expires, heldEpoch, requestEpoch, requestStart, pending,
              grantEpoch, grantStart, grantValue, badRead>>
Request(n) == /\ Runnable(n) /\ pending[n] = "none"
              /\ requestEpoch' = [requestEpoch EXCEPT ![n] = epoch]
              /\ requestStart' = [requestStart EXCEPT ![n] = clocks[n]]
              /\ pending' = [pending EXCEPT ![n] = "request"]
              /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch, mode,
                  drainStart, value, view, expires, heldEpoch, grantEpoch,
                  grantStart, grantValue, badRead>>
\* Successful provider CAS. A reply may arrive after takeover or a long pause.
Grant(n) == /\ pending[n] = "request" /\ n = owner /\ mode = "active"
            /\ requestEpoch[n] = epoch
            /\ grantEpoch' = [grantEpoch EXCEPT ![n] = epoch]
            /\ grantStart' = [grantStart EXCEPT ![n] = requestStart[n]]
            /\ grantValue' = [grantValue EXCEPT ![n] = value]
            /\ pending' = [pending EXCEPT ![n] = "reply"]
            /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch, mode,
                drainStart, value, view, expires, heldEpoch, requestEpoch,
                requestStart, badRead>>
Deliver(n) == /\ Runnable(n) /\ pending[n] = "reply"
              /\ expires' = [expires EXCEPT ![n] =
                   (IF UnsafeLateGrant THEN clocks[n] ELSE grantStart[n]) + Lease]
              /\ heldEpoch' = [heldEpoch EXCEPT ![n] = grantEpoch[n]]
              /\ view' = [view EXCEPT ![n] = IF @ > grantValue[n] THEN @ ELSE grantValue[n]]
              /\ pending' = [pending EXCEPT ![n] = "none"]
              /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch, mode,
                  drainStart, value, requestEpoch, requestStart, grantEpoch,
                  grantStart, grantValue, badRead>>
\* Takeover fences future writes/renewals immediately, but old read grants
\* remain possible. The replacement cannot activate until their maximum
\* real-time lifetime has elapsed. Its clock might be the faster one.
Fence(n) == /\ Runnable(n) /\ epoch < MaxEpoch
            /\ owner' = n /\ epoch' = epoch + 1 /\ mode' = "draining"
            /\ drainStart' = [drainStart EXCEPT ![n] = clocks[n]]
            /\ UNCHANGED <<tick, clocks, online, paused, value, view, expires,
                heldEpoch, requestEpoch, requestStart, pending, grantEpoch,
                grantStart, grantValue, badRead>>
Activate(n) == /\ Runnable(n) /\ owner = n /\ mode = "draining"
               /\ (UnsafeSkipDrain \/ clocks[n] >= drainStart[n] + 2 * Lease)
               /\ mode' = "active"
               /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch,
                   drainStart, value, view, expires, heldEpoch, requestEpoch,
                   requestStart, pending, grantEpoch, grantStart, grantValue, badRead>>
Commit(n) == /\ LeaseValid(n) /\ n = owner /\ heldEpoch[n] = epoch
             /\ mode = "active" /\ value < MaxValue
             /\ value' = value + 1 /\ view' = [view EXCEPT ![n] = value + 1]
             /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch, mode,
                 drainStart, expires, heldEpoch, requestEpoch, requestStart,
                 pending, grantEpoch, grantStart, grantValue, badRead>>
Read(n) == /\ LeaseValid(n)
           /\ badRead' = (badRead \/ view[n] # value)
           /\ UNCHANGED <<tick, clocks, online, paused, owner, epoch, mode,
               drainStart, value, view, expires, heldEpoch, requestEpoch,
               requestStart, pending, grantEpoch, grantStart, grantValue>>
Pause(n) == /\ n \in online /\ paused' = paused \cup {n}
            /\ UNCHANGED <<tick, clocks, online, owner, epoch, mode, drainStart,
                value, view, expires, heldEpoch, requestEpoch, requestStart,
                pending, grantEpoch, grantStart, grantValue, badRead>>
Resume(n) == /\ n \in paused /\ paused' = paused \ {n}
             /\ UNCHANGED <<tick, clocks, online, owner, epoch, mode, drainStart,
                 value, view, expires, heldEpoch, requestEpoch, requestStart,
                 pending, grantEpoch, grantStart, grantValue, badRead>>
Crash(n) == /\ n \in online /\ online' = online \ {n} /\ paused' = paused \ {n}
            /\ expires' = [expires EXCEPT ![n] = 0]
            /\ view' = [view EXCEPT ![n] = 0]
            /\ pending' = [pending EXCEPT ![n] = "none"]
            /\ UNCHANGED <<tick, clocks, owner, epoch, mode, drainStart, value,
                heldEpoch, requestEpoch, requestStart, grantEpoch, grantStart,
                grantValue, badRead>>
Restart(n) == /\ n \notin online /\ online' = online \cup {n}
              /\ UNCHANGED <<tick, clocks, paused, owner, epoch, mode, drainStart,
                  value, view, expires, heldEpoch, requestEpoch, requestStart,
                  pending, grantEpoch, grantStart, grantValue, badRead>>
Next == Tick \/ (\E n \in Nodes: Request(n) \/ Grant(n) \/ Deliver(n) \/ Fence(n)
         \/ Activate(n) \/ Commit(n) \/ Read(n) \/ Pause(n) \/ Resume(n) \/ Crash(n) \/ Restart(n))
NoStaleRead == ~badRead
Spec == Init /\ [][Next]_vars
=============================================================================
