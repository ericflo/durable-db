-------------------------- MODULE OwnedWal --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxSeq, MaxTick, Lease, UnsafeSkipDrain, UnsafePausedClock,
          UnsafeLateGrant, UnsafeAckFromStart, UnsafeAckGap, UnsafeStaleRead,
          UnsafeRestartDrain
VARIABLES tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead
vars == <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>
Numbers == 1..MaxSeq
OldRunning == oldOnline /\ ~paused
OldValid == OldRunning /\ oldClock < expires
CurrentView == IF phase = "new" THEN <<cut, newAcknowledged>> ELSE <<oldView, FALSE>>
Prefix(s) == CHOOSE n \in 0..MaxSeq: /\ 1..n \subseteq s
               /\ (n = MaxSeq \/ n + 1 \notin s)
\* One ownership transfer: the successor remains the final owner. This is
\* a composed candidate, not general membership or a runtime timing proof.
Init == /\ tick = 0 /\ oldClock = 0 /\ newClock = 0
        /\ oldOnline = TRUE /\ newOnline = TRUE /\ paused = FALSE
        /\ phase = "old" /\ fenceClock = 0
        /\ pending = "none" /\ requested = 0 /\ grantStart = 0 /\ expires = 0
        /\ oldView = 0 /\ planned = 0 /\ uploaded = {} /\ acknowledged = {}
        /\ observed = {} /\ scanning = FALSE /\ haveScan = FALSE
        /\ sealed = FALSE /\ cut = 0
        /\ newDurable = FALSE /\ newAcknowledged = FALSE /\ badRead = FALSE
\* A paused process still accumulates elapsed time. Clock rates may differ by
\* a factor of two. Tick is a bound on real-time transitions, not a runtime clock.
Tick == (tick < MaxTick)
    /\ tick' = (tick + 1)
    /\ oldClock' \in (IF UnsafePausedClock /\ paused THEN {oldClock} ELSE {oldClock+1, oldClock+2})
    /\ newClock' \in ({newClock+1, newClock+2})
    /\ UNCHANGED <<oldOnline, newOnline, paused, phase, fenceClock, pending, requested, grantStart,
      expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Request == (OldRunning)
    /\ (pending = "none")
    /\ pending' = ("request")
    /\ requested' = (oldClock)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, grantStart,
      expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Grant == (pending = "request")
    /\ (phase = "old")
    /\ pending' = ("reply")
    /\ grantStart' = (requested)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, requested,
      expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Deliver == (OldRunning)
    /\ (pending = "reply")
    /\ pending' = ("none")
    /\ expires' = ((IF UnsafeLateGrant THEN oldClock ELSE grantStart) + Lease)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, requested,
      grantStart, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Prepare == (OldValid)
    /\ (planned < MaxSeq)
    /\ planned' = (planned + 1)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>

Upload(n) == (n \in (1..planned) \ uploaded)
    /\ uploaded' = (uploaded \cup {n})
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>

\* No global owner/epoch test on this data-path transition: the local grant
\* must remain valid after upload. Permission at Prepare alone is insufficient.
Acknowledge(n) == (OldValid \/ (UnsafeAckFromStart /\ OldRunning))
    /\ (n \in uploaded)
    /\ (n > oldView)
    /\ (UnsafeAckGap \/ n = oldView + 1)
    /\ oldView' = (n)
    /\ acknowledged' = (acknowledged \cup {n})
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, planned, uploaded, observed, scanning, haveScan, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Fence == (newOnline)
    /\ (phase = "old")
    /\ phase' = ("draining")
    /\ fenceClock' = (newClock)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>

Drain == (newOnline)
    /\ (phase = "draining")
    /\ (UnsafeSkipDrain \/ newClock >= fenceClock + 2 * Lease)
    /\ phase' = ("recovering")
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, sealed, cut, newDurable, newAcknowledged, badRead>>

BeginScan == (newOnline)
    /\ (phase = "recovering")
    /\ (~sealed)
    /\ (~scanning)
    /\ observed' = ({})
    /\ haveScan' = (FALSE)
    /\ scanning' = (TRUE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, sealed, cut, newDurable,
      newAcknowledged, badRead>>

ReadNext == (newOnline)
    /\ (scanning)
    /\ (Cardinality(observed) + 1 \in uploaded)
    /\ observed' = (observed \cup {Cardinality(observed) + 1})
    /\ haveScan' = ((Cardinality(observed) + 1 = MaxSeq))
    /\ scanning' = ((Cardinality(observed) + 1 < MaxSeq))
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, sealed, cut, newDurable,
      newAcknowledged, badRead>>

ReadMissing == (newOnline)
    /\ (scanning)
    /\ (Cardinality(observed) + 1 \notin uploaded \/ UnsafeStaleRead)
    /\ haveScan' = (TRUE)
    /\ scanning' = (FALSE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, sealed, cut,
      newDurable, newAcknowledged, badRead>>

Seal == (newOnline)
    /\ (phase = "recovering")
    /\ (haveScan)
    /\ (~sealed)
    /\ sealed' = (TRUE)
    /\ cut' = (Prefix(observed))
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, newDurable, newAcknowledged, badRead>>

Activate == (newOnline)
    /\ (phase = "recovering")
    /\ (sealed)
    /\ phase' = ("new")
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, sealed, cut, newDurable, newAcknowledged, badRead>>

NewUpload == (newOnline)
    /\ (phase = "new")
    /\ newDurable' = (TRUE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, sealed, cut, newAcknowledged, badRead>>

NewAcknowledge == (newOnline)
    /\ (phase = "new")
    /\ (newDurable)
    /\ (~newAcknowledged)
    /\ newAcknowledged' = (TRUE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, sealed, cut, newDurable, badRead>>

ReadOld == (OldValid)
    /\ badRead' = ((badRead \/ <<oldView, FALSE>> # CurrentView))
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, paused, phase, fenceClock, pending,
      requested, grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning,
      haveScan, sealed, cut, newDurable, newAcknowledged>>

Pause == (oldOnline)
    /\ paused' = (TRUE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, phase, fenceClock, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>

Resume == (oldOnline)
    /\ paused' = (FALSE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, newOnline, phase, fenceClock, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>

CrashOld == (oldOnline)
    /\ oldOnline' = (FALSE)
    /\ expires' = (0)
    /\ pending' = ("none")
    /\ oldView' = (0)
    /\ UNCHANGED <<tick, oldClock, newClock, newOnline, paused, phase, fenceClock, requested, grantStart,
      planned, uploaded, acknowledged, observed, scanning, haveScan, sealed, cut, newDurable,
      newAcknowledged, badRead>>

CrashNew == (newOnline)
    /\ (phase # "new")
    /\ newOnline' = (FALSE)
    /\ observed' = ({})
    /\ scanning' = (FALSE)
    /\ haveScan' = (FALSE)
    /\ UNCHANGED <<tick, oldClock, newClock, oldOnline, paused, phase, fenceClock, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, sealed, cut, newDurable,
      newAcknowledged, badRead>>

\* A replacement process can have a different clock origin. Restart a pending
\* drain interval on that process; timestamps from the former clock are unusable.
\* A completed, durable drain decision and a durable seal survive replacement.
RestartNew == (~newOnline)
    /\ newOnline' = (TRUE)
    /\ newClock' \in {0, 2 * MaxTick}
    /\ fenceClock' = (IF phase = "draining" /\ ~UnsafeRestartDrain
                      THEN newClock' ELSE fenceClock)
    /\ UNCHANGED <<tick, oldClock, oldOnline, paused, phase, pending, requested,
      grantStart, expires, oldView, planned, uploaded, acknowledged, observed, scanning, haveScan,
      sealed, cut, newDurable, newAcknowledged, badRead>>
Next == Tick \/ Request \/ Grant \/ Deliver \/ Prepare \/ Fence \/ Drain \/ BeginScan \/ ReadNext \/
      ReadMissing \/ Seal \/ Activate \/ NewUpload \/ NewAcknowledge \/ ReadOld \/ Pause \/ Resume \/
      CrashOld \/ CrashNew \/ RestartNew
    \/ (\E n \in Numbers: Upload(n) \/ Acknowledge(n))
NoStaleRead == ~badRead
AcknowledgedPrefixPreserved == sealed => acknowledged \subseteq 1..cut
NewAcknowledgementDurable == newAcknowledged => newDurable
TypeOK == /\ planned \in 0..MaxSeq /\ uploaded \subseteq 1..planned
          /\ acknowledged \subseteq uploaded /\ cut \in 0..MaxSeq
          /\ observed = 1..Cardinality(observed)
Spec == Init /\ [][Next]_vars
=====================================================================
