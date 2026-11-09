------------------------ MODULE DelegatedGrant ------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Children, Lease, MaxTick, UnsafeFullRemaining, UnsafeLateReply,
          UnsafePausedRoot, UnsafePausedChild, UnsafeSkipDrain
VARIABLES tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused, children
vars == <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused, children>>

\* One already acquired root grant. Its request-start acquisition is checked
\* separately in OwnedWal. Each child requests at most one delegated grant.
Init == /\ tick = 0 /\ rootClock = 0 /\ nextClock = 0
        /\ phase = "old" /\ fenceClock = 0
        /\ rootOnline = TRUE /\ rootPaused = FALSE
        /\ children = [c \in Children |->
             [clock |-> 0, requested |-> 0, pending |-> "idle", duration |-> 0,
              expires |-> 0, online |-> TRUE, paused |-> FALSE]]
RootValid == rootOnline /\ ~rootPaused /\ rootClock < Lease
Running(c) == children[c].online /\ ~children[c].paused
Valid(c) == Running(c) /\ children[c].clock < children[c].expires

Tick == /\ tick < MaxTick
        /\ tick' = tick + 1
        /\ rootClock' \in (IF UnsafePausedRoot /\ rootPaused
                          THEN {rootClock} ELSE {rootClock + 1, rootClock + 2})
        /\ nextClock' \in {nextClock + 1, nextClock + 2}
        /\ \E advance \in [Children -> {1, 2}]:
             children' = [c \in Children |->
               [children[c] EXCEPT !.clock =
                 IF UnsafePausedChild /\ children[c].paused THEN @ ELSE @ + advance[c]]]
        /\ UNCHANGED <<phase, fenceClock, rootOnline, rootPaused>>

Request(c) == /\ Running(c) /\ children[c].pending = "idle"
              /\ children' = [children EXCEPT ![c].pending = "request",
                               ![c].requested = children[c].clock]
              /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused>>

\* Convert the root's remaining time conservatively. At the modeled rate
\* bounds, a slow child can spend at most half a fast root's remaining ticks.
\* The old root may issue while fenced, but no child may outlive its grant.
Issue(c) == /\ RootValid /\ children[c].pending = "request"
            /\ children' = [children EXCEPT ![c].pending = "reply",
                 ![c].duration = IF UnsafeFullRemaining THEN Lease - rootClock
                                ELSE (Lease - rootClock) \div 2]
            /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused>>

Deliver(c) == /\ Running(c) /\ children[c].pending = "reply"
              /\ children' = [children EXCEPT ![c].pending = "done",
                   ![c].expires = (IF UnsafeLateReply THEN children[c].clock
                                   ELSE children[c].requested) + children[c].duration]
              /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused>>

Fence == /\ phase = "old" /\ phase' = "draining" /\ fenceClock' = nextClock
         /\ UNCHANGED <<tick, rootClock, nextClock, rootOnline, rootPaused, children>>
Activate == /\ phase = "draining"
            /\ (UnsafeSkipDrain \/ nextClock >= fenceClock + 2 * Lease)
            /\ phase' = "new"
            /\ UNCHANGED <<tick, rootClock, nextClock, fenceClock, rootOnline, rootPaused, children>>
PauseRoot == /\ rootOnline /\ rootPaused' = ~rootPaused
             /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, children>>
PauseChild(c) == /\ children[c].online
                 /\ children' = [children EXCEPT ![c].paused = ~@]
                 /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused>>
CrashRoot == /\ rootOnline /\ rootOnline' = FALSE
             /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootPaused, children>>
CrashChild(c) == /\ children[c].online
                 /\ children' = [children EXCEPT ![c].online = FALSE, ![c].expires = 0,
                                  ![c].pending = "done"]
                 /\ UNCHANGED <<tick, rootClock, nextClock, phase, fenceClock, rootOnline, rootPaused>>

Next == Tick \/ Fence \/ Activate \/ PauseRoot \/ CrashRoot
        \/ (\E c \in Children: Request(c) \/ Issue(c) \/ Deliver(c) \/ PauseChild(c) \/ CrashChild(c))
LeafWithinRootPermission == \A c \in Children: Valid(c) => rootClock < Lease
NoOldPermissionAfterActivation == phase = "new" => \A c \in Children: ~Valid(c)
TypeOK == /\ tick \in 0..MaxTick /\ phase \in {"old", "draining", "new"}
          /\ \A c \in Children:
               /\ children[c].duration \in 0..Lease
               /\ children[c].pending \in {"idle", "request", "reply", "done"}
Spec == Init /\ [][Next]_vars
=======================================================================
