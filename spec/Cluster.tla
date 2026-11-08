---------------------------- MODULE Cluster ----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Nodes, Requests, MaxEpoch, UnsafeGC
VARIABLES online, connected, phase, request, kept, seen, objects, root, epoch,
          history, acknowledged, issued, candidates, protected, gcPhase, gcSeen

vars == <<online, connected, phase, request, kept, seen, objects, root, epoch,
          history, acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

Init ==
  /\ online = Nodes
  /\ connected = Nodes
  /\ phase = [n \in Nodes |-> "idle"]
  /\ request = [n \in Nodes |-> 0]
  /\ kept = [n \in Nodes |-> {}]
  /\ seen = [n \in Nodes |-> 0]
  /\ objects = {0}
  /\ root = {0}
  /\ epoch = 0
  /\ history = <<>>
  /\ acknowledged = {}
  /\ issued = {}
  /\ candidates = [n \in Nodes |-> {}]
  /\ protected = [n \in Nodes |-> {}]
  /\ gcPhase = [n \in Nodes |-> "idle"]
  /\ gcSeen = [n \in Nodes |-> 0]

Available(n) == n \in online /\ n \in connected

Begin(n, r, k) ==
  /\ Available(n) /\ phase[n] = "idle" /\ r \notin issued /\ k \subseteq root
  /\ phase' = [phase EXCEPT ![n] = "read"]
  /\ request' = [request EXCEPT ![n] = r]
  /\ kept' = [kept EXCEPT ![n] = k]
  /\ seen' = [seen EXCEPT ![n] = epoch]
  /\ issued' = issued \cup {r}
  /\ UNCHANGED <<online, connected, objects, root, epoch, history,
       acknowledged, candidates, protected, gcPhase, gcSeen>>

Upload(n) ==
  /\ Available(n) /\ phase[n] = "read"
  /\ objects' = objects \cup {request[n]}
  /\ phase' = [phase EXCEPT ![n] = "uploaded"]
  /\ UNCHANGED <<online, connected, request, kept, seen, root, epoch, history,
       acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

Commit(n) ==
  /\ Available(n) /\ phase[n] = "uploaded" /\ seen[n] = epoch
  /\ epoch < MaxEpoch
  /\ root' = kept[n] \cup {request[n]}
  /\ epoch' = epoch + 1
  /\ history' = Append(history, [id |-> request[n], refs |-> kept[n] \cup {request[n]}])
  /\ phase' = [phase EXCEPT ![n] = "committed"]
  /\ UNCHANGED <<online, connected, request, kept, seen, objects, acknowledged,
       issued, candidates, protected, gcPhase, gcSeen>>

Conflict(n) ==
  /\ Available(n) /\ phase[n] = "uploaded" /\ seen[n] # epoch
  /\ phase' = [phase EXCEPT ![n] = "idle"]
  \* A retry starts a NEW attempt with new object names. Never reuse an orphan.
  /\ UNCHANGED <<online, connected, request, kept, seen, objects, root, epoch,
       history, acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

Ack(n) ==
  /\ Available(n) /\ phase[n] = "committed"
  /\ acknowledged' = acknowledged \cup {request[n]}
  /\ phase' = [phase EXCEPT ![n] = "idle"]
  /\ UNCHANGED <<online, connected, request, kept, seen, objects, root, epoch,
       history, issued, candidates, protected, gcPhase, gcSeen>>

ListGarbage(n) ==
  /\ Available(n) /\ gcPhase[n] = "idle"
  /\ candidates' = [candidates EXCEPT ![n] = objects]
  /\ gcPhase' = [gcPhase EXCEPT ![n] = "listed"]
  /\ UNCHANGED <<online, connected, phase, request, kept, seen, objects, root,
       epoch, history, acknowledged, issued, protected, gcSeen>>

ReadFence(n) ==
  /\ Available(n) /\ gcPhase[n] = "listed"
  /\ protected' = [protected EXCEPT ![n] = root]
  /\ gcSeen' = [gcSeen EXCEPT ![n] = epoch]
  /\ gcPhase' = [gcPhase EXCEPT ![n] = "read"]
  /\ UNCHANGED <<online, connected, phase, request, kept, seen, objects, root,
       epoch, history, acknowledged, issued, candidates>>

Fence(n) ==
  /\ Available(n) /\ gcPhase[n] = "read" /\ epoch < MaxEpoch
  /\ gcSeen[n] = epoch
  /\ epoch' = IF UnsafeGC THEN epoch ELSE epoch + 1
  /\ gcPhase' = [gcPhase EXCEPT ![n] = "fenced"]
  /\ UNCHANGED <<online, connected, phase, request, kept, seen, objects, root,
       history, acknowledged, issued, candidates, protected, gcSeen>>

Collect(n, o) ==
  /\ Available(n) /\ gcPhase[n] = "fenced"
  /\ o \in candidates[n] /\ o \notin protected[n] /\ o \in objects
  /\ objects' = objects \ {o}
  /\ UNCHANGED <<online, connected, phase, request, kept, seen, root, epoch,
       history, acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

FinishGC(n) ==
  /\ Available(n) /\ gcPhase[n] # "idle"
  /\ gcPhase' = [gcPhase EXCEPT ![n] = "idle"]
  /\ candidates' = [candidates EXCEPT ![n] = {}]
  /\ UNCHANGED <<online, connected, phase, request, kept, seen, objects, root,
       epoch, history, acknowledged, issued, protected, gcSeen>>

CrashOrScaleDown(n) ==
  /\ n \in online
  /\ online' = online \ {n}
  /\ phase' = [phase EXCEPT ![n] = "idle"]
  /\ gcPhase' = [gcPhase EXCEPT ![n] = "idle"]
  /\ candidates' = [candidates EXCEPT ![n] = {}]
  /\ UNCHANGED <<connected, request, kept, seen, objects, root, epoch, history,
       acknowledged, issued, protected, gcSeen>>

StartOrScaleUp(n) ==
  /\ n \notin online /\ online' = online \cup {n}
  /\ UNCHANGED <<connected, phase, request, kept, seen, objects, root, epoch,
       history, acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

Partition(n) ==
  /\ connected' = IF n \in connected THEN connected \ {n} ELSE connected \cup {n}
  /\ UNCHANGED <<online, phase, request, kept, seen, objects, root, epoch, history,
       acknowledged, issued, candidates, protected, gcPhase, gcSeen>>

Next == \E n \in Nodes:
  \/ (\E r \in Requests, k \in SUBSET root: Begin(n,r,k))
  \/ Upload(n) \/ Commit(n) \/ Conflict(n) \/ Ack(n)
  \/ ListGarbage(n) \/ ReadFence(n) \/ Fence(n)
  \/ (\E o \in Requests \cup {0}: Collect(n,o))
  \/ FinishGC(n) \/ CrashOrScaleDown(n) \/ StartOrScaleUp(n) \/ Partition(n)

TypeOK == /\ online \subseteq Nodes /\ connected \subseteq Nodes
          /\ root \subseteq Requests \cup {0} /\ objects \subseteq Requests \cup {0}
          /\ epoch \in 0..MaxEpoch /\ issued \subseteq Requests
ReferencedObjectsExist == root \subseteq objects
AcknowledgedCommitsPersist == acknowledged \subseteq {history[i].id : i \in 1..Len(history)}
NoDuplicateCommit == Cardinality({history[i].id : i \in 1..Len(history)}) = Len(history)
CurrentIsLatest == IF Len(history) = 0 THEN root = {0} ELSE root = history[Len(history)].refs
Spec == Init /\ [][Next]_vars
=============================================================================
