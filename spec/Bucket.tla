------------------------------ MODULE Bucket ------------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Nodes, Databases, Attempts, MaxRevision, UnsafeGC
VARIABLES revision, heads, files, issued, online, connected, writer, collector
vars == <<revision, heads, files, issued, online, connected, writer, collector>>
IdleWriter == [phase |-> "idle", seen |-> 0, db |-> CHOOSE d \in Databases: TRUE, file |-> 0]
IdleCollector == [phase |-> "idle", seen |-> 0, listed |-> {}, protected |-> {}]
Init == /\ revision = 0 /\ heads = [d \in Databases |-> 0]
        /\ files = {0} /\ issued = {} /\ online = Nodes /\ connected = Nodes
        /\ writer = [n \in Nodes |-> IdleWriter]
        /\ collector = [n \in Nodes |-> IdleCollector]
Available(n) == n \in online /\ n \in connected
Begin(n,d,a) ==
  /\ Available(n) /\ writer[n].phase = "idle" /\ a \notin issued
  /\ writer' = [writer EXCEPT ![n] = [phase |-> "read", seen |-> revision, db |-> d, file |-> a]]
  /\ issued' = issued \cup {a}
  /\ UNCHANGED <<revision, heads, files, online, connected, collector>>
Upload(n) ==
  /\ Available(n) /\ writer[n].phase = "read"
  /\ files' = files \cup {writer[n].file}
  /\ writer' = [writer EXCEPT ![n].phase = "uploaded"]
  /\ UNCHANGED <<revision, heads, issued, online, connected, collector>>
Publish(n) ==
  /\ Available(n) /\ writer[n].phase = "uploaded" /\ revision < MaxRevision
  /\ writer[n].seen = revision
  /\ heads' = [heads EXCEPT ![writer[n].db] = writer[n].file]
  /\ revision' = revision + 1
  /\ writer' = [writer EXCEPT ![n].phase = "idle"]
  /\ UNCHANGED <<files, issued, online, connected, collector>>
Abort(n) ==
  /\ writer[n].phase # "idle"
  /\ writer' = [writer EXCEPT ![n].phase = "idle"]
  /\ UNCHANGED <<revision, heads, files, issued, online, connected, collector>>
List(n) ==
  /\ Available(n) /\ collector[n].phase = "idle"
  /\ collector' = [collector EXCEPT ![n].phase = "listed", ![n].listed = files]
  /\ UNCHANGED <<revision, heads, files, issued, online, connected, writer>>
Read(n) ==
  /\ Available(n) /\ collector[n].phase = "listed"
  /\ collector' = [collector EXCEPT ![n].phase = "read", ![n].seen = revision,
        ![n].protected = {heads[d]: d \in Databases}]
  /\ UNCHANGED <<revision, heads, files, issued, online, connected, writer>>
Fence(n) ==
  /\ Available(n) /\ collector[n].phase = "read" /\ collector[n].seen = revision
  /\ revision < MaxRevision
  /\ revision' = IF UnsafeGC THEN revision ELSE revision + 1
  /\ collector' = [collector EXCEPT ![n].phase = "fenced"]
  /\ UNCHANGED <<heads, files, issued, online, connected, writer>>
Delete(n,a) ==
  /\ Available(n) /\ collector[n].phase = "fenced"
  /\ a \in collector[n].listed \ collector[n].protected /\ a \in files
  /\ files' = files \ {a}
  /\ UNCHANGED <<revision, heads, issued, online, connected, writer, collector>>
Finish(n) ==
  /\ collector[n].phase # "idle"
  /\ collector' = [collector EXCEPT ![n] = IdleCollector]
  /\ UNCHANGED <<revision, heads, files, issued, online, connected, writer>>
Scale(n) ==
  /\ online' = IF n \in online THEN online \ {n} ELSE online \cup {n}
  /\ writer' = [writer EXCEPT ![n] = IdleWriter]
  /\ collector' = [collector EXCEPT ![n] = IdleCollector]
  /\ UNCHANGED <<revision, heads, files, issued, connected>>
Partition(n) ==
  /\ connected' = IF n \in connected THEN connected \ {n} ELSE connected \cup {n}
  /\ UNCHANGED <<revision, heads, files, issued, online, writer, collector>>
Next == \E n \in Nodes:
  \/ (\E d \in Databases, a \in Attempts: Begin(n,d,a))
  \/ Upload(n) \/ Publish(n) \/ Abort(n) \/ List(n) \/ Read(n) \/ Fence(n)
  \/ (\E a \in Attempts \cup {0}: Delete(n,a))
  \/ Finish(n) \/ Scale(n) \/ Partition(n)
TypeOK == /\ revision \in 0..MaxRevision /\ heads \in [Databases -> Attempts \cup {0}]
          /\ files \subseteq Attempts \cup {0}
ReferencedManifestsExist == {heads[d]: d \in Databases} \subseteq files
Spec == Init /\ [][Next]_vars
=============================================================================
