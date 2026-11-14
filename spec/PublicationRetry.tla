------------------------- MODULE PublicationRetry -------------------------
\* Native tenant ledger updates (initialization, refill, grants and maintenance
\* claims) use head = tenant-directory root and gc = metadata collection fence.
\* Renewals/read barriers/namespace grants change revision without either root.
\* Ledger/index bytes may come from the existing bounded, digest-checked cache.
\* Only the live invocation can confirm its fresh directory identity once.
\* Abort discards that identity; no operation reconstructs it after restart.
\* A cached abandoned path still cannot publish across a changed collector fence.
\* Cache lookup/eviction stutter here; byte validation is checked in Rust.
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Attempts, MaxRevision, UnsafeRetry, UnsafeConfirmOther, UnsafeConfirmTwice
VARIABLES revision, gc, head, files, issued, phase, file, seen, snapshot, observedGC,
          collector, listed, protected, collectorSeen, published, confirmations
vars == <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,
          collector,listed,protected,collectorSeen,published,confirmations>>
Init == /\ revision = 0 /\ gc = 0 /\ head = 0 /\ files = {0} /\ issued = {}
        /\ phase = "idle" /\ file = 0 /\ seen = 0 /\ snapshot = 0 /\ observedGC = 0
        /\ collector = "idle" /\ listed = {} /\ protected = 0 /\ collectorSeen = 0
        /\ published = {} /\ confirmations = [a \in Attempts |-> 0]
Begin(a) == /\ phase = "idle" /\ a \notin issued
            /\ phase' = "read" /\ file' = a /\ issued' = issued \cup {a}
            /\ seen' = revision /\ snapshot' = head /\ observedGC' = gc
            /\ UNCHANGED <<revision,gc,head,files,collector,listed,protected,collectorSeen,published,confirmations>>
Upload == /\ phase = "read" /\ files' = files \cup {file} /\ phase' = "uploaded"
          /\ UNCHANGED <<revision,gc,head,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
Retry == /\ phase = "uploaded" /\ seen # revision /\ snapshot = head
         /\ (UnsafeRetry \/ observedGC = gc) /\ seen' = revision
         /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
\* A fresh staged directory is a one-invocation receipt. A lost reply can
\* be resolved from an exact positive (even historical) root observation.
\* Stale absence, a different identity and restart cannot restore rights.
Publish(ack) == /\ phase \in {"uploaded", "unknown"} /\ file \notin published
                /\ seen = revision /\ revision < MaxRevision
                /\ head' = file /\ revision' = revision+1
                /\ published' = published \cup {file}
                /\ phase' = IF ack THEN "idle" ELSE "unknown"
                /\ confirmations' = [confirmations EXCEPT ![file] = IF ack THEN @+1 ELSE @]
                /\ UNCHANGED <<gc,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
NoReply == /\ phase = "uploaded" /\ phase' = "unknown"
           /\ UNCHANGED <<revision,gc,head,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
Confirm(observation) ==
    /\ phase = "unknown" /\ observation \in published \cup {0}
    /\ IF UnsafeConfirmOther THEN observation # 0 ELSE observation = file
    /\ confirmations' = [confirmations EXCEPT ![file] = @+1]
    /\ phase' = IF UnsafeConfirmTwice THEN phase ELSE "idle"
    /\ UNCHANGED <<revision,gc,head,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published>>
ReadBarrier == /\ revision < MaxRevision /\ revision' = revision+1
               /\ UNCHANGED <<gc,head,files,issued,phase,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
List == /\ collector = "idle" /\ collector' = "listed" /\ listed' = files
        /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,protected,collectorSeen,published,confirmations>>
Read == /\ collector = "listed" /\ collector' = "read" /\ collectorSeen' = revision /\ protected' = head
        /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,listed,published,confirmations>>
Fence == /\ collector = "read" /\ collectorSeen = revision /\ revision < MaxRevision
         /\ revision' = revision+1 /\ gc' = revision+1 /\ collector' = "fenced"
         /\ UNCHANGED <<head,files,issued,phase,file,seen,snapshot,observedGC,listed,protected,collectorSeen,published,confirmations>>
Delete(a) == /\ collector = "fenced" /\ a \in listed \ {protected} /\ a \in files
             /\ files' = files \ {a}
             /\ UNCHANGED <<revision,gc,head,issued,phase,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
Abort == /\ phase # "idle" /\ phase' = "idle"
         /\ UNCHANGED <<revision,gc,head,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen,published,confirmations>>
Finish == /\ collector # "idle" /\ collector' = "idle"
          /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,listed,protected,collectorSeen,published,confirmations>>
Next == (\E a \in Attempts: Begin(a) \/ Delete(a)) \/ Upload \/ Retry \/ (\E ack \in BOOLEAN: Publish(ack)) \/ NoReply
        \/ (\E observation \in published \cup {0}: Confirm(observation)) \/ ReadBarrier
        \/ List \/ Read \/ Fence \/ Abort \/ Finish
TypeOK == /\ revision \in 0..MaxRevision /\ gc \in 0..revision
          /\ files \subseteq Attempts \cup {0} /\ head \in Attempts \cup {0}
          /\ published \subseteq issued /\ confirmations \in [Attempts -> Nat]
ConfirmedOnce == \A a \in Attempts: confirmations[a] <= IF a \in published THEN 1 ELSE 0
ReferencedManifestExists == head \in files
Spec == Init /\ [][Next]_vars
=============================================================================
