------------------------- MODULE PublicationRetry -------------------------
\* Native tenant ledger updates (initialization, refill, grants and maintenance
\* claims) use head = tenant-directory root and gc = metadata collection fence.
\* Renewals/read barriers/namespace grants change revision without either root.
\* Ledger/index bytes may come from the existing bounded, digest-checked cache.
\* This does not cache the authority comparison or recreate volatile credit.
\* A cached abandoned path still cannot publish across a changed collector fence.
\* Cache lookup/eviction stutter here; byte validation is checked in Rust.
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Attempts, MaxRevision, UnsafeRetry
VARIABLES revision, gc, head, files, issued, phase, file, seen, snapshot, observedGC,
          collector, listed, protected, collectorSeen
vars == <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,
          collector,listed,protected,collectorSeen>>
Init == /\ revision = 0 /\ gc = 0 /\ head = 0 /\ files = {0} /\ issued = {}
        /\ phase = "idle" /\ file = 0 /\ seen = 0 /\ snapshot = 0 /\ observedGC = 0
        /\ collector = "idle" /\ listed = {} /\ protected = 0 /\ collectorSeen = 0
Begin(a) == /\ phase = "idle" /\ a \notin issued
            /\ phase' = "read" /\ file' = a /\ issued' = issued \cup {a}
            /\ seen' = revision /\ snapshot' = head /\ observedGC' = gc
            /\ UNCHANGED <<revision,gc,head,files,collector,listed,protected,collectorSeen>>
Upload == /\ phase = "read" /\ files' = files \cup {file} /\ phase' = "uploaded"
          /\ UNCHANGED <<revision,gc,head,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
Retry == /\ phase = "uploaded" /\ seen # revision /\ snapshot = head
         /\ (UnsafeRetry \/ observedGC = gc) /\ seen' = revision
         /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,snapshot,observedGC,collector,listed,protected,collectorSeen>>
Publish == /\ phase = "uploaded" /\ seen = revision /\ revision < MaxRevision
           /\ head' = file /\ revision' = revision+1 /\ phase' = "idle"
           /\ UNCHANGED <<gc,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
ReadBarrier == /\ revision < MaxRevision /\ revision' = revision+1
               /\ UNCHANGED <<gc,head,files,issued,phase,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
List == /\ collector = "idle" /\ collector' = "listed" /\ listed' = files
        /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,protected,collectorSeen>>
Read == /\ collector = "listed" /\ collector' = "read" /\ collectorSeen' = revision /\ protected' = head
        /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,listed>>
Fence == /\ collector = "read" /\ collectorSeen = revision /\ revision < MaxRevision
         /\ revision' = revision+1 /\ gc' = revision+1 /\ collector' = "fenced"
         /\ UNCHANGED <<head,files,issued,phase,file,seen,snapshot,observedGC,listed,protected,collectorSeen>>
Delete(a) == /\ collector = "fenced" /\ a \in listed \ {protected} /\ a \in files
             /\ files' = files \ {a}
             /\ UNCHANGED <<revision,gc,head,issued,phase,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
Abort == /\ phase # "idle" /\ phase' = "idle"
         /\ UNCHANGED <<revision,gc,head,files,issued,file,seen,snapshot,observedGC,collector,listed,protected,collectorSeen>>
Finish == /\ collector # "idle" /\ collector' = "idle"
          /\ UNCHANGED <<revision,gc,head,files,issued,phase,file,seen,snapshot,observedGC,listed,protected,collectorSeen>>
Next == (\E a \in Attempts: Begin(a) \/ Delete(a)) \/ Upload \/ Retry \/ Publish \/ ReadBarrier
        \/ List \/ Read \/ Fence \/ Abort \/ Finish
TypeOK == /\ revision \in 0..MaxRevision /\ gc \in 0..revision
          /\ files \subseteq Attempts \cup {0} /\ head \in Attempts \cup {0}
ReferencedManifestExists == head \in files
Spec == Init /\ [][Next]_vars
=============================================================================
