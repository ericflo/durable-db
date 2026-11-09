------------------------- MODULE PublicationRebase -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS MaxRevision, UnsafeFence, UnsafeTarget, UnsafeMerge
VARIABLES revision, gc, head, other, latestOther, files, issued, phase,
          seen, baseHead, baseGC, planOther, history, overwritten,
          collector, listed, protected, collectorSeen
vars == <<revision,gc,head,other,latestOther,files,issued,phase,
          seen,baseHead,baseGC,planOther,history,overwritten,
          collector,listed,protected,collectorSeen>>
Snapshot(r,g,h,o) == [rev |-> r, fence |-> g, target |-> h, rest |-> o]
Remember == history' = history \cup {Snapshot(revision',gc',head',other')}
Init == /\ revision = 0 /\ gc = 0 /\ head = 0 /\ other = 0 /\ latestOther = 0
        /\ files = {0} /\ issued = {} /\ phase = "idle" /\ seen = 0
        /\ baseHead = 0 /\ baseGC = 0 /\ planOther = 0 /\ overwritten = FALSE
        /\ history = {Snapshot(0,0,0,0)}
        /\ collector = "idle" /\ listed = {} /\ protected = 0 /\ collectorSeen = 0
\* Raw provider observations may be arbitrarily stale. Revision comparison,
\* not freshness of this observation, decides whether publication can succeed.
Begin(h) == /\ phase = "idle" /\ 1 \notin issued
            /\ phase' = "read" /\ issued' = issued \cup {1}
            /\ seen' = h.rev /\ baseHead' = h.target /\ baseGC' = h.fence
            /\ planOther' = h.rest
            /\ UNCHANGED <<revision,gc,head,other,latestOther,files,history,overwritten,
                           collector,listed,protected,collectorSeen>>
Upload == /\ phase = "read" /\ files' = files \cup {1} /\ phase' = "uploaded"
          /\ UNCHANGED <<revision,gc,head,other,latestOther,issued,seen,baseHead,baseGC,
                         planOther,history,overwritten,collector,listed,protected,collectorSeen>>
\* Rebuilding the persistent directory must copy every unrelated entry from
\* the newly observed root. Reusing the old path would lose an acknowledged write.
Rebase(h) == /\ phase = "uploaded" /\ seen # revision
             /\ (UnsafeFence \/ h.fence = baseGC)
             /\ (UnsafeTarget \/ h.target = baseHead)
             /\ seen' = h.rev
             /\ planOther' = IF UnsafeMerge THEN planOther ELSE h.rest
             /\ UNCHANGED <<revision,gc,head,other,latestOther,files,issued,phase,
                            baseHead,baseGC,history,overwritten,collector,listed,protected,collectorSeen>>
Publish == /\ phase = "uploaded" /\ seen = revision /\ revision < MaxRevision
           /\ head' = 1 /\ other' = planOther /\ revision' = revision+1
           /\ phase' = "idle" /\ overwritten' = (head # baseHead)
           /\ UNCHANGED <<gc,latestOther,files,issued,seen,baseHead,baseGC,planOther,
                          collector,listed,protected,collectorSeen>>
           /\ Remember
\* Completed valid writes by competing publishers. Fresh identities cannot be reused.
OtherWrite == /\ revision < MaxRevision
              /\ revision' = revision+1 /\ other' = other+1 /\ latestOther' = latestOther+1
              /\ UNCHANGED <<gc,head,files,issued,phase,seen,baseHead,baseGC,planOther,
                             overwritten,collector,listed,protected,collectorSeen>>
              /\ Remember
TargetWrite == /\ revision < MaxRevision /\ 2 \notin issued
               /\ revision' = revision+1 /\ head' = 2 /\ files' = files \cup {2}
               /\ issued' = issued \cup {2}
               /\ UNCHANGED <<gc,other,latestOther,phase,seen,baseHead,baseGC,planOther,
                              overwritten,collector,listed,protected,collectorSeen>>
               /\ Remember
ReadBarrier == /\ revision < MaxRevision /\ revision' = revision+1
               /\ UNCHANGED <<gc,head,other,latestOther,files,issued,phase,seen,baseHead,baseGC,
                              planOther,overwritten,collector,listed,protected,collectorSeen>>
               /\ Remember
List == /\ collector = "idle" /\ collector' = "listed" /\ listed' = files
        /\ UNCHANGED <<revision,gc,head,other,latestOther,files,issued,phase,seen,baseHead,
                       baseGC,planOther,history,overwritten,protected,collectorSeen>>
Read(h) == /\ collector = "listed" /\ collector' = "read"
           /\ collectorSeen' = h.rev /\ protected' = h.target
           /\ UNCHANGED <<revision,gc,head,other,latestOther,files,issued,phase,seen,baseHead,
                          baseGC,planOther,history,overwritten,listed>>
Fence == /\ collector = "read" /\ collectorSeen = revision /\ revision < MaxRevision
         /\ revision' = revision+1 /\ gc' = revision+1 /\ collector' = "fenced"
         /\ UNCHANGED <<head,other,latestOther,files,issued,phase,seen,baseHead,baseGC,
                        planOther,overwritten,listed,protected,collectorSeen>>
         /\ Remember
Delete(a) == /\ collector = "fenced" /\ a \in listed \ {protected} /\ a \in files
             /\ files' = files \ {a}
             /\ UNCHANGED <<revision,gc,head,other,latestOther,issued,phase,seen,baseHead,baseGC,
                            planOther,history,overwritten,collector,listed,protected,collectorSeen>>
Abort == /\ phase # "idle" /\ phase' = "idle"
         /\ UNCHANGED <<revision,gc,head,other,latestOther,files,issued,seen,baseHead,baseGC,
                        planOther,history,overwritten,collector,listed,protected,collectorSeen>>
Finish == /\ collector # "idle" /\ collector' = "idle"
          /\ UNCHANGED <<revision,gc,head,other,latestOther,files,issued,phase,seen,baseHead,baseGC,
                         planOther,history,overwritten,listed,protected,collectorSeen>>
Next == (\E h \in history: Begin(h) \/ Rebase(h) \/ Read(h))
        \/ (\E a \in {0,1,2}: Delete(a))
        \/ Upload \/ Publish \/ OtherWrite \/ TargetWrite \/ ReadBarrier
        \/ List \/ Fence \/ Abort \/ Finish
TypeOK == /\ revision \in 0..MaxRevision /\ gc \in 0..revision
          /\ files \subseteq {0,1,2} /\ head \in {0,1,2}
          /\ other \in 0..MaxRevision /\ latestOther \in 0..MaxRevision
ReferencedManifestExists == head \in files
OtherWritesPreserved == other = latestOther
TargetWriteNotOverwritten == ~overwritten
Spec == Init /\ [][Next]_vars
=============================================================================
