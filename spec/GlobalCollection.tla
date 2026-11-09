-------------------------- MODULE GlobalCollection --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Nodes, Jobs, Databases, Collectors, MaxRevision,
          UnsafeOriginFence, UnsafeNoFence, UnsafeLateList
VARIABLES running, begun, owner, target, expected, kept, originFence, uploaded,
          abandoned, submitted, proposalFence, published, acknowledged,
          objects, root, revision, fence, gc, candidates, observedRoot,
          observedRevision, protected, lateListed
vars == <<running, begun, owner, target, expected, kept, originFence, uploaded,
          abandoned, submitted, proposalFence, published, acknowledged,
          objects, root, revision, fence, gc, candidates, observedRoot,
          observedRevision, protected, lateListed>>
None == "none"
EmptyRoot == [d \in Databases |-> None]
Pages(j) == IF j = None THEN {} ELSE {j} \cup kept[j]
References(r) == UNION {Pages(r[d]): d \in Databases}

Init == /\ running = Nodes /\ begun = {} /\ uploaded = {} /\ abandoned = {}
        /\ submitted = {} /\ published = {} /\ acknowledged = {} /\ objects = {}
        /\ owner = [j \in Jobs |-> None] /\ target = [j \in Jobs |-> None]
        /\ expected = [j \in Jobs |-> None] /\ kept = [j \in Jobs |-> {}]
        /\ originFence = [j \in Jobs |-> 0] /\ proposalFence = [j \in Jobs |-> 0]
        /\ root = EmptyRoot /\ revision = 0 /\ fence = 0
        /\ gc = [c \in Collectors |-> "idle"] /\ candidates = [c \in Collectors |-> {}]
        /\ observedRoot = [c \in Collectors |-> EmptyRoot]
        /\ observedRevision = [c \in Collectors |-> 0]
        /\ protected = [c \in Collectors |-> EmptyRoot] /\ lateListed = [c \in Collectors |-> FALSE]

\* The engine snapshots both its database pointer and the global data fence
\* before preparing any page. A page may arrive even after that process dies.
Begin(j,n,d,k) ==
    /\ j \notin begun /\ n \in running /\ k \subseteq Pages(root[d])
    /\ begun' = begun \cup {j}
    /\ owner' = [owner EXCEPT ![j] = n] /\ target' = [target EXCEPT ![j] = d]
    /\ expected' = [expected EXCEPT ![j] = root[d]]
    /\ kept' = [kept EXCEPT ![j] = k]
    /\ originFence' = [originFence EXCEPT ![j] = fence]
    /\ UNCHANGED <<running, uploaded, abandoned, submitted, proposalFence,
          published, acknowledged, objects, root, revision, fence, gc, candidates,
          observedRoot, observedRevision, protected, lateListed>>
Upload(j) ==
    /\ j \in begun \ uploaded
    /\ uploaded' = uploaded \cup {j} /\ objects' = objects \cup {j}
    /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence,
          abandoned, submitted, proposalFence, published, acknowledged, root,
          revision, fence, gc, candidates, observedRoot, observedRevision, protected, lateListed>>
Submit(j) ==
    /\ j \in uploaded \ (abandoned \cup submitted)
    /\ owner[j] \in running
    /\ UnsafeOriginFence \/ originFence[j] = fence
    /\ submitted' = submitted \cup {j}
    /\ proposalFence' = [proposalFence EXCEPT ![j] = fence]
    /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
          abandoned, published, acknowledged, objects, root, revision, fence,
          gc, candidates, observedRoot, observedRevision, protected, lateListed>>
\* Any live helper can publish an abandoned origin's durable proposal.
Publish(j) ==
    /\ running # {} /\ j \in submitted \ published /\ revision < MaxRevision
    /\ root[target[j]] = expected[j] /\ proposalFence[j] = fence
    /\ published' = published \cup {j} /\ root' = [root EXCEPT ![target[j]] = j]
    /\ revision' = revision + 1
    /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
          abandoned, submitted, proposalFence, acknowledged, objects, fence,
          gc, candidates, observedRoot, observedRevision, protected, lateListed>>
Ack(j) == /\ j \in published \ acknowledged /\ owner[j] \in running /\ j \notin abandoned
          /\ acknowledged' = acknowledged \cup {j}
          /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
              abandoned, submitted, proposalFence, published, objects, root, revision,
              fence, gc, candidates, observedRoot, observedRevision, protected, lateListed>>
Crash(n) == /\ n \in running /\ running' = running \ {n}
            /\ abandoned' = abandoned \cup {j \in begun: owner[j] = n}
            /\ UNCHANGED <<begun, owner, target, expected, kept, originFence, uploaded,
                submitted, proposalFence, published, acknowledged, objects, root,
                revision, fence, gc, candidates, observedRoot, observedRevision, protected, lateListed>>
Restart(n) == /\ n \notin running /\ running' = running \cup {n}
              /\ UNCHANGED <<begun, owner, target, expected, kept, originFence, uploaded,
                  abandoned, submitted, proposalFence, published, acknowledged, objects, root,
                  revision, fence, gc, candidates, observedRoot, observedRevision, protected, lateListed>>

List(c) == /\ running # {} /\ gc[c] = "idle" /\ candidates' = [candidates EXCEPT ![c] = objects] /\ gc' = [gc EXCEPT ![c] = "listed"]
        /\ lateListed' = [lateListed EXCEPT ![c] = FALSE]
        /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
            abandoned, submitted, proposalFence, published, acknowledged, objects, root,
            revision, fence, observedRoot, observedRevision, protected>>
Observe(c) == /\ running # {} /\ gc[c] = "listed" /\ gc' = [gc EXCEPT ![c] = "observed"]
           /\ observedRoot' = [observedRoot EXCEPT ![c] = root] /\ observedRevision' = [observedRevision EXCEPT ![c] = revision]
           /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
               abandoned, submitted, proposalFence, published, acknowledged, objects,
               root, revision, fence, candidates, protected, lateListed>>
Seal(c) == /\ running # {} /\ gc[c] = "observed" /\ observedRevision[c] = revision
        /\ revision < MaxRevision /\ revision' = revision + 1
        /\ fence' = (IF UnsafeNoFence THEN fence ELSE fence + 1)
        /\ protected' = [protected EXCEPT ![c] = observedRoot[c]] /\ gc' = [gc EXCEPT ![c] = "sealed"]
        /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
            abandoned, submitted, proposalFence, published, acknowledged, objects,
            root, candidates, observedRoot, observedRevision, lateListed>>
LateList(c) == /\ UnsafeLateList /\ running # {} /\ gc[c] = "sealed" /\ ~lateListed[c]
            /\ candidates' = [candidates EXCEPT ![c] = objects] /\ lateListed' = [lateListed EXCEPT ![c] = TRUE]
            /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
                abandoned, submitted, proposalFence, published, acknowledged, objects,
                root, revision, fence, gc, observedRoot, observedRevision, protected>>
Delete(c) == /\ gc[c] = "sealed"
          /\ objects' = objects \ (candidates[c] \ References(protected[c])) /\ gc' = [gc EXCEPT ![c] = "idle"]
          /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
              abandoned, submitted, proposalFence, published, acknowledged, root,
              revision, fence, candidates, observedRoot, observedRevision, protected, lateListed>>
AbandonSweep(c) == /\ gc[c] # "idle" /\ gc' = [gc EXCEPT ![c] = "idle"]
                /\ UNCHANGED <<running, begun, owner, target, expected, kept, originFence, uploaded,
                    abandoned, submitted, proposalFence, published, acknowledged, objects,
                    root, revision, fence, candidates, observedRoot, observedRevision, protected, lateListed>>

Next == (\E j \in Jobs, n \in Nodes, d \in Databases, k \in SUBSET Jobs: Begin(j,n,d,k))
        \/ (\E j \in Jobs: Upload(j) \/ Submit(j) \/ Publish(j) \/ Ack(j))
        \/ (\E n \in Nodes: Crash(n) \/ Restart(n))
        \/ (\E c \in Collectors: List(c) \/ Observe(c) \/ Seal(c) \/ LateList(c) \/ Delete(c) \/ AbandonSweep(c))
TypeOK == /\ running \subseteq Nodes /\ begun \subseteq Jobs /\ uploaded \subseteq begun
          /\ submitted \subseteq uploaded /\ published \subseteq submitted
          /\ objects \subseteq uploaded /\ abandoned \subseteq begun
          /\ owner \in [Jobs -> Nodes \cup {None}] /\ target \in [Jobs -> Databases \cup {None}]
          /\ kept \in [Jobs -> SUBSET Jobs]
          /\ expected \in [Jobs -> Jobs \cup {None}] /\ root \in [Databases -> Jobs \cup {None}]
          /\ originFence \in [Jobs -> 0..MaxRevision] /\ proposalFence \in [Jobs -> 0..MaxRevision]
          /\ revision \in 0..MaxRevision /\ fence \in 0..MaxRevision
          /\ gc \in [Collectors -> {"idle", "listed", "observed", "sealed"}]
          /\ candidates \in [Collectors -> SUBSET Jobs]
          /\ observedRoot \in [Collectors -> [Databases -> Jobs \cup {None}]]
          /\ protected \in [Collectors -> [Databases -> Jobs \cup {None}]]
          /\ observedRevision \in [Collectors -> 0..MaxRevision] /\ lateListed \in [Collectors -> BOOLEAN]
LiveObjects == References(root) \subseteq objects
DurableAcknowledgement == acknowledged \subseteq published
RootIdentity == \A d \in Databases: root[d] # None => root[d] \in published /\ target[root[d]] = d
Spec == Init /\ [][Next]_vars
=============================================================================
