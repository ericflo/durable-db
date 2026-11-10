----------------------- MODULE PublicationSafety -----------------------
\* Native tenant-ledger metadata uses this protocol too: its publisher rebuilds
\* fresh objects after ANY failed exact comparison, and collection lists before
\* a nonce-changing comparison. Directory membership supplies protected objects.
EXTENDS Naturals, FiniteSets
CONSTANTS Jobs, Collectors, Pages
VARIABLES live, objects, used, fence, job, candidate, protected, gc
vars == <<live, objects, used, fence, job, candidate, protected, gc>>
JobType == [phase : {"empty", "upload", "ready", "done"}, epoch : Nat,
            kept : SUBSET Pages, fresh : SUBSET Pages]
EmptyJob == [phase |-> "empty", epoch |-> 0, kept |-> {}, fresh |-> {}]
Init == /\ live = {} /\ objects = {} /\ used = {} /\ fence = 0
        /\ job = [j \in Jobs |-> EmptyJob]
        /\ candidate = [c \in Collectors |-> {}]
        /\ protected = [c \in Collectors |-> {}]
        /\ gc = [c \in Collectors |-> "idle"]
Prepare(j, keep, new) ==
    /\ j \in Jobs /\ job[j].phase = "empty"
    /\ keep \subseteq live /\ new \subseteq (Pages \ used)
    /\ job' = [job EXCEPT ![j] = [phase |-> "upload", epoch |-> fence,
                                   kept |-> keep, fresh |-> new]]
    /\ used' = used \cup new
    /\ UNCHANGED <<live, objects, fence, candidate, protected, gc>>
Upload(j) ==
    /\ j \in Jobs /\ job[j].phase = "upload"
    /\ objects' = objects \cup job[j].fresh
    /\ job' = [job EXCEPT ![j].phase = "ready"]
    /\ UNCHANGED <<live, used, fence, candidate, protected, gc>>
\* Retained references from other databases may be rebased into the new root.
\* Omitting target-head comparisons overapproximates actual publication: this
\* model proves object reachability, not transaction authorization/linearizability.
Publish(j, retained, checkOrigin) ==
    /\ j \in Jobs /\ job[j].phase = "ready" /\ retained \subseteq live
    /\ (~checkOrigin \/ job[j].epoch = fence)
    /\ live' = retained \cup job[j].kept \cup job[j].fresh
    /\ job' = [job EXCEPT ![j].phase = "done"]
    /\ UNCHANGED <<objects, used, fence, candidate, protected, gc>>
\* A listing may be stale, but every listed identity was allocated before the
\* listing completed. Identities are never reused, including after crashes.
List(c, names) ==
    /\ c \in Collectors /\ gc[c] = "idle" /\ names \subseteq used
    /\ candidate' = [candidate EXCEPT ![c] = names]
    /\ gc' = [gc EXCEPT ![c] = "listed"]
    /\ UNCHANGED <<live, objects, used, fence, job, protected>>
Fence(c, advance) ==
    /\ c \in Collectors /\ gc[c] = "listed"
    /\ protected' = [protected EXCEPT ![c] = live]
    /\ gc' = [gc EXCEPT ![c] = "fenced"]
    /\ fence' = (IF advance THEN fence + 1 ELSE fence)
    /\ UNCHANGED <<live, objects, used, job, candidate>>
Delete(c) ==
    /\ c \in Collectors /\ gc[c] = "fenced"
    /\ objects' = objects \ (candidate[c] \ protected[c])
    /\ UNCHANGED <<live, used, fence, job, candidate, protected, gc>>
Finish(c) ==
    /\ c \in Collectors /\ gc[c] = "fenced"
    /\ gc' = [gc EXCEPT ![c] = "idle"]
    /\ UNCHANGED <<live, objects, used, fence, job, candidate, protected>>
Steps(origin, advance) ==
    \/ \E j \in Jobs, keep \in SUBSET live, new \in SUBSET (Pages \ used): Prepare(j, keep, new)
    \/ \E j \in Jobs: Upload(j)
    \/ \E j \in Jobs, retained \in SUBSET live: Publish(j, retained, origin)
    \/ \E c \in Collectors, names \in SUBSET used: List(c, names)
    \/ \E c \in Collectors: Fence(c, advance) \/ Delete(c) \/ Finish(c)
Next == Steps(TRUE, TRUE)
Spec == Init /\ [][Next]_vars
UnsafeOriginSpec == Init /\ [][Steps(FALSE, TRUE)]_vars
UnsafeFenceSpec == Init /\ [][Steps(TRUE, FALSE)]_vars
TypeOK == /\ live \subseteq Pages /\ objects \subseteq Pages /\ used \subseteq Pages
          /\ fence \in Nat /\ job \in [Jobs -> JobType]
          /\ candidate \in [Collectors -> SUBSET Pages]
          /\ protected \in [Collectors -> SUBSET Pages]
          /\ gc \in [Collectors -> {"idle", "listed", "fenced"}]
LiveExists == live \subseteq objects
Allocated == /\ objects \subseteq used
             /\ \A c \in Collectors: candidate[c] \subseteq used
Observed == \A j \in Jobs: /\ job[j].epoch <= fence /\ job[j].fresh \subseteq used
Valid(j) == /\ job[j].phase \in {"upload", "ready"} /\ job[j].epoch = fence
LiveProtected == \A c \in Collectors: gc[c] = "fenced" =>
                    live \cap candidate[c] \subseteq protected[c]
PendingProtected == \A j \in Jobs: Valid(j) =>
    /\ job[j].kept \subseteq objects
    /\ \A c \in Collectors: gc[c] = "fenced" =>
        /\ job[j].kept \cap candidate[c] \subseteq protected[c]
        /\ job[j].fresh \cap candidate[c] = {}
Uploaded == \A j \in Jobs: (Valid(j) /\ job[j].phase = "ready") => job[j].fresh \subseteq objects
Invariant == TypeOK /\ LiveExists /\ Allocated /\ Observed /\ LiveProtected /\ PendingProtected /\ Uploaded
\* These bounds are used only by TLC; the separate TLAPS proof has no bound.
Small == fence <= 2 /\ Cardinality(used) <= 3
=============================================================================
