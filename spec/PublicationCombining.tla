----------------------- MODULE PublicationCombining -----------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC
CONSTANTS Jobs, Databases, Helpers, MaxRevision,
          UnsafeFence, UnsafeTarget, UnsafeMerge, UnsafeAbort
VARIABLES history, files, proposals, phase, target, base, response,
          chosen, seen, helping, committed, commits, latest, badTarget,
          gcPhase, candidates, proposalCandidates, gcSeen, protected
vars == <<history, files, proposals, phase, target, base, response,
          chosen, seen, helping, committed, commits, latest, badTarget,
          gcPhase, candidates, proposalCandidates, gcSeen, protected>>

EmptyHeads == [d \in Databases |-> 0]
InitialRoot == [rev |-> 0, fence |-> 0, heads |-> EmptyHeads]
Current == history[Len(history)]
Revision == Len(history)-1
Merge(old, selected) == [d \in Databases |->
    IF \E j \in selected: target[j] = d
    THEN CHOOSE j \in selected: target[j] = d ELSE old[d]]
UniqueTargets(selected) == \A a,b \in selected: target[a] = target[b] => a = b
Eligible(j, root) == /\ (UnsafeFence \/ base[j].fence = root.fence)
                     /\ (UnsafeTarget \/ base[j].heads[target[j]] = root.heads[target[j]])

Init == /\ history = <<InitialRoot>> /\ files = {} /\ proposals = {}
        /\ phase = [j \in Jobs |-> "idle"]
        /\ target = [j \in Jobs |-> CHOOSE d \in Databases: TRUE]
        /\ base = [j \in Jobs |-> InitialRoot]
        /\ response = [j \in Jobs |-> "waiting"]
        /\ chosen = [h \in Helpers |-> {}]
        /\ seen = [h \in Helpers |-> InitialRoot]
        /\ helping = [h \in Helpers |-> FALSE]
        /\ committed = {} /\ commits = [j \in Jobs |-> 0]
        /\ latest = EmptyHeads /\ badTarget = FALSE
        /\ gcPhase = "idle" /\ candidates = {} /\ proposalCandidates = {}
        /\ gcSeen = InitialRoot /\ protected = EmptyHeads

Begin(j,d,r) == /\ phase[j] = "idle" /\ r \in 1..Len(history)
                /\ phase' = [phase EXCEPT ![j] = "begun"]
                /\ target' = [target EXCEPT ![j] = d]
                /\ base' = [base EXCEPT ![j] = history[r]]
                /\ UNCHANGED <<history, files, proposals, response, chosen, seen, helping,
                               committed, commits, latest, badTarget, gcPhase, candidates,
                               proposalCandidates, gcSeen, protected>>
Upload(j) == /\ phase[j] = "begun"
             /\ phase' = [phase EXCEPT ![j] = "uploaded"]
             /\ files' = files \cup {j}
             /\ UNCHANGED <<history, proposals, target, base, response, chosen, seen, helping,
                            committed, commits, latest, badTarget, gcPhase, candidates,
                            proposalCandidates, gcSeen, protected>>
Submit(j) == /\ phase[j] = "uploaded"
             /\ phase' = [phase EXCEPT ![j] = "submitted"]
             /\ proposals' = proposals \cup {j}
             /\ UNCHANGED <<history, files, target, base, response, chosen, seen, helping,
                            committed, commits, latest, badTarget, gcPhase, candidates,
                            proposalCandidates, gcSeen, protected>>

Select(h, selected, r) ==
    /\ ~helping[h] /\ r \in 1..Len(history)
    \* Retained discovery can outlive a proposal's deletion or helper reset.
    \* Overapproximate the bounded local batch by every submitted identity;
    \* each selection still rechecks its target and original collection fence.
    /\ selected \subseteq {j \in Jobs: phase[j] = "submitted"}
    /\ selected # {} /\ UniqueTargets(selected)
    /\ \A j \in selected: Eligible(j, history[r])
    /\ chosen' = [chosen EXCEPT ![h] = selected]
    /\ seen' = [seen EXCEPT ![h] = history[r]]
    /\ helping' = [helping EXCEPT ![h] = TRUE]
    /\ UNCHANGED <<history, files, proposals, phase, target, base, response,
                   committed, commits, latest, badTarget, gcPhase, candidates,
                   proposalCandidates, gcSeen, protected>>

Publish(h) ==
    /\ helping[h] /\ seen[h].rev = Revision /\ Revision < MaxRevision
    /\ LET start == IF UnsafeMerge THEN base[CHOOSE j \in chosen[h]: TRUE].heads
                    ELSE seen[h].heads
           next == [rev |-> Revision+1, fence |-> Current.fence, heads |-> Merge(start, chosen[h])]
       IN history' = Append(history, next)
    /\ committed' = committed \cup chosen[h]
    /\ commits' = [j \in Jobs |-> commits[j] + IF j \in chosen[h] THEN 1 ELSE 0]
    /\ latest' = Merge(latest, chosen[h])
    /\ badTarget' = (badTarget \/ (\E j \in chosen[h]: Current.heads[target[j]] # base[j].heads[target[j]]))
    /\ helping' = [helping EXCEPT ![h] = FALSE]
    /\ chosen' = [chosen EXCEPT ![h] = {}]
    /\ seen' = [seen EXCEPT ![h] = InitialRoot]
    /\ UNCHANGED <<files, proposals, phase, target, base, response,
                   gcPhase, candidates, proposalCandidates, gcSeen, protected>>

\* An existing unbatched publisher can compete with the helpers.
Direct(j) == /\ phase[j] = "uploaded" /\ Revision < MaxRevision
             /\ base[j].fence = Current.fence
             /\ base[j].heads[target[j]] = Current.heads[target[j]]
             /\ history' = Append(history, [rev |-> Revision+1, fence |-> Current.fence,
                                             heads |-> Merge(Current.heads, {j})])
             /\ committed' = committed \cup {j}
             /\ commits' = [commits EXCEPT ![j] = @+1]
             /\ latest' = Merge(latest, {j})
             /\ phase' = [phase EXCEPT ![j] = "done"]
             /\ UNCHANGED <<files, proposals, target, base, response, chosen, seen, helping,
                            badTarget, gcPhase, candidates, proposalCandidates, gcSeen, protected>>

Reset(h) == /\ helping[h]
            /\ helping' = [helping EXCEPT ![h] = FALSE]
            /\ chosen' = [chosen EXCEPT ![h] = {}]
            /\ seen' = [seen EXCEPT ![h] = InitialRoot]
            /\ UNCHANGED <<history, files, proposals, phase, target, base, response,
                           committed, commits, latest, badTarget, gcPhase, candidates,
                           proposalCandidates, gcSeen, protected>>

Unknown(j) == /\ response[j] = "waiting"
              /\ response' = [response EXCEPT ![j] = "unknown"]
              /\ UNCHANGED <<history, files, proposals, phase, target, base, chosen, seen, helping,
                             committed, commits, latest, badTarget, gcPhase, candidates,
                             proposalCandidates, gcSeen, protected>>
Abort(j) == /\ response[j] = "waiting" /\ j \notin committed
            /\ phase[j] \in IF UnsafeAbort THEN {"begun", "uploaded", "submitted"} ELSE {"begun", "uploaded"}
            /\ response' = [response EXCEPT ![j] = "aborted"]
            /\ phase' = [phase EXCEPT ![j] = IF @ = "submitted" THEN @ ELSE "done"]
            /\ UNCHANGED <<history, files, proposals, target, base, chosen, seen, helping,
                           committed, commits, latest, badTarget, gcPhase, candidates,
                           proposalCandidates, gcSeen, protected>>
\* Return this publication's fresh pointer, not a later observation's pointer.
\* A historical success remains a valid receipt after the target changes; it
\* does not authorize serving or let a subsequent CAS skip its comparison.
\* Observation does not need a helper slot: another helper may still be busy.
Ack(j,r) == /\ response[j] = "waiting" /\ r \in 1..Len(history)
            /\ history[r].heads[target[j]] = j
            /\ response' = [response EXCEPT ![j] = "success"]
            /\ UNCHANGED <<history, files, proposals, phase, target, base, chosen, seen, helping,
                           committed, commits, latest, badTarget, gcPhase, candidates,
                           proposalCandidates, gcSeen, protected>>

List == /\ gcPhase = "idle" /\ gcPhase' = "listed"
        /\ candidates' = files /\ proposalCandidates' = proposals
        /\ UNCHANGED <<history, files, proposals, phase, target, base, response, chosen, seen,
                       helping, committed, commits, latest, badTarget, gcSeen, protected>>
Observe(r) == /\ gcPhase = "listed" /\ r \in 1..Len(history)
              /\ gcPhase' = "observed" /\ gcSeen' = history[r]
              /\ UNCHANGED <<history, files, proposals, phase, target, base, response, chosen, seen,
                             helping, committed, commits, latest, badTarget, candidates,
                             proposalCandidates, protected>>
Fence == /\ gcPhase = "observed" /\ gcSeen.rev = Revision /\ Revision < MaxRevision
         /\ history' = Append(history, [rev |-> Revision+1, fence |-> Current.fence+1, heads |-> Current.heads])
         /\ protected' = gcSeen.heads /\ gcPhase' = "deleting"
         /\ UNCHANGED <<files, proposals, phase, target, base, response, chosen, seen, helping,
                        committed, commits, latest, badTarget, candidates, proposalCandidates, gcSeen>>
Delete(j) == /\ gcPhase = "deleting" /\ j \in candidates
             /\ j \notin {protected[d]: d \in Databases}
             /\ files' = files \ {j} /\ candidates' = candidates \ {j}
             /\ UNCHANGED <<history, proposals, phase, target, base, response, chosen, seen, helping,
                            committed, commits, latest, badTarget, gcPhase, proposalCandidates, gcSeen, protected>>
\* Successful helpers may discard committed proposals without advancing the
\* fence. Their unique target identity cannot replace its old target again.
DeleteProposal(j) == /\ ((gcPhase = "deleting" /\ j \in proposalCandidates)
                         \/ (j \in committed /\ j \in proposals))
                     /\ proposals' = proposals \ {j} /\ proposalCandidates' = proposalCandidates \ {j}
                     /\ UNCHANGED <<history, files, phase, target, base, response, chosen, seen, helping,
                                    committed, commits, latest, badTarget, gcPhase, candidates, gcSeen, protected>>
AbandonGC == /\ gcPhase # "idle" /\ gcPhase' = "idle"
             /\ candidates' = {} /\ proposalCandidates' = {} /\ gcSeen' = InitialRoot /\ protected' = EmptyHeads
             /\ UNCHANGED <<history, files, proposals, phase, target, base, response, chosen, seen, helping,
                            committed, commits, latest, badTarget>>
Barrier == /\ Revision < MaxRevision
           /\ history' = Append(history, [rev |-> Revision+1, fence |-> Current.fence, heads |-> Current.heads])
           /\ UNCHANGED <<files, proposals, phase, target, base, response, chosen, seen, helping,
                          committed, commits, latest, badTarget, gcPhase, candidates, proposalCandidates, gcSeen, protected>>

Next == Barrier \/ List \/ Fence \/ AbandonGC
        \/ (\E r \in 1..Len(history): Observe(r))
        \/ (\E j \in Jobs: Upload(j) \/ Submit(j) \/ Direct(j) \/ Unknown(j) \/ Abort(j)
             \/ Delete(j) \/ DeleteProposal(j) \/ (\E r \in 1..Len(history): Ack(j,r))
             \/ (\E d \in Databases, r \in 1..Len(history): Begin(j,d,r)))
        \/ (\E h \in Helpers: Publish(h) \/ Reset(h)
             \/ (\E selected \in SUBSET Jobs, r \in 1..Len(history): Select(h,selected,r)))

RootType == [rev: 0..MaxRevision, fence: 0..MaxRevision, heads: [Databases -> Jobs \cup {0}]]
TypeOK == /\ history \in Seq(RootType) /\ Len(history) \in 1..(MaxRevision+1)
          /\ \A r \in 1..Len(history): history[r].rev = r-1
          /\ files \subseteq Jobs /\ proposals \subseteq Jobs
          /\ phase \in [Jobs -> {"idle", "begun", "uploaded", "submitted", "done"}]
          /\ target \in [Jobs -> Databases] /\ base \in [Jobs -> RootType]
          /\ response \in [Jobs -> {"waiting", "unknown", "aborted", "success"}]
          /\ chosen \in [Helpers -> SUBSET Jobs] /\ seen \in [Helpers -> RootType]
          /\ helping \in [Helpers -> BOOLEAN] /\ committed \subseteq Jobs
          /\ commits \in [Jobs -> 0..MaxRevision] /\ latest \in [Databases -> Jobs \cup {0}]
          /\ badTarget \in BOOLEAN /\ gcPhase \in {"idle", "listed", "observed", "deleting"}
          /\ candidates \subseteq Jobs /\ proposalCandidates \subseteq Jobs
          /\ gcSeen \in RootType /\ protected \in [Databases -> Jobs \cup {0}]
ReferencedManifestsExist == \A d \in Databases: Current.heads[d] = 0 \/ Current.heads[d] \in files
ExpectedTargetsCompared == ~badTarget
UnrelatedWritesPreserved == Current.heads = latest
NoDuplicateCommit == \A j \in Jobs: commits[j] <= 1
AcknowledgedHistory == \A j \in Jobs: response[j] = "success" => j \in committed
NoCommitAfterDefiniteAbort == \A j \in Jobs: response[j] = "aborted" => j \notin committed
NotSupersededReceipt == ~\E j \in Jobs:
    response[j] = "success" /\ Current.heads[target[j]] # j
NotReceiptWhileHelperBusy == ~\E j \in Jobs, h \in Helpers:
    response[j] = "success" /\ helping[h]
\* Fault: let a submitted but unconfirmed proposal manufacture a receipt.
PrematureReceipt(j) == /\ phase[j] = "submitted" /\ response[j] = "waiting"
    /\ response' = [response EXCEPT ![j] = "success"]
    /\ UNCHANGED <<history, files, proposals, phase, target, base, chosen, seen, helping,
                   committed, commits, latest, badTarget, gcPhase, candidates,
                   proposalCandidates, gcSeen, protected>>
\* Only rename fresh job identities within Jobs and interchangeable helpers
\* within Helpers. Databases and the collector are never mixed with either.
Symmetry == {jp @@ hp: jp \in Permutations(Jobs), hp \in Permutations(Helpers)}
Spec == Init /\ [][Next]_vars
\* Coverage must deliver the receipt during the busy interval, not merely
\* start a new helper after an earlier acknowledgement.
QueuedReceiptSpec == Init /\ [][Next /\
    ((\E j \in Jobs: response[j] # "success" /\ response'[j] = "success")
     => \E h \in Helpers: helping[h])]_vars
UnsafeReceiptSpec == Init /\ [][Next \/ (\E j \in Jobs: PrematureReceipt(j))]_vars
=============================================================================
