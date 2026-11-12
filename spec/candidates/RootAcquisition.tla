------------------------ MODULE RootAcquisition ------------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Candidates, Attempts, UnsafeCompare, UnsafeRetarget
ASSUME /\ IsFiniteSet(Candidates) /\ Candidates # {} /\ 0 \notin Candidates
       /\ Attempts \in Nat /\ Attempts > 0
       /\ UnsafeCompare \in BOOLEAN /\ UnsafeRetarget \in BOOLEAN

\* Fresh candidate identities are NOT ordered by their eventual winning epoch.
\* Each binds once to an observed predecessor. A lost claim is recovered by
\* exact validation of that same identity/epoch, never retargeted to a new one.
\* Start may observe any historical source. Partition observation therefore
\* needs no read barrier: only the subsequent exact comparison confirms it.
\* Activation changes the confirmed fence to open. Start(activate) abstracts
\* completed predecessor revocation, checked separately by RootRegistration.
\* Validation preserves phase: a lost activation that applied can be recovered;
\* a still-fenced owner needs another fresh activation, never another claim.
\* No serving intervals, journal data, fairness or clock contract here.
VARIABLES history, target, requests, active, confirmed, timedOut, applied, lateApplied
vars == <<history, target, requests, active, confirmed, timedOut, applied, lateApplied>>
Ids == 1..Attempts
None == 0
Initial == [owner |-> None, epoch |-> 0, revision |-> 0, open |-> FALSE]
Current == history[Len(history)]
Fresh == [status |-> "fresh", kind |-> "claim", owner |-> None,
          expected |-> 0, epoch |-> 0, open |-> FALSE, result |-> Initial]
Init == /\ history = <<Initial>>
        /\ target = [c \in Candidates |-> 0]
        /\ requests = [a \in Ids |-> Fresh]
        /\ active = [c \in Candidates |-> 0]
        /\ confirmed = [c \in Candidates |-> 0]
        /\ timedOut = {} /\ applied = {} /\ lateApplied = {}

NextId(a) == requests[a].status = "fresh"
            /\ \A b \in Ids: b < a => requests[b].status # "fresh"
Start(c, a, source, kind) ==
    /\ active[c] = 0 /\ NextId(a)
    /\ source \in {history[i] : i \in 1..Len(history)}
    /\ IF kind = "claim" THEN target[c] = 0 \/ UnsafeRetarget
       ELSE /\ source.owner = c /\ source.epoch = target[c]
            /\ kind = "activate" => /\ confirmed[c] # 0 /\ ~source.open
                                    /\ source = requests[confirmed[c]].result
    /\ LET epoch == IF kind = "claim" THEN source.epoch + 1 ELSE source.epoch
       IN /\ requests' = [requests EXCEPT ![a] =
                [status |-> "pending", kind |-> kind, owner |-> c,
                 expected |-> source.revision, epoch |-> epoch,
                 open |-> IF kind = "claim" THEN FALSE ELSE kind = "activate" \/ source.open,
                 result |-> Initial]]
          /\ target' = IF kind = "claim" THEN [target EXCEPT ![c] = epoch] ELSE target
    /\ active' = [active EXCEPT ![c] = a]
    /\ confirmed' = [confirmed EXCEPT ![c] = 0]
    /\ UNCHANGED <<history, timedOut, applied, lateApplied>>

Apply(a) ==
    /\ requests[a].status = "pending"
    /\ LET r == requests[a]
           success == UnsafeCompare \/ r.expected = Current.revision
           result == [owner |-> r.owner, epoch |-> r.epoch, revision |-> Current.revision+1, open |-> r.open]
       IN /\ history' = IF success THEN Append(history, result) ELSE history
          /\ requests' = [requests EXCEPT
                ![a].status = IF success THEN "ok" ELSE "conflict",
                ![a].result = IF success THEN result ELSE Initial]
          /\ applied' = IF success THEN applied \cup {a} ELSE applied
          /\ lateApplied' = IF success /\ a \in timedOut THEN lateApplied \cup {a} ELSE lateApplied
    /\ UNCHANGED <<target, active, confirmed, timedOut>>

Timeout(c, a) ==
    /\ active[c] = a /\ requests[a].status \in {"pending", "ok", "conflict"}
    /\ active' = [active EXCEPT ![c] = 0]
    /\ confirmed' = [confirmed EXCEPT ![c] = 0]
    /\ timedOut' = timedOut \cup {a}
    /\ UNCHANGED <<history, target, requests, applied, lateApplied>>

Deliver(a) ==
    /\ requests[a].status \in {"ok", "conflict"}
    /\ LET c == requests[a].owner IN
       /\ confirmed' = IF active[c] = a /\ requests[a].status = "ok"
                       THEN [confirmed EXCEPT ![c] = a] ELSE confirmed
       /\ active' = [active EXCEPT ![c] = IF @ = a THEN 0 ELSE @]
    /\ requests' = [requests EXCEPT ![a].status = "done"]
    /\ UNCHANGED <<history, target, timedOut, applied, lateApplied>>

Next == (\E a \in Ids: Apply(a) \/ Deliver(a))
        \/ (\E c \in Candidates, a \in Ids:
            Timeout(c, a) \/ (\E kind \in {"claim", "validate", "activate"}:
                \E source \in {history[i] : i \in 1..Len(history)}: Start(c, a, source, kind)))
Spec == Init /\ [][Next]_vars

Metadata == [owner : Candidates \cup {None}, epoch : Nat, revision : Nat, open : BOOLEAN]
TypeOK == /\ history \in Seq(Metadata) /\ Len(history) > 0
          /\ target \in [Candidates -> Nat]
          /\ active \in [Candidates -> 0..Attempts]
          /\ confirmed \in [Candidates -> 0..Attempts]
          /\ requests \in [Ids -> [status : {"fresh", "pending", "ok", "conflict", "done"},
                kind : {"claim", "validate", "activate"}, owner : Candidates \cup {None},
                expected : Nat, epoch : Nat, open : BOOLEAN, result : Metadata]]
          /\ timedOut \subseteq Ids /\ applied \subseteq Ids /\ lateApplied \subseteq applied
EpochOrder == \A i, j \in 1..Len(history): i <= j => history[i].epoch <= history[j].epoch
ContiguousEpochs == {history[i].epoch : i \in 1..Len(history)} = 0..Current.epoch
UniqueWinner == \A i, j \in 2..Len(history):
    (history[i].owner = history[j].owner) <=> (history[i].epoch = history[j].epoch)
ClaimBinding == \A i \in 2..Len(history): history[i].epoch = target[history[i].owner]
ConfirmedInvocation == \A c \in Candidates: confirmed[c] # 0 =>
    LET a == confirmed[c] IN
    /\ a \in applied /\ a \notin timedOut /\ requests[a].status = "done"
    /\ requests[a].result.owner = c /\ requests[a].result.epoch = target[c]
OpenWasActivated == \A i \in 1..Len(history): history[i].open =>
    \E a \in applied: requests[a].kind = "activate"
        /\ requests[a].owner = history[i].owner
        /\ requests[a].result.revision <= history[i].revision
\* Negative control: observing/validating a fence must not invent activation.
OpenValidation(a) == /\ requests[a].status = "pending"
                     /\ requests[a].kind = "validate" /\ ~requests[a].open
                     /\ requests' = [requests EXCEPT ![a].open = TRUE]
                     /\ UNCHANGED <<history, target, active, confirmed, timedOut, applied, lateApplied>>
UnsafeValidateSpec == Init /\ [][Next \/ (\E a \in Ids: OpenValidation(a))]_vars
NotRecoveredActivation == ~(\E c \in Candidates: confirmed[c] # 0 /\
    requests[confirmed[c]].kind = "validate" /\ requests[confirmed[c]].result.open /\
    \E a \in timedOut \cap applied: requests[a].owner = c /\ requests[a].kind = "activate")

\* Reachability controls: these are witnessed behaviors, not liveness claims.
NotCompetingWinners == ~(Current.epoch >= 2 /\
    \E a, b \in Ids: requests[a].kind = "claim" /\ requests[b].kind = "claim"
        /\ requests[a].owner # None /\ requests[b].owner # None
        /\ requests[a].owner # requests[b].owner
        /\ requests[a].expected = requests[b].expected)
NotRecoveredClaim == ~(\E c \in Candidates: confirmed[c] # 0 /\
    requests[confirmed[c]].kind = "validate" /\
    \E a \in lateApplied: requests[a].owner = c /\ requests[a].kind = "claim")
=======================================================================
