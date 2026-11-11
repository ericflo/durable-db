----------------------- MODULE RootRegistration -----------------------
EXTENDS RootAcquisition
CONSTANTS UnsafeEarlyPublication, UnsafeParent, UnsafeImmediateOnly

\* Each fresh root identity names an immutable registration containing its
\* unique journal identity and its predecessor registration. Candidate IDs
\* stand for those unique pairs here. The permit is a separate object; revoking
\* it does not delete the registration.
\* No data frames, prefix cuts or leases.
VARIABLES parents, durableParents, persisted, known, sent, acknowledged, live, ready
dataVars == <<parents, durableParents, persisted, known, sent, acknowledged, live, ready>>
allVars == <<vars, dataVars>>
DataInit == /\ parents = [c \in Candidates |-> None]
            /\ durableParents = parents
            /\ persisted = {} /\ known = {} /\ sent = {} /\ acknowledged = {}
            /\ live = {} /\ ready = {}

StartRegistered(c, a, source, kind) ==
    /\ Start(c, a, source, kind)
    /\ parents' = IF kind = "claim"
                  THEN [parents EXCEPT ![c] = IF UnsafeParent THEN None ELSE source.owner]
                  ELSE parents
    /\ UNCHANGED <<durableParents, persisted, known, sent, acknowledged, live, ready>>

\* PUT completion and the caller's knowledge are separate. A lost reply is
\* resolved only by positively observing this immutable registration. Completion
\* abstracts registration plus single-attempt permit creation; RevocableOwner
\* separately checks create-before-publish, no recreation and response grouping.
UploadRegistration(c) ==
    /\ target[c] > 0 /\ c \notin persisted
    /\ durableParents' = [durableParents EXCEPT ![c] = parents[c]]
    /\ persisted' = persisted \cup {c}
    /\ live' = live \cup {c}
    /\ UNCHANGED <<vars, parents, known, sent, acknowledged, ready>>
ObserveRegistration(c) ==
    /\ c \in persisted /\ c \notin known
    /\ known' = known \cup {c}
    /\ UNCHANGED <<vars, parents, durableParents, persisted, sent, acknowledged, live, ready>>
Send(a) ==
    /\ requests[a].status = "pending" /\ a \notin sent
    /\ requests[a].kind = "validate" \/ UnsafeEarlyPublication \/ requests[a].owner \in known
    /\ sent' = sent \cup {a}
    /\ UNCHANGED <<vars, parents, durableParents, persisted, known, acknowledged, live, ready>>
ApplyRegistered(a) == /\ a \in sent /\ Apply(a) /\ UNCHANGED dataVars
DeliverRegistered(a) == Deliver(a) /\ UNCHANGED dataVars
TimeoutRegistered(c, a) == Timeout(c, a) /\ UNCHANGED dataVars

Winners == {history[i].owner : i \in 2..Len(history)}
\* Overapproximate acknowledgement eligibility: even a fenced former winner
\* may acquire this marker. Only discoverability is checked, not data safety.
MarkAcknowledged(c) ==
    /\ c \in Winners /\ c \notin acknowledged
    /\ acknowledged' = acknowledged \cup {c}
    /\ UNCHANGED <<vars, parents, durableParents, persisted, known, sent, live, ready>>

RECURSIVE Walk(_, _)
Walk(c, remaining) ==
    IF c = None \/ remaining = 0 THEN {}
    ELSE {c} \cup (IF c \in persisted THEN Walk(durableParents[c], remaining-1) ELSE {})
\* Recovery uses ONLY the current durable register and persisted parent links.
\* It does not consult target, captured parents, replies or any process cache.
Reachable == Walk(Current.owner, Cardinality(Candidates)+1)
RegistrationAvailable == Reachable \subseteq persisted
WinnerDiscovery == Winners \subseteq Reachable
AcknowledgedDiscoverable == acknowledged \subseteq Reachable
OrderedAncestry == \A c \in persisted:
    durableParents[c] # None => target[durableParents[c]] < target[c]
\* A confirmed claim can be arbitrarily old. Its immutable ancestor chain is
\* still safe to revoke. An intermediate winner need not have run any recovery.
Ancestors(c) == Walk(durableParents[c], Cardinality(Candidates))
Required(c) == IF UnsafeImmediateOnly
              THEN {durableParents[c]} \ {None} ELSE Ancestors(c)
RevokeAncestor(c, d) ==
    /\ confirmed[c] # 0 /\ c \in persisted
    /\ d \in Required(c) \cap live
    /\ live' = live \ {d}
    /\ UNCHANGED <<vars, parents, durableParents, persisted, known, sent, acknowledged, ready>>
FinishRevocation(c) ==
    /\ confirmed[c] # 0 /\ c \in persisted /\ c \notin ready
    /\ Required(c) \cap live = {}
    /\ ready' = ready \cup {c}
    /\ UNCHANGED <<vars, parents, durableParents, persisted, known, sent, acknowledged, live>>
AllEarlierPermitsRevoked == \A c \in ready:
    {d \in Winners: target[d] < target[c]} \cap live = {}
NotInterruptedRevocation == ~(Current.epoch = 3 /\ Current.owner \in ready
    /\ (\E c \in Winners: target[c] = 1 /\ c \in ready)
    /\ (\E c \in Winners: target[c] = 2 /\ c \notin ready))
DataTypeOK == /\ parents \in [Candidates -> Candidates \cup {None}]
              /\ durableParents \in [Candidates -> Candidates \cup {None}]
              /\ persisted \subseteq Candidates /\ known \subseteq persisted
              /\ sent \subseteq Ids /\ acknowledged \subseteq Winners
              /\ live \subseteq persisted /\ ready \subseteq Winners

CombinedNext == (\E a \in Ids: Send(a) \/ ApplyRegistered(a) \/ DeliverRegistered(a))
    \/ (\E c, d \in Candidates: RevokeAncestor(c, d))
    \/ (\E c \in Candidates: FinishRevocation(c))
    \/ (\E c \in Candidates: UploadRegistration(c) \/ ObserveRegistration(c) \/ MarkAcknowledged(c))
    \/ (\E c \in Candidates, a \in Ids:
        TimeoutRegistered(c, a) \/ (\E kind \in {"claim", "validate"}:
            \E source \in {history[i] : i \in 1..Len(history)}: StartRegistered(c, a, source, kind)))
CombinedSpec == Init /\ DataInit /\ [][CombinedNext]_allVars
NotThreeRegistrations == ~(Current.epoch = 3 /\ Cardinality(acknowledged) >= 2
                          /\ acknowledged \subseteq Reachable)
=======================================================================
