-------------------------- MODULE RootCatalog --------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets
CONSTANTS Epochs, Attempts, LogLength, UnsafeCompare, UnsafeInherit,
          UnsafeReseal, UnsafeEarly, UnsafeScan

\* Conditional prefix catalogs with explicit captured requests and late apply.
\* Drain is an abstraction boundary: RootRequests/GrantBounds must establish
\* that all earlier acknowledgement permission has ended before it completes.
\* Ordered winning epochs and durable journal registrations are assumed here.
\* A confirmed seal preserves all prior cuts. The implementation may retain
\* its immutable ancestry cursor across that seal, loading the new confirmed
\* catalog before BeginScan for the next journal. An uncertain reply restarts
\* discovery after exact validation. No prefix choice or comparison is skipped.
VARIABLES history, active, entered, objects, acknowledged, scans, requests,
          uploaded, known, sent, pending, timedOut, lateApplied
vars == <<history, active, entered, objects, acknowledged, scans, requests,
          uploaded, known, sent, pending, timedOut, lateApplied>>
Owners == 1..Epochs
Ids == 1..Attempts
EmptyCuts == [e \in Owners |-> -1]
Initial == [owner |-> 1, revision |-> 0, cuts |-> EmptyCuts]
Current == history[Len(history)]
Idle == [epoch |-> 0, position |-> 0, finished |-> FALSE, source |-> Initial]
Fresh == [status |-> "fresh", kind |-> "fence", owner |-> 1,
          expected |-> 0, cuts |-> EmptyCuts]
Init == /\ history = <<Initial>> /\ active = {1} /\ entered = {1}
        /\ objects = [p \in Owners |-> {}]
        /\ acknowledged = [p \in Owners |-> 0]
        /\ scans = [p \in Owners |-> Idle]
        /\ requests = [a \in Ids |-> Fresh]
        /\ uploaded = {} /\ known = {} /\ sent = {}
        /\ pending = [p \in Owners |-> 0]
        /\ timedOut = {} /\ lateApplied = {}
NextId(a) == requests[a].status = "fresh"
             /\ \A b \in Ids: b < a => requests[b].status # "fresh"
Upload(p, n) == /\ n \in 1..LogLength /\ n \notin objects[p]
               /\ p \in entered
               /\ objects' = [objects EXCEPT ![p] = @ \cup {n}]
               /\ UNCHANGED <<history, active, entered, acknowledged, scans, requests,
                              uploaded, known, sent, pending, timedOut, lateApplied>>
Ack(p, n) == /\ p \in active /\ n > acknowledged[p]
            /\ 1..n \subseteq objects[p]
            /\ acknowledged' = [acknowledged EXCEPT ![p] = n]
            /\ UNCHANGED <<history, active, entered, objects, scans, requests,
                           uploaded, known, sent, pending, timedOut, lateApplied>>
Drain == /\ Current.owner \notin active
         /\ active' = {Current.owner}
         /\ UNCHANGED <<history, entered, objects, acknowledged, scans, requests,
                        uploaded, known, sent, pending, timedOut, lateApplied>>
BeginScan(p, e, source) ==
    /\ scans[p] = Idle /\ e < p /\ source.owner = p
    /\ p \in active \/ (UnsafeScan /\ p = Current.owner)
    /\ source.cuts[e] = -1 \/ UnsafeReseal
    /\ scans' = [scans EXCEPT ![p] =
          [epoch |-> e, position |-> 0, finished |-> FALSE, source |-> source]]
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, requests,
                   uploaded, known, sent, pending, timedOut, lateApplied>>
ReadScan(p) ==
    /\ scans[p].epoch > 0 /\ ~scans[p].finished
    /\ LET s == scans[p] IN
         scans' = [scans EXCEPT ![p] =
           IF s.position + 1 \in objects[s.epoch]
           THEN [s EXCEPT !.position = @ + 1] ELSE [s EXCEPT !.finished = TRUE]]
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, requests,
                   uploaded, known, sent, pending, timedOut, lateApplied>>
ForgetScan(p) == /\ scans[p] # Idle /\ scans' = [scans EXCEPT ![p] = Idle]
                /\ UNCHANGED <<history, active, entered, objects, acknowledged, requests,
                               uploaded, known, sent, pending, timedOut, lateApplied>>
StartFence(p, a, source) ==
    /\ NextId(a) /\ pending[p] = 0 /\ p \notin entered
    /\ p = source.owner + 1
    /\ requests' = [requests EXCEPT ![a] =
          [status |-> "pending", kind |-> "fence", owner |-> p,
           expected |-> source.revision,
           cuts |-> IF UnsafeInherit THEN EmptyCuts ELSE source.cuts]]
    /\ pending' = [pending EXCEPT ![p] = a]
    /\ entered' = entered \cup {p} /\ sent' = sent \cup {a}
    /\ UNCHANGED <<history, active, objects, acknowledged, scans,
                   uploaded, known, timedOut, lateApplied>>
\* Exact validation (historically called renewal) can confirm the same owner
\* after an uncertain seal. Restart discovery from that confirmed catalog:
\* an adopted selection is never extended by a late old journal upload.
\* Refreshing a scan's comparison requires an unchanged catalog snapshot.
StartRenew(p, a, source) ==
    /\ NextId(a) /\ pending[p] = 0 /\ source.owner = p
    /\ requests' = [requests EXCEPT ![a] =
          [status |-> "pending", kind |-> "renew", owner |-> p,
           expected |-> source.revision, cuts |-> source.cuts]]
    /\ pending' = [pending EXCEPT ![p] = a] /\ sent' = sent \cup {a}
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans,
                   uploaded, known, timedOut, lateApplied>>
StartSeal(p, a, source) ==
    /\ NextId(a) /\ pending[p] = 0 /\ p \in active /\ scans[p].finished
    /\ source.owner = p /\ source.cuts = scans[p].source.cuts
    /\ LET s == scans[p] IN
         requests' = [requests EXCEPT ![a] =
           [status |-> "pending", kind |-> "seal", owner |-> p,
            expected |-> source.revision,
            cuts |-> [s.source.cuts EXCEPT ![s.epoch] = s.position]]]
    /\ scans' = [scans EXCEPT ![p] = Idle]
    /\ pending' = [pending EXCEPT ![p] = a]
    /\ UNCHANGED <<history, active, entered, objects, acknowledged,
                   uploaded, known, sent, timedOut, lateApplied>>
UploadCatalog(a) ==
    /\ requests[a].kind = "seal" /\ requests[a].status = "pending" /\ a \notin uploaded
    /\ uploaded' = uploaded \cup {a}
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans, requests,
                   known, sent, pending, timedOut, lateApplied>>
ObserveCatalog(a) ==
    /\ a \in uploaded /\ a \notin known /\ known' = known \cup {a}
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans, requests,
                   uploaded, sent, pending, timedOut, lateApplied>>
Send(a) ==
    /\ requests[a].status = "pending" /\ requests[a].kind = "seal" /\ a \notin sent
    /\ a \in known \/ UnsafeEarly
    /\ sent' = sent \cup {a}
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans, requests,
                   uploaded, known, pending, timedOut, lateApplied>>
Apply(a) ==
    /\ a \in sent /\ requests[a].status = "pending"
    /\ LET r == requests[a]
           success == UnsafeCompare \/ r.expected = Current.revision
       IN /\ history' = IF success THEN Append(history,
                   [owner |-> r.owner, revision |-> Current.revision+1, cuts |-> r.cuts]) ELSE history
          /\ requests' = [requests EXCEPT ![a].status = IF success THEN "ok" ELSE "conflict"]
          /\ lateApplied' = IF success /\ a \in timedOut THEN lateApplied \cup {a} ELSE lateApplied
    /\ UNCHANGED <<active, entered, objects, acknowledged, scans,
                   uploaded, known, sent, pending, timedOut>>
Timeout(p) == /\ pending[p] # 0
              /\ timedOut' = timedOut \cup {pending[p]}
              /\ pending' = [pending EXCEPT ![p] = 0]
              /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans,
                             requests, uploaded, known, sent, lateApplied>>
Deliver(a) ==
    /\ requests[a].status \in {"ok", "conflict"}
    /\ pending' = [pending EXCEPT ![requests[a].owner] = IF @ = a THEN 0 ELSE @]
    /\ requests' = [requests EXCEPT ![a].status = "done"]
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, scans,
                   uploaded, known, sent, timedOut, lateApplied>>
Next == Drain \/ (\E a \in Ids: UploadCatalog(a) \/ ObserveCatalog(a) \/ Send(a) \/ Apply(a) \/ Deliver(a))
    \/ (\E p \in Owners: ReadScan(p) \/ ForgetScan(p) \/ Timeout(p)
         \/ (\E n \in 1..LogLength: Upload(p,n) \/ Ack(p,n))
         \/ (\E a \in Ids, source \in {history[i] : i \in 1..Len(history)}:
              StartSeal(p,a,source) \/ StartFence(p,a,source) \/ StartRenew(p,a,source))
         \/ (\E e \in Owners, source \in {history[i] : i \in 1..Len(history)}: BeginScan(p,e,source)))
Spec == Init /\ [][Next]_vars
ImmutableSelection == \A i,j \in 1..Len(history): i <= j =>
    \A e \in Owners: history[i].cuts[e] >= 0 => history[j].cuts[e] = history[i].cuts[e]
SelectedHistory == \A e \in Owners: Current.cuts[e] >= 0 =>
    /\ acknowledged[e] <= Current.cuts[e]
    /\ 1..Current.cuts[e] \subseteq objects[e]
DurableCatalog == \A a \in Ids:
    (requests[a].kind = "seal" /\ requests[a].status \in {"ok", "done"}) =>
      (requests[a].cuts \in {history[i].cuts : i \in 2..Len(history)} => a \in uploaded)
NotLateSelection == ~(\E a \in lateApplied: requests[a].kind = "seal" /\ Current.owner = Epochs)
NotInheritedSelection == ~(Current.owner = 3 /\ Current.cuts[1] >= 0)
========================================================================
