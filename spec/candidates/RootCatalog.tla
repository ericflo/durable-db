-------------------------- MODULE RootCatalog --------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets
CONSTANTS Epochs, Attempts, LogLength, UnsafeCompare, UnsafeInherit,
          UnsafeReseal, UnsafeEarly, UnsafeScan

\* Conditional prefix catalogs with explicit captured requests and late apply.
\* Drain is an abstraction boundary: RevocableOwner establishes the required
\* response ordering after permanent predecessor revocation. No clock here.
\* Ordered winning epochs and durable journal registrations are assumed here.
\* A confirmed seal preserves all prior cuts. The implementation may retain
\* its immutable ancestry cursor across that seal, loading the new confirmed
\* catalog before BeginScan for the next journal. After exact validation an
\* unchanged snapshot can reuse its completed scan. Each retry has fresh catalog
\* uploads and a captured comparison; no request or uncertain PUT is replayed.
\* Collection is abstracted out here. Rust also compares the captured collection
\* generation, checkpoint and owner; tests exercise those invalidation guards.
\* One seal may publish several fully scanned journals from that same snapshot;
\* every selected cut is immutable and all catalog bytes precede the exact CAS.
VARIABLES history, active, entered, objects, acknowledged, scans, requests,
          uploaded, known, sent, pending, timedOut, lateApplied
vars == <<history, active, entered, objects, acknowledged, scans, requests,
          uploaded, known, sent, pending, timedOut, lateApplied>>
Owners == 1..Epochs
Ids == 1..Attempts
EmptyCuts == [e \in Owners |-> -1]
Initial == [owner |-> 1, revision |-> 0, cuts |-> EmptyCuts]
Current == history[Len(history)]
Idle == [epochs |-> {}, position |-> [e \in Owners |-> 0],
         finished |-> {}, source |-> Initial, attempted |-> FALSE, reused |-> FALSE]
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
BeginScan(p, epochs, source) ==
    /\ scans[p] = Idle /\ epochs # {} /\ epochs \subseteq 1..(p-1)
    /\ source.owner = p
    /\ p \in active \/ (UnsafeScan /\ p = Current.owner)
    /\ \A e \in epochs: source.cuts[e] = -1 \/ UnsafeReseal
    /\ scans' = [scans EXCEPT ![p] =
          [Idle EXCEPT !.epochs = epochs, !.source = source]]
    /\ UNCHANGED <<history, active, entered, objects, acknowledged, requests,
                   uploaded, known, sent, pending, timedOut, lateApplied>>
ReadScan(p, e) ==
    /\ e \in scans[p].epochs \ scans[p].finished
    /\ LET s == scans[p] IN
         scans' = [scans EXCEPT ![p] =
           IF s.position[e] + 1 \in objects[e]
           THEN [s EXCEPT !.position[e] = @ + 1]
           ELSE [s EXCEPT !.finished = @ \cup {e}]]
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
\* after an uncertain seal. A changed catalog requires new discovery; an
\* unchanged one permits reusing a finished scan, even after late uploads.
\* An adopted selection is never extended by a late old journal upload.
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
    /\ NextId(a) /\ pending[p] = 0 /\ p \in active
    /\ scans[p].epochs # {} /\ scans[p].finished = scans[p].epochs
    /\ source.owner = p /\ source.cuts = scans[p].source.cuts
    /\ LET s == scans[p] IN
         requests' = [requests EXCEPT ![a] =
           [status |-> "pending", kind |-> "seal", owner |-> p,
            expected |-> source.revision,
            cuts |-> [e \in Owners |->
                IF e \in s.epochs THEN s.position[e] ELSE s.source.cuts[e]]]]
    /\ scans' = [scans EXCEPT ![p] =
          [scans[p] EXCEPT !.attempted = TRUE, !.reused = scans[p].attempted]]
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
    \/ (\E p \in Owners: (\E e \in Owners: ReadScan(p,e)) \/ ForgetScan(p) \/ Timeout(p)
         \/ (\E n \in 1..LogLength: Upload(p,n) \/ Ack(p,n))
         \/ (\E a \in Ids, source \in {history[i] : i \in 1..Len(history)}:
              StartSeal(p,a,source) \/ StartFence(p,a,source) \/ StartRenew(p,a,source))
         \/ (\E epochs \in SUBSET (1..(p-1)), source \in {history[i] : i \in 1..Len(history)}: BeginScan(p,epochs,source)))
Spec == Init /\ [][Next]_vars
ImmutableSelection == \A i,j \in 1..Len(history): i <= j =>
    \A e \in Owners: history[i].cuts[e] >= 0 => history[j].cuts[e] = history[i].cuts[e]
SelectedHistory == \A e \in Owners: Current.cuts[e] >= 0 =>
    /\ acknowledged[e] <= Current.cuts[e]
    /\ 1..Current.cuts[e] \subseteq objects[e]
DurableCatalog == \A a \in Ids:
    (requests[a].kind = "seal" /\ requests[a].status \in {"ok", "done"}) =>
      (requests[a].cuts \in {history[i].cuts : i \in 2..Len(history)} => a \in uploaded)
NotRetriedScan == ~(\E p \in Owners: scans[p].reused)
NotLateSelection == ~(\E a \in lateApplied: requests[a].kind = "seal" /\ Current.owner = Epochs)
NotInheritedSelection == ~(Current.owner = 3 /\ Current.cuts[1] >= 0)
NotBatchedSelection == ~(Current.owner = 3 /\ Current.cuts[1] >= 0 /\ Current.cuts[2] >= 0)
========================================================================
