----------------------------- MODULE Journal -----------------------------
EXTENDS Naturals, FiniteSets
VARIABLES writer, objects, control, scan
vars == <<writer, objects, control, scan>>
EmptyScan == [position |-> 0, running |-> FALSE, finished |-> FALSE]
Init == /\ writer = [planned |-> 0, known |-> {}, durable |-> 0,
                     acknowledged |-> 0, online |-> TRUE]
        /\ objects = {}
        /\ control = [phase |-> "old", sealed |-> FALSE, cut |-> 0,
                      originalCut |-> 0, online |-> TRUE]
        /\ scan = EmptyScan

\* Fresh sequence identities contain one immutable value. Late uploads can
\* finish after the old process disappears or a successor seals its prefix.
\* Upload admission refusal occurs before any data request and stutters here.
\* Retrying admission retains the same identity; it permits only the first PUT.
\* After an attempted/cancelled PUT, absence still never authorizes a retry.
Prepare == /\ writer.online
           /\ writer' = [writer EXCEPT !.planned = @ + 1]
           /\ UNCHANGED <<objects, control, scan>>
Upload(n) == /\ n \in 1..writer.planned /\ n \notin objects
             /\ objects' = objects \cup {n}
             /\ UNCHANGED <<writer, control, scan>>
\* A successful PUT reply or an exact resolving GET establishes knowledge.
\* A missing/unknown response is a stutter: it never establishes absence.
Observe(n, check) == /\ writer.online /\ n \in 1..writer.planned
                    /\ (~check \/ n \in objects)
                    /\ writer' = [writer EXCEPT !.known = @ \cup {n}]
                    /\ UNCHANGED <<objects, control, scan>>
Consume(n, contiguous) == /\ writer.online /\ n \in writer.known
                         /\ n > writer.durable
                         /\ (~contiguous \/ n = writer.durable + 1)
                         /\ writer' = [writer EXCEPT !.durable = n]
                         /\ UNCHANGED <<objects, control, scan>>
\* The journal itself does not acknowledge clients. This is its caller's
\* obligation: a consumed prefix AND valid serving permission are required.
Acknowledge(n, permission) == /\ writer.online /\ n \in 1..writer.durable
                             /\ n > writer.acknowledged
                             /\ (~permission \/ control.phase \in {"old", "draining"})
                             /\ writer' = [writer EXCEPT !.acknowledged = n]
                             /\ UNCHANGED <<objects, control, scan>>
Fence == /\ control.phase = "old"
         /\ control' = [control EXCEPT !.phase = "draining"]
         /\ UNCHANGED <<writer, objects, scan>>
\* External serving-authority boundary, as in the preceding WalHandoff model.
\* The OwnedWal candidate explores one clock-based realization separately.
Drain == /\ control.phase = "draining"
         /\ control' = [control EXCEPT !.phase = "recovering"]
         /\ UNCHANGED <<writer, objects, scan>>
BeginScan(early) == /\ control.online /\ ~control.sealed /\ ~scan.running
                   /\ (control.phase = "recovering" \/ (early /\ control.phase = "draining"))
                   /\ scan' = [position |-> 0, running |-> TRUE, finished |-> FALSE]
                   /\ UNCHANGED <<writer, objects, control>>
ReadNext == /\ control.online /\ scan.running /\ scan.position + 1 \in objects
            /\ scan' = [scan EXCEPT !.position = @ + 1]
            /\ UNCHANGED <<writer, objects, control>>
ReadMissing(fresh) == /\ control.online /\ scan.running
                     /\ (~fresh \/ scan.position + 1 \notin objects)
                     /\ scan' = [scan EXCEPT !.running = FALSE, !.finished = TRUE]
                     /\ UNCHANGED <<writer, objects, control>>
\* Atomic conditional selection is a controller obligation. The Rust reader
\* consumes a selected prefix; it does not implement this publication step.
Seal == /\ control.online /\ control.phase = "recovering"
        /\ scan.finished /\ ~control.sealed
        /\ control' = [control EXCEPT !.sealed = TRUE, !.cut = scan.position,
                       !.originalCut = scan.position]
        /\ UNCHANGED <<writer, objects, scan>>
Activate == /\ control.online /\ control.phase = "recovering" /\ control.sealed
            /\ control' = [control EXCEPT !.phase = "new"]
            /\ UNCHANGED <<writer, objects, scan>>
Reopen(enabled) == /\ enabled /\ control.online /\ control.phase = "new"
                   /\ \E n \in 0..writer.planned:
                        /\ 1..n \subseteq objects
                        /\ (n = writer.planned \/ n + 1 \notin objects)
                        /\ control' = [control EXCEPT !.cut = n]
                   /\ UNCHANGED <<writer, objects, scan>>
CrashOld == /\ writer.online /\ writer' = [writer EXCEPT !.online = FALSE]
            /\ UNCHANGED <<objects, control, scan>>
CrashNew == /\ control.online /\ control' = [control EXCEPT !.online = FALSE]
            /\ scan' = EmptyScan /\ UNCHANGED <<writer, objects>>
RestartNew == /\ ~control.online /\ control' = [control EXCEPT !.online = TRUE]
              /\ UNCHANGED <<writer, objects, scan>>
Steps(knowledge, contiguous, early, fresh, permission, reopen) ==
    \/ Prepare \/ Fence \/ Drain \/ BeginScan(early) \/ ReadNext
    \/ ReadMissing(fresh) \/ Seal \/ Activate \/ Reopen(reopen)
    \/ CrashOld \/ CrashNew \/ RestartNew
    \/ (\E n \in 1..writer.planned: Upload(n) \/ Observe(n, knowledge)
          \/ Consume(n, contiguous) \/ Acknowledge(n, permission))
Next == Steps(TRUE, TRUE, FALSE, TRUE, TRUE, FALSE)
Spec == Init /\ [][Next]_vars
UnsafeKnowledgeSpec == Init /\ [][Steps(FALSE, TRUE, FALSE, TRUE, TRUE, FALSE)]_vars
UnsafeGapSpec == Init /\ [][Steps(TRUE, FALSE, FALSE, TRUE, TRUE, FALSE)]_vars
UnsafeEarlySpec == Init /\ [][Steps(TRUE, TRUE, TRUE, TRUE, TRUE, FALSE)]_vars
UnsafeMissingSpec == Init /\ [][Steps(TRUE, TRUE, FALSE, FALSE, TRUE, FALSE)]_vars
UnsafePermissionSpec == Init /\ [][Steps(TRUE, TRUE, FALSE, TRUE, FALSE, FALSE)]_vars
UnsafeReopenSpec == Init /\ [][Steps(TRUE, TRUE, FALSE, TRUE, TRUE, TRUE)]_vars

TypeOK == /\ writer \in [planned : Nat, known : SUBSET (1..writer.planned),
                        durable : Nat, acknowledged : Nat, online : BOOLEAN]
          /\ objects \subseteq 1..writer.planned
          /\ writer.known \subseteq 1..writer.planned
          /\ control \in [phase : {"old", "draining", "recovering", "new"},
                          sealed : BOOLEAN, cut : Nat, originalCut : Nat,
                          online : BOOLEAN]
          /\ scan \in [position : Nat, running : BOOLEAN, finished : BOOLEAN]
KnownDurable == writer.known \subseteq objects
ConsumedDurable == 1..writer.durable \subseteq objects
AcknowledgedDurable == 1..writer.acknowledged \subseteq objects
ScannedDurable == 1..scan.position \subseteq objects
FinishedCoversAcknowledgements ==
    (control.phase \in {"recovering", "new"} /\ scan.finished) => scan.position >= writer.acknowledged
SelectedDurable == control.sealed => 1..control.cut \subseteq objects
AcknowledgedPrefixPreserved == control.sealed => writer.acknowledged <= control.cut
SealedPrefixImmutable == control.sealed => control.cut = control.originalCut
SealedPhase == control.sealed => control.phase \in {"recovering", "new"}
ScanState == /\ ~(scan.running /\ scan.finished)
             /\ ((scan.running \/ scan.finished) => control.phase \in {"recovering", "new"})
             /\ (scan.running => ~control.sealed)
Invariant == TypeOK /\ KnownDurable /\ ConsumedDurable /\ AcknowledgedDurable
             /\ ScannedDurable /\ FinishedCoversAcknowledgements /\ SelectedDurable
             /\ AcknowledgedPrefixPreserved /\ SealedPrefixImmutable /\ SealedPhase /\ ScanState
\* TLC only: the inductive proof does not use this exploration bound.
Small == writer.planned <= 3
=============================================================================
