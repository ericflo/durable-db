--------------------------- MODULE WalHandoff ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxSeq, UnsafeAckGap, UnsafeEarlySnapshot,
          UnsafeStaleSnapshot, UnsafeLateAck, UnsafeReopenSeal
VARIABLES phase, planned, uploaded, acknowledged, oldOnline, newOnline,
          observed, haveSnapshot, scanning, sealed, cut, originalCut
vars == <<phase, planned, uploaded, acknowledged, oldOnline, newOnline,
          observed, haveSnapshot, scanning, sealed, cut, originalCut>>
Numbers == 1..MaxSeq
Prefix(s) == CHOOSE n \in 0..MaxSeq:
               /\ (1..n) \subseteq s
               /\ (n = MaxSeq \/ n + 1 \notin s)
Init == /\ phase = "old" /\ planned = 0 /\ uploaded = {}
        /\ acknowledged = {} /\ oldOnline = TRUE /\ newOnline = TRUE
        /\ observed = {} /\ haveSnapshot = FALSE /\ scanning = FALSE
        /\ sealed = FALSE /\ cut = 0 /\ originalCut = 0
\* One old incarnation prepares immutable sequence positions. Retransmitted
\* bytes and record identity are abstracted as identical; uploads can finish late.
Prepare == /\ oldOnline /\ phase \in {"old", "draining"}
           /\ planned < MaxSeq /\ planned' = planned + 1
           /\ UNCHANGED <<phase, uploaded, acknowledged, oldOnline, newOnline,
                observed, haveSnapshot, scanning, sealed, cut, originalCut>>
Upload(n) == /\ n \in (1..planned) \ uploaded
             /\ uploaded' = uploaded \cup {n}
             /\ UNCHANGED <<phase, planned, acknowledged, oldOnline, newOnline,
                  observed, haveSnapshot, scanning, sealed, cut, originalCut>>
Acknowledge(n) == /\ oldOnline
                  /\ (phase \in {"old", "draining"} \/ UnsafeLateAck)
                  /\ n \in uploaded
                  /\ (UnsafeAckGap \/ n <= Prefix(uploaded))
                  /\ acknowledged' = acknowledged \cup {n}
                  /\ UNCHANGED <<phase, planned, uploaded, oldOnline, newOnline,
                       observed, haveSnapshot, scanning, sealed, cut, originalCut>>
Fence == /\ phase = "old" /\ phase' = "draining"
         /\ UNCHANGED <<planned, uploaded, acknowledged, oldOnline, newOnline,
              observed, haveSnapshot, scanning, sealed, cut, originalCut>>
\* Assumed serving-authority boundary: after this event the old incarnation
\* cannot successfully acknowledge, even if it resumes or an upload finishes.
\* This model does NOT establish how that boundary is implemented.
Drain == /\ phase = "draining" /\ phase' = "recovering"
         /\ UNCHANGED <<planned, uploaded, acknowledged, oldOnline, newOnline,
              observed, haveSnapshot, scanning, sealed, cut, originalCut>>
\* Probe deterministic epoch/sequence names in order. Fresh negative GETs
\* are required. A late upload can arrive behind the cursor or after EOF.
BeginScan == /\ newOnline /\ ~sealed /\ ~scanning
             /\ (phase = "recovering" \/ (UnsafeEarlySnapshot /\ phase = "draining"))
             /\ observed' = {} /\ haveSnapshot' = FALSE /\ scanning' = TRUE
             /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                  newOnline, sealed, cut, originalCut>>
ReadNext == /\ newOnline /\ scanning
            /\ LET n == Cardinality(observed) + 1 IN
                 /\ n \in uploaded
                 /\ observed' = observed \cup {n}
                 /\ scanning' = (n < MaxSeq) /\ haveSnapshot' = (n = MaxSeq)
            /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                 newOnline, sealed, cut, originalCut>>
ReadMissing == /\ newOnline /\ scanning
               /\ (Cardinality(observed) + 1 \notin uploaded \/ UnsafeStaleSnapshot)
               /\ scanning' = FALSE /\ haveSnapshot' = TRUE
               /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                    newOnline, observed, sealed, cut, originalCut>>
\* The successor durably seals an exact old prefix before serving or writing.
\* A missing response/crash may lose the observation, never the durable seal.
Seal == /\ newOnline /\ phase = "recovering" /\ haveSnapshot /\ ~sealed
        /\ sealed' = TRUE /\ cut' = Prefix(observed) /\ originalCut' = Prefix(observed)
        /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
             newOnline, observed, haveSnapshot, scanning>>
Activate == /\ newOnline /\ phase = "recovering" /\ sealed
            /\ phase' = "new"
            /\ UNCHANGED <<planned, uploaded, acknowledged, oldOnline, newOnline,
                 observed, haveSnapshot, scanning, sealed, cut, originalCut>>
ReopenSeal == /\ UnsafeReopenSeal /\ newOnline /\ phase = "new"
              /\ cut' = Prefix(uploaded)
              /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                   newOnline, observed, haveSnapshot, scanning, sealed, originalCut>>
CrashOld == /\ oldOnline /\ oldOnline' = FALSE
            /\ UNCHANGED <<phase, planned, uploaded, acknowledged, newOnline,
                 observed, haveSnapshot, scanning, sealed, cut, originalCut>>
CrashNew == /\ newOnline /\ newOnline' = FALSE /\ observed' = {} /\ haveSnapshot' = FALSE /\ scanning' = FALSE
            /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                 sealed, cut, originalCut>>
RestartNew == /\ ~newOnline /\ newOnline' = TRUE
              /\ UNCHANGED <<phase, planned, uploaded, acknowledged, oldOnline,
                   observed, haveSnapshot, scanning, sealed, cut, originalCut>>
Next == Prepare \/ Fence \/ Drain \/ BeginScan \/ ReadNext \/ ReadMissing \/ Seal \/ Activate \/ ReopenSeal
        \/ CrashOld \/ CrashNew \/ RestartNew
        \/ (\E n \in Numbers: Upload(n) \/ Acknowledge(n))
AcknowledgedPrefixPreserved == sealed => acknowledged \subseteq 1..cut
SealedPrefixImmutable == sealed => cut = originalCut
TypeOK == /\ uploaded \subseteq 1..planned /\ planned \in 0..MaxSeq
          /\ acknowledged \subseteq uploaded /\ cut \in 0..MaxSeq
          /\ observed = 1..Cardinality(observed)
Spec == Init /\ [][Next]_vars
=============================================================================
