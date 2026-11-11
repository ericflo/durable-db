----------------------- MODULE RevocableOwner -----------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Frames, Fault
VARIABLE s
Numbers == 1..Frames
Prefix(u) == CHOOSE n \in 0..Frames:
    /\ 1..n \subseteq u /\ (n = Frames \/ n+1 \notin u)
Ready(n) == /\ n \in s.uploaded
            /\ (Fault = "Gap" \/ 1..n \subseteq s.uploaded)
Visible == IF s.phase = "new" THEN s.cut + (IF s.newWrite THEN 1 ELSE 0)
           ELSE s.oldView
Permit == s.permit \/ (Fault = "StaleGet" /\ s.applied)

\* One selected successor; exact conditional acquisition and fresh identities
\* are assumed from the separate register/acquisition components. All process
\* steps/replies may pause arbitrarily. There are no clocks or failure timers.
Init == s = [permit |-> FALSE, applied |-> FALSE, published |-> FALSE,
    lateReply |-> FALSE, lateUpload |-> FALSE,
    phase |-> "old", uploaded |-> {}, oldView |-> 0, cut |-> 0, newWrite |-> FALSE,
    ack |-> [phase |-> "new", n |-> 0],
    read |-> [phase |-> "new", checked |-> FALSE, valid |-> FALSE,
              view |-> 0, floor |-> 0]]

\* Exactly one physical PUT, completed before the identity is published.
\* No retries/recreation after deletion, even following a lost response.
CreatePermit == /\ ~s.applied \/ (Fault = "Recreate" /\ ~s.permit)
                /\ s' = [s EXCEPT !.permit = TRUE, !.applied = TRUE]
Publish == /\ ~s.published /\ (s.applied \/ Fault = "PublishEarly")
           /\ s' = [s EXCEPT !.published = TRUE]
Upload(n) == /\ s.published /\ n \in Numbers \ s.uploaded
             /\ s' = [s EXCEPT !.uploaded = @ \cup {n},
                                !.lateUpload = @ \/ (s.phase = "new")]

\* Complete the contiguous prefix BEFORE issuing its fresh permit GET.
BeginAck(n) == /\ s.published /\ s.ack.phase = "new"
               /\ (Ready(n) \/ Fault = "EarlyAck")
               /\ s' = [s EXCEPT !.ack = [phase |-> "prepared", n |-> n]]
CheckAck == /\ s.ack.phase = "prepared" /\ Permit
            /\ s' = [s EXCEPT !.ack.phase = "checked"]
ReplyAck == /\ s.ack.phase = "checked" /\ Ready(s.ack.n)
            /\ s' = [s EXCEPT !.oldView = s.ack.n, !.ack.phase = "done"]

\* A read/ack batch is sealed before its GET starts. A delayed success may
\* finish its existing operations, never admit requests that begin later.
BeginRead == /\ s.published /\ s.read.phase = "new"
             /\ s' = [s EXCEPT !.read.phase = "prepared", !.read.view = s.oldView,
                               !.read.floor = Visible]
CheckRead == /\ ~s.read.checked
             /\ (s.read.phase = "prepared" \/ Fault = "LateJoin")
             /\ s' = [s EXCEPT !.read.checked = TRUE, !.read.valid = Permit]
ReplyRead == /\ s.read.phase = "prepared" /\ s.read.checked /\ s.read.valid
             /\ s' = [s EXCEPT !.read.phase = "done", !.lateReply = s.newWrite]

\* Confirm revocation BEFORE discovering/sealing the old durable prefix.
\* Prefix abstracts the maintained journal model's fresh ordered point reads.
Revoke == /\ s.published /\ s.phase = "old"
          /\ s' = [s EXCEPT !.permit = FALSE, !.phase = "fenced"]
Scan == /\ s.phase = "fenced" \/ (Fault = "ScanEarly" /\ s.phase = "old")
        /\ s.published
        /\ s' = [s EXCEPT !.cut = Prefix(s.uploaded), !.phase = "scanned"]
Activate == /\ s.phase = "scanned" /\ s' = [s EXCEPT !.phase = "new"]
NewWrite == /\ s.phase = "new" /\ ~s.newWrite
            /\ s' = [s EXCEPT !.newWrite = TRUE]

Next == CreatePermit \/ Publish \/ Revoke \/ Scan \/ Activate \/ NewWrite
        \/ BeginRead \/ CheckRead \/ ReplyRead \/ CheckAck \/ ReplyAck
        \/ \E n \in Numbers: Upload(n) \/ BeginAck(n)
TypeOK == s \in [permit: BOOLEAN, applied: BOOLEAN, published: BOOLEAN,
    lateReply: BOOLEAN, lateUpload: BOOLEAN,
    phase: {"old", "fenced", "scanned", "new"}, uploaded: SUBSET Numbers,
    oldView: 0..Frames, cut: 0..Frames, newWrite: BOOLEAN,
    ack: [phase: {"new", "prepared", "checked", "done"}, n: 0..Frames],
    read: [phase: {"new", "prepared", "done"}, checked: BOOLEAN, valid: BOOLEAN,
           view: 0..Frames, floor: 0..(Frames+1)]]
AcknowledgedPrefixPreserved == s.phase \in {"scanned", "new"} => s.oldView <= s.cut
NoStaleRead == s.read.phase = "done" => s.read.floor <= s.read.view
Safety == AcknowledgedPrefixPreserved /\ NoStaleRead
NoDelayedReply == ~s.lateReply
NoLateUpload == ~s.lateUpload
Spec == Init /\ [][Next]_s
=====================================================================
