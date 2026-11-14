--------------------------- MODULE LiveTransactions ---------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Requests, MaxWrites, UnsafeGap, UnsafeRead, UnsafeReject,
          UnsafePermission, UnsafeRetry, UnsafeCondition,
          UnsafeStalePolicy, UnsafePolicyReply, UnsafeCheckpoint
VARIABLES history, stored, visible, live, conflicts, answers, reads, checkpoint
vars == <<history, stored, visible, live, conflicts, answers, reads, checkpoint>>
Init == /\ history = <<>> /\ stored = {} /\ visible = 0 /\ live = TRUE
        /\ conflicts = {} /\ answers = {} /\ reads = {}
        /\ checkpoint = [phase |-> "idle", cut |-> 0]
(* A serial database condition represents validation against accepted history.
   A retry resolves its existing pending/committed identity before validation.
   Ownership supplies live; its revocation and registration contracts are
   modeled separately. Each response records permission at its final check.
   HTTP disconnects are stuttering steps: they may suppress Answer, but cannot
   roll back Accept or cancel its Upload/Publish. The server owns that work. *)
Staging == checkpoint.phase \in {"building", "staged"}
Authorized(n) == \A i \in 1..n: history[i].kind # "policy"
Accept(request, snapshot, condition) ==
    /\ live /\ Len(history) < MaxWrites /\ (~Staging \/ UnsafeCheckpoint) /\ snapshot = Len(history)
    /\ (condition \/ UnsafeCondition)
    /\ Authorized(IF UnsafeStalePolicy THEN visible ELSE Len(history))
    /\ UnsafeRetry \/ request \notin {history[i].id : i \in 1..Len(history)}
    /\ history' = Append(history, [id |-> request, condition |-> condition, kind |-> "data"])
    /\ UNCHANGED <<stored, visible, live, conflicts, answers, reads, checkpoint>>
(* Catalog updates share journal order. A pending revocation blocks new
   admissions; reads and replies still use the committed policy. *)
Revoke == /\ live /\ Len(history) < MaxWrites /\ (~Staging \/ UnsafeCheckpoint) /\ Authorized(Len(history))
          /\ history' = Append(history, [id |-> "revoke", condition |-> TRUE, kind |-> "policy"])
          /\ UNCHANGED <<stored, visible, live, conflicts, answers, reads, checkpoint>>
(* HTTP coalescing refines successive Accept/Reject steps, then one immutable
   upload of a contiguous group. Catalog operations delimit admission groups;
   every response still resolves its own identity/policy and checks permission.
   Singleton/partial arrivals remain allowed as a safety overapproximation.
   Queue bytes, request-local errors and physical PUT counts are Rust checks. *)
Upload(i,j) == /\ i \in 1..Len(history) /\ j \in i..Len(history)
             /\ stored' = stored \cup (i..j)
             /\ UNCHANGED <<history, visible, live, conflicts, answers, reads, checkpoint>>
Publish(i) == /\ i \in (visible+1)..Len(history)
              /\ IF UnsafeGap THEN i \in stored ELSE 1..i \subseteq stored
              /\ visible' = i
              /\ UNCHANGED <<history, stored, live, conflicts, answers, reads, checkpoint>>
Reject(snapshot) ==
    /\ live /\ snapshot < Len(history)
    /\ conflicts' = conflicts \cup {Len(history)}
    /\ UNCHANGED <<history, stored, visible, live, answers, reads, checkpoint>>
Answer(i, kind) ==
    /\ live \/ UnsafePermission
    /\ Authorized(visible) \/ UnsafePolicyReply
    /\ IF kind = "commit" THEN i \in 1..visible
       ELSE i \in conflicts /\ (UnsafeReject \/ i <= visible)
    /\ answers' = answers \cup {[at |-> i, permission |-> live, authorized |-> Authorized(visible)]}
    /\ UNCHANGED <<history, stored, visible, live, conflicts, reads, checkpoint>>
Read == /\ live \/ UnsafePermission
        /\ Authorized(visible) \/ UnsafePolicyReply
        /\ reads' = reads \cup {[at |-> IF UnsafeRead THEN Len(history) ELSE visible,
                                 permission |-> live, authorized |-> Authorized(visible),
                                 phase |-> checkpoint.phase]}
        /\ UNCHANGED <<history, stored, visible, live, conflicts, answers, checkpoint>>
Expire == /\ live /\ live' = FALSE
          /\ UNCHANGED <<history, stored, visible, conflicts, answers, reads, checkpoint>>
(* Staging retains a shared serving borrow: mutations stop at a fully durable
   cut, while ordinary reads still seal and validate their committed result.
   Publication/rebase remains exclusive in Rust, as before; Checkpoint and
   collection models retain its separate register and immutable-object rules.
   Ending also abstracts cancellation before publication: it grants no authority.
   Optional handoff staging may end when a foreground command arrives. Its
   abandoned immutable PUTs may still finish; publication never starts for that
   cut. Rust allows one yield per owner, retains charges/retry pacing and never
   yields a required checkpoint, so a busy queue cannot keep cancelling staging.
   Those scheduling/resource rules are implementation obligations; no new
   authority or transition is introduced here. *)
BeginCheckpoint == /\ checkpoint.phase = "idle" /\ live
                   /\ visible > 0 /\ visible = Len(history)
                   /\ checkpoint' = [phase |-> "building", cut |-> visible]
                   /\ UNCHANGED <<history, stored, visible, live, conflicts, answers, reads>>
StageCheckpoint == /\ checkpoint.phase = "building"
                   /\ checkpoint' = [checkpoint EXCEPT !.phase = "staged"]
                   /\ UNCHANGED <<history, stored, visible, live, conflicts, answers, reads>>
EndCheckpoint == /\ Staging
                 /\ checkpoint' = [checkpoint EXCEPT !.phase = "done"]
                 /\ UNCHANGED <<history, stored, visible, live, conflicts, answers, reads>>
CheckpointCut == Staging => checkpoint.cut = visible /\ visible = Len(history)
NoReadDuringStaging == \A r \in reads: r.phase # "building"
Next == (\E r \in Requests, s \in 0..MaxWrites, c \in BOOLEAN: Accept(r,s,c))
        \/ (\E i,j \in 1..MaxWrites: Upload(i,j))
        \/ (\E i \in 1..MaxWrites: Publish(i)
            \/ Answer(i,"commit") \/ Answer(i,"reject"))
        \/ (\E s \in 0..MaxWrites: Reject(s)) \/ Read \/ Expire \/ Revoke \/ BeginCheckpoint \/ StageCheckpoint \/ EndCheckpoint
Spec == Init /\ [][Next]_vars
DurableView == 1..visible \subseteq stored
DurableResponses == \A r \in answers \cup reads: 1..r.at \subseteq stored
Permission == \A r \in answers \cup reads: r.permission
AtMostOnce == \A i,j \in 1..Len(history): history[i].id = history[j].id => i = j
Conditions == \A i \in 1..Len(history): history[i].condition
AuthorizedAdmissions == \A i \in 1..Len(history): history[i].kind = "data" => Authorized(i-1)
AuthorizedResponses == \A r \in answers \cup reads: r.authorized
=============================================================================
