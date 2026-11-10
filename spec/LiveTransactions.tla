--------------------------- MODULE LiveTransactions ---------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Requests, MaxWrites, UnsafeGap, UnsafeRead, UnsafeReject,
          UnsafePermission, UnsafeRetry, UnsafeCondition,
          UnsafeStalePolicy, UnsafePolicyReply
VARIABLES history, stored, visible, live, conflicts, answers, reads
vars == <<history, stored, visible, live, conflicts, answers, reads>>
Init == /\ history = <<>> /\ stored = {} /\ visible = 0 /\ live = TRUE
        /\ conflicts = {} /\ answers = {} /\ reads = {}
(* A serial database condition represents validation against accepted history.
   A retry resolves its existing pending/committed identity before validation.
   Ownership supplies live; its clock, drain and registration contracts are
   modeled separately. Each response records permission at its final check. *)
Authorized(n) == \A i \in 1..n: history[i].kind # "policy"
Accept(request, snapshot, condition) ==
    /\ live /\ Len(history) < MaxWrites /\ snapshot = Len(history)
    /\ (condition \/ UnsafeCondition)
    /\ Authorized(IF UnsafeStalePolicy THEN visible ELSE Len(history))
    /\ UnsafeRetry \/ request \notin {history[i].id : i \in 1..Len(history)}
    /\ history' = Append(history, [id |-> request, condition |-> condition, kind |-> "data"])
    /\ UNCHANGED <<stored, visible, live, conflicts, answers, reads>>
(* Catalog updates share journal order. A pending revocation blocks new
   admissions; reads and replies still use the committed policy. *)
Revoke == /\ live /\ Len(history) < MaxWrites /\ Authorized(Len(history))
          /\ history' = Append(history, [id |-> "revoke", condition |-> TRUE, kind |-> "policy"])
          /\ UNCHANGED <<stored, visible, live, conflicts, answers, reads>>
Upload(i) == /\ i \in 1..Len(history) /\ stored' = stored \cup {i}
             /\ UNCHANGED <<history, visible, live, conflicts, answers, reads>>
Publish(i) == /\ i \in (visible+1)..Len(history)
              /\ IF UnsafeGap THEN i \in stored ELSE 1..i \subseteq stored
              /\ visible' = i
              /\ UNCHANGED <<history, stored, live, conflicts, answers, reads>>
Reject(snapshot) ==
    /\ live /\ snapshot < Len(history)
    /\ conflicts' = conflicts \cup {Len(history)}
    /\ UNCHANGED <<history, stored, visible, live, answers, reads>>
Answer(i, kind) ==
    /\ live \/ UnsafePermission
    /\ Authorized(visible) \/ UnsafePolicyReply
    /\ IF kind = "commit" THEN i \in 1..visible
       ELSE i \in conflicts /\ (UnsafeReject \/ i <= visible)
    /\ answers' = answers \cup {[at |-> i, permission |-> live, authorized |-> Authorized(visible)]}
    /\ UNCHANGED <<history, stored, visible, live, conflicts, reads>>
Read == /\ live \/ UnsafePermission
        /\ Authorized(visible) \/ UnsafePolicyReply
        /\ reads' = reads \cup {[at |-> IF UnsafeRead THEN Len(history) ELSE visible,
                                 permission |-> live, authorized |-> Authorized(visible)]}
        /\ UNCHANGED <<history, stored, visible, live, conflicts, answers>>
Expire == /\ live /\ live' = FALSE
          /\ UNCHANGED <<history, stored, visible, conflicts, answers, reads>>
Next == (\E r \in Requests, s \in 0..MaxWrites, c \in BOOLEAN: Accept(r,s,c))
        \/ (\E i \in 1..MaxWrites: Upload(i) \/ Publish(i)
            \/ Answer(i,"commit") \/ Answer(i,"reject"))
        \/ (\E s \in 0..MaxWrites: Reject(s)) \/ Read \/ Expire \/ Revoke
Spec == Init /\ [][Next]_vars
DurableView == 1..visible \subseteq stored
DurableResponses == \A r \in answers \cup reads: 1..r.at \subseteq stored
Permission == \A r \in answers \cup reads: r.permission
AtMostOnce == \A i,j \in 1..Len(history): history[i].id = history[j].id => i = j
Conditions == \A i \in 1..Len(history): history[i].condition
AuthorizedAdmissions == \A i \in 1..Len(history): history[i].kind = "data" => Authorized(i-1)
AuthorizedResponses == \A r \in answers \cup reads: r.authorized
=============================================================================
