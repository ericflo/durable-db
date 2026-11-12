------------------------ MODULE OwnerChallenge ------------------------
EXTENDS Integers, FiniteSets
CONSTANT Fault
ASSUME Fault \in {"None", "Compare", "Coalesce", "Wait", "Answer", "Forget"}
VARIABLE s
vars == <<s>>
Candidates == {1, 2}
\* One incumbent, two challengers, one takeover, at most four metadata writes.
\* The register's exact CAS and unique revision assumptions are checked by the
\* existing provider/ownership models. No hint grants response permission here.
Init == s = [owner |-> 0, nonce |-> 0, revision |-> 0, alive |-> TRUE,
  joined |-> [c \in Candidates |-> 0], waited |-> {},
  captured |-> [c \in Candidates |-> -1], answer |-> -1,
  answered |-> FALSE, replaced |-> FALSE, valid |-> TRUE, coalesced |-> TRUE, kept |-> TRUE]
Challenge(c) == /\ s.owner = 0 /\ s.revision < 4
  /\ s' = [s EXCEPT !.nonce = IF @ = 0 \/ Fault = "Coalesce" THEN c ELSE @,
    !.revision = @ + 1,
    !.coalesced = @ /\ (s.nonce = 0 \/ s.nonce = c \/ Fault # "Coalesce")]
Join(c) == /\ s.owner = 0 /\ s.nonce # 0 /\ s.joined[c] = 0
  /\ s' = [s EXCEPT !.joined[c] = s.nonce]
Wait(c) == /\ s.joined[c] # 0
  /\ s' = [s EXCEPT !.waited = @ \cup {c}]
Prepare(c) == /\ s.owner = 0 /\ s.nonce # 0 /\ s.nonce = s.joined[c]
  /\ (c \in s.waited \/ Fault = "Wait")
  /\ s' = [s EXCEPT !.captured[c] = s.revision]
\* Captured requests may apply after their caller disappears or loses a reply.
Apply(c) == /\ s.captured[c] >= 0 /\ s.owner = 0 /\ s.revision < 4
  /\ (s.revision = s.captured[c] \/ Fault = "Compare")
  /\ s' = [s EXCEPT !.owner = c, !.nonce = 0, !.revision = @ + 1,
    !.replaced = TRUE,
    !.valid = ~s.answered /\ c \in s.waited]
\* Retrying an uncertain claim retains the completed owner/nonce wait. It
\* grants no authority and cannot survive an answer's changed comparison.
Forget(c) == /\ s.captured[c] >= 0
  /\ s' = [s EXCEPT !.captured[c] = -1,
    !.waited = IF Fault = "Forget" THEN @ \ {c} ELSE @,
    !.kept = @ /\ (c \notin s.waited \/ Fault # "Forget")]
RetryKeepsWait == s.kept
PrepareAnswer == /\ s.owner = 0 /\ s.nonce # 0 /\ s.alive
  /\ s' = [s EXCEPT !.answer = s.revision]
Answer == /\ s.owner = 0 /\ s.revision < 4
  /\ ((s.answer = s.revision /\ s.answer >= 0) \/ Fault = "Answer")
  /\ s' = [s EXCEPT !.nonce = 0, !.revision = @ + 1, !.answered = TRUE]
Stop == s' = [s EXCEPT !.alive = FALSE]
\* Unrelated metadata work can invalidate a comparison, never answer a probe.
Other == /\ s.revision < 4 /\ s' = [s EXCEPT !.revision = @ + 1]
Next == (\E c \in Candidates: Challenge(c) \/ Join(c) \/ Wait(c) \/ Prepare(c) \/ Apply(c) \/ Forget(c))
  \/ PrepareAnswer \/ Answer \/ Stop \/ Other
Coalesced == s.coalesced
Waited == s.replaced => s.valid
OwnerAnswered == s.answered => s.answer >= 0
\* One probe round: stop publishing after an answer. A new round uses a fresh
\* UUID in Rust; extending this bounded check to arbitrary rounds is separate.
Bound == ~s.answered \/ s.nonce = 0
DeadOwnerRecovered == ~(s.replaced /\ ~s.alive)
LiveOwnerAnswered == ~s.answered
Spec == Init /\ [][Next]_vars
=======================================================================
