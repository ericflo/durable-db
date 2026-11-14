------------------------------ MODULE DurableDb ------------------------------
(***************************************************************************)
(* The durable-db ownership, commit, checkpoint, collection and deletion    *)
(* protocol for ONE database, over an object store that offers only PUT,   *)
(* GET, LIST and DELETE.  No conditional writes, no clocks, no leases.     *)
(*                                                                         *)
(* Objects (all immutable; a key is written by exactly one writer):        *)
(*   claim    c/<epoch>          a writer's permit; deleting it revokes    *)
(*   manifest m/<epoch>/<gen>    a durable cut of the state                *)
(*   frame    l/<epoch>/<n>      one group of committed transactions       *)
(*   tombstone m/~               the database is deleted, for good         *)
(*                                                                         *)
(* An epoch id is <<number, writer>>, ordered lexicographically.           *)
(* Tables are abstracted into the manifest's `state`; their collection     *)
(* follows the same rule as superseded manifests and covered frames.       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Writers,               \* a set of naturals; ties between equal epoch numbers
  MaxEpoch,              \* epoch numbers range over 1..MaxEpoch
  MaxTxn,                \* transactions ever proposed
  Deletion,              \* whether an owner may be asked to delete the database
  \* Protocol rules.  The real protocol sets every one of these to TRUE.
  \* Each negative control sets exactly one to FALSE and must be refuted.
  CheckBeforeAck,        \* acknowledge only after a claim check that began after the frame PUT
  AbortOnHigherClaim,    \* give up when a higher claim is listed
  AbortOnHigherManifest, \* give up when a higher epoch has a manifest
  RevokeBeforeRead,      \* delete lower claims before reading manifests and frames
  CheckBeforeCollect,    \* delete superseded objects only after a claim check
  SingleClaimAttempt,    \* never use a claim whose PUT may still be in flight
  KeepClaimWhileSending, \* never delete the own claim while an own manifest PUT may still arrive
  KeepClaimAfterRevoke,  \* after revoking, give the claim back only if a higher epoch outranks this one
  CheckBeforeServing,    \* expose recovered state only after a claim check that follows the first manifest
  CheckBeforeTombstone,  \* write the tombstone only after a claim check that began after a manifest marked deleted was stored
  TombstoneBeforePurge   \* delete a deleted database's objects only once the tombstone is stored

VARIABLES
  \* ---- the object store ----
  claims,     \* epoch ids whose claim object exists
  manifests,  \* [e, g, state, cov, del]
  frames,     \* [e, n, txn]
  tomb,       \* whether the tombstone exists
  \* ---- requests that may still arrive, at any time, any number of times ----
  sentC,      \* claim PUTs the claimant gave up on (or, in the control, all of them)
  sentM,      \* manifest PUTs (retried until success, so duplicates may arrive late)
  sentF,      \* frame PUTs (likewise)
  sentT,      \* whether a tombstone PUT was sent
  \* ---- writer-local state, lost on a crash ----
  pc, ep, st, nb, sent, conf, pm, todo, collect, rs,
  gen,        \* newest manifest generation sent (never reused, even after a failure)
  fc,         \* frames covered by that manifest
  cg, cc,     \* generation and coverage of the newest manifest stored AND checked
  dl,         \* whether the state this writer holds is "the database is deleted"
  \* ---- bookkeeping ----
  lastN,      \* a writer never reuses an epoch id (fresh random nonce)
  nextTxn, acked, badRead,
  gone        \* some writer has concluded that the database is deleted

store == <<claims, manifests, frames, tomb>>
wire  == <<sentC, sentM, sentF, sentT>>
local == <<pc, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, dl>>
book  == <<lastN, nextTxn, acked, badRead, gone>>
vars  == <<store, wire, local, book>>

Less(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])
Top(S) == CHOOSE x \in S : \A y \in S : y = x \/ Less(y, x)
IsPrefix(p, s) == Len(p) <= Len(s) /\ \A i \in 1..Len(p) : p[i] = s[i]

\* What a writer starts from when the database does not exist yet. It is
\* not an object: the store begins empty.
Genesis == [e |-> <<0, 0>>, g |-> 0, state |-> <<>>, cov |-> 0, del |-> FALSE]

\* The newest manifest of the highest epoch among `ms`.
Newest(ms) ==
  LET e == Top({m.e : m \in ms})
      g == CHOOSE x \in {m.g : m \in {y \in ms : y.e = e}} :
             \A m \in ms : m.e = e => m.g <= x
  IN  CHOOSE m \in ms : m.e = e /\ m.g = g

\* The transactions in the contiguous frames of epoch e that follow frame c.
Run(fs, e, c) ==
  LET Has(i) == \E f \in fs : f.e = e /\ f.n = c + i
      k == CHOOSE x \in 0..MaxTxn :
             (\A i \in 1..x : Has(i)) /\ (x = MaxTxn \/ ~Has(x + 1))
  IN  [i \in 1..k |-> (CHOOSE f \in fs : f.e = e /\ f.n = c + i).txn]

Manifest(w, g, c) ==
  [e |-> ep[w], g |-> g, state |-> SubSeq(st[w], 1, nb[w] + c), cov |-> c, del |-> dl[w]]
Sent(w) == Manifest(w, gen[w], fc[w])       \* the newest manifest this writer sent
Checked(w) == Manifest(w, cg[w], cc[w])     \* the newest one it knows is stored and its own
Visible(w) == SubSeq(st[w], 1, nb[w] + conf[w])

Init ==
  /\ claims = {} /\ manifests = {} /\ frames = {} /\ tomb = FALSE
  /\ sentC = {} /\ sentM = {} /\ sentF = {} /\ sentT = FALSE
  /\ pc = [w \in Writers |-> "idle"]
  /\ ep = [w \in Writers |-> <<0, w>>]
  /\ st = [w \in Writers |-> <<>>]
  /\ nb = [w \in Writers |-> 0]
  /\ sent = [w \in Writers |-> 0]
  /\ conf = [w \in Writers |-> 0]
  /\ gen = [w \in Writers |-> 0]
  /\ fc = [w \in Writers |-> 0]
  /\ cg = [w \in Writers |-> 0]
  /\ cc = [w \in Writers |-> 0]
  /\ pm = [w \in Writers |-> Genesis]
  /\ todo = [w \in Writers |-> {}]
  /\ collect = [w \in Writers |-> FALSE]
  /\ rs = [w \in Writers |-> {}]
  /\ dl = [w \in Writers |-> FALSE]
  /\ lastN = [w \in Writers |-> 0]
  /\ nextTxn = 1 /\ acked = {} /\ badRead = FALSE /\ gone = FALSE

Reset(w) ==
  /\ pc' = [pc EXCEPT ![w] = "idle"]
  /\ st' = [st EXCEPT ![w] = <<>>]
  /\ nb' = [nb EXCEPT ![w] = 0]
  /\ sent' = [sent EXCEPT ![w] = 0]
  /\ conf' = [conf EXCEPT ![w] = 0]
  /\ gen' = [gen EXCEPT ![w] = 0]
  /\ fc' = [fc EXCEPT ![w] = 0]
  /\ cg' = [cg EXCEPT ![w] = 0]
  /\ cc' = [cc EXCEPT ![w] = 0]
  /\ pm' = [pm EXCEPT ![w] = Genesis]
  /\ todo' = [todo EXCEPT ![w] = {}]
  /\ collect' = [collect EXCEPT ![w] = FALSE]
  /\ rs' = [rs EXCEPT ![w] = {}]
  /\ dl' = [dl EXCEPT ![w] = FALSE]
  /\ UNCHANGED ep

(* ------------------------------ activation ------------------------------ *)

\* The epoch number is only a hint (in practice 1 + the highest listed), so
\* any unused number is allowed: safety must not depend on the choice.
\* A node that already sees the tombstone does not begin.
Start(w) ==
  /\ pc[w] = "idle" /\ ~tomb
  /\ \E n \in (lastN[w] + 1)..MaxEpoch :
       /\ ep' = [ep EXCEPT ![w] = <<n, w>>]
       /\ lastN' = [lastN EXCEPT ![w] = n]
  /\ pc' = [pc EXCEPT ![w] = "claim"]
  /\ UNCHANGED <<store, wire, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, dl,
                 nextTxn, acked, badRead, gone>>

\* One PUT attempt.  Acknowledged: proceed.  Otherwise: abandon this id.
PutClaim(w) ==
  /\ pc[w] = "claim"
  /\ \/ /\ claims' = claims \cup {ep[w]}
        /\ pc' = [pc EXCEPT ![w] = "listc"]
        /\ sentC' = IF SingleClaimAttempt THEN sentC ELSE sentC \cup {ep[w]}
     \/ /\ sentC' = sentC \cup {ep[w]}
        /\ pc' = [pc EXCEPT ![w] = "idle"]
        /\ UNCHANGED claims
  /\ UNCHANGED <<manifests, frames, tomb, sentM, sentF, sentT, ep, st, nb, sent, conf, gen,
                 fc, cg, cc, pm, todo, collect, rs, dl, book>>

ListClaims(w) ==
  /\ pc[w] = "listc"
  /\ IF AbortOnHigherClaim /\ \E c \in claims : Less(ep[w], c)
       THEN /\ pc' = [pc EXCEPT ![w] = "abort"]
            /\ UNCHANGED todo
       ELSE /\ todo' = [todo EXCEPT ![w] = {c \in claims : Less(c, ep[w])}]
            /\ pc' = [pc EXCEPT ![w] = IF RevokeBeforeRead THEN "revoke" ELSE "listm"]
  /\ UNCHANGED <<store, wire, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, collect, rs, dl, book>>

Revoke(w) ==
  /\ pc[w] = "revoke"
  /\ IF todo[w] = {}
       THEN /\ pc' = [pc EXCEPT ![w] = IF RevokeBeforeRead THEN "listm" ELSE "putm"]
            /\ UNCHANGED <<claims, todo>>
       ELSE \E c \in todo[w] :
              /\ claims' = claims \ {c}
              /\ todo' = [todo EXCEPT ![w] = @ \ {c}]
              /\ UNCHANGED pc
  /\ UNCHANGED <<manifests, frames, tomb, wire, ep, st, nb, sent, conf, gen, fc, cg, cc, pm,
                 collect, rs, dl, book>>

\* The tombstone is listed with the manifests. With no manifest at all the
\* database does not exist, and this writer creates it. (A writer that was
\* not asked to create stops instead, keeping its claim: Crash.)
ListManifests(w) ==
  /\ pc[w] = "listm"
  /\ LET lower == {m \in manifests : Less(m.e, ep[w])} IN
       IF tomb \/ (AbortOnHigherManifest /\ \E m \in manifests : Less(ep[w], m.e))
         THEN /\ pc' = [pc EXCEPT ![w] = "abort"]
              /\ UNCHANGED pm
         ELSE /\ pm' = [pm EXCEPT ![w] = IF lower = {} THEN Genesis ELSE Newest(lower)]
              /\ pc' = [pc EXCEPT ![w] = "getm"]
  /\ UNCHANGED <<store, wire, ep, st, nb, sent, conf, gen, fc, cg, cc, todo, collect, rs, dl, book>>

\* GET the manifest and the frames after it.  A missing manifest means a
\* higher epoch collected it, so this writer has already been revoked.
ReadState(w) ==
  /\ pc[w] = "getm"
  /\ IF pm[w] = Genesis \/ pm[w] \in manifests
       THEN LET s == IF pm[w].del THEN <<>>
                     ELSE pm[w].state \o Run(frames, pm[w].e, pm[w].cov) IN
            /\ st' = [st EXCEPT ![w] = s]
            /\ nb' = [nb EXCEPT ![w] = Len(s)]
            /\ dl' = [dl EXCEPT ![w] = pm[w].del]
            /\ pc' = [pc EXCEPT ![w] = IF RevokeBeforeRead THEN "putm" ELSE "revoke"]
       ELSE /\ pc' = [pc EXCEPT ![w] = "abort"]
            /\ UNCHANGED <<st, nb, dl>>
  /\ UNCHANGED <<store, wire, ep, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, book>>

PutFirstManifest(w) ==
  /\ pc[w] = "putm"
  /\ sentM' = sentM \cup {Manifest(w, 0, 0)}
  /\ pc' = [pc EXCEPT ![w] = "waitm"]
  /\ UNCHANGED <<store, sentC, sentF, sentT, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo,
                 collect, rs, dl, book>>

\* The first manifest is stored. One claim check later, everything this
\* writer recovered is permanent, including transactions its predecessor
\* stored but never acknowledged: retries of them are now answered and
\* readers now see them. The same holds for a deletion the predecessor
\* stored but never confirmed: it is adopted, and is final from here.
Activate(w) ==
  /\ pc[w] = "waitm"
  /\ Sent(w) \in manifests
  /\ IF ep[w] \in claims \/ ~CheckBeforeServing \/ (dl[w] /\ ~CheckBeforeTombstone)
       THEN IF dl[w]
              THEN /\ pc' = [pc EXCEPT ![w] = "tomb"]
                   /\ gone' = TRUE
                   /\ UNCHANGED <<acked, st, nb, sent, conf, gen, fc, cg, cc, pm, todo,
                                  collect, rs, dl>>
              ELSE /\ pc' = [pc EXCEPT ![w] = "active"]
                   /\ acked' = acked \cup {st[w]}
                   /\ collect' = [collect EXCEPT ![w] = ep[w] \in claims \/ ~CheckBeforeCollect]
                   /\ UNCHANGED <<gone, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, rs, dl>>
       ELSE /\ Reset(w)
            /\ UNCHANGED <<acked, gone>>
  /\ UNCHANGED <<store, wire, ep, lastN, nextTxn, badRead>>

\* Giving the claim back. Reached only when a higher claim, a higher
\* epoch's manifest or the tombstone was seen: something above this epoch
\* stays in the store, so no lower epoch can serve in its place.
Abort(w) ==
  /\ pc[w] = "abort"
  /\ claims' = claims \ {ep[w]}
  /\ Reset(w)
  /\ UNCHANGED <<manifests, frames, tomb, wire, book>>

\* Any other failure during a takeover is a Crash: the claim stays. The
\* writer may already have revoked a lower epoch whose first manifest is
\* still on its way; were both claims gone, an epoch below that manifest
\* could serve and then be outranked by it. Forbidden, and only here to be
\* refuted.
GiveUp(w) ==
  /\ ~KeepClaimAfterRevoke
  /\ pc[w] \in {"revoke", "listm", "getm", "putm"}
  /\ pc' = [pc EXCEPT ![w] = "abort"]
  /\ UNCHANGED <<store, wire, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, dl,
                 book>>

(* ------------------------------- serving -------------------------------- *)

\* Validate against local state and send the frame.  Frames are pipelined.
Commit(w) ==
  /\ pc[w] = "active"
  /\ nextTxn <= MaxTxn
  /\ st' = [st EXCEPT ![w] = Append(@, nextTxn)]
  /\ sent' = [sent EXCEPT ![w] = @ + 1]
  /\ sentF' = sentF \cup {[e |-> ep[w], n |-> sent[w] + 1, txn |-> nextTxn]}
  /\ nextTxn' = nextTxn + 1
  /\ UNCHANGED <<store, sentC, sentM, sentT, pc, ep, nb, conf, gen, fc, cg, cc, pm, todo,
                 collect, rs, dl, lastN, acked, badRead, gone>>

\* The claim check: one HEAD that begins after frames conf+1..k are stored.
\* It acknowledges those frames and answers every read that began before it.
Check(w) ==
  /\ pc[w] = "active"
  /\ \E k \in conf[w]..sent[w] :
       /\ \A i \in (conf[w] + 1)..k : \E f \in frames : f.e = ep[w] /\ f.n = i
       /\ (k > conf[w] \/ rs[w] # {})
       /\ IF ep[w] \in claims \/ ~CheckBeforeAck
            THEN /\ conf' = [conf EXCEPT ![w] = k]
                 /\ acked' = acked \cup {SubSeq(st[w], 1, nb[w] + i) : i \in (conf[w] + 1)..k}
                 /\ badRead' = (badRead \/ \E p \in rs[w] : ~IsPrefix(p, SubSeq(st[w], 1, nb[w] + k)))
                 /\ rs' = [rs EXCEPT ![w] = {}]
                 /\ collect' = [collect EXCEPT ![w] = @ \/ ep[w] \in claims]
                 /\ UNCHANGED <<pc, st, nb, sent, gen, fc, cg, cc, pm, todo, dl>>
            ELSE /\ Reset(w)                     \* revoked: forget everything
                 /\ UNCHANGED <<acked, badRead>>
  /\ UNCHANGED <<store, wire, ep, lastN, nextTxn, gone>>

\* A linearizable read arrives.  It must observe everything acknowledged so far.
StartRead(w) ==
  /\ pc[w] = "active" /\ rs[w] = {} /\ acked # {}
  /\ rs' = [rs EXCEPT ![w] = acked]
  /\ UNCHANGED <<store, wire, pc, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, dl,
                 book>>

(* --------------------------- checkpoint, collection --------------------- *)

Flush(w) ==
  /\ pc[w] = "active" /\ conf[w] > fc[w]
  /\ gen' = [gen EXCEPT ![w] = @ + 1]
  /\ fc' = [fc EXCEPT ![w] = conf[w]]
  /\ sentM' = sentM \cup {Manifest(w, gen[w] + 1, conf[w])}
  /\ pc' = [pc EXCEPT ![w] = "flushing"]
  /\ UNCHANGED <<store, sentC, sentF, sentT, ep, st, nb, sent, conf, cg, cc, pm, todo, collect, rs,
                 dl, book>>

\* The claim check that follows a stored manifest. Only now may the writer
\* delete what that manifest no longer references.
FlushCheck(w) ==
  /\ pc[w] = "flushing"
  /\ Sent(w) \in manifests
  /\ IF ep[w] \in claims \/ ~CheckBeforeCollect
       THEN /\ pc' = [pc EXCEPT ![w] = "active"]
            /\ cg' = [cg EXCEPT ![w] = gen[w]]
            /\ cc' = [cc EXCEPT ![w] = fc[w]]
            /\ collect' = [collect EXCEPT ![w] = TRUE]
            /\ UNCHANGED <<st, nb, sent, conf, gen, fc, pm, todo, rs, dl>>
       ELSE Reset(w)
  /\ UNCHANGED <<store, wire, ep, book>>

\* The upload failed as far as the writer can tell. It carries on from its
\* last checked manifest; the abandoned one may still arrive at any time.
FlushAbandon(w) ==
  /\ pc[w] = "flushing"
  /\ pc' = [pc EXCEPT ![w] = "active"]
  /\ UNCHANGED <<store, wire, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, dl,
                 book>>

\* Delete one object that the newest checked manifest no longer needs:
\* anything of a lower epoch, own older manifests, own covered frames.
Collect(w) ==
  /\ pc[w] = "active" /\ collect[w]
  /\ \/ \E m \in manifests :
          /\ Less(m.e, ep[w]) \/ (m.e = ep[w] /\ m.g < cg[w])
          /\ manifests' = manifests \ {m}
          /\ UNCHANGED frames
     \/ \E f \in frames :
          /\ Less(f.e, ep[w]) \/ (f.e = ep[w] /\ f.n <= cc[w])
          /\ frames' = frames \ {f}
          /\ UNCHANGED manifests
  /\ UNCHANGED <<claims, tomb, wire, local, book>>

(* -------------------------------- deletion ------------------------------ *)

\* Deleting the database is a change of state like any other: the owner
\* stops serving and stores a manifest that says "deleted". If a successor
\* took over before that manifest was stored, the successor's manifests
\* outrank it and the deletion never happened.
Delete(w) ==
  /\ Deletion /\ pc[w] = "active"
  /\ dl' = [dl EXCEPT ![w] = TRUE]
  /\ st' = [st EXCEPT ![w] = <<>>]
  /\ nb' = [nb EXCEPT ![w] = 0]
  /\ sent' = [sent EXCEPT ![w] = 0]
  /\ conf' = [conf EXCEPT ![w] = 0]
  /\ gen' = [gen EXCEPT ![w] = @ + 1]
  /\ fc' = [fc EXCEPT ![w] = 0]
  /\ cg' = [cg EXCEPT ![w] = 0]
  /\ cc' = [cc EXCEPT ![w] = 0]
  /\ rs' = [rs EXCEPT ![w] = {}]
  /\ collect' = [collect EXCEPT ![w] = FALSE]
  /\ sentM' = sentM \cup {[e |-> ep[w], g |-> gen[w] + 1, state |-> <<>>, cov |-> 0, del |-> TRUE]}
  /\ pc' = [pc EXCEPT ![w] = "deleting"]
  /\ UNCHANGED <<store, sentC, sentF, sentT, ep, pm, todo, book>>

\* The claim check that follows the stored manifest. If the claim stands,
\* every later owner starts from that manifest, so the deletion is final.
\* If the upload fails as far as the writer can tell, it stops (Crash).
DeleteCheck(w) ==
  /\ pc[w] = "deleting"
  /\ Sent(w) \in manifests
  /\ IF ep[w] \in claims \/ ~CheckBeforeTombstone
       THEN /\ pc' = [pc EXCEPT ![w] = "tomb"]
            /\ gone' = TRUE
            /\ UNCHANGED <<st, nb, sent, conf, gen, fc, cg, cc, pm, todo, collect, rs, dl>>
       ELSE /\ Reset(w)
            /\ UNCHANGED gone
  /\ UNCHANGED <<store, wire, ep, lastN, nextTxn, acked, badRead>>

\* The tombstone records a deletion that is already final. It lets a node
\* answer "gone" from one listing, and it outlives the purge below.
PutTombstone(w) ==
  /\ pc[w] = "tomb"
  /\ sentT' = TRUE
  /\ pc' = [pc EXCEPT ![w] = "purge"]
  /\ UNCHANGED <<store, sentC, sentM, sentF, ep, st, nb, sent, conf, gen, fc, cg, cc, pm, todo,
                 collect, rs, dl, book>>

\* Delete every object of the database except the tombstone. Uploads sent
\* earlier may still arrive afterwards; the tombstone keeps them unread.
Purge(w) ==
  /\ \/ pc[w] = "purge" /\ tomb
     \/ pc[w] \in {"tomb", "purge"} /\ ~TombstoneBeforePurge
  /\ claims' = {} /\ manifests' = {} /\ frames' = {}
  /\ Reset(w)
  /\ UNCHANGED <<tomb, wire, book>>

(* --------------------------- failure and network ------------------------ *)

\* Graceful release: delete the own claim, once the newest manifest sent is
\* known to be stored. A failed activation gives up the same way, which is
\* why it may do so only before its first manifest is sent (Abort).
Release(w) ==
  /\ \/ pc[w] = "active" /\ (Sent(w) \in manifests \/ ~KeepClaimWhileSending)
     \/ pc[w] \in {"waitm", "flushing"} /\ ~KeepClaimWhileSending
  /\ claims' = claims \ {ep[w]}
  /\ Reset(w)
  /\ UNCHANGED <<manifests, frames, tomb, wire, book>>

Crash(w) ==
  /\ pc[w] # "idle"
  /\ Reset(w)
  /\ UNCHANGED <<store, wire, book>>

\* A request sent earlier arrives now: first delivery, or a duplicate, even
\* after its sender crashed and even after the object was collected.
ArriveClaim == \E c \in sentC \ claims :
  /\ claims' = claims \cup {c}
  /\ UNCHANGED <<manifests, frames, tomb, wire, local, book>>
ArriveManifest == \E m \in sentM \ manifests :
  /\ manifests' = manifests \cup {m}
  /\ UNCHANGED <<claims, frames, tomb, wire, local, book>>
ArriveFrame == \E f \in sentF \ frames :
  /\ frames' = frames \cup {f}
  /\ UNCHANGED <<claims, manifests, tomb, wire, local, book>>
ArriveTombstone ==
  /\ sentT /\ ~tomb
  /\ tomb' = TRUE
  /\ UNCHANGED <<claims, manifests, frames, wire, local, book>>

Next ==
  \/ \E w \in Writers :
       \/ Start(w) \/ PutClaim(w) \/ ListClaims(w) \/ Revoke(w) \/ ListManifests(w)
       \/ ReadState(w) \/ PutFirstManifest(w) \/ Activate(w) \/ Abort(w) \/ GiveUp(w)
       \/ Commit(w) \/ Check(w) \/ StartRead(w)
       \/ Flush(w) \/ FlushCheck(w) \/ FlushAbandon(w) \/ Collect(w)
       \/ Delete(w) \/ DeleteCheck(w) \/ PutTombstone(w) \/ Purge(w)
       \/ Release(w) \/ Crash(w)
  \/ ArriveClaim \/ ArriveManifest \/ ArriveFrame \/ ArriveTombstone

Spec == Init /\ [][Next]_vars

(* -------------------------------- safety -------------------------------- *)

\* Every acknowledged transaction, together with the exact history before
\* it, is recoverable from the bucket alone.  All acknowledged histories are
\* therefore prefixes of one sequence: no fork, no loss, no reordering.
\* A deleted database owes nothing.
Durable ==
  \/ gone
  \/ manifests = {} /\ acked = {}
  \/ manifests # {} /\ LET h == Newest(manifests) IN
       h.del \/ \A p \in acked : IsPrefix(p, h.state \o Run(frames, h.e, h.cov))

\* A checked read never misses a transaction acknowledged before it began.
FreshReads == ~badRead

\* Nobody collects the manifest of an owner whose claim still stands.
OwnerIntact == \A w \in Writers :
  (pc[w] \in {"active", "flushing"} /\ ep[w] \in claims) => Checked(w) \in manifests

\* Deletion is final: once any writer has concluded that the database is
\* deleted, no writer holds it, so nothing is acknowledged or read again.
Final == gone => \A w \in Writers : ~(pc[w] \in {"active", "flushing"} /\ ep[w] \in claims)

=============================================================================
