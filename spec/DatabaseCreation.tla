------------------------ MODULE DatabaseCreation ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Actors, Ids, Configs, MaxVersion,
          UnsafeReceiptLoss, UnsafeConfigMatch, UnsafeNameOnly, UnsafeCurrentResult
VARIABLES root, origin, calls, badIdentity, badResult
vars == <<root, origin, calls, badIdentity, badResult>>
Empty == [exists |-> FALSE, version |-> 0, revision |-> 0,
          id |-> "none", config |-> "none", incarnation |-> "none",
          receipt |-> FALSE]
InitialCall == [phase |-> "idle", id |-> "none", config |-> "none"]
Init == /\ root = Empty /\ origin = Empty
        /\ calls = [a \in Actors |-> InitialCall]
        /\ badIdentity = FALSE /\ badResult = FALSE
Invoke(a,id,config) ==
    /\ calls[a].phase = "idle"
    /\ calls' = [calls EXCEPT ![a] = [phase |-> "invoked", id |-> id, config |-> config]]
    /\ UNCHANGED <<root, origin, badIdentity, badResult>>
Prepare(a) ==
    /\ calls[a].phase = "invoked" /\ ~root.exists
    /\ calls' = [calls EXCEPT ![a].phase = "prepared"]
    /\ UNCHANGED <<root, origin, badIdentity, badResult>>
Disappear(a) ==
    /\ calls[a].phase = "prepared"
    /\ calls' = [calls EXCEPT ![a].phase = "unknown"]
    /\ UNCHANGED <<root, origin, badIdentity, badResult>>
\* A helper may publish after its originating caller disappears. Only an absent
\* target can accept a fresh creation; concurrent creators use fresh identities.
Publish(a) ==
    /\ calls[a].phase \in {"prepared", "unknown"} /\ ~root.exists
    /\ root' = [exists |-> TRUE, version |-> 1, revision |-> 0,
                id |-> calls[a].id, config |-> calls[a].config,
                incarnation |-> a, receipt |-> TRUE]
    /\ origin' = root'
    /\ UNCHANGED <<calls, badIdentity, badResult>>
Mutate ==
    /\ root.exists /\ root.version < MaxVersion
    /\ root' = [root EXCEPT !.version = @+1, !.revision = @+1,
                 !.receipt = IF UnsafeReceiptLoss THEN FALSE ELSE @]
    /\ UNCHANGED <<origin, calls, badIdentity, badResult>>
Recover(a) ==
    /\ calls[a].phase \in {"invoked", "prepared", "unknown"}
    /\ root.exists
    /\ (UnsafeNameOnly \/
        (root.receipt /\ calls[a].id = root.id /\
          (UnsafeConfigMatch \/ calls[a].config = root.config)))
    /\ calls' = [calls EXCEPT ![a].phase = "returned"]
    /\ badIdentity' = (badIdentity \/ calls[a].id # origin.id \/ calls[a].config # origin.config)
    /\ badResult' = (badResult \/ (UnsafeCurrentResult /\ root.revision # origin.revision))
    /\ UNCHANGED <<root, origin>>
Next == Mutate \/ (\E a \in Actors: Prepare(a) \/ Disappear(a) \/ Publish(a) \/ Recover(a)
                  \/ (\E id \in Ids, config \in Configs: Invoke(a,id,config)))
RootType == [exists: BOOLEAN, version: 0..MaxVersion, revision: 0..MaxVersion,
             id: Ids \cup {"none"}, config: Configs \cup {"none"},
             incarnation: Actors \cup {"none"}, receipt: BOOLEAN]
CallType == [phase: {"idle","invoked","prepared","unknown","returned"},
             id: Ids \cup {"none"}, config: Configs \cup {"none"}]
TypeOK == /\ root \in RootType /\ origin \in RootType /\ calls \in [Actors -> CallType]
          /\ badIdentity \in BOOLEAN /\ badResult \in BOOLEAN
ReceiptRetained == root.exists => /\ root.receipt /\ root.id = origin.id
                                  /\ root.config = origin.config /\ root.incarnation = origin.incarnation
ExactIdentity == ~badIdentity
OriginalResult == ~badResult
Spec == Init /\ [][Next]_vars
=============================================================================
