---------------------- MODULE AuthorizationLifecycle ----------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC
CONSTANTS Operations, Tokens, MaxVersion,
          UnsafeStaleAuthorization, UnsafeGeneration, UnsafeUserRevision, UnsafeRead
VARIABLES history, required, issued, generation, requests, badAccess
vars == <<history, required, issued, generation, requests, badAccess>>
InitialPolicy == [enabled |-> FALSE, generation |-> 0, revoked |-> {}]
InitialRoot == [version |-> 0, revision |-> 0, policy |-> InitialPolicy]
Current == history[Len(history)]
Allow(t, r, legacy, ignoreGeneration) ==
    IF legacy THEN ~r.policy.enabled /\ t = "legacy"
    ELSE IF t = "legacy" THEN ~r.policy.enabled
    ELSE /\ t \in issued
         /\ (~r.policy.enabled \/
              ((ignoreGeneration \/ generation[t] = r.policy.generation)
               /\ t \notin r.policy.revoked))
Permitted(t, r, legacy) == Allow(t,r,legacy,FALSE)
InitialRequest == [phase |-> "idle", token |-> "legacy", legacy |-> FALSE,
                   kind |-> "read", lower |-> 0, upper |-> 0,
                   base |-> InitialRoot, authorization |-> InitialRoot]
Init == /\ history = <<InitialRoot>> /\ required = InitialPolicy
        /\ issued = {} /\ generation = [t \in Tokens |-> 0]
        /\ requests = [o \in Operations |-> InitialRequest] /\ badAccess = FALSE

Mint(t) == /\ t \notin issued
           /\ issued' = issued \cup {t}
           /\ generation' = [generation EXCEPT ![t] = Current.policy.generation]
           /\ UNCHANGED <<history, required, requests, badAccess>>
PolicyUpdate(policy) ==
    /\ Current.version < MaxVersion
    /\ history' = Append(history, [version |-> Current.version+1,
                                    revision |-> Current.revision, policy |-> policy])
    /\ required' = policy
    /\ UNCHANGED <<issued, generation, requests, badAccess>>
Enable == /\ ~Current.policy.enabled
          /\ PolicyUpdate([Current.policy EXCEPT !.enabled = TRUE])
Rotate == /\ Current.policy.enabled
          /\ PolicyUpdate([Current.policy EXCEPT !.generation = @+1, !.revoked = {}])
Revoke(t) == /\ Current.policy.enabled /\ t \in issued /\ t \notin Current.policy.revoked
             /\ PolicyUpdate([Current.policy EXCEPT !.revoked = @ \cup {t}])

Invoke(o,t,k,l) == /\ requests[o].phase = "idle"
                  /\ t \in issued \cup {"legacy"}
                  /\ requests' = [requests EXCEPT ![o] = [@ EXCEPT
                       !.phase = "waiting", !.token = t, !.legacy = l, !.kind = k,
                       !.lower = Current.version, !.authorization = Current]]
                  /\ UNCHANGED <<history, required, issued, generation, badAccess>>
Prepare(o,r) ==
    /\ requests[o].phase = "waiting" /\ r \in 1..Len(history)
    /\ LET base == history[r]
           auth == IF UnsafeStaleAuthorization THEN requests[o].authorization ELSE base
       IN /\ Allow(requests[o].token, auth, requests[o].legacy, UnsafeGeneration)
          /\ (requests[o].kind = "write" \/ UnsafeRead \/ base.version = Current.version)
          /\ requests' = [requests EXCEPT ![o] = [@ EXCEPT !.phase = "prepared", !.base = base]]
    /\ UNCHANGED <<history, required, issued, generation, badAccess>>

\* Any helper may publish an already prepared write after its caller disappears.
Publish(o) ==
    /\ requests[o].kind = "write" /\ requests[o].phase \in {"prepared", "unknown"}
    /\ Current.version < MaxVersion
    /\ IF UnsafeUserRevision THEN requests[o].base.revision = Current.revision
                             ELSE requests[o].base.version = Current.version
    /\ history' = Append(history, [version |-> Current.version+1,
          revision |-> requests[o].base.revision+1, policy |-> requests[o].base.policy])
    /\ badAccess' = (badAccess \/ ~Permitted(requests[o].token, Current, requests[o].legacy))
    /\ requests' = [requests EXCEPT ![o].phase = "committed"]
    /\ UNCHANGED <<required, issued, generation>>
Return(o) == /\ requests[o].kind = "read" /\ requests[o].phase = "prepared"
             /\ badAccess' = (badAccess \/ ~Permitted(requests[o].token, requests[o].base, requests[o].legacy))
             /\ requests' = [requests EXCEPT ![o] = [@ EXCEPT !.phase = "returned", !.upper = Current.version]]
             /\ UNCHANGED <<history, required, issued, generation>>
Unknown(o) == /\ requests[o].kind = "write" /\ requests[o].phase = "prepared"
              /\ requests' = [requests EXCEPT ![o].phase = "unknown"]
              /\ UNCHANGED <<history, required, issued, generation, badAccess>>

Next == Enable \/ Rotate \/ (\E t \in Tokens: Mint(t) \/ Revoke(t))
        \/ (\E o \in Operations: Publish(o) \/ Return(o) \/ Unknown(o)
             \/ (\E r \in 1..Len(history): Prepare(o,r))
             \/ (\E t \in Tokens \cup {"legacy"}, k \in {"read","write"}, l \in BOOLEAN: Invoke(o,t,k,l)))

PolicyType == [enabled: BOOLEAN, generation: 0..MaxVersion, revoked: SUBSET Tokens]
RootType == [version: 0..MaxVersion, revision: 0..MaxVersion, policy: PolicyType]
RequestType == [phase: {"idle","waiting","prepared","unknown","committed","returned"},
                token: Tokens \cup {"legacy"}, legacy: BOOLEAN, kind: {"read","write"},
                lower: 0..MaxVersion, upper: 0..MaxVersion, base: RootType, authorization: RootType]
TypeOK == /\ history \in Seq(RootType) /\ Len(history) \in 1..(MaxVersion+1)
          /\ \A r \in 1..Len(history): history[r].version = r-1
          /\ required \in PolicyType /\ issued \subseteq Tokens
          /\ generation \in [Tokens -> 0..MaxVersion]
          /\ requests \in [Operations -> RequestType] /\ badAccess \in BOOLEAN
PolicyPreserved == Current.policy = required
AuthorizationAtLinearization == ~badAccess
ReadWithinInvocation == \A o \in Operations: requests[o].phase = "returned" =>
    /\ requests[o].lower <= requests[o].base.version
    /\ requests[o].base.version <= requests[o].upper
Spec == Init /\ [][Next]_vars
=============================================================================
