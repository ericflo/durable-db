--------------------------- MODULE SigningRotation ---------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Pods, Tokens, UnsafePromotion, UnsafeRetirement
VARIABLES phase, config, running, issued, signingKey, live
vars == <<phase, config, running, issued, signingKey, live>>

\* Configuration is immutable during a process lifetime. Restart reads the
\* operator's current configuration. A partitioned serving process is running;
\* it cannot be omitted from a rollout completion barrier.
Keys(c) == IF c = 0 THEN {"old"} ELSE IF c < 3 THEN {"old", "new"} ELSE {"new"}
Signer(c) == IF c < 2 THEN "old" ELSE "new"
Converged(p) == \A n \in running: config[n] >= p
Init == /\ phase = 0 /\ config = [n \in Pods |-> 0] /\ running = Pods
        /\ issued = {} /\ signingKey = [t \in Tokens |-> "old"] /\ live = {}

Advance == /\ phase < 3
           /\ IF phase = 0 THEN TRUE
              ELSE IF phase = 1 THEN (UnsafePromotion \/ Converged(1))
              ELSE /\ Converged(2)
                   /\ (UnsafeRetirement \/ ~\E t \in live: signingKey[t] = "old")
           /\ phase' = phase + 1
           /\ UNCHANGED <<config, running, issued, signingKey, live>>
Reload(n) == /\ n \in running /\ config[n] < phase
             /\ config' = [config EXCEPT ![n] = phase]
             /\ UNCHANGED <<phase, running, issued, signingKey, live>>
Crash(n) == /\ n \in running /\ running' = running \ {n}
            /\ UNCHANGED <<phase, config, issued, signingKey, live>>
Restart(n) == /\ n \notin running /\ running' = running \cup {n}
              /\ config' = [config EXCEPT ![n] = phase]
              /\ UNCHANGED <<phase, issued, signingKey, live>>
Issue(n,t) == /\ n \in running /\ t \notin issued
              /\ issued' = issued \cup {t} /\ live' = live \cup {t}
              /\ signingKey' = [signingKey EXCEPT ![t] = Signer(config[n])]
              /\ UNCHANGED <<phase, config, running>>
Expire(t) == /\ t \in live /\ live' = live \ {t}
             /\ UNCHANGED <<phase, config, running, issued, signingKey>>

Next == Advance \/ (\E n \in Pods: Reload(n) \/ Crash(n) \/ Restart(n)
                                \/ (\E t \in Tokens: Issue(n,t)))
        \/ (\E t \in Tokens: Expire(t))
TypeOK == /\ phase \in 0..3 /\ config \in [Pods -> 0..3] /\ running \subseteq Pods
          /\ issued \subseteq Tokens /\ live \subseteq issued
          /\ signingKey \in [Tokens -> {"old", "new"}]
RollingInteroperability == \A t \in live, n \in running: signingKey[t] \in Keys(config[n])
RetiredKeyNeverIssued == phase = 3 => \A n \in running: Signer(config[n]) = "new"
Spec == Init /\ [][Next]_vars
=============================================================================
