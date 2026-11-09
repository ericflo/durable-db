------------------------- MODULE OwnershipClaim -------------------------
EXTENDS Naturals
CONSTANTS OtherOwners, Empty, Claimant, InitialOwner, MaxRevision,
          UnsafeRetarget, UnsafeComparison
VARIABLES owner, revision, phase, target, expected, original, published, badOverwrite
vars == <<owner, revision, phase, target, expected, original, published, badOverwrite>>
Init == /\ owner = InitialOwner /\ revision = 0 /\ phase = "start"
        /\ target = Empty /\ original = Empty /\ expected = 0
        /\ published = FALSE /\ badOverwrite = FALSE
Begin == /\ phase = "start"
         /\ target' = owner /\ original' = owner /\ expected' = revision
         /\ phase' = "ready"
         /\ UNCHANGED <<owner, revision, published, badOverwrite>>
\* Other hosts can claim/release ownership or advance a revision while retaining
\* their identity. Metadata/fence conflicts need not imply an owner change.
Environment == /\ revision < MaxRevision
               /\ owner' \in OtherOwners \cup {Empty}
               /\ revision' = revision + 1
               /\ UNCHANGED <<phase, target, expected, original, published, badOverwrite>>
Publish == /\ phase = "ready" /\ revision < MaxRevision
           /\ (UnsafeComparison \/ expected = revision)
           /\ badOverwrite' = (badOverwrite \/ owner \notin {Empty, original})
           /\ owner' = Claimant /\ revision' = revision + 1
           /\ published' = TRUE /\ phase' \in {"done", "unknown"}
           /\ UNCHANGED <<target, expected, original>>
Conflict == /\ phase = "ready" /\ expected # revision
            /\ phase' = "conflict"
            /\ UNCHANGED <<owner, revision, target, expected, original, published, badOverwrite>>
\* Retrying keeps the FIRST observed owner as the target, and revalidates it on
\* every later snapshot, including after a backoff. Never silently target a new
\* competitor just because the caller originally requested explicit takeover.
Retry == /\ phase = "conflict"
         /\ IF owner = Claimant
               THEN /\ phase' = "done" /\ UNCHANGED <<target, expected>>
               ELSE IF UnsafeRetarget \/ owner \in {Empty, target}
                    THEN /\ phase' = "ready" /\ expected' = revision
                         /\ target' = (IF UnsafeRetarget THEN owner ELSE target)
                    ELSE /\ phase' = "failed" /\ UNCHANGED <<target, expected>>
         /\ UNCHANGED <<owner, revision, original, published, badOverwrite>>
UnknownBeforePublication == /\ phase = "ready" /\ phase' = "unknown"
                            /\ UNCHANGED <<owner, revision, target, expected, original, published, badOverwrite>>
ResolveUnknown == /\ phase = "unknown"
                  /\ phase' = (IF owner = Claimant THEN "done" ELSE "failed")
                  /\ UNCHANGED <<owner, revision, target, expected, original, published, badOverwrite>>
Next == Begin \/ Environment \/ Publish \/ Conflict \/ Retry \/ UnknownBeforePublication \/ ResolveUnknown
TypeOK == /\ owner \in OtherOwners \cup {Empty, Claimant}
          /\ target \in OtherOwners \cup {Empty} /\ original \in OtherOwners \cup {Empty}
          /\ revision \in 0..MaxRevision /\ expected \in 0..MaxRevision
          /\ phase \in {"start", "ready", "conflict", "unknown", "done", "failed"}
          /\ published \in BOOLEAN /\ badOverwrite \in BOOLEAN
PinnedTarget == ~badOverwrite
AcknowledgedClaimExists == phase = "done" => published
Spec == Init /\ [][Next]_vars
=============================================================================
