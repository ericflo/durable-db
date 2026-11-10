--------------------------- MODULE TenantAdmission ---------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Tenants, Nodes, Grants, Operations, Capacity, InitialCapacity, MaxAttempts,
          UnsafeLateCheck, UnsafeCrossScope, UnsafeMaintenanceOmission
ASSUME Cardinality(Tenants) = 2
\* Byte-dimension abstraction: each admission needs both contributions. The
\* runtime subtracts their sum atomically and spends ONE transaction attempt.
\* Retained-row rewrites, index/manifest overhead and retries are not modeled.
Components == {"journal", "checkpoint"}
Required(o) == {<<o,c>>: c \in Components}
A == CHOOSE t \in Tenants: TRUE
B == CHOOSE t \in Tenants: t # A
VARIABLES initializeda, runninga, allocateda, ceilinga, issueda, ownera, amounta, currenta, remaininga, chargeda, delivereda, refundeda
VARIABLES initializedb, runningb, allocatedb, ceilingb, issuedb, ownerb, amountb, currentb, remainingb, chargedb, deliveredb, refundedb
VARIABLE assigned
vars == <<initializeda, runninga, allocateda, ceilinga, issueda, ownera, amounta, currenta, remaininga, chargeda, delivereda, refundeda, initializedb, runningb, allocatedb, ceilingb, issuedb, ownerb, amountb, currentb, remainingb, chargedb, deliveredb, refundedb, assigned>>
\* Two independent instances of the existing credit protocol. Directory CAS
\* preserves the other ledger; each record is bound to a stable database name.
Credita == INSTANCE UploadCredits WITH
    Operations <- Operations \X Components,
    initialized <- initializeda, running <- runninga, allocated <- allocateda, ceiling <- ceilinga, issued <- issueda, owner <- ownera, amount <- amounta, current <- currenta, remaining <- remaininga, charged <- chargeda, delivered <- delivereda, refunded <- refundeda,
    UnsafeRefund <- FALSE, UnsafeRestore <- FALSE, UnsafeBeforeCharge <- FALSE,
    UnsafeRefillReset <- FALSE, UnsafeInitializeReset <- FALSE
Creditb == INSTANCE UploadCredits WITH
    Operations <- Operations \X Components,
    initialized <- initializedb, running <- runningb, allocated <- allocatedb, ceiling <- ceilingb, issued <- issuedb, owner <- ownerb, amount <- amountb, current <- currentb, remaining <- remainingb, charged <- chargedb, delivered <- deliveredb, refunded <- refundedb,
    UnsafeRefund <- FALSE, UnsafeRestore <- FALSE, UnsafeBeforeCharge <- FALSE,
    UnsafeRefillReset <- FALSE, UnsafeInitializeReset <- FALSE
Init == /\ Credita!Init /\ Creditb!Init /\ assigned = {}
Charged(t) == IF t = A THEN chargeda ELSE chargedb
Assign(t,o) == /\ <<t,o>> \notin assigned
               /\ Required(o) \subseteq Charged(t) \/ UnsafeLateCheck
                  \/ (UnsafeMaintenanceOmission /\ <<o,"journal">> \in Charged(t))
                  \/ (UnsafeCrossScope /\ \E u \in Tenants \ {t}: Required(o) \subseteq Charged(u))
               /\ assigned' = assigned \cup {<<t,o>>}
               /\ UNCHANGED <<<<initializeda, runninga, allocateda, ceilinga, issueda, ownera, amounta, currenta, remaininga, chargeda, delivereda, refundeda>>, <<initializedb, runningb, allocatedb, ceilingb, issuedb, ownerb, amountb, currentb, remainingb, chargedb, deliveredb, refundedb>>>>
Next == (Credita!Next /\ UNCHANGED <<<<initializedb, runningb, allocatedb, ceilingb, issuedb, ownerb, amountb, currentb, remainingb, chargedb, deliveredb, refundedb>>, assigned>>)
        \/ (Creditb!Next /\ UNCHANGED <<<<initializeda, runninga, allocateda, ceilinga, issueda, ownera, amounta, currenta, remaininga, chargeda, delivereda, refundeda>>, assigned>>)
        \/ (\E t \in Tenants, o \in Operations: Assign(t,o))
ScopeCoverage == /\ Credita!TypeOK /\ Creditb!TypeOK
                 /\ Credita!DurableCoverage /\ Creditb!DurableCoverage
                 /\ Credita!DeliveryCovered /\ Creditb!DeliveryCovered
                 /\ Credita!UploadBound /\ Creditb!UploadBound
                 /\ \A p \in assigned: Required(p[2]) \subseteq Charged(p[1])
Spec == Init /\ [][Next]_vars
=============================================================================
