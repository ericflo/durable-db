--------------------------- MODULE TenantAdmission ---------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Tenants, Nodes, Grants, Operations, Capacity, InitialCapacity, MaxAttempts,
          UnsafeLateCheck, UnsafeCrossScope, UnsafeCopyOmission
ASSUME Cardinality(Tenants) = 2
\* Byte-dimension abstraction of one transaction: direct work (journal +
\* new rows with their table/manifest encoding allowance) and base-table reblocking
\* plus copied manifest paths need same-name credit before assignment. For each checkpoint row with key
\* length k >= 19 and value length v >= 0, the singleton data/index encoding
\* 106 + 3*k + v <= 5*(14 + k + v). A table's authenticated data bytes include
\* all row encodings and block headers, so five times those bytes bounds any
\* repacking of its retained rows. This is conservative admission, not billing.
\* The writer pays even for a neighbor's rows and index in a shared table. Runtime
\* charges the sum and ONE attempt. Range selection/byte sizing are checked
\* separately against actual objects. Copies include authenticated manifest
\* nodes on affected paths, including the rightmost path for appends. Manifest
\* growth pays two singleton paths (at most nine levels) per output table;
\* old-derived boundaries use the maximum key length. Byte/row-triggered table
\* flushes are bounded by input bytes/(chunk bytes - maximum row bytes) and
\* input rows/chunk rows. Boundary flushes pay once per touched base table or
\* new terminal row; an append also funds splitting the old rightmost path.
\* This sizing argument is tested separately; maintenance retries are omitted.
Components == {"direct", "copies"}
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
                  \/ (UnsafeCopyOmission /\ (Required(o) \ {<<o,"copies">>}) \subseteq Charged(t))
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
