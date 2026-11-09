--------------------------- MODULE LogicalBackup ---------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Keys, Values, Requests, MaxRevision,
          UnsafeScanRevision, UnsafeTruncated, UnsafeForeign, UnsafeRestoreRevision
VARIABLES source, revision, phase, original, observed, archive, sealed,
          owner, target, targetRevision, verified, pending, foreignChanged, completed
vars == <<source, revision, phase, original, observed, archive, sealed,
          owner, target, targetRevision, verified, pending, foreignChanged, completed>>
N == Cardinality(Keys)
Empty == <<>>
Init == /\ source \in [1..N -> Values] /\ revision = 0
        /\ phase = "idle" /\ original = Empty /\ observed = 0
        /\ archive = Empty /\ sealed = FALSE
        /\ owner = "none" /\ target = Empty /\ targetRevision = 0
        /\ verified = 0 /\ pending = [r \in Requests |-> 0]
        /\ foreignChanged = FALSE /\ completed = FALSE
WriteSource(k,v) ==
    /\ revision < MaxRevision
    /\ source' = [source EXCEPT ![k] = v] /\ revision' = revision + 1
    /\ UNCHANGED <<phase, original, observed, archive, sealed, owner, target,
                   targetRevision, verified, pending, foreignChanged, completed>>
Begin == /\ phase = "idle" /\ phase' = "reading"
         /\ original' = source /\ observed' = revision
         /\ UNCHANGED <<source, revision, archive, sealed, owner, target,
                        targetRevision, verified, pending, foreignChanged, completed>>
ReadPage == /\ phase = "reading" /\ Len(archive) < N
            /\ (UnsafeScanRevision \/ observed = revision)
            /\ archive' = Append(archive, source[Len(archive)+1])
            /\ UNCHANGED <<source, revision, phase, original, observed, sealed,
                           owner, target, targetRevision, verified, pending, foreignChanged, completed>>
Seal == /\ phase = "reading" /\ (Len(archive) = N \/ UnsafeTruncated)
        /\ sealed' = TRUE /\ phase' = "sealed"
        /\ UNCHANGED <<source, revision, original, observed, archive, owner,
                       target, targetRevision, verified, pending, foreignChanged, completed>>
FailExport == /\ phase = "reading" /\ phase' = "failed"
              /\ UNCHANGED <<source, revision, original, observed, archive, sealed,
                             owner, target, targetRevision, verified, pending, foreignChanged, completed>>
ForeignCreate == /\ owner = "none" /\ owner' = "foreign"
                 /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                                target, targetRevision, verified, pending, foreignChanged, completed>>
Claim == /\ sealed /\ (owner \in {"none", "ours"} \/ UnsafeForeign)
         /\ owner' = "ours" /\ foreignChanged' = (foreignChanged \/ owner = "foreign")
         /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                        target, targetRevision, verified, pending, completed>>
VerifyPrefix == /\ owner = "ours" /\ target = SubSeq(archive, 1, targetRevision)
                /\ verified' = targetRevision
                /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                               owner, target, targetRevision, pending, foreignChanged, completed>>
Prepare(r) == /\ owner = "ours" /\ verified < Len(archive) /\ pending[r] = 0
              /\ pending' = [pending EXCEPT ![r] = verified+1]
              /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                             owner, target, targetRevision, verified, foreignChanged, completed>>
\* Requests may arrive after crash/restart. Expected revision prevents a delayed
\* request from being applied a second time; original receipts can also recover it.
Apply(r) == /\ pending[r] > 0 /\ targetRevision < N+1
            /\ (targetRevision = pending[r]-1 \/ UnsafeRestoreRevision)
            /\ target' = Append(target, archive[pending[r]])
            /\ targetRevision' = targetRevision+1
            /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                           owner, verified, pending, foreignChanged, completed>>
CrashRestore == /\ verified' = 0
                /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                               owner, target, targetRevision, pending, foreignChanged, completed>>
Complete == /\ owner = "ours" /\ targetRevision = Len(archive) /\ target = archive
            /\ completed' = TRUE
            /\ UNCHANGED <<source, revision, phase, original, observed, archive, sealed,
                           owner, target, targetRevision, verified, pending, foreignChanged>>
Next == Begin \/ ReadPage \/ Seal \/ FailExport \/ ForeignCreate \/ Claim \/ VerifyPrefix
        \/ CrashRestore \/ Complete \/ (\E r \in Requests: Prepare(r) \/ Apply(r))
        \/ (\E k \in 1..N, v \in Values: WriteSource(k,v))
TypeOK == /\ revision \in 0..MaxRevision /\ observed \in 0..MaxRevision
          /\ phase \in {"idle", "reading", "sealed", "failed"}
          /\ owner \in {"none", "ours", "foreign"}
          /\ targetRevision \in 0..(N+1) /\ verified \in 0..(N+1)
          /\ pending \in [Requests -> 0..N]
          /\ sealed \in BOOLEAN /\ completed \in BOOLEAN /\ foreignChanged \in BOOLEAN
ConsistentArchive == sealed => archive = original
ForeignUntouched == ~foreignChanged
RestorePrefix == owner = "ours" => /\ targetRevision <= Len(archive)
                                   /\ targetRevision = Len(target)
                                   /\ target = SubSeq(archive, 1, targetRevision)
CompleteSnapshot == completed => target = original
Spec == Init /\ [][Next]_vars
=============================================================================
