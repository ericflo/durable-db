------------------------- MODULE PartitionControl -------------------------
EXTENDS RootControl
CONSTANTS Partitions, Generations, MaxOwnerRequests,
          UnsafeAssignment, UnsafeMoveDrain, UnsafeOwnerReply
VARIABLES placement, grants, recoveredMove
ownerVars == <<placement, grants, recoveredMove>>
allVars == <<vars, ownerVars>>
Owners == Ids \X Partitions \X (1..Generations)
OwnerValid(o) == now < grants[o].expires
OwnerReplyEnd(o) == (IF UnsafeOwnerReply THEN now ELSE grants[o].requested)
                   + grants[o].duration
OwnerInit ==
    /\ recoveredMove = FALSE
    /\ placement = [d \in Partitions |->
         [generation |-> 1, phase |-> "open", root |-> 0, drain |-> 0]]
    /\ grants = [o \in Owners |->
         [pending |-> "none", requested |-> 0, duration |-> 0, expires |-> 0, requests |-> 0, extended |-> FALSE]]
\* The actual root protocol runs unchanged. Placement and freeze decisions
\* survive root loss; this component abstracts their conditional wire update.
ControlStep == Next /\ UNCHANGED ownerVars
RequestOwner(o) == /\ grants[o].pending = "none"
                   /\ o[3] <= placement[o[2]].generation
                   /\ grants' = [grants EXCEPT ![o].pending = "request",
                        ![o].requested = now, ![o].requests = @ + 1]
                   /\ UNCHANGED <<vars, placement, recoveredMove>>
IssueOwner(o) ==
    /\ RootValid(o[1]) /\ grants[o].pending = "request"
    /\ (UnsafeAssignment \/ (placement[o[2]].phase = "open"
                              /\ placement[o[2]].generation = o[3]))
    /\ grants' = [grants EXCEPT ![o].pending = "reply",
         ![o].duration = roots[o[1]].expires - now]
    /\ UNCHANGED <<vars, placement, recoveredMove>>
DeliverOwner(o) == /\ grants[o].pending = "reply"
                   /\ grants' = [grants EXCEPT ![o].pending = "none",
                        ![o].expires = Max(@, OwnerReplyEnd(o)),
                        ![o].extended = @ \/ (grants[o].expires > 0
                                               /\ OwnerReplyEnd(o) > grants[o].expires)]
                   /\ UNCHANGED <<vars, placement, recoveredMove>>
LoseOwnerReply(o) == /\ grants[o].pending = "reply"
                     /\ grants' = [grants EXCEPT ![o].pending = "none"]
                     /\ UNCHANGED <<vars, placement, recoveredMove>>
\* Freeze the old generation before any replacement can get a grant. The
\* same-root drain covers old requests, including replies still in flight.
\* Grants for other partitions keep being issued without provider mutations.
StartMove(p, d) ==
    /\ RootValid(p) /\ authority.owner = p
    /\ placement[d].phase = "open" /\ placement[d].generation < Generations
    /\ placement' = [placement EXCEPT ![d] =
         [generation |-> @.generation + 1, phase |-> "frozen", root |-> p, drain |-> now]]
    /\ authority' = [authority EXCEPT !.revision = @ + 1]
    /\ UNCHANGED <<now, roots, children, grants, recoveredMove>>
\* A later root has already drained all grants from the previous root. It
\* need not interpret a predecessor's local clock origin to finish a move.
FinishMove(p, d) ==
    /\ RootValid(p) /\ authority.owner = p /\ placement[d].phase = "frozen"
    /\ (UnsafeMoveDrain \/ p > placement[d].root \/ now >= placement[d].drain + Lease)
    /\ placement' = [placement EXCEPT ![d].phase = "open"]
    /\ recoveredMove' = (recoveredMove \/ (p > placement[d].root))
    /\ authority' = [authority EXCEPT !.revision = @ + 1]
    /\ UNCHANGED <<now, roots, children, grants>>
PartitionNext == ControlStep
    \/ (\E o \in Owners: RequestOwner(o) \/ IssueOwner(o) \/ DeliverOwner(o) \/ LoseOwnerReply(o))
    \/ (\E p \in Ids, d \in Partitions: StartMove(p, d) \/ FinishMove(p, d))
PartitionSpec == Init /\ OwnerInit /\ [][PartitionNext]_allVars
OwnerWithinRoot == \A o \in Owners: OwnerValid(o) => RootValid(o[1])
PartitionExclusive == \A a, b \in Owners:
    (a[2] = b[2] /\ OwnerValid(a) /\ OwnerValid(b)) => a[3] = b[3]
OwnerTypeOK ==
    /\ recoveredMove \in BOOLEAN
    /\ placement \in [Partitions ->
         [generation : 1..Generations, phase : {"open", "frozen"}, root : 0..Epochs, drain : Nat]]
    /\ grants \in [Owners ->
         [pending : {"none", "request", "reply"}, requested : Nat, duration : Nat,
          expires : Nat, requests : Nat, extended : BOOLEAN]]
PartitionInvariant == Strong /\ RootExclusive /\ ChildWithinRoot /\ ChildExclusive
                      /\ OwnerTypeOK /\ OwnerWithinRoot /\ PartitionExclusive
\* Reach a replacement and an unmoved partition with simultaneous permission
\* under the same original root. This is coverage, not an availability theorem.
NotIndependentMove == ~\E a, b \in Owners:
    a[1] = 1 /\ b[1] = 1 /\ a[2] # b[2] /\ a[3] = 2 /\ b[3] = 1
    /\ placement[b[2]].generation = 1 /\ OwnerValid(a) /\ OwnerValid(b)
NotRecoveredMove == ~recoveredMove
NotRenewedOwnerMove == ~\E p \in Ids, d \in Partitions:
    grants[<<p, d, 1>>].extended /\ OwnerValid(<<p, d, 2>>)
NotRepeatedMove == ~\E o \in Owners: o[3] = 3 /\ OwnerValid(o)
PartitionSmall == Small /\ \A o \in Owners: grants[o].requests <= MaxOwnerRequests
=============================================================================
