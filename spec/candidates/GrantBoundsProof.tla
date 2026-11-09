------------------------ MODULE GrantBoundsProof ------------------------
EXTENDS GrantBounds, TLAPS

\* A root grant measured from request start cannot remain valid beyond this
\* abstract elapsed-time bound, including a slow reading's additive error.
THEOREM RootExpirationBound ==
  ASSUME NEW lease \in Nat, NEW error \in Nat, NEW elapsed \in Nat,
         NEW reading \in Int, reading >= elapsed - error, reading < lease
  PROVE elapsed < lease + error
  BY SMT

\* The old request starts before the successor's fence/drain observation.
\* Its elapsed interval is at least the successor's, so the same lower bound
\* applies to the old reading even when the two clocks have different rates.
THEOREM DrainExcludes ==
  ASSUME NEW lease \in Nat, NEW error \in Nat, NEW elapsed \in Nat,
         NEW oldReading \in Int, NEW newReading \in Int,
         oldReading >= elapsed - error,
         newReading <= 2 * elapsed + error,
         newReading >= DrainWait(lease, error)
  PROVE oldReading >= lease
  BY SMT DEF DrainWait

THEOREM ChildDurationBound ==
  ASSUME NEW remaining \in Nat, NEW error \in Nat,
         NEW duration \in Nat, duration = ChildDuration(remaining, error), remaining >= 3 * error
  PROVE 2 * duration + 3 * error <= remaining
  BY SMT DEF ChildDuration

\* Delivery may be arbitrarily delayed. Expiry remains anchored to the
\* child's request, which precedes the parent's remaining-time observation.
THEOREM ChildWithinParent ==
  ASSUME NEW remaining \in Nat, NEW error \in Nat,
         NEW sinceRequest \in Nat, NEW sinceIssue \in Nat,
         NEW childReading \in Nat, NEW parentReading \in Int,
         sinceIssue <= sinceRequest,
         childReading >= sinceRequest - error,
         parentReading <= 2 * sinceIssue + error,
         childReading < ChildDuration(remaining, error)
  PROVE parentReading < remaining
<1>1. ChildDuration(remaining, error) \in Nat
  BY SMT DEF ChildDuration
<1>2. remaining >= 3 * error
  BY SMT, <1>1 DEF ChildDuration
<1>3. 2 * ChildDuration(remaining, error) + 3 * error <= remaining
  BY ChildDurationBound, <1>1, <1>2
<1> QED BY SMT, <1>1, <1>3
=============================================================================
