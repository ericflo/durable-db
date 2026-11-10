--------------------------- MODULE GrantBounds ---------------------------
EXTENDS Integers
\* Research timing contract: over a real interval represented by t reference
\* units, an elapsed clock reading lies between t-error and 2*t+error.
\* Readings are nondecreasing. The bound includes sampling/quantization error;
\* it is an assumption about the platform, not something this module proves.
DrainWait(lease, error) == 2 * lease + 3 * error
ChildDuration(remaining, error) ==
    IF remaining > 3 * error THEN (remaining - 3 * error) \div 2 ELSE 0
\* If each accepted timestamp is within maxError of a continuous reference
\* clock, an elapsed measurement includes the errors at BOTH endpoints.
\* maxError includes calibration, counter extrapolation and sampling/rounding.
SampleElapsedError(maxError) == 2 * maxError
=============================================================================
