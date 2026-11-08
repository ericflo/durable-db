--------------------------- MODULE ReadBarrier ---------------------------
EXTENDS Naturals, Sequences, TLC
CONSTANTS Nodes, Values, MaxRevision, UnsafeRead
VARIABLES history, phase, seen, result, at
vars == <<history, phase, seen, result, at>>
Revision == Len(history)-1
Init == /\ history = <<0>> /\ phase = [n \in Nodes |-> "idle"]
        /\ seen = [n \in Nodes |-> 0] /\ result = [n \in Nodes |-> 0]
        /\ at = [n \in Nodes |-> 0]
Write(v) == /\ Revision < MaxRevision
            /\ history' = Append(history,v)
            /\ UNCHANGED <<phase,seen,result,at>>
Observe(n,r) == /\ phase[n] = "idle" /\ r \in 0..Revision
               /\ phase' = [phase EXCEPT ![n] = "read"]
               /\ seen' = [seen EXCEPT ![n] = r]
               /\ UNCHANGED <<history,result,at>>
Validate(n) == /\ phase[n] = "read" /\ Revision < MaxRevision
               /\ (UnsafeRead \/ seen[n] = Revision)
               /\ result' = [result EXCEPT ![n] = history[seen[n]+1]]
               /\ at' = [at EXCEPT ![n] = Revision]
               /\ phase' = [phase EXCEPT ![n] = "returned"]
               /\ history' = Append(history,history[Len(history)])
               /\ UNCHANGED seen
Restart(n) == /\ phase' = [phase EXCEPT ![n] = "idle"]
              /\ UNCHANGED <<history,seen,result,at>>
Next == (\E v \in Values: Write(v)) \/ (\E n \in Nodes:
          (\E r \in 0..Revision: Observe(n,r)) \/ Validate(n) \/ Restart(n))
ReadLinearizesAtValidation == \A n \in Nodes: phase[n] = "returned" => result[n] = history[at[n]+1]
Spec == Init /\ [][Next]_vars
=============================================================================
