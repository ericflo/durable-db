------------------------------ MODULE SortedTable ------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS UnsafeSeek, UnsafeCorrupt
Rows == 1..6
Key(r) == ((r-1) \div 3) + 1
Revision(r) == 3 - ((r-1) % 3)
Min(S) == CHOOSE r \in S: \A other \in S: r <= other
VARIABLES source, remaining, blocks, phase, key, snapshot, candidate, result
vars == <<source,remaining,blocks,phase,key,snapshot,candidate,result>>
Init == /\ source \in SUBSET Rows /\ source # {}
        /\ remaining = source /\ blocks = <<>> /\ phase = "build"
        /\ key = 0 /\ snapshot = 0 /\ candidate = 0 /\ result = -2
AppendRow(split) == /\ phase = "build" /\ remaining # {}
                    /\ LET r == Min(remaining) IN
                       /\ remaining' = remaining \ {r}
                       /\ blocks' = IF split \/ Len(blocks) = 0
                                     THEN Append(blocks, <<r>>)
                                     ELSE [blocks EXCEPT ![Len(blocks)] = Append(@,r)]
                    /\ UNCHANGED <<source,phase,key,snapshot,candidate,result>>
Query(k,s) == /\ phase = "build" /\ remaining = {}
              /\ phase' = "seek" /\ key' = k /\ snapshot' = s
              /\ UNCHANGED <<source,remaining,blocks,candidate,result>>
Eligible(r) == Key(r) = key /\ Revision(r) <= snapshot
Target == (key-1)*3 + 4 - snapshot
Seek == /\ phase = "seek"
        /\ LET choices == {i \in 1..Len(blocks):
                           IF UnsafeSeek THEN Key(blocks[i][Len(blocks[i])]) >= key
                           ELSE blocks[i][Len(blocks[i])] >= Target}
           IN candidate' = IF choices = {} THEN Len(blocks)+1 ELSE Min(choices)
        /\ phase' = "read"
        /\ UNCHANGED <<source,remaining,blocks,key,snapshot,result>>
(* Each row can represent either a value or a deletion marker. The index must
   return the latest visible ROW, including tombstones, for its merge caller.
   Authenticated metadata and checksum detection abstract the hash contract. *)
Read(corrupt) == /\ phase = "read"
                 /\ result' = IF candidate > Len(blocks) THEN 0
                              ELSE IF corrupt THEN IF UnsafeCorrupt THEN 0 ELSE -1
                              ELSE LET rows == {blocks[candidate][i]: i \in 1..Len(blocks[candidate])}
                                       visible == {r \in rows: Eligible(r)}
                                   IN IF visible = {} THEN 0 ELSE Min(visible)
                 /\ phase' = "done"
                 /\ UNCHANGED <<source,remaining,blocks,key,snapshot,candidate>>
Next == (\E split \in BOOLEAN: AppendRow(split))
        \/ (\E k \in 1..3,s \in 0..3: Query(k,s))
        \/ Seek \/ (\E corrupt \in BOOLEAN: Read(corrupt))
Spec == Init /\ [][Next]_vars
Correct == phase = "done" =>
           LET visible == {r \in source: Eligible(r)}
               expected == IF visible = {} THEN 0 ELSE Min(visible)
           IN result \in {-1,expected}
=============================================================================
