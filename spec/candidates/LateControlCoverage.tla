----------------------- MODULE LateControlCoverage -----------------------
EXTENDS PartitionJournal
\* Ghost flags witness actual late applications, including acknowledged data.
\* They do not constrain the protocol. Every original safety predicate remains
\* enabled; each forbidden-state check must reach its specific event.
VARIABLES lateFreeze, lateMove, lateSelection, lateOwnerSelection
traceVars == <<combinedVars, lateFreeze, lateMove, lateSelection, lateOwnerSelection>>
LateFreeze == \E p \in Ids, d \in Partitions:
    ~RootValid(p) /\ acknowledged[<<d, 1>>] > 0 /\ StartMove(p, d)
LateMove == \E p \in Ids, d \in Partitions:
    ~RootValid(p) /\ acknowledged[<<d, 1>>] > 0 /\ FinishMove(p, d)
LateSelect == \E j \in Journals, p \in Ids:
    ~RootValid(p) /\ Select(j, p)
    /\ acknowledged[Older(j, scans[j].generation)] > 0
LateOwnerSelect == \E j \in Journals, p \in Ids:
    RootValid(p) /\ ~JournalValid(j) /\ Select(j, p)
    /\ acknowledged[Older(j, scans[j].generation)] > 0
TraceNext == /\ HistoryNext
             /\ lateFreeze' = (lateFreeze \/ LateFreeze)
             /\ lateMove' = (lateMove \/ LateMove)
             /\ lateSelection' = (lateSelection \/ LateSelect)
             /\ lateOwnerSelection' = (lateOwnerSelection \/ LateOwnerSelect)
TraceInit == /\ Init /\ OwnerInit /\ HistoryInit
             /\ lateFreeze = FALSE /\ lateMove = FALSE /\ lateSelection = FALSE
             /\ lateOwnerSelection = FALSE
TraceSpec == TraceInit /\ [][TraceNext]_traceVars
NotLateFreeze == ~lateFreeze
NotLateMove == ~lateMove
NotLateSelection == ~lateSelection
NotLateOwnerSelection == ~lateOwnerSelection
=============================================================================
