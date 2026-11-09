use super::*;
use proptest::prelude::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

fn validator() -> Validator {
    Validator::new(0, Limits::default()).unwrap()
}

#[test]
fn disjoint_writers_share_a_snapshot_but_pending_writes_still_conflict() {
    let mut v = validator();
    let snapshot = v.snapshot();
    assert_eq!(v.accept(snapshot, &[Read::Key("a")], &["a"]), Ok(1));
    assert_eq!(v.accept(snapshot, &[Read::Key("b")], &["b"]), Ok(2));
    assert_eq!(v.snapshot(), snapshot);
    assert_eq!(
        v.accept(snapshot, &[Read::Key("a")], &["c"]),
        Err(Error::Conflict)
    );
    v.publish_through(2).unwrap();
    assert_eq!(v.snapshot().revision(), 2);
}

#[test]
fn write_skew_is_rejected_before_either_transaction_is_durable() {
    let mut v = validator();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[Read::Key("doctor/b")], &["doctor/a"])
        .unwrap();
    assert_eq!(
        v.accept(snapshot, &[Read::Key("doctor/a")], &["doctor/b"]),
        Err(Error::Conflict)
    );
}

#[test]
fn an_empty_scan_detects_insertions_and_absence_checks_detect_deletions() {
    let mut v = validator();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &["members/2"]).unwrap();
    let range = Read::Range {
        start: Included("members/"),
        end: Excluded("members0"),
    };
    assert_eq!(
        v.accept(snapshot, &[range], &["summary"]),
        Err(Error::Conflict)
    );
    v.publish_through(1).unwrap();
    let before_delete = v.snapshot();
    // Both deletion and reinsertion are mutations even if the final value is
    // absent or equal to the original. The controller supplies the same key.
    v.accept(before_delete, &[], &["members/2"]).unwrap();
    assert_eq!(v.accept(before_delete, &[range], &[]), Err(Error::Conflict));
    assert_eq!(
        v.accept(snapshot, &[Read::Key("members/2")], &[]),
        Err(Error::Conflict)
    );
}

#[test]
fn range_boundaries_are_exact_and_empty_ranges_do_not_panic() {
    for (start, end, conflicts) in [
        (Included("b"), Included("b"), true),
        (Included("b"), Excluded("b"), false),
        (Excluded("b"), Included("b"), false),
        (Excluded("b"), Excluded("b"), false),
        (Unbounded, Excluded("b"), false),
        (Unbounded, Included("b"), true),
        (Excluded("b"), Unbounded, false),
        (Included("b"), Unbounded, true),
        (Unbounded, Unbounded, true),
        (Included("a"), Excluded("c"), true),
    ] {
        let mut v = validator();
        let snapshot = v.snapshot();
        v.accept(snapshot, &[], &["b"]).unwrap();
        let result = v.accept(snapshot, &[Read::Range { start, end }], &[]);
        assert_eq!(
            result,
            if conflicts {
                Err(Error::Conflict)
            } else {
                Ok(2)
            }
        );
    }
}

#[test]
fn whole_database_conditions_keep_their_strict_revision_meaning() {
    let mut v = validator();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &[]).unwrap();
    assert_eq!(
        v.accept(snapshot, &[Read::Database], &[]),
        Err(Error::Conflict)
    );
    assert_eq!(v.accept(snapshot, &[Read::Key("unmodified")], &[]), Ok(2));
}

#[test]
fn capacity_never_discards_pending_writes() {
    let mut v = Validator::new(
        0,
        Limits {
            transactions: 2,
            key_references: 2,
            key_bytes: 2,
        },
    )
    .unwrap();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &["a", "a"]).unwrap();
    v.accept(snapshot, &[], &["b"]).unwrap();
    assert_eq!(v.accept(snapshot, &[], &["c"]), Err(Error::Busy));
    assert_eq!((v.accepted, v.floor, v.keys, v.bytes), (2, 0, 2, 2));
    v.publish_through(1).unwrap();
    // Reclamation cannot make an old snapshot appear conflict-free.
    assert_eq!(v.accept(snapshot, &[], &["c"]), Err(Error::SnapshotExpired));
    assert_eq!((v.accepted, v.floor, v.keys, v.bytes), (2, 0, 2, 2));
    v.accept(v.snapshot(), &[], &["c"]).unwrap();
    assert_eq!((v.floor, v.keys, v.bytes), (1, 2, 2));
    assert_eq!(v.latest.get("b"), Some(&2));
}

#[test]
fn reclaiming_an_old_write_keeps_a_newer_version_of_the_same_key() {
    let mut v = Validator::new(
        0,
        Limits {
            transactions: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &["a"]).unwrap();
    v.accept(snapshot, &[], &["a"]).unwrap();
    v.publish_through(2).unwrap();
    v.accept(v.snapshot(), &[], &["b"]).unwrap();
    assert_eq!(v.floor, 1);
    assert_eq!(v.latest.get("a"), Some(&2));
}

#[test]
fn transaction_count_bounds_empty_writes_too() {
    let mut v = Validator::new(
        0,
        Limits {
            transactions: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &[]).unwrap();
    assert_eq!(v.accept(snapshot, &[], &[]), Err(Error::Busy));
}

#[test]
fn wide_validation_has_a_work_budget_and_fresh_snapshots_avoid_scanning() {
    let mut v = validator();
    let keys: Vec<String> = (0..=MAX_RANGE_STEPS).map(|n| format!("k{n:05}")).collect();
    for chunk in keys.chunks(MAX_TERMS) {
        let writes: Vec<&str> = chunk.iter().map(String::as_str).collect();
        v.accept(v.snapshot(), &[], &writes).unwrap();
    }
    v.publish_through(v.accepted).unwrap();
    let snapshot = v.snapshot();
    v.accept(snapshot, &[], &["z"]).unwrap();
    let read = Read::Range {
        start: Unbounded,
        end: Excluded("m"),
    };
    let before = v.accepted;
    assert_eq!(
        v.accept(snapshot, &[read], &["result"]),
        Err(Error::WorkLimit)
    );
    assert_eq!(v.accepted, before);
    v.publish_through(before).unwrap();
    assert_eq!(v.accept(v.snapshot(), &[read], &["result"]), Ok(before + 1));
}

#[test]
fn recovery_and_other_databases_cannot_reuse_snapshot_tokens() {
    let old = Validator::new(42, Limits::default()).unwrap().snapshot();
    let mut recovered = Validator::new(42, Limits::default()).unwrap();
    assert_eq!(
        recovered.accept(old, &[], &["a"]),
        Err(Error::SnapshotExpired)
    );
    assert_eq!(
        recovered.accept(validator().snapshot(), &[], &[]),
        Err(Error::SnapshotExpired)
    );
    assert_eq!(recovered.accept(recovered.snapshot(), &[], &["a"]), Ok(43));
}

#[test]
fn invalid_input_and_exhaustion_do_not_advance_the_validator() {
    let mut v = validator();
    let snapshot = v.snapshot();
    for (reads, writes, error) in [
        (vec![], vec![""], Error::Invalid),
        (
            vec![Read::Range {
                start: Included("z"),
                end: Included("a"),
            }],
            vec![],
            Error::Invalid,
        ),
        (vec![Read::Database; MAX_TERMS + 1], vec![], Error::TooLarge),
        (vec![], vec!["a"; MAX_TERMS + 1], Error::TooLarge),
    ] {
        assert_eq!(v.accept(snapshot, &reads, &writes), Err(error));
        assert_eq!((v.accepted, v.keys, v.bytes), (0, 0, 0));
    }
    assert_eq!(v.publish_through(1), Err(Error::Invalid));
    let mut last = Validator::new(u64::MAX, Limits::default()).unwrap();
    assert_eq!(
        last.accept(last.snapshot(), &[], &[]),
        Err(Error::RevisionExhausted)
    );
    assert_eq!(last.snapshot().revision(), u64::MAX);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]
    #[test]
    fn accepted_histories_match_a_serial_reference(
        actions in prop::collection::vec((0u8..6, 0usize..64, 0usize..3, 0u8..8), 1..100)
    ) {
        let keys = ["a", "b", "c"];
        let mut v = Validator::new(0, Limits { transactions: 4, key_references: 4, key_bytes: 4 }).unwrap();
        let mut history = vec![[0u64; 3]];
        let mut snapshots = vec![v.snapshot()];
        for (action, pick, key, mask) in actions {
            match action {
                0 => snapshots.push(v.snapshot()),
                1 => { v.publish_through(v.accepted).unwrap(); }
                _ => {
                    let snapshot = snapshots[pick % snapshots.len()];
                    let read = match action {
                        2 => Read::Key(keys[key]),
                        3 => Read::Range { start: Included(keys[key]), end: Unbounded },
                        4 => Read::Database,
                        _ => Read::Range { start: Unbounded, end: Excluded(keys[key]) },
                    };
                    let before = (v.accepted, v.floor, v.keys, v.bytes, v.latest.clone());
                    let writes: Vec<&str> = (0..3).filter(|k| mask & (1 << k) != 0).map(|k| keys[k]).collect();
                    let result = v.accept(snapshot, &[read], &writes);
                    if let Ok(revision) = result {
                        let observed = history[snapshot.revision() as usize];
                        let previous = *history.last().unwrap();
                        if matches!(read, Read::Database) {
                            prop_assert_eq!(snapshot.revision(), revision - 1);
                        }
                        for k in 0..3 {
                            let checked = match action {
                                2 => k == key,
                                3 => k >= key,
                                4 => true,
                                _ => k < key,
                            };
                            if checked { prop_assert_eq!(observed[k], previous[k]); }
                        }
                        let mut next = previous;
                        for (k, value) in next.iter_mut().enumerate() {
                            if mask & (1 << k) != 0 { *value = revision; }
                        }
                        history.push(next);
                    } else {
                        prop_assert_eq!((v.accepted, v.floor, v.keys, v.bytes, v.latest.clone()), before);
                    }
                    prop_assert!(v.changes.len() <= 4 && v.keys <= 4 && v.bytes <= 4);
                    prop_assert!(v.floor <= v.durable && v.durable <= v.accepted);
                }
            }
        }
    }
}
