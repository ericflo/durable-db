use super::*;
use crate::transactions::record::Mutation;
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) fn record(
    db: Database<'_>,
    revision: u64,
    request: &str,
    mutations: &[Mutation<'_>],
) -> Bytes {
    Commit::encode(
        db,
        revision,
        revision - 1,
        revision.saturating_sub(128),
        request,
        [revision as u8; 32],
        mutations,
    )
    .unwrap()
}

/// A test projection, not the proposed production value index or an RSS bound.
#[derive(Default)]
pub(crate) struct Memory {
    pub states: BTreeMap<(String, Uuid), State>,
    pub values: BTreeMap<(String, Uuid, String), Vec<u8>>,
    pub calls: usize,
    pub fail_at: Option<usize>,
    pub dropped: Arc<AtomicBool>,
}
impl Drop for Memory {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl Target for Memory {
    async fn take_state(&mut self, database: Database<'_>) -> anyhow::Result<State> {
        Ok(self
            .states
            .remove(&(database.name.into(), database.generation))
            .unwrap_or(State::empty(database)?))
    }
    async fn apply(&mut self, commit: &Commit<'_>, state: State) -> anyhow::Result<()> {
        let db = commit.database();
        self.calls += 1;
        for mutation in commit.mutations() {
            let key = (db.name.into(), db.generation, mutation.key.into());
            if let Some(value) = mutation.value {
                self.values.insert(key, value.to_vec());
            } else {
                self.values.remove(&key);
            }
            // Intentionally fail after changing values, before metadata. The
            // runner must drop the private target, not return this mixed state.
            ensure!(
                self.fail_at != Some(self.calls),
                "injected projection error"
            );
        }
        self.states.insert((db.name.into(), db.generation), state);
        Ok(())
    }
}

#[test]
fn receipt_window_never_treats_an_expired_retry_as_proven_absent() {
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(8),
    };
    let mut state = State::empty(db).unwrap();
    for revision in 1..=130 {
        let bytes = record(db, revision, &format!("r-{revision}"), &[]);
        state = state.advance(&Commit::decode(&bytes).unwrap()).unwrap();
        assert_eq!(state.receipts.len(), (revision as usize).min(128));
    }
    assert_eq!(state.revision(), 130);
    assert_eq!(
        state.resolve("r-1", [1; 32], 0).unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        state.resolve("r-3", [3; 32], 0).unwrap(),
        Resolution::Committed(3)
    );
    assert_eq!(
        state.resolve("r-3", [4; 32], 2).unwrap(),
        Resolution::Reused
    );
    assert_eq!(
        state.resolve("unused", [0; 32], 1).unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        state.resolve("unused", [0; 32], 2).unwrap(),
        Resolution::Missing
    );
    assert!(state.resolve("unused", [0; 32], 131).is_err());
    let stale = Commit::encode(db, 131, 1, 3, "unused", [0; 32], &[]).unwrap();
    assert!(state.advance(&Commit::decode(&stale).unwrap()).is_err());
}

#[test]
fn retention_is_recorded_can_exceed_128_and_cannot_resurrect_forgotten_receipts() {
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(8),
    };
    let mut state = State::empty(db).unwrap();
    for revision in 1..=256 {
        let bytes =
            Commit::encode(db, revision, 0, 0, &format!("r-{revision}"), [0; 32], &[]).unwrap();
        state = state.advance(&Commit::decode(&bytes).unwrap()).unwrap();
    }
    assert_eq!(state.receipts.len(), 256);
    assert_eq!(state.by_request.len(), 256);
    assert_eq!(
        state.resolve("r-1", [0; 32], 0).unwrap(),
        Resolution::Committed(1)
    );
    for (revision, floor) in [(257, 200), (258, 200)] {
        let bytes = Commit::encode(
            db,
            revision,
            revision - 1,
            floor,
            &format!("r-{revision}"),
            [0; 32],
            &[],
        )
        .unwrap();
        state = state.advance(&Commit::decode(&bytes).unwrap()).unwrap();
    }
    assert_eq!(state.receipts.len(), 58);
    assert_eq!(state.by_request.len(), 58);
    assert_eq!(
        state.resolve("r-200", [0; 32], 199).unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        state.resolve("r-201", [0; 32], 0).unwrap(),
        Resolution::Committed(201)
    );
    let regress = Commit::encode(db, 259, 258, 199, "r-259", [0; 32], &[]).unwrap();
    assert!(state.advance(&Commit::decode(&regress).unwrap()).is_err());
}

#[test]
fn replay_rejects_gaps_duplicates_and_wrong_database_generation() {
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(8),
    };
    let first = record(db, 1, "request", &[]);
    for bad in [
        record(db, 3, "other", &[]),
        record(db, 1, "other", &[]),
        record(db, 2, "request", &[]),
        record(
            Database {
                generation: Uuid::from_u128(9),
                ..db
            },
            2,
            "other",
            &[],
        ),
        record(
            Database {
                name: "another",
                ..db
            },
            2,
            "other",
            &[],
        ),
    ] {
        let state = State::empty(db)
            .unwrap()
            .advance(&Commit::decode(&first).unwrap())
            .unwrap();
        assert!(state.advance(&Commit::decode(&bad).unwrap()).is_err());
    }
    let state = State::empty(db)
        .unwrap()
        .advance(&Commit::decode(&first).unwrap())
        .unwrap();
    // Same logical name, fresh generation: no inherited receipt or revision.
    let recreated = State::empty(Database {
        generation: Uuid::from_u128(9),
        ..db
    })
    .unwrap();
    assert_eq!(
        state.resolve("request", [1; 32], 0).unwrap(),
        Resolution::Committed(1)
    );
    assert_eq!(
        recreated.resolve("request", [1; 32], 0).unwrap(),
        Resolution::Missing
    );
}
