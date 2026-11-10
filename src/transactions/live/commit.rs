use super::*;
use crate::{
    Error, Result,
    engine::{CommitRequest, CommitResult},
};

impl<C: Clock> Owner<C> {
    /// Evaluate the existing wire contract. The caller must authorize this
    /// database generation. Uploads and acknowledgements use the same queue
    /// and tickets as native transaction admission.
    pub async fn prepare_request(
        &mut self,
        database: Database<'_>,
        request: &CommitRequest,
    ) -> Result<Prepared> {
        let decoded = request.decode()?;
        self.check()?;
        self.maintain().await?;
        self.warm(database).await?;
        // Receipt identity contains only durable request fields. It must not
        // depend on a validator token, owner epoch or checkpoint incarnation.
        let mut hash = Sha256::new();
        hash.update(b"durable-db/wire-commit/1");
        hash.update((database.name.len() as u64).to_be_bytes());
        hash.update(database.name.as_bytes());
        hash.update(database.generation.as_bytes());
        hash.update(decoded.digest);
        let fingerprint = hash.finalize().into();
        let key = (
            Arc::from(database.name),
            database.generation,
            Arc::from(request.request_id.as_str()),
        );
        if let Some(pending) = self.pending.get(&key) {
            if pending.fingerprint != fingerprint {
                return self.request_answer(Outcome::Rejected(Rejection::IdentityReused), false);
            }
            self.check()?;
            return Ok(Prepared {
                tickets: vec![Ticket {
                    replayed: true,
                    ..pending.ticket
                }],
                upload: None,
            });
        }
        let view = self
            .projection
            .view(database)
            .await?
            .context("missing live view")?;
        match view
            .resolve(
                &request.request_id,
                fingerprint,
                request.expected_revision.min(view.revision()),
            )
            .await?
        {
            Resolution::Committed(revision) => {
                return self.request_answer(Outcome::Committed(revision), true);
            }
            Resolution::Reused => {
                return self.request_answer(Outcome::Rejected(Rejection::IdentityReused), false);
            }
            Resolution::Expired => {
                return self.request_answer(Outcome::Rejected(Rejection::ReceiptExpired), false);
            }
            Resolution::Missing => (),
        }
        let conflict = Outcome::Rejected(Rejection::Validation(validation::Error::Conflict));
        if view.revision() != request.expected_revision {
            return self.request_answer(conflict, false);
        }
        for (key, expected) in &decoded.checks {
            let actual = view
                .get(key.as_bytes(), view.revision())
                .await?
                .and_then(|r| r.value);
            if actual.as_deref() != expected.as_deref() {
                return self.request_answer(conflict, false);
            }
        }
        let name: Arc<str> = database.name.into();
        let snapshot = Snapshot {
            token: self.active[&(name.clone(), database.generation)]
                .validator
                .snapshot(),
            name,
            generation: database.generation,
        };
        let mutations: Vec<_> = decoded
            .writes
            .iter()
            .map(|(key, value)| Mutation {
                key,
                value: value.as_deref(),
            })
            .collect();
        Ok(self
            .prepare_hashed(
                &[Request {
                    snapshot: &snapshot,
                    id: &request.request_id,
                    reads: &[Read::Database],
                    mutations: &mutations,
                }],
                Some(&[fingerprint]),
            )
            .await?)
    }
    fn request_answer(&self, result: Outcome, replayed: bool) -> Result<Prepared> {
        self.check()?;
        Ok(Prepared {
            tickets: vec![Ticket {
                owner: self.identity,
                position: if matches!(result, Outcome::Committed(_)) {
                    0
                } else {
                    self.writer.planned_position()
                },
                result,
                replayed,
            }],
            upload: None,
        })
    }
    /// None means the caller must wait for contiguous journal durability.
    /// Every returned result rechecks ownership, including replayed receipts.
    pub fn commit_result(&self, ticket: Ticket) -> Result<Option<CommitResult>> {
        match self.outcome(ticket)? {
            Outcome::Pending => Ok(None),
            Outcome::Committed(revision) => Ok(Some(CommitResult {
                revision,
                replayed: ticket.replayed,
            })),
            Outcome::Rejected(reason) => Err(match reason {
                Rejection::IdentityReused => {
                    Error::Invalid("request_id was used for a different request".into())
                }
                Rejection::ReceiptExpired => Error::ReceiptExpired,
                Rejection::Capacity => Error::Busy,
                Rejection::Validation(error) => match error {
                    validation::Error::Conflict | validation::Error::SnapshotExpired => {
                        Error::Conflict
                    }
                    validation::Error::Invalid => Error::Invalid(error.to_string()),
                    validation::Error::TooLarge
                    | validation::Error::WorkLimit
                    | validation::Error::RevisionExhausted => Error::Limit(error.to_string()),
                    validation::Error::Busy => Error::Busy,
                },
            }),
        }
    }
}
