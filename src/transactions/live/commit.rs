use super::*;
use crate::{
    Error, Result,
    engine::{CommitRequest, CommitResult, DecodedCommit},
};

enum Evaluation {
    Ready(Snapshot),
    Reply(Ticket),
}

impl<C: Clock> Owner<C> {
    /// Evaluate the existing wire contract. The caller must authorize this
    /// database generation. Uploads and acknowledgements use the same queue
    /// and tickets as native transaction admission.
    pub async fn prepare_request(
        &mut self,
        database: Database<'_>,
        request: &CommitRequest,
    ) -> Result<Prepared> {
        self.prepare_requests(&[(database, request)]).await
    }
    /// Evaluate bounded wire requests before accepting any of them, then group
    /// their new commits into one journal upload. Replies retain input order.
    pub async fn prepare_requests(
        &mut self,
        requests: &[(Database<'_>, &CommitRequest)],
    ) -> Result<Prepared> {
        if !(1..=128).contains(&requests.len()) {
            return Err(Error::Invalid("batch needs 1..128 transactions".into()));
        }
        let mut decoded = Vec::with_capacity(requests.len());
        let mut bytes = 0;
        for (_, request) in requests {
            let input = request.decode()?;
            bytes += input.bytes;
            if bytes > journal::MAX_BATCH_BYTES {
                return Err(Error::Limit("transaction batch exceeds 4 MiB".into()));
            }
            decoded.push(input);
        }
        let identities: Vec<_> = requests
            .iter()
            .zip(&decoded)
            .map(|((db, _), d)| wire_fingerprint(*db, d.digest))
            .collect();
        let mut seen = BTreeMap::new();
        for ((db, request), fingerprint) in requests.iter().zip(&identities) {
            if seen
                .insert(
                    (db.name, db.generation, request.request_id.as_str()),
                    fingerprint,
                )
                .is_some_and(|prior| prior != fingerprint)
            {
                return Err(Error::Invalid(
                    "batch reuses request_id for different contents".into(),
                ));
            }
        }
        self.check()?;
        self.maintain().await?;
        let blocked = self.checkpoint_due();
        let mut evaluations = Vec::with_capacity(requests.len());
        for (((database, request), decoded), fingerprint) in
            requests.iter().zip(&decoded).zip(&identities)
        {
            evaluations.push(
                self.evaluate_request(*database, request, decoded, *fingerprint)
                    .await?,
            );
        }
        let mutations: Vec<Vec<_>> = decoded
            .iter()
            .map(|d| {
                d.writes
                    .iter()
                    .map(|(key, value)| Mutation {
                        key,
                        value: value.as_deref(),
                    })
                    .collect()
            })
            .collect();
        let mut inputs = Vec::new();
        let mut fingerprints = Vec::new();
        for (i, evaluated) in evaluations.iter().enumerate() {
            if let Evaluation::Ready(snapshot) = evaluated {
                inputs.push(Request {
                    snapshot,
                    id: &requests[i].1.request_id,
                    reads: &[Read::Database],
                    mutations: &mutations[i],
                });
                fingerprints.push(identities[i]);
            }
        }
        if blocked && !inputs.is_empty() {
            return Err(Error::Busy);
        }
        let prepared = if inputs.is_empty() {
            Prepared {
                tickets: Vec::new(),
                upload: None,
            }
        } else {
            self.prepare_hashed(&inputs, Some(&fingerprints)).await?
        };
        let mut accepted = prepared.tickets.into_iter();
        let tickets = evaluations
            .into_iter()
            .map(|e| match e {
                Evaluation::Ready(_) => accepted.next().expect("one ticket per admitted request"),
                Evaluation::Reply(ticket) => ticket,
            })
            .collect();
        self.check()?;
        Ok(Prepared {
            tickets,
            upload: prepared.upload,
        })
    }
    async fn evaluate_request(
        &mut self,
        database: Database<'_>,
        request: &CommitRequest,
        decoded: &DecodedCommit,
        fingerprint: [u8; 32],
    ) -> Result<Evaluation> {
        self.warm(database).await?;
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
            return Ok(Evaluation::Reply(Ticket {
                replayed: true,
                ..pending.ticket
            }));
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
        Ok(Evaluation::Ready(snapshot))
    }
    fn request_answer(&self, result: Outcome, replayed: bool) -> Result<Evaluation> {
        self.check()?;
        Ok(Evaluation::Reply(Ticket {
            owner: self.identity,
            position: if matches!(result, Outcome::Committed(_)) {
                0
            } else {
                self.writer.planned_position()
            },
            result,
            replayed,
        }))
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

fn wire_fingerprint(database: Database<'_>, digest: [u8; 32]) -> [u8; 32] {
    // Receipt identity contains only durable request fields. It must not
    // depend on a validator token, owner epoch or checkpoint incarnation.
    let mut hash = Sha256::new();
    hash.update(b"durable-db/wire-commit/1");
    hash.update((database.name.len() as u64).to_be_bytes());
    hash.update(database.name.as_bytes());
    hash.update(database.generation.as_bytes());
    hash.update(digest);
    hash.finalize().into()
}
