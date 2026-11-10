//! Lifecycle rows use the existing journal and checkpoint index. The catalog's
//! fixed internal generation is never returned by a public database lookup.
use super::*;
use crate::{
    Error, Result,
    auth::Access,
    engine::{
        self, CommitRequest, CommitResult, CreateRequest, CreateResult, PolicyInfo, PolicyRequest,
        PolicyResult, ReadRequest, ReadResult,
        access::{self, Policy},
        creation::{self, CreationReceipt},
    },
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

const CATALOG: Database<'static> = Database {
    name: "durable-catalog",
    generation: Uuid::from_u128(1),
};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    format: u8,
    policy: Policy,
    creation: CreationReceipt,
}
impl Entry {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.format == 1, "unsupported native catalog");
        self.policy.validate()?;
        self.creation.validate(&self.policy)?;
        ensure!(
            !self.generation()?.is_nil() && self.generation()? != CATALOG.generation,
            "reserved catalog generation"
        );
        Ok(())
    }
    fn generation(&self) -> anyhow::Result<Uuid> {
        Ok(Uuid::parse_str(&self.policy.info().incarnation)?)
    }
    fn authorize(&self, db: &str, caller: &Access, write: bool) -> Result<()> {
        access::authorize(Some(&self.policy), db, caller, write)
    }
}
#[derive(Clone)]
pub enum CatalogResult {
    Created(CreateResult),
    Policy(PolicyResult),
}
impl CatalogResult {
    fn replay(&self) -> Self {
        let mut result = self.clone();
        match &mut result {
            Self::Created(r) => r.replayed = true,
            Self::Policy(r) => r.replayed = true,
        }
        result
    }
}
pub struct CatalogChange {
    pub upload: Option<journal::PreparedUpload>,
    ticket: Ticket,
    result: CatalogResult,
}
pub(super) struct PendingChange {
    intent: [u8; 32],
    pub ticket: Ticket,
    result: CatalogResult,
}
pub struct AuthorizedTicket {
    ticket: Ticket,
    name: Arc<str>,
    generation: Uuid,
}
pub struct AuthorizedBatch {
    pub upload: Option<journal::PreparedUpload>,
    pub tickets: Vec<AuthorizedTicket>,
}
fn administrator(caller: &Access) -> Result<()> {
    if matches!(caller, Access::Administrator) {
        Ok(())
    } else {
        Err(Error::Unauthorized)
    }
}
fn intent(kind: &[u8], db: &str, digest: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(kind);
    hash.update((db.len() as u64).to_be_bytes());
    hash.update(db.as_bytes());
    hash.update(digest.as_bytes());
    hash.finalize().into()
}
impl<C: Clock> Owner<C> {
    async fn catalog_entry(&self, db: &str) -> Result<Option<Entry>> {
        engine::validate_db(db)?;
        self.check()?;
        let Some(view) = self.projection.view(CATALOG).await? else {
            return Ok(None);
        };
        let Some(value) = view
            .get(db.as_bytes(), view.revision())
            .await?
            .and_then(|r| r.value)
        else {
            return Ok(None);
        };
        let entry: Entry = serde_json::from_slice(&value).context("invalid native catalog row")?;
        entry.validate()?;
        self.check()?;
        Ok(Some(entry))
    }
    fn pending_change(&self, db: &str, intent: [u8; 32]) -> Result<Option<CatalogChange>> {
        self.check()?;
        let Some(pending) = self.catalog_pending.get(db) else {
            return Ok(None);
        };
        if pending.intent != intent {
            return Err(Error::Busy);
        }
        Ok(Some(CatalogChange {
            upload: None,
            ticket: pending.ticket,
            result: pending.result.replay(),
        }))
    }
    fn catalog_reply(&self, result: CatalogResult) -> Result<CatalogChange> {
        self.check()?;
        Ok(CatalogChange {
            upload: None,
            ticket: Ticket {
                owner: self.identity,
                position: 0,
                result: Outcome::Committed(0),
                replayed: true,
            },
            result,
        })
    }
    async fn prepare_catalog(
        &mut self,
        db: &str,
        entry: Entry,
        intent: [u8; 32],
        result: CatalogResult,
    ) -> Result<CatalogChange> {
        self.maintain().await?;
        if self.checkpoint_due() {
            return Err(Error::Busy);
        }
        entry.validate()?;
        let value = Bytes::from(serde_json::to_vec(&entry).context("encode native catalog row")?);
        let snapshot = self.snapshot(CATALOG).await?;
        let prepared = self
            .prepare_hashed(
                &[Request {
                    snapshot: &snapshot,
                    id: &hex::encode(intent),
                    reads: &[Read::Key(db)],
                    mutations: &[Mutation {
                        key: db,
                        value: Some(&value),
                    }],
                }],
                Some(&[intent]),
            )
            .await?;
        let ticket = prepared.tickets[0];
        if matches!(ticket.result, Outcome::Committed(_))
            && ticket.position > self.writer.durable_prefix().position()
        {
            self.catalog_pending.insert(
                Arc::from(db),
                PendingChange {
                    intent,
                    ticket,
                    result: result.clone(),
                },
            );
        }
        Ok(CatalogChange {
            upload: prepared.upload,
            ticket,
            result,
        })
    }
    /// Administrative, idempotent protected creation. Data admission remains
    /// impossible until this catalog record enters the contiguous durable view.
    pub async fn prepare_creation(
        &mut self,
        db: &str,
        request: &CreateRequest,
        caller: &Access,
    ) -> Result<CatalogChange> {
        administrator(caller)?;
        engine::validate_db(db)?;
        request.validate()?;
        let identity = intent(
            b"durable-db/catalog-create/1",
            db,
            &creation::digest(request)?,
        );
        if let Some(pending) = self.pending_change(db, identity)? {
            return Ok(pending);
        }
        if let Some(entry) = self.catalog_entry(db).await? {
            return self.catalog_reply(CatalogResult::Created(entry.creation.recover(request)?));
        }
        let policy = Policy::created();
        let creation = CreationReceipt::created(request, &policy)?;
        let mut result = creation.recover(request)?;
        result.replayed = false;
        self.prepare_catalog(
            db,
            Entry {
                format: 1,
                policy,
                creation,
            },
            identity,
            CatalogResult::Created(result),
        )
        .await
    }
    /// A pending policy change gates this database's subsequent write admission.
    /// It shares journal order with previously accepted writes and checkpoints.
    pub async fn prepare_policy(
        &mut self,
        db: &str,
        request: &PolicyRequest,
        caller: &Access,
    ) -> Result<CatalogChange> {
        administrator(caller)?;
        engine::validate_db(db)?;
        let digest = request.digest()?;
        let identity = intent(b"durable-db/catalog-policy/1", db, &digest);
        if let Some(pending) = self.pending_change(db, identity)? {
            return Ok(pending);
        }
        let mut entry = self.catalog_entry(db).await?.ok_or(Error::NotFound)?;
        let result = entry.policy.change(request, &digest)?;
        if result.replayed {
            return self.catalog_reply(CatalogResult::Policy(result));
        }
        self.prepare_catalog(db, entry, identity, CatalogResult::Policy(result))
            .await
    }
    pub fn catalog_result(&self, change: &CatalogChange) -> Result<Option<CatalogResult>> {
        Ok(self
            .commit_result(change.ticket)?
            .map(|_| change.result.clone()))
    }
    pub async fn info_authorized(&self, db: &str, caller: &Access) -> Result<engine::Info> {
        caller.check_scope(db, false)?;
        let entry = self.catalog_entry(db).await?.ok_or(Error::NotFound)?;
        entry.authorize(db, caller, false)?;
        let view = self
            .projection
            .view(Database {
                name: db,
                generation: entry.generation()?,
            })
            .await?;
        let usage = view
            .as_ref()
            .map_or(Default::default(), recovery::View::statistics);
        let info = engine::Info {
            revision: view.as_ref().map_or(0, recovery::View::revision),
            keys: usage.keys,
            bytes: usage.bytes,
            limits: entry.creation.limits().clone(),
            policy: Some(entry.policy.info()),
        };
        entry.authorize(db, caller, false)?;
        self.check()?;
        Ok(info)
    }
    pub async fn policy_info(&self, db: &str, caller: &Access) -> Result<PolicyInfo> {
        caller.check_scope(db, false)?;
        let entry = self.catalog_entry(db).await?.ok_or(Error::NotFound)?;
        entry.authorize(db, caller, false)?;
        self.check()?;
        Ok(entry.policy.info())
    }
    pub async fn read_authorized(
        &self,
        db: &str,
        request: &ReadRequest,
        caller: &Access,
    ) -> Result<ReadResult> {
        caller.check_scope(db, false)?;
        let entry = self.catalog_entry(db).await?.ok_or(Error::NotFound)?;
        entry.authorize(db, caller, false)?;
        let result = self
            .read_request(
                Database {
                    name: db,
                    generation: entry.generation()?,
                },
                request,
            )
            .await?;
        entry.authorize(db, caller, false)?;
        self.check()?;
        Ok(result)
    }
    pub async fn prepare_authorized(
        &mut self,
        requests: &[(&str, &CommitRequest, &Access)],
    ) -> Result<AuthorizedBatch> {
        if !(1..=128).contains(&requests.len()) {
            return Err(Error::Invalid("batch needs 1..128 transactions".into()));
        }
        let mut bindings = Vec::with_capacity(requests.len());
        let mut limits = Vec::with_capacity(requests.len());
        for (db, _, caller) in requests {
            caller.check_scope(db, true)?;
            let entry = self.catalog_entry(db).await?.ok_or(Error::NotFound)?;
            entry.authorize(db, caller, true)?;
            if self.catalog_pending.contains_key(*db) {
                return Err(Error::Busy);
            }
            limits.push(entry.creation.limits().clone());
            bindings.push(Database {
                name: db,
                generation: entry.generation()?,
            });
        }
        let inputs: Vec<_> = bindings
            .iter()
            .copied()
            .zip(requests.iter().map(|(_, r, _)| *r))
            .collect();
        let prepared = self.prepare_limited(&inputs, Some(&limits)).await?;
        let tickets = prepared
            .tickets
            .into_iter()
            .zip(bindings)
            .map(|(ticket, db)| AuthorizedTicket {
                ticket,
                name: db.name.into(),
                generation: db.generation,
            })
            .collect();
        Ok(AuthorizedBatch {
            upload: prepared.upload,
            tickets,
        })
    }
    pub async fn authorized_result(
        &self,
        ticket: &AuthorizedTicket,
        caller: &Access,
    ) -> Result<Option<CommitResult>> {
        caller.check_scope(&ticket.name, true)?;
        let entry = self
            .catalog_entry(&ticket.name)
            .await?
            .ok_or(Error::NotFound)?;
        entry.authorize(&ticket.name, caller, true)?;
        if entry.generation()? != ticket.generation {
            return Err(Error::Unauthorized);
        }
        self.commit_result(ticket.ticket)
    }
}
