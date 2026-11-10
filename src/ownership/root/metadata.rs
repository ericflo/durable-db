use super::{Error, Identity, Profile};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

const FORMAT: &str = "durable-db-format";
const PREFIX: &str = "durable-db-prefix";
const ROOT: &str = "durable-db-owned-root";
pub(crate) const VERSION: &str = "owned-candidate-8";
const PROBE: &str = "durable-db-probe";
const LEGACY_PROBE: &str = "durable-db-read-fence";

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Fenced,
    Open,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    pub identity: Identity,
    pub phase: Phase,
    pub catalog: Option<Uuid>,
    pub checkpoint: Option<crate::index::recovery::checkpoint::Reference>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    epoch: u64,
    incarnation: String,
    phase: Phase,
    lifetime_ns: u64,
    error_ns: u64,
    catalog: Option<String>,
    checkpoint: Option<String>,
}

pub(super) fn decode(
    info: &BTreeMap<String, String>,
    prefix: &str,
    profile: Profile,
    budgeted: bool,
) -> Result<Option<Record>, Error> {
    if !info.contains_key(FORMAT) {
        if budgeted
            || info
                .keys()
                .any(|k| k.starts_with("durable-db-") && k != PROBE && k != LEGACY_PROBE)
        {
            return Err(Error::Incompatible);
        }
        return Ok(None);
    }
    if info.get(FORMAT).map(String::as_str) != Some(VERSION)
        || info.get(PREFIX).map(String::as_str) != Some(prefix)
        || info.contains_key(crate::upload_budget::FIELD) != budgeted
        || info.keys().any(|k| {
            k.starts_with("durable-db-")
                && ![
                    FORMAT,
                    PREFIX,
                    ROOT,
                    PROBE,
                    LEGACY_PROBE,
                    crate::upload_budget::FIELD,
                    crate::upload_budget::DATABASE_FIELD,
                    crate::upload_budget::COLLECTION_FIELD,
                ]
                .contains(&k.as_str())
        })
    {
        return Err(Error::Incompatible);
    }
    if budgeted {
        crate::upload_budget::validate_info(info).map_err(|_| Error::Incompatible)?;
        // Budget bootstrap must precede the first registration upload. No
        // ownership identity or permission exists until the ordinary claim.
        if !info.contains_key(ROOT) {
            return Ok(None);
        }
    }
    let encoded = info
        .get(ROOT)
        .filter(|v| v.len() <= 1024)
        .ok_or(Error::Incompatible)?;
    let wire: Wire = serde_json::from_str(encoded).map_err(|_| Error::Incompatible)?;
    let incarnation = Uuid::parse_str(&wire.incarnation).map_err(|_| Error::Incompatible)?;
    let catalog = wire
        .catalog
        .map(|s| {
            let id = Uuid::parse_str(&s).map_err(|_| Error::Incompatible)?;
            if id.is_nil() || id.to_string() != s {
                return Err(Error::Incompatible);
            }
            Ok(id)
        })
        .transpose()?;
    if wire.epoch == 0
        || incarnation.is_nil()
        || incarnation.to_string() != wire.incarnation
        || u128::from(wire.lifetime_ns) != profile.bounds.root_lifetime().as_nanos()
        || u128::from(wire.error_ns) != profile.error.as_nanos()
    {
        return Err(Error::Incompatible);
    }
    Ok(Some(Record {
        identity: Identity {
            epoch: wire.epoch,
            incarnation,
        },
        phase: wire.phase,
        catalog,
        checkpoint: wire
            .checkpoint
            .map(|s| {
                crate::index::recovery::checkpoint::Reference::decode(&s)
                    .map_err(|_| Error::Incompatible)
            })
            .transpose()?,
    }))
}

pub(super) fn encode(
    observed: &BTreeMap<String, String>,
    prefix: &str,
    profile: Profile,
    record: Record,
    budgeted: bool,
) -> Result<BTreeMap<String, String>, Error> {
    // Validate before replacing anything, including stale observations and
    // namespaces belonging to the existing publication engine.
    decode(observed, prefix, profile, budgeted)?;
    let mut info = observed.clone();
    info.insert(FORMAT.into(), VERSION.into());
    info.insert(PREFIX.into(), prefix.into());
    info.insert(
        ROOT.into(),
        serde_json::to_string(&Wire {
            epoch: record.identity.epoch,
            incarnation: record.identity.incarnation.to_string(),
            phase: record.phase,
            lifetime_ns: profile.bounds.root_lifetime().as_nanos() as u64,
            error_ns: profile.error.as_nanos() as u64,
            catalog: record.catalog.map(|id| id.to_string()),
            checkpoint: record.checkpoint.map(|r| r.encode()),
        })
        .map_err(|_| Error::Incompatible)?,
    );
    // Use the B2 subset, reserving a field for the common mutation identity.
    // Session additionally bounds total bytes before invoking the provider.
    budget(&info).map_err(|_| Error::Incompatible)?;
    Ok(info)
}

/// Reserve space for the owner, both provider probes and a full budget receipt
/// window, so initialization/refill cannot make subsequent control work fail.
pub(crate) fn budget(info: &BTreeMap<String, String>) -> anyhow::Result<()> {
    let mut reserved = info.clone();
    for (field, bytes) in [
        (ROOT, 1024),
        (
            crate::upload_budget::COLLECTION_FIELD,
            if info.contains_key(crate::upload_budget::FIELD) {
                1024
            } else {
                0
            },
        ),
        (
            crate::upload_budget::DATABASE_FIELD,
            if info.contains_key(crate::upload_budget::FIELD) {
                1024
            } else {
                0
            },
        ),
        (PROBE, 36),
        (
            LEGACY_PROBE,
            if info.contains_key(crate::upload_budget::FIELD) {
                36
            } else {
                0
            },
        ),
        (
            crate::upload_budget::FIELD,
            if info.contains_key(crate::upload_budget::FIELD) {
                2304
            } else {
                0
            },
        ),
    ] {
        if bytes > 0 && reserved.get(field).is_none_or(|v| v.len() < bytes) {
            reserved.insert(field.into(), "0".repeat(bytes));
        }
    }
    anyhow::ensure!(
        reserved.len() <= 10 && serde_json::to_vec(&reserved)?.len() <= 8192 - 128,
        "native authority metadata budget exceeded"
    );
    Ok(())
}
