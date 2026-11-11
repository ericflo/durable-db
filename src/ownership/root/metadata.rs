use super::{Encoding, Error, Identity};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::SocketAddr};
use uuid::Uuid;

const FORMAT: &str = "durable-db-format";
const PREFIX: &str = "durable-db-prefix";
const ROOT: &str = "durable-db-owned-root";
const RETAINED: &str = "durable-db-retained-checkpoint";
pub(crate) const VERSION: &str = "owned-candidate-23";
const PROBE: &str = "durable-db-probe";
const LEGACY_PROBE: &str = "durable-db-read-fence";

// Exclude timed remote writers while preserving released exclusive local data.
pub(super) fn version(budgeted: bool, encoding: Encoding) -> &'static str {
    if budgeted {
        VERSION
    } else if encoding.local {
        "owned-candidate-15"
    } else {
        "owned-candidate-23-unbudgeted"
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Fenced,
    Open,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    pub collection: u64,
    pub identity: Identity,
    pub phase: Phase,
    pub address: Option<SocketAddr>,
    pub catalog: Option<Uuid>,
    pub checkpoint: Option<crate::index::recovery::checkpoint::Reference>,
    pub retained: Option<super::RetainedCheckpoint>,
}
impl Record {
    /// Backup-slot changes do not invalidate a staged recovery. Publication
    /// preserves the confirmed slot and still compares every recovery field,
    /// including the collection generation captured before staging.
    pub(crate) fn same_recovery(self, other: Self) -> bool {
        Self {
            retained: None,
            ..self
        } == Self {
            retained: None,
            ..other
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedWire {
    id: String,
    checkpoint: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    epoch: u64,
    incarnation: String,
    phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<SocketAddr>,
    lifetime_ns: u64,
    error_ns: u64,
    catalog: Option<String>,
    checkpoint: Option<String>,
}

pub(super) fn decode(
    info: &BTreeMap<String, String>,
    prefix: &str,
    encoding: Encoding,
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
    if info.get(FORMAT).map(String::as_str) != Some(version(budgeted, encoding))
        || info.get(PREFIX).map(String::as_str) != Some(prefix)
        || info.contains_key(crate::upload_budget::FIELD) != budgeted
        || info.keys().any(|k| {
            k.starts_with("durable-db-")
                && ![
                    FORMAT,
                    PREFIX,
                    ROOT,
                    RETAINED,
                    PROBE,
                    LEGACY_PROBE,
                    crate::upload_budget::FIELD,
                    crate::upload_budget::DATABASE_FIELD,
                    super::collection::FIELD,
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
        if !info.contains_key(ROOT) && !info.contains_key(RETAINED) {
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
        || wire.lifetime_ns != encoding.legacy_lifetime()
        || wire.error_ns != 0
    {
        return Err(Error::Incompatible);
    }
    if let Some(address) = wire.address {
        super::valid_address(address)?;
        if encoding.local {
            return Err(Error::Incompatible);
        }
    }
    Ok(Some(Record {
        collection: super::collection::generation(info).map_err(|_| Error::Incompatible)?,
        identity: Identity {
            epoch: wire.epoch,
            incarnation,
        },
        phase: wire.phase,
        address: wire.address,
        catalog,
        checkpoint: wire
            .checkpoint
            .map(|s| {
                crate::index::recovery::checkpoint::Reference::decode(&s)
                    .map_err(|_| Error::Incompatible)
            })
            .transpose()?,
        retained: info
            .get(RETAINED)
            .map(|encoded| {
                if encoded.len() > 1024 {
                    return Err(Error::Incompatible);
                }
                let wire: RetainedWire =
                    serde_json::from_str(encoded).map_err(|_| Error::Incompatible)?;
                let id = Uuid::parse_str(&wire.id).map_err(|_| Error::Incompatible)?;
                if id.is_nil() || id.to_string() != wire.id {
                    return Err(Error::Incompatible);
                }
                Ok(super::RetainedCheckpoint {
                    id,
                    reference: crate::index::recovery::checkpoint::Reference::decode(
                        &wire.checkpoint,
                    )
                    .map_err(|_| Error::Incompatible)?,
                })
            })
            .transpose()?,
    }))
}

pub(super) fn encode(
    observed: &BTreeMap<String, String>,
    prefix: &str,
    encoding: Encoding,
    record: Record,
    budgeted: bool,
) -> Result<BTreeMap<String, String>, Error> {
    // Validate before replacing anything, including stale observations and
    // namespaces belonging to the existing publication engine.
    decode(observed, prefix, encoding, budgeted)?;
    let mut info = observed.clone();
    info.insert(FORMAT.into(), version(budgeted, encoding).into());
    info.insert(PREFIX.into(), prefix.into());
    if let Some(retained) = record.retained {
        let encoded = serde_json::to_string(&RetainedWire {
            id: retained.id.to_string(),
            checkpoint: retained
                .reference
                .encode()
                .map_err(|_| Error::Incompatible)?,
        })
        .map_err(|_| Error::Incompatible)?;
        if encoded.len() > 1024 {
            return Err(Error::Incompatible);
        }
        info.insert(RETAINED.into(), encoded);
    } else {
        info.remove(RETAINED);
    }
    info.insert(
        ROOT.into(),
        serde_json::to_string(&Wire {
            epoch: record.identity.epoch,
            incarnation: record.identity.incarnation.to_string(),
            phase: record.phase,
            address: record.address,
            lifetime_ns: encoding.legacy_lifetime(),
            error_ns: 0,
            catalog: record.catalog.map(|id| id.to_string()),
            checkpoint: record
                .checkpoint
                .map(|r| r.encode())
                .transpose()
                .map_err(|_| Error::Incompatible)?,
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
        (RETAINED, if info.contains_key(RETAINED) { 1024 } else { 0 }),
        (
            super::collection::FIELD,
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
