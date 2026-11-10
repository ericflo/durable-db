use super::{Error, Identity, Profile};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

const FORMAT: &str = "durable-db-format";
const PREFIX: &str = "durable-db-prefix";
const ROOT: &str = "durable-db-owned-root";
const VERSION: &str = "owned-candidate-1";
const PROBE: &str = "durable-db-probe";
const LEGACY_PROBE: &str = "durable-db-read-fence";

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Fenced,
    Open,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Record {
    pub identity: Identity,
    pub phase: Phase,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    epoch: u64,
    incarnation: String,
    phase: Phase,
    lifetime_ns: u64,
    error_ns: u64,
}

pub(super) fn decode(
    info: &BTreeMap<String, String>,
    prefix: &str,
    profile: Profile,
) -> Result<Option<Record>, Error> {
    if !info.contains_key(FORMAT) {
        if info
            .keys()
            .any(|k| k.starts_with("durable-db-") && k != PROBE && k != LEGACY_PROBE)
        {
            return Err(Error::Incompatible);
        }
        return Ok(None);
    }
    if info.get(FORMAT).map(String::as_str) != Some(VERSION)
        || info.get(PREFIX).map(String::as_str) != Some(prefix)
        || info.keys().any(|k| {
            k.starts_with("durable-db-")
                && ![FORMAT, PREFIX, ROOT, PROBE, LEGACY_PROBE].contains(&k.as_str())
        })
    {
        return Err(Error::Incompatible);
    }
    let encoded = info
        .get(ROOT)
        .filter(|v| v.len() <= 1024)
        .ok_or(Error::Incompatible)?;
    let wire: Wire = serde_json::from_str(encoded).map_err(|_| Error::Incompatible)?;
    let incarnation = Uuid::parse_str(&wire.incarnation).map_err(|_| Error::Incompatible)?;
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
    }))
}

pub(super) fn encode(
    observed: &BTreeMap<String, String>,
    prefix: &str,
    profile: Profile,
    record: Record,
) -> Result<BTreeMap<String, String>, Error> {
    // Validate before replacing anything, including stale observations and
    // namespaces belonging to the existing publication engine.
    decode(observed, prefix, profile)?;
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
        })
        .map_err(|_| Error::Incompatible)?,
    );
    // Use the B2 subset, reserving a field for the common mutation identity.
    // Session additionally bounds total bytes before invoking the provider.
    if info.len() + usize::from(!info.contains_key(PROBE)) > 10 {
        return Err(Error::Incompatible);
    }
    Ok(info)
}
