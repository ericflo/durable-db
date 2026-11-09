//! One metadata wire encoding for B2 and S3. B2 permits ten bucketInfo entries.
use super::HEAD;
use anyhow::{Context, ensure};
use std::collections::BTreeMap;

const FORMAT: &str = "durable-db-format";
const CONTROL: &str = "durable-db-control";
const LEGACY_PROBES: [&str; 2] = ["durable-db-probe", "durable-db-read-fence"];

pub(crate) fn encode(info: &BTreeMap<String, String>) -> anyhow::Result<BTreeMap<String, String>> {
    if super::budgeted_format(info).is_none() {
        return Ok(info.clone());
    }
    ensure!(!info.contains_key(CONTROL), "nested control envelope");
    let mut wire = BTreeMap::new();
    let mut control = BTreeMap::new();
    for (key, value) in info {
        if key.starts_with("durable-db-") && key != HEAD && key != FORMAT {
            control.insert(key.clone(), value.clone());
        } else {
            wire.insert(key.clone(), value.clone());
        }
    }
    // Keep the incompatible root envelope and format marker visible to old
    // binaries. They must fail closed before selecting or publishing data.
    ensure!(wire.contains_key(HEAD), "missing budgeted root envelope");
    wire.insert(CONTROL.into(), serde_json::to_string(&control)?);
    Ok(wire)
}

pub(crate) fn decode(
    mut wire: BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(encoded) = wire.remove(CONTROL) else {
        // Legacy format 3 remains byte-compatible. No released format-4
        // namespace exists without the packed control document.
        ensure!(
            super::budgeted_format(&wire).is_none(),
            "missing control envelope"
        );
        return Ok(wire);
    };
    ensure!(
        super::budgeted_format(&wire).is_some() && wire.contains_key(HEAD),
        "invalid control envelope markers"
    );
    let mut control: BTreeMap<String, String> =
        serde_json::from_str(&encoded).context("invalid control envelope")?;
    for key in control.keys() {
        ensure!(
            key.starts_with("durable-db-") && ![HEAD, FORMAT, CONTROL].contains(&key.as_str()),
            "invalid packed control field"
        );
    }
    for (key, value) in wire {
        // Old binaries probe before checking the format. Their nonsemantic
        // startup/read fences may appear outside the document; preserve them
        // without permitting an outer ledger/root override.
        ensure!(
            !key.starts_with("durable-db-")
                || [HEAD, FORMAT].contains(&key.as_str())
                || LEGACY_PROBES.contains(&key.as_str()),
            "unrecognized outer control field"
        );
        control.insert(key, value);
    }
    Ok(control)
}

pub(super) fn reserved_wire(
    info: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut wire = encode(info)?;
    if super::budgeted_format(info).is_some() {
        for key in LEGACY_PROBES {
            wire.insert(key.into(), "0".repeat(36));
        }
        ensure!(
            wire.len() <= 10,
            "publication authority metadata entry budget exceeded"
        );
    }
    Ok(wire)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packed_metadata_preserves_fields_leaves_room_for_old_probes_and_refuses_overrides() {
        let mut info = BTreeMap::from([
            (FORMAT.into(), "4".into()),
            (
                HEAD.into(),
                r#"{"upload_format":4,"directory":null}"#.into(),
            ),
            ("unrelated".into(), "retained".into()),
        ]);
        for n in 0..32 {
            info.insert(format!("durable-db-field-{n}"), "value".into());
        }
        let mut wire = encode(&info).unwrap();
        assert_eq!(wire.len(), 4);
        assert_eq!(reserved_wire(&info).unwrap().len(), 6);
        assert_eq!(decode(wire.clone()).unwrap(), info);
        assert!(serde_json::from_str::<crate::directory::Root>(&wire[HEAD]).is_err());
        wire.insert("durable-db-probe".into(), "old-process-probe".into());
        assert_eq!(
            decode(wire.clone()).unwrap()["durable-db-probe"],
            "old-process-probe"
        );
        wire.insert("durable-db-upload-budget".into(), "override".into());
        assert!(decode(wire).is_err());
        let mut excessive = info;
        for n in 0..5 {
            excessive.insert(format!("unrelated-{n}"), "v".into());
        }
        assert!(reserved_wire(&excessive).is_err());
    }
}
