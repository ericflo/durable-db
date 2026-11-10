use super::{MAX_INLINE_BYTES, MAX_NAME_BYTES, Version};
use anyhow::{Context, ensure};
use bytes::{Buf, Bytes};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"DDBFILE1";
const HEADER: usize = 62;
const CHECKSUM: usize = 32;

pub(super) struct Record {
    pub name: String,
    pub value: Option<Bytes>,
    pub expected: Option<Version>,
    pub version: Version,
}

pub(super) fn validate(name: &str, value: Option<&[u8]>) -> anyhow::Result<()> {
    ensure!(
        (1..=MAX_NAME_BYTES).contains(&name.len())
            && !name.contains('\0')
            && name
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".."),
        "invalid logical file name"
    );
    ensure!(
        value.is_none_or(|v| v.len() <= MAX_INLINE_BYTES),
        "logical file exceeds inline limit"
    );
    Ok(())
}
impl Record {
    pub fn encode(&self) -> anyhow::Result<Bytes> {
        validate(&self.name, self.value.as_deref())?;
        ensure!(
            !self.version.epoch.is_nil() && self.version.revision > 0,
            "invalid logical file version"
        );
        ensure!(
            self.expected.is_none_or(|v| !v.epoch.is_nil()
                && v.revision > 0
                && v.revision < self.version.revision),
            "invalid logical predecessor version"
        );
        let mut out = Vec::with_capacity(
            HEADER + self.name.len() + self.value.as_ref().map_or(0, Bytes::len) + CHECKSUM,
        );
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(self.version.epoch.as_bytes());
        out.extend_from_slice(&self.version.revision.to_be_bytes());
        out.extend_from_slice(self.expected.map_or(Uuid::nil(), |v| v.epoch).as_bytes());
        out.extend_from_slice(&self.expected.map_or(0, |v| v.revision).to_be_bytes());
        out.extend_from_slice(&(self.name.len() as u16).to_be_bytes());
        out.extend_from_slice(
            &self
                .value
                .as_ref()
                .map_or(u32::MAX, |v| v.len() as u32)
                .to_be_bytes(),
        );
        out.extend_from_slice(self.name.as_bytes());
        if let Some(value) = &self.value {
            out.extend_from_slice(value);
        }
        out.extend_from_slice(&Sha256::digest(&out));
        Ok(out.into())
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            (HEADER + CHECKSUM..=HEADER + MAX_NAME_BYTES + MAX_INLINE_BYTES + CHECKSUM)
                .contains(&bytes.len()),
            "invalid logical record length"
        );
        let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM);
        ensure!(
            Sha256::digest(body).as_slice() == checksum,
            "logical record checksum mismatch"
        );
        let mut input = body;
        ensure!(&input[..8] == MAGIC, "unknown logical record format");
        input.advance(8);
        let epoch = Uuid::from_slice(&input[..16])?;
        input.advance(16);
        let revision = input.get_u64();
        let previous_epoch = Uuid::from_slice(&input[..16])?;
        input.advance(16);
        let previous_revision = input.get_u64();
        let name_len = input.get_u16() as usize;
        let value_len = input.get_u32();
        let payload_len = if value_len == u32::MAX {
            0
        } else {
            value_len as usize
        };
        ensure!(
            name_len.checked_add(payload_len) == Some(input.len()),
            "logical record length mismatch"
        );
        let name = std::str::from_utf8(input.get(..name_len).context("logical name missing")?)?;
        input.advance(name_len);
        let value = (value_len != u32::MAX).then_some(input);
        validate(name, value)?;
        ensure!(
            !epoch.is_nil() && revision > 0,
            "invalid logical file version"
        );
        let expected = if previous_epoch.is_nil() {
            ensure!(previous_revision == 0, "invalid absent predecessor");
            None
        } else {
            ensure!(
                previous_revision > 0 && previous_revision < revision,
                "invalid logical predecessor version"
            );
            Some(Version {
                epoch: previous_epoch,
                revision: previous_revision,
            })
        };
        Ok(Self {
            name: name.into(),
            value: value.map(Bytes::copy_from_slice),
            expected,
            version: Version { epoch, revision },
        })
    }
}
