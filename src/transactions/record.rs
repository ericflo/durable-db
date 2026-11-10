//! Canonical accepted-transaction records, shared by writing and recovery.
//!
//! These contain evaluated mutations, not instructions to rerun application
//! code. Encoding does not establish admission, durability or serving authority.
use anyhow::{Context, ensure};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"DDBTXN01";
const HEADER: usize = 84;
const CHECKSUM: usize = 32;
const MAX_MUTATIONS: usize = 128;
const MAX_VALUE: usize = 64 * 1024;
const DELETE: u32 = u32::MAX;
/// Maximum declared receipt window in this internal record format. Retention
/// is a per-record decision, not a process-local recovery configuration.
pub const MAX_RECEIPTS: u64 = 65_536;

/// The generation prevents receipts or values from aliasing a recreated name.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Database<'a> {
    pub name: &'a str,
    pub generation: Uuid,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Mutation<'a> {
    pub key: &'a str,
    /// None deletes; Some(empty) stores an empty value.
    pub value: Option<&'a [u8]>,
}

/// Borrows application bytes from its journal frame; no Debug or owned clone.
pub struct Commit<'a> {
    database: Database<'a>,
    revision: u64,
    snapshot_revision: u64,
    receipt_floor: u64,
    request_id: &'a str,
    fingerprint: [u8; 32],
    mutations: Vec<Mutation<'a>>,
}

impl<'a> Commit<'a> {
    /// Mutations must already be evaluated, unique and in key order. The
    /// fingerprint covers the original request, including its read conditions;
    /// it is supplied by the transaction layer, not derived from final values.
    pub fn encode(
        database: Database<'_>,
        revision: u64,
        snapshot_revision: u64,
        receipt_floor: u64,
        request_id: &str,
        fingerprint: [u8; 32],
        mutations: &[Mutation<'_>],
    ) -> anyhow::Result<Bytes> {
        validate_header(
            database,
            revision,
            snapshot_revision,
            receipt_floor,
            request_id,
        )?;
        let length = Self::encoded_len(database, request_id, mutations)?;
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(database.generation.as_bytes());
        bytes.extend_from_slice(&revision.to_be_bytes());
        bytes.extend_from_slice(&snapshot_revision.to_be_bytes());
        bytes.extend_from_slice(&receipt_floor.to_be_bytes());
        bytes.extend_from_slice(&fingerprint);
        bytes.push(database.name.len() as u8);
        bytes.push(request_id.len() as u8);
        bytes.extend_from_slice(&(mutations.len() as u16).to_be_bytes());
        bytes.extend_from_slice(database.name.as_bytes());
        bytes.extend_from_slice(request_id.as_bytes());
        for mutation in mutations {
            bytes.extend_from_slice(&(mutation.key.len() as u16).to_be_bytes());
            bytes.extend_from_slice(
                &mutation
                    .value
                    .map_or(DELETE, |v| v.len() as u32)
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(mutation.key.as_bytes());
            if let Some(value) = mutation.value {
                bytes.extend_from_slice(value);
            }
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        debug_assert_eq!(bytes.len(), length);
        Ok(bytes.into())
    }

    pub(crate) fn encoded_len(
        database: Database<'_>,
        request_id: &str,
        mutations: &[Mutation<'_>],
    ) -> anyhow::Result<usize> {
        validate_header(database, 1, 0, 0, request_id)?;
        validate_mutations(mutations)?;
        let length = HEADER
            + database.name.len()
            + request_id.len()
            + mutations
                .iter()
                .map(|m| 6 + m.key.len() + m.value.map_or(0, <[u8]>::len))
                .sum::<usize>()
            + CHECKSUM;
        ensure!(
            length <= crate::journal::MAX_RECORD_BYTES,
            "transaction record exceeds journal bound"
        );
        Ok(length)
    }

    pub fn decode(bytes: &'a [u8]) -> anyhow::Result<Self> {
        ensure!(
            (HEADER + CHECKSUM..=crate::journal::MAX_RECORD_BYTES).contains(&bytes.len()),
            "invalid transaction record length"
        );
        let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM);
        ensure!(
            Sha256::digest(body).as_slice() == checksum,
            "transaction checksum mismatch"
        );
        let mut input = Input(body);
        ensure!(
            input.take(8)? == MAGIC,
            "unsupported transaction record format"
        );
        let generation = Uuid::from_bytes(input.array()?);
        let revision = u64::from_be_bytes(input.array()?);
        let snapshot_revision = u64::from_be_bytes(input.array()?);
        let receipt_floor = u64::from_be_bytes(input.array()?);
        let fingerprint = input.array()?;
        let name_len = input.array::<1>()?[0] as usize;
        let request_len = input.array::<1>()?[0] as usize;
        let count = u16::from_be_bytes(input.array()?) as usize;
        ensure!(count <= MAX_MUTATIONS, "too many transaction mutations");
        let database = Database {
            name: input.text(name_len)?,
            generation,
        };
        let request_id = input.text(request_len)?;
        validate_header(
            database,
            revision,
            snapshot_revision,
            receipt_floor,
            request_id,
        )?;
        // The only decode allocation: at most 128 borrowed key/value views.
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            let key_len = u16::from_be_bytes(input.array()?) as usize;
            let value_len = u32::from_be_bytes(input.array()?);
            let key = input.text(key_len)?;
            let value = if value_len == DELETE {
                None
            } else {
                ensure!(
                    value_len as usize <= MAX_VALUE,
                    "transaction value exceeds bound"
                );
                Some(input.take(value_len as usize)?)
            };
            mutations.push(Mutation { key, value });
        }
        ensure!(input.0.is_empty(), "trailing transaction bytes");
        validate_mutations(&mutations)?;
        Ok(Self {
            database,
            revision,
            snapshot_revision,
            receipt_floor,
            request_id,
            fingerprint,
            mutations,
        })
    }

    pub fn database(&self) -> Database<'a> {
        self.database
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn snapshot_revision(&self) -> u64 {
        self.snapshot_revision
    }
    /// Discard receipts at or below this revision when applying this record.
    /// The transaction layer chooses and admits the retention policy; replay
    /// must use the recorded decision, never its current configuration.
    pub fn receipt_floor(&self) -> u64 {
        self.receipt_floor
    }
    pub fn request_id(&self) -> &'a str {
        self.request_id
    }
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
    pub fn mutations(&self) -> impl ExactSizeIterator<Item = Mutation<'a>> + '_ {
        self.mutations.iter().copied()
    }
}

fn validate_header(
    database: Database<'_>,
    revision: u64,
    snapshot: u64,
    receipt_floor: u64,
    request: &str,
) -> anyhow::Result<()> {
    crate::engine::validate_db(database.name)?;
    ensure!(!database.generation.is_nil(), "invalid database generation");
    ensure!(
        revision > 0 && snapshot < revision && receipt_floor < revision,
        "invalid transaction revisions"
    );
    ensure!(
        revision - receipt_floor <= MAX_RECEIPTS,
        "receipt history exceeds recovery allocation bound"
    );
    ensure!(
        (1..=128).contains(&request.len()),
        "invalid transaction request identity"
    );
    Ok(())
}

fn validate_mutations(mutations: &[Mutation<'_>]) -> anyhow::Result<()> {
    ensure!(
        mutations.len() <= MAX_MUTATIONS,
        "too many transaction mutations"
    );
    let mut previous = None;
    for mutation in mutations {
        ensure!(
            (1..=1024).contains(&mutation.key.len()) && !mutation.key.contains('\0'),
            "invalid transaction key"
        );
        ensure!(
            previous.is_none_or(|p| p < mutation.key),
            "unordered or duplicate transaction keys"
        );
        ensure!(
            mutation.value.is_none_or(|v| v.len() <= MAX_VALUE),
            "transaction value exceeds bound"
        );
        previous = Some(mutation.key);
    }
    Ok(())
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, length: usize) -> anyhow::Result<&'a [u8]> {
        ensure!(length <= self.0.len(), "truncated transaction record");
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> anyhow::Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("checked array length"))
    }
    fn text(&mut self, length: usize) -> anyhow::Result<&'a str> {
        std::str::from_utf8(self.take(length)?).context("invalid transaction text encoding")
    }
}

#[cfg(test)]
mod tests;
