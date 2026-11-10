use anyhow::{Context, ensure};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_RECORDS: usize = 1024;
const MAGIC: &[u8; 8] = b"DDBWAL01";
const PREFIX_MAGIC: &[u8; 8] = b"DDBPFX01";
const HEADER: usize = 84;
const CHECKSUM: usize = 32;
const PREFIX_BYTES: usize = 112;
pub(super) type Digest = [u8; CHECKSUM];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogId {
    partition: [u8; 16],
    epoch: [u8; 16],
}

impl LogId {
    /// Reserved coordinator journal. Its epoch is the controller's fresh,
    /// never-restored incarnation; only that controller may open a writer once.
    pub(crate) fn root(incarnation: Uuid) -> Self {
        Self {
            partition: *Uuid::from_u128(1).as_bytes(),
            epoch: *incarnation.as_bytes(),
        }
    }

    pub(super) fn fresh(partition: Uuid) -> anyhow::Result<Self> {
        ensure!(!partition.is_nil(), "journal partition must not be nil");
        Ok(Self {
            partition: *partition.as_bytes(),
            epoch: *Uuid::new_v4().as_bytes(),
        })
    }

    pub fn partition(self) -> Uuid {
        Uuid::from_bytes(self.partition)
    }

    pub fn epoch(self) -> Uuid {
        Uuid::from_bytes(self.epoch)
    }

    pub(super) fn key(self, position: u64) -> String {
        // One directory level also works with the single-process local binding.
        format!(
            "journal/{}-{}-{position:016x}",
            self.partition().simple(),
            self.epoch().simple()
        )
    }

    fn encode(self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.partition);
        output.extend_from_slice(&self.epoch);
    }

    fn decode(input: &mut Input<'_>) -> anyhow::Result<Self> {
        let id = Self {
            partition: input.array()?,
            epoch: input.array()?,
        };
        ensure!(
            !id.partition().is_nil() && !id.epoch().is_nil(),
            "invalid journal identity"
        );
        Ok(id)
    }
}

/// A particular log prefix. This value is not proof of a durable handoff decision.
/// Its selecting controller must persist it conditionally before activating a successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Prefix {
    pub(super) log: LogId,
    pub(super) position: u64,
    pub(super) digest: Digest,
}

impl Prefix {
    pub const ENCODED_BYTES: usize = PREFIX_BYTES;

    pub(super) fn empty(log: LogId) -> Self {
        Self {
            log,
            position: 0,
            digest: [0; CHECKSUM],
        }
    }

    pub fn log(self) -> LogId {
        self.log
    }

    pub fn position(self) -> u64 {
        self.position
    }

    /// Fixed-size, checksummed encoding for a future conditional control record.
    pub fn encode(self) -> Bytes {
        let mut bytes = Vec::with_capacity(PREFIX_BYTES);
        bytes.extend_from_slice(PREFIX_MAGIC);
        self.log.encode(&mut bytes);
        bytes.extend_from_slice(&self.position.to_be_bytes());
        bytes.extend_from_slice(&self.digest);
        append_checksum(&mut bytes);
        bytes.into()
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(bytes.len() == PREFIX_BYTES, "invalid journal prefix length");
        verify_checksum(bytes)?;
        let mut input = Input::new(&bytes[..PREFIX_BYTES - CHECKSUM]);
        ensure!(
            &input.array::<8>()? == PREFIX_MAGIC,
            "invalid journal prefix format"
        );
        let prefix = Self {
            log: LogId::decode(&mut input)?,
            position: u64::from_be_bytes(input.array()?),
            digest: input.array()?,
        };
        ensure!(
            prefix.position != 0 || prefix.digest == [0; CHECKSUM],
            "invalid empty journal prefix"
        );
        Ok(prefix)
    }
}

/// A validated frame. Record views borrow the frame and cannot outlive its budget.
/// Deliberately does not implement Debug, which could disclose application data.
pub struct Batch {
    pub(super) prefix: Prefix,
    pub(super) previous: Digest,
    bytes: Bytes,
    records: Vec<std::ops::Range<usize>>,
    pub(super) reservation: Option<std::sync::Arc<super::budget::Reservation>>,
}

impl Batch {
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn records(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.records.iter().map(|range| &self.bytes[range.clone()])
    }

    pub(super) fn decode(bytes: Bytes, log: LogId, position: u64) -> anyhow::Result<Self> {
        ensure!(
            (HEADER + CHECKSUM..=MAX_BATCH_BYTES).contains(&bytes.len()),
            "journal frame exceeds size bounds"
        );
        let digest = verify_checksum(&bytes)?;
        let end = bytes.len() - CHECKSUM;
        let mut input = Input::new(&bytes[..end]);
        ensure!(&input.array::<8>()? == MAGIC, "unsupported journal format");
        ensure!(
            LogId::decode(&mut input)? == log,
            "journal identity mismatch"
        );
        ensure!(
            u64::from_be_bytes(input.array()?) == position && position != 0,
            "journal position mismatch"
        );
        let previous = input.array()?;
        let count = u32::from_be_bytes(input.array()?) as usize;
        ensure!(
            (1..=MAX_RECORDS).contains(&count),
            "journal record count exceeds bounds"
        );
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let len = u32::from_be_bytes(input.array()?) as usize;
            ensure!(len <= MAX_RECORD_BYTES, "journal record exceeds size bound");
            let start = input.position;
            input.skip(len)?;
            records.push(start..input.position);
        }
        ensure!(input.position == end, "trailing journal bytes");
        Ok(Self {
            prefix: Prefix {
                log,
                position,
                digest,
            },
            previous,
            bytes,
            records,
            reservation: None,
        })
    }
}

pub(super) fn encoded_len(records: &[Bytes]) -> anyhow::Result<usize> {
    ensure!(
        (1..=MAX_RECORDS).contains(&records.len()),
        "journal record count exceeds bounds"
    );
    let mut length = HEADER + CHECKSUM;
    for record in records {
        ensure!(
            record.len() <= MAX_RECORD_BYTES,
            "journal record exceeds size bound"
        );
        length = length
            .checked_add(4 + record.len())
            .context("journal length overflow")?;
        ensure!(
            length <= MAX_BATCH_BYTES,
            "journal frame exceeds size bound"
        );
    }
    Ok(length)
}

pub(super) fn encode(
    log: LogId,
    position: u64,
    previous: Digest,
    records: &[Bytes],
) -> anyhow::Result<(Bytes, Digest)> {
    let mut bytes = Vec::with_capacity(encoded_len(records)?);
    bytes.extend_from_slice(MAGIC);
    log.encode(&mut bytes);
    bytes.extend_from_slice(&position.to_be_bytes());
    bytes.extend_from_slice(&previous);
    bytes.extend_from_slice(&(records.len() as u32).to_be_bytes());
    for record in records {
        bytes.extend_from_slice(&(record.len() as u32).to_be_bytes());
        bytes.extend_from_slice(record);
    }
    let digest = append_checksum(&mut bytes);
    Ok((bytes.into(), digest))
}

fn append_checksum(bytes: &mut Vec<u8>) -> Digest {
    let digest: Digest = Sha256::digest(&*bytes).into();
    bytes.extend_from_slice(&digest);
    digest
}

fn verify_checksum(bytes: &[u8]) -> anyhow::Result<Digest> {
    let end = bytes
        .len()
        .checked_sub(CHECKSUM)
        .context("truncated journal checksum")?;
    let digest: Digest = Sha256::digest(&bytes[..end]).into();
    ensure!(bytes[end..] == digest, "journal checksum mismatch");
    Ok(digest)
}

struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn skip(&mut self, length: usize) -> anyhow::Result<()> {
        let end = self
            .position
            .checked_add(length)
            .context("journal length overflow")?;
        ensure!(end <= self.bytes.len(), "truncated journal frame");
        self.position = end;
        Ok(())
    }

    fn array<const N: usize>(&mut self) -> anyhow::Result<[u8; N]> {
        let start = self.position;
        self.skip(N)?;
        Ok(self.bytes[start..self.position].try_into()?)
    }
}
