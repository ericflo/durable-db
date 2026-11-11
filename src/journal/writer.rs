use super::{
    budget::{Budget, Reservation},
    frame::{self, Batch, Digest, LogId, Prefix},
};
use crate::storage::Storage;
use anyhow::{Context, ensure};
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct QueueLimits {
    batches: usize,
    bytes: usize,
}

impl QueueLimits {
    pub(crate) fn share(self, partitions: usize) -> anyhow::Result<Self> {
        Self::new(self.batches, self.bytes / partitions)
    }

    pub fn new(batches: usize, bytes: usize) -> anyhow::Result<Self> {
        ensure!(
            (1..=64).contains(&batches),
            "journal queue must hold between 1 and 64 batches"
        );
        ensure!(
            (120..=256 * 1024 * 1024).contains(&bytes),
            "invalid journal byte budget"
        );
        Ok(Self { batches, bytes })
    }
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            batches: 16,
            bytes: 16 * 1024 * 1024,
        }
    }
}

struct Pending {
    bytes: Bytes,
    digest: Digest,
    durable: bool,
    reservation: Arc<Reservation>,
}

/// One fresh incarnation. It cannot reopen an old epoch or reuse a position.
/// The queue includes prepared, in-flight, unresolved and unconsumed durable frames.
pub struct Writer {
    storage: Arc<dyn Storage>,
    limits: QueueLimits,
    prefix: Prefix,
    planned: u64,
    previous: Digest,
    pending: BTreeMap<u64, Pending>,
    pending_bytes: usize,
    budget: Budget,
}

impl Writer {
    pub fn new(
        storage: Arc<dyn Storage>,
        partition: Uuid,
        limits: QueueLimits,
    ) -> anyhow::Result<Self> {
        Ok(Self::with_log(storage, LogId::fresh(partition)?, limits))
    }

    pub(crate) fn for_root(
        storage: Arc<dyn Storage>,
        incarnation: Uuid,
        limits: QueueLimits,
    ) -> Self {
        Self::with_log(storage, LogId::root(incarnation), limits)
    }

    fn with_log(storage: Arc<dyn Storage>, log: LogId, limits: QueueLimits) -> Self {
        Self {
            storage,
            limits,
            prefix: Prefix::empty(log),
            planned: 0,
            previous: [0; 32],
            pending: BTreeMap::new(),
            pending_bytes: 0,
            budget: Budget::new(limits.batches, limits.bytes),
        }
    }

    pub fn log(&self) -> LogId {
        self.prefix.log
    }

    /// The contiguous prefix already consumed with `pop_durable`.
    pub fn durable_prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    /// Includes uploads whose replies are still pending and unconsumed batches,
    /// even when a point read has already established their durability.
    pub fn reserved_bytes(&self) -> usize {
        self.budget.reserved_bytes()
    }

    pub fn prepare(&mut self, records: &[Bytes]) -> anyhow::Result<PreparedUpload> {
        self.reserve(frame::encoded_len(records)?)?.finish(records)
    }

    /// Reserve before accepting transactions. Dropping this value without
    /// finishing releases capacity and does not consume a journal position.
    pub(crate) fn reserve(&mut self, length: usize) -> anyhow::Result<Reserved<'_>> {
        ensure!(
            (120..=super::MAX_BATCH_BYTES).contains(&length),
            "invalid journal reservation"
        );
        ensure!(
            self.pending.len() < self.limits.batches,
            "journal queue is full"
        );
        ensure!(
            length <= self.limits.bytes - self.pending_bytes,
            "journal byte budget exhausted"
        );
        let reservation = self.budget.reserve(length)?;
        let position = self
            .planned
            .checked_add(1)
            .context("journal epoch exhausted")?;
        Ok(Reserved {
            writer: self,
            reservation,
            position,
            length,
        })
    }

    pub(crate) fn planned_position(&self) -> u64 {
        self.planned
    }
}

pub(crate) struct Reserved<'a> {
    writer: &'a mut Writer,
    reservation: Arc<Reservation>,
    position: u64,
    length: usize,
}
impl Reserved<'_> {
    pub(crate) fn position(&self) -> u64 {
        self.position
    }
    pub(crate) fn finish(self, records: &[Bytes]) -> anyhow::Result<PreparedUpload> {
        ensure!(
            frame::encoded_len(records)? <= self.length,
            "journal reservation exceeded"
        );
        let writer = self.writer;
        let (bytes, digest) = frame::encode(writer.log(), self.position, writer.previous, records)?;
        writer.pending.insert(
            self.position,
            Pending {
                bytes: bytes.clone(),
                digest,
                durable: false,
                reservation: self.reservation.clone(),
            },
        );
        writer.pending_bytes += bytes.len();
        writer.previous = digest;
        writer.planned = self.position;
        Ok(PreparedUpload {
            storage: writer.storage.clone(),
            prefix: Prefix {
                log: writer.log(),
                position: self.position,
                digest,
            },
            bytes,
            _reservation: self.reservation,
        })
    }
}

impl Writer {
    pub fn complete(&mut self, completion: Completion) -> anyhow::Result<()> {
        ensure!(
            completion.prefix.log == self.log(),
            "completion belongs to another journal"
        );
        ensure!(
            (1..=self.planned).contains(&completion.prefix.position),
            "unknown journal completion"
        );
        if completion.prefix.position <= self.prefix.position {
            return Ok(()); // A point read may have resolved it before the upload reply arrived.
        }
        let pending = self
            .pending
            .get_mut(&completion.prefix.position)
            .context("missing journal position")?;
        ensure!(
            pending.digest == completion.prefix.digest,
            "journal completion digest mismatch"
        );
        pending.durable |= completion.durable;
        Ok(())
    }

    /// A missing object remains unresolved: a previously sent PUT may still arrive.
    /// Resolution never sends another PUT or changes the batch identity.
    pub async fn resolve(&mut self, position: u64) -> anyhow::Result<Resolution> {
        ensure!(
            (1..=self.planned).contains(&position),
            "unknown journal position"
        );
        if position <= self.prefix.position {
            return Ok(Resolution::Durable);
        }
        let pending = self
            .pending
            .get(&position)
            .context("missing journal position")?;
        if pending.durable {
            return Ok(Resolution::Durable);
        }
        let object = self
            .storage
            .get(&self.log().key(position), pending.bytes.len())
            .await?;
        let Some(object) = object else {
            return Ok(Resolution::Unknown);
        };
        ensure!(
            object.bytes == pending.bytes,
            "journal resolution bytes differ"
        );
        self.pending
            .get_mut(&position)
            .context("missing journal position")?
            .durable = true;
        Ok(Resolution::Durable)
    }

    /// Consume the next durable frame. This does not authorize a client acknowledgement.
    pub fn pop_durable(&mut self) -> anyhow::Result<Option<Batch>> {
        let Some(position) = self.prefix.position.checked_add(1) else {
            return Ok(None);
        };
        let Some(pending) = self.pending.get(&position).filter(|p| p.durable) else {
            return Ok(None);
        };
        let mut batch = Batch::decode(pending.bytes.clone(), self.log(), position)?;
        batch.reservation = Some(pending.reservation.clone());
        ensure!(
            batch.previous == self.prefix.digest,
            "journal prefix digest mismatch"
        );
        self.pending_bytes -= pending.bytes.len();
        self.pending.remove(&position);
        self.prefix = batch.prefix;
        Ok(Some(batch))
    }
}

/// At most one physical PUT. Only proven admission refusal can return this owned
/// handle for a first send. Cancellation gives no such proof: the position stays
/// unresolved until an exact read or a future handoff selects the old prefix.
pub struct PreparedUpload {
    storage: Arc<dyn Storage>,
    prefix: Prefix,
    bytes: Bytes,
    _reservation: Arc<Reservation>,
}

impl PreparedUpload {
    pub fn position(&self) -> u64 {
        self.prefix.position
    }

    pub async fn send(self) -> Completion {
        // Keep the reservation through the await, including cancellation. Do not
        // let a successful resolving GET free memory still held by this upload.
        let result = self
            .storage
            .put(
                &self.prefix.log.key(self.prefix.position),
                self.bytes.clone(),
            )
            .await;
        let unsent = result
            .as_ref()
            .err()
            .is_some_and(|e| e.is::<crate::upload_budget::NotSent>());
        Completion {
            prefix: self.prefix,
            durable: result.is_ok(),
            unsent: unsent.then_some(self),
        }
    }
}

/// An upload response, not a transaction result. Unknown responses may have persisted.
pub struct Completion {
    prefix: Prefix,
    durable: bool,
    unsent: Option<PreparedUpload>,
}

impl Completion {
    /// Recover the owned upload only when admission proved no data request was
    /// sent. The same identity/buffers remain reserved. Unknown provider replies
    /// and cancellation never produce this witness and never permit another PUT.
    pub fn take_unsent(&mut self) -> Option<PreparedUpload> {
        self.unsent.take()
    }
    pub fn resolution(&self) -> Resolution {
        if self.durable {
            Resolution::Durable
        } else {
            Resolution::Unknown
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolution {
    Durable,
    Unknown,
}
