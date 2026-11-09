use anyhow::Context;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Reservations follow owned frames and in-flight requests, not queue positions.
pub(super) struct Budget {
    bytes: Arc<Semaphore>,
    batches: Arc<Semaphore>,
    limit: usize,
}

impl Budget {
    pub fn new(batches: usize, bytes: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(bytes)),
            batches: Arc::new(Semaphore::new(batches)),
            limit: bytes,
        }
    }

    pub fn reserve(&self, bytes: usize) -> anyhow::Result<Arc<Reservation>> {
        Ok(Arc::new(Reservation {
            _batch: self
                .batches
                .clone()
                .try_acquire_owned()
                .context("journal frames remain in use")?,
            _bytes: self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes as u32)
                .context("journal buffers remain in use")?,
        }))
    }

    pub fn reserved_bytes(&self) -> usize {
        self.limit - self.bytes.available_permits()
    }
}

pub(super) struct Reservation {
    _bytes: OwnedSemaphorePermit,
    _batch: OwnedSemaphorePermit,
}
