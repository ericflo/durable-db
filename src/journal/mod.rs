//! Immutable log foundation for the owned-partition engine.
//!
//! This module establishes byte durability and ordered replay, not ownership or
//! transaction success. Before acknowledging a transaction, the future serving
//! controller must check permission and make handoff decisions durable. See
//! `docs/design-owned-partitions.md` and the preceding WAL models.
mod budget;
mod frame;
mod reader;
mod writer;

pub use frame::{Batch, LogId, MAX_BATCH_BYTES, MAX_RECORD_BYTES, MAX_RECORDS, Prefix};
pub use reader::Reader;
pub use writer::{Completion, PreparedUpload, QueueLimits, Resolution, Writer};

#[cfg(test)]
mod tests;
