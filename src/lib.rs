//! durable-db: a transactional ordered database whose source of truth is an
//! object-storage bucket.
//!
//! The protocol is specified in `spec/DurableDb.tla` and explained in
//! `docs/architecture.md`. In short: every database is a chain of immutable
//! objects, one writer at a time holds a revocable claim, and every
//! acknowledgement is preceded by a check that the claim still stands.
pub mod auth;
pub mod bench;
pub mod cluster;
pub mod db;
pub mod http;
pub mod io;
pub mod layout;
pub mod mem;
pub mod metrics;
pub mod node;
pub mod store;
pub mod table;

use store::StoreError;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("database not found")]
    NotFound,
    #[error("database was deleted")]
    Gone,
    #[error("database already exists with a different configuration")]
    Exists,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Invalid(String),
    #[error("database storage quota exceeded")]
    Quota,
    #[error("the request body is larger than this server accepts")]
    TooLarge,
    #[error("too many requests")]
    RateLimited,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    /// Nothing happened; try again shortly.
    #[error("{0}")]
    Busy(String),
    /// This node stopped owning the database. A commit in flight may or may
    /// not have been applied; retry it with the same idempotency key.
    #[error("ownership moved: {0}")]
    Lost(String),
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
}

impl Error {
    /// Whether repeating the identical request can succeed.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Busy(_) | Self::Lost(_) | Self::Unavailable(_) | Self::RateLimited
        )
    }
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Unavailable(reason) => Self::Unavailable(reason),
            StoreError::Fatal(reason) => Self::Unavailable(format!("fatal: {reason}")),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
