//! Common provider primitives, independent of the serving engine.
//!
//! Both providers expose immutable object storage and a conditional metadata
//! register. Connecting does not initialize a database format or grant serving
//! permission. The existing publication engine remains an explicit consumer.
use crate::{
    publication::{Authority, Published},
    storage::Storage,
    upload_budget::UploadLimits,
};
use std::sync::Arc;

pub mod b2;
pub mod s3;
mod transport;

pub struct Connection {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
    prefix: String,
}

impl Connection {
    fn new(files: Arc<dyn Storage>, authority: Arc<dyn Authority>, prefix: &str) -> Self {
        Self {
            files,
            authority,
            prefix: prefix.into(),
        }
    }

    /// Measured object operations; callers own identity, budget and reclamation rules.
    /// Each PUT has at most one physical attempt. Object CAS is not portable to B2;
    /// use the separate authority for conditional control updates.
    pub fn objects(&self) -> Arc<dyn Storage> {
        Arc::new(crate::measured::Files(self.files.clone()))
    }

    /// Raw observations may be stale. A successful replacement is durable, but
    /// neither an observation nor a replacement alone establishes serving permission.
    pub fn authority(&self) -> Arc<dyn Authority> {
        Arc::new(crate::measured::Register(self.authority.clone()))
    }

    /// Explicitly initialize or open the current publication engine.
    pub async fn into_published(self, limits: Option<UploadLimits>) -> anyhow::Result<Published> {
        Published::open_with_budget(self.files, self.authority, &self.prefix, limits).await
    }
}
