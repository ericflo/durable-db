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
pub mod control;
pub mod partitions;
mod register;
pub mod s3;
mod transport;

// Deletion is restricted to the namespaces with implemented collectors. Native
// journal names additionally bind a canonical incarnation and frame position.
pub(crate) fn collection_key(key: &str) -> anyhow::Result<()> {
    if let Some(scoped) = key.strip_prefix("partitions/") {
        let (partition, key) = scoped
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid partition scope"))?;
        anyhow::ensure!(
            partition.len() == 8
                && partition
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid partition scope"
        );
        anyhow::ensure!(!key.starts_with("partitions/"), "nested partition scope");
        return collection_key(key);
    }
    if key.starts_with("permits/") {
        crate::ownership::root::permit_key(key)?;
    } else if key.starts_with("journal/") {
        crate::journal::LogId::parse_key(key)?;
    } else if key.starts_with("tables/") {
        crate::storage::immutable::Blob::validate_key(key)?;
    } else {
        anyhow::ensure!(
            ["blocks/", "manifests/", "indexes/", "proposals/"]
                .iter()
                .any(|prefix| key.starts_with(prefix)),
            "invalid collection namespace"
        );
    }
    Ok(())
}

pub struct Connection {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
    prefix: String,
    allocator: Option<Arc<crate::upload_budget::Allocator>>,
    measured: bool,
}

impl Connection {
    pub(crate) fn new(
        files: Arc<dyn Storage>,
        authority: Arc<dyn Authority>,
        prefix: &str,
    ) -> Self {
        Self {
            files,
            authority: Arc::new(register::Register(authority)),
            prefix: prefix.into(),
            allocator: None,
            measured: false,
        }
    }

    /// Measured object operations; callers own identity, budget and reclamation rules.
    /// Each PUT has at most one physical attempt. Object CAS is not portable to B2;
    /// use the separate authority for conditional control updates.
    pub fn objects(&self) -> Arc<dyn Storage> {
        if self.measured {
            self.files.clone()
        } else {
            Arc::new(crate::measured::Files(self.files.clone()))
        }
    }

    /// Initialize a fresh native namespace's cumulative upload ceiling, or open
    /// its existing ledger without resetting counters or changing limits. All
    /// objects from this connection, including bootstrap registrations, are
    /// charged before PUT. Unused local credit is lost on connection loss.
    /// Existing unbudgeted/legacy namespaces require a separate migration.
    pub async fn with_upload_budget(self, limits: UploadLimits) -> anyhow::Result<Self> {
        let connection = self.with_lazy_upload_budget()?;
        connection.initialize_upload_budget(limits).await?;
        Ok(connection)
    }

    // A controller must expose administrative refill even when creating the
    // first partition record is unfunded. Attach admission now, initialize later.
    pub(crate) fn with_lazy_upload_budget(mut self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.allocator.is_none(),
            "connection already has an upload budget"
        );
        let allocator = Arc::new(crate::upload_budget::Allocator::new(self.authority()));
        self.files = Arc::new(crate::upload_budget::Files {
            inner: self.files,
            allocator: allocator.clone(),
        });
        self.allocator = Some(allocator);
        Ok(self)
    }
    pub(crate) async fn initialize_upload_budget(
        &self,
        limits: UploadLimits,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.allocator.is_some(),
            "connection has no upload admission"
        );
        crate::upload_budget::initialize_native(&self.authority(), &self.prefix, limits).await
    }

    pub(crate) async fn fund_claim(&self) -> anyhow::Result<bool> {
        match &self.allocator {
            Some(allocator) => {
                allocator
                    .prefund(crate::ownership::root::CLAIM_UPLOADS)
                    .await
            }
            None => Ok(false),
        }
    }

    /// Raw observations may be stale. A successful replacement is durable, but
    /// neither an observation nor a replacement alone establishes serving permission.
    /// Each replacement adds a fresh identity in the reserved `durable-db-probe`
    /// field, so repeated logical metadata still requires an exact comparison.
    /// The returned metadata includes that identity. Uncertain writes are not retried.
    pub fn authority(&self) -> Arc<dyn Authority> {
        if self.measured {
            self.authority.clone()
        } else {
            Arc::new(crate::measured::Register(self.authority.clone()))
        }
    }

    /// Start a fresh control caller with explicit invocation and uncertainty
    /// handling. This establishes no ownership or permission to serve data.
    pub fn control(&self) -> control::Session {
        control::Session::new(self.authority())
    }

    /// Construct a fresh clock-independent native candidate. Claim, activation
    /// and selected journal recovery must complete before serving.
    pub fn root(&self) -> Result<crate::ownership::root::Root, crate::ownership::root::Error> {
        crate::ownership::root::Root::new(
            self.objects(),
            self.control(),
            &self.prefix,
            crate::ownership::root::Encoding::REMOTE,
            self.allocator.is_some(),
            None,
        )
    }
    pub(crate) fn exclusive_root(
        &self,
        directory: Arc<crate::storage::Local>,
    ) -> Result<crate::ownership::root::Root, crate::ownership::root::Error> {
        crate::ownership::root::Root::new(
            self.objects(),
            self.control(),
            &self.prefix,
            crate::ownership::root::Encoding::LOCAL,
            false,
            Some(directory),
        )
    }

    /// Open the shared directory for independently owned native partitions.
    /// Both shared and per-partition upload ceilings are mandatory. This does
    /// not elect owners, choose placement, or route HTTP requests.
    pub async fn into_partitions(
        self,
        limits: UploadLimits,
        cache: Arc<crate::cache::Cache>,
    ) -> anyhow::Result<partitions::Partitions> {
        anyhow::ensure!(
            self.allocator.is_none(),
            "partition directory requires an uninitialized connection"
        );
        partitions::Partitions::open(self, limits, cache).await
    }

    /// Explicitly initialize or open the current publication engine.
    pub async fn into_published(self, limits: Option<UploadLimits>) -> anyhow::Result<Published> {
        Published::open_with_budget(self.files, self.authority, &self.prefix, limits).await
    }
}

#[cfg(test)]
async fn check_native_budget(connection: Connection) {
    use crate::ownership::root::Outcome;
    let limits = UploadLimits {
        bytes: 4096,
        attempts: 4,
    };
    let connection = connection.with_upload_budget(limits).await.unwrap();
    for confirmed in [false, true] {
        let mut root = connection.root().unwrap();
        let observed = root.observe().await.unwrap();
        let claim = root.claim(observed).unwrap();
        let outcome = root.finish(claim.execute().await).unwrap();
        assert!(if confirmed {
            matches!(outcome, Outcome::Confirmed)
        } else {
            matches!(outcome, Outcome::Conflict)
        });
    }
    let files = connection.objects();
    assert!(
        files
            .put("tables/refused", bytes::Bytes::new())
            .await
            .is_err()
    );
    assert_eq!(
        files.upload_budget().await.unwrap().unwrap().allocated,
        limits
    );
    files
        .replenish_upload_budget(&crate::upload_budget::UploadRequest {
            request_id: "fund-tenant-metadata".into(),
            expected_configuration_revision: 0,
            limits: UploadLimits {
                bytes: 1024 * 1024,
                attempts: 100,
            },
        })
        .await
        .unwrap();
    let root = connection.root().unwrap();
    for name in ["a", "b"] {
        let ledger = root.database_budget(name).unwrap().unwrap();
        ledger
            .replenish(&crate::upload_budget::UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 200,
                    attempts: 1,
                },
            })
            .await
            .unwrap();
        ledger.charge(100).await.unwrap();
        assert!(ledger.charge(1).await.is_err());
    }
    assert!(root.collection().collect().await.unwrap() > 0);
    for name in ["a", "b"] {
        assert_eq!(
            root.database_budget(name)
                .unwrap()
                .unwrap()
                .status()
                .await
                .unwrap()
                .allocated
                .attempts,
            1
        );
    }
    assert_eq!(
        connection
            .objects()
            .list("manifests/", None, 128)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        connection
            .objects()
            .list("indexes/", None, 128)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(root.observe().await.is_ok());
    assert!(connection.authority().read().await.unwrap().info.len() <= 10);
    let journal = format!(
        "journal/{:032x}-{}-{:016x}",
        1,
        uuid::Uuid::new_v4().simple(),
        1
    );
    files
        .put(&journal, bytes::Bytes::from_static(b"frame"))
        .await
        .unwrap();
    let authority = connection.authority();
    for refused in [
        "journal/frame",
        "journal/../ROOT",
        "tables/retained",
        "ownership/retained",
    ] {
        assert!(authority.delete_versions(refused).await.is_err());
    }
    assert!(files.get(&journal, 16).await.unwrap().is_some());
    authority.delete_versions(&journal).await.unwrap();
    assert!(files.get(&journal, 16).await.unwrap().is_none());
}
