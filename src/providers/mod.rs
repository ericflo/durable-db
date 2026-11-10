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
mod register;
pub mod s3;
mod transport;

pub struct Connection {
    files: Arc<dyn Storage>,
    authority: Arc<dyn Authority>,
    prefix: String,
    budgeted: bool,
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
            budgeted: false,
        }
    }

    /// Measured object operations; callers own identity, budget and reclamation rules.
    /// Each PUT has at most one physical attempt. Object CAS is not portable to B2;
    /// use the separate authority for conditional control updates.
    pub fn objects(&self) -> Arc<dyn Storage> {
        Arc::new(crate::measured::Files(self.files.clone()))
    }

    /// Initialize a fresh native namespace's cumulative upload ceiling, or open
    /// its existing ledger without resetting counters or changing limits. All
    /// objects from this connection, including bootstrap registrations, are
    /// charged before PUT. Unused local credit is lost on connection loss.
    /// Existing unbudgeted/legacy namespaces require a separate migration.
    pub async fn with_upload_budget(mut self, limits: UploadLimits) -> anyhow::Result<Self> {
        anyhow::ensure!(!self.budgeted, "connection already has an upload budget");
        let authority = self.authority();
        crate::upload_budget::initialize_native(&authority, &self.prefix, limits).await?;
        self.files = Arc::new(crate::upload_budget::Files {
            inner: self.files,
            allocator: Arc::new(crate::upload_budget::Allocator::new(authority)),
        });
        self.budgeted = true;
        Ok(self)
    }

    /// Raw observations may be stale. A successful replacement is durable, but
    /// neither an observation nor a replacement alone establishes serving permission.
    /// Each replacement adds a fresh identity in the reserved `durable-db-probe`
    /// field, so repeated logical metadata still requires an exact comparison.
    /// The returned metadata includes that identity. Uncertain writes are not retried.
    pub fn authority(&self) -> Arc<dyn Authority> {
        Arc::new(crate::measured::Register(self.authority.clone()))
    }

    /// Start a fresh control caller with explicit invocation and uncertainty
    /// handling. This establishes no ownership or permission to serve data.
    pub fn control(&self) -> control::Session {
        control::Session::new(self.authority())
    }

    /// Create a candidate root controller under an explicitly supplied clock
    /// contract. This can initialize a fresh experimental ownership format;
    /// it rejects the existing engine's format and does not serve database data.
    pub fn root<C: crate::ownership::timing::Clock>(
        &self,
        clock: C,
        profile: crate::ownership::root::Profile,
    ) -> Result<crate::ownership::root::Root<C>, crate::ownership::root::Error> {
        crate::ownership::root::Root::new(
            self.objects(),
            self.control(),
            &self.prefix,
            clock,
            profile,
            self.budgeted,
        )
    }

    /// Explicitly initialize or open the current publication engine.
    pub async fn into_published(self, limits: Option<UploadLimits>) -> anyhow::Result<Published> {
        Published::open_with_budget(self.files, self.authority, &self.prefix, limits).await
    }
}

#[cfg(test)]
async fn check_native_budget(connection: Connection) {
    use crate::ownership::{
        root::{Outcome, Profile},
        timing::Clock,
    };
    use std::time::Duration;
    struct Fixed;
    impl Clock for Fixed {
        fn read(&self) -> Option<Duration> {
            Some(Duration::ZERO)
        }
    }
    let limits = UploadLimits {
        bytes: 4096,
        attempts: 2,
    };
    let connection = connection.with_upload_budget(limits).await.unwrap();
    for confirmed in [false, true] {
        let mut root = connection
            .root(
                Fixed,
                Profile::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap(),
            )
            .unwrap();
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
    let root = connection
        .root(
            Fixed,
            Profile::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap(),
        )
        .unwrap();
    for name in ["a", "b"] {
        let ledger = root.database_budget(name).unwrap().unwrap();
        ledger
            .replenish(&crate::upload_budget::UploadRequest {
                request_id: "fund".into(),
                expected_configuration_revision: 0,
                limits: UploadLimits {
                    bytes: 100,
                    attempts: 1,
                },
            })
            .await
            .unwrap();
        ledger.charge(100).await.unwrap();
        assert!(ledger.charge(1).await.is_err());
    }
    assert!(root.collection().unwrap().collect().await.unwrap() > 0);
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
}
