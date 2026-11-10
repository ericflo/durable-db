//! Single-directory native serving. Exclusion comes from Local's lifetime OS
//! lock, never elapsed time. This adapter cannot construct a cloud connection.
use super::{Backend, Native};
use crate::{
    cache::Cache,
    ownership::{
        root::{Outcome, Profile},
        timing::Clock,
    },
    providers::Connection,
    publication::{Authority, Bucket},
    storage::{Local, Storage, Version},
    transactions::live::{Limits, Opening},
};
use anyhow::{Context, ensure};
use async_trait::async_trait;
use object_store::UpdateVersion;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

struct Register(Arc<Local>);
#[async_trait]
impl Authority for Register {
    async fn read(&self) -> anyhow::Result<Bucket> {
        let Some(object) = self.0.get("authority.json", 16384).await? else {
            return Ok(Bucket {
                revision: 0,
                version: None,
                info: BTreeMap::new(),
            });
        };
        let mut bucket: Bucket = serde_json::from_slice(&object.bytes)?;
        bucket.version = Some(object.version);
        Ok(bucket)
    }
    async fn replace(
        &self,
        old: &Bucket,
        info: BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Bucket>> {
        let mut next = Bucket {
            revision: old
                .revision
                .checked_add(1)
                .context("local authority revision exhausted")?,
            info,
            version: None,
        };
        let bytes = serde_json::to_vec(&next)?;
        ensure!(bytes.len() <= 16384, "local authority exceeds bound");
        next.version = Some(Version(UpdateVersion {
            e_tag: Some(hex::encode(Sha256::digest(&bytes))),
            version: None,
        }));
        Ok(self
            .0
            .cas("authority.json", old.version.as_ref(), bytes.into())
            .await?
            .then_some(next))
    }
    async fn delete_versions(&self, key: &str) -> anyhow::Result<()> {
        crate::providers::collection_key(key)?;
        self.0.delete(key).await
    }
}

// Private logical adapter, not a physical-clock claim. Keeping the directory
// alive retains exclusion even when the HTTP caller or worker is cancelled.
struct Exclusive {
    _directory: Arc<Local>,
    startup: Arc<AtomicU64>,
    pid: u32,
}
impl Clock for Exclusive {
    fn read(&self) -> Option<Duration> {
        (std::process::id() == self.pid)
            .then(|| Duration::from_nanos(self.startup.load(Ordering::Acquire)))
    }
}

/// Open a fresh or native local directory exclusively. Earlier local formats
/// require export with their original binary and restore into a new directory.
/// The directory must live on one host with working exclusive locks and fsync;
/// shared/network filesystems and copying live process memory are unsupported.
pub async fn open(
    data: PathBuf,
    cache: Arc<Cache>,
    capacity: usize,
    maintenance: Duration,
) -> anyhow::Result<impl Backend> {
    let directory = Arc::new(Local::open(data)?);
    directory.cleanup_temps()?;
    ensure!(
        directory.list("heads/", None, 1).await?.is_empty(),
        "legacy local directory: export with the previous binary and restore into a fresh directory"
    );
    if directory.get("authority.json", 16384).await?.is_none() {
        for prefix in ["journal/", "tables/", "catalogs/", "indexes/", "manifests/"] {
            ensure!(
                directory.list(prefix, None, 1).await?.is_empty(),
                "local authority is missing from a nonempty native directory"
            );
        }
    }
    let connection = Connection::new(
        directory.clone(),
        Arc::new(Register(directory.clone())),
        "local-exclusive",
    );
    let startup = Arc::new(AtomicU64::new(0));
    let clock = Exclusive {
        _directory: directory,
        startup: startup.clone(),
        pid: std::process::id(),
    };
    let mut root = connection.root(
        clock,
        Profile::new(Duration::from_nanos(1), Duration::ZERO)?,
    )?;
    let observed = root.observe().await?;
    let claim = root.claim(observed)?;
    ensure!(
        matches!(root.finish(claim.execute().await)?, Outcome::Confirmed),
        "local claim not confirmed"
    );
    // Acquiring the OS lock already excludes the predecessor and all its work.
    // Advance only the controller's startup sequence; serving never reads a physical clock.
    startup.store(2, Ordering::Release);
    let observed = root.observe().await?;
    let validate = root.validate(observed)?;
    ensure!(
        matches!(root.finish(validate.execute().await)?, Outcome::Confirmed),
        "local validation not confirmed"
    );
    let activate = root.activate()?;
    ensure!(
        matches!(root.finish(activate.execute().await)?, Outcome::Confirmed),
        "local activation not confirmed"
    );
    loop {
        let mut recovery = root.recovery()?;
        let mut pending = None;
        while let Some(mut journal) = recovery.next().await? {
            if !journal.is_selected() {
                while journal.read_next().await?.is_some() {}
                pending = Some(journal);
                break;
            }
        }
        let Some(journal) = pending else { break };
        let select = root.select_prefix(journal)?;
        ensure!(
            matches!(root.finish(select.execute().await)?, Outcome::Confirmed),
            "local journal selection not confirmed"
        );
    }
    let owner = Opening::new(root, Limits::default(), cache)?
        .recover()
        .await?;
    Ok(Native::start(owner, capacity, None, maintenance)?)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn exclusive_local_clock_refuses_forked_handles() {
        use std::{io, os::unix::process::CommandExt, process::Command};
        let directory = tempfile::tempdir().unwrap();
        let clock = Exclusive {
            _directory: Arc::new(Local::open(directory.path().into()).unwrap()),
            startup: Arc::new(AtomicU64::new(2)),
            pid: std::process::id(),
        };
        assert_eq!(clock.read(), Some(Duration::from_nanos(2)));
        let mut child = Command::new("/bin/true");
        // SAFETY: the mismatched PID path performs no allocation, I/O or locking.
        unsafe {
            child.pre_exec(move || {
                if clock.read().is_none() {
                    Err(io::Error::from_raw_os_error(1))
                } else {
                    Ok(())
                }
            });
        }
        assert_eq!(
            child.spawn().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}
