//! Ordered replay of a complete, immutable coordinator-journal catalog.
use super::{Identity, Record, catalog};
use crate::{
    directory,
    journal::{Batch, Prefix, Reader},
    storage::Storage,
};
use anyhow::{Context, ensure};
use std::{collections::VecDeque, sync::Arc};
use uuid::Uuid;

const PAGE_ENTRIES: usize = 64;

/// A complete frame boundary in a selected journal, not a serving capability.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cut {
    pub identity: Identity,
    pub prefix: Prefix,
}

/// Streams selected history oldest first; native recovery can resume from an
/// authenticated checkpoint cut. This does not itself apply
/// transactions or authorize a reply. Build a private view, finish replay, then
/// check current ownership before exposing it. Discard an invalid private view.
pub struct Replay {
    files: Arc<dyn Storage>,
    cache: Option<Arc<crate::cache::Cache>>,
    record: Record,
    index: Option<directory::Root>,
    loaded: bool,
    after: Option<String>,
    entries: VecDeque<String>,
    next_epoch: u64,
    current: Option<Identity>,
    reader: Option<Reader>,
    batch: Option<Batch>,
    completed: bool,
    cut: Option<Cut>,
}
impl Replay {
    pub(super) fn new(
        files: Arc<dyn Storage>,
        record: Record,
        cache: Option<Arc<crate::cache::Cache>>,
    ) -> Self {
        Self {
            files,
            cache,
            record,
            index: None,
            loaded: false,
            after: None,
            entries: VecDeque::new(),
            next_epoch: 1,
            current: None,
            reader: None,
            batch: None,
            completed: false,
            cut: None,
        }
    }

    /// True only after every selected frame and the complete index were read.
    /// It is a fact about this immutable snapshot, not current serving authority.
    pub fn completed(&self) -> bool {
        self.completed
    }

    pub(crate) fn at_start(&self) -> bool {
        !self.loaded
    }

    pub(crate) fn origin(&self) -> Record {
        self.record
    }
    pub(crate) fn files(&self) -> Arc<dyn Storage> {
        self.files.clone()
    }
    pub(crate) fn cut(&self) -> Option<Cut> {
        self.cut
    }

    async fn load(&mut self) -> anyhow::Result<()> {
        let index = catalog::load(&self.files, self.record.catalog).await?;
        ensure!(
            index.as_ref().map_or(0, |r| r.count) == self.record.identity.epoch - 1,
            "recovery catalog is incomplete"
        );
        self.index = index;
        self.loaded = true;
        Ok(())
    }

    pub(crate) async fn resume(&mut self, cut: Cut) -> anyhow::Result<()> {
        ensure!(
            self.at_start()
                && cut.identity.epoch > 0
                && cut.identity.epoch < self.record.identity.epoch,
            "invalid checkpoint replay epoch"
        );
        ensure!(
            cut.prefix.log() == crate::journal::LogId::root(cut.identity.incarnation),
            "checkpoint journal identity mismatch"
        );
        self.load().await?;
        let selected =
            catalog::selected(&self.files, &self.cache, self.index.as_ref(), cut.identity)
                .await?
                .context("checkpoint journal is not selected")?;
        self.reader = Some(
            Reader::resume(self.files.clone(), cut.prefix, selected)?
                .with_cache(self.cache.clone()),
        );
        self.current = Some(cut.identity);
        self.next_epoch = cut.identity.epoch;
        self.after = Some(format!(
            "{:016x}-{}.json",
            cut.identity.epoch,
            cut.identity.incarnation.simple()
        ));
        self.cut = Some(cut);
        Ok(())
    }

    /// At most one borrowed frame and 64 queued catalog keys. The borrow forces
    /// the caller to release each frame before requesting one from another log.
    pub async fn read_next(&mut self) -> anyhow::Result<Option<ReplayBatch<'_>>> {
        self.batch = None;
        if self.completed {
            return Ok(None);
        }
        if !self.loaded {
            self.load().await?;
        }
        loop {
            if self.current.is_some() {
                let reader = self
                    .reader
                    .as_mut()
                    .expect("selected identity has a reader");
                if let Some(batch) = reader.read_next().await? {
                    self.cut = Some(Cut {
                        identity: self.current.unwrap(),
                        prefix: batch.prefix(),
                    });
                    self.batch = Some(batch);
                    return Ok(Some(ReplayBatch {
                        identity: self.current.expect("reader has a selected identity"),
                        batch: self.batch.as_ref().expect("stored replay frame"),
                    }));
                }
                self.cut = Some(Cut {
                    identity: self.current.unwrap(),
                    prefix: reader
                        .finished_prefix()
                        .context("journal replay is incomplete")?,
                });
                self.current = None;
                self.next_epoch = self
                    .next_epoch
                    .checked_add(1)
                    .context("replay epoch exhausted")?;
            }
            if self.entries.is_empty() {
                self.entries = catalog::directory(&self.files, &self.cache)
                    .list(self.index.as_ref(), self.after.as_deref(), PAGE_ENTRIES)
                    .await?
                    .into();
                if self.entries.is_empty() {
                    ensure!(
                        self.next_epoch == self.record.identity.epoch,
                        "recovery catalog has an epoch gap"
                    );
                    self.completed = true;
                    return Ok(None);
                }
            }
            let name = self.entries.front().expect("loaded nonempty page");
            let (identity, after) = parse_entry(name)?;
            ensure!(
                identity.epoch == self.next_epoch && identity.epoch < self.record.identity.epoch,
                "recovery catalog has an unexpected epoch"
            );
            let prefix = catalog::selected(&self.files, &self.cache, self.index.as_ref(), identity)
                .await?
                .context("recovery catalog entry disappeared")?;
            if let Some(reader) = &mut self.reader {
                reader.reset(prefix)?;
            } else {
                self.reader =
                    Some(Reader::replay(self.files.clone(), prefix).with_cache(self.cache.clone()));
            }
            self.current = Some(identity);
            self.after = Some(after.to_owned());
            self.entries.pop_front();
        }
    }
}

fn parse_entry(name: &str) -> anyhow::Result<(Identity, &str)> {
    let after = name.strip_prefix("heads/").context("invalid catalog key")?;
    let key = after.strip_suffix(".json").context("invalid catalog key")?;
    let (epoch, incarnation) = key.split_once('-').context("invalid catalog identity")?;
    let epoch = u64::from_str_radix(epoch, 16)?;
    let incarnation = Uuid::parse_str(incarnation)?;
    ensure!(
        epoch > 0
            && !incarnation.is_nil()
            && key == format!("{epoch:016x}-{}", incarnation.simple()),
        "noncanonical catalog identity"
    );
    Ok((Identity { epoch, incarnation }, after))
}

/// One frame borrowed from its replay cursor. Application data is not Debug.
pub struct ReplayBatch<'a> {
    identity: Identity,
    batch: &'a Batch,
}
impl ReplayBatch<'_> {
    pub fn identity(&self) -> Identity {
        self.identity
    }
    pub fn position(&self) -> u64 {
        self.batch.prefix().position()
    }
    pub fn records(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.batch.records()
    }
}
