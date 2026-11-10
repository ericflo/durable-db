//! Conditional logical files over immutable journal uploads.
//!
//! This bounded namespace is an integration component, not a
//! serving owner. Its caller must establish ownership before admission and
//! recheck it before returning reads or acknowledgements. Recovery consumes a
//! complete selected catalog; no bucket-register operation occurs here.
mod record;
use crate::storage::immutable::{Blob, REFERENCE_BYTES};

use crate::{journal, ownership::root::Replay};
use anyhow::{Context, ensure};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use uuid::Uuid;

pub const MAX_INLINE_BYTES: usize = 512 * 1024;
pub const MAX_NAME_BYTES: usize = 1024;

/// Opaque logical identity, unrelated to a provider ETag. An identical overwrite
/// gets a different version. Fresh writer incarnations prevent reuse after a cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version {
    epoch: Uuid,
    revision: u64,
}
impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:016x}", self.epoch.simple(), self.revision)
    }
}

impl std::str::FromStr for Version {
    type Err = anyhow::Error;
    fn from_str(text: &str) -> anyhow::Result<Self> {
        ensure!(text.len() == 49, "invalid logical version");
        let (epoch, revision) = text.split_once('-').context("invalid logical version")?;
        let version = Self {
            epoch: Uuid::parse_str(epoch)?,
            revision: u64::from_str_radix(revision, 16)?,
        };
        ensure!(
            !version.epoch.is_nil() && version.revision > 0 && version.to_string() == text,
            "invalid logical version"
        );
        Ok(version)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("logical file precondition failed")]
    Precondition { through: u64 },
}

#[derive(Clone, Copy)]
pub enum Condition {
    Any,
    Absent,
    Matches(Version),
}

pub struct Change<'a> {
    pub name: &'a str,
    /// None deletes the logical name. Physical journal objects are retained.
    pub value: Option<&'a [u8]>,
    pub condition: Condition,
}

/// Resident logical names and inline payload, not database-count limits. The
/// serving controller must also bound aggregate namespaces, caller-held copies,
/// and physical uploads. Blob references charge their fixed metadata, not their
/// remote payload. Checkpoints and collection are not implemented yet.
#[derive(Clone, Copy)]
pub struct Limits {
    files: usize,
    bytes: usize,
}
impl Limits {
    pub fn new(files: usize, bytes: usize) -> anyhow::Result<Self> {
        ensure!(
            (1..=1_000_000).contains(&files),
            "invalid logical file limit"
        );
        ensure!(
            (1..=1024 * 1024 * 1024).contains(&bytes),
            "invalid logical byte limit"
        );
        Ok(Self { files, bytes })
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            files: 16_384,
            bytes: 64 * 1024 * 1024,
        }
    }
}

enum Content {
    Inline(Bytes),
    External(Blob),
}
impl Content {
    fn resident_bytes(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::External(_) => REFERENCE_BYTES,
        }
    }
    fn size(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::External(blob) => blob.size(),
        }
    }
    fn inline(&self) -> Option<&[u8]> {
        match self {
            Self::Inline(bytes) => Some(bytes),
            Self::External(_) => None,
        }
    }
}
#[derive(Clone, Copy)]
enum InputContent<'a> {
    Inline(&'a [u8]),
    External(&'a Blob),
}
impl InputContent<'_> {
    fn resident_bytes(self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::External(_) => REFERENCE_BYTES,
        }
    }
    fn owned(self) -> Content {
        match self {
            Self::Inline(bytes) => Content::Inline(Bytes::copy_from_slice(bytes)),
            Self::External(blob) => Content::External(*blob),
        }
    }
}
struct Input<'a> {
    name: &'a str,
    value: Option<InputContent<'a>>,
    condition: Condition,
}

pub struct File {
    version: Version,
    content: Content,
}
impl File {
    pub fn version(&self) -> Version {
        self.version
    }
    pub fn size(&self) -> u64 {
        self.content.size()
    }
    pub fn bytes(&self) -> Option<&[u8]> {
        self.content.inline()
    }
    pub fn blob(&self) -> Option<&Blob> {
        match &self.content {
            Content::External(blob) => Some(blob),
            Content::Inline(_) => None,
        }
    }
}

/// Complete private projection. It cannot be constructed from an advanced
/// replay cursor or exposed during restoration.
pub struct View {
    files: BTreeMap<String, File>,
    revision: u64,
    bytes: usize,
    limits: Limits,
}
impl View {
    pub fn empty(limits: Limits) -> Self {
        Self {
            files: BTreeMap::new(),
            revision: 0,
            bytes: 0,
            limits,
        }
    }
    pub fn get(&self, name: &str) -> Option<&File> {
        self.files.get(name)
    }
    /// Borrowed ordered results; no allocation proportional to the namespace.
    pub fn list<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a str, &'a File)> {
        self.files
            .range::<str, _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            ))
            .take_while(move |(name, _)| name.starts_with(prefix))
            .map(|(name, file)| (name.as_str(), file))
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn resident_bytes(&self) -> usize {
        self.bytes
    }
    pub fn len(&self) -> usize {
        self.files.len()
    }
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn apply(&mut self, record: record::Record) -> anyhow::Result<()> {
        ensure!(
            self.revision.checked_add(1) == Some(record.version.revision),
            "logical revision gap or repetition"
        );
        let old = self.files.get(&record.name);
        ensure!(
            old.map(|f| f.version) == record.expected,
            "logical file predecessor mismatch"
        );
        let (files, bytes) = usage(
            self.files.len(),
            self.bytes,
            &record.name,
            old.map(|f| f.content.resident_bytes()),
            record.value.as_ref().map(Content::resident_bytes),
        )?;
        check_usage(self.limits, files, bytes)?;
        self.revision = record.version.revision;
        self.bytes = bytes;
        if let Some(content) = record.value {
            // Record buffers never stay pinned by a tiny surviving value.
            self.files.insert(
                record.name,
                File {
                    version: record.version,
                    content,
                },
            );
        } else {
            self.files.remove(&record.name);
        }
        Ok(())
    }
}

pub async fn restore(mut replay: Replay, limits: Limits) -> anyhow::Result<View> {
    ensure!(
        replay.at_start(),
        "logical file recovery requires an unread catalog cursor"
    );
    let mut view = View::empty(limits);
    while let Some(batch) = replay.read_next().await? {
        for bytes in batch.records() {
            let record = record::Record::decode(bytes)?;
            ensure!(
                record.version.epoch == batch.identity().incarnation,
                "logical record belongs to another journal incarnation"
            );
            view.apply(record)?;
        }
    }
    ensure!(replay.completed(), "logical file recovery did not complete");
    Ok(view)
}

struct Pending {
    position: u64,
    records: Vec<record::Record>,
}

pub struct Namespace {
    writer: journal::Writer,
    view: View,
    pending: VecDeque<Pending>,
    accepted_revision: u64,
    accepted_files: usize,
    accepted_bytes: usize,
    failed: bool,
}

impl Namespace {
    pub fn new(view: View, writer: journal::Writer) -> anyhow::Result<Self> {
        ensure!(
            writer.pending_batches() == 0 && writer.durable_prefix().position() == 0,
            "logical namespace needs a fresh journal writer"
        );
        Ok(Self {
            accepted_revision: view.revision,
            accepted_files: view.len(),
            accepted_bytes: view.bytes,
            view,
            writer,
            pending: VecDeque::new(),
            failed: false,
        })
    }
    /// Previously published complete prefix, never speculative accepted changes.
    pub fn view(&self) -> anyhow::Result<&View> {
        ensure!(
            !self.failed,
            "logical namespace retired after an internal failure"
        );
        Ok(&self.view)
    }
    pub fn durable_prefix(&self) -> journal::Prefix {
        self.writer.durable_prefix()
    }
    pub fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    fn accepted(&self, name: &str) -> Option<(Version, usize)> {
        for pending in self.pending.iter().rev() {
            if let Some(record) = pending.records.iter().find(|r| r.name == name) {
                return record
                    .value
                    .as_ref()
                    .map(|v| (record.version, v.resident_bytes()));
            }
        }
        self.view
            .get(name)
            .map(|f| (f.version, f.content.resident_bytes()))
    }

    /// Atomically admit distinct names against *all accepted predecessors*.
    /// A precondition error carries the predecessor position: the caller must
    /// wait for that prefix before reporting a definite conditional conflict.
    /// Otherwise a failed comparison could expose a speculative write.
    /// The returned versions are provisional until the entire batch is durable
    /// and the caller has checked its current serving permission.
    pub fn prepare(&mut self, changes: &[Change<'_>]) -> anyhow::Result<Prepared> {
        self.prepare_inputs(changes.iter().map(|change| Input {
            name: change.name,
            value: change.value.map(InputContent::Inline),
            condition: change.condition,
        }))
    }

    /// Publish one known-successful immutable upload through the same ordered
    /// journal. The upload may become an orphan if admission fails; never refund
    /// its physical allocation merely because the logical condition failed.
    pub fn prepare_reference(
        &mut self,
        name: &str,
        blob: &Blob,
        condition: Condition,
    ) -> anyhow::Result<Prepared> {
        self.prepare_inputs(std::iter::once(Input {
            name,
            value: Some(InputContent::External(blob)),
            condition,
        }))
    }

    fn prepare_inputs<'a>(
        &mut self,
        changes: impl ExactSizeIterator<Item = Input<'a>>,
    ) -> anyhow::Result<Prepared> {
        ensure!(!self.failed, "logical namespace is retired");
        ensure!(
            changes.len() > 0 && changes.len() <= journal::MAX_RECORDS,
            "invalid logical batch size"
        );
        let mut names = BTreeSet::new();
        let mut records = Vec::with_capacity(changes.len());
        let mut encoded = Vec::with_capacity(changes.len());
        let mut files = self.accepted_files;
        let mut bytes = self.accepted_bytes;
        let mut revision = self.accepted_revision;
        let mut encoded_bytes = 0usize;
        for change in changes {
            record::validate(
                change.name,
                match change.value {
                    Some(InputContent::Inline(bytes)) => Some(bytes),
                    _ => None,
                },
            )?;
            ensure!(names.insert(change.name), "duplicate logical name in batch");
            let old = self.accepted(change.name);
            let expected = old.map(|(version, _)| version);
            let matches = match change.condition {
                Condition::Any => true,
                Condition::Absent => expected.is_none(),
                Condition::Matches(version) => expected == Some(version),
            };
            if !matches {
                return Err(Error::Precondition {
                    through: self
                        .pending
                        .back()
                        .map_or(self.writer.durable_prefix().position(), |p| p.position),
                }
                .into());
            }
            (files, bytes) = usage(
                files,
                bytes,
                change.name,
                old.map(|(_, len)| len),
                change.value.map(InputContent::resident_bytes),
            )?;
            check_usage(self.view.limits, files, bytes)?;
            revision = revision
                .checked_add(1)
                .context("logical revision exhausted")?;
            let record = record::Record {
                name: change.name.into(),
                value: change.value.map(InputContent::owned),
                expected,
                version: Version {
                    epoch: self.writer.log().epoch(),
                    revision,
                },
            };
            let wire = record.encode()?;
            encoded_bytes = encoded_bytes
                .checked_add(wire.len() + 4)
                .context("logical batch size overflow")?;
            ensure!(
                encoded_bytes <= journal::MAX_BATCH_BYTES - 120,
                "logical batch exceeds journal bound"
            );
            encoded.push(wire);
            records.push(record);
        }
        let upload = self.writer.prepare(&encoded)?;
        let versions = records.iter().map(|r| r.version).collect();
        self.pending.push_back(Pending {
            position: upload.position(),
            records,
        });
        self.accepted_revision = revision;
        self.accepted_files = files;
        self.accepted_bytes = bytes;
        Ok(Prepared { upload, versions })
    }

    pub fn complete(&mut self, completion: journal::Completion) -> anyhow::Result<()> {
        self.writer.complete(completion)?;
        self.publish()
    }
    /// Absence never undoes an uncertain operation. No retry PUT is sent.
    pub async fn resolve(&mut self, position: u64) -> anyhow::Result<journal::Resolution> {
        ensure!(!self.failed, "logical namespace is retired");
        let result = self.writer.resolve(position).await?;
        self.publish()?;
        Ok(result)
    }
    fn publish(&mut self) -> anyhow::Result<()> {
        ensure!(!self.failed, "logical namespace is retired");
        // Retire before consuming: any unexpected mismatch must not expose a
        // partially applied batch. This synchronous section has no cancel point.
        self.failed = true;
        while let Some(batch) = self.writer.pop_durable()? {
            let pending = self
                .pending
                .pop_front()
                .context("logical pending batch missing")?;
            ensure!(
                pending.position == batch.prefix().position(),
                "logical batch order mismatch"
            );
            for record in pending.records {
                self.view.apply(record)?;
            }
        }
        self.failed = false;
        Ok(())
    }
}

pub struct Prepared {
    upload: journal::PreparedUpload,
    versions: Vec<Version>,
}
impl Prepared {
    pub fn position(&self) -> u64 {
        self.upload.position()
    }
    pub fn versions(&self) -> &[Version] {
        &self.versions
    }
    pub async fn send(self) -> journal::Completion {
        self.upload.send().await
    }
}

fn usage(
    files: usize,
    bytes: usize,
    name: &str,
    old: Option<usize>,
    new: Option<usize>,
) -> anyhow::Result<(usize, usize)> {
    let files = files
        .checked_sub(usize::from(old.is_some()))
        .and_then(|n| n.checked_add(usize::from(new.is_some())))
        .context("logical file count overflow")?;
    let bytes = bytes
        .checked_sub(old.map_or(0, |n| n + name.len()))
        .and_then(|n| n.checked_add(new.map_or(0, |v| v + name.len())))
        .context("logical byte count overflow")?;
    Ok((files, bytes))
}
fn check_usage(limits: Limits, files: usize, bytes: usize) -> anyhow::Result<()> {
    ensure!(
        files <= limits.files && bytes <= limits.bytes,
        "logical namespace resident limit exceeded"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
