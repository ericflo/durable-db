//! Immutable root/journal identities and a durable predecessor chain.
use super::{Encoding, Identity};
use crate::{journal::LogId, storage::Storage};
use anyhow::{Context, ensure};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"DDBREG01";
const FIXED_BYTES: usize = 106;
const MAX_BYTES: usize = FIXED_BYTES + 128;

/// A durable identity, not a selected journal prefix or permission to serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Registration {
    identity: Identity,
    parent: Option<Identity>,
}

impl Registration {
    pub fn identity(self) -> Identity {
        self.identity
    }
    pub fn parent(self) -> Option<Identity> {
        self.parent
    }
    pub fn journal(self) -> LogId {
        LogId::root(self.identity.incarnation)
    }

    pub(super) fn new(identity: Identity, parent: Option<Identity>) -> Self {
        Self { identity, parent }
    }

    fn encode(self, prefix: &str, encoding: Encoding) -> Bytes {
        let mut bytes = Vec::with_capacity(MAX_BYTES);
        bytes.extend_from_slice(MAGIC);
        for identity in [
            self.identity,
            self.parent.unwrap_or(Identity {
                epoch: 0,
                incarnation: Uuid::nil(),
            }),
        ] {
            bytes.extend_from_slice(&identity.epoch.to_be_bytes());
            bytes.extend_from_slice(identity.incarnation.as_bytes());
        }
        // Preserve released local registration bytes; remote values are reserved zeros.
        bytes.extend_from_slice(&encoding.legacy_lifetime().to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(&(prefix.len() as u16).to_be_bytes());
        bytes.extend_from_slice(prefix.as_bytes());
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        bytes.into()
    }

    fn decode(
        bytes: &[u8],
        expected: Uuid,
        prefix: &str,
        encoding: Encoding,
    ) -> anyhow::Result<Self> {
        ensure!(
            (FIXED_BYTES + 1..=MAX_BYTES).contains(&bytes.len()),
            "invalid registration length"
        );
        let (body, checksum) = bytes.split_at(bytes.len() - 32);
        ensure!(
            Sha256::digest(body).as_slice() == checksum,
            "registration checksum mismatch"
        );
        let mut input = body;
        ensure!(
            &take::<8>(&mut input)? == MAGIC,
            "unknown registration format"
        );
        let identity = read_identity(&mut input)?;
        let parent = read_identity(&mut input)?;
        ensure!(
            identity.incarnation == expected && identity.epoch > 0 && !expected.is_nil(),
            "registration identity mismatch"
        );
        let parent = if identity.epoch == 1 {
            ensure!(
                parent.epoch == 0 && parent.incarnation.is_nil(),
                "first registration has a predecessor"
            );
            None
        } else {
            ensure!(
                parent.epoch == identity.epoch - 1
                    && !parent.incarnation.is_nil()
                    && parent.incarnation != identity.incarnation,
                "invalid registration predecessor"
            );
            Some(parent)
        };
        let lifetime = u64::from_be_bytes(take(&mut input)?);
        let error = u64::from_be_bytes(take(&mut input)?);
        ensure!(
            lifetime == encoding.legacy_lifetime() && error == 0,
            "registration storage encoding mismatch"
        );
        let length = usize::from(u16::from_be_bytes(take(&mut input)?));
        ensure!(
            length == input.len() && input == prefix.as_bytes(),
            "registration namespace mismatch"
        );
        Ok(Self { identity, parent })
    }

    pub(super) async fn load(
        files: &Arc<dyn Storage>,
        incarnation: Uuid,
        prefix: &str,
        encoding: Encoding,
    ) -> anyhow::Result<Self> {
        let object = files
            .get(&key(incarnation), MAX_BYTES)
            .await?
            .context("registered root is missing")?;
        Self::decode(&object.bytes, incarnation, prefix, encoding)
    }

    pub(super) fn prepare(
        self,
        files: Arc<dyn Storage>,
        prefix: &str,
        encoding: Encoding,
    ) -> Prepared {
        Prepared {
            files,
            key: key(self.identity.incarnation),
            bytes: self.encode(prefix, encoding),
        }
    }
}

fn key(incarnation: Uuid) -> String {
    format!("ownership/{}", incarnation.simple())
}
fn take<const N: usize>(input: &mut &[u8]) -> anyhow::Result<[u8; N]> {
    let (value, rest) = input
        .split_first_chunk::<N>()
        .context("truncated registration")?;
    *input = rest;
    Ok(*value)
}
fn read_identity(input: &mut &[u8]) -> anyhow::Result<Identity> {
    Ok(Identity {
        epoch: u64::from_be_bytes(take(input)?),
        incarnation: Uuid::from_bytes(take(input)?),
    })
}

/// One physical PUT. An uncertain reply may be resolved by a positive exact
/// read; an absent/failed read does not authorize root publication or a refund.
pub(super) struct Prepared {
    files: Arc<dyn Storage>,
    key: String,
    bytes: Bytes,
}
impl Prepared {
    pub async fn execute(self) -> anyhow::Result<()> {
        if let Err(error) = self.files.put(&self.key, self.bytes.clone()).await {
            match self.files.get(&self.key, MAX_BYTES).await {
                Ok(Some(object)) if object.bytes == self.bytes => {}
                _ => {
                    return Err(error.context(
                        "root registration outcome unresolved; publication not attempted",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Bounded-memory traversal using only durable registrations. Missing or corrupt
/// objects are errors, never an end-of-history marker. Errors do not advance it.
pub struct Registrations {
    files: Arc<dyn Storage>,
    prefix: String,
    encoding: Encoding,
    next: Option<Identity>,
}
impl Registrations {
    pub(super) fn new(
        files: Arc<dyn Storage>,
        prefix: String,
        encoding: Encoding,
        identity: Identity,
    ) -> Self {
        Self {
            files,
            prefix,
            encoding,
            next: Some(identity),
        }
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Registration>> {
        let Some(identity) = self.next else {
            return Ok(None);
        };
        let registration = Registration::load(
            &self.files,
            identity.incarnation,
            &self.prefix,
            self.encoding,
        )
        .await?;
        ensure!(
            registration.identity == identity,
            "registration epoch mismatch"
        );
        self.next = registration.parent;
        Ok(Some(registration))
    }
}
