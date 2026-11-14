//! Object names and the small records stored under them.
//!
//! ```text
//! <root>db/<name>/c/<epoch>            claim     the writer's revocable permit
//! <root>db/<name>/m/<epoch>/<gen>      manifest  a durable cut of the state
//! <root>db/<name>/l/<epoch>/<frame>    frame     one group of transactions
//! <root>db/<name>/t/<epoch>/<id>       table     immutable sorted data
//! <root>db/<name>/m/~                  tombstone the database is deleted for good
//! ```
//!
//! An epoch is `<number>-<nonce>`, fixed width, so byte order is epoch order.
//! Every object below an epoch is written only by the process that chose its
//! nonce, which is why no write is ever conditional.
use crate::{Error, Result, table::TableMeta};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch {
    pub number: u64,
    pub nonce: u64,
}

impl Epoch {
    pub fn after(number: u64) -> Self {
        Self {
            number: number + 1,
            nonce: rand::random(),
        }
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:012x}-{:016x}", self.number, self.nonce)
    }
}

impl FromStr for Epoch {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        let (number, nonce) = s.split_once('-').ok_or(())?;
        if number.len() != 12 || nonce.len() != 16 {
            return Err(());
        }
        Ok(Self {
            number: u64::from_str_radix(number, 16).map_err(|_| ())?,
            nonce: u64::from_str_radix(nonce, 16).map_err(|_| ())?,
        })
    }
}

/// Database names become path segments and URL segments.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !name.starts_with('.')
}

#[derive(Debug, Clone)]
pub struct Layout {
    prefix: String,
}

impl Layout {
    pub fn new(root: &str, name: &str) -> Self {
        Self {
            prefix: format!("{root}db/{name}/"),
        }
    }

    pub fn databases(root: &str) -> String {
        format!("{root}db/")
    }

    pub fn everything(&self) -> &str {
        &self.prefix
    }
    pub fn claims(&self) -> String {
        format!("{}c/", self.prefix)
    }
    pub fn claim(&self, epoch: Epoch) -> String {
        format!("{}c/{epoch}", self.prefix)
    }
    pub fn manifests(&self) -> String {
        format!("{}m/", self.prefix)
    }
    pub fn epoch_manifests(&self, epoch: Epoch) -> String {
        format!("{}m/{epoch}/", self.prefix)
    }
    pub fn manifest(&self, epoch: Epoch, generation: u32) -> String {
        format!("{}m/{epoch}/{generation:08x}", self.prefix)
    }
    pub fn tombstone(&self) -> String {
        format!("{}m/~", self.prefix)
    }
    pub fn frames(&self) -> String {
        format!("{}l/", self.prefix)
    }
    pub fn epoch_frames(&self, epoch: Epoch) -> String {
        format!("{}l/{epoch}/", self.prefix)
    }
    pub fn frame(&self, epoch: Epoch, number: u64) -> String {
        format!("{}l/{epoch}/{number:012x}", self.prefix)
    }
    pub fn tables(&self) -> String {
        format!("{}t/", self.prefix)
    }
    pub fn epoch_tables(&self, epoch: Epoch) -> String {
        format!("{}t/{epoch}/", self.prefix)
    }
    pub fn table(&self, epoch: Epoch, id: u32) -> String {
        format!("{}t/{epoch}/{id:08x}", self.prefix)
    }

    /// The epoch named by a claim, manifest, frame or table key.
    pub fn epoch_of(&self, key: &str) -> Option<Epoch> {
        let rest = key.strip_prefix(&self.prefix)?.get(2..)?;
        rest.split('/').next()?.parse().ok()
    }

    pub fn generation_of(&self, key: &str) -> Option<u32> {
        u32::from_str_radix(key.rsplit('/').next()?, 16).ok()
    }

    pub fn frame_of(&self, key: &str) -> Option<u64> {
        u64::from_str_radix(key.rsplit('/').next()?, 16).ok()
    }
}

/// Per-database settings, changed only through the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbConfig {
    /// Stored bytes (tables plus unflushed writes) above which writes that add data are refused.
    pub max_bytes: u64,
    /// Tokens minted for an earlier generation are rejected.
    #[serde(default)]
    pub token_generation: u32,
    /// Requests per second allowed for this database; zero means the node default.
    #[serde(default)]
    pub max_requests_per_second: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1 << 30,
            token_generation: 0,
            max_requests_per_second: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claim {
    /// Where the claimant accepts forwarded requests, when it has peers.
    pub node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    /// The revision of the newest transaction contained in `tables`.
    pub revision: u64,
    /// Frames of this epoch up to this number are contained in `tables`.
    pub covered: u64,
    /// Set by a graceful release: no frame follows this manifest.
    pub sealed: bool,
    /// Newest first.
    pub tables: Vec<TableMeta>,
    pub config: DbConfig,
}

pub const FORMAT: u32 = 2;

impl Manifest {
    pub fn encode(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).expect("manifest encodes"))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let manifest: Self =
            serde_json::from_slice(bytes).map_err(|e| Error::Corrupt(format!("manifest: {e}")))?;
        if manifest.format != FORMAT {
            return Err(Error::Corrupt(format!(
                "manifest format {} is not supported by this release",
                manifest.format
            )));
        }
        Ok(manifest)
    }
}

/// One transaction as stored in the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameTxn {
    pub writes: Vec<(Bytes, Option<Bytes>)>,
    pub config: Option<DbConfig>,
}

/// A group of transactions made durable by one PUT. Transaction `i` has
/// revision `first_revision + i`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub first_revision: u64,
    pub txns: Vec<FrameTxn>,
}

const FRAME_MAGIC: &[u8; 4] = b"DDBF";

impl Frame {
    pub fn encode(&self) -> Bytes {
        let mut out = FRAME_MAGIC.to_vec();
        out = postcard::to_extend(self, out).expect("frame encodes");
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Bytes::from(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = || Error::Corrupt("log frame".into());
        if bytes.len() < 8 || &bytes[..4] != FRAME_MAGIC {
            return Err(corrupt());
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32fast::hash(body).to_le_bytes() != crc {
            return Err(corrupt());
        }
        postcard::from_bytes(&body[4..]).map_err(|_| corrupt())
    }
}

/// Keys beginning with this byte are internal and invisible to clients.
pub const INTERNAL: u8 = 0;

/// Where the receipt for an idempotency key lives.
pub fn receipt_key(idempotency_key: &str) -> Bytes {
    let mut key = Vec::with_capacity(3 + idempotency_key.len());
    key.extend_from_slice(&[INTERNAL, b'r', b'/']);
    key.extend_from_slice(idempotency_key.as_bytes());
    Bytes::from(key)
}

pub fn is_receipt(key: &[u8]) -> bool {
    key.starts_with(&[INTERNAL, b'r', b'/'])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_order_as_their_keys() {
        let low = Epoch {
            number: 9,
            nonce: u64::MAX,
        };
        let high = Epoch {
            number: 10,
            nonce: 0,
        };
        assert!(low < high && low.to_string() < high.to_string());
        assert_eq!(low.to_string().parse::<Epoch>(), Ok(low));
        let layout = Layout::new("root/", "app");
        assert_eq!(layout.epoch_of(&layout.claim(high)), Some(high));
        assert_eq!(layout.epoch_of(&layout.frame(low, 7)), Some(low));
        assert_eq!(layout.frame_of(&layout.frame(low, 7)), Some(7));
        assert_eq!(layout.generation_of(&layout.manifest(low, 3)), Some(3));
        assert_eq!(layout.epoch_of(&layout.tombstone()), None);
        assert!(
            layout.tombstone()
                > layout.manifest(
                    Epoch {
                        number: u64::MAX >> 16,
                        nonce: u64::MAX
                    },
                    0
                )
        );
    }

    #[test]
    fn frames_round_trip_and_reject_damage() {
        let frame = Frame {
            first_revision: 42,
            txns: vec![FrameTxn {
                writes: vec![
                    (Bytes::from_static(b"k"), Some(Bytes::from_static(b"{}"))),
                    (Bytes::from_static(b"d"), None),
                ],
                config: None,
            }],
        };
        let bytes = frame.encode();
        let decoded = Frame::decode(&bytes).unwrap();
        assert_eq!(decoded.first_revision, 42);
        assert_eq!(decoded.txns[0].writes.len(), 2);
        let mut damaged = bytes.to_vec();
        damaged[6] ^= 1;
        assert!(Frame::decode(&damaged).is_err());
    }

    #[test]
    fn names_are_path_safe() {
        assert!(valid_name("site-1.todo_app"));
        for bad in ["", ".hidden", "a/b", "a b", "ü", &"x".repeat(129)] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
