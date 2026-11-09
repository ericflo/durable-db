//! Bounded conflict tracking for one database and one serving incarnation.
//!
//! A snapshot belongs to a committed view. Validate against *all* accepted
//! predecessors, then record every mutated key, including deletes. Key ranges
//! include absent keys, so an insertion into an empty scan conflicts too.
//!
//! The owner must serialize validation with transaction evaluation, quota and
//! journal reservation, and advancement of the committed value view. A returned
//! revision means accepted, not durable or acknowledged. Discard this validator
//! on uncertain recovery; a new instance invalidates all old snapshot tokens.
use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound,
    sync::Arc,
};
use uuid::Uuid;

const MAX_TERMS: usize = 128;
const MAX_KEY_BYTES: usize = 1024;
const MAX_RANGE_STEPS: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub transactions: usize,
    pub key_references: usize,
    /// Sum of key lengths across retained transactions, counting repetitions.
    pub key_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            transactions: 4096,
            key_references: 65_536,
            key_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid validation limits, key, range, or revision")]
    Invalid,
    #[error("transaction exceeds validation limits")]
    TooLarge,
    #[error("snapshot expired; obtain a new consistent snapshot")]
    SnapshotExpired,
    #[error("a read key, range, or database revision changed")]
    Conflict,
    #[error("pending transactions exhaust validation capacity")]
    Busy,
    #[error("validation work budget exceeded; obtain a fresh snapshot")]
    WorkLimit,
    #[error("database revision exhausted")]
    RevisionExhausted,
}

/// Internal token, not an authenticated wire representation. A controller must
/// bind any external token to the tenant/database and consistent value view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    incarnation: Uuid,
    revision: u64,
}

impl Snapshot {
    pub fn revision(self) -> u64 {
        self.revision
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Read<'a> {
    Key(&'a str),
    Range {
        start: Bound<&'a str>,
        end: Bound<&'a str>,
    },
    /// Retains the existing whole-database revision condition, including when
    /// an intervening transaction changes only its receipt and no user key.
    Database,
}

struct Changes {
    revision: u64,
    keys: Vec<Arc<str>>,
    bytes: usize,
}

pub struct Validator {
    incarnation: Uuid,
    accepted: u64,
    durable: u64,
    floor: u64,
    limits: Limits,
    keys: usize,
    bytes: usize,
    changes: VecDeque<Changes>,
    latest: BTreeMap<Arc<str>, u64>,
}

impl Validator {
    pub fn new(recovered_revision: u64, limits: Limits) -> Result<Self, Error> {
        if limits.transactions == 0
            || limits.transactions > 4096
            || limits.key_references == 0
            || limits.key_references > 262_144
            || limits.key_bytes == 0
            || limits.key_bytes > 16 * 1024 * 1024
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            incarnation: Uuid::new_v4(),
            accepted: recovered_revision,
            durable: recovered_revision,
            floor: recovered_revision,
            limits,
            keys: 0,
            bytes: 0,
            changes: VecDeque::new(),
            latest: BTreeMap::new(),
        })
    }

    /// Capture this token together with its matching durable value snapshot.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            incarnation: self.incarnation,
            revision: self.durable,
        }
    }

    /// Call only after the journal establishes contiguous durability and the
    /// owner installs that same committed value view. This grants no permission
    /// to serve or acknowledge; the ownership protocol supplies that separately.
    pub fn publish_through(&mut self, revision: u64) -> Result<(), Error> {
        if revision < self.durable || revision > self.accepted {
            return Err(Error::Invalid);
        }
        self.durable = revision;
        Ok(())
    }

    /// Validate and reserve conflict metadata atomically. On any error the
    /// validator is unchanged by this invocation; the error says nothing about
    /// an earlier request's outcome. Resolve receipts before validation, and
    /// never treat snapshot expiry as proof that an uncertain write failed.
    /// Every mutation key, including deletion and
    /// no-op writes, must be supplied. Fallible journal/tenant admission belongs
    /// before this call; an accepted revision must never be reused or rolled back.
    pub fn accept(
        &mut self,
        snapshot: Snapshot,
        reads: &[Read<'_>],
        writes: &[&str],
    ) -> Result<u64, Error> {
        validate_inputs(reads, writes)?;
        if snapshot.incarnation != self.incarnation || snapshot.revision < self.floor {
            return Err(Error::SnapshotExpired);
        }
        if snapshot.revision > self.durable {
            return Err(Error::Invalid);
        }
        let revision = self
            .accepted
            .checked_add(1)
            .ok_or(Error::RevisionExhausted)?;
        // This allocation is bounded by MAX_TERMS, before copying key bytes.
        let mut keys = writes.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let bytes: usize = keys.iter().map(|key| key.len()).sum();
        if keys.len() > self.limits.key_references || bytes > self.limits.key_bytes {
            return Err(Error::TooLarge);
        }
        let mut evictions = 0;
        let mut retained_keys = self.keys;
        let mut retained_bytes = self.bytes;
        let mut floor = self.floor;
        while self.changes.len() - evictions >= self.limits.transactions
            || retained_keys + keys.len() > self.limits.key_references
            || retained_bytes + bytes > self.limits.key_bytes
        {
            let change = &self.changes[evictions];
            if change.revision > self.durable {
                return Err(Error::Busy);
            }
            floor = change.revision;
            retained_keys -= change.keys.len();
            retained_bytes -= change.bytes;
            evictions += 1;
        }
        if snapshot.revision < floor {
            return Err(Error::SnapshotExpired);
        }
        let mut range_steps = 0;
        for read in reads {
            let conflict = match *read {
                Read::Key(key) => self.latest.get(key).is_some_and(|v| *v > snapshot.revision),
                Read::Range { start, end }
                    if snapshot.revision != self.accepted && !empty_range(start, end) =>
                {
                    let mut changed = false;
                    for (_, version) in self.latest.range::<str, _>((start, end)) {
                        if range_steps == MAX_RANGE_STEPS {
                            return Err(Error::WorkLimit);
                        }
                        range_steps += 1;
                        if *version > snapshot.revision {
                            changed = true;
                            break;
                        }
                    }
                    changed
                }
                Read::Range { .. } => false,
                Read::Database => snapshot.revision != self.accepted,
            };
            if conflict {
                return Err(Error::Conflict);
            }
        }
        for _ in 0..evictions {
            let change = self.changes.pop_front().expect("planned eviction");
            for key in change.keys {
                if self.latest.get(&key) == Some(&change.revision) {
                    self.latest.remove(&key);
                }
            }
        }
        let keys: Vec<Arc<str>> = keys
            .into_iter()
            .map(|key| {
                let key = self
                    .latest
                    .get_key_value(key)
                    .map(|(key, _)| key.clone())
                    .unwrap_or_else(|| Arc::from(key));
                self.latest.insert(key.clone(), revision);
                key
            })
            .collect();
        self.keys = retained_keys + keys.len();
        self.bytes = retained_bytes + bytes;
        self.floor = floor;
        self.accepted = revision;
        self.changes.push_back(Changes {
            revision,
            keys,
            bytes,
        });
        Ok(revision)
    }
}

fn validate_key(key: &str) -> Result<(), Error> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(Error::Invalid);
    }
    Ok(())
}

fn endpoints<'a>(start: Bound<&'a str>, end: Bound<&'a str>) -> Option<(&'a str, &'a str)> {
    match (start, end) {
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            Some((a, b))
        }
        _ => None,
    }
}

fn empty_range(start: Bound<&str>, end: Bound<&str>) -> bool {
    endpoints(start, end).is_some_and(|(a, b)| {
        a == b && (matches!(start, Bound::Excluded(_)) || matches!(end, Bound::Excluded(_)))
    })
}

fn validate_inputs(reads: &[Read<'_>], writes: &[&str]) -> Result<(), Error> {
    if reads.len() > MAX_TERMS || writes.len() > MAX_TERMS {
        return Err(Error::TooLarge);
    }
    for key in writes {
        validate_key(key)?;
    }
    for read in reads {
        match *read {
            Read::Key(key) => validate_key(key)?,
            Read::Range { start, end } => {
                for bound in [start, end] {
                    if let Bound::Included(key) | Bound::Excluded(key) = bound {
                        validate_key(key)?;
                    }
                }
                if endpoints(start, end).is_some_and(|(a, b)| a > b) {
                    return Err(Error::Invalid);
                }
            }
            Read::Database => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
