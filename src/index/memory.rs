//! Bounded revisioned rows for one database generation. Append only evaluated
//! records from an established durable prefix; this index is never a WAL.
use super::table::{Entry, Row};
use crate::transactions::record::Commit;
use anyhow::ensure;
use bytes::Bytes;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, btree_map},
    ops::Bound,
};

#[derive(Clone, Copy)]
pub struct Limits {
    pub versions: usize,
    /// Sum of retained key and value lengths, not allocator overhead or RSS.
    pub bytes: usize,
}
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Usage {
    pub versions: usize,
    pub bytes: usize,
}
type Key = (Bytes, Reverse<u64>);

pub struct Memory {
    rows: BTreeMap<Key, Option<Bytes>>,
    revision: u64,
    limits: Limits,
    usage: Usage,
}
impl Memory {
    pub fn new(limits: Limits) -> anyhow::Result<Self> {
        ensure!(
            limits.versions > 0 && limits.bytes > 0,
            "invalid memory index limits"
        );
        Ok(Self {
            rows: BTreeMap::new(),
            revision: 0,
            limits,
            usage: Usage::default(),
        })
    }
    pub fn usage(&self) -> Usage {
        self.usage
    }

    /// Admission precedes every allocation/mutation. Failure leaves the index
    /// unchanged. Database identity and receipt validation belong to recovery.
    pub(crate) fn append(&mut self, commit: &Commit<'_>) -> anyhow::Result<()> {
        ensure!(
            self.revision.checked_add(1) == Some(commit.revision()),
            "memory index revision gap"
        );
        let versions = self.usage.versions.checked_add(commit.mutations().len());
        let bytes = self.usage.bytes.checked_add(
            commit
                .mutations()
                .map(|m| m.key.len() + m.value.map_or(0, <[u8]>::len))
                .sum(),
        );
        ensure!(
            versions.is_some_and(|n| n <= self.limits.versions)
                && bytes.is_some_and(|n| n <= self.limits.bytes),
            "memory index capacity exhausted"
        );
        for mutation in commit.mutations() {
            self.rows.insert(
                (
                    Bytes::copy_from_slice(mutation.key.as_bytes()),
                    Reverse(commit.revision()),
                ),
                mutation.value.map(Bytes::copy_from_slice),
            );
        }
        self.usage = Usage {
            versions: versions.unwrap(),
            bytes: bytes.unwrap(),
        };
        self.revision = commit.revision();
        Ok(())
    }
    pub fn get(&self, key: &[u8], revision: u64) -> anyhow::Result<Option<Entry>> {
        super::validate_key(key)?;
        Ok(self
            .rows
            .range((Bytes::copy_from_slice(key), Reverse(revision))..)
            .next()
            .filter(|((k, _), _)| k.as_ref() == key)
            .map(|((key, Reverse(revision)), value)| Entry {
                key: key.clone(),
                revision: *revision,
                value: value.clone(),
            }))
    }
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> anyhow::Result<Cursor<'_>> {
        super::validate_range(start, end)?;
        Ok(Cursor {
            rows: self.rows.range((
                Bound::Included((Bytes::copy_from_slice(start), Reverse(revision))),
                end.map_or(Bound::Unbounded, |end| {
                    Bound::Excluded((Bytes::copy_from_slice(end), Reverse(u64::MAX)))
                }),
            )),
            revision,
            previous: None,
        })
    }
    /// All retained versions in table order. Borrowing prevents mutation during
    /// a flush; publishing the resulting reference is a separate protocol.
    pub fn rows(&self) -> impl Iterator<Item = Row<'_>> {
        self.rows
            .iter()
            .map(|((key, Reverse(revision)), value)| Row {
                key,
                revision: *revision,
                value: value.as_deref(),
            })
    }
}
pub struct Cursor<'a> {
    rows: btree_map::Range<'a, Key, Option<Bytes>>,
    revision: u64,
    previous: Option<Bytes>,
}
impl Iterator for Cursor<'_> {
    type Item = Entry;
    fn next(&mut self) -> Option<Entry> {
        if self.revision == 0 {
            return None;
        }
        for ((key, Reverse(revision)), value) in self.rows.by_ref() {
            if *revision > self.revision || self.previous.as_ref() == Some(key) {
                continue;
            }
            self.previous = Some(key.clone());
            return Some(Entry {
                key: key.clone(),
                revision: *revision,
                value: value.clone(),
            });
        }
        None
    }
}

#[cfg(test)]
mod tests;
