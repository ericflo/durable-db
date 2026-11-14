//! The in-memory table of writes not yet in an immutable table.
//!
//! Every key keeps its versions in ascending order. Readers see the newest
//! version at or below the confirmed revision; the single writer validates
//! against the newest version of all, confirmed or not.
use crate::table::Entry;
use bytes::Bytes;
use smallvec::SmallVec;
use std::{collections::BTreeMap, ops::Bound};

type Versions = SmallVec<[(u64, Option<Bytes>); 1]>;

#[derive(Default)]
pub struct Mem {
    map: BTreeMap<Bytes, Versions>,
    bytes: usize,
}

fn weight(key: &Bytes, value: &Option<Bytes>) -> usize {
    key.len() + value.as_ref().map_or(0, Bytes::len) + 48
}

impl Mem {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Record `key` at `version`. Versions older than the newest one at or
    /// below `confirmed` can no longer be read and are dropped.
    pub fn put(&mut self, key: Bytes, version: u64, value: Option<Bytes>, confirmed: u64) {
        self.bytes += weight(&key, &value);
        let versions = self.map.entry(key.clone()).or_default();
        let keep_from = versions
            .iter()
            .rposition(|(v, _)| *v <= confirmed)
            .unwrap_or(0);
        for (_, old) in versions.drain(..keep_from) {
            self.bytes -= weight(&key, &old);
        }
        versions.push((version, value));
    }

    /// The newest version of `key` at or below `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<(u64, Option<Bytes>)> {
        self.map
            .get(key)?
            .iter()
            .rev()
            .find(|(version, _)| *version <= snapshot)
            .cloned()
    }

    /// Entries with `lo <= key < hi` (an empty `hi` is unbounded) visible at
    /// `snapshot`, in scan order, until `live` values have been collected.
    /// Tombstones met on the way are included so they can hide older tables.
    pub fn range(
        &self,
        lo: &[u8],
        hi: &[u8],
        reverse: bool,
        snapshot: u64,
        live: usize,
    ) -> Vec<Entry> {
        let upper = if hi.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(hi)
        };
        let range = self.map.range::<[u8], _>((Bound::Included(lo), upper));
        let mut out = Vec::new();
        if live == 0 {
            return out;
        }
        let mut remaining = live;
        let mut visit = |key: &Bytes, versions: &Versions| {
            if let Some((version, value)) = versions.iter().rev().find(|(v, _)| *v <= snapshot) {
                remaining -= usize::from(value.is_some());
                out.push(Entry {
                    key: key.clone(),
                    version: *version,
                    value: value.clone(),
                });
            }
            remaining > 0
        };
        if reverse {
            for (key, versions) in range.rev() {
                if !visit(key, versions) {
                    break;
                }
            }
        } else {
            for (key, versions) in range {
                if !visit(key, versions) {
                    break;
                }
            }
        }
        out
    }

    /// Every key's newest version at or below `upto`, ascending: the content
    /// of the next table.
    pub fn snapshot(&self, upto: u64) -> Vec<Entry> {
        self.range(&[], &[], false, upto, usize::MAX)
    }

    /// Forget versions at or below `upto`; a table holds them now.
    pub fn drain(&mut self, upto: u64) {
        let mut freed = 0;
        self.map.retain(|key, versions| {
            versions.retain(|(version, value)| {
                let keep = *version > upto;
                if !keep {
                    freed += weight(key, value);
                }
                keep
            });
            !versions.is_empty()
        });
        self.bytes -= freed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn readers_see_only_their_snapshot() {
        let mut mem = Mem::default();
        mem.put(b("a"), 1, Some(b("one")), 0);
        mem.put(b("a"), 2, Some(b("two")), 1);
        mem.put(b("a"), 3, None, 1);
        assert_eq!(mem.get(b"a", 0), None);
        assert_eq!(mem.get(b"a", 1), Some((1, Some(b("one")))));
        assert_eq!(mem.get(b"a", 2), Some((2, Some(b("two")))));
        assert_eq!(mem.get(b"a", 9), Some((3, None)));
        // Confirming revision 3 lets the next write drop what nobody can read.
        mem.put(b("a"), 4, Some(b("four")), 3);
        assert_eq!(mem.get(b"a", 2), None);
        assert_eq!(mem.get(b"a", 3), Some((3, None)));
    }

    #[test]
    fn ranges_stop_after_enough_live_entries_and_keep_tombstones() {
        let mut mem = Mem::default();
        for (i, key) in ["a", "b", "c", "d", "e"].into_iter().enumerate() {
            let value = (key != "b").then(|| b(key));
            mem.put(b(key), i as u64 + 1, value, 0);
        }
        let keys = |entries: Vec<Entry>| entries.into_iter().map(|e| e.key).collect::<Vec<_>>();
        assert_eq!(
            keys(mem.range(b"a", b"", false, 9, 2)),
            [b("a"), b("b"), b("c")]
        );
        assert_eq!(
            keys(mem.range(b"b", b"e", true, 9, 9)),
            [b("d"), b("c"), b("b")]
        );
        assert_eq!(keys(mem.range(b"a", b"", false, 2, 9)), [b("a"), b("b")]);
    }

    #[test]
    fn drain_keeps_newer_versions_and_accounts_bytes() {
        let mut mem = Mem::default();
        mem.put(b("a"), 1, Some(b("x")), 0);
        mem.put(b("b"), 2, Some(b("y")), 0);
        mem.put(b("a"), 3, Some(b("z")), 0);
        assert_eq!(mem.snapshot(2).len(), 2);
        mem.drain(2);
        assert_eq!(mem.get(b"b", 9), None);
        assert_eq!(mem.get(b"a", 9), Some((3, Some(b("z")))));
        mem.drain(3);
        assert!(mem.is_empty());
        assert_eq!(mem.bytes(), 0);
    }
}
