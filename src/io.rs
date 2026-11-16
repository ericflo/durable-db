//! Storage access shared by every open database: the object store, bounded
//! retries, and two caches of immutable table bytes.
//!
//! Memory holds decoded blocks. A directory, when configured, holds whole
//! table objects, so a restart or a cold block costs a local read instead of
//! a request to the bucket. Both are bounded and both are disposable: every
//! block carries a checksum, a damaged cache file is deleted and fetched
//! again, and nothing here is ever the only copy of anything.
use crate::{
    Error, Result,
    store::{ObjectStore, StoreError, StoreResult},
    table::{Block, Index},
};
use bytes::Bytes;
use parking_lot::Mutex;
use quick_cache::{DefaultHashBuilder, Lifecycle, OptionsBuilder, Weighter, sync::Cache};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// What the memory cache holds of a table: its blocks, and its index under
/// a block number no block has.
#[derive(Clone)]
enum Part {
    Block(Arc<Block>),
    Index(Arc<Index>),
}

const INDEX: u32 = u32::MAX;

#[derive(Clone)]
struct PartWeight;

impl Weighter<(Arc<str>, u32), Part> for PartWeight {
    fn weight(&self, key: &(Arc<str>, u32), part: &Part) -> u64 {
        let held = match part {
            Part::Block(block) => block.weight(),
            Part::Index(index) => index.weight(),
        };
        (key.0.len() + held) as u64
    }
}

/// A table's index stays in memory, whatever blocks want its room, for this
/// long after a read last asked for it.
const INDEX_IDLE: Duration = Duration::from_secs(60);

/// Keeps the indexes of tables that are being read in the memory cache.
///
/// The cache puts out what was asked for least recently, and counted an
/// index as it counts a block. But every read of a table needs its index
/// and one block of very many, and an index that is put out costs the end
/// of its table, 256 KiB, from the bucket where a block costs 32 KiB. A
/// database killed with 27 runs had each read ask for a block of each: so
/// many passed through the cache that it held two thirds of the indexes at
/// a time, and a third of all that was read from storage was indexes read
/// again. So an index that a read asked for within `INDEX_IDLE` is not put
/// out, while all the indexes in memory are within half the cache: with
/// more than that they go as blocks do.
///
/// The cache is of parts, each with its share of the bytes, and what is
/// held in a part stays there when the part is full: blocks then make way
/// for one another around it. So an index is held only when sixteen of
/// its size fit in a part, and a part is `CACHE_PART` at least when the
/// cache is. A part passes its share only when the indexes held in it
/// alone come to more than that, which with indexes of 52 KiB and parts
/// of 12 MiB takes 230 of them in one part.
#[derive(Clone)]
struct Pins {
    /// Bytes of all indexes in memory, and how many may be held to.
    indexes: Arc<AtomicU64>,
    most: u64,
    /// The largest index that is held, once the cache is made.
    largest: Arc<AtomicU64>,
    started: Instant,
    idle: Duration,
}

/// The least that a part of the memory cache holds, of a cache that large.
const CACHE_PART: u64 = 4 << 20;

impl Pins {
    fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

impl Lifecycle<(Arc<str>, u32), Part> for Pins {
    type RequestState = ();

    fn begin_request(&self) {}

    fn is_pinned(&self, _: &(Arc<str>, u32), part: &Part) -> bool {
        match part {
            Part::Index(index) => {
                index.weight() as u64 <= self.largest.load(Ordering::Relaxed)
                    && self.indexes.load(Ordering::Relaxed) <= self.most
                    && self.now().saturating_sub(index.used()) < self.idle.as_millis() as u64
            }
            Part::Block(_) => false,
        }
    }
}

/// Tables larger than this are read from the bucket by range only.
const DISK_OBJECT_BYTES: u64 = 64 << 20;

/// What may be written to the cache directory and not yet be on the disk,
/// beside the table being written, and in how many tables at most, before
/// the disk is asked for it. A request's own table does not have the disk
/// asked until there is twice as much.
const UNSYNCED_BYTES: u64 = 32 << 20;
const UNSYNCED_TABLES: usize = 64;

/// A table is in memory while it is fetched for the disk cache, so only
/// this many are fetched at a time.
const FILLS: usize = 2;

/// The disk cache holds no more tables than 64 and one for each of these
/// in its budget, and never more than `DISK_TABLES`: it remembers some 150
/// bytes of each, which is then 40 MiB at most however large the cache.
const DISK_TABLE_BYTES: u64 = 32 << 10;
const DISK_TABLES: usize = 1 << 18;

/// How many scans of requests read ahead of themselves at a time, and in
/// how many runs of tables merges do. What a scan read ahead is in memory,
/// a megabyte of it at most.
pub const AHEAD_SCANS: usize = 16;
pub const AHEAD_MERGES: usize = 16;

/// A table that no read has asked for in this long gives way to one that
/// reads had to go to the bucket for.
const IDLE: Duration = Duration::from_secs(300);

/// A table that is not in the disk cache is fetched for it once the reads
/// that missed it have had one part in this many of it from the bucket. So
/// what is fetched is never more than this many times what was read.
const FETCH_PART: u64 = 16;

/// How many tables the disk cache counts such reads for. With more, it
/// begins the count again.
const MISSED_TABLES: usize = 1024;

/// How many tables that have left their manifests are remembered, so that
/// none of them is fetched: 3 MiB of names.
const GONE_TABLES: usize = 65_536;

/// A table's name in the cache directory.
type Name = [u8; 16];

/// What the disk cache holds.
#[derive(Default)]
struct Held {
    /// Each table's size, its place in `order`, and when this server last
    /// read or wrote it: never, for one an earlier server left.
    tables: HashMap<Name, (u64, u64, Option<Instant>)>,
    /// The tables by their last use, the least recent first.
    order: BTreeMap<u64, Name>,
    next: u64,
    /// Bytes of the tables.
    used: u64,
    /// Room promised to tables that are being fetched: how many, and
    /// their bytes.
    promises: usize,
    promised: u64,
    /// Bytes that reads have had from the bucket of tables that are not
    /// here, since each was last here.
    missed: HashMap<Name, u64>,
    /// Tables that no manifest lists any more, and the order they left
    /// in: the first to leave is the first forgotten.
    gone: HashSet<Name>,
    left: VecDeque<Name>,
}

impl Held {
    fn insert(&mut self, name: Name, size: u64, at: Option<Instant>) {
        self.tables.insert(name, (size, self.next, at));
        self.order.insert(self.next, name);
        self.next += 1;
        self.used += size;
    }

    fn remove(&mut self, name: &Name) -> bool {
        let Some((size, place, _)) = self.tables.remove(name) else {
            return false;
        };
        self.order.remove(&place);
        self.used -= size;
        true
    }

    /// Note that tables are in use. Returns the bytes of those that are
    /// here, and how many they are: after this they are the most recently
    /// used.
    fn keep(&mut self, names: &[Name]) -> (u64, usize) {
        let mut kept = (0, 0);
        for name in names {
            if self.touch(name) {
                kept.0 += self.tables[name].0;
                kept.1 += 1;
            }
        }
        kept
    }

    /// Note that a table is in use. False when it is not here.
    fn touch(&mut self, name: &Name) -> bool {
        let Some((_, place, at)) = self.tables.get_mut(name) else {
            return false;
        };
        self.order.remove(place);
        *place = self.next;
        *at = Some(Instant::now());
        self.order.insert(self.next, *name);
        self.next += 1;
        true
    }
}

/// Whole tables in a directory, the least recently used put out first.
///
/// A table this server wrote is taken, since it cost nothing to have,
/// and the least recently used make way. While a merge runs, the tables
/// it has read through make way before any that is in use, since they
/// leave with the merge's manifest, and those it has yet to read and
/// those it has stored never do. One that reads missed is fetched once
/// they have read a sixteenth
/// of it from the bucket, and only into room that is free, or that tables
/// no read has asked for in five minutes leave: it never puts out a table
/// that is in use. When a server reads more than its cache holds, the
/// tables that were here first stay and the others are read from the
/// bucket a block at a time. Fetching each one whole at the first read
/// that missed it, in place of another that was as much in use, had a
/// server with a 1 GiB cache and a 4 GiB database fetch half a gigabyte a
/// second from its store (finding 43).
pub struct DiskCache {
    dir: PathBuf,
    budget: u64,
    idle: Duration,
    counter: AtomicU64,
    held: Mutex<Held>,
    /// Objects being downloaded in the background.
    filling: Mutex<HashSet<String>>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Tables fetched whole, for reads of parts of them that missed.
    pub fetched: AtomicU64,
    /// Tables written here that the disk has not been asked for yet.
    unsynced: Mutex<Unsynced>,
    /// Held while the disk is asked for what was written: one at a time.
    syncing: Mutex<()>,
    /// How many bytes of tables it has been asked for.
    synced: AtomicU64,
}

/// Files of tables written to the cache directory, open, and their bytes.
#[derive(Default)]
struct Unsynced {
    files: Vec<(Name, fs::File, u64)>,
    bytes: u64,
}

impl Unsynced {
    fn over(&self, times: u64) -> bool {
        self.bytes > UNSYNCED_BYTES * times
            || self.files.len() as u64 > UNSYNCED_TABLES as u64 * times
    }
}

impl DiskCache {
    pub fn open(dir: impl Into<PathBuf>, budget: u64) -> io::Result<Self> {
        Self::open_idle(dir, budget, IDLE)
    }

    /// As `open`, with how long a table goes unread before one that a read
    /// missed may take its place.
    pub fn open_idle(dir: impl Into<PathBuf>, budget: u64, idle: Duration) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut found = Vec::new();
        for entry in fs::read_dir(&dir)?.flatten() {
            let file = entry.file_name();
            let file = file.to_string_lossy();
            let mut name = Name::default();
            if file.starts_with("tmp-") {
                // Half-written, from an earlier run.
                let _ = fs::remove_file(entry.path());
            } else if hex::decode_to_slice(file.as_bytes(), &mut name).is_ok()
                && let Ok(meta) = entry.metadata()
            {
                found.push((meta.modified().ok(), name, meta.len()));
            }
        }
        // The order they were written in stands for the order of their use.
        found.sort();
        let cache = Self {
            dir,
            budget,
            idle,
            counter: AtomicU64::new(0),
            held: Mutex::default(),
            filling: Mutex::default(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetched: AtomicU64::new(0),
            unsynced: Mutex::default(),
            syncing: Mutex::new(()),
            synced: AtomicU64::new(0),
        };
        let mut out = Vec::new();
        {
            let mut held = cache.held.lock();
            for (_, name, size) in found {
                held.insert(name, size, None);
            }
            // The budget may be smaller than it was.
            cache.make_room(&mut held, 0, (&[], (0, 0)), &mut out);
        }
        cache.unlink(&out);
        Ok(cache)
    }

    pub fn used(&self) -> u64 {
        self.held.lock().used
    }

    fn name(key: &str) -> Name {
        let mut name = Name::default();
        name.copy_from_slice(&Sha256::digest(key.as_bytes())[..16]);
        name
    }

    fn path(&self, name: &Name) -> PathBuf {
        self.dir.join(hex::encode(name))
    }

    fn holds(&self, name: &Name) -> bool {
        self.held.lock().tables.contains_key(name)
    }

    /// Whether `size` more bytes, in `tables` more tables, are more than
    /// the cache may hold.
    fn over(&self, held: &Held, size: u64, tables: usize) -> bool {
        held.used + held.promised + size > self.budget
            || held.tables.len() + held.promises + tables > Self::most(self.budget)
    }

    /// How many tables a cache of `budget` bytes holds at most.
    fn most(budget: u64) -> usize {
        ((budget / DISK_TABLE_BYTES) as usize + 64).min(DISK_TABLES)
    }

    /// The least recently used table, if no read has asked for it in so
    /// long that it gives way to one that reads want. One that an earlier
    /// server left has not been asked for.
    fn unread(&self, held: &Held) -> Option<Name> {
        let name = held.order.values().next()?;
        let (_, _, at) = held.tables[name];
        at.is_none_or(|at| at.elapsed() >= self.idle)
            .then_some(*name)
    }

    /// Note that a read had `read` bytes from the bucket of a table of
    /// `size` bytes that is not here. True when the table is to be fetched:
    /// such reads have had enough of it, and there is room.
    fn wanted(&self, name: &Name, size: u64, read: u64) -> bool {
        let mut held = self.held.lock();
        if held.gone.contains(name) {
            return false;
        }
        if held.missed.len() >= MISSED_TABLES && !held.missed.contains_key(name) {
            held.missed.clear();
        }
        let missed = held.missed.entry(*name).or_default();
        *missed = missed.saturating_add(read);
        missed.saturating_mul(FETCH_PART) >= size && self.room_in(&held, size)
    }

    /// Whether a table of `size` bytes that reads want may be fetched:
    /// there is room for it, or the least recently used table has gone
    /// unread, and `reserve` will see how much room such tables make. This
    /// is asked for every read that misses, so it looks at one table.
    fn room_in(&self, held: &Held, size: u64) -> bool {
        size <= self.budget.min(DISK_OBJECT_BYTES)
            && (!self.over(held, size, 1) || self.unread(held).is_some())
    }

    /// Put out tables until `size` more bytes fit, and name them in `out`
    /// for `unlink`: the least recently used while no read has asked for
    /// it in five minutes, then those of `done`, then the least recently
    /// used. `kept` are the bytes and the number of the most recently used
    /// that stay. False, with none put out, when the bytes would not fit
    /// with every other table gone, for those and for the room that
    /// fetches under way were promised.
    fn make_room(
        &self,
        held: &mut Held,
        size: u64,
        (done, kept): (&[Name], (u64, usize)),
        out: &mut Vec<Name>,
    ) -> bool {
        let tables = usize::from(size > 0);
        if held.promised + kept.0 + size > self.budget
            || held.promises + kept.1 + tables > Self::most(self.budget)
        {
            return false;
        }
        let mut done = done.iter();
        while self.over(held, size, tables) {
            let name = self
                .unread(held)
                .or_else(|| done.find(|name| held.tables.contains_key(*name)).copied())
                .or_else(|| held.order.values().next().copied());
            let Some(name) = name else {
                break;
            };
            held.remove(&name);
            out.push(name);
        }
        true
    }

    /// Remove the files of tables that were put out. Every read of a table
    /// asks what is held, so this is done when that has been let go of.
    fn unlink(&self, names: &[Name]) {
        if names.is_empty() {
            return;
        }
        // A file that is still open is not gone with its name, and what
        // was written to it would be written to the disk for nothing.
        let mut closed = Vec::new();
        {
            let mut unsynced = self.unsynced.lock();
            let files = std::mem::take(&mut unsynced.files);
            for (name, file, size) in files {
                if names.contains(&name) {
                    unsynced.bytes -= size;
                    closed.push(file);
                } else {
                    unsynced.files.push((name, file, size));
                }
            }
        }
        drop(closed);
        for name in names {
            let _ = fs::remove_file(self.path(name));
        }
    }

    /// Promise room to a table of `size` bytes that reads want and that is
    /// about to be fetched. Tables that have gone unread are put out for
    /// it, the least recently used first, and stay out if they did not
    /// make enough. What is promised is taken by `put`, or given back by
    /// `release`.
    fn reserve(&self, size: u64) -> bool {
        if size > self.budget.min(DISK_OBJECT_BYTES) {
            return false;
        }
        let mut held = self.held.lock();
        let mut out = Vec::new();
        let mut room = true;
        while self.over(&held, size, 1) {
            let Some(name) = self.unread(&held) else {
                room = false;
                break;
            };
            held.remove(&name);
            out.push(name);
        }
        if room {
            held.promises += 1;
            held.promised += size;
        }
        drop(held);
        self.unlink(&out);
        room
    }

    fn release(&self, size: u64) {
        let mut held = self.held.lock();
        held.promises -= 1;
        held.promised -= size;
    }

    fn read(&self, name: &Name, start: u64, end: u64) -> Option<Bytes> {
        if !self.held.lock().touch(name) {
            return None;
        }
        let read = || -> io::Result<Bytes> {
            let mut file = fs::File::open(self.path(name))?;
            let end = end.min(file.metadata()?.len());
            let start = start.min(end);
            file.seek(SeekFrom::Start(start))?;
            let mut body = vec![0; (end - start) as usize];
            file.read_exact(&mut body)?;
            Ok(Bytes::from(body))
        };
        match read() {
            Ok(body) => Some(body),
            Err(_) => {
                // Put out between the two, or gone from the directory
                // behind the cache's back.
                self.remove(name);
                None
            }
        }
    }

    /// Put a whole table here: one this server wrote, for which the least
    /// recently used make way, or one that was fetched, into the room
    /// `reserve` promised it. A table never changes, so one that is here is
    /// not written again.
    fn put(&self, key: &str, body: &[u8], promised: Option<u64>) {
        self.store(key, body, promised, (&[], &[]), true);
    }

    /// `put`, of a table that a request read whole, which the request
    /// waits for: it never waits for the disk to take what others wrote.
    fn put_read(&self, key: &str, body: &[u8], promised: Option<u64>) {
        self.store(key, body, promised, (&[], &[]), false);
    }

    /// `put`, of a table written while a merge has read through `done`
    /// and has `kept`: the tables it has yet to read through and those it
    /// has stored. Of the tables that are in use, those of `done` make way
    /// first: they leave the cache when the merge is published, and to put
    /// out others for its tables has a merge of a database that was all
    /// here leave half of it here. Those of `kept` count as just used and
    /// none makes way. A table the merge is coming to it would read from
    /// the bucket, which is certain and at once, where a read of the table
    /// being put is neither. A table it has stored is read by nothing
    /// until the merge is published, and would count as unread, and be the
    /// first to go, in a merge that takes more than five minutes. When the
    /// others do not make enough room, the table is not kept.
    fn put_merged(
        &self,
        key: &str,
        body: &[u8],
        promised: Option<u64>,
        done: &[Name],
        kept: &[Name],
    ) {
        self.store(key, body, promised, (done, kept), true);
    }

    /// `put_merged`; with `wait`, what stores a table waits for the disk to
    /// take what was written, when that is too much.
    fn store(
        &self,
        key: &str,
        body: &[u8],
        promised: Option<u64>,
        (done, kept): (&[Name], &[Name]),
        wait: bool,
    ) {
        let (name, size) = (Self::name(key), body.len() as u64);
        let tmp = self.dir.join(format!(
            "tmp-{}-{}",
            std::process::id(),
            self.counter.fetch_add(1, Ordering::Relaxed)
        ));
        let write = || {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(body)?;
            io::Result::Ok(file)
        };
        let fits = size <= self.budget.min(DISK_OBJECT_BYTES);
        let written = fits.then(write).and_then(Result::ok);
        let mut held = self.held.lock();
        if let Some(bytes) = promised {
            held.promises -= 1;
            held.promised -= bytes;
        }
        let kept = held.keep(kept);
        let mut out = Vec::new();
        let stored = written.is_some()
            && !held.gone.contains(&name)
            && !held.tables.contains_key(&name)
            && self.make_room(&mut held, size, (done, kept), &mut out)
            && fs::rename(&tmp, self.path(&name)).is_ok();
        if stored {
            held.insert(name, size, Some(Instant::now()));
            held.missed.remove(&name);
        }
        drop(held);
        self.unlink(&out);
        match written {
            Some(file) if stored => self.sync(name, file, size, wait),
            _ => {
                let _ = fs::remove_file(&tmp);
            }
        }
    }

    /// Have the disk take what was written, once the tables it has not
    /// been asked for are more than `UNSYNCED_BYTES` or `UNSYNCED_TABLES`.
    /// Not for the tables' sake, which the bucket has: what is written and
    /// not yet on the disk is memory that cannot be given up, and it
    /// counts against the limit a server is held to. Two fetches and a
    /// merge wrote 570 MiB so in a second, to a disk that was not asked to
    /// hurry, and the server was killed at its limit of 1 GiB.
    ///
    /// The disk is asked once at a time. Whoever stores a table that is
    /// one too many waits for it, and for one that is under way, so that
    /// what is stored is held to the disk's speed: written and not yet on
    /// the disk are then the tables of one request to the disk, and as
    /// many again waiting, and the tables being written. A request's own
    /// table, which a client waits for, does not have the disk asked until
    /// there are twice as many. And not after each table: a table that is
    /// not kept is never synced, and that is most of what a merge stores
    /// in a cache smaller than its database. Where the store was a
    /// directory on the same disk, a sync of every table made a merge of
    /// everything take half as long again.
    fn sync(&self, name: Name, file: fs::File, size: u64, wait: bool) {
        {
            let mut unsynced = self.unsynced.lock();
            unsynced.files.push((name, file, size));
            unsynced.bytes += size;
            if !unsynced.over(if wait { 1 } else { 2 }) {
                return;
            }
        }
        let _turn = self.syncing.lock();
        let taken = {
            let mut unsynced = self.unsynced.lock();
            // Another took them while this waited for its turn.
            if !unsynced.over(1) {
                return;
            }
            std::mem::take(&mut *unsynced)
        };
        for (name, file, _) in &taken.files {
            // A table the disk did not take is not trusted to be there.
            if file.sync_data().is_err() {
                self.remove(name);
            }
        }
        self.synced.fetch_add(taken.bytes, Ordering::Relaxed);
    }

    fn remove(&self, name: &Name) -> bool {
        let removed = {
            let mut held = self.held.lock();
            held.missed.remove(name);
            held.remove(name)
        };
        if removed {
            self.unlink(&[*name]);
        }
        removed
    }

    /// Take out tables that no manifest lists any more, and remember that
    /// they are gone. A request that began before reads them still: a
    /// fetch for it, or one that was under way, would bring back a table
    /// that nothing reads after, and that counts as in use for as long as
    /// one that is. After a merge of everything, one such table kept the
    /// last table of the result out of a cache that had been its size.
    fn discard(&self, names: &[Name]) {
        let mut out = Vec::new();
        let mut held = self.held.lock();
        for name in names {
            held.missed.remove(name);
            if held.remove(name) {
                out.push(*name);
            }
            if held.gone.insert(*name) {
                if held.left.len() == GONE_TABLES
                    && let Some(first) = held.left.pop_front()
                {
                    held.gone.remove(&first);
                }
                held.left.push_back(*name);
            }
        }
        drop(held);
        self.unlink(&out);
    }
}

pub struct Io {
    pub store: Arc<dyn ObjectStore>,
    blocks: Cache<(Arc<str>, u32), Part, PartWeight, DefaultHashBuilder, Pins>,
    pins: Pins,
    pub disk: Option<Arc<DiskCache>>,
    pub block_hits: AtomicU64,
    pub block_misses: AtomicU64,
    /// Bytes of table indexes in memory, in the cache or in use by a reader.
    pub index_bytes: Arc<AtomicU64>,
    /// Bounds how many table merges run at once on this server.
    pub merges: Arc<tokio::sync::Semaphore>,
    fills: Arc<tokio::sync::Semaphore>,
    /// The turns of requests' scans to read ahead, and those of merges.
    pub ahead: Arc<tokio::sync::Semaphore>,
    pub merge_ahead: Arc<tokio::sync::Semaphore>,
}

impl Io {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cache_bytes: u64,
        disk: Option<DiskCache>,
    ) -> Arc<Self> {
        Self::new_idle(store, cache_bytes, disk, INDEX_IDLE)
    }

    /// As `new`, with how long an index stays in memory after a read last
    /// asked for it, whatever blocks want its room.
    pub fn new_idle(
        store: Arc<dyn ObjectStore>,
        cache_bytes: u64,
        disk: Option<DiskCache>,
        idle: Duration,
    ) -> Arc<Self> {
        let cache_bytes = cache_bytes.max(1 << 20);
        let pins = Pins {
            indexes: Arc::default(),
            most: cache_bytes / 2,
            largest: Arc::default(),
            started: Instant::now(),
            idle,
        };
        // As many parts as the cache library makes by itself, four for
        // each processor, and no more than are `CACHE_PART` each.
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let large = (cache_bytes / CACHE_PART).max(1);
        let parts = (cores * 4).next_power_of_two().min(1 << large.ilog2());
        let options = OptionsBuilder::new()
            .estimated_items_capacity((cache_bytes / 16_384) as usize + 16)
            .weight_capacity(cache_bytes)
            .shards(parts)
            .build()
            .expect("a cache of some size");
        let blocks = Cache::with_options(
            options,
            PartWeight,
            DefaultHashBuilder::default(),
            pins.clone(),
        );
        let largest = blocks.shard_capacity() / 16;
        pins.largest.store(largest, Ordering::Relaxed);
        Arc::new(Self {
            store,
            blocks,
            disk: disk.map(Arc::new),
            block_hits: AtomicU64::new(0),
            block_misses: AtomicU64::new(0),
            index_bytes: pins.indexes.clone(),
            pins,
            merges: Arc::new(tokio::sync::Semaphore::new(2)),
            fills: Arc::new(tokio::sync::Semaphore::new(FILLS)),
            ahead: Arc::new(tokio::sync::Semaphore::new(AHEAD_SCANS)),
            merge_ahead: Arc::new(tokio::sync::Semaphore::new(AHEAD_MERGES)),
        })
    }

    pub fn cached_block(&self, table: &Arc<str>, block: u32) -> Option<Arc<Block>> {
        let found = match self.blocks.get(&(table.clone(), block)) {
            Some(Part::Block(block)) => Some(block),
            _ => None,
        };
        let counter = if found.is_some() {
            &self.block_hits
        } else {
            &self.block_misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    pub fn cache_block(&self, table: &Arc<str>, block: u32, data: Arc<Block>) {
        self.blocks
            .insert((table.clone(), block), Part::Block(data));
    }

    pub fn cached_index(&self, table: &Arc<str>) -> Option<Arc<Index>> {
        match self.blocks.get(&(table.clone(), INDEX)) {
            Some(Part::Index(index)) => {
                index.use_at(self.pins.now());
                Some(index)
            }
            _ => None,
        }
    }

    /// Keep a table's index for as long as reads ask for it and the cache
    /// has room for it. The table itself only remembers where it is.
    pub fn cache_index(&self, table: &Arc<str>, index: Arc<Index>) {
        index.use_at(self.pins.now());
        self.blocks
            .insert((table.clone(), INDEX), Part::Index(index));
    }

    pub fn cache_bytes(&self) -> u64 {
        self.blocks.weight()
    }

    /// Whether a table is in the disk cache, where reading it costs no
    /// request.
    pub fn local(&self, key: &str) -> bool {
        self.disk
            .as_ref()
            .is_some_and(|disk| disk.holds(&DiskCache::name(key)))
    }

    /// Bytes `start..end` of an immutable table of `size` bytes that the
    /// manifest says exists: from the disk cache when it is there, otherwise
    /// from the bucket. With `keep`, a table that was not in the disk cache
    /// is fetched for it when such reads have had enough of it and it has
    /// room. A merge reads each of its tables once, and keeps none.
    pub async fn range(
        self: &Arc<Self>,
        key: &str,
        size: u64,
        start: u64,
        end: u64,
        keep: bool,
    ) -> Result<Bytes> {
        if let Some(disk) = &self.disk {
            let name = DiskCache::name(key);
            // What is not here is known without asking the directory.
            let hit = if disk.holds(&name) {
                let cache = disk.clone();
                tokio::task::spawn_blocking(move || cache.read(&name, start, end))
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            if let Some(bytes) = hit {
                disk.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(bytes);
            }
            disk.misses.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = retry(|| self.store.get_range(key, start, end))
            .await?
            .ok_or_else(|| {
                let object = crate::layout::object(key);
                Error::Lost(format!("{object} was collected by a newer owner"))
            })?;
        if keep
            && let Some(disk) = &self.disk
            && disk.wanted(&DiskCache::name(key), size, bytes.len() as u64)
        {
            if start == 0 && end >= size {
                let (disk, key, body) = (disk.clone(), key.to_owned(), bytes.clone());
                let _ = tokio::task::spawn_blocking(move || {
                    let size = body.len() as u64;
                    if disk.reserve(size) {
                        disk.put_read(&key, &body, Some(size));
                    }
                })
                .await;
            } else {
                self.fill(disk, key, size);
            }
        }
        Ok(bytes)
    }

    /// Fetch a whole table for the disk cache, off the request's path. One
    /// that has no turn now is fetched after a later read of it.
    fn fill(self: &Arc<Self>, disk: &Arc<DiskCache>, key: &str, size: u64) {
        let Ok(turn) = self.fills.clone().try_acquire_owned() else {
            return;
        };
        if !disk.filling.lock().insert(key.to_owned()) {
            return;
        }
        let (io, disk, key) = (self.clone(), disk.clone(), key.to_owned());
        tokio::spawn(async move {
            let cache = disk.clone();
            let room = tokio::task::spawn_blocking(move || cache.reserve(size)).await;
            if room.unwrap_or(false) {
                let body = retry(|| io.store.get(&key)).await.ok().flatten();
                let (cache, key) = (disk.clone(), key.clone());
                let _ = tokio::task::spawn_blocking(move || match body {
                    Some(body) => {
                        cache.fetched.fetch_add(1, Ordering::Relaxed);
                        cache.put(&key, &body, Some(size));
                    }
                    None => cache.release(size),
                })
                .await;
            }
            disk.filling.lock().remove(&key);
            drop(turn);
        });
    }

    /// Put a whole table that this server wrote into the disk cache.
    /// Returns when it is there, so that its bytes are not left waiting in
    /// memory. While a merge runs, `done` are the tables it has read
    /// through and `kept` those it has yet to and those it has stored; a
    /// flush outside a merge has neither. The table takes the room of the
    /// first before that of any other in use, and never that of the
    /// second. Left to the order of their use it took just that, since the
    /// tables a merge is coming to are those it used longest ago, and the
    /// merge then read them from the bucket: a merge of a whole database,
    /// with a cache as large as that database, fetched most of it again.
    pub async fn remember(&self, key: &str, body: Bytes, done: &[&str], kept: &[&str]) {
        if let Some(disk) = &self.disk {
            let names = |keys: &[&str]| -> Vec<Name> {
                keys.iter().map(|key| DiskCache::name(key)).collect()
            };
            let (done, kept) = (names(done), names(kept));
            let (disk, key) = (disk.clone(), key.to_owned());
            let put = move || disk.put_merged(&key, &body, None, &done, &kept);
            let _ = tokio::task::spawn_blocking(put).await;
        }
    }

    /// Discard a table's copy in the disk cache, when what was read of the
    /// table failed its checksum. Returns whether there is a disk cache, in
    /// which case reading again goes to the bucket. The copy may be gone
    /// already: another reader that it failed has discarded it, and this
    /// one had its bytes all the same.
    pub async fn forget(&self, key: &str) -> bool {
        let Some(disk) = &self.disk else { return false };
        let (disk, name) = (disk.clone(), DiskCache::name(key));
        let _ = tokio::task::spawn_blocking(move || disk.remove(&name)).await;
        true
    }

    /// Take out of the disk cache tables that no manifest lists any more.
    pub async fn discard(&self, keys: &[String]) {
        if let Some(disk) = &self.disk
            && !keys.is_empty()
        {
            let disk = disk.clone();
            let names: Vec<Name> = keys.iter().map(|key| DiskCache::name(key)).collect();
            let _ = tokio::task::spawn_blocking(move || disk.discard(&names)).await;
        }
    }
}

/// Retry an idempotent storage call through transient failures.
pub async fn retry<T, F, Fut>(mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = StoreResult<T>>,
{
    let mut delay = Duration::from_millis(20);
    for attempt in 0.. {
        match call().await {
            Ok(value) => return Ok(value),
            Err(StoreError::Unavailable(reason)) if attempt < 6 => {
                tracing::debug!(%reason, attempt, "storage call failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    fn key(n: u8) -> String {
        format!("root/db/app/t/1/{n:08x}")
    }

    fn name(n: u8) -> Name {
        DiskCache::name(&key(n))
    }

    /// An index is held in the memory cache while reads ask for it and
    /// the indexes are within half the cache, and no longer.
    #[test]
    fn an_index_is_held_while_reads_ask_for_it() {
        let store = Arc::new(crate::store::memory::MemoryStore::new());
        let io = Io::new(store, 64 << 20, None);
        // A part of the cache is 4 MiB at least, whatever the processors.
        let size = io.blocks.shard_capacity();
        assert!(size >= CACHE_PART && io.blocks.capacity() == 64 << 20);
        let table: Arc<str> = Arc::from(key(1));
        let index = Arc::new(Index::weighing(size as usize / 16, &io.index_bytes));
        let part = Part::Index(index.clone());
        let held = |pins: &Pins| pins.is_pinned(&(table.clone(), INDEX), &part);
        // An index of which sixteen do not fit in a part is not held.
        let large = Arc::new(Index::weighing(size as usize / 16 + 1, &io.index_bytes));
        large.use_at(io.pins.now());
        let large = Part::Index(large);
        assert!(!io.pins.is_pinned(&(table.clone(), INDEX), &large));
        drop(large);
        let brief = Pins {
            idle: Duration::from_millis(5),
            ..io.pins.clone()
        };
        index.use_at(io.pins.now());
        std::thread::sleep(Duration::from_millis(20));
        assert!(held(&io.pins) && !held(&brief));
        // With more than half the cache in indexes, none is held.
        let loaded = io.index_bytes.swap((32 << 20) + 1, Ordering::Relaxed);
        assert!(!held(&io.pins));
        io.index_bytes.store(32 << 20, Ordering::Relaxed);
        assert!(held(&io.pins));
        io.index_bytes.store(loaded, Ordering::Relaxed);
        // To ask the cache for it is use, as to put it there is.
        index.use_at(u64::MAX);
        io.cache_index(&table, index.clone());
        assert!(index.used() < u64::MAX);
        index.use_at(u64::MAX);
        assert!(io.cached_index(&table).is_some());
        assert!(index.used() < u64::MAX);
    }

    /// A cache of `budget` bytes with tables of 1000 bytes written into it.
    fn cache(dir: &tempfile::TempDir, budget: u64, tables: std::ops::Range<u8>) -> DiskCache {
        let cache = DiskCache::open_idle(dir.path(), budget, HOUR).unwrap();
        for n in tables {
            cache.put(&key(n), &[n; 1000], None);
        }
        cache
    }

    fn here(cache: &DiskCache) -> Vec<u8> {
        (0..=255).filter(|n| cache.holds(&name(*n))).collect()
    }

    /// Whether a table of `size` bytes that reads missed would be given
    /// room.
    fn room(cache: &DiskCache, size: u64) -> bool {
        cache.room_in(&cache.held.lock(), size)
    }

    fn files(dir: &tempfile::TempDir) -> usize {
        fs::read_dir(dir.path()).unwrap().count()
    }

    /// A table that could not be written, as on a disk that is full, is
    /// not kept and puts out nothing to make room for itself. Here the
    /// file it would be written to cannot be made, since a directory has
    /// its name: so it is even where the directory's mode would not stop
    /// whoever runs the test.
    #[test]
    fn a_table_that_could_not_be_written_puts_out_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..4);
        let next = cache.counter.load(Ordering::Relaxed);
        let tmp = dir
            .path()
            .join(format!("tmp-{}-{next}", std::process::id()));
        fs::create_dir(&tmp).unwrap();
        cache.put(&key(4), &[4; 1000], None);
        assert_eq!(here(&cache), [0, 1, 2, 3]);
        assert_eq!((cache.used(), cache.unsynced.lock().files.len()), (4000, 4));
        // The next is written, and the least recently used makes way.
        cache.put(&key(5), &[5; 1000], None);
        assert_eq!(here(&cache), [1, 2, 3, 5]);
    }

    /// What is written to the cache directory is memory until the disk
    /// has it, so the disk is asked for it before it is too much: after
    /// more than sixty-four tables, or more than 32 MiB of them, and twice
    /// that for a table a request put. A table that is not kept is not
    /// among them, and one that is put out leaves them.
    #[test]
    fn the_disk_is_asked_for_what_was_written_before_it_is_too_much() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(dir.path(), 200 << 20).unwrap();
        let synced = || cache.synced.load(Ordering::Relaxed);
        let unsynced = || {
            let unsynced = cache.unsynced.lock();
            (unsynced.files.len(), unsynced.bytes)
        };
        for n in 0..UNSYNCED_TABLES as u8 {
            cache.put(&key(n), &[n; 1000], None);
        }
        // One that is here already is written and not kept.
        cache.put(&key(0), &[0; 1000], None);
        assert_eq!((synced(), unsynced()), (0, (64, 64_000)));
        cache.put(&key(100), &[100; 1000], None);
        assert_eq!((synced(), unsynced()), (65_000, (0, 0)));
        // Two of 16 MiB are not more than 32 MiB, and a table of a
        // thousand bytes with them is.
        let large = vec![1; 16 << 20];
        cache.put(&key(101), &large, None);
        cache.put(&key(102), &large, None);
        assert_eq!((synced(), unsynced().1), (65_000, 32 << 20));
        cache.put(&key(103), &[103; 1000], None);
        let at = 66_000 + (32 << 20);
        assert_eq!((synced(), unsynced()), (at, (0, 0)));
        // A request's table has the disk asked at twice as much.
        cache.put(&key(104), &large, None);
        cache.put(&key(105), &large, None);
        cache.put_read(&key(106), &[106; 1000], None);
        cache.put_read(&key(107), &large, None);
        assert_eq!((synced(), unsynced()), (at, (4, (48 << 20) + 1000)));
        cache.put_read(&key(108), &large, None);
        let at = at + (64 << 20) + 1000;
        assert_eq!((synced(), unsynced()), (at, (0, 0)));
        // A table that is put out is not synced: its file is closed.
        cache.put(&key(109), &[109; 1000], None);
        assert_eq!(unsynced(), (1, 1000));
        assert!(cache.remove(&name(109)));
        assert_eq!((synced(), unsynced()), (at, (0, 0)));
        assert_eq!(files(&dir), 73);
    }

    /// The disk is asked once at a time, and whoever stores a table that is
    /// one too many while it is asked waits for that, and then for nothing
    /// if what was written was taken meanwhile.
    #[test]
    fn the_disk_is_asked_once_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 200 << 20).unwrap());
        let turn = cache.syncing.lock();
        let writer = |n: u8| {
            let cache = cache.clone();
            std::thread::spawn(move || cache.put(&key(n), &vec![n; (32 << 20) + 1], None))
        };
        let (first, second) = (writer(1), writer(2));
        // Both store their tables, and wait.
        let begun = Instant::now();
        while cache.unsynced.lock().files.len() < 2 {
            assert!(begun.elapsed() < Duration::from_secs(20), "both are stored");
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(20));
        assert!(!first.is_finished() && !second.is_finished());
        assert_eq!(cache.synced.load(Ordering::Relaxed), 0);
        drop(turn);
        first.join().unwrap();
        second.join().unwrap();
        let synced = cache.synced.load(Ordering::Relaxed);
        assert_eq!(
            (synced, cache.unsynced.lock().bytes),
            (2 * ((32 << 20) + 1), 0)
        );
    }

    #[test]
    fn a_table_that_was_written_puts_out_the_least_recently_used() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..4);
        assert_eq!((cache.used(), files(&dir)), (4000, 4));
        // The first is read, so the second is the least recently used.
        assert_eq!(cache.read(&name(0), 990, 2000).unwrap(), vec![0u8; 10]);
        cache.put(&key(4), &[4; 1000], None);
        assert_eq!(here(&cache), [0, 2, 3, 4]);
        // One that needs the room of two puts out two.
        cache.put(&key(5), &[5; 1500], None);
        assert_eq!(here(&cache), [0, 4, 5]);
        assert_eq!((cache.used(), files(&dir)), (3500, 3));
        // One larger than the cache is not kept, and puts out nothing.
        cache.put(&key(6), &[6; 4001], None);
        assert_eq!(here(&cache), [0, 4, 5]);
        assert_eq!((cache.used(), files(&dir)), (3500, 3));
        // One that is here is not written again.
        cache.put(&key(5), &[5; 1500], None);
        assert_eq!((cache.used(), files(&dir)), (3500, 3));
        // A table of more than 64 MiB is not kept by a cache that has the
        // room, nor fetched for it.
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open_idle(dir.path(), 1 << 30, HOUR).unwrap();
        cache.put(&key(7), &vec![7; DISK_OBJECT_BYTES as usize + 1], None);
        assert_eq!((cache.used(), files(&dir)), (0, 0));
        assert!(!room(&cache, DISK_OBJECT_BYTES + 1) && !cache.reserve(DISK_OBJECT_BYTES + 1));
        assert!(room(&cache, DISK_OBJECT_BYTES) && cache.reserve(DISK_OBJECT_BYTES));
    }

    #[test]
    fn a_table_that_a_merge_wrote_does_not_put_out_one_the_merge_is_coming_to() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..4);
        // A merge has read the first and is coming to the others, one of
        // which is not here. Its table takes the room of the first.
        let coming = [name(1), name(2), name(9), name(3)];
        cache.put_merged(&key(4), &[4; 1000], None, &[], &coming);
        assert_eq!(here(&cache), [1, 2, 3, 4]);
        // When it has read the second, a table of twice the size takes the
        // room of the second and of the table the merge wrote before.
        cache.put_merged(&key(5), &[5; 2000], None, &[], &[name(2), name(3)]);
        assert_eq!(here(&cache), [2, 3, 5]);
        // One that would need the room of a table the merge is coming to
        // is not kept, and puts out nothing.
        cache.put_merged(&key(6), &[6; 2001], None, &[], &[name(2), name(3)]);
        assert_eq!(here(&cache), [2, 3, 5]);
        assert_eq!((cache.used(), files(&dir)), (4000, 3));
        // The tables it is coming to were used after the one it wrote.
        cache.put(&key(7), &[7; 1000], None);
        assert_eq!(here(&cache), [2, 3, 7]);
        // Nor is one kept that would need the place of such a table, in a
        // cache that holds as many tables as it may.
        let dir = tempfile::tempdir().unwrap();
        let full = self::cache(&dir, 100_000, 0..67);
        let coming: Vec<Name> = (0..67).map(name).collect();
        full.put_merged(&key(67), &[67; 1000], None, &[], &coming);
        assert_eq!((full.used(), files(&dir)), (67_000, 67));
        assert_eq!(here(&full), (0..67).collect::<Vec<u8>>());
        full.put_merged(&key(67), &[67; 1000], None, &[], &coming[1..]);
        assert_eq!(here(&full), (1..=67).collect::<Vec<u8>>());
    }

    #[test]
    fn a_table_that_a_merge_wrote_puts_out_those_the_merge_has_read_before_others_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..4);
        // A merge has read the third, which is gone when it is published,
        // and one that is not here. Its table takes the room of the third,
        // though the first was used longer ago.
        let done = [name(9), name(2)];
        cache.put_merged(&key(4), &[4; 1000], None, &done, &[name(3)]);
        assert_eq!(here(&cache), [0, 1, 3, 4]);
        // With none of those left, the least recently used makes way.
        cache.put_merged(&key(5), &[5; 1000], None, &done, &[name(3)]);
        assert_eq!(here(&cache), [1, 3, 4, 5]);
        // A table that no read has asked for makes way before one the
        // merge has read: nothing is waiting for it.
        let dir = tempfile::tempdir().unwrap();
        let idle = DiskCache::open_idle(dir.path(), 4000, Duration::ZERO).unwrap();
        for n in 0..4 {
            idle.put(&key(n), &[n; 1000], None);
        }
        idle.put_merged(&key(4), &[4; 1000], None, &[name(2)], &[name(3)]);
        assert_eq!(here(&idle), [1, 2, 3, 4]);
        // The tables the merge has stored are read by nothing until it is
        // published. They do not count as unread: each one after them
        // takes the room of a table the merge has read through.
        let dir = tempfile::tempdir().unwrap();
        let idle = DiskCache::open_idle(dir.path(), 4000, Duration::ZERO).unwrap();
        for n in 0..4 {
            idle.put(&key(n), &[n; 1000], None);
        }
        let names = |tables: &[u8]| -> Vec<Name> { tables.iter().map(|n| name(*n)).collect() };
        idle.put_merged(
            &key(10),
            &[10; 1000],
            None,
            &names(&[0]),
            &names(&[1, 2, 3]),
        );
        idle.put_merged(
            &key(11),
            &[11; 1000],
            None,
            &names(&[0, 1]),
            &names(&[2, 3, 10]),
        );
        assert_eq!(here(&idle), [2, 3, 10, 11]);
        idle.put_merged(
            &key(12),
            &[12; 1000],
            None,
            &names(&[1, 2]),
            &names(&[3, 10, 11]),
        );
        assert_eq!(here(&idle), [3, 10, 11, 12]);
    }

    #[test]
    fn a_table_that_left_its_manifest_is_not_fetched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..3);
        // A table is being fetched when it and another leave the manifest.
        assert!(cache.wanted(&name(7), 1000, 1000) && cache.reserve(1000));
        cache.discard(&[name(7), name(1)]);
        assert_eq!((here(&cache), files(&dir)), (vec![0, 2], 2));
        // The fetch arrives. The table is not kept, and what it was
        // promised is room again.
        cache.put(&key(7), &[7; 1000], Some(1000));
        assert_eq!((here(&cache), files(&dir)), (vec![0, 2], 2));
        assert!(room(&cache, 2000) && !room(&cache, 2001));
        // Reads of either by requests that began before ask for no fetch.
        assert!(!cache.wanted(&name(7), 1000, 1000));
        assert!(!cache.wanted(&name(1), 1000, 1000));
        assert!(cache.wanted(&name(8), 1000, 1000));
        // What is remembered of them is bounded, and the first to leave is
        // the first forgotten.
        let other = |n: usize| DiskCache::name(&format!("other/{n}"));
        let gone: Vec<Name> = (0..GONE_TABLES - 2).map(other).collect();
        cache.discard(&gone);
        assert!(!cache.wanted(&name(7), 1000, 1000) && !cache.wanted(&other(0), 1000, 1000));
        cache.discard(&[other(GONE_TABLES), other(0)]);
        assert!(cache.wanted(&name(7), 1000, 1000));
        assert!(!cache.wanted(&name(1), 1000, 1000) && !cache.wanted(&other(0), 1000, 1000));
        // Also when more leave at once than are remembered.
        let many: Vec<Name> = (0..GONE_TABLES + 3).map(|n| other(n + 100_000)).collect();
        cache.discard(&many);
        let held = cache.held.lock();
        assert_eq!(
            (held.gone.len(), held.left.len()),
            (GONE_TABLES, GONE_TABLES)
        );
        assert!(!held.gone.contains(&many[2]) && held.gone.contains(&many[3]));
    }

    #[test]
    fn a_table_that_a_read_missed_does_not_put_out_one_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..4);
        assert!(!room(&cache, 1000) && !cache.reserve(1000));
        assert_eq!(here(&cache), [0, 1, 2, 3]);
        // Room that is free is promised, once.
        assert!(cache.remove(&name(2)));
        assert!(room(&cache, 1000) && cache.reserve(1000));
        assert!(!room(&cache, 1000) && !cache.reserve(1000));
        assert!(!room(&cache, 1) && !cache.reserve(1));
        // What is promised is not counted as held until it is here.
        assert_eq!(cache.used(), 3000);
        cache.put(&key(7), &[7; 1000], Some(1000));
        assert_eq!(here(&cache), [0, 1, 3, 7]);
        assert_eq!((cache.used(), files(&dir)), (4000, 4));
        assert!(!room(&cache, 1));
        // A promise that is given back is room again.
        assert!(cache.remove(&name(3)));
        assert!(cache.reserve(600));
        assert!(!cache.reserve(600));
        cache.release(600);
        assert!(cache.reserve(1000));
        // A table that was written meanwhile leaves the promise its room.
        cache.put(&key(8), &[8; 1000], None);
        assert_eq!(here(&cache), [1, 7, 8]);
        // One that would not fit beside the promise with every table gone
        // is not kept, and puts out nothing.
        cache.put(&key(10), &[10; 3001], None);
        assert_eq!(here(&cache), [1, 7, 8]);
        assert_eq!((cache.used(), files(&dir)), (3000, 3));
        cache.put(&key(9), &[9; 1000], Some(1000));
        assert_eq!(here(&cache), [1, 7, 8, 9]);
        assert_eq!((cache.used(), files(&dir)), (4000, 4));
        // A fetched table that is here already gives its promise back.
        assert!(cache.remove(&name(1)));
        assert!(cache.reserve(1000));
        cache.put(&key(9), &[9; 1000], Some(1000));
        assert_eq!((cache.used(), files(&dir)), (3000, 3));
        assert!(cache.reserve(1000));
    }

    #[test]
    fn a_table_is_wanted_when_reads_have_had_a_sixteenth_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..3);
        // 99 bytes of 1,600 are less than a sixteenth. A hundred are one,
        // but the room that is free is for 1,000.
        assert!(!cache.wanted(&name(7), 1600, 99));
        assert!(!cache.wanted(&name(7), 1600, 1));
        assert!(!cache.wanted(&name(8), 1000, 62));
        assert!(cache.wanted(&name(8), 1000, 1));
        // It is wanted until it is here, and each table is counted alone.
        assert!(cache.wanted(&name(8), 1000, 0));
        assert!(!cache.wanted(&name(9), 1000, 62));
        // What was read of a table while there was no room for it counts
        // when there is.
        assert!(cache.remove(&name(0)));
        assert!(cache.wanted(&name(7), 1600, 0));
        // A table that has been here is counted from the beginning: one
        // that tables which were written put out,
        assert!(cache.reserve(1000));
        cache.put(&key(8), &[8; 1000], Some(1000));
        assert!(cache.read(&name(1), 0, 1).is_some());
        assert!(cache.read(&name(2), 0, 1).is_some());
        cache.put(&key(20), &[20; 1000], None);
        cache.put(&key(21), &[21; 1000], None);
        assert_eq!(here(&cache), [1, 2, 20, 21]);
        assert!(cache.remove(&name(20)));
        assert!(!cache.wanted(&name(8), 1000, 62));
        assert!(cache.wanted(&name(8), 1000, 1));
        // and one that was discarded, whether it was here
        assert!(cache.reserve(1000));
        cache.put(&key(8), &[8; 1000], Some(1000));
        assert!(cache.remove(&name(8)));
        assert!(!cache.wanted(&name(8), 1000, 62));
        // or not.
        assert!(!cache.remove(&name(9)));
        assert!(!cache.wanted(&name(9), 1000, 62));
        // One that is larger than the cache is never wanted.
        assert!(!cache.wanted(&name(6), 4001, 4001));
        // No more tables are counted than a thousand or so: with more,
        // the count begins again.
        let dir = tempfile::tempdir().unwrap();
        let cache = self::cache(&dir, 4000, 0..0);
        let other = |n: usize| DiskCache::name(&format!("other/{n}"));
        assert!(!cache.wanted(&other(0), 1000, 62));
        for n in 1..MISSED_TABLES {
            assert!(!cache.wanted(&other(n), 1000, 1));
        }
        assert_eq!(cache.held.lock().missed.len(), MISSED_TABLES);
        assert!(!cache.wanted(&other(0), 1000, 0));
        assert_eq!(cache.held.lock().missed.len(), MISSED_TABLES);
        assert!(!cache.wanted(&other(MISSED_TABLES), 1000, 1));
        assert_eq!(cache.held.lock().missed.len(), 1);
        assert!(!cache.wanted(&other(0), 1000, 62));
    }

    #[test]
    fn a_table_that_went_unread_gives_way_to_one_that_a_read_missed() {
        let dir = tempfile::tempdir().unwrap();
        drop(cache(&dir, 4000, 0..4));
        // What an earlier server left has gone unread by this one.
        let cache = cache(&dir, 4000, 0..0);
        assert_eq!(here(&cache), [0, 1, 2, 3]);
        // None goes for a table that the cache could not hold.
        assert!(!room(&cache, 4001) && !cache.reserve(4001));
        assert_eq!(here(&cache), [0, 1, 2, 3]);
        assert!(cache.read(&name(0), 0, 1).is_some());
        assert!(cache.read(&name(2), 0, 1).is_some());
        // Two are in use now. One of the others goes for the room of one.
        assert!(room(&cache, 1000) && cache.reserve(1000));
        assert_eq!(here(&cache), [0, 2, 3]);
        cache.put(&key(7), &[7; 1000], Some(1000));
        // The other goes for a table that needs more room than it leaves,
        // and that is promised none.
        assert!(room(&cache, 1001) && !cache.reserve(1001));
        assert_eq!(here(&cache), [0, 2, 7]);
        // Then the least recently used is in use: nothing more goes, and
        // the room that was left is for a table it is enough for.
        assert!(!room(&cache, 1001) && !cache.reserve(1001));
        assert_eq!(here(&cache), [0, 2, 7]);
        assert!(room(&cache, 1000) && cache.reserve(1000));
        assert!(!room(&cache, 1) && !cache.reserve(1));
        cache.release(1000);

        // With no time to go unread in, every table has.
        let none = DiskCache::open_idle(dir.path(), 3000, Duration::ZERO).unwrap();
        assert!(none.read(&name(0), 0, 1).is_some());
        assert!(none.reserve(2000));
        assert_eq!(here(&none), [0]);
    }

    #[test]
    fn what_an_earlier_server_left_is_found_and_fitted_to_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        drop(cache(&dir, 4000, 0..4));
        fs::write(dir.path().join("tmp-1-1"), b"half").unwrap();
        fs::write(dir.path().join("notes"), b"not a table").unwrap();
        let second = cache(&dir, 4000, 0..0);
        assert_eq!((second.used(), files(&dir)), (4000, 5));
        assert_eq!(second.read(&name(3), 0, 4).unwrap(), vec![3u8; 4]);
        assert!(dir.path().join("notes").exists());
        drop(second);
        let third = cache(&dir, 2500, 0..0);
        assert_eq!((third.used(), files(&dir)), (2000, 3));
        assert_eq!(here(&third).len(), 2);
    }

    #[test]
    fn a_table_that_left_the_directory_is_a_miss_and_is_counted_no_more() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(&dir, 4000, 0..2);
        fs::remove_file(cache.path(&name(0))).unwrap();
        assert!(cache.read(&name(0), 0, 10).is_none());
        assert_eq!(here(&cache), [1]);
        assert_eq!(cache.used(), 1000);
        cache.put(&key(0), &[0; 1000], None);
        assert_eq!(cache.read(&name(0), 0, 10).unwrap(), vec![0u8; 10]);
    }

    #[test]
    fn the_cache_holds_a_bounded_number_of_tables() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open_idle(dir.path(), 3 * DISK_TABLE_BYTES, HOUR).unwrap();
        for n in 0..100 {
            cache.put(&key(n), &[n], None);
        }
        assert_eq!(here(&cache), (33..100).collect::<Vec<u8>>());
        assert_eq!((cache.used(), files(&dir)), (67, 67));
        assert!(!room(&cache, 1));
        assert!(cache.remove(&name(50)));
        assert!(cache.reserve(1));
        assert!(!cache.reserve(1));
        // The manifest's cache of 1 GiB holds 32,832 tables at most, and
        // no cache more than a quarter of a million.
        assert_eq!(DiskCache::most(1 << 30), 32_832);
        assert_eq!(DiskCache::most(8 << 30), 262_144);
        assert_eq!(DiskCache::most(1 << 40), 262_144);
        // A table that is written when every place is promised to one
        // that is being fetched is not kept.
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open_idle(dir.path(), 3 * DISK_TABLE_BYTES, HOUR).unwrap();
        for _ in 0..67 {
            assert!(cache.reserve(1));
        }
        cache.put(&key(0), &[0], None);
        assert_eq!((cache.used(), files(&dir)), (0, 0));
    }
}
