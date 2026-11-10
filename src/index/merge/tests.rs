use super::*;
use crate::{
    cache::Cache,
    storage::{Object, Remote, Storage, Version},
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering as AO},
    },
};
use tokio::sync::Semaphore;

struct Files {
    inner: Remote,
    reads: AtomicUsize,
    fail: AtomicBool,
    pause: AtomicBool,
    entered: Semaphore,
}
impl Files {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Remote::new(Arc::new(object_store::memory::InMemory::new()), "merge").unwrap(),
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: Semaphore::new(0),
        })
    }
}
#[async_trait]
impl Storage for Files {
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        size: u64,
    ) -> anyhow::Result<Bytes> {
        self.reads.fetch_add(1, AO::SeqCst);
        ensure!(!self.fail.swap(false, AO::SeqCst), "injected range error");
        if self.pause.swap(false, AO::SeqCst) {
            self.entered.add_permits(1);
            futures::future::pending::<()>().await;
        }
        self.inner.get_range(key, range, size).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, _: &str, _: usize) -> anyhow::Result<Option<Object>> {
        panic!("range reads only")
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        panic!("no conditional table writes")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        panic!("manifest supplies sources")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        panic!("readers never delete")
    }
}
type TestRow<'a> = (&'a [u8], u64, Option<&'a [u8]>);
async fn table(files: Arc<Files>, rows: &[TestRow<'_>]) -> Table {
    let reference = table::build(
        files.clone(),
        rows.iter().map(|&(key, revision, value)| table::Row {
            key,
            revision,
            value,
        }),
    )
    .await
    .unwrap();
    Table::open(files, Arc::new(Cache::new(0, None).unwrap()), reference)
        .await
        .unwrap()
}
async fn collect(mut cursor: Cursor) -> Vec<(Bytes, u64, Option<Bytes>)> {
    let mut out = Vec::new();
    while let Some(row) = cursor.next().await.unwrap() {
        out.push((row.key, row.revision, row.value));
    }
    out
}

#[tokio::test]
async fn overlapping_snapshots_preserve_deletions_empty_values_and_binary_order() {
    let older = table(
        Files::new(),
        &[
            (b"a", 1, Some(b"old")),
            (b"b", 2, Some(b"")),
            (b"c", 1, Some(b"c")),
            (b"\xff", 1, Some(b"last")),
        ],
    )
    .await;
    let newer = table(
        Files::new(),
        &[(b"a", 3, None), (b"c", 4, Some(b"new")), (b"d", 2, None)],
    )
    .await;
    for tables in [vec![older.clone(), newer.clone()], vec![newer, older]] {
        let current = collect(Cursor::new(&tables, b"", None, 4).unwrap()).await;
        assert_eq!(current.len(), 5);
        assert!(current[0].2.is_none());
        assert_eq!(current[1].2, Some(Bytes::new()));
        assert_eq!(current[2].2.as_deref(), Some(b"new".as_slice()));
        assert!(current[3].2.is_none());
        assert_eq!(current[4].0, b"\xff".as_slice());
        let old = get(&tables, b"a", 2).await.unwrap().unwrap();
        assert_eq!(old.value.as_deref(), Some(b"old".as_slice()));
        assert!(
            get(&tables, b"a", 3)
                .await
                .unwrap()
                .unwrap()
                .value
                .is_none()
        );
        assert!(get(&tables, b"absent", 4).await.unwrap().is_none());
        assert!(get(&tables, b"a", 0).await.unwrap().is_none());
        let range = collect(Cursor::new(&tables, b"b", Some(b"d"), 2).unwrap()).await;
        assert_eq!(
            range.iter().map(|r| r.0.as_ref()).collect::<Vec<_>>(),
            [b"b", b"c"]
        );
    }
}

#[tokio::test]
async fn contradictory_visible_rows_fail_and_retire_scans_but_newer_versions_dominate() {
    let a = table(Files::new(), &[(b"a", 1, Some(b"first"))]).await;
    let b = table(Files::new(), &[(b"a", 1, None)]).await;
    let c = table(Files::new(), &[(b"a", 2, Some(b"newer"))]).await;
    assert!(get(&[a.clone(), b.clone()], b"a", 1).await.is_err());
    let mut cursor = Cursor::new(&[a.clone(), b.clone()], b"", None, 1).unwrap();
    assert!(cursor.next().await.is_err());
    assert!(cursor.next().await.is_err());
    // The result must not depend on the order in which source reads complete.
    for tables in [
        vec![a.clone(), b.clone(), c.clone()],
        vec![c.clone(), b.clone(), a.clone()],
        vec![b, c, a.clone()],
    ] {
        let row = get(&tables, b"a", 2).await.unwrap().unwrap();
        assert_eq!(row.revision, 2);
        let rows = collect(Cursor::new(&tables, b"", None, 2).unwrap()).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 2);
    }
    assert_eq!(
        collect(Cursor::new(&[a.clone(), a], b"", None, 1).unwrap())
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn failed_and_cancelled_sources_never_become_absence_or_skip_buffered_heads() {
    for cancel in [false, true] {
        let first = Files::new();
        let second = Files::new();
        let tables = [
            table(first.clone(), &[(b"a", 1, Some(b"old")), (b"c", 1, None)]).await,
            table(second.clone(), &[(b"a", 2, None), (b"b", 2, Some(b"next"))]).await,
        ];
        let baseline = first.reads.load(AO::SeqCst);
        let mut cursor = Cursor::new(&tables, b"", None, 2).unwrap();
        if cancel {
            second.pause.store(true, AO::SeqCst);
            tokio::select! {
                result = cursor.next() => panic!("paused source completed: {}", result.is_ok()),
                permit = second.entered.acquire() => permit.unwrap().forget(),
            }
        } else {
            second.fail.store(true, AO::SeqCst);
            assert!(cursor.next().await.is_err());
        }
        assert_eq!(first.reads.load(AO::SeqCst), baseline + 1);
        let rows = collect(cursor).await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, b"a".as_slice());
        assert!(rows[0].2.is_none());
        // The first source's held frontier survived the other source's failure.
        assert_eq!(first.reads.load(AO::SeqCst), baseline + 1);
        second.fail.store(true, AO::SeqCst);
        assert!(get(&tables, b"a", 2).await.is_err());
        assert!(
            get(&tables, b"a", 2)
                .await
                .unwrap()
                .unwrap()
                .value
                .is_none()
        );
    }
}

#[tokio::test]
async fn fan_in_is_bounded_and_empty_views_still_validate_queries() {
    assert!(get(&[], b"", 1).await.is_err());
    assert!(Cursor::new(&[], b"z", Some(b"a"), 1).is_err());
    assert!(get(&[], b"a", 1).await.unwrap().is_none());
    assert!(
        Cursor::new(&[], b"", None, 1)
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none()
    );
    let t = table(Files::new(), &[(b"a", 1, None)]).await;
    let mut tables = vec![t.clone(); MAX_SOURCES];
    assert!(
        get(&tables, b"a", 1)
            .await
            .unwrap()
            .unwrap()
            .value
            .is_none()
    );
    assert_eq!(
        collect(Cursor::new(&tables, b"", None, 1).unwrap())
            .await
            .len(),
        1
    );
    tables.push(t);
    assert!(get(&tables, b"a", 1).await.is_err());
    assert!(Cursor::new(&tables, b"", None, 1).is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn partitioned_histories_match_union_oracle(
        input in proptest::collection::vec((0u8..24, 1u64..12, 0u8..4, proptest::option::of(proptest::collection::vec(proptest::num::u8::ANY, 0..20))), 1..160),
        snapshot in 0u64..14, start in 0u8..12, end in 12u8..25,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Unique (key, revision) gives an unambiguous reference history.
            let history: BTreeMap<_, _> = input.into_iter().map(|(k,r,s,v)| ((k,r),(s,v))).collect();
            let mut tables = Vec::new();
            for source in 0..4 {
                let mut rows: Vec<_> = history.iter().filter(|(_, (s,_))| *s == source)
                    .map(|(&(k,r), (_,v))| (vec![k], r, v.clone())).collect();
                rows.sort_by(|a,b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
                if !rows.is_empty() {
                    let borrowed: Vec<_> = rows.iter().map(|(k,r,v)| (k.as_slice(),*r,v.as_deref())).collect();
                    tables.push(table(Files::new(), &borrowed).await);
                }
            }
            let mut expected = BTreeMap::new();
            for (&(key, revision), (_, value)) in &history {
                if revision <= snapshot { expected.insert(key,(revision,value.clone().map(Bytes::from))); }
            }
            for key in 0..25 {
                let actual = get(&tables, &[key], snapshot).await.unwrap().map(|r| (r.revision,r.value));
                assert_eq!(actual, expected.get(&key).cloned());
            }
            let rows = collect(Cursor::new(&tables, &[start], Some(&[end]), snapshot).unwrap()).await;
            let oracle: Vec<_> = expected.range(start..end).map(|(&k,(r,v))| (Bytes::copy_from_slice(&[k]),*r,v.clone())).collect();
            assert_eq!(rows, oracle);
        });
    }
}

#[tokio::test]
async fn read_failures_after_output_preserve_the_next_key_at_a_block_boundary() {
    let files = Files::new();
    let value = vec![1; 64 * 1024];
    let tables = [
        table(
            files.clone(),
            &[
                (b"a", 1, Some(&value)),
                (b"b", 1, Some(&value)),
                (b"d", 1, Some(&value)),
            ],
        )
        .await,
        table(Files::new(), &[(b"a", 2, None), (b"c", 2, None)]).await,
    ];
    let mut cursor = Cursor::new(&tables, b"", None, 2).unwrap();
    assert!(cursor.next().await.unwrap().unwrap().value.is_none());
    files.fail.store(true, AO::SeqCst);
    assert!(cursor.next().await.is_err());
    let rest = collect(cursor).await;
    assert_eq!(
        rest.iter().map(|r| r.0.as_ref()).collect::<Vec<_>>(),
        [b"b", b"c", b"d"]
    );
}

#[tokio::test]
async fn point_reads_bound_in_flight_io_and_cancel_without_detached_work() {
    let mut files = Vec::new();
    let mut tables = Vec::new();
    for _ in 0..MAX_SOURCES {
        let f = Files::new();
        tables.push(table(f.clone(), &[(b"a", 1, None)]).await);
        f.pause.store(true, AO::SeqCst);
        files.push(f);
    }
    {
        let lookup = get(&tables, b"a", 1);
        tokio::pin!(lookup);
        tokio::select! {
            result = &mut lookup => panic!("paused read completed: {}", result.is_ok()),
            _ = async { for f in &files[..POINT_CONCURRENCY] { f.entered.acquire().await.unwrap().forget(); } } => (),
        }
        assert!(
            files[..POINT_CONCURRENCY]
                .iter()
                .all(|f| f.reads.load(AO::SeqCst) == 2)
        );
        assert!(
            files[POINT_CONCURRENCY..]
                .iter()
                .all(|f| f.reads.load(AO::SeqCst) == 1)
        );
    }
    // The future owns its reads; cancellation leaves no background tasks.
    for f in &files {
        f.pause.store(false, AO::SeqCst);
    }
    assert!(
        get(&tables, b"a", 1)
            .await
            .unwrap()
            .unwrap()
            .value
            .is_none()
    );
}
