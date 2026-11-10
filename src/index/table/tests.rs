use super::*;
use crate::storage::{Object, Remote, Version};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrder},
};

struct Files {
    raw: Arc<object_store::memory::InMemory>,
    inner: Remote,
    reads: AtomicUsize,
    puts: AtomicUsize,
    fail_index: AtomicBool,
    fail_read: AtomicBool,
    ranges: std::sync::Mutex<Vec<std::ops::Range<u64>>>,
}
impl Files {
    fn new() -> Arc<Self> {
        let raw = Arc::new(object_store::memory::InMemory::new());
        Arc::new(Self {
            inner: Remote::new(raw.clone(), "native").unwrap(),
            raw,
            reads: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            fail_index: AtomicBool::new(false),
            fail_read: AtomicBool::new(false),
            ranges: std::sync::Mutex::new(Vec::new()),
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
        self.reads.fetch_add(1, AtomicOrder::SeqCst);
        ensure!(
            !self.fail_read.swap(false, AtomicOrder::SeqCst),
            "injected range failure"
        );
        self.ranges.lock().unwrap().push(range.clone());
        self.inner.get_range(key, range, size).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let is_index = bytes.starts_with(b"DDBIDX01");
        self.puts.fetch_add(1, AtomicOrder::SeqCst);
        self.inner.put(key, bytes).await?;
        ensure!(
            !(is_index && self.fail_index.swap(false, AtomicOrder::SeqCst)),
            "lost index upload reply"
        );
        Ok(())
    }
    async fn get(&self, _: &str, _: usize) -> anyhow::Result<Option<Object>> {
        panic!("native tables use bounded ranges")
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        panic!("native tables use immutable fresh uploads")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        panic!("native tables do not discover data through listing")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        panic!("table reader never reclaims data")
    }
}
fn cache() -> Arc<Cache> {
    Arc::new(Cache::new(256 * 1024, None).unwrap())
}

#[tokio::test]
async fn point_hits_still_validate_rows_after_the_winner() {
    let files = Files::new();
    let reference = build(
        files.clone(),
        [
            Row {
                key: b"a",
                revision: 1,
                value: Some(b"found"),
            },
            Row {
                key: b"z",
                revision: 1,
                value: None,
            },
        ],
    )
    .await
    .unwrap();
    let data = reference
        .data
        .read_range(files.as_ref(), 0..reference.data.size())
        .await
        .unwrap();
    let index = reference
        .index
        .read_range(files.as_ref(), 0..reference.index.size())
        .await
        .unwrap();
    for last in [b'a', b'b'] {
        let mut bad = data.to_vec();
        *bad.last_mut().unwrap() = last;
        let mut blocks = codec::decode_index(&index, reference.data.size()).unwrap();
        assert_eq!(blocks.len(), 1);
        // Reauthenticate the mutated bytes to test structural validation after
        // the hit, rather than merely failing the outer SHA-256 check.
        blocks[0].digest = Sha256::digest(&bad).into();
        let corrupt = Reference {
            data: Blob::upload(files.clone(), bad.into()).await.unwrap(),
            index: Blob::upload(files.clone(), codec::index(&blocks))
                .await
                .unwrap(),
        };
        let table = Table::open(files.clone(), cache(), corrupt).await.unwrap();
        assert!(table.get(b"a", 1).await.is_err());
        let mut scan = table.scan(b"", None, 1).unwrap();
        assert!(scan.next().await.is_err());
    }
}
async fn collected(mut cursor: Cursor) -> Vec<(Bytes, u64, Option<Bytes>)> {
    let mut out = Vec::new();
    while let Some(row) = cursor.next().await.unwrap() {
        out.push((row.key, row.revision, row.value));
    }
    out
}
#[tokio::test]
async fn revisions_cross_blocks_and_tombstones_survive_range_scans_and_cold_reopen() {
    let files = Files::new();
    let value = vec![7; 16 * 1024];
    let mut rows = Vec::new();
    for revision in (1..=16).rev() {
        rows.push(Row {
            key: b"a",
            revision,
            value: if revision == 6 { None } else { Some(&value) },
        });
    }
    rows.push(Row {
        key: b"b",
        revision: 2,
        value: Some(b""),
    });
    rows.push(Row {
        key: b"c",
        revision: 1,
        value: None,
    });
    let reference = build(files.clone(), rows).await.unwrap();
    let encoded = reference.encode();
    let table = Table::open(files.clone(), cache(), Reference::decode(&encoded).unwrap())
        .await
        .unwrap();
    assert!(table.0.blocks.len() > 3);
    for revision in 1..=18 {
        let row = table.get(b"a", revision).await.unwrap().unwrap();
        assert_eq!(row.revision, revision.min(16));
        assert_eq!(row.value.is_none(), revision == 6);
    }
    assert!(table.get(b"a", 0).await.unwrap().is_none());
    assert!(table.get(b"absent", 18).await.unwrap().is_none());
    let rows = collected(table.scan(b"a", Some(b"c"), 6).unwrap()).await;
    assert_eq!(rows.len(), 2);
    assert!(rows[0].2.is_none() && rows[1].2.as_ref().unwrap().is_empty());
    let rows = collected(table.scan(b"", None, 1).unwrap()).await;
    assert_eq!(rows.len(), 2);
    assert!(rows[1].2.is_none());
    drop(table);
    let recovered = Table::open(files.clone(), cache(), Reference::decode(&encoded).unwrap())
        .await
        .unwrap();
    let reads = files.reads.load(AtomicOrder::SeqCst);
    recovered.get(b"a", 5).await.unwrap();
    assert_eq!(files.reads.load(AtomicOrder::SeqCst), reads + 1);
    recovered.get(b"a", 5).await.unwrap();
    assert_eq!(files.reads.load(AtomicOrder::SeqCst), reads + 1);
    assert!(
        files
            .ranges
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.end - r.start <= MAX_BLOCK_BYTES as u64)
    );
    assert_eq!(files.puts.load(AtomicOrder::SeqCst), 2);
}
#[tokio::test]
async fn invalid_input_and_unknown_uploads_never_return_a_table_reference() {
    let files = Files::new();
    assert!(build(files.clone(), []).await.is_err());
    for rows in [
        vec![Row {
            key: b"",
            revision: 1,
            value: None,
        }],
        vec![Row {
            key: b"a",
            revision: 0,
            value: None,
        }],
        vec![
            Row {
                key: b"b",
                revision: 2,
                value: None,
            },
            Row {
                key: b"a",
                revision: 1,
                value: None,
            },
        ],
        vec![
            Row {
                key: b"a",
                revision: 2,
                value: None,
            },
            Row {
                key: b"a",
                revision: 2,
                value: None,
            },
        ],
    ] {
        assert!(build(files.clone(), rows).await.is_err());
    }
    assert_eq!(files.puts.load(AtomicOrder::SeqCst), 0);
    files.fail_index.store(true, AtomicOrder::SeqCst);
    assert!(
        build(
            files.clone(),
            [Row {
                key: b"a",
                revision: 1,
                value: None
            }]
        )
        .await
        .is_err()
    );
    assert_eq!(files.puts.load(AtomicOrder::SeqCst), 2);
    let reference = build(
        files.clone(),
        [Row {
            key: b"a",
            revision: 1,
            value: None,
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        files.inner.list("tables/", None, 10).await.unwrap().len(),
        4
    );
    let bytes = reference.encode();
    for n in 0..bytes.len() {
        let mut bad = bytes.to_vec();
        bad[n] ^= 1;
        assert!(Reference::decode(&bad).is_err());
    }
}
#[tokio::test]
async fn failed_or_corrupt_block_never_becomes_absence_and_cursor_retries_the_same_block() {
    use object_store::{ObjectStoreExt, path::Path};
    let files = Files::new();
    let reference = build(
        files.clone(),
        [Row {
            key: b"a",
            revision: 3,
            value: Some(b"value"),
        }],
    )
    .await
    .unwrap();
    let table = Table::open(files.clone(), cache(), reference)
        .await
        .unwrap();
    let mut cursor = table.scan(b"", None, 3).unwrap();
    files.fail_read.store(true, AtomicOrder::SeqCst);
    assert!(cursor.next().await.is_err());
    assert!(cursor.next().await.unwrap().is_some());
    assert!(cursor.next().await.unwrap().is_none());
    let objects = files.inner.list("tables/", None, 10).await.unwrap();
    for key in objects {
        let path = Path::from(format!("native/{key}"));
        let bytes = files.raw.get(&path).await.unwrap().bytes().await.unwrap();
        if bytes.starts_with(b"DDBBLK01") {
            let mut bad = bytes.to_vec();
            let last = bad.len() - 1;
            bad[last] ^= 1;
            files.raw.put(&path, Bytes::from(bad).into()).await.unwrap();
        }
    }
    // A fresh disposable cache must expose the corrupted remote block as error.
    let cold = Table(Arc::new(Inner {
        storage: files,
        cache: cache(),
        data: table.0.data,
        blocks: codec::decode_index(&codec::index(&table.0.blocks), table.0.data.size()).unwrap(),
    }));
    assert!(cold.get(b"a", 3).await.is_err());
}
#[tokio::test]
async fn structural_index_validation_rejects_bad_extents_order_counts_and_bounds() {
    let files = Files::new();
    let reference = build(
        files.clone(),
        [Row {
            key: b"a",
            revision: 3,
            value: Some(b"value"),
        }],
    )
    .await
    .unwrap();
    let table = Table::open(files, cache(), reference).await.unwrap();
    let index = codec::index(&table.0.blocks);
    for end in 0..index.len() {
        assert!(codec::decode_index(&index[..end], table.0.data.size()).is_err());
    }
    for n in [0, 8, 13, 20, 24, 31, 34, 42, 46] {
        let mut bad = index.to_vec();
        bad[n] ^= 128;
        assert!(codec::decode_index(&bad, table.0.data.size()).is_err());
    }
    let mut extra = index.to_vec();
    extra.push(0);
    assert!(codec::decode_index(&extra, table.0.data.size()).is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    #[test]
    fn lookup_and_scan_match_a_revisioned_reference_map(input in proptest::collection::vec((1u8..8,1u64..16,proptest::option::of(proptest::collection::vec(proptest::num::u8::ANY,0..1024))),1..160)) {
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut map=BTreeMap::new();
            for (key,revision,value) in input { map.insert((key,std::cmp::Reverse(revision)),value); }
            let keys:Vec<_>=map.keys().map(|(key,_)|vec![*key]).collect();
            let rows=map.iter().zip(&keys).map(|(((_,revision),value),key)|Row {key,revision:revision.0,value:value.as_deref()});
            let files=Files::new();
            let reference=build(files.clone(),rows).await.unwrap();
            let table=Table::open(files,cache(),reference).await.unwrap();
            for snapshot in [0,1,5,10,15,16] {
                let expected: BTreeMap<_,_>=(1u8..8).filter_map(|key|map.iter().find(|((k,r),_)|*k==key && r.0<=snapshot).map(|((_,r),v)|(key,(r.0,v.clone())))).collect();
                for key in 1u8..9 {
                    let actual=table.get(&[key],snapshot).await.unwrap().map(|e|(e.revision,e.value.map(|v|v.to_vec())));
                    assert!(actual==expected.get(&key).cloned());
                }
                let actual=collected(table.scan(&[2],Some(&[6]),snapshot).unwrap()).await;
                let expected:Vec<_>=expected.range(2..6).map(|(k,(r,v))|(Bytes::copy_from_slice(&[*k]),*r,v.as_ref().map(|v|Bytes::copy_from_slice(v)))).collect();
                assert!(actual==expected);
            }
        });
    }
}

#[tokio::test]
async fn disk_cache_is_disposable_and_corruption_refetches_the_authenticated_block() {
    let files = Files::new();
    let directory = tempfile::tempdir().unwrap();
    let cache = Arc::new(Cache::new(0, Some((directory.path().to_owned(), 256 * 1024))).unwrap());
    let reference = build(
        files.clone(),
        [Row {
            key: b"a",
            revision: 1,
            value: Some(b"value"),
        }],
    )
    .await
    .unwrap();
    let table = Table::open(files.clone(), cache.clone(), reference.clone())
        .await
        .unwrap();
    table.get(b"a", 1).await.unwrap();
    let gets = files.reads.load(AtomicOrder::SeqCst);
    table.get(b"a", 1).await.unwrap();
    assert_eq!(files.reads.load(AtomicOrder::SeqCst), gets);
    for entry in std::fs::read_dir(directory.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().len() == 64 {
            std::fs::write(entry.path(), b"torn cache write").unwrap();
        }
    }
    assert!(table.get(b"a", 1).await.unwrap().is_some());
    assert_eq!(files.reads.load(AtomicOrder::SeqCst), gets + 1);
    assert_eq!(cache.memory_bytes().await, 0);
    drop((table, cache));
    let cache = Arc::new(Cache::new(0, Some((directory.path().to_owned(), 256 * 1024))).unwrap());
    let table = Table::open(files.clone(), cache, reference).await.unwrap();
    let gets = files.reads.load(AtomicOrder::SeqCst);
    table.get(b"a", 1).await.unwrap();
    assert_eq!(files.reads.load(AtomicOrder::SeqCst), gets);
}

#[tokio::test]
async fn local_directory_reopen_preserves_maximum_rows_and_empty_values() {
    let directory = tempfile::tempdir().unwrap();
    let files = Arc::new(crate::storage::Local::open(directory.path().to_owned()).unwrap());
    let key = vec![b'k'; MAX_KEY_BYTES];
    let value = vec![7; 64 * 1024];
    let reference = build(
        files.clone(),
        [
            Row {
                key: &key,
                revision: 3,
                value: Some(&value),
            },
            Row {
                key: &key,
                revision: 2,
                value: None,
            },
            Row {
                key: &key,
                revision: 1,
                value: Some(b""),
            },
        ],
    )
    .await
    .unwrap()
    .encode();
    drop(files);
    let files = Arc::new(crate::storage::Local::open(directory.path().to_owned()).unwrap());
    let table = Table::open(
        files.clone(),
        cache(),
        Reference::decode(&reference).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        table
            .get(&key, 3)
            .await
            .unwrap()
            .unwrap()
            .value
            .unwrap()
            .as_ref()
            == value
    );
    assert!(table.get(&key, 2).await.unwrap().unwrap().value.is_none());
    assert!(
        table
            .get(&key, 1)
            .await
            .unwrap()
            .unwrap()
            .value
            .unwrap()
            .is_empty()
    );
    assert!(
        build(
            files.clone(),
            [Row {
                key: &vec![0; MAX_KEY_BYTES + 1],
                revision: 1,
                value: None
            }]
        )
        .await
        .is_err()
    );
    assert!(
        build(
            files,
            [Row {
                key: b"k",
                revision: 1,
                value: Some(&vec![0; 65537])
            }]
        )
        .await
        .is_err()
    );
}
