use super::*;
use crate::{
    cache::Cache,
    index::{merge, table},
    storage::Remote,
    transactions::record::{Database, Mutation},
};
use std::sync::Arc;
use uuid::Uuid;
fn record(revision: u64, mutations: &[Mutation<'_>]) -> Bytes {
    crate::transactions::recovery::tests::record(
        Database {
            name: "app",
            generation: Uuid::from_u128(8),
        },
        revision,
        &format!("r-{revision}"),
        mutations,
    )
}
fn append(memory: &mut Memory, revision: u64, mutations: &[Mutation<'_>]) -> anyhow::Result<()> {
    memory.append(&Commit::decode(&record(revision, mutations))?)
}
fn memory() -> Memory {
    Memory::new(Limits {
        versions: 1024,
        bytes: 1024 * 1024,
    })
    .unwrap()
}

#[tokio::test]
async fn recovered_versions_flush_and_merge_without_losing_deletions_or_empty_values() {
    let mut m = memory();
    append(
        &mut m,
        1,
        &[
            Mutation {
                key: "a",
                value: Some(b"old"),
            },
            Mutation {
                key: "b",
                value: Some(b""),
            },
        ],
    )
    .unwrap();
    let files =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "memory").unwrap());
    let reference = table::build(files.clone(), m.rows()).await.unwrap();
    let old = table::Table::open(files, Arc::new(Cache::new(0, None).unwrap()), reference)
        .await
        .unwrap();
    append(
        &mut m,
        2,
        &[
            Mutation {
                key: "a",
                value: None,
            },
            Mutation {
                key: "c",
                value: Some(b"new"),
            },
        ],
    )
    .unwrap();
    append(&mut m, 3, &[]).unwrap();
    assert_eq!(
        m.usage(),
        Usage {
            versions: 4,
            bytes: 10
        }
    );
    for revision in 0..=3 {
        let mut merged =
            merge::Cursor::with_memory(std::slice::from_ref(&old), &m, b"", None, revision)
                .unwrap();
        let expected: Vec<_> = m
            .scan(b"", None, revision)
            .unwrap()
            .map(|r| (r.key, r.revision, r.value))
            .collect();
        let mut actual = Vec::new();
        while let Some(row) = merged.next().await.unwrap() {
            actual.push((row.key, row.revision, row.value));
        }
        assert_eq!(actual, expected);
        for key in [b"a", b"b", b"c", b"d"] {
            let actual = merge::get_with_memory(std::slice::from_ref(&old), &m, key, revision)
                .await
                .unwrap()
                .map(|r| (r.revision, r.value));
            let expected = m.get(key, revision).unwrap().map(|r| (r.revision, r.value));
            assert_eq!(actual, expected);
        }
    }
    assert!(m.get(b"a", 2).unwrap().unwrap().value.is_none());
    assert_eq!(m.get(b"b", 2).unwrap().unwrap().value, Some(Bytes::new()));
    assert_eq!(m.scan(b"b", Some(b"c"), 2).unwrap().count(), 1);
    assert!(m.scan(b"z", Some(b"a"), 1).is_err());
    assert!(m.get(b"", 1).is_err());
    m.reclaim_versions();
    assert_eq!(m.floor(), 3);
    assert_eq!(
        m.usage(),
        Usage {
            versions: 3,
            bytes: 6
        }
    );
    for revision in 0..3 {
        assert!(m.get(b"a", revision).is_err());
        assert!(m.scan(b"", None, revision).is_err());
        assert!(m.scan_ordered(b"", None, revision, true).is_err());
        assert!(
            merge::get_with_memory(std::slice::from_ref(&old), &m, b"a", revision)
                .await
                .is_err()
        );
    }
    // Pruning must not resurrect the table's deleted value or drop an empty
    // value. Both scan directions and the merged view retain the same result.
    let expected: Vec<_> = m
        .scan(b"", None, 3)
        .unwrap()
        .map(|r| (r.key, r.value))
        .collect();
    assert_eq!(
        expected,
        vec![
            (Bytes::from("a"), None),
            (Bytes::from("b"), Some(Bytes::new())),
            (Bytes::from("c"), Some(Bytes::from("new")))
        ]
    );
    let reverse: Vec<_> = m
        .scan_ordered(b"", None, 3, true)
        .unwrap()
        .map(|r| (r.key, r.value))
        .collect();
    assert_eq!(reverse, expected.iter().rev().cloned().collect::<Vec<_>>());
    let mut merged =
        merge::Cursor::with_memory(std::slice::from_ref(&old), &m, b"", None, 3).unwrap();
    for row in &expected {
        let actual = merged.next().await.unwrap().unwrap();
        assert_eq!(&(actual.key, actual.value), row);
    }
    assert!(merged.next().await.unwrap().is_none());
    drop(merged);
    let usage = m.usage();
    append(&mut m, 4, &[]).unwrap();
    m.reclaim_versions();
    assert_eq!(m.floor(), 3, "no removal needlessly expires a snapshot");
    assert_eq!(m.usage(), usage);
    assert!(
        merge::Cursor::with_memory(&vec![old.clone(); merge::MAX_SOURCES], &m, b"", None, 3)
            .is_err()
    );
    assert!(
        merge::get_with_memory(&vec![old; merge::MAX_SOURCES], &m, b"a", 3)
            .await
            .is_err()
    );
}

#[test]
fn admission_checks_all_mutations_before_changing_rows_or_revision() {
    for limits in [
        Limits {
            versions: 1,
            bytes: 100,
        },
        Limits {
            versions: 100,
            bytes: 5,
        },
    ] {
        let mut m = Memory::new(limits).unwrap();
        append(
            &mut m,
            1,
            &[Mutation {
                key: "a",
                value: Some(b"one"),
            }],
        )
        .unwrap();
        let usage = m.usage();
        assert!(
            append(
                &mut m,
                2,
                &[
                    Mutation {
                        key: "a",
                        value: None
                    },
                    Mutation {
                        key: "b",
                        value: Some(b"bad")
                    }
                ]
            )
            .is_err()
        );
        assert_eq!(m.usage(), usage);
        assert_eq!(
            m.get(b"a", 2).unwrap().unwrap().value.as_deref(),
            Some(b"one".as_slice())
        );
        assert!(m.get(b"b", 2).unwrap().is_none());
        // The rejected append did not consume a revision; an empty commit fits.
        append(&mut m, 2, &[]).unwrap();
        assert!(append(&mut m, 2, &[]).is_err());
        assert!(append(&mut m, 4, &[]).is_err());
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn history_snapshots_match_reference(
        changes in proptest::collection::vec((0usize..8,proptest::option::of(proptest::collection::vec(proptest::num::u8::ANY,0..20))),1..120),
        snapshot in 0u64..130,
    ) {
        let mut m = memory();
        let mut oracle = BTreeMap::new();
        for (i,(key,value)) in changes.iter().enumerate() {
            let revision = i as u64 + 1;
            let key = format!("k-{key}");
            append(&mut m,revision,&[Mutation{key:&key,value:value.as_deref()}]).unwrap();
            if revision <= snapshot { oracle.insert(Bytes::from(key),(revision,value.clone().map(Bytes::from))); }
        }
        let actual: BTreeMap<_,_> = m.scan(b"",None,snapshot).unwrap().map(|r|(r.key,(r.revision,r.value))).collect();
        assert_eq!(actual,oracle);
        let reverse: Vec<_> = m.scan_ordered(b"",None,snapshot,true).unwrap().map(|r|(r.key,(r.revision,r.value))).collect();
        assert_eq!(reverse,oracle.iter().rev().map(|(k,v)|(k.clone(),v.clone())).collect::<Vec<_>>());
        for key in (0..10).map(|k|format!("k-{k}")) {
            assert_eq!(m.get(key.as_bytes(),snapshot).unwrap().map(|r|(r.revision,r.value)),oracle.get(key.as_bytes()).cloned());
        }
    }
}

#[tokio::test]
async fn a_newer_memory_version_dominates_conflicting_older_table_versions() {
    let files = Arc::new(
        Remote::new(Arc::new(object_store::memory::InMemory::new()), "conflicts").unwrap(),
    );
    let mut tables = Vec::new();
    for value in [Some(b"old".as_slice()), None] {
        let reference = table::build(
            files.clone(),
            [table::Row {
                key: b"a",
                revision: 1,
                value,
            }],
        )
        .await
        .unwrap();
        tables.push(
            table::Table::open(
                files.clone(),
                Arc::new(Cache::new(0, None).unwrap()),
                reference,
            )
            .await
            .unwrap(),
        );
    }
    let mut m = memory();
    append(&mut m, 1, &[]).unwrap();
    append(
        &mut m,
        2,
        &[Mutation {
            key: "a",
            value: Some(b"new"),
        }],
    )
    .unwrap();
    assert!(merge::get_with_memory(&tables, &m, b"a", 1).await.is_err());
    let row = merge::get_with_memory(&tables, &m, b"a", 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.value.as_deref(), Some(b"new".as_slice()));
    let mut scan = merge::Cursor::with_memory(&tables, &m, b"", None, 2).unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().revision, 2);
    assert!(scan.next().await.unwrap().is_none());
}
