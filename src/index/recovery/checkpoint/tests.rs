use super::*;
use crate::storage::{Object, Remote, Version};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
struct Files {
    inner: Remote,
    puts: AtomicUsize,
    range_reads: AtomicUsize,
    ranges: std::sync::Mutex<std::collections::BTreeSet<String>>,
    lose: AtomicUsize,
    pause: AtomicUsize,
    entered: tokio::sync::Semaphore,
}
impl Files {
    fn new(lose: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Remote::new(
                Arc::new(object_store::memory::InMemory::new()),
                "checkpoint",
            )
            .unwrap(),
            puts: AtomicUsize::new(0),
            range_reads: AtomicUsize::new(0),
            ranges: Default::default(),
            lose: AtomicUsize::new(lose),
            pause: AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
        })
    }
}
#[async_trait]
impl Storage for Files {
    async fn get(&self, k: &str, m: usize) -> anyhow::Result<Option<Object>> {
        self.inner.get(k, m).await
    }
    async fn get_range(&self, k: &str, r: std::ops::Range<u64>, s: u64) -> anyhow::Result<Bytes> {
        self.range_reads.fetch_add(1, Ordering::SeqCst);
        self.ranges.lock().unwrap().insert(k.into());
        self.inner.get_range(k, r, s).await
    }
    async fn put(&self, k: &str, b: Bytes) -> anyhow::Result<()> {
        self.inner.put(k, b).await?;
        let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.pause.load(Ordering::SeqCst) {
            self.entered.add_permits(1);
            futures::future::pending::<()>().await;
        }
        ensure!(n != self.lose.load(Ordering::SeqCst), "lost upload reply");
        Ok(())
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        panic!("checkpoint data never uses conditional file PUT")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        panic!("checkpoint recovery never lists the bucket")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        panic!("checkpoint construction never collects")
    }
}
fn cut() -> Cut {
    // Generate the real prefix encoding without granting serving authority.
    let files = Files::new(0);
    let writer = crate::journal::Writer::for_root(
        files,
        Uuid::from_u128(9),
        crate::journal::QueueLimits::default(),
    );
    Cut {
        identity: Identity {
            epoch: 1,
            incarnation: Uuid::from_u128(9),
        },
        prefix: writer.durable_prefix(),
    }
}
fn prefix() -> Vec<u8> {
    let mut out = b"app\0".to_vec();
    out.extend_from_slice(Uuid::from_u128(7).as_bytes());
    out
}
fn meta(revision: u64, floor: u64) -> Bytes {
    let mut out = revision.to_be_bytes().to_vec();
    out.extend_from_slice(&floor.to_be_bytes());
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(&0u64.to_be_bytes());
    out.into()
}
fn receipt(fingerprint: u8) -> Bytes {
    Bytes::from(vec![fingerprint; 32])
}
#[tokio::test]
async fn contribution_matches_encoded_rows_including_tombstones_and_block_splits() {
    use crate::transactions::{record::Commit, recovery::Target};
    let name = "d".repeat(64);
    let request = "r".repeat(128);
    let value = vec![42; 64 * 1024];
    let keys: Vec<_> = (0..128)
        .map(|n| format!("{n:03}{}", "k".repeat(1021)))
        .collect();
    for count in [0, 1, 128] {
        let files = Files::new(0);
        let db = Database {
            name: &name,
            generation: Uuid::from_u128(7),
        };
        let mutations: Vec<_> = keys[..count]
            .iter()
            .enumerate()
            .map(|(n, key)| Mutation {
                key,
                value: match n % 16 {
                    0 => None,
                    15 => Some(&value),
                    _ => Some(b""),
                },
            })
            .collect();
        let encoded = Commit::encode(db, 1, 0, 0, &request, [1; 32], &mutations).unwrap();
        let commit = Commit::decode(&encoded).unwrap();
        let mut projection = Projection::new(Limits::default()).unwrap();
        let state = projection
            .take_state(db)
            .await
            .unwrap()
            .advance(&commit)
            .unwrap();
        projection.apply(&commit, state).await.unwrap();
        let reference = build_projection(&projection, files.clone(), cut())
            .await
            .unwrap();
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        );
        let row_bytes = data_row_bytes(files.as_ref(), &snapshot).await;
        assert_eq!(row_bytes, contribution_bytes(db, &request, &mutations));
        snapshot.verify(Limits::default()).await.unwrap();
    }
}
async fn data_row_bytes(files: &dyn Storage, snapshot: &Snapshot) -> usize {
    let mut tables = snapshot.seek(Bytes::new()).unwrap();
    let mut bytes = 0;
    while let Some(table) = tables.next().await.unwrap() {
        let [data, index] = table.reference.blobs();
        let header = index.read_range(files, 0..index.size()).await.unwrap();
        let blocks = u32::from_be_bytes(header[8..12].try_into().unwrap());
        bytes += data.size() as usize - 12 * blocks as usize;
    }
    bytes
}
#[tokio::test]
async fn retained_receipt_admission_matches_rewritten_rows_and_resets_only_at_checkpoint() {
    use crate::transactions::{record::Commit, recovery::Target};
    let files = Files::new(0);
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(7),
    };
    let mut projection = Projection::new(Limits::default()).unwrap();
    for (revision, id) in [(1, "r".repeat(128)), (2, "short".into())] {
        let bytes = Commit::encode(db, revision, revision - 1, 0, &id, [1; 32], &[]).unwrap();
        let commit = Commit::decode(&bytes).unwrap();
        let state = projection
            .take_state(db)
            .await
            .unwrap()
            .advance(&commit)
            .unwrap();
        projection.apply(&commit, state).await.unwrap();
        assert_eq!(
            projection
                .view(db)
                .await
                .unwrap()
                .unwrap()
                .checkpoint_receipt_rewrite_bytes()
                .unwrap(),
            0
        );
    }
    let mut reference = build_projection(&projection, files.clone(), cut())
        .await
        .unwrap();
    for (revision, floor, id) in [(3, 0, "third"), (4, 2, "fourth")] {
        // Cold metadata comes from the checkpoint; receipt pricing adds no read.
        projection = Projection::new(Limits::default()).unwrap();
        projection.rebase(Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        ));
        projection.warm(db, 0, 0).await.unwrap();
        let reads = files.range_reads.load(Ordering::SeqCst);
        let retained = projection
            .view(db)
            .await
            .unwrap()
            .unwrap()
            .checkpoint_receipt_rewrite_bytes()
            .unwrap();
        assert_eq!(files.range_reads.load(Ordering::SeqCst), reads);
        assert!(retained > 0);
        let admitted = contribution_bytes(db, id, &[]) + retained;
        let bytes = Commit::encode(db, revision, revision - 1, floor, id, [2; 32], &[]).unwrap();
        let commit = Commit::decode(&bytes).unwrap();
        let state = projection
            .take_state(db)
            .await
            .unwrap()
            .advance(&commit)
            .unwrap();
        projection.apply(&commit, state).await.unwrap();
        assert_eq!(
            projection
                .view(db)
                .await
                .unwrap()
                .unwrap()
                .checkpoint_receipt_rewrite_bytes()
                .unwrap(),
            0
        );
        reference = build_projection(&projection, files.clone(), cut())
            .await
            .unwrap();
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        );
        let actual = data_row_bytes(files.as_ref(), &snapshot).await;
        if floor == 0 {
            assert_eq!(actual, admitted);
        } else {
            assert!(
                actual < admitted,
                "expired base receipts remain conservatively charged"
            );
        }
        snapshot.verify(Limits::default()).await.unwrap();
    }
}
async fn image(files: Arc<dyn Storage>, bad: u8) -> anyhow::Result<Reference> {
    let mut builder = Builder::new(files.clone());
    let prefix = prefix();
    let mut metadata = meta(2, 0).to_vec();
    if bad == 5 {
        // Structurally valid but incorrect counts must fail a full-image audit.
        metadata[16..24].copy_from_slice(&1u64.to_be_bytes());
        metadata[24..32].copy_from_slice(&2u64.to_be_bytes());
    }
    builder
        .push(&prefix, 0, b"", 2, Some(metadata.into()))
        .await?;
    if bad != 1 {
        builder
            .push(&prefix, 1, b"first", 1, Some(receipt(1)))
            .await?;
    }
    builder
        .push(
            &prefix,
            1,
            b"second",
            if bad == 2 { 1 } else { 2 },
            Some(receipt(2)),
        )
        .await?;
    builder
        .push(&prefix, 2, b"x", if bad == 3 { 3 } else { 2 }, None)
        .await?;
    builder.flush().await?;
    let tables = builder.tree.tables;
    let tree = builder.tree.finish().await?;
    Ok(Reference {
        cut: cut(),
        tree,
        tables,
        rows: builder.rows + u64::from(bad == 4),
        databases: 1,
    })
}
#[tokio::test]
async fn unknown_upload_at_every_stage_never_returns_a_checkpoint_reference() {
    for lose in 1..=3 {
        let files = Files::new(lose);
        assert!(image(files.clone(), 0).await.is_err());
        assert_eq!(files.puts.load(Ordering::SeqCst), lose);
        assert_eq!(
            files.inner.list("tables/", None, 10).await.unwrap().len(),
            lose
        );
    }
}
#[tokio::test]
async fn receipts_counts_bounds_and_missing_or_corrupt_objects_fail_closed() {
    for bad in 1..=5 {
        let files = Files::new(0);
        let reference = image(files.clone(), bad).await.unwrap();
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        );
        let database = snapshot
            .database(Database {
                name: "app",
                generation: Uuid::from_u128(7),
            })
            .await
            .unwrap()
            .unwrap();
        if bad <= 2 {
            assert!(database.restore_state(2, 1000).await.is_err());
        } else if bad == 3 {
            assert!(database.get("x").await.is_err());
        }
        assert!(load(files, reference, Limits::default()).await.is_err());
    }
    let files = Files::new(0);
    let reference = image(files.clone(), 0).await.unwrap();
    for limits in [
        Limits {
            receipts: 1,
            ..Limits::default()
        },
        Limits {
            receipt_bytes: 1,
            ..Limits::default()
        },
        Limits {
            value_bytes: 1,
            ..Limits::default()
        },
    ] {
        let result = load(files.clone(), reference, limits).await;
        // One tombstone key fits exactly one value byte.
        assert_eq!(result.is_ok(), limits.value_bytes == 1);
    }
    let root = reference.tree.unwrap().1;
    let mut encoded = Vec::new();
    root.encode(&mut encoded);
    let key = format!(
        "tables/{}",
        Uuid::from_slice(&encoded[..16]).unwrap().simple()
    );
    let original = files
        .inner
        .get(&key, 144 * 1024)
        .await
        .unwrap()
        .unwrap()
        .bytes;
    files
        .inner
        .put(&key, Bytes::from(vec![0; original.len()]))
        .await
        .unwrap();
    assert!(
        load(files.clone(), reference, Limits::default())
            .await
            .is_err()
    );
    files.inner.delete(&key).await.unwrap();
    assert!(
        load(files.clone(), reference, Limits::default())
            .await
            .is_err()
    );
    files.inner.put(&key, original).await.unwrap();
    let projection = load(files.clone(), reference, Limits::default())
        .await
        .unwrap();
    let view = projection
        .view(Database {
            name: "app",
            generation: Uuid::from_u128(7),
        })
        .await
        .unwrap()
        .unwrap();
    assert!(view.get(b"x", 1).await.is_err());
    assert!(view.get(b"x", 2).await.unwrap().unwrap().value.is_none());
    assert_eq!(
        view.resolve("first", [1; 32], 0).await.unwrap(),
        crate::transactions::recovery::Resolution::Committed(1)
    );
}
#[tokio::test]
async fn manifest_reuse_preserves_order_and_only_uploads_changed_paths() {
    fn descriptor(template: &[u8], identity: u128) -> table::Reference {
        // Distinct valid descriptors; this test traverses the manifest only.
        let mut bytes = template.to_vec();
        bytes[8..24].copy_from_slice(Uuid::from_u128(identity).as_bytes());
        let end = bytes.len() - 32;
        let checksum = Sha256::digest(&bytes[..end]);
        bytes[end..].copy_from_slice(&checksum);
        table::Reference::decode(&bytes).unwrap()
    }
    let files = Files::new(0);
    let template = table::build(
        files.clone(),
        [Row {
            key: b"x",
            revision: 1,
            value: None,
        }],
    )
    .await
    .unwrap()
    .encode();
    for count in [0, 1, 63, 64, 65, 4095, 4096, 4097, 8193] {
        let mut builder = tree::Builder::new(files.clone());
        let mut expected = Vec::new();
        for n in 0..count {
            let reference = descriptor(&template, n + 1);
            expected.push(reference.encode());
            builder
                .push(reference, Bytes::from(format!("key-{n:08}")))
                .await
                .unwrap();
        }
        let root = builder.finish().await.unwrap();
        let reads = files.range_reads.load(Ordering::SeqCst);
        let mut reader = tree::Reader::new(files.clone(), root).unwrap();
        for expected in &expected {
            assert_eq!(
                reader.next().await.unwrap().unwrap().reference.encode(),
                *expected
            );
        }
        assert!(reader.next().await.unwrap().is_none());
        let traversal_reads = files.range_reads.load(Ordering::SeqCst) - reads;
        if count == 8193 {
            assert_eq!(traversal_reads, 133);
        }
        for changed in [false, true] {
            let reads = files.range_reads.load(Ordering::SeqCst);
            let puts = files.puts.load(Ordering::SeqCst);
            let mut reader = tree::Reader::new(files.clone(), root).unwrap();
            let mut builder = tree::Builder::new(files.clone());
            let mut rewritten = expected.clone();
            let mut n = 0;
            while let Some(mut routed) = reader.next().await.unwrap() {
                builder.reuse_from(&reader);
                if changed && n == count as usize / 2 {
                    routed.reference = descriptor(&template, count + 1);
                    rewritten[n] = routed.reference.encode();
                }
                builder.push(routed.reference, routed.last).await.unwrap();
                n += 1;
            }
            let next = builder.finish().await.unwrap();
            assert_eq!(
                files.range_reads.load(Ordering::SeqCst) - reads,
                traversal_reads,
                "reuse adds no provider reads to the existing traversal"
            );
            assert_eq!(
                files.puts.load(Ordering::SeqCst) - puts,
                if changed {
                    root.map_or(0, |(height, _)| height as usize + 1)
                } else {
                    0
                },
                "only the changed leaf and its ancestors are uploaded"
            );
            if !changed {
                assert!(
                    next == root,
                    "unchanged manifests retain their exact identity"
                );
            }
            let mut reader = tree::Reader::new(files.clone(), next).unwrap();
            for expected in rewritten {
                assert_eq!(
                    reader.next().await.unwrap().unwrap().reference.encode(),
                    expected
                );
            }
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn cancelling_a_completed_manifest_upload_does_not_return_a_publishable_handle() {
    let files = Files::new(0);
    files.pause.store(3, Ordering::SeqCst);
    tokio::select! {
        result=image(files.clone(),0)=>panic!("paused upload completed: {}",result.is_ok()),
        permit=files.entered.acquire()=>permit.unwrap().forget(),
    }
    assert_eq!(
        files.inner.list("tables/", None, 10).await.unwrap().len(),
        3
    );
    files.pause.store(0, Ordering::SeqCst);
    let reference = image(files.clone(), 0).await.unwrap();
    assert_eq!(
        files.inner.list("tables/", None, 10).await.unwrap().len(),
        6
    );
    assert!(
        load(files.clone(), reference, Limits::default())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn local_checkpoint_reopens_without_process_memory_or_cache() {
    let dir = tempfile::tempdir().unwrap();
    let files = Arc::new(crate::storage::Local::open(dir.path().to_owned()).unwrap());
    let reference = image(files, 0).await.unwrap();
    let files = Arc::new(crate::storage::Local::open(dir.path().to_owned()).unwrap());
    let projection = load(files, reference, Limits::default()).await.unwrap();
    let view = projection
        .view(Database {
            name: "app",
            generation: Uuid::from_u128(7),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.revision(), 2);
    assert!(view.get(b"x", 2).await.unwrap().unwrap().value.is_none());
    assert_eq!(
        view.resolve("second", [2; 32], 1).await.unwrap(),
        crate::transactions::recovery::Resolution::Committed(2)
    );
}

#[tokio::test]
async fn malformed_routing_nodes_retire_the_reader_and_maximum_bounds_roundtrip() {
    let files = Files::new(0);
    let reference = table::build(
        files.clone(),
        [Row {
            key: b"x",
            revision: 1,
            value: None,
        }],
    )
    .await
    .unwrap();
    let key = Bytes::from(vec![255; 2048]);
    let mut builder = tree::Builder::new(files.clone());
    builder.push(reference.clone(), key.clone()).await.unwrap();
    let mut reader = tree::Reader::new(files.clone(), builder.finish().await.unwrap()).unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().last, key);
    assert!(reader.next().await.unwrap().is_none());
    let mut leaf = b"DDBCPN01".to_vec();
    leaf.extend_from_slice(&[0, 1]);
    leaf.extend_from_slice(&1u16.to_be_bytes());
    leaf.extend_from_slice(b"a");
    leaf.extend_from_slice(&reference.encode());
    let child = Blob::upload(files.clone(), leaf.clone().into())
        .await
        .unwrap();
    let mut wrong_parent = b"DDBCPN01".to_vec();
    wrong_parent.extend_from_slice(&[1, 1]);
    wrong_parent.extend_from_slice(&1u16.to_be_bytes());
    wrong_parent.extend_from_slice(b"z");
    child.encode(&mut wrong_parent);
    let mut trailing = leaf.clone();
    trailing.push(0);
    let mut too_long = leaf.clone();
    too_long[10..12].copy_from_slice(&2049u16.to_be_bytes());
    let mut bad_count = leaf;
    bad_count[9] = 0;
    for (height, bytes) in [
        (1, wrong_parent),
        (0, trailing),
        (0, too_long),
        (0, bad_count),
    ] {
        let node = Blob::upload(files.clone(), bytes.into()).await.unwrap();
        let mut reader = tree::Reader::new(files.clone(), Some((height, node))).unwrap();
        assert!(reader.next().await.is_err());
        assert!(reader.next().await.is_err());
    }
}

#[tokio::test]
async fn rewrites_reuse_untouched_tables_and_replace_expired_receipt_windows() {
    use crate::transactions::{
        record::{Commit, Mutation},
        recovery::{Resolution, Target},
    };
    use std::collections::{BTreeMap, BTreeSet};
    async fn references(snapshot: &Snapshot) -> BTreeSet<Bytes> {
        let mut manifest = snapshot.seek(Bytes::new()).unwrap();
        let mut references = BTreeSet::new();
        while let Some(table) = manifest.next().await.unwrap() {
            references.insert(table.reference.encode());
        }
        references
    }
    let files = Files::new(0);
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(7),
    };
    let prefix = prefix();
    let mut builder = Builder::new(files.clone());
    let mut metadata = meta(2, 0).to_vec();
    metadata[16..24].copy_from_slice(&160u64.to_be_bytes());
    metadata[24..32].copy_from_slice(&(160u64 * (8 + 256)).to_be_bytes());
    builder
        .push(&prefix, 0, b"", 2, Some(metadata.into()))
        .await
        .unwrap();
    // Separate metadata from receipt rows: expired receipts must be removed
    // even when their table contains no key present in the new receipt window.
    builder.flush().await.unwrap();
    for (id, revision) in [(b"first".as_slice(), 1), (b"second", 2)] {
        builder
            .push(&prefix, 1, id, revision, Some(receipt(revision as u8)))
            .await
            .unwrap();
    }
    builder.flush().await.unwrap();
    let mut expected = BTreeMap::new();
    for n in 0..160 {
        let key = format!("key/{n:04}");
        let value = Bytes::from(vec![42; 256]);
        builder
            .push(&prefix, 2, key.as_bytes(), 2, Some(value.clone()))
            .await
            .unwrap();
        expected.insert(key, value);
        if n % 32 == 31 {
            builder.flush().await.unwrap();
        }
    }
    let tables = builder.tree.tables;
    let reference = Reference {
        cut: cut(),
        tables,
        tree: builder.tree.finish().await.unwrap(),
        rows: builder.rows,
        databases: 1,
    };
    let snapshot = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    );
    snapshot.verify(Limits::default()).await.unwrap();
    let old = references(&snapshot).await;
    assert_eq!(old.len(), 7);
    let mut projection = Projection::new(Limits::default()).unwrap();
    projection.rebase(snapshot.clone());
    // Exact retries and refused writes may warm receipts without a commit.
    assert_eq!(projection.warm(db, 0, 0).await.unwrap(), (2, 0));
    assert_eq!(projection.usage().receipts, 2);
    let puts = files.puts.load(Ordering::SeqCst);
    let reads = files.range_reads.load(Ordering::SeqCst);
    files.ranges.lock().unwrap().clear();
    let unchanged = build_projection(&projection, files.clone(), cut())
        .await
        .unwrap();
    assert_eq!(
        files.puts.load(Ordering::SeqCst) - puts,
        0,
        "unchanged tables and manifest need no uploads; the cut is inline"
    );
    assert_eq!(
        files.range_reads.load(Ordering::SeqCst) - reads,
        1,
        "one manifest read, no retained table indexes or data"
    );
    for reference in &old {
        let key = format!(
            "tables/{}",
            Uuid::from_slice(&reference[8..24]).unwrap().simple()
        );
        assert!(
            !files.ranges.lock().unwrap().contains(&key),
            "no reused data-block reads"
        );
    }
    let unchanged = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        unchanged,
    );
    assert_eq!(references(&unchanged).await, old);
    snapshot.verify(Limits::default()).await.unwrap();
    let encoded = Commit::encode(
        db,
        3,
        2,
        2,
        "third",
        [3; 32],
        &[
            Mutation {
                key: "key/0031",
                value: None,
            },
            Mutation {
                key: "key/0032",
                value: Some(b""),
            },
            Mutation {
                key: "key/0040",
                value: Some(b"x"),
            },
            Mutation {
                key: "key/0160",
                value: Some(b"y"),
            },
        ],
    )
    .unwrap();
    let commit = Commit::decode(&encoded).unwrap();
    let state = projection
        .take_state(db)
        .await
        .unwrap()
        .advance(&commit)
        .unwrap();
    projection.apply(&commit, state).await.unwrap();
    for name in ["aaa", "zzz"] {
        let db = Database {
            name,
            generation: db.generation,
        };
        let encoded = Commit::encode(
            db,
            1,
            0,
            0,
            "new",
            [1; 32],
            &[Mutation {
                key: "x",
                value: Some(b""),
            }],
        )
        .unwrap();
        let commit = Commit::decode(&encoded).unwrap();
        let state = projection
            .take_state(db)
            .await
            .unwrap()
            .advance(&commit)
            .unwrap();
        projection.apply(&commit, state).await.unwrap();
    }
    let puts = files.puts.load(Ordering::SeqCst);
    let reference = build_projection(&projection, files.clone(), cut())
        .await
        .unwrap();
    assert_eq!(files.puts.load(Ordering::SeqCst) - puts, 5);
    let rewritten = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    );
    assert_eq!(references(&rewritten).await.intersection(&old).count(), 3);
    // Cancelling after an upload can strand fresh bytes, but exposes no new
    // reference and cannot damage either the source or the completed image.
    files
        .pause
        .store(files.puts.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    tokio::select! {
        result = build_projection(&projection, files.clone(), cut()) => panic!("paused build completed: {}", result.is_ok()),
        permit = files.entered.acquire() => permit.unwrap().forget(),
    }
    files.pause.store(0, Ordering::SeqCst);
    rewritten.verify(Limits::default()).await.unwrap();
    assert_eq!(rewritten.header().databases, 3);
    for name in ["aaa", "zzz"] {
        let view = rewritten
            .database(Database {
                name,
                generation: db.generation,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            view.get("x").await.unwrap().unwrap().value,
            Some(Bytes::new())
        );
    }
    let view = rewritten.database(db).await.unwrap().unwrap();
    assert_eq!(
        view.resolve("first", [1; 32], 0).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.resolve("second", [2; 32], 1).await.unwrap(),
        Resolution::Expired
    );
    assert_eq!(
        view.resolve("third", [3; 32], 2).await.unwrap(),
        Resolution::Committed(3)
    );
    expected.remove("key/0031");
    expected.insert("key/0032".into(), Bytes::new());
    expected.insert("key/0040".into(), Bytes::from_static(b"x"));
    expected.insert("key/0160".into(), Bytes::from_static(b"y"));
    for (key, value) in &expected {
        assert_eq!(
            view.get(key).await.unwrap().unwrap().value.as_ref(),
            Some(value)
        );
    }
    assert!(view.get("key/0031").await.unwrap().unwrap().value.is_none());
    assert_eq!(view.statistics().keys, expected.len() as u64);
    assert_eq!(
        view.statistics().bytes,
        expected
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum::<u64>()
    );
    assert!(
        snapshot
            .database(db)
            .await
            .unwrap()
            .unwrap()
            .get("key/0031")
            .await
            .unwrap()
            .unwrap()
            .value
            .is_some()
    );
    // A changed neighbor can share a table with warmed, unchanged metadata.
    // Rebuilding that table must retain the clean database's receipt window.
    let mut manifest = rewritten.seek(Bytes::new()).unwrap();
    let first = manifest.next().await.unwrap().unwrap();
    let table = rewritten.table(&first).await.unwrap();
    assert_eq!(split_key(table.first_key()).unwrap().0.name, "aaa");
    assert_eq!(split_key(table.last_key()).unwrap().0.name, "app");
    let mut mixed = Projection::new(Limits::default()).unwrap();
    mixed.rebase(rewritten.clone());
    mixed.warm(db, 0, 0).await.unwrap();
    let neighbor = Database { name: "aaa", ..db };
    let encoded = Commit::encode(neighbor, 2, 1, 1, "again", [4; 32], &[]).unwrap();
    let commit = Commit::decode(&encoded).unwrap();
    let state = mixed
        .take_state(neighbor)
        .await
        .unwrap()
        .advance(&commit)
        .unwrap();
    mixed.apply(&commit, state).await.unwrap();
    let reference = build_projection(&mixed, files.clone(), cut())
        .await
        .unwrap();
    let mixed = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    );
    mixed.verify(Limits::default()).await.unwrap();
    assert_eq!(
        mixed
            .database(db)
            .await
            .unwrap()
            .unwrap()
            .resolve("third", [3; 32], 2)
            .await
            .unwrap(),
        Resolution::Committed(3)
    );
    let current = references(&rewritten).await;
    let retained = current.intersection(&old).next().unwrap();
    let index = table::Reference::decode(retained).unwrap().blobs()[1].key();
    files.inner.delete(&index).await.unwrap();
    // Retaining an authenticated reference is not a full-image audit. The
    // missing index still fails explicit verification, never becoming absence.
    let reference = build_projection(&projection, files.clone(), cut())
        .await
        .unwrap();
    assert!(
        Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference
        )
        .verify(Limits::default())
        .await
        .is_err()
    );
    let base = projection.base.as_ref().unwrap();
    let first = base
        .seek(Bytes::new())
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    files
        .inner
        .delete(&first.reference.blobs()[1].key())
        .await
        .unwrap();
    assert!(
        build_projection(&projection, files, cut()).await.is_err(),
        "a replaced table must be read and validated"
    );
}
