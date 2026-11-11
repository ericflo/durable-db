use super::*;
use crate::storage::{Object, Remote, Version};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
struct Files {
    inner: Remote,
    puts: AtomicUsize,
    uploaded: AtomicUsize,
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
            uploaded: AtomicUsize::new(0),
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
        self.uploaded.fetch_add(b.len(), Ordering::SeqCst);
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
async fn contribution_covers_complete_checkpoints_including_tombstones_and_splits() {
    use crate::transactions::{record::Commit, recovery::Target};
    // A possibly applied PUT spends its allowance even without a reply. An
    // overrun is rejected before the physical provider sees another request.
    let files = Files::new(1);
    let attempt = crate::upload_budget::Attempt::bounded(files.clone(), 3);
    assert!(
        attempt
            .put("lost", Bytes::from_static(b"ab"))
            .await
            .is_err()
    );
    assert!(
        attempt
            .put("over", Bytes::from_static(b"cd"))
            .await
            .is_err()
    );
    assert_eq!(files.puts.load(Ordering::SeqCst), 1);
    attempt.put("last", Bytes::from_static(b"e")).await.unwrap();
    assert!(attempt.put("empty", Bytes::new()).await.is_err());
    assert_eq!(files.uploaded.load(Ordering::SeqCst), 3);
    let name = "d".repeat(64);
    let value = vec![42; 64 * 1024];
    for (count, large, long) in [
        (0, false, false),
        (0, false, true),
        (1, false, true),
        (128, false, true),
        (128, true, true),
        (4097, false, false),
    ] {
        let keys: Vec<_> = (0..count)
            .map(|n| format!("{n:04}{}", "k".repeat(if long { 1020 } else { 0 })))
            .collect();
        let files = Files::new(0);
        let db = Database {
            name: if long { &name } else { "a" },
            generation: Uuid::from_u128(7),
        };
        let mutations: Vec<_> = keys[..count]
            .iter()
            .enumerate()
            .map(|(n, key)| Mutation {
                key,
                value: match if large { 15 } else { n % 16 } {
                    0 => None,
                    15 => Some(&value),
                    _ => Some(b""),
                },
            })
            .collect();
        let mut projection = Projection::new(Limits::default()).unwrap();
        let batches: Vec<&[Mutation<'_>]> = if mutations.is_empty() {
            vec![&[]]
        } else {
            mutations.chunks(if large { 8 } else { 128 }).collect()
        };
        let mut charged = 0;
        for (position, batch) in batches.into_iter().enumerate() {
            let request = format!("{position:03}{}", "r".repeat(if long { 125 } else { 0 }));
            let revision = position as u64 + 1;
            let encoded =
                Commit::encode(db, revision, revision - 1, 0, &request, [1; 32], batch).unwrap();
            let commit = Commit::decode(&encoded).unwrap();
            let state = projection
                .take_state(db)
                .await
                .unwrap()
                .advance(&commit)
                .unwrap();
            projection.apply(&commit, state).await.unwrap();
            charged += contribution_bytes(db, &request, batch);
        }
        let reserve =
            projection.checkpoint_charges().await.unwrap()[&(db.name.to_owned(), db.generation)];
        assert!(reserve <= charged as u64);
        let funded = crate::upload_budget::Attempt::bounded(files.clone(), reserve);
        let reference = build_projection(&projection, funded, cut()).await.unwrap();
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        );
        assert!(
            files.uploaded.load(Ordering::SeqCst) <= charged,
            "complete image exceeds admission"
        );
        if large || count > CHUNK_ROWS {
            assert!(reference.tables > 1, "fixture exercises table splits");
        }
        snapshot.verify(Limits::default()).await.unwrap();
    }
}
async fn encoded_table_bytes(snapshot: &Snapshot) -> u64 {
    let mut tables = snapshot.seek(Bytes::new()).unwrap();
    let mut bytes = 0;
    while let Some(table) = tables.next().await.unwrap() {
        bytes += table
            .reference
            .blobs()
            .iter()
            .map(|b| b.size())
            .sum::<u64>();
    }
    bytes
}
#[tokio::test]
async fn shared_copy_admission_covers_neighbors_new_databases_and_dirty_writes() {
    use crate::transactions::{record::Commit, recovery::Target};
    let files = Files::new(0);
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(7),
    };
    let mut projection = Projection::new(Limits::default()).unwrap();
    let value = vec![42; 64 * 1024];
    for name in ["app", "bee"] {
        let database = Database { name, ..db };
        let bytes = Commit::encode(
            database,
            1,
            0,
            0,
            "first",
            [1; 32],
            &[Mutation {
                key: "x",
                value: Some(&value),
            }],
        )
        .unwrap();
        let commit = Commit::decode(&bytes).unwrap();
        let state = projection
            .take_state(database)
            .await
            .unwrap()
            .advance(&commit)
            .unwrap();
        projection.apply(&commit, state).await.unwrap();
        assert_eq!(
            projection
                .checkpoint_rewrite_bytes(database, &[])
                .await
                .unwrap(),
            0
        );
    }
    let mut reference = build_projection(&projection, files.clone(), cut())
        .await
        .unwrap();
    assert_eq!(reference.tables, 1, "neighbors share a base table");
    for (revision, floor) in [(2, 0), (3, 1)] {
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(1024 * 1024, None).unwrap()),
            reference,
        );
        let routed = snapshot
            .seek(Bytes::new())
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let table_bytes = copy_contribution(routed.reference.blobs()[0].size());
        projection.rebase(snapshot);
        let mutations = [
            Mutation {
                key: "x",
                value: None,
            },
            Mutation {
                key: "x",
                value: Some(b""),
            },
        ];
        let reads = files.range_reads.load(Ordering::SeqCst);
        let copies = projection
            .checkpoint_rewrite_bytes(db, &mutations)
            .await
            .unwrap();
        assert_eq!(
            copies,
            table_bytes + reference.tree.unwrap().1.size(),
            "deduplicate receipt/value routes to the same table"
        );
        assert_eq!(
            files.range_reads.load(Ordering::SeqCst),
            reads,
            "cached manifest pricing reads no table"
        );
        assert_eq!(
            projection
                .checkpoint_rewrite_bytes(Database { name: "aaa", ..db }, &[])
                .await
                .unwrap(),
            copies,
            "new database in a gap must fund the shared table it displaces"
        );
        assert_eq!(
            projection
                .checkpoint_rewrite_bytes(Database { name: "zzz", ..db }, &[])
                .await
                .unwrap(),
            reference.tree.unwrap().1.size()
                + tree::growth_bytes(crate::index::MAX_KEY_BYTES) as u64,
            "append copies no existing rows but rewrites the rightmost manifest path"
        );
        let bytes = Commit::encode(
            db,
            revision,
            revision - 1,
            floor,
            if revision == 2 { "next" } else { "last" },
            [2; 32],
            &mutations[..1],
        )
        .unwrap();
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
                .checkpoint_rewrite_bytes(db, &mutations)
                .await
                .unwrap(),
            copies,
            "dirty writes conservatively pay again; no auxiliary paid-table state"
        );
        reference = build_projection(&projection, files.clone(), cut())
            .await
            .unwrap();
        let snapshot = Snapshot::new(
            files.clone(),
            Arc::new(Cache::new(0, None).unwrap()),
            reference,
        );
        assert!(
            encoded_table_bytes(&snapshot).await
                <= contribution_bytes(db, "next", &mutations[..1]) as u64 + copies
        );
        assert_eq!(
            snapshot
                .database(Database { name: "bee", ..db })
                .await
                .unwrap()
                .unwrap()
                .get("x")
                .await
                .unwrap()
                .unwrap()
                .value
                .as_deref(),
            Some(value.as_slice())
        );
        snapshot.verify(Limits::default()).await.unwrap();
    }
    let snapshot = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    );
    projection.rebase(snapshot);
    files
        .inner
        .delete(&reference.tree.unwrap().1.key())
        .await
        .unwrap();
    let puts = files.puts.load(Ordering::SeqCst);
    assert!(
        projection.checkpoint_rewrite_bytes(db, &[]).await.is_err(),
        "missing price evidence fails closed"
    );
    assert_eq!(files.puts.load(Ordering::SeqCst), puts);
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
    // Inserts/deletions cover 63/65 and 4095/4097 without building redundant bases.
    for (count, suffix) in [
        (0, 0),
        (1, 0),
        (64, 0),
        (65, 0),
        (4096, 0),
        (8193, 0),
        (65, 2036),
    ] {
        let mut builder = tree::Builder::new(files.clone());
        let mut expected = Vec::new();
        for n in 0..count {
            let reference = descriptor(&template, n + 1);
            let key = Bytes::from(format!("key-{:08}{}", 2 * n + 1, "x".repeat(suffix)));
            expected.push((reference.encode(), key.clone()));
            builder.push(reference, key).await.unwrap();
        }
        let root = builder.finish().await.unwrap();
        let reads = files.range_reads.load(Ordering::SeqCst);
        let mut reader = tree::Reader::new(files.clone(), root).unwrap();
        while reader.next().await.unwrap().is_some() {}
        let traversal_reads = files.range_reads.load(Ordering::SeqCst) - reads;
        if count == 8193 {
            assert_eq!(traversal_reads, 133);
        }
        for (edit, position) in [
            ("same", 0),
            ("replace", count / 2),
            ("insert", 0),
            ("delete", 0),
            ("remove-leaf", 0),
            ("insert", count.saturating_sub(1)),
        ] {
            let reads = files.range_reads.load(Ordering::SeqCst);
            let puts = files.puts.load(Ordering::SeqCst);
            let uploaded = files.uploaded.load(Ordering::SeqCst);
            let mut priced = Default::default();
            let mut allowance = if edit == "same" {
                0
            } else {
                tree::growth_bytes(crate::index::MAX_KEY_BYTES) as u64
            };
            let mut reader = tree::Reader::new(files.clone(), root).unwrap();
            let mut builder = tree::Builder::new(files.clone());
            let mut rewritten = expected.clone();
            let replacement = descriptor(&template, count + 1);
            let key = Bytes::from(format!("key-{:08}{}", 2 * position, "x".repeat(suffix)));
            match edit {
                "replace" if count > 0 => rewritten[position as usize].0 = replacement.encode(),
                "insert" => {
                    rewritten.insert(position as usize, (replacement.encode(), key.clone()))
                }
                "delete" if count > 0 => {
                    rewritten.remove(0);
                }
                "remove-leaf" => {
                    rewritten.drain(..(count.min(64) as usize));
                }
                _ => {}
            }
            let mut n = 0;
            while let Some(mut routed) = reader.next().await.unwrap() {
                if edit != "same" && (n == position || edit == "remove-leaf" && n < 64) {
                    allowance += reader.path_bytes(&mut priced);
                }
                builder.align_to(&reader).await.unwrap();
                if edit == "insert" && n == position {
                    builder
                        .push(replacement.clone(), key.clone())
                        .await
                        .unwrap();
                }
                if edit == "replace" && n == position {
                    routed.reference = replacement.clone();
                }
                if !(edit == "delete" && n == 0 || edit == "remove-leaf" && n < 64) {
                    builder.push(routed.reference, routed.last).await.unwrap();
                }
                n += 1;
            }
            if edit == "insert" && count == 0 {
                builder.push(replacement, key).await.unwrap();
            }
            let next = builder.finish().await.unwrap();
            assert_eq!(
                files.range_reads.load(Ordering::SeqCst) - reads,
                traversal_reads,
                "alignment adds no provider reads"
            );
            let uploads = files.puts.load(Ordering::SeqCst) - puts;
            assert!(
                (files.uploaded.load(Ordering::SeqCst) - uploaded) as u64 <= allowance,
                "{count} tables, {edit}: manifest encoding exceeds admission"
            );
            let path = root.map_or(0, |(height, _)| height as usize + 1);
            let bound = match edit {
                "same" => 0,
                "insert" => 2 * path + 1,
                "remove-leaf" => path.saturating_sub(1),
                _ => path,
            };
            assert!(
                uploads <= bound,
                "{count} tables, {edit} at {position}: {uploads} uploads > {bound}"
            );
            if count == 8193 && position == 0 {
                assert_eq!(
                    uploads,
                    match edit {
                        "insert" => 5,
                        "delete" => 3,
                        "remove-leaf" => 2,
                        _ => 0,
                    }
                );
            }
            if edit == "same" {
                assert!(
                    next == root,
                    "unchanged manifests retain their exact identity"
                );
            }
            if count >= 4096 && edit == "insert" && position == 0 {
                let puts = files.puts.load(Ordering::SeqCst);
                let mut reader = tree::Reader::new(files.clone(), next).unwrap();
                let mut builder = tree::Builder::new(files.clone());
                while let Some(routed) = reader.next().await.unwrap() {
                    builder.align_to(&reader).await.unwrap();
                    builder.push(routed.reference, routed.last).await.unwrap();
                }
                assert!(builder.finish().await.unwrap() == next);
                assert_eq!(
                    files.puts.load(Ordering::SeqCst),
                    puts,
                    "ragged groups stay stable"
                );
            }
            for reverse in [false, true] {
                let mut reader = if reverse {
                    tree::Reader::seek_reverse(
                        files.clone(),
                        next,
                        None,
                        Arc::new(Cache::new(0, None).unwrap()),
                    )
                    .unwrap()
                } else {
                    tree::Reader::new(files.clone(), next).unwrap()
                };
                let rows: Vec<_> = if reverse {
                    rewritten.iter().rev().collect()
                } else {
                    rewritten.iter().collect()
                };
                for (reference, last) in rows {
                    let routed = reader.next().await.unwrap().unwrap();
                    assert_eq!(
                        (routed.reference.encode(), routed.last),
                        (reference.clone(), last.clone())
                    );
                }
                assert!(reader.next().await.unwrap().is_none());
            }
        }
    }
}

#[tokio::test]
async fn repeated_prefix_inserts_keep_manifest_height_bounded() {
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
    let mut builder = tree::Builder::new(files.clone());
    for n in 128..192 {
        builder
            .push(reference.clone(), Bytes::from(format!("key-{n:04}")))
            .await
            .unwrap();
    }
    let mut root = builder.finish().await.unwrap();
    for n in (0..128).rev() {
        let mut reader = tree::Reader::new(files.clone(), root).unwrap();
        let mut builder = tree::Builder::new(files.clone());
        let mut first = true;
        while let Some(routed) = reader.next().await.unwrap() {
            builder.align_to(&reader).await.unwrap();
            if first {
                builder
                    .push(reference.clone(), Bytes::from(format!("key-{n:04}")))
                    .await
                    .unwrap();
                first = false;
            }
            builder.push(routed.reference, routed.last).await.unwrap();
        }
        root = builder.finish().await.unwrap();
        assert!(
            root.unwrap().0 <= 1,
            "prefix inserts must not build a tall chain of sparse nodes"
        );
    }
    let mut reader = tree::Reader::new(files, root).unwrap();
    for n in 0..192 {
        assert_eq!(
            reader.next().await.unwrap().unwrap().last,
            format!("key-{n:04}")
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn lost_or_cancelled_boundary_flush_cannot_replace_the_base_manifest() {
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
    let mut builder = tree::Builder::new(files.clone());
    for n in 0..65 {
        builder
            .push(reference.clone(), Bytes::from(format!("key-{n:04}")))
            .await
            .unwrap();
    }
    let root = builder.finish().await.unwrap();
    let rewrite = || async {
        let mut builder = tree::Builder::new(files.clone());
        let mut reader = tree::Reader::new(files.clone(), root)?;
        let mut first = true;
        while let Some(routed) = reader.next().await? {
            builder.align_to(&reader).await?;
            if first {
                builder
                    .push(reference.clone(), Bytes::from_static(b"added"))
                    .await?;
                first = false;
            }
            builder.push(routed.reference, routed.last).await?;
        }
        builder.finish().await
    };
    // The first PUT fills a leaf; the second flushes its remainder at the old
    // boundary. Both faults occur after the provider has stored that object.
    files
        .lose
        .store(files.puts.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(rewrite().await.is_err());
    files.lose.store(0, Ordering::SeqCst);
    files
        .pause
        .store(files.puts.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    tokio::select! {
        result = rewrite() => panic!("paused boundary flush completed: {}", result.is_ok()),
        permit = files.entered.acquire() => permit.unwrap().forget(),
    }
    files.pause.store(0, Ordering::SeqCst);
    let next = rewrite().await.unwrap();
    for (root, added) in [(root, false), (next, true)] {
        let mut reader = tree::Reader::new(files.clone(), root).unwrap();
        if added {
            assert_eq!(
                reader.next().await.unwrap().unwrap().last,
                b"added".as_slice()
            );
        }
        for n in 0..65 {
            assert_eq!(
                reader.next().await.unwrap().unwrap().last,
                format!("key-{n:04}")
            );
        }
        assert!(reader.next().await.unwrap().is_none());
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

#[tokio::test]
async fn reblocking_retained_rows_can_exceed_the_original_data_and_index_sizes() {
    use crate::transactions::{record::Commit, recovery::Target};
    let files = Files::new(0);
    let db = Database {
        name: "app",
        generation: Uuid::from_u128(7),
    };
    let prefix_bytes = db.name.len() + 18;
    let metadata_bytes = 14 + prefix_bytes + 32;
    let receipt_bytes = 14 + prefix_bytes + "initial".len() + 32;
    let mut rows = Vec::new();
    for group in 0..8 {
        let first = format!("group/{group:02}/a");
        let middle = format!("group/{group:02}/b{}", "x".repeat(1000));
        let last = format!("group/{group:02}/c");
        let headers = 3 * (14 + prefix_bytes) + first.len() + middle.len() + last.len();
        let value = 65536
            - headers
            - if group == 0 {
                metadata_bytes + receipt_bytes
            } else {
                0
            };
        rows.extend([
            (first, Vec::new()),
            (middle, vec![42; value]),
            (last, Vec::new()),
        ]);
    }
    let mutations: Vec<_> = rows
        .iter()
        .map(|(key, value)| Mutation {
            key,
            value: Some(value.as_slice()),
        })
        .collect();
    let mut projection = Projection::new(Limits::default()).unwrap();
    let bytes = Commit::encode(db, 1, 0, 0, "initial", [1; 32], &mutations).unwrap();
    let commit = Commit::decode(&bytes).unwrap();
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
    assert_eq!(reference.tables, 1);
    let snapshot = Snapshot::new(
        files.clone(),
        Arc::new(Cache::new(0, None).unwrap()),
        reference,
    );
    let old_bytes = encoded_table_bytes(&snapshot).await;
    let uploaded = files.uploaded.load(Ordering::SeqCst);
    projection.rebase(snapshot);
    // The base blocks end on short keys. A small insertion shifts their
    // boundaries onto the unchanged 1 KiB keys, growing every block's index.
    let mutation = [Mutation {
        key: "aaaa",
        value: Some(b""),
    }];
    let direct = contribution_bytes(db, "next", &mutation) as u64;
    let copies = projection
        .checkpoint_rewrite_bytes(db, &mutation)
        .await
        .unwrap();
    let bytes = Commit::encode(db, 2, 1, 0, "next", [2; 32], &mutation).unwrap();
    let commit = Commit::decode(&bytes).unwrap();
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
    let image_bytes = files.uploaded.load(Ordering::SeqCst) - uploaded;
    let reserve =
        projection.checkpoint_charges().await.unwrap()[&(db.name.to_owned(), db.generation)];
    assert!(image_bytes as u64 <= reserve && reserve <= direct + copies);
    assert!(
        image_bytes as u64 <= direct + copies,
        "complete rebuilt image exceeds admission"
    );
    let snapshot = Snapshot::new(files, Arc::new(Cache::new(0, None).unwrap()), reference);
    let actual = encoded_table_bytes(&snapshot).await;
    assert!(
        actual > direct + old_bytes,
        "fixture must exceed the old copy allowance: {actual} <= {}",
        direct + old_bytes
    );
    assert!(
        actual <= direct + copies,
        "reblocked tables exceed admission: {actual} > {}",
        direct + copies
    );
    assert_eq!(
        (actual, image_bytes, direct + copies),
        (533_678, 533_873, 2_819_918)
    );
    snapshot.verify(Limits::default()).await.unwrap();
}
