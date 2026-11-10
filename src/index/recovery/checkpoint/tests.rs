use super::*;
use crate::storage::{Object, Remote, Version};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
struct Files {
    inner: Remote,
    puts: AtomicUsize,
    lose: usize,
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
            lose,
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
        self.inner.get_range(k, r, s).await
    }
    async fn put(&self, k: &str, b: Bytes) -> anyhow::Result<()> {
        self.inner.put(k, b).await?;
        let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.pause.load(Ordering::SeqCst) {
            self.entered.add_permits(1);
            futures::future::pending::<()>().await;
        }
        ensure!(n != self.lose, "lost upload reply");
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
    out.into()
}
fn receipt(fingerprint: u8) -> Bytes {
    Bytes::from(vec![fingerprint; 32])
}
async fn image(files: Arc<dyn Storage>, bad: u8) -> anyhow::Result<Reference> {
    let mut builder = Builder::new(files.clone());
    let prefix = prefix();
    builder.push(&prefix, 0, b"", 2, Some(meta(2, 0))).await?;
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
    Header {
        cut: cut(),
        tree,
        tables,
        rows: builder.rows + u64::from(bad == 4),
        databases: 1,
    }
    .upload(files)
    .await
}
#[tokio::test]
async fn unknown_upload_at_every_stage_never_returns_a_checkpoint_reference() {
    for lose in 1..=4 {
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
    for bad in 1..=4 {
        let files = Files::new(0);
        let reference = image(files.clone(), bad).await.unwrap();
        let header = Header::load(files.clone(), reference).await.unwrap();
        assert!(load(files, header, Limits::default()).await.is_err());
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
        let header = Header::load(files.clone(), reference).await.unwrap();
        let result = load(files.clone(), header, limits).await;
        // One tombstone key fits exactly one value byte.
        assert_eq!(result.is_ok(), limits.value_bytes == 1);
    }
    let header = Header::load(files.clone(), reference).await.unwrap();
    let root = header.tree.unwrap().1;
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
        load(
            files.clone(),
            Header::load(files.clone(), reference).await.unwrap(),
            Limits::default()
        )
        .await
        .is_err()
    );
    files.inner.delete(&key).await.unwrap();
    assert!(
        load(
            files.clone(),
            Header::load(files.clone(), reference).await.unwrap(),
            Limits::default()
        )
        .await
        .is_err()
    );
    files.inner.put(&key, original).await.unwrap();
    let projection = load(
        files.clone(),
        Header::load(files.clone(), reference).await.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    let view = projection
        .view(Database {
            name: "app",
            generation: Uuid::from_u128(7),
        })
        .unwrap()
        .unwrap();
    assert!(view.get(b"x", 1).is_err());
    assert!(view.get(b"x", 2).unwrap().unwrap().value.is_none());
    assert_eq!(
        view.state().resolve("first", [1; 32], 0).unwrap(),
        crate::transactions::recovery::Resolution::Committed(1)
    );
}
#[tokio::test]
async fn manifest_bulk_tree_preserves_order_across_full_and_partial_levels() {
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
            // Distinct valid descriptors; this test traverses the manifest only.
            let mut bytes = template.to_vec();
            bytes[8..24].copy_from_slice(Uuid::from_u128(n + 1).as_bytes());
            let end = bytes.len() - 32;
            let checksum = Sha256::digest(&bytes[..end]);
            bytes[end..].copy_from_slice(&checksum);
            expected.push(Bytes::from(bytes.clone()));
            builder
                .push(
                    table::Reference::decode(&bytes).unwrap(),
                    Bytes::from(format!("key-{n:08}")),
                )
                .await
                .unwrap();
        }
        let root = builder.finish().await.unwrap();
        let mut reader = tree::Reader::new(files.clone(), root).unwrap();
        for expected in expected {
            assert_eq!(
                reader.next().await.unwrap().unwrap().reference.encode(),
                expected
            );
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn cancelling_a_completed_header_upload_does_not_return_a_publishable_handle() {
    let files = Files::new(0);
    files.pause.store(4, Ordering::SeqCst);
    tokio::select! {
        result=image(files.clone(),0)=>panic!("paused upload completed: {}",result.is_ok()),
        permit=files.entered.acquire()=>permit.unwrap().forget(),
    }
    assert_eq!(
        files.inner.list("tables/", None, 10).await.unwrap().len(),
        4
    );
    files.pause.store(0, Ordering::SeqCst);
    let reference = image(files.clone(), 0).await.unwrap();
    assert_eq!(
        files.inner.list("tables/", None, 10).await.unwrap().len(),
        8
    );
    assert!(
        load(
            files.clone(),
            Header::load(files, reference).await.unwrap(),
            Limits::default()
        )
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
    let header = Header::load(files.clone(), reference).await.unwrap();
    let projection = load(files, header, Limits::default()).await.unwrap();
    let view = projection
        .view(Database {
            name: "app",
            generation: Uuid::from_u128(7),
        })
        .unwrap()
        .unwrap();
    assert_eq!(view.state().revision(), 2);
    assert!(view.get(b"x", 2).unwrap().unwrap().value.is_none());
    assert_eq!(
        view.state().resolve("second", [2; 32], 1).unwrap(),
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
