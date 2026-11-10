use crate::storage::Storage;
use anyhow::ensure;
use bytes::{Buf, Bytes};
use sha2::{Digest, Sha256};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

pub const MAX_BLOB_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const REFERENCE_BYTES: usize = 56;

/// A known-successful immutable upload, or its reference restored from selected
/// history. Fields cannot be supplied by an application. This is not ownership
/// or permission to publish. Physical admission and orphan collection belong to
/// the controller; an uncertain upload returns no reference and is never retried.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Blob {
    id: Uuid,
    size: u64,
    digest: [u8; 32],
}
impl Blob {
    pub async fn upload(storage: Arc<dyn Storage>, bytes: Bytes) -> anyhow::Result<Self> {
        ensure!(
            (1..=MAX_BLOB_BYTES).contains(&bytes.len()),
            "invalid immutable blob size"
        );
        let blob = Self {
            id: Uuid::new_v4(),
            size: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        storage.put(&blob.key(), bytes).await?;
        Ok(blob)
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    fn key(&self) -> String {
        format!("tables/{}", self.id.simple())
    }
    /// Partial reads rely on the index's block-integrity checks. A complete
    /// bounded read also checks the digest recorded in the journal reference.
    pub async fn read_range(
        &self,
        storage: &dyn Storage,
        range: Range<u64>,
    ) -> anyhow::Result<Bytes> {
        let complete = range == (0..self.size);
        let bytes = storage.get_range(&self.key(), range, self.size).await?;
        if complete {
            ensure!(
                Sha256::digest(&bytes).as_slice() == self.digest,
                "immutable blob digest mismatch"
            );
        }
        Ok(bytes)
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.id.as_bytes());
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(&self.digest);
    }
    pub(crate) fn decode(mut bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            bytes.len() == REFERENCE_BYTES,
            "invalid immutable blob reference length"
        );
        let id = Uuid::from_slice(&bytes[..16])?;
        bytes.advance(16);
        let size = bytes.get_u64();
        ensure!(
            !id.is_nil() && (1..=MAX_BLOB_BYTES as u64).contains(&size),
            "invalid immutable blob reference"
        );
        Ok(Self {
            id,
            size,
            digest: bytes.try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Object, Remote, Version};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Faults {
        inner: Remote,
        lose: AtomicBool,
        puts: AtomicUsize,
    }
    #[async_trait]
    impl Storage for Faults {
        async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
            self.inner.get(key, max).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
            size: u64,
        ) -> anyhow::Result<Bytes> {
            self.inner.get_range(key, range, size).await
        }
        async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.put(key, bytes).await?;
            ensure!(
                !self.lose.swap(false, Ordering::SeqCst),
                "lost blob response"
            );
            Ok(())
        }
        async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
            panic!("blob uploads must be unconditional and fresh")
        }
        async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
            panic!("blob reads must not list")
        }
        async fn delete(&self, _: &str) -> anyhow::Result<()> {
            panic!("blob publication does not reclaim objects")
        }
    }
    fn storage() -> Arc<Faults> {
        Arc::new(Faults {
            inner: Remote::new(Arc::new(object_store::memory::InMemory::new()), "blob").unwrap(),
            lose: AtomicBool::new(false),
            puts: AtomicUsize::new(0),
        })
    }
    #[tokio::test]
    async fn unknown_upload_has_no_publishable_reference_and_new_attempts_never_reuse_names() {
        let files = storage();
        files.lose.store(true, Ordering::SeqCst);
        assert!(
            Blob::upload(files.clone(), Bytes::from_static(b"payload"))
                .await
                .is_err()
        );
        assert_eq!(files.puts.load(Ordering::SeqCst), 1);
        let first = Blob::upload(files.clone(), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        let second = Blob::upload(files.clone(), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_ne!(first.id, second.id);
        assert_eq!(
            files.inner.list("tables/", None, 10).await.unwrap().len(),
            3
        );
        assert_eq!(
            first.read_range(files.as_ref(), 0..7).await.unwrap(),
            b"payload"[..]
        );
        assert!(Blob::upload(files.clone(), Bytes::new()).await.is_err());
        assert_eq!(files.puts.load(Ordering::SeqCst), 3);
    }
    #[tokio::test]
    async fn blob_ranges_detect_missing_truncated_and_same_size_corrupt_full_payloads() {
        let files = storage();
        let blob = Blob::upload(files.clone(), Bytes::from_static(b"immutable"))
            .await
            .unwrap();
        assert_eq!(
            blob.read_range(files.as_ref(), 2..5).await.unwrap(),
            b"mut"[..]
        );
        let mut encoded = Vec::new();
        blob.encode(&mut encoded);
        let recovered = Blob::decode(&encoded).unwrap();
        assert_eq!(
            recovered.read_range(files.as_ref(), 0..9).await.unwrap(),
            b"immutable"[..]
        );
        files
            .inner
            .put(&blob.key(), Bytes::from_static(b"corrupted"))
            .await
            .unwrap();
        assert!(blob.read_range(files.as_ref(), 0..9).await.is_err());
        files
            .inner
            .put(&blob.key(), Bytes::from_static(b"short"))
            .await
            .unwrap();
        assert!(blob.read_range(files.as_ref(), 2..5).await.is_err());
        files.inner.delete(&blob.key()).await.unwrap();
        assert!(blob.read_range(files.as_ref(), 2..5).await.is_err());
        for end in 0..REFERENCE_BYTES {
            assert!(Blob::decode(&encoded[..end]).is_err());
        }
        for (start, end) in [(0, 16), (16, 24)] {
            let mut bad = encoded.clone();
            bad[start..end].fill(0);
            assert!(Blob::decode(&bad).is_err());
        }
    }
}
