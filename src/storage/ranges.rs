//! Bounded reads of immutable object slices. This is a transport check, not a
//! publication protocol: callers must use a fresh immutable identity and the
//! size recorded by its durable reference. It grants no serving permission.
use super::{Local, Remote};
use anyhow::{Context, ensure};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{GetOptions, ObjectStore};
use std::ops::Range;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub const MAX_RANGE_BYTES: usize = 8 * 1024 * 1024;

fn length(range: &Range<u64>, object_size: u64) -> anyhow::Result<usize> {
    ensure!(
        range.start < range.end && range.end <= object_size,
        "invalid immutable object range"
    );
    let length =
        usize::try_from(range.end - range.start).context("object range exceeds address space")?;
    ensure!(
        length <= MAX_RANGE_BYTES,
        "object range exceeds read budget"
    );
    Ok(length)
}

impl Remote {
    pub(super) async fn read_range(
        &self,
        key: &str,
        range: Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        let length = length(&range, object_size)?;
        let result = self
            .inner
            .get_opts(
                &self.path(key),
                GetOptions {
                    range: Some(range.clone().into()),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            result.meta.size == object_size,
            "immutable object size changed"
        );
        ensure!(
            result.range == range,
            "provider returned a different object range"
        );
        let mut stream = result.into_stream();
        let mut bytes = Vec::with_capacity(length);
        while let Some(chunk) = stream.try_next().await? {
            ensure!(
                chunk.len() <= length - bytes.len(),
                "object range stream exceeds read budget"
            );
            bytes.extend_from_slice(&chunk);
        }
        ensure!(bytes.len() == length, "object range stream is truncated");
        Ok(bytes.into())
    }
}

impl Local {
    pub(super) async fn read_range(
        &self,
        key: &str,
        range: Range<u64>,
        object_size: u64,
    ) -> anyhow::Result<Bytes> {
        let length = length(&range, object_size)?;
        let mut file = tokio::fs::File::open(self.path(key)?).await?;
        ensure!(
            file.metadata().await?.len() == object_size,
            "immutable object size changed"
        );
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).await?;
        Ok(bytes.into())
    }
}
