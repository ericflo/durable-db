//! Consistent logical export and resumable restore through the ordinary HTTP API.
mod archive;
mod http;
use crate::engine::{
    CommitRequest, CommitResult, CreateRequest, CreateResult, Info, Pair, ReadRequest, ReadResult,
    Scan, Write, validate_db,
};
use anyhow::{Context, ensure};
use archive::{Header, Verified, Writer};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::{HttpStatus, Remote};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{io::BufWriter, path::Path};

#[derive(Serialize)]
pub struct ArchiveInfo {
    pub snapshot: Header,
    pub sha256: String,
    pub restore_chunks: u64,
}
pub fn inspect(input: &Path) -> anyhow::Result<ArchiveInfo> {
    let archive = Verified::open(input)?;
    Ok(ArchiveInfo {
        snapshot: archive.header,
        sha256: archive.digest,
        restore_chunks: archive.chunks,
    })
}
fn path(database: &str, suffix: &str) -> String {
    format!("/v1/databases/{database}{suffix}")
}
fn same_snapshot(before: &Info, after: &Info) -> bool {
    before.revision == after.revision
        && before.keys == after.keys
        && before.bytes == after.bytes
        && before.policy.as_ref().map(|p| &p.incarnation)
            == after.policy.as_ref().map(|p| &p.incarnation)
}
/// Export never overwrites an existing path. Conflict/cancellation leaves no final archive.
pub async fn export(
    url: &str,
    token_file: &Path,
    database: &str,
    output: &Path,
) -> anyhow::Result<ArchiveInfo> {
    validate_db(database)?;
    ensure!(!output.exists(), "output already exists");
    let remote = Remote::new(url, token_file)?;
    let info: Info = remote.get(&path(database, "")).await?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    let header = Header::from_info(database, &info);
    let mut writer = Writer::new(BufWriter::new(temporary.as_file_mut()), header.clone())?;
    let mut after = None;
    let mut limit = 128;
    loop {
        let request = ReadRequest {
            expected_revision: Some(info.revision),
            keys: vec![],
            scans: vec![Scan {
                prefix: String::new(),
                start: None,
                end: None,
                after: after.clone(),
                reverse: false,
                limit,
            }],
        };
        let result: ReadResult = match remote
            .call(Method::POST, &path(database, "/read"), Some(&request))
            .await
        {
            Ok(result) => result,
            Err(e)
                if e.downcast_ref::<HttpStatus>()
                    .is_some_and(|s| s.0 == StatusCode::PAYLOAD_TOO_LARGE)
                    && limit > 1 =>
            {
                limit /= 2;
                continue;
            }
            Err(e) => return Err(e),
        };
        ensure!(
            result.revision == info.revision && result.values.is_empty() && result.scans.len() == 1,
            "export response changed snapshot or shape"
        );
        let scan = result.scans.into_iter().next().unwrap();
        ensure!(
            scan.items.len() <= limit
                && scan
                    .next
                    .as_ref()
                    .is_none_or(|next| scan.items.last().is_some_and(|p| &p.key == next)),
            "invalid export cursor"
        );
        for pair in scan.items {
            writer.pair(pair)?;
        }
        after = scan.next;
        if after.is_none() {
            break;
        }
    }
    let final_info: Info = remote.get(&path(database, "")).await?;
    ensure!(
        same_snapshot(&info, &final_info),
        "source changed during export; retry with a new consistent snapshot"
    );
    let sha256 = writer.finish()?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(output)
        .map_err(|_| anyhow::anyhow!("cannot publish archive without overwriting"))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    let verified = inspect(output)?;
    ensure!(
        verified.sha256 == sha256,
        "published archive checksum mismatch"
    );
    Ok(verified)
}
#[derive(Serialize)]
pub struct RestoreInfo {
    pub archive_sha256: String,
    pub source_revision: u64,
    pub destination: Info,
    pub verified_keys: u64,
}
async fn verify_chunk(
    remote: &Remote,
    database: &str,
    revision: u64,
    pairs: &[Pair],
) -> anyhow::Result<()> {
    let result: ReadResult = remote
        .call(
            Method::POST,
            &path(database, "/read"),
            Some(&ReadRequest {
                expected_revision: Some(revision),
                keys: pairs.iter().map(|p| p.key.clone()).collect(),
                scans: vec![],
            }),
        )
        .await?;
    ensure!(
        result.revision == revision
            && result.scans.is_empty()
            && result.values.len() == pairs.len()
            && pairs
                .iter()
                .zip(&result.values)
                .all(|(p, v)| v.as_ref() == Some(&p.value)),
        "destination does not match the archive prefix; restore stopped"
    );
    Ok(())
}
/// The destination remains operator-only until this returns. It is a new protected
/// database, not an atomic replacement or a restoration of old access credentials.
pub async fn restore(
    url: &str,
    token_file: &Path,
    database: &str,
    input: &Path,
) -> anyhow::Result<RestoreInfo> {
    validate_db(database)?;
    let mut archive = Verified::open(input)?; // Complete validation precedes every remote action.
    let remote = Remote::new(url, token_file)?;
    let mut hash = Sha256::new();
    hash.update(b"durable-db.restore.v1\0");
    hash.update(archive.digest.as_bytes());
    hash.update(b"\0");
    hash.update(database.as_bytes());
    let identity = hex::encode(hash.finalize());
    let created: CreateResult = remote
        .call(
            Method::PUT,
            &path(database, ""),
            Some(&CreateRequest {
                request_id: identity.clone(),
                limits: archive.header.limits.clone(),
            }),
        )
        .await?;
    let incarnation = created
        .info
        .policy
        .context("restore creation is not protected")?
        .incarnation;
    ensure!(
        created.info.revision == 0 && created.info.keys == 0 && created.info.bytes == 0,
        "creation did not recover its original result"
    );
    let before: Info = remote.get(&path(database, "")).await?;
    ensure!(
        before
            .policy
            .as_ref()
            .is_some_and(|p| p.incarnation == incarnation)
            && before.revision <= archive.chunks
            && before.limits.max_bytes == archive.header.limits.max_bytes
            && before.limits.max_keys == archive.header.limits.max_keys,
        "destination identity or revision does not match this restore"
    );
    let mut index = 0;
    let mut keys = 0;
    let mut bytes = 0;
    if before.revision == 0 {
        ensure!(
            before.keys == 0 && before.bytes == 0,
            "destination is not empty"
        );
    }
    while let Some(pairs) = archive.chunk()? {
        index += 1;
        for pair in &pairs {
            keys += 1;
            bytes += pair.key.len() as u64 + STANDARD.decode(&pair.value)?.len() as u64;
        }
        if index <= before.revision {
            verify_chunk(&remote, database, before.revision, &pairs).await?;
            if index == before.revision {
                ensure!(
                    before.keys == keys && before.bytes == bytes,
                    "destination contains data outside the verified prefix"
                );
            }
        } else {
            let request = CommitRequest {
                expected_revision: index - 1,
                request_id: format!("{identity}-{index}"),
                checks: vec![],
                writes: pairs
                    .into_iter()
                    .map(|p| Write {
                        key: p.key,
                        value: Some(p.value),
                    })
                    .collect(),
            };
            let result: CommitResult = remote
                .call(Method::POST, &path(database, "/commit"), Some(&request))
                .await?;
            ensure!(
                result.revision == index,
                "restore commit returned an unexpected revision"
            );
        }
    }
    archive.rewind()?;
    while let Some(pairs) = archive.chunk()? {
        verify_chunk(&remote, database, archive.chunks, &pairs).await?;
    }
    let destination: Info = remote.get(&path(database, "")).await?;
    ensure!(
        destination.revision == archive.chunks
            && destination.keys == archive.header.keys
            && destination.bytes == archive.header.bytes
            && destination
                .policy
                .as_ref()
                .is_some_and(|p| p.incarnation == incarnation),
        "final restore verification failed"
    );
    Ok(RestoreInfo {
        archive_sha256: archive.digest,
        source_revision: archive.header.revision,
        destination,
        verified_keys: keys,
    })
}
