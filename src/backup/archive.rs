//! Bounded, self-checking logical archive. No credentials or access policy are restored.
use crate::engine::{Info, Limits, Pair, validate_db};
use anyhow::{Context, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub const MAX_ARCHIVE: u64 = 1024 * 1024 * 1024;
const MAX_LINE: usize = 100 * 1024;
pub const CHUNK_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub format: u8,
    pub database: String,
    pub revision: u64,
    pub incarnation: Option<String>,
    pub limits: Limits,
    pub keys: u64,
    pub bytes: u64,
}
impl Header {
    pub fn from_info(database: &str, info: &Info) -> Self {
        Self {
            format: 1,
            database: database.into(),
            revision: info.revision,
            incarnation: info.policy.as_ref().map(|p| p.incarnation.clone()),
            limits: info.limits.clone(),
            keys: info.keys,
            bytes: info.bytes,
        }
    }
    fn validate(&self) -> anyhow::Result<()> {
        validate_db(&self.database)?;
        ensure!(
            self.format == 1
                && self.limits.max_bytes > 0
                && self.limits.max_bytes <= 256 * 1024 * 1024
                && self.limits.max_keys > 0
                && self.limits.max_keys <= 1_000_000
                && self.keys <= self.limits.max_keys
                && self.bytes <= self.limits.max_bytes,
            "unsupported archive format or limits"
        );
        if let Some(id) = &self.incarnation {
            uuid::Uuid::parse_str(id).context("invalid archive incarnation")?;
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Record {
    Header {
        header: Header,
    },
    Pair {
        key: String,
        value: String,
    },
    Footer {
        sha256: String,
        keys: u64,
        bytes: u64,
    },
}
#[derive(Default)]
struct Accounting {
    last: Option<String>,
    keys: u64,
    bytes: u64,
}
impl Accounting {
    fn add(&mut self, pair: &Pair, header: &Header) -> anyhow::Result<()> {
        ensure!(
            !pair.key.is_empty()
                && pair.key.len() <= 1024
                && !pair.key.contains('\0')
                && self.last.as_ref().is_none_or(|last| last < &pair.key),
            "invalid archive key order or bound"
        );
        ensure!(
            pair.value.len() <= 88 * 1024,
            "archive value exceeds encoded bound"
        );
        let value = STANDARD
            .decode(&pair.value)
            .map_err(|_| anyhow::anyhow!("invalid archive base64"))?;
        ensure!(
            value.len() <= 64 * 1024 && STANDARD.encode(&value) == pair.value,
            "invalid archive value"
        );
        self.keys += 1;
        self.bytes += pair.key.len() as u64 + value.len() as u64;
        ensure!(
            self.keys <= header.keys && self.bytes <= header.bytes,
            "archive exceeds declared totals"
        );
        self.last = Some(pair.key.clone());
        Ok(())
    }
    fn complete(&self, header: &Header) -> anyhow::Result<()> {
        ensure!(
            self.keys == header.keys && self.bytes == header.bytes,
            "archive totals do not match snapshot"
        );
        Ok(())
    }
}
pub struct Writer<W: Write> {
    output: W,
    header: Header,
    hash: Sha256,
    count: Accounting,
}
impl<W: Write> Writer<W> {
    pub fn new(output: W, header: Header) -> anyhow::Result<Self> {
        header.validate()?;
        let mut writer = Self {
            output,
            header,
            hash: Sha256::new(),
            count: Accounting::default(),
        };
        writer.record(&Record::Header {
            header: writer.header.clone(),
        })?;
        Ok(writer)
    }
    fn record(&mut self, record: &Record) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        ensure!(line.len() <= MAX_LINE, "archive record exceeds bound");
        self.hash.update(&line);
        self.output.write_all(&line)?;
        Ok(())
    }
    pub fn pair(&mut self, pair: Pair) -> anyhow::Result<()> {
        self.count.add(&pair, &self.header)?;
        self.record(&Record::Pair {
            key: pair.key,
            value: pair.value,
        })
    }
    pub fn finish(mut self) -> anyhow::Result<String> {
        self.count.complete(&self.header)?;
        let sha256 = hex::encode(self.hash.finalize());
        serde_json::to_writer(
            &mut self.output,
            &Record::Footer {
                sha256: sha256.clone(),
                keys: self.count.keys,
                bytes: self.count.bytes,
            },
        )?;
        self.output.write_all(b"\n")?;
        self.output.flush()?;
        Ok(sha256)
    }
}
fn line(reader: &mut BufReader<File>) -> anyhow::Result<Option<(Record, Vec<u8>)>> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_LINE + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    ensure!(
        bytes.len() <= MAX_LINE && bytes.last() == Some(&b'\n'),
        "archive record truncated or oversized"
    );
    let record =
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid archive record"))?;
    Ok(Some((record, bytes)))
}
/// Copy before validation: later modifications of the supplied path cannot change
/// the already-validated stream that drives a restore. The spool is private/unlinked.
pub struct Verified {
    pub header: Header,
    pub digest: String,
    pub chunks: u64,
    reader: BufReader<File>,
    queued: Option<Pair>,
}
impl Verified {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let source = File::open(path)?;
        ensure!(
            source.metadata()?.is_file(),
            "archive must be a regular file"
        );
        let mut spool = tempfile::tempfile()?;
        let size = std::io::copy(&mut source.take(MAX_ARCHIVE + 1), &mut spool)?;
        ensure!(size <= MAX_ARCHIVE, "archive exceeds 1 GiB bound");
        spool.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(spool);
        let Some((Record::Header { header }, first)) = line(&mut reader)? else {
            bail!("missing archive header")
        };
        header.validate()?;
        let mut hash = Sha256::new();
        hash.update(first);
        let mut count = Accounting::default();
        let digest = loop {
            match line(&mut reader)? {
                Some((Record::Pair { key, value }, raw)) => {
                    count.add(&Pair { key, value }, &header)?;
                    hash.update(raw);
                }
                Some((
                    Record::Footer {
                        sha256,
                        keys,
                        bytes,
                    },
                    _,
                )) => {
                    count.complete(&header)?;
                    ensure!(
                        keys == header.keys
                            && bytes == header.bytes
                            && sha256 == hex::encode(hash.finalize()),
                        "archive checksum or footer mismatch"
                    );
                    ensure!(line(&mut reader)?.is_none(), "archive has trailing records");
                    break sha256;
                }
                _ => bail!("archive is incomplete or has unexpected records"),
            }
        };
        let mut result = Self {
            header,
            digest,
            chunks: 0,
            reader,
            queued: None,
        };
        result.rewind()?;
        while result.chunk()?.is_some() {
            result.chunks += 1;
        }
        result.rewind()?;
        Ok(result)
    }
    pub fn rewind(&mut self) -> anyhow::Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        line(&mut self.reader)?;
        self.queued = None;
        Ok(())
    }
    pub fn chunk(&mut self) -> anyhow::Result<Option<Vec<Pair>>> {
        let mut pairs = Vec::new();
        let mut size = 0;
        loop {
            let pair = match self.queued.take() {
                Some(pair) => pair,
                None => match line(&mut self.reader)? {
                    Some((Record::Pair { key, value }, _)) => Pair { key, value },
                    Some((Record::Footer { .. }, _)) | None => break,
                    _ => bail!("unexpected validated archive record"),
                },
            };
            let encoded = serde_json::to_vec(&pair)?.len();
            if !pairs.is_empty() && (pairs.len() == 128 || size + encoded > CHUNK_BYTES) {
                self.queued = Some(pair);
                break;
            }
            size += encoded;
            pairs.push(pair);
        }
        Ok((!pairs.is_empty()).then_some(pairs))
    }
}
