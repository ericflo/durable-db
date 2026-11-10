use super::*;
use crate::storage::immutable::REFERENCE_BYTES;
use bytes::Buf;

const INDEX: &[u8; 8] = b"DDBIDX01";
const BLOCK: &[u8; 8] = b"DDBBLK01";
const REF: &[u8; 8] = b"DDBSST01";
const DELETE: u32 = u32::MAX;

pub(super) fn validate_row(row: &Row<'_>) -> anyhow::Result<()> {
    ensure!(
        !row.key.is_empty()
            && row.key.len() <= MAX_KEY_BYTES
            && row.revision > 0
            && row.value.is_none_or(|v| v.len() <= 64 * 1024),
        "invalid table row"
    );
    Ok(())
}
pub(super) fn row_length(row: &Row<'_>) -> usize {
    14 + row.key.len() + row.value.map_or(0, <[u8]>::len)
}
pub(super) fn write_row(row: &Row<'_>, out: &mut Vec<u8>) {
    out.extend_from_slice(&(row.key.len() as u16).to_be_bytes());
    out.extend_from_slice(&row.revision.to_be_bytes());
    out.extend_from_slice(&row.value.map_or(DELETE, |v| v.len() as u32).to_be_bytes());
    out.extend_from_slice(row.key);
    if let Some(value) = row.value {
        out.extend_from_slice(value);
    }
}
pub(super) fn finish_block(
    data: &mut Vec<u8>,
    blocks: &mut Vec<Block>,
    pending: &mut Vec<u8>,
    first: Boundary,
    last: Boundary,
    count: usize,
) -> anyhow::Result<()> {
    ensure!(
        blocks.len() < MAX_BLOCKS
            && pending.len() + 12 <= MAX_BLOCK_BYTES
            && count <= MAX_BLOCK_ROWS,
        "table block limit exceeded"
    );
    ensure!(
        data.len() + pending.len() + 12 <= MAX_BLOB_BYTES,
        "table data size limit exceeded"
    );
    let offset = data.len();
    data.extend_from_slice(BLOCK);
    data.extend_from_slice(&(count as u32).to_be_bytes());
    data.append(pending);
    blocks.push(Block {
        first,
        last,
        offset: offset as u64,
        length: (data.len() - offset) as u32,
        rows: count as u32,
        digest: Sha256::digest(&data[offset..]).into(),
    });
    Ok(())
}
fn write_boundary(boundary: &Boundary, out: &mut Vec<u8>) {
    out.extend_from_slice(&(boundary.key.len() as u16).to_be_bytes());
    out.extend_from_slice(&boundary.key);
    out.extend_from_slice(&boundary.revision.to_be_bytes());
}
pub(super) fn index(blocks: &[Block]) -> Bytes {
    let mut out = Vec::new();
    out.extend_from_slice(INDEX);
    out.extend_from_slice(&(blocks.len() as u32).to_be_bytes());
    for block in blocks {
        write_boundary(&block.first, &mut out);
        write_boundary(&block.last, &mut out);
        out.extend_from_slice(&block.offset.to_be_bytes());
        out.extend_from_slice(&block.length.to_be_bytes());
        out.extend_from_slice(&block.rows.to_be_bytes());
        out.extend_from_slice(&block.digest);
    }
    out.into()
}
fn take<'a>(bytes: &mut &'a [u8], count: usize) -> anyhow::Result<&'a [u8]> {
    let result = bytes.get(..count).context("truncated table encoding")?;
    bytes.advance(count);
    Ok(result)
}
fn u16(bytes: &mut &[u8]) -> anyhow::Result<u16> {
    Ok(u16::from_be_bytes(take(bytes, 2)?.try_into()?))
}
fn u32(bytes: &mut &[u8]) -> anyhow::Result<u32> {
    Ok(u32::from_be_bytes(take(bytes, 4)?.try_into()?))
}
fn u64(bytes: &mut &[u8]) -> anyhow::Result<u64> {
    Ok(u64::from_be_bytes(take(bytes, 8)?.try_into()?))
}
fn boundary(bytes: &mut &[u8]) -> anyhow::Result<Boundary> {
    let len = u16(bytes)? as usize;
    ensure!(
        (1..=MAX_KEY_BYTES).contains(&len),
        "invalid table boundary length"
    );
    let key = Bytes::copy_from_slice(take(bytes, len)?);
    let revision = u64(bytes)?;
    ensure!(revision > 0, "invalid table boundary revision");
    Ok(Boundary { key, revision })
}
pub(super) fn decode_index(mut bytes: &[u8], size: u64) -> anyhow::Result<Vec<Block>> {
    ensure!(
        bytes.len() <= MAX_INDEX_BYTES && take(&mut bytes, 8)? == INDEX,
        "invalid table index header"
    );
    let count = u32(&mut bytes)? as usize;
    ensure!(
        (1..=MAX_BLOCKS).contains(&count),
        "invalid table index count"
    );
    let mut blocks: Vec<Block> = Vec::with_capacity(count);
    let mut expected_offset = 0u64;
    for _ in 0..count {
        let first = boundary(&mut bytes)?;
        let last = boundary(&mut bytes)?;
        ensure!(
            !first.compare(&last.key, last.revision).is_gt(),
            "reversed table block bounds"
        );
        if let Some(previous) = blocks.last() {
            ensure!(
                previous.last.compare(&first.key, first.revision).is_lt(),
                "overlapping table block bounds"
            );
        }
        let offset = u64(&mut bytes)?;
        let length = u32(&mut bytes)?;
        let rows = u32(&mut bytes)?;
        ensure!(
            offset == expected_offset
                && (27..=MAX_BLOCK_BYTES).contains(&(length as usize))
                && (1..=MAX_BLOCK_ROWS).contains(&(rows as usize)),
            "invalid table block extent"
        );
        ensure!(
            (rows == 1) == first.compare(&last.key, last.revision).is_eq()
                && 12 + u64::from(rows) * 15 <= u64::from(length),
            "table row count disagrees with block bounds"
        );
        expected_offset = offset
            .checked_add(u64::from(length))
            .context("table extent overflow")?;
        ensure!(expected_offset <= size, "table extent exceeds data object");
        let digest = take(&mut bytes, 32)?.try_into()?;
        blocks.push(Block {
            first,
            last,
            offset,
            length,
            rows,
            digest,
        });
    }
    ensure!(
        bytes.is_empty() && expected_offset == size,
        "table index does not cover its data object"
    );
    Ok(blocks)
}
/// Validate every row, including the tail after a point hit, without copying
/// unrequested keys/values. Visitors only populate private return buffers.
fn visit_block<'a>(
    mut bytes: &'a [u8],
    expected: &Block,
    mut visit: impl FnMut(Row<'a>),
) -> anyhow::Result<()> {
    ensure!(
        bytes.len() == expected.length as usize
            && bytes.len() <= MAX_BLOCK_BYTES
            && take(&mut bytes, 8)? == BLOCK,
        "invalid table block header"
    );
    let count = u32(&mut bytes)?;
    ensure!(
        count == expected.rows && (1..=MAX_BLOCK_ROWS).contains(&(count as usize)),
        "invalid table row count"
    );
    let mut previous: Option<(&[u8], u64)> = None;
    for _ in 0..count {
        let length = u16(&mut bytes)? as usize;
        let revision = u64(&mut bytes)?;
        let value_length = u32(&mut bytes)?;
        ensure!(
            (1..=MAX_KEY_BYTES).contains(&length)
                && revision > 0
                && (value_length == DELETE || value_length <= 64 * 1024),
            "invalid table row lengths"
        );
        let key = take(&mut bytes, length)?;
        let value = if value_length == DELETE {
            None
        } else {
            Some(take(&mut bytes, value_length as usize)?)
        };
        if let Some((prior, rev)) = previous {
            ensure!(
                prior.cmp(key).then_with(|| revision.cmp(&rev)).is_lt(),
                "unordered table rows"
            );
        } else {
            ensure!(
                expected.first.compare(key, revision).is_eq(),
                "table block first bound mismatch"
            );
        }
        previous = Some((key, revision));
        visit(Row {
            key,
            revision,
            value,
        });
    }
    ensure!(bytes.is_empty(), "table block has trailing bytes");
    let (key, revision) = previous.expect("nonempty block checked");
    ensure!(
        expected.last.compare(key, revision).is_eq(),
        "table block last bound mismatch"
    );
    Ok(())
}
fn owned(row: Row<'_>) -> Entry {
    Entry {
        key: Bytes::copy_from_slice(row.key),
        revision: row.revision,
        value: row.value.map(Bytes::copy_from_slice),
    }
}
pub(super) fn decode_block(bytes: &[u8], expected: &Block) -> anyhow::Result<Vec<Entry>> {
    ensure!(
        (1..=MAX_BLOCK_ROWS).contains(&(expected.rows as usize)),
        "invalid table row count"
    );
    let mut rows = Vec::with_capacity(expected.rows as usize);
    visit_block(bytes, expected, |row| rows.push(owned(row)))?;
    Ok(rows)
}
pub(super) fn lookup_block(
    bytes: &[u8],
    expected: &Block,
    key: &[u8],
    revision: u64,
) -> anyhow::Result<Option<Entry>> {
    let mut found = None;
    visit_block(bytes, expected, |row| {
        if found.is_none() && row.key == key && row.revision <= revision {
            found = Some(owned(row));
        }
    })?;
    Ok(found)
}
pub(super) fn reference(reference: &Reference) -> Bytes {
    let mut out = Vec::with_capacity(8 + 2 * REFERENCE_BYTES + 32);
    out.extend_from_slice(REF);
    reference.data.encode(&mut out);
    reference.index.encode(&mut out);
    out.extend_from_slice(&Sha256::digest(&out));
    out.into()
}
pub(super) fn decode_reference(bytes: &[u8]) -> anyhow::Result<Reference> {
    ensure!(
        bytes.len() == 8 + 2 * REFERENCE_BYTES + 32 && &bytes[..8] == REF,
        "invalid table reference"
    );
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    ensure!(
        Sha256::digest(body).as_slice() == digest,
        "table reference digest mismatch"
    );
    let data = Blob::decode(&body[8..8 + REFERENCE_BYTES])?;
    let index = Blob::decode(&body[8 + REFERENCE_BYTES..])?;
    ensure!(
        index.size() <= MAX_INDEX_BYTES as u64,
        "table index size limit exceeded"
    );
    Ok(Reference { data, index })
}
