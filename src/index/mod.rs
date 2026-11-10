//! Native value-index components. These do not grant serving authority or
//! implement a transaction engine, compactor or collector.
pub mod memory;
pub mod merge;
pub mod recovery;
pub mod table;

// Includes room for a database/generation prefix around a 1 KiB application key.
const MAX_KEY_BYTES: usize = 2048;

fn validate_key(key: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !key.is_empty() && key.len() <= MAX_KEY_BYTES,
        "invalid index lookup key"
    );
    Ok(())
}

fn validate_range(start: &[u8], end: Option<&[u8]>) -> anyhow::Result<()> {
    anyhow::ensure!(
        start.len() <= MAX_KEY_BYTES
            && end.is_none_or(|end| end.len() <= MAX_KEY_BYTES && start < end),
        "invalid index scan bounds"
    );
    Ok(())
}
