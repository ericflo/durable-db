use super::*;
use crate::{
    Error, Result,
    engine::{Pair, ReadRequest, ReadResult, ScanResult},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};

impl<C: Clock> Owner<C> {
    /// Evaluate the public read contract against one committed view. Database
    /// existence and access must already be established by the caller. Reading
    /// a cold generation does not load its receipt window or create a validator.
    pub async fn read_request(
        &self,
        database: Database<'_>,
        request: &ReadRequest,
    ) -> Result<ReadResult> {
        request.validate()?;
        self.check()?;
        let view = self.projection.view(database).await?;
        let revision = view.as_ref().map_or(0, recovery::View::revision);
        if request.expected_revision.is_some_and(|r| r != revision) {
            return Err(Error::Conflict);
        }
        let mut result = ReadResult {
            revision,
            values: Vec::new(),
            scans: Vec::new(),
        };
        let mut size = 0;
        let mut steps = 0;
        for key in &request.keys {
            let row = match &view {
                Some(view) => view.get(key.as_bytes(), revision).await?,
                None => None,
            };
            let value = row.and_then(|r| r.value).map(|v| STANDARD.encode(v));
            size += value.as_ref().map_or(0, String::len);
            check_size(size)?;
            result.values.push(value);
        }
        for scan in &request.scans {
            let prefix_end = prefix_end(&scan.prefix);
            let start = [
                Some(scan.prefix.as_str()),
                scan.start.as_deref(),
                if scan.reverse {
                    None
                } else {
                    scan.after.as_deref()
                },
            ]
            .into_iter()
            .flatten()
            .max()
            .unwrap();
            let end = [
                prefix_end.as_deref(),
                scan.end.as_deref(),
                if scan.reverse {
                    scan.after.as_deref()
                } else {
                    None
                },
            ]
            .into_iter()
            .flatten()
            .min();
            let mut items = Vec::new();
            let mut more = false;
            if let Some(view) = &view
                && end.is_none_or(|end| start < end)
            {
                let mut cursor = view.scan_ordered(
                    start.as_bytes(),
                    end.map(str::as_bytes),
                    revision,
                    scan.reverse,
                )?;
                while let Some(row) = cursor.next().await? {
                    steps += 1;
                    if steps > 4096 {
                        return Err(Error::Limit(
                            "read work budget exceeded; narrow the scan".into(),
                        ));
                    }
                    let key = std::str::from_utf8(&row.key).context("invalid stored key")?;
                    if !key.starts_with(&scan.prefix) {
                        break;
                    }
                    if scan.after.as_deref() == Some(key) {
                        continue;
                    }
                    let Some(value) = row.value else { continue };
                    if items.len() == scan.limit {
                        more = true;
                        break;
                    }
                    let value = STANDARD.encode(value);
                    size += key.len() + value.len();
                    check_size(size)?;
                    items.push(Pair {
                        key: key.into(),
                        value,
                    });
                }
            }
            let next = more.then(|| items.last().expect("positive scan limit").key.clone());
            result.scans.push(ScanResult { items, next });
        }
        result.validate_size()?;
        self.check()?;
        Ok(result)
    }
}
// UTF-8 preserves scalar ordering. Advancing the last nonmaximum scalar gives
// a valid exclusive prefix bound; at a UTF-8 width boundary it can be one byte
// longer than a maximum-length user key. It is a bound, never a stored key.
fn prefix_end(prefix: &str) -> Option<String> {
    let mut end = prefix.to_owned();
    while let Some((offset, ch)) = end.char_indices().next_back() {
        end.truncate(offset);
        if ch == char::MAX {
            continue;
        }
        let next = if ch as u32 == 0xd7ff {
            0xe000
        } else {
            ch as u32 + 1
        };
        end.push(char::from_u32(next).expect("next Unicode scalar"));
        return Some(end);
    }
    None
}
fn check_size(size: usize) -> Result<()> {
    if size > 1024 * 1024 {
        return Err(Error::Limit(
            "read result exceeds 1 MiB; reduce scan limit".into(),
        ));
    }
    Ok(())
}
