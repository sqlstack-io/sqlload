use crate::batching::_helpers::gzip::gzip;
use crate::batching::models::{BatchRow, CompressedChunk, OversizedRow};

/// Compress `rows` as one body, or split them into proportionally smaller groups until each fits.
pub(crate) fn split_rows(
    rows: &[&BatchRow],
    limit: u64,
) -> Result<Vec<CompressedChunk>, OversizedRow> {
    let Some(last) = rows.last() else {
        return Ok(Vec::new());
    };
    let plain = ndjson(rows);
    let body = gzip(&plain);
    let compressed = body.len() as u64;
    if compressed <= limit {
        return Ok(vec![CompressedChunk {
            body,
            rows: rows.len(),
            uncompressed_bytes: plain.len() as u64,
            last_offset: last.offset,
        }]);
    }
    if rows.len() == 1 {
        return Err(OversizedRow {
            topic: last.topic.clone(),
            partition: last.partition,
            offset: last.offset,
            compressed_bytes: compressed,
        });
    }
    let parts = compressed.div_ceil(limit).max(2) as usize;
    let mut chunks: Vec<CompressedChunk> = Vec::new();
    for part in rows.chunks(rows.len().div_ceil(parts)) {
        chunks.extend(split_rows(part, limit)?);
    }
    Ok(chunks)
}

fn ndjson(rows: &[&BatchRow]) -> Vec<u8> {
    let mut plain: Vec<u8> = Vec::with_capacity(rows.iter().map(|row| row.line.len() + 1).sum());
    for row in rows {
        plain.extend_from_slice(row.line.as_bytes());
        plain.push(b'\n');
    }
    plain
}
