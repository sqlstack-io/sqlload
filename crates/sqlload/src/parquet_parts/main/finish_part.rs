//! Finish the Parquet file being written.

use crate::parquet_parts::models::PartWriter;

/// Close the open file; returns the writer, ready for another, and the file's bytes (`None` when none was open).
pub fn finish_part(mut parts: PartWriter) -> Result<(PartWriter, Option<Vec<u8>>), String> {
    parts.schema = None;
    let Some(writer) = parts.open.take() else {
        return Ok((parts, None));
    };
    let bytes = writer.into_inner().map_err(|error| error.to_string())?;
    parts.finished_bytes += bytes.len();
    Ok((parts, Some(bytes)))
}
