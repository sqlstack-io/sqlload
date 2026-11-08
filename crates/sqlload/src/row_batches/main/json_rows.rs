//! Rows from JSON or NDJSON bytes.

use crate::row_batches::_helpers::json_document::select_path;
use crate::row_batches::_helpers::json_seed::RowSink;
use crate::row_batches::constants::BYTE_ORDER_MARK;
use crate::row_batches::models::{JsonLayout, JsonRead, RowColumns};

/// Parse `bytes` as `read` says, handing `on_rows` the rows every `read.batch_rows` rows; malformed JSON is an error naming its line.
pub fn json_rows(
    bytes: &[u8],
    read: &JsonRead,
    on_rows: impl FnMut(RowColumns) -> Result<(), String>,
) -> Result<(), String> {
    let bytes = bytes.strip_prefix(BYTE_ORDER_MARK).unwrap_or(bytes);
    let mut sink = RowSink::new(read.batch_rows, read.projection.clone(), on_rows);
    match (read.layout, read.path.as_deref()) {
        (JsonLayout::Lines, None) => sink.read_lines(bytes)?,
        (JsonLayout::Lines, Some(_)) => {
            return Err("a path picks rows out of a JSON document, not NDJSON".to_owned());
        }
        (JsonLayout::Document, None) => sink.read_document(bytes)?,
        (JsonLayout::Document, Some(path)) => sink.read_document(select_path(bytes, path)?)?,
    }
    sink.finish()
}
