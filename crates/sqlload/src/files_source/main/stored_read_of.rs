//! How a Parquet column's values are read.

use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use crate::files_source::_helpers::plan::columns::parquet_columns;
use crate::files_source::models::StoredRead;

/// How the file with `footer` holds its column `name`, from its stored Parquet type and any embedded Arrow type: nanoseconds, epoch integers or plain. `None` when the file has no such column.
pub fn stored_read_of(footer: &ArrowReaderMetadata, name: &str) -> Option<StoredRead> {
    parquet_columns(footer)
        .into_iter()
        .find(|column| column.name == name)
        .map(|column| column.stored_read)
}
