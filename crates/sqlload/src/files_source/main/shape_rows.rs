//! Shape rows made from a file (by a `transform`) as the file's own rows are.

use arrow_array::RecordBatch;

use crate::files_source::_helpers::plan::shape::{output_schema, shape_batch};
use crate::files_source::models::{DataFile, FilesRead};

/// `batch`, made from `file`'s rows, in `read`'s columns: each cast to its type, missing ones NULL, partition and source-file columns filled in, and rows numbered after `rows_before`; a value that does not convert names its column.
pub fn shape_rows(
    batch: &RecordBatch,
    (file, file_name, rows_before): (&DataFile, &str, u64),
    read: &FilesRead,
) -> Result<RecordBatch, String> {
    let schema = output_schema(&read.columns, &read.nanos)?;
    shape_batch(batch, file, (file_name, rows_before), &schema).map_err(|error| error.reason)
}
