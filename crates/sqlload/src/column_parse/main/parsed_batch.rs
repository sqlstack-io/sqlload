use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use chrono_tz::Tz;

use crate::column_parse::_helpers::arrays::{parsed_columns, row_labels};
use crate::column_parse::models::ColumnParse;

/// `batch` with each column `parses` names (by its loaded name) parsed into a `timestamp_tz` (UTC) or `date` column, a time without an offset in its `tz`, else `zone`; other columns as they are. A value no format reads fails, naming its row (its file and row there, for a files source's batch, else its row in the read, after `rows_before`), the column and the value, unless its parse has `on_error = "null"`.
pub fn parsed_batch(
    batch: &RecordBatch,
    parses: &BTreeMap<String, ColumnParse>,
    zone: Tz,
    rows_before: u64,
) -> Result<RecordBatch, String> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|field| parses.contains_key(field.name()))
    {
        return Ok(batch.clone());
    }
    let label = row_labels(batch, rows_before);
    let (fields, arrays) = parsed_columns(batch, parses, zone, &label)?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())
}
