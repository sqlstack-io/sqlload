//! A batch's rows as BigQuery takes them.

use arrow_array::RecordBatch;

use crate::config::models::ColumnConfig;
use crate::file_format::_helpers::columns::bigquery_kind;
use crate::file_format::_helpers::parquet::record_batch;
use crate::file_format::models::ParquetKind;

/// `lines` (JSON objects keyed by column name) as one Arrow batch typed by each column's BigQuery type: `INT64` and its aliases, `TIMESTAMP` (milliseconds, UTC), `DATE`, and text for the rest; a value its column cannot hold is an error naming the column.
pub fn bigquery_rows(columns: &[ColumnConfig], lines: &[&str]) -> Result<RecordBatch, String> {
    let kinds: Vec<ParquetKind> = columns.iter().map(bigquery_kind).collect();
    record_batch(columns, &kinds, lines)
}
