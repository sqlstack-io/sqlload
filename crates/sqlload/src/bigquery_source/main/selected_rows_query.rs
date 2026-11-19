//! The query a destination writing through a BigQuery source's own connection runs to select its rows itself.

use crate::bigquery_source::_helpers::query::select_query;
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};

/// The `SELECT` of `request`'s read columns that the Storage Read API would read: a table read directly is read as of the reader's instant (`FOR SYSTEM_TIME AS OF`), so every part of one load sees the same rows; a view, a query table or a table with a `where` is queried as SQLLoad's own read queries it.
pub fn selected_rows_query(reader: &BigQueryReader, request: &BigQueryReadRequest<'_>) -> String {
    let direct = request.sql.is_none() && request.table.estimated_bytes >= 0 && !request.filtered;
    select_query(
        &reader.project,
        request,
        direct.then_some(reader.snapshot_ms),
    )
}
