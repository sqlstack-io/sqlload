//! Check that a condition runs on a table, without running it.

use crate::bigquery_api::main::query_schema::query_schema;
use crate::bigquery_source::_helpers::query::count_query;
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::postgres_source::models::SourceTable;

/// Whether counting the rows of `table` (or of its query `sql`) that `filter` selects would run, from a dry run: nothing is read or billed.
pub async fn dry_count(
    reader: &BigQueryReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<(), BigQuerySourceError> {
    let query = count_query(&reader.project, (table, sql), filter);
    query_schema(&reader.client, &query).await?;
    Ok(())
}
