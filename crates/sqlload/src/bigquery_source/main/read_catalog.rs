//! Discover a BigQuery source's tables: tables and views matching `include`, and query tables described by a dry run.

use crate::bigquery_api::main::query_schema::query_schema;
use crate::bigquery_source::_helpers::catalog::catalog_tables;
use crate::bigquery_source::_helpers::type_map::source_column;
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::postgres_source::models::SourceTable;

/// Every table and view of the reader's project whose `dataset.table` matches `include` and not `exclude` (exactly, as BigQuery names are), from metadata, so nothing is billed; then one table per query in `queries` (name and SQL), described by a dry run. A view, or anything else the Storage Read API cannot read directly, has `estimated_bytes` -1, so it is read through a query.
pub async fn read_catalog(
    reader: &BigQueryReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, BigQuerySourceError> {
    let mut tables: Vec<SourceTable> = match include.is_empty() {
        true => Vec::new(),
        false => catalog_tables(reader, (include, exclude)).await?,
    };
    for (name, sql) in queries {
        let fields = query_schema(&reader.client, sql).await.map_err(|error| {
            BigQuerySourceError::Invalid(format!("query table {name}: {error}"))
        })?;
        tables.push(SourceTable {
            schema: String::new(),
            name: name.clone(),
            columns: fields.iter().map(source_column).collect(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
        });
    }
    Ok(tables)
}
