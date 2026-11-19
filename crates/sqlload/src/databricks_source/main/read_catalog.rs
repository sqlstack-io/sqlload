//! Discover a Databricks source's tables: tables and views matching `include`, and query tables described by their result's columns.

use crate::databricks_api::main::execute_arrow::execute_arrow;
use crate::databricks_source::_helpers::catalog::catalog_tables;
use crate::databricks_source::_helpers::query::subquery;
use crate::databricks_source::_helpers::type_map::source_column;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::postgres_source::models::SourceTable;
use crate::project::models::SourceConfig;

/// Every table and view of the reader's catalog whose `schema.table` matches `source`'s `include` and not its `exclude`, from `information_schema`; then one table per query table of `source`, its columns those of the query as a load wraps it, run with `LIMIT 0` so it reads no rows.
pub async fn read_catalog(
    reader: &DatabricksReader,
    source: &SourceConfig,
) -> Result<Vec<SourceTable>, DatabricksSourceError> {
    let mut tables: Vec<SourceTable> = match source.include.is_empty() {
        true => Vec::new(),
        false => catalog_tables(reader, (&source.include, &source.exclude)).await?,
    };
    let queries = source
        .tables
        .iter()
        .filter_map(|(name, table)| Some((name, table.sql.as_deref()?)));
    for (name, sql) in queries {
        let described = execute_arrow(
            &reader.client,
            &format!("SELECT * FROM {} LIMIT 0", subquery(sql)),
        )
        .await
        .map_err(|error| DatabricksSourceError::Invalid(format!("query table {name}: {error}")))?;
        tables.push(SourceTable {
            split_pages: None,
            schema: String::new(),
            name: name.clone(),
            columns: described
                .columns
                .iter()
                .map(|(column, sql_type)| source_column(column, sql_type, true))
                .collect(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
            listed_keys: Vec::new(),
            other_location: None,
        });
    }
    Ok(tables)
}
