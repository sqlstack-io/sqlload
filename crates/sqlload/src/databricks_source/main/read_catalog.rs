//! Discover a Databricks source's tables: tables and views matching `include`, and query tables described by their result's columns.

use crate::databricks_api::main::execute_arrow::execute_arrow;
use crate::databricks_source::_helpers::catalog::catalog_tables;
use crate::databricks_source::_helpers::query::subquery;
use crate::databricks_source::_helpers::type_map::source_column;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::postgres_source::models::SourceTable;

/// Every table and view of the reader's catalog whose `schema.table` matches `include` and not `exclude`, from `information_schema`; then one table per query in `queries` (name and SQL), its columns those of the query as a load wraps it, run with `LIMIT 0` so it reads no rows.
pub async fn read_catalog(
    reader: &DatabricksReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, DatabricksSourceError> {
    let mut tables: Vec<SourceTable> = match include.is_empty() {
        true => Vec::new(),
        false => catalog_tables(reader, (include, exclude)).await?,
    };
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
        });
    }
    Ok(tables)
}
