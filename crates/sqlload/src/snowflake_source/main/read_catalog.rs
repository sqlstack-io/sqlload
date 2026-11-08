//! Discover a Snowflake source's tables: catalog tables matching `include`, and query tables described without reading rows.

use crate::postgres_source::models::{SourceColumn, SourceTable};
use crate::snowflake_source::_helpers::catalog::{catalog_tables, source_column, table_matcher};
use crate::snowflake_source::_helpers::unload_query::subquery;
use crate::snowflake_source::constants::{CATALOG_SQL, DESCRIBED_NOT_NULL};
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::SnowflakeReader;
use crate::sql_api::main::execute_script_rows::execute_script_rows;
use crate::sql_api::main::execute_sql::execute_sql;

/// Every table and view of the connection's database whose `SCHEMA.TABLE` matches `include` and not `exclude` (ignoring case), then one table per query in `queries` (name and SQL), its columns as Snowflake describes the query's result.
pub async fn read_catalog(
    reader: &SnowflakeReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, SnowflakeSourceError> {
    let mut tables: Vec<SourceTable> = Vec::new();
    if !include.is_empty() {
        let include = table_matcher(include)?;
        let exclude = table_matcher(exclude)?;
        let rows = execute_sql(&reader.sql, CATALOG_SQL).await?;
        tables.extend(catalog_tables(&rows, &include, &exclude));
    }
    for (name, sql) in queries {
        let statements = vec![
            format!("SELECT * FROM {} LIMIT 0", subquery(sql)),
            "DESCRIBE RESULT LAST_QUERY_ID()".to_owned(),
        ];
        let described = execute_script_rows(&reader.sql, &statements)
            .await
            .map_err(|error| {
                SnowflakeSourceError::Invalid(format!("query table {name}: {error}"))
            })?;
        let result = described.last().cloned().unwrap_or_default();
        let columns: Vec<SourceColumn> = result
            .rows
            .iter()
            .map(|row| {
                let text = |column: &str| result.value(row, column).unwrap_or_default().to_owned();
                source_column(
                    &text("name"),
                    &text("type"),
                    text("null?") != DESCRIBED_NOT_NULL,
                )
            })
            .collect();
        tables.push(SourceTable {
            schema: String::new(),
            name: name.clone(),
            columns,
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
        });
    }
    Ok(tables)
}
