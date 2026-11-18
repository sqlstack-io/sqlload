//! Discover a DuckDB source's tables: tables and views matching `include`, and query tables described by their result's columns.

use crate::duckdb_source::_helpers::catalog::catalog_tables;
use crate::duckdb_source::_helpers::connection::open_read_only;
use crate::duckdb_source::_helpers::query::subquery;
use crate::duckdb_source::_helpers::type_map::source_column;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::postgres_source::models::SourceTable;

/// Every table and view of the reader's database whose `schema.table` matches `include` and not `exclude`; then one table per query in `queries` (name and SQL), its columns those of the query as a load wraps it, described with `DESCRIBE` so it reads no rows.
pub async fn read_catalog(
    reader: &DuckDbReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, DuckDbSourceError> {
    let (reader, include, exclude, queries) =
        (reader.clone(), include.to_vec(), exclude.to_vec(), queries.to_vec());
    tokio::task::spawn_blocking(move || {
        let connection = open_read_only(&reader)?;
        let mut tables: Vec<SourceTable> = match include.is_empty() {
            true => Vec::new(),
            false => catalog_tables(&connection, (&include, &exclude))?,
        };
        for (name, sql) in &queries {
            let described = describe(&connection, sql).map_err(|error| {
                DuckDbSourceError::Invalid(format!("query table {name}: {error}"))
            })?;
            tables.push(SourceTable {
                split_pages: None,
                schema: String::new(),
                name: name.clone(),
                columns: described
                    .iter()
                    .map(|(column, data_type, nullable)| source_column(column, data_type, *nullable))
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
    })
    .await
    .map_err(|error| DuckDbSourceError::Invalid(format!("reading the catalog stopped: {error}")))?
}

/// Each result column of `sql`: its name, DuckDB type and whether it may be NULL.
fn describe(
    connection: &duckdb::Connection,
    sql: &str,
) -> Result<Vec<(String, String, bool)>, duckdb::Error> {
    let mut statement =
        connection.prepare(&format!("DESCRIBE SELECT * FROM {}", subquery(sql)))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?.is_none_or(|null| null != "NO"),
        ))
    })?;
    rows.collect()
}
