//! Discover a DuckDB or MotherDuck source's tables: tables and views matching `include`, and query tables described by their result's columns.

use crate::duckdb_source::_helpers::catalog::{catalog_tables, described_columns};
use crate::duckdb_source::_helpers::connection::text_rows;
use crate::duckdb_source::_helpers::query::subquery;
use crate::duckdb_source::_helpers::type_map::source_column;
use crate::duckdb_source::constants::CATALOG_QUERIES;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::postgres_source::models::SourceTable;
use crate::project::models::SourceConfig;

/// Every table and view of the reader's database whose `schema.table` matches `source`'s `include` and not its `exclude`; then one table per query table of `source`, its columns those of the query as a load wraps it, described with `DESCRIBE` so it reads no rows. Every query runs on one connection.
pub async fn read_catalog(
    reader: &DuckDbReader,
    source: &SourceConfig,
) -> Result<Vec<SourceTable>, DuckDbSourceError> {
    let named: Vec<(&String, &str)> = source
        .tables
        .iter()
        .filter_map(|(name, table)| Some((name, table.sql.as_deref()?)))
        .collect();
    let catalog = match source.include.is_empty() {
        true => 0,
        false => CATALOG_QUERIES.len(),
    };
    let queries: Vec<String> = CATALOG_QUERIES[..catalog]
        .iter()
        .map(|query| (*query).to_owned())
        .chain(
            named
                .iter()
                .map(|(_, sql)| format!("DESCRIBE SELECT * FROM {}", subquery(sql))),
        )
        .collect();
    let answers = text_rows(reader, queries).await.map_err(|(index, error)| {
        match index
            .checked_sub(catalog)
            .and_then(|query| named.get(query))
        {
            Some((name, _)) => DuckDbSourceError::Invalid(format!("query table {name}: {error}")),
            None => error,
        }
    })?;
    let (listed, described) = answers.split_at(catalog);
    let mut tables: Vec<SourceTable> = match catalog {
        0 => Vec::new(),
        _ => catalog_tables(listed, (&source.include, &source.exclude))?,
    };
    for ((name, _), rows) in named.iter().zip(described) {
        tables.push(SourceTable {
            split_pages: None,
            schema: String::new(),
            name: (*name).clone(),
            columns: described_columns(rows)
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
}
