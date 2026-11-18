//! The tables and views `include` matches, as source tables from DuckDB's catalog functions.

use std::collections::BTreeMap;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::duckdb_source::_helpers::type_map::source_column;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::postgres_source::models::SourceTable;

/// The database's tables and views whose `schema.table` matches `include` and not `exclude` (exactly, as DuckDB keeps names), sorted: from `duckdb_tables()` and `duckdb_views()`, their columns from `duckdb_columns()` and their primary keys from `duckdb_constraints()`, all of the file's own database.
pub(crate) fn catalog_tables(
    connection: &duckdb::Connection,
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, DuckDbSourceError> {
    let (included, excluded) = (matcher(include)?, matcher(exclude)?);
    let mut tables: BTreeMap<(String, String), SourceTable> = BTreeMap::new();
    let mut listed = connection.prepare(
        "SELECT schema_name, table_name, comment, estimated_size FROM duckdb_tables() WHERE database_name = current_database() \
         UNION ALL SELECT schema_name, view_name, comment, -1 FROM duckdb_views() WHERE database_name = current_database() AND NOT internal",
    )?;
    let rows = listed.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    for row in rows {
        let (schema, name, comment, estimated_rows) = row?;
        let qualified = format!("{schema}.{name}");
        if !included.is_match(&qualified) || excluded.is_match(&qualified) {
            continue;
        }
        tables.insert(
            (schema.clone(), name.clone()),
            SourceTable {
                split_pages: None,
                schema,
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                indexed_columns: Vec::new(),
                estimated_rows,
                estimated_bytes: -1,
                comment: comment.filter(|comment| !comment.is_empty()),
                listed_keys: Vec::new(),
            },
        );
    }
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let mut columns = connection.prepare(
        "SELECT schema_name, table_name, column_name, data_type, is_nullable FROM duckdb_columns() WHERE database_name = current_database() ORDER BY schema_name, table_name, column_index",
    )?;
    let rows = columns.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, bool>(4)?,
        ))
    })?;
    for row in rows {
        let (key, name, data_type, nullable) = row?;
        if let Some(table) = tables.get_mut(&key) {
            table.columns.push(source_column(&name, &data_type, nullable));
        }
    }
    let mut keys = connection.prepare(
        "SELECT schema_name, table_name, constraint_column_names FROM duckdb_constraints() WHERE database_name = current_database() AND constraint_type = 'PRIMARY KEY'",
    )?;
    let rows = keys.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            row.get::<_, duckdb::types::Value>(2)?,
        ))
    })?;
    for row in rows {
        let (key, names) = row?;
        if let (Some(table), duckdb::types::Value::List(names)) = (tables.get_mut(&key), names) {
            table.primary_key = names
                .into_iter()
                .filter_map(|name| match name {
                    duckdb::types::Value::Text(name) => Some(name),
                    _ => None,
                })
                .collect();
        }
    }
    Ok(tables.into_values().collect())
}

/// One glob set for `schema.table` patterns, matched exactly.
fn matcher(patterns: &[String]) -> Result<GlobSet, DuckDbSourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern).build().map_err(|error| {
            DuckDbSourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
        })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| DuckDbSourceError::Invalid(error.to_string()))
}
