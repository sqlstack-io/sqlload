//! The tables and views `include` matches, as source tables from DuckDB's catalog functions.

use std::collections::BTreeMap;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::duckdb_source::_helpers::connection::TextRows;
use crate::duckdb_source::_helpers::type_map::source_column;
use crate::duckdb_source::constants::{CATALOG_QUERIES, NOT_NULL, TRUE_TEXT};
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::postgres_source::models::SourceTable;

/// The database's tables and views whose `schema.table` matches `include` and not `exclude` (exactly, as DuckDB keeps names), sorted, from the answers to `CATALOG_QUERIES` in order: the tables and views, their columns, and their primary keys.
pub(crate) fn catalog_tables(
    answers: &[TextRows],
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, DuckDbSourceError> {
    let (included, excluded) = (matcher(include)?, matcher(exclude)?);
    let [listed, columns, keys] = answers else {
        return Err(DuckDbSourceError::Invalid(format!(
            "the catalog answered {} of its {} queries",
            answers.len(),
            CATALOG_QUERIES.len()
        )));
    };
    let text = |row: &[Option<String>], index: usize| {
        row.get(index).cloned().flatten().unwrap_or_default()
    };
    let mut tables: BTreeMap<(String, String), SourceTable> = BTreeMap::new();
    for row in listed {
        let (schema, name) = (text(row, 0), text(row, 1));
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
                estimated_rows: text(row, 3).parse().unwrap_or(-1),
                estimated_bytes: -1,
                comment: Some(text(row, 2)).filter(|comment| !comment.is_empty()),
                listed_keys: Vec::new(),
            },
        );
    }
    for row in columns {
        if let Some(table) = tables.get_mut(&(text(row, 0), text(row, 1))) {
            let nullable = text(row, 4) == TRUE_TEXT;
            table
                .columns
                .push(source_column(&text(row, 2), &text(row, 3), nullable));
        }
    }
    for row in keys {
        if let Some(table) = tables.get_mut(&(text(row, 0), text(row, 1))) {
            table.primary_key.push(text(row, 2));
        }
    }
    Ok(tables.into_values().collect())
}

/// Each result column a `DESCRIBE` answered: its name, DuckDB type and whether it may be NULL.
pub(crate) fn described_columns(rows: &TextRows) -> Vec<(String, String, bool)> {
    rows.iter()
        .map(|row| {
            let text = |index: usize| row.get(index).cloned().flatten();
            (
                text(0).unwrap_or_default(),
                text(1).unwrap_or_default(),
                text(2).is_none_or(|null| null != NOT_NULL),
            )
        })
        .collect()
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
