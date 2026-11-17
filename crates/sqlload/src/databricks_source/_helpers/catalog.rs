//! The tables and views `include` matches, as source tables from the catalog's `information_schema`.

use std::collections::{BTreeMap, BTreeSet};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::databricks_api::main::execute_statement::execute_statement;
use crate::databricks_source::_helpers::query::{quote_identifier, quote_text};
use crate::databricks_source::_helpers::type_map::source_column;
use crate::databricks_source::constants::{INFORMATION_SCHEMA, NOT_NULLABLE};
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::postgres_source::models::SourceTable;

/// The reader's catalog's tables and views whose `schema.table` matches `include` and not `exclude` (exactly, as Unity Catalog stores names, lower-cased), sorted: three metadata queries, for the tables, the columns of the schemas they are in, and their primary keys.
pub(crate) async fn catalog_tables(
    reader: &DatabricksReader,
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, DatabricksSourceError> {
    let (included, excluded) = (matcher(include)?, matcher(exclude)?);
    let schema = |view: &str| {
        format!(
            "{}.{INFORMATION_SCHEMA}.{view}",
            quote_identifier(&reader.catalog)
        )
    };
    let listed = execute_statement(
        &reader.client,
        &format!(
            "SELECT table_schema, table_name, comment FROM {} WHERE table_schema <> {} ORDER BY table_schema, table_name",
            schema("tables"),
            quote_text(INFORMATION_SCHEMA)
        ),
    )
    .await?;
    let mut tables: BTreeMap<(String, String), SourceTable> = BTreeMap::new();
    for row in &listed.rows {
        let (Some(schema), Some(name)) = (text(row, 0), text(row, 1)) else {
            continue;
        };
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
                estimated_rows: -1,
                estimated_bytes: -1,
                comment: text(row, 2).filter(|comment| !comment.is_empty()),
                listed_keys: Vec::new(),
            },
        );
    }
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let schemas: BTreeSet<&str> = tables.keys().map(|(schema, _)| schema.as_str()).collect();
    let within = schemas
        .iter()
        .map(|schema| quote_text(schema))
        .collect::<Vec<String>>()
        .join(", ");
    let columns = execute_statement(
        &reader.client,
        &format!(
            "SELECT table_schema, table_name, column_name, full_data_type, is_nullable FROM {} WHERE table_schema IN ({within}) ORDER BY table_schema, table_name, ordinal_position",
            schema("columns")
        ),
    )
    .await?;
    for row in &columns.rows {
        if let (Some(table), Some(name), Some(sql_type)) = (
            table_key(row).and_then(|key| tables.get_mut(&key)),
            text(row, 2),
            text(row, 3),
        ) {
            let nullable = text(row, 4).is_none_or(|nullable| nullable != NOT_NULLABLE);
            table
                .columns
                .push(source_column(&name, &sql_type, nullable));
        }
    }
    let keys = execute_statement(
        &reader.client,
        &format!(
            "SELECT k.table_schema, k.table_name, k.column_name FROM {} AS c JOIN {} AS k ON k.constraint_catalog = c.constraint_catalog AND k.constraint_schema = c.constraint_schema AND k.constraint_name = c.constraint_name WHERE c.constraint_type = 'PRIMARY KEY' AND k.table_schema IN ({within}) ORDER BY k.table_schema, k.table_name, k.ordinal_position",
            schema("table_constraints"),
            schema("key_column_usage")
        ),
    )
    .await?;
    for row in &keys.rows {
        if let (Some(table), Some(column)) = (
            table_key(row).and_then(|key| tables.get_mut(&key)),
            text(row, 2),
        ) {
            table.primary_key.push(column);
        }
    }
    Ok(tables.into_values().collect())
}

/// The `(schema, table)` the row's first two values name.
fn table_key(row: &[Option<String>]) -> Option<(String, String)> {
    Some((text(row, 0)?, text(row, 1)?))
}

/// The row's value at `index`, when it has one.
fn text(row: &[Option<String>], index: usize) -> Option<String> {
    row.get(index).cloned().flatten()
}

/// One glob set for `schema.table` patterns, matched exactly.
fn matcher(patterns: &[String]) -> Result<GlobSet, DatabricksSourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern).build().map_err(|error| {
            DatabricksSourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
        })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| DatabricksSourceError::Invalid(error.to_string()))
}
