//! Catalog rows and described query columns as source tables.

use std::collections::BTreeMap;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::snowflake_source::_helpers::type_map::{loaded_name, natural_type};
use crate::snowflake_source::constants::CATALOG_NOT_NULL;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::sql_api::models::SqlRows;

/// A case-insensitive matcher for `SCHEMA.TABLE` patterns such as `MARTS.*`.
pub(crate) fn table_matcher(patterns: &[String]) -> Result<GlobSet, SnowflakeSourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .case_insensitive(true)
            .build()
            .map_err(|error| {
                SnowflakeSourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
            })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| SnowflakeSourceError::Invalid(error.to_string()))
}

/// Catalog rows grouped into tables, keeping those `include` matches and `exclude` does not.
pub(crate) fn catalog_tables(
    rows: &SqlRows,
    include: &GlobSet,
    exclude: &GlobSet,
) -> Vec<SourceTable> {
    let mut tables: BTreeMap<(String, String), SourceTable> = BTreeMap::new();
    for row in &rows.rows {
        let cell = |index: usize| row.get(index).cloned().flatten();
        let text = |index: usize| cell(index).unwrap_or_default();
        let number = |index: usize| cell(index).and_then(|value| catalog_number(&value));
        let (schema, name) = (text(0), text(1));
        let qualified = format!("{schema}.{name}");
        if !include.is_match(&qualified) || exclude.is_match(&qualified) {
            continue;
        }
        let data_type = text(3);
        let type_text = match data_type.as_str() {
            "NUMBER" => format!(
                "NUMBER({},{})",
                number(4).unwrap_or(38),
                number(5).unwrap_or(0)
            ),
            "TEXT" => match number(6) {
                Some(length) => format!("VARCHAR({length})"),
                None => "VARCHAR".to_owned(),
            },
            _ => data_type,
        };
        let table = tables
            .entry((schema.clone(), name.clone()))
            .or_insert_with(|| SourceTable {
                schema,
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                indexed_columns: Vec::new(),
                estimated_rows: number(8).unwrap_or(-1),
                estimated_bytes: number(9).unwrap_or(-1),
                comment: cell(10).filter(|comment| !comment.trim().is_empty()),
            });
        table.columns.push(source_column(
            &text(2),
            &type_text,
            text(7) != CATALOG_NOT_NULL,
        ));
    }
    tables.into_values().collect()
}

/// A column of Snowflake type `type_text`, loaded under its lower-case name when it is a plain upper-case identifier.
pub(crate) fn source_column(name: &str, type_text: &str, nullable: bool) -> SourceColumn {
    let loaded = loaded_name(name);
    SourceColumn {
        source_spelling: (loaded != name).then(|| name.to_owned()),
        name: loaded,
        postgres_type: type_text.to_owned(),
        pg_type: PgType::Other,
        column_type: natural_type(type_text),
        nullable,
    }
}

/// A catalog count or size; `None` when the catalog has none, as for a view.
fn catalog_number(value: &str) -> Option<i64> {
    match value.parse::<i64>() {
        Ok(number) => Some(number),
        Err(_) => None,
    }
}
