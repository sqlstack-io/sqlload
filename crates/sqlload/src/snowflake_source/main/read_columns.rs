//! Decide how each column of a Snowflake table or query is read.

use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, SourceTable, WireType};
use crate::snowflake_source::_helpers::type_map::{quote_identifier, unload_expression};

/// How to read every column of `table`, using `pinned` types where given and the natural mapping otherwise; each read column's expression is its quoted Snowflake name, which cursor filters compare against.
pub fn read_columns(
    table: &SourceTable,
    pinned: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ReadColumn>, Vec<String>> {
    let mut columns: Vec<ReadColumn> = Vec::with_capacity(table.columns.len());
    let mut problems: Vec<String> = Vec::new();
    for column in &table.columns {
        let output = match (pinned.get(&column.name), &column.column_type) {
            (Some(pinned), _) => pinned.clone(),
            (None, Ok(natural)) => natural.clone(),
            (None, Err(reason)) => {
                problems.push(format!("{}: {reason}", column.name));
                continue;
            }
        };
        let quoted = quote_identifier(column.source_spelling.as_deref().unwrap_or(&column.name));
        if unload_expression(&column.postgres_type, &output, &quoted).is_none() {
            problems.push(format!(
                "{} has type {}, which cannot be read as {output}",
                column.name, column.postgres_type
            ));
            continue;
        }
        columns.push(ReadColumn {
            name: column.name.clone(),
            expression: quoted,
            wire: WireType::Text,
            output,
        });
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}
