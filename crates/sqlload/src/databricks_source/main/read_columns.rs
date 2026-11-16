//! Decide how each column of a Databricks table or query is read.

use std::collections::BTreeMap;

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::databricks_source::_helpers::query::quote_identifier;
use crate::postgres_source::models::{ReadColumn, SourceTable, WireType};

/// How to read every column of `table`, using `pinned` types where given and the natural mapping otherwise; each read column's expression is its quoted name, which cursor filters compare against. A pin with no Arrow type, or an `array(T)` pin (Databricks sends its arrays as JSON text, read as `json`), is a problem.
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
        let readable =
            arrow_field(&column.name, &output).is_some() && !matches!(output, ColumnType::Array(_));
        if !readable {
            problems.push(format!(
                "{} has type {}, which cannot be read as {output}",
                column.name, column.postgres_type
            ));
            continue;
        }
        columns.push(ReadColumn {
            name: column.name.clone(),
            expression: quote_identifier(column.source_spelling.as_deref().unwrap_or(&column.name)),
            wire: WireType::Text,
            output,
        });
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}
