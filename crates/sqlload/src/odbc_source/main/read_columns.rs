//! Decide how each column of an ODBC table or query is read.

use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::odbc_source::_helpers::query::quote_identifier;
use crate::postgres_source::models::{ReadColumn, SourceTable, WireType};

/// How to read every column of `table`, using `pinned` types where given and the described type otherwise; each read column's expression is its quoted name, which cursor filters compare against.
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
        columns.push(ReadColumn {
            name: column.name.clone(),
            expression: quote_identifier(&column.name),
            wire: WireType::Text,
            output,
        });
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}
