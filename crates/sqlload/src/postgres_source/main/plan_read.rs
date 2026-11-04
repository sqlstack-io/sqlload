//! Decide how each column of a source table is read.

use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::postgres_source::_helpers::type_map::read_column;
use crate::postgres_source::models::{ReadColumn, SourceTable};

/// How to read every column of `table`, using `pinned` types where given and the natural mapping otherwise.
pub fn plan_read(
    table: &SourceTable,
    pinned: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ReadColumn>, Vec<String>> {
    let mut columns: Vec<ReadColumn> = Vec::with_capacity(table.columns.len());
    let mut problems: Vec<String> = Vec::new();
    for column in &table.columns {
        let output = match pinned.get(&column.name) {
            Some(pinned) => Ok(pinned.clone()),
            None => column
                .column_type
                .clone()
                .map_err(|reason| format!("{}: {reason}", column.name)),
        };
        match output.and_then(|output| read_column(column, &output)) {
            Ok(read) => columns.push(read),
            Err(problem) => problems.push(problem),
        }
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}
