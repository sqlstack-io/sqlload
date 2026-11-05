//! Append one record to the run log table.

use crate::column_types::models::Warehouse;
use crate::run_log::constants::RUNS_COLUMNS;
use crate::run_log::models::RunRecord;
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::main::sql_literal::sql_literal;
use crate::adapters::contract::main::stored_name::stored_name;
use crate::adapters::contract::models::TableName;

/// `INSERT INTO <runs> (...) VALUES (...)` for `record`, NULL for fields it does not have.
pub fn insert_run(
    runs: &TableName,
    record: &RunRecord,
    warehouse: Warehouse,
) -> Result<String, String> {
    let json = serde_json::to_string(record).map_err(|error| error.to_string())?;
    let values: Vec<Option<String>> = vec![
        Some(record.run_id.clone()),
        Some(record.event.to_string()),
        Some(record.source.clone()),
        Some(record.source_table.clone()),
        Some(record.table_name.clone()),
        record.cursor_column.clone(),
        record.cursor_before.clone(),
        record.cursor_after.clone(),
        record.state.clone(),
        Some(record.row_count.to_string()),
        record.note.clone(),
        Some(record.recorded_at.clone()),
        Some(record.format_version.to_string()),
        Some(json),
    ];
    let names: Vec<String> = RUNS_COLUMNS
        .iter()
        .map(|name| format!("\"{}\"", stored_name(name, warehouse)))
        .collect();
    let literals: Vec<String> = values
        .iter()
        .map(|value| match value {
            Some(text) => sql_literal(text, warehouse),
            None => "NULL".to_owned(),
        })
        .collect();
    Ok(format!(
        "INSERT INTO {} ({}) VALUES ({})",
        qualified_name(runs, warehouse),
        names.join(", "),
        literals.join(", ")
    ))
}
