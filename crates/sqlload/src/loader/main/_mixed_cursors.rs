//! Refuse one cursor range for selected tables whose cursors compare different kinds of value.

use crate::column_types::models::ColumnType;
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::models::{PlanOptions, ProjectPlan, TablePlan};

/// The kind of value `table`'s cursor compares: integers, numbers, text, times (dates and timestamps alike), or its own type; `None` without a cursor, or when a table already blocked has no known cursor type.
fn cursor_kind(table: &TablePlan) -> Option<String> {
    let cursor = table.spec.cursor.as_ref()?;
    let read = table
        .read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
        .map(|column| column.output.clone());
    let partition = table.spec.files.as_ref().and_then(|files| {
        files
            .partition_types
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&cursor.column))
            .map(|(_, column_type)| column_type.clone())
    });
    Some(match read.or(partition) {
        Some(ColumnType::Int | ColumnType::SmallInt) => "integer".to_owned(),
        Some(ColumnType::Date | ColumnType::TimestampTz | ColumnType::TimestampNtz) => {
            "date or timestamp".to_owned()
        }
        Some(ColumnType::Text | ColumnType::Varchar(_)) => "text".to_owned(),
        Some(ColumnType::Decimal { .. } | ColumnType::Float) => "number".to_owned(),
        Some(other) => other.to_string(),
        None if table.schema.blocked.is_some() || table.spec.python.is_none() => return None,
        None => "unknown until its first load".to_owned(),
    })
}

/// The plan with every table that has a cursor blocked when `--start-cursor` or `--end-cursor` meets selected tables whose cursors compare different kinds of value, so no range is read as another type.
pub(in crate::loader) fn block_mixed_cursors(
    mut plan: ProjectPlan,
    options: &PlanOptions,
) -> ProjectPlan {
    if options.start_cursor.is_none() && options.end_cursor.is_none() {
        return plan;
    }
    let mut kinds: Vec<(String, String)> = Vec::new();
    for table in &plan.tables {
        if let Some(kind) = cursor_kind(table) {
            kinds.push((table.spec.destination.table.clone(), kind));
        }
    }
    if kinds.iter().all(|(_, kind)| *kind == kinds[0].1) {
        return plan;
    }
    let listed: Vec<String> = kinds
        .iter()
        .map(|(table, kind)| format!("{table} ({kind})"))
        .collect();
    let reason = format!(
        "--start-cursor/--end-cursor apply to every selected table, but their cursors compare different kinds of value: {}. Fix: select tables of one cursor type per run (--select), each run with its own range",
        listed.join(", ")
    );
    for table in plan
        .tables
        .iter_mut()
        .filter(|table| table.spec.cursor.is_some())
    {
        match &mut table.schema.blocked {
            Some(blocked) => blocked.reasons.push(reason.clone()),
            None => table.schema = blocked_plan(vec![reason.clone()]),
        }
    }
    plan
}
