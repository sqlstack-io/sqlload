use crate::loader::constants::PLAN_DETAIL_COLUMN;
use crate::loader::models::TablePlan;
use crate::schema_plan::models::TableAction;
use crate::write_sql::main::display_name::display_name;
use crate::write_sql::models::WriteMode;

/// A section: a heading with an optional right-aligned note, then each item as a tree branch.
pub(crate) fn tree(heading: &str, note: &str, items: Vec<Vec<String>>) -> Vec<String> {
    if items.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![aligned(heading, note)];
    let count = items.len();
    for (index, item) in items.into_iter().enumerate() {
        let last = index + 1 == count;
        let (branch, indent) = if last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };
        let detail_count = item.len().saturating_sub(1);
        for (line_index, line) in item.into_iter().enumerate() {
            match line_index {
                0 => lines.push(format!("{branch}{line}")),
                _ => {
                    let sub = if line_index == detail_count {
                        "└── "
                    } else {
                        "├── "
                    };
                    lines.push(format!("{indent}{sub}{line}"));
                }
            }
        }
    }
    lines.push(String::new());
    lines
}

fn aligned(left: &str, right: &str) -> String {
    match right.is_empty() {
        true => left.to_owned(),
        false => format!("{left:<width$}{right}", width = PLAN_DETAIL_COLUMN),
    }
}

/// `name` padded so `detail` starts in the plan's detail column (after a branch prefix).
fn row(name: &str, detail: &str) -> String {
    format!("{name:<width$}{detail}", width = PLAN_DETAIL_COLUMN - 4)
}

pub(crate) fn object_lines(table: &TablePlan) -> Vec<String> {
    let name = display_name(&table.spec.destination, table.spec.warehouse);
    let mut lines = vec![match &table.schema.action {
        TableAction::Recreate => row(
            &name,
            &format!(
                "recreate table ({} columns, --reload)",
                table.schema.write_columns.len()
            ),
        ),
        TableAction::Create => row(
            &name,
            &format!(
                "create table ({} columns, from source catalog)",
                table.schema.write_columns.len()
            ),
        ),
        _ => row(&name, "alter table"),
    }];
    if let TableAction::Alter(changes) = &table.schema.action {
        lines.extend(changes.iter().map(ToString::to_string));
    }
    lines.extend(
        table
            .spec
            .excluded_columns
            .iter()
            .map(|(column, rule)| format!("excluded  {column} ({rule})")),
    );
    lines.extend(
        table
            .spec
            .masked
            .iter()
            .map(|(column, method)| format!("masked  {column} ({method})")),
    );
    lines
}

pub(crate) fn load_lines(table: &TablePlan) -> Vec<String> {
    let spec = &table.spec;
    let strategy = match &table.mode {
        WriteMode::Merge { unique_key } => format!("merge (unique_key: {})", unique_key.join(", ")),
        WriteMode::DeleteInsert { cursor_column, .. } => {
            format!("delete_insert (cursor: {cursor_column})")
        }
        WriteMode::Append => "append".to_owned(),
        WriteMode::Replace => "replace".to_owned(),
        WriteMode::Recreate => "recreate from the whole source (--reload)".to_owned(),
    };
    let origin = format!("  ({})", spec.strategy_origin);
    let mut lines = vec![
        row(&spec.destination.table, &format!("{strategy}{origin}")),
        format!(
            "source  postgres  {}  {}",
            spec.source_name,
            spec.source_table.qualified_name()
        ),
    ];
    lines.extend(
        table
            .cursor_note
            .iter()
            .map(|note| format!("cursor  {note}")),
    );
    lines
}

pub(crate) fn blocked_lines(table: &TablePlan) -> Vec<String> {
    let Some(blocked) = &table.schema.blocked else {
        return Vec::new();
    };
    let mut lines = vec![row(&table.spec.destination.table, "cannot load")];
    lines.extend(blocked.reasons.iter().cloned());
    lines.extend(
        blocked
            .fix_flag
            .iter()
            .map(|flag| format!("fix  sqlload load {} {flag}", table.spec.destination.table)),
    );
    lines
}

pub(crate) fn warning_lines(table: &TablePlan) -> Vec<Vec<String>> {
    table
        .schema
        .warnings
        .iter()
        .map(|warning| vec![format!("{}  {warning}", table.spec.destination.table)])
        .collect()
}
