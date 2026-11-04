use crate::files_destination::models::{PendingLoad, PendingPhase};
use crate::loader::constants::PLAN_DETAIL_COLUMN;
use crate::loader::models::{FilesOutput, TablePlan};
use crate::project::models::FilesFormat;
use crate::routing::models::Route;
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
            .columns
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
    let source = match &spec.files {
        Some(files) => format!(
            "source  files  {}  {}  {}",
            spec.source_name, files.location, files.encoding
        ),
        None => format!(
            "source  postgres  {}  {}",
            spec.source_name,
            spec.source_table.qualified_name()
        ),
    };
    let mut lines = vec![
        row(&spec.destination.table, &format!("{strategy}{origin}")),
        source,
    ];
    lines.extend(
        spec.files
            .as_ref()
            .zip(table.files.as_ref())
            .map(|(source, read)| {
                let skipped = match source.table.skipped {
                    0 => String::new(),
                    count => format!(
                        ", {count} skipped (names that do not fit {})",
                        source.encoding
                    ),
                };
                format!(
                    "files   {} to read of {} listed{skipped}",
                    read.files.len(),
                    source.table.files.len()
                )
            }),
    );
    lines.extend(table.output.as_ref().map(files_line));
    lines.extend(table.output.iter().map(|output| {
        format!(
            "footers {} read from the newest partition ({} files listed, max_footer_reads {})",
            output.footers_read, output.listed, output.max_footer_reads
        )
    }));
    if table.output.is_some() {
        lines.extend(table.schema.action.summary().into_iter().map(
            |change| match change.as_str() {
                "create" => format!(
                    "schema  first files, {} columns",
                    table.schema.write_columns.len()
                ),
                "recreate" => {
                    "schema  every file rewritten with the current columns (--reload)".to_owned()
                }
                "unchanged" => "schema  as the newest files".to_owned(),
                _ => format!("schema  new files {change}"),
            },
        ));
    }
    lines.extend(table.route.as_ref().map(route_line));
    lines.extend(
        table
            .cursor_note
            .iter()
            .map(|note| format!("cursor  {note}")),
    );
    lines.extend(
        table
            .output
            .iter()
            .flat_map(|output| output.pending.iter())
            .map(pending_line),
    );
    lines.extend(
        table
            .output
            .iter()
            .flat_map(|output| output.notes.iter())
            .map(|note| format!("note    {note}")),
    );
    lines
}

fn pending_line(load: &PendingLoad) -> String {
    let (stopped, repair) = match load.phase {
        PendingPhase::Writing => ("while writing", "deletes its"),
        PendingPhase::Replacing => ("while deleting the files it replaced", "finishes deleting"),
    };
    format!(
        "cleanup load {} stopped {stopped}; load {repair} {} files first",
        load.load_id,
        load.deletes.len()
    )
}

fn files_line(output: &FilesOutput) -> String {
    let partition = match &output.path.partition {
        Some(partition) => format!("  partitions {partition}"),
        None => String::new(),
    };
    let format = match output.format {
        FilesFormat::Parquet => "parquet (zstd)",
    };
    format!("files   {}  {format}{partition}", output.path.folder)
}

fn route_line(route: &Route) -> String {
    let steps: Vec<String> = route.steps.iter().map(ToString::to_string).collect();
    format!("route  {}", steps.join(" -> "))
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
