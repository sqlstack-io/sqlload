use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::files::models::{PendingLoad, PendingPhase};
use crate::adapters::snowflake::models::PlannedStage;
use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::column_types::models::ColumnType;
use crate::files_source::models::SourceFormat;
use crate::loader::_helpers::sources::source_readers;
use crate::loader::_helpers::table_plan::is_direct;
use crate::loader::constants::{PARALLEL_READERS, PLAN_DETAIL_COLUMN};
use crate::loader::models::{
    CommitRule, FilesOutput, PythonTable, SourceEnd, TablePlan, WholeReason,
};
use crate::project::models::BatchStep;
use crate::project::models::FilesFormat;
use crate::project::models::MaskMethod;
use crate::routing::models::Route;
use crate::run_log::main::describe_state::describe_state;
use crate::schema_plan::models::{ColumnOrigin, TableAction};

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
    let name = sql_adapter(table.spec.warehouse).render_display_name(&table.spec.destination);
    let mut lines = vec![match &table.schema.action {
        TableAction::Recreate => row(
            &name,
            &format!(
                "recreate table ({} columns, --reload)",
                table.schema.write_columns.len()
            ),
        ),
        TableAction::Create if table.spec.python.is_some() => row(&name, &python_create(table)),
        TableAction::Create => row(
            &name,
            &format!(
                "create table ({} columns, from source catalog)",
                table.schema.write_columns.len()
            ),
        ),
        _ if table.spec.python.is_some() => row(&name, "alter table at load"),
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

pub(crate) fn stage_object_lines(stage: &PlannedStage) -> Vec<String> {
    vec![row(&stage.name, &format!("create stage on {}", stage.url))]
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
    let source = match (&spec.files, &spec.python) {
        (Some(files), _) => format!(
            "source  files  {}  {}  {}",
            spec.source_name, files.location, files.encoding
        ),
        (None, Some(python)) => format!("source  python  {} ({})", python.module, spec.source_name),
        (None, None) => format!(
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
        spec.python
            .as_ref()
            .map(|python| python_columns_line(table, python)),
    );
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
    lines.extend(
        spec.files
            .iter()
            .flat_map(|source| source.notes.iter())
            .map(|note| format!("note    {note}")),
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
    if reload_moves_columns(table) {
        lines.push("schema  changed columns move to the end of the column order".to_owned());
    }
    lines.extend(table.route.as_ref().map(route_line));
    if let Some(reason) = table
        .route
        .as_ref()
        .filter(|_| table.stage.is_some() && spec.files.is_some())
        .map(|route| route.reason.clone())
    {
        lines.push(format!("why     {reason}"));
    }
    if table.route.as_ref().is_some_and(|route| {
        is_direct(Some(route))
            && spec
                .masked
                .columns
                .values()
                .any(|method| *method == MaskMethod::Hash)
    }) {
        lines.push(
            "note    hashed columns reach Snowflake in plain text in a temporary table, gone with its session; mask = \"hmac\" keeps them out, through SQLLoad (much slower)"
                .to_owned(),
        );
    }
    lines.push(batch_line(table));
    if matches!(table.commits, CommitRule::Windows(_))
        && spec.python.is_none()
        && spec.files.is_none()
    {
        lines.push(
            "note    the windows are read in one REPEATABLE READ snapshot, held open on the source for the whole table load (mind idle_in_transaction_session_timeout and vacuum)"
                .to_owned(),
        );
    }
    if source_readers(table) > 1 {
        lines.push(format!(
            "note    read over up to {PARALLEL_READERS} connections in one snapshot, each a range of the table's pages, when it is a plain table of 128 MiB or more on Postgres 14+"
        ));
    }
    lines.extend(
        table
            .cursor_note
            .iter()
            .map(|note| format!("cursor  {note}")),
    );
    lines.extend(table.logged.as_ref().map(|logged| {
        format!(
            "state   logged: {}",
            describe_state(logged.current.as_ref())
        )
    }));
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

/// How the load commits: by cursor window, in cursor order, by size, by files batch, or as one commit; with a hint to set `batch` for a large first load that would be one commit.
fn batch_line(table: &TablePlan) -> String {
    let spec = &table.spec;
    let at_end = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let described = match (&table.commits, at_end) {
        (CommitRule::Windows(windows), _) => {
            let step = match spec.batch {
                Some(BatchStep::Duration(step)) => humantime::format_duration(step).to_string(),
                Some(BatchStep::Integer(step)) => step.to_string(),
                Some(BatchStep::Whole) | None => String::new(),
            };
            let end = match &spec.source_end {
                Some(SourceEnd::Maximum { maximum, .. }) => {
                    format!(", up to the source's maximum {maximum} (read at plan)")
                }
                Some(SourceEnd::Empty) => ", none: the source has no cursor values".to_owned(),
                None => String::new(),
            };
            format!(
                "{} cursor windows of {step}, read and committed one at a time{end}",
                windows.count
            )
        }
        (CommitRule::FileGroups(groups), _) => {
            let parquet = spec
                .files
                .as_ref()
                .is_some_and(|files| files.encoding.format == SourceFormat::Parquet);
            let drift = match parquet {
                true => "; each batch's footers are combined, so later batches may add columns",
                false => "",
            };
            format!(
                "{} batches of whole partitions (up to {MAX_BATCH_FILES} files or about 1 GB each){drift}",
                groups.len()
            )
        }
        (_, true) if sends_rows(table.stage.as_ref()) => {
            "about 1 GB at a time into a staging table, copied and swapped in once at the end"
                .to_owned()
        }
        (_, true) => "about 1 GB at a time into a new table, swapped in once at the end".to_owned(),
        (CommitRule::Ordered { column, .. }, false) => format!(
            "read in {column} order (an index leads with it, NULLs first), committing about every 1 GB between distinct values{}",
            match table.mode {
                WriteMode::DeleteInsert { .. } => ", each commit replacing its own cursor range",
                _ => "",
            }
        ),
        (CommitRule::Size, false) => "committing about every 1 GB".to_owned(),
        (CommitRule::Whole(reason), false) => {
            let estimated = u64::try_from(spec.source_table.estimated_bytes).unwrap_or_default();
            let large = !table.exists && (spec.python.is_some() || estimated > BATCH_BYTES as u64);
            let why = match reason {
                WholeReason::Chosen => "batch = \"whole\"",
                WholeReason::TextCursor => {
                    "a text cursor, which the source and destination may sort differently"
                }
                WholeReason::AppendWithoutCursor => {
                    "append without a cursor: a rerun after a partial commit would duplicate it"
                }
                WholeReason::Unordered | WholeReason::Blocked => {
                    "committing part of an unordered cursor read could skip rows"
                }
            };
            let hint = match (large, reason) {
                (true, WholeReason::Unordered) => {
                    "; for a large first load, set batch (such as batch = \"1d\") to commit it window by window"
                }
                _ => "",
            };
            format!("one commit for the whole read ({why}){hint}")
        }
    };
    let logged = match table.logged {
        Some(_) => ", and only when ctx.state has changed",
        None => "",
    };
    format!("batch   {described}{logged}")
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

/// Whether a Snowflake `--reload` re-adds any column, as its build table starts `LIKE` the table to keep the grants.
fn reload_moves_columns(table: &TablePlan) -> bool {
    let warehouse = table.spec.warehouse;
    let Some(live) = &table.live else {
        return false;
    };
    if table.mode != WriteMode::Recreate || !sql_adapter(warehouse).builds_like_target() {
        return false;
    }
    let sql = |column_type: &ColumnType| {
        sql_adapter(warehouse)
            .render_loader_logical_type(column_type)
            .sql
    };
    for column in &table.schema.write_columns {
        let name = sql_adapter(warehouse).stored_name(&column.name);
        let known = live.iter().find(|known| known.name == name);
        if known.is_none_or(|known| sql(&known.column_type) != sql(&column.column_type)) {
            return true;
        }
    }
    false
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

/// A Python table's object line before its first load.
fn python_create(table: &TablePlan) -> String {
    let declared = table
        .schema
        .write_columns
        .iter()
        .filter(|column| column.origin != ColumnOrigin::Metadata)
        .count();
    match declared {
        0 => "create table on first load (columns inferred from the data)".to_owned(),
        count => format!(
            "create table on first load ({count} declared columns, the rest inferred from the data)"
        ),
    }
}

/// Where a Python table's columns come from.
fn python_columns_line(table: &TablePlan, python: &PythonTable) -> String {
    let declared = match python.declared.len() {
        0 => String::new(),
        count => format!(", {count} declared"),
    };
    match table.exists {
        true => format!("columns inferred at load and checked against the live table{declared}"),
        false => format!("columns inferred on first load{declared}"),
    }
}
