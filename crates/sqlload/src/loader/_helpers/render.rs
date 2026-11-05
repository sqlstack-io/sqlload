use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::files::models::{PendingLoad, PendingPhase};
use crate::adapters::snowflake::models::PlannedStage;
use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::models::SourceFormat;
use crate::loader::_helpers::sources::source_readers;
use crate::loader::_helpers::table_plan::is_direct;
use crate::loader::constants::{LABEL_WIDTH, PARALLEL_READERS, PLAN_DETAIL_COLUMN};
use crate::loader::models::{
    CommitRule, FilesOutput, PlanDetail, PlanItem, PythonTable, SourceEnd, TablePlan, WholeReason,
};
use crate::project::models::BatchStep;
use crate::project::models::FilesFormat;
use crate::project::models::MaskMethod;
use crate::routing::models::{Route, Step, Store};
use crate::run_log::main::describe_state::describe_state;
use crate::schema_plan::models::{ColumnOrigin, TableAction};

/// A section: a heading with an optional right-aligned note, then each item as a tree branch; nothing when it has no items.
pub(crate) fn tree(heading: &str, note: &str, items: &[PlanItem]) -> Vec<String> {
    if items.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![aligned(&format!("{heading} ({})", items.len()), note)];
    for (index, item) in items.iter().enumerate() {
        let last = index + 1 == items.len();
        let (branch, indent) = match last {
            true => ("└── ", "    "),
            false => ("├── ", "│   "),
        };
        lines.push(match item.action.is_empty() {
            true => format!("{branch}{}", item.name),
            false => format!("{branch}{}", row(&item.name, &item.action)),
        });
        for (detail_index, detail) in item.details.iter().enumerate() {
            let (sub, continued) = match detail_index + 1 == item.details.len() {
                true => ("└── ", "    "),
                false => ("├── ", "│   "),
            };
            let label = match detail.label.is_empty() {
                true => String::new(),
                false => format!("{:<width$} ", detail.label, width = LABEL_WIDTH - 1),
            };
            for (line_index, text) in detail.text.split('\n').enumerate() {
                lines.push(match line_index {
                    0 => format!("{indent}{sub}{label}{text}"),
                    _ => format!(
                        "{indent}{continued}{:width$}{text}",
                        "",
                        width = label.len()
                    ),
                });
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

fn detail(label: &str, text: impl Into<String>) -> PlanDetail {
    PlanDetail {
        label: label.to_owned(),
        text: text.into(),
    }
}

/// `count` with thousands separators, as `30,000`.
pub(crate) fn thousands(count: u64) -> String {
    let digits = count.to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// A finding as an `Errors` item: the reason, and the fix where the text gives one after `Fix:`.
pub(crate) fn error_item(name: &str, reasons: &[String]) -> PlanItem {
    let mut details: Vec<PlanDetail> = Vec::new();
    for reason in reasons {
        match reason.split_once(" Fix: ") {
            Some((why, fix)) => {
                details.push(detail("reason", why.trim_end_matches('.')));
                details.push(detail("fix", fix));
            }
            None => details.push(detail("reason", reason.as_str())),
        }
    }
    PlanItem {
        name: name.to_owned(),
        action: String::new(),
        details,
    }
}

/// A project problem, `subject: reason`, as an `Errors` item.
pub(crate) fn problem_item(problem: &str) -> PlanItem {
    match problem.split_once(": ") {
        Some((subject, reason)) if !subject.contains(' ') || subject.starts_with("connection ") => {
            error_item(subject, &[reason.to_owned()])
        }
        _ => error_item("project", &[problem.to_owned()]),
    }
}

pub(crate) fn object_item(table: &TablePlan) -> PlanItem {
    let name = sql_adapter(table.spec.warehouse).render_display_name(&table.spec.destination);
    let action = match &table.schema.action {
        TableAction::Recreate => "recreate table (--reload)".to_owned(),
        TableAction::Create if table.spec.python.is_some() => python_create(table),
        TableAction::Create => "create table".to_owned(),
        _ if table.spec.python.is_some() => "alter table at load".to_owned(),
        _ => "alter table".to_owned(),
    };
    let mut details: Vec<PlanDetail> = Vec::new();
    match &table.schema.action {
        TableAction::Create | TableAction::Recreate if table.spec.python.is_none() => {
            details.extend(column_details(table));
        }
        TableAction::Alter(changes) => {
            details.extend(changes.iter().map(|change| detail("", change.to_string())));
        }
        _ => {}
    }
    details.extend(
        table
            .spec
            .excluded_columns
            .iter()
            .map(|(column, rule)| detail("", format!("excluded  {column} ({rule})"))),
    );
    details.extend(
        table
            .spec
            .masked
            .columns
            .iter()
            .map(|(column, method)| detail("", format!("masked  {column} ({method})"))),
    );
    PlanItem {
        name,
        action,
        details,
    }
}

/// A new table's columns: name, portable type and the warehouse's type, then the `_sqlload_*` columns on one line.
fn column_details(table: &TablePlan) -> Vec<PlanDetail> {
    let adapter = sql_adapter(table.spec.warehouse);
    let (metadata, data): (Vec<_>, Vec<_>) = table
        .schema
        .write_columns
        .iter()
        .partition(|column| column.name.starts_with("_sqlload_"));
    let name_width = data
        .iter()
        .map(|column| column.name.len())
        .max()
        .unwrap_or(0)
        + 1;
    let type_width = data
        .iter()
        .map(|column| column.column_type.to_string().len())
        .max()
        .unwrap_or(0)
        + 3;
    let from_path = |name: &str| {
        table.spec.files.as_ref().is_some_and(|files| {
            files
                .partition_types
                .iter()
                .any(|(partition, _)| partition == name)
        })
    };
    let mut details: Vec<PlanDetail> = data
        .iter()
        .map(|column| {
            let rendered = adapter.render_loader_logical_type(&column.column_type);
            let note = match (
                column.column_type == ColumnType::Uuid,
                from_path(&column.name),
            ) {
                (_, true) => "  (from the path)",
                (true, false) if rendered.sql.starts_with("VARCHAR") => "  (stored as text)",
                _ => "",
            };
            detail(
                "",
                format!(
                    "{:<name_width$}{:<type_width$}-> {}{note}",
                    column.name,
                    column.column_type.to_string(),
                    rendered.sql
                ),
            )
        })
        .collect();
    let names: Vec<String> = metadata
        .iter()
        .enumerate()
        .map(|(index, column)| match index {
            0 => column.name.clone(),
            _ => column
                .name
                .strip_prefix("_sqlload")
                .unwrap_or(&column.name)
                .to_owned(),
        })
        .collect();
    if !names.is_empty() {
        details.push(detail("", format!("+ {}", names.join(", "))));
    }
    details
}

pub(crate) fn stage_item(stage: &PlannedStage) -> PlanItem {
    PlanItem {
        name: stage.name.clone(),
        action: format!("create stage on {}", stage.url),
        details: Vec::new(),
    }
}

pub(crate) fn load_item(table: &TablePlan) -> PlanItem {
    let spec = &table.spec;
    let action = match &table.mode {
        WriteMode::Merge { unique_key } => format!("merge (unique_key: {})", unique_key.join(", ")),
        WriteMode::DeleteInsert { cursor_column, .. } => {
            format!("delete_insert (cursor: {cursor_column})")
        }
        WriteMode::Append => match &spec.cursor {
            Some(cursor) => format!("append (cursor: {})", cursor.column),
            None => "append".to_owned(),
        },
        WriteMode::Replace => "replace".to_owned(),
        WriteMode::Recreate => "recreate from the whole source (--reload)".to_owned(),
    };
    let source = match (&spec.files, &spec.python) {
        (Some(files), _) => format!("files  {}  {}", files.location, files.encoding),
        (None, Some(python)) => format!("python  {} ({})", python.module, spec.source_name),
        (None, None) => format!(
            "postgres  {}  {}",
            spec.source_name,
            spec.source_table.qualified_name()
        ),
    };
    let mut details = vec![detail("source", source)];
    details.extend(
        spec.python
            .as_ref()
            .map(|python| detail("schema", python_columns_line(table, python))),
    );
    details.extend(
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
                let fresh = match table.mode {
                    WriteMode::Recreate | WriteMode::Replace => "to read",
                    _ => "new",
                };
                detail(
                    "files",
                    format!(
                        "{} {fresh} of {} listed{skipped}",
                        thousands(read.files.len() as u64),
                        thousands(source.table.files.len() as u64)
                    ),
                )
            }),
    );
    details.extend(
        spec.files
            .iter()
            .flat_map(|source| source.notes.iter())
            .map(|note| detail("note", note.as_str())),
    );
    details.extend(table.output.as_ref().map(files_line));
    details.extend(
        table
            .output
            .iter()
            .filter(|output| output.footers_read > 0)
            .map(|output| {
                detail(
                    "footers",
                    format!(
                        "{} read from the newest partition ({} files listed, max_footer_reads {})",
                        output.footers_read, output.listed, output.max_footer_reads
                    ),
                )
            }),
    );
    if table.output.is_some() {
        details.extend(table.schema.action.summary().into_iter().map(|change| {
            detail(
                "schema",
                match change.as_str() {
                    "create" => {
                        format!("first files, {} columns", table.schema.write_columns.len())
                    }
                    "recreate" => {
                        "every file rewritten with the current columns (--reload)".to_owned()
                    }
                    "unchanged" => "as the newest files".to_owned(),
                    _ => format!("new files {change}"),
                },
            )
        }));
    }
    if reload_moves_columns(table) {
        details.push(detail(
            "schema",
            "changed columns move to the end of the column order",
        ));
    }
    details.extend(
        table
            .route
            .as_ref()
            .map(|route| detail("route", route_text(table, route))),
    );
    if let Some(reason) = table
        .route
        .as_ref()
        .filter(|_| {
            table.stage.is_some() && spec.files.is_some() && !is_direct(table.route.as_ref())
        })
        .map(|route| route.reason.clone())
    {
        details.push(detail("why", reason));
    }
    if table.route.as_ref().is_some_and(|route| {
        is_direct(Some(route))
            && spec
                .masked
                .columns
                .values()
                .any(|method| *method == MaskMethod::Hash)
    }) {
        details.push(detail(
            "note",
            "hashed columns reach Snowflake in plain text in a temporary table, gone with its session; mask = \"hmac\" keeps them out, through SQLLoad (much slower)",
        ));
    }
    details.push(detail("batch", batch_line(table)));
    if matches!(table.commits, CommitRule::Windows(_))
        && spec.python.is_none()
        && spec.files.is_none()
    {
        details.push(detail(
            "note",
            "windows are read in one REPEATABLE READ snapshot, held open on the source for the whole table load (mind idle_in_transaction_session_timeout and vacuum)",
        ));
    }
    if source_readers(table) > 1 {
        details.push(detail(
            "note",
            format!(
                "read over up to {PARALLEL_READERS} connections in one snapshot, each a range of the table's pages, when it is a plain table of 128 MiB or more on Postgres 14+"
            ),
        ));
    }
    details.extend(
        table
            .cursor_note
            .iter()
            .map(|note| detail("cursor", note.as_str())),
    );
    details.extend(table.logged.as_ref().map(|logged| {
        detail(
            "state",
            format!("logged: {}", describe_state(logged.current.as_ref())),
        )
    }));
    details.extend(
        table
            .output
            .iter()
            .flat_map(|output| output.pending.iter())
            .map(pending_line),
    );
    details.extend(
        table
            .output
            .iter()
            .flat_map(|output| output.notes.iter())
            .map(|note| detail("note", note.as_str())),
    );
    PlanItem {
        name: spec.destination.table.clone(),
        action,
        details,
    }
}

/// How the load commits: by cursor window, in cursor order, by size, by files batch, or as one commit; with a hint to set `batch_size` for a large first load that would be one commit.
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
            let windows = match windows.count {
                1 => "1 window".to_owned(),
                count => format!("{count} windows"),
            };
            format!("{windows} of {step}, one commit each{end}")
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
                "{} of whole partitions, up to {} files or about 1 GB per commit{drift}",
                match groups.len() {
                    1 => "1 batch".to_owned(),
                    count => format!("{count} batches"),
                },
                thousands(MAX_BATCH_FILES as u64)
            )
        }
        (_, true) if sends_rows(table.stage.as_ref()) => {
            "about 1 GB at a time into a staging table, copied and swapped in once at the end"
                .to_owned()
        }
        (_, true) => "about 1 GB at a time into a new table, swapped in once at the end".to_owned(),
        (CommitRule::Ordered { column, .. }, false) => format!(
            "about every 1 GB, in {column} order (an index leads with it, NULLs first), between distinct values{}",
            match table.mode {
                WriteMode::DeleteInsert { .. } => "; each commit replaces its own cursor range",
                _ => "",
            }
        ),
        (CommitRule::Size, false) => "about every 1 GB".to_owned(),
        (CommitRule::Whole(reason), false) => {
            let estimated = u64::try_from(spec.source_table.estimated_bytes).unwrap_or_default();
            let large = !table.exists && (spec.python.is_some() || estimated > BATCH_BYTES as u64);
            let why = match reason {
                WholeReason::Chosen => "batch_size = \"none\"",
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
                    "; for a large first load, set batch_size (such as batch_size = \"1d\") to commit it window by window"
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
    format!("{described}{logged}")
}

fn pending_line(load: &PendingLoad) -> PlanDetail {
    let (stopped, repair) = match load.phase {
        PendingPhase::Writing => ("while writing", "deletes its"),
        PendingPhase::Replacing => ("while deleting the files it replaced", "finishes deleting"),
    };
    detail(
        "cleanup",
        format!(
            "load {} stopped {stopped}; load {repair} {} files first",
            load.load_id,
            load.deletes.len()
        ),
    )
}

fn files_line(output: &FilesOutput) -> PlanDetail {
    let partition = match &output.path.partition {
        Some(partition) => format!("  partitions {partition}"),
        None => String::new(),
    };
    let format = match output.format {
        FilesFormat::Parquet => "parquet (zstd)",
    };
    detail(
        "files",
        format!("{}  {format}{partition}", output.path.folder),
    )
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

/// How a table's data moves, in short: the warehouse's own read of the files where it reads them, otherwise each step, naming files by their real format.
fn route_text(table: &TablePlan, route: &Route) -> String {
    let warehouse = table.spec.warehouse;
    let real = table.spec.files.as_ref().map(|files| files.encoding.format);
    let format = |given: &dyn std::fmt::Display| {
        real.map_or_else(|| given.to_string(), |format| format.to_string())
    };
    let reads = match warehouse {
        Warehouse::Snowflake => "COPY".to_owned(),
        _ => format!("{warehouse} reads them"),
    };
    match route.steps.as_slice() {
        [Step::UseFiles(files), Step::DestinationPulls(_)] => format!(
            "{} {} directly from {} (nothing through the loader)",
            warehouse,
            match warehouse {
                Warehouse::Snowflake => "COPYs",
                _ => "reads",
            },
            files.store
        ),
        [
            Step::UseFiles(files),
            Step::CopyFiles(Store::InternalStage(stage)),
            Step::DestinationPulls(_),
        ] => format!(
            "{} files from {} uploaded unchanged to {stage} -> {reads}",
            format(&files.format),
            files.store
        ),
        steps => steps
            .iter()
            .filter(|step| !matches!(step, Step::UseFiles(_)))
            .map(|step| match step {
                Step::ReadArrow => "read as Arrow".to_owned(),
                Step::FileReader(given) => format!("read {} as Arrow", format(given)),
                Step::ParquetWriter(Store::InternalStage(stage)) => {
                    format!("Parquet -> PUT to {stage}")
                }
                Step::ParquetWriter(store) => format!("Parquet -> {store}"),
                Step::DestinationPulls(_) => reads.clone(),
                Step::DestinationTakesArrow => format!("{warehouse} takes Arrow"),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(" -> "),
    }
}

/// A table that cannot load, as an `Errors` item, with the flag that unblocks it where one does.
pub(crate) fn blocked_item(table: &TablePlan) -> Option<PlanItem> {
    let blocked = table.schema.blocked.as_ref()?;
    let mut item = error_item(&table.spec.destination.table, &blocked.reasons);
    item.details.extend(blocked.fix_flag.iter().map(|flag| {
        detail(
            "fix",
            format!("sqlload load {} {flag}", table.spec.destination.table),
        )
    }));
    Some(item)
}

/// A table's warnings, as one `Warnings` item.
pub(crate) fn warning_item(table: &TablePlan) -> Option<PlanItem> {
    (!table.schema.warnings.is_empty()).then(|| PlanItem {
        name: table.spec.destination.table.clone(),
        action: String::new(),
        details: table
            .schema
            .warnings
            .iter()
            .map(|warning| detail("", warning.as_str()))
            .collect(),
    })
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
