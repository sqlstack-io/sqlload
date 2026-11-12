use std::time::Duration;

use chrono::SecondsFormat;

use crate::adapters::contract::models::{ChangeApply, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::files::models::{PendingLoad, PendingPhase};
use crate::adapters::snowflake::models::PlannedStage;
use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::column_types::models::ColumnType;
use crate::files_source::models::SourceFormat;
use crate::loader::_helpers::sources::source_readers;
use crate::loader::_helpers::table_plan::{is_direct, publishes_once, pulls_unloaded};
use crate::loader::constants::{
    CONSTANT_NOTE, CUT_ELEMENTS_NOTE, LABEL_WIDTH, PARALLEL_READERS, PLAN_DETAIL_COLUMN,
    RESULT_SCHEMA_VERSION,
};
use crate::loader::models::{
    CommitRule, FilesOutput, ObjectChange, PlanDetail, PlanItem, PlanView, ProjectPlan,
    PythonTable, RunReport, RunResult, RunSummary, SourceEnd, TableOutcome, TablePlan, TableResult,
    TallyLine, WarehouseRoute, WarehouseSource, WholeReason,
};
use crate::project::models::BatchStep;
use crate::project::models::MaskMethod;
use crate::project::models::{FilesFormat, OutputCompression};
use crate::routing::models::{Route, Step, Store};
use crate::run_log::main::describe_state::describe_state;
use crate::schema_plan::models::{ColumnChange, ColumnOrigin, DesiredColumn, TableAction};
use crate::store_paths::models::StoreKind;

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

/// `name` padded so `detail` starts in the plan's detail column (after a branch prefix), or two spaces after a name too long for it.
fn row(name: &str, detail: &str) -> String {
    format!("{name:<width$}  {detail}", width = PLAN_DETAIL_COLUMN - 6)
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
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
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

/// `errors` with items of exactly the same details merged into one that names each, so a reason shared by many tables is printed once.
pub(crate) fn merged_errors(errors: Vec<PlanItem>) -> Vec<PlanItem> {
    let mut merged: Vec<PlanItem> = Vec::new();
    for item in errors {
        match merged.iter_mut().find(|seen| seen.details == item.details) {
            Some(seen) => seen.name = format!("{}, {}", seen.name, item.name),
            None => merged.push(item),
        }
    }
    merged
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
        TableAction::Create | TableAction::Recreate
            if table.spec.python.is_none()
                || table.schema.write_columns.iter().any(|column| {
                    !matches!(
                        column.origin,
                        ColumnOrigin::Metadata | ColumnOrigin::Constant
                    )
                }) =>
        {
            details.extend(column_details(table));
        }
        TableAction::Alter(changes) => {
            details.extend(
                changes
                    .iter()
                    .map(|change| detail("", format!("{change}{}", change_note(table, change)))),
            );
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

/// What the type an added or widened column ends up with loses, as `  (...)`, or nothing, as a new table's column lines note it.
fn change_note(table: &TablePlan, change: &ColumnChange) -> String {
    let (column_type, cut) = match change {
        ColumnChange::Add { column, .. } => (&column.column_type, cut_elements(table, column)),
        ColumnChange::Widen { to, .. } | ColumnChange::Rebuild { to, .. } => (to, None),
    };
    let rendered = sql_adapter(table.spec.warehouse).render_loader_logical_type(column_type);
    let losses: Vec<&str> = [cut, rendered.lossy.as_deref()]
        .into_iter()
        .flatten()
        .collect();
    match losses.is_empty() {
        true => String::new(),
        false => format!("  ({})", losses.join("; ")),
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
        table.spec.files.as_ref().map_or("", |files| {
            match (
                files.constants.iter().any(|constant| constant == name),
                files
                    .partition_types
                    .iter()
                    .any(|(partition, _)| partition == name),
            ) {
                (true, _) => CONSTANT_NOTE,
                (false, true) => "  (from the path)",
                (false, false) => "",
            }
        })
    };
    let mut details: Vec<PlanDetail> = data
        .iter()
        .map(|column| {
            let rendered = adapter.render_loader_logical_type(&column.column_type);
            let losses: Vec<&str> = [cut_elements(table, column), rendered.lossy.as_deref()]
                .into_iter()
                .flatten()
                .collect();
            let note = match (from_path(&column.name), losses.is_empty()) {
                _ if column.origin == ColumnOrigin::Constant => CONSTANT_NOTE.to_owned(),
                (path, _) if !path.is_empty() => path.to_owned(),
                (_, false) => format!("  ({})", losses.join("; ")),
                (_, true) => String::new(),
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

/// The plan line for loading a table. A captured table whose destination does not exist yet is shown as copied first, as on its source's first load or when it joins a source whose slot already streams.
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
        WriteMode::Changes(change) => {
            let changes = match change.apply {
                ChangeApply::SoftDeletes => format!(
                    "apply changes, soft deletes (key: {})",
                    change.unique_key.join(", ")
                ),
                ChangeApply::HardDeletes => format!(
                    "apply changes, hard deletes (key: {})",
                    change.unique_key.join(", ")
                ),
                ChangeApply::Changelog => "append changes (changelog)".to_owned(),
            };
            match table.exists {
                true => changes,
                false => format!("snapshot copy, then {changes}"),
            }
        }
    };
    let source = match (&spec.files, &spec.python) {
        (Some(files), _) => format!("files  {}  {}", files.location, files.encoding),
        (None, Some(python)) => format!("python  {} ({})", python.module, spec.source_name),
        (None, None) if spec.warehouse_source.is_some() => {
            let read = spec
                .warehouse_source
                .as_ref()
                .map_or("", |source| source.system);
            let what = match spec
                .warehouse_source
                .as_ref()
                .and_then(|source| source.sql.as_ref())
            {
                Some(_) => format!("query {}", spec.source_table.name),
                None => spec.source_table.qualified_name(),
            };
            format!("{read}  {}  {what}", spec.source_name)
        }
        (None, None) if spec.sheet.is_some() => {
            let range = spec
                .sheet
                .as_ref()
                .and_then(|sheet| sheet.range.clone())
                .unwrap_or_else(|| format!("tab {}", spec.source_table.name));
            format!(
                "google_sheets  {}  {range}  {} rows",
                spec.source_name, spec.source_table.estimated_rows
            )
        }
        (None, None) => match &spec.cdc {
            Some(cdc) => format!(
                "postgres_cdc  {}  {}  slot {}",
                spec.source_name,
                spec.source_table.qualified_name(),
                cdc.slot
            ),
            None => format!(
                "postgres  {}  {}",
                spec.source_name,
                spec.source_table.qualified_name()
            ),
        },
    };
    let source = match &spec.tunnel {
        Some(route) => format!("{source}  via {route}"),
        None => source,
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
                let unmatched = match source.table.unmatched {
                    0 => String::new(),
                    count => format!(", {count} skipped (paths that do not match path_columns)"),
                };
                let fresh = match table.mode {
                    WriteMode::Recreate | WriteMode::Replace => "to read",
                    _ => "new",
                };
                detail(
                    "files",
                    format!(
                        "{} {fresh} of {} listed{skipped}{unmatched}",
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
    if let Some(WarehouseSource {
        route: WarehouseRoute::Unload { stage, .. },
        ..
    }) = &spec.warehouse_source
    {
        details.push(detail("stage", stage.as_str()));
    }
    if let Some(reason) = table
        .route
        .as_ref()
        .filter(|_| {
            (table.stage.is_some() && spec.files.is_some() && !is_direct(table.route.as_ref()))
                || spec.warehouse_source.is_some()
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
        && spec.warehouse_source.is_none()
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
            let commits = match publishes_once(table) {
                true => "staged one after another and published in one commit",
                false => "one commit each",
            };
            format!("{windows} of {step}, {commits}{end}")
        }
        (CommitRule::FileGroups(groups), _) if matches!(table.mode, WriteMode::Merge { .. }) => {
            format!(
                "{}, one commit each, oldest modified first (ties by path), each recorded in the run log",
                match groups.len() {
                    1 => "1 file".to_owned(),
                    count => format!("{count} files"),
                }
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
                "{} of whole partitions, up to {} files or about 1 GB per commit{drift}",
                match groups.len() {
                    1 => "1 batch".to_owned(),
                    count => format!("{count} batches"),
                },
                thousands(MAX_BATCH_FILES as u64)
            )
        }
        (_, true) if pulls_unloaded(table.route.as_ref()) => {
            "each unload loaded at once into a new table, swapped in once at the end".to_owned()
        }
        (_, true) if sql_adapter(spec.warehouse).keeps_tables() => {
            "about 1 GB at a time into a staging table, then the table emptied and refilled in one transaction".to_owned()
        }
        (_, true) if sends_rows(table.stage.as_ref()) => {
            "about 1 GB at a time into a staging table, copied and swapped in once at the end"
                .to_owned()
        }
        (_, true) => "about 1 GB at a time into a new table, swapped in once at the end".to_owned(),
        (CommitRule::Ordered { column, .. }, false) if publishes_once(table) => format!(
            "in {column} order (an index leads with it, NULLs first), staged about 1 GB at a time and published in one commit replacing the whole range"
        ),
        (CommitRule::Ordered { column, .. }, false) => format!(
            "about every 1 GB, in {column} order (an index leads with it, NULLs first), between distinct values{}",
            match table.mode {
                WriteMode::DeleteInsert { .. } => "; each commit replaces its own cursor range",
                _ => "",
            }
        ),
        (CommitRule::Size, false) if pulls_unloaded(table.route.as_ref()) => {
            "one commit: the destination loads the whole unload at once".to_owned()
        }
        (CommitRule::Size, false) => "about every 1 GB".to_owned(),
        (CommitRule::Whole(reason), false) => {
            let estimated = u64::try_from(spec.source_table.estimated_bytes).unwrap_or_default();
            let large = !table.exists && (spec.python.is_some() || estimated > BATCH_BYTES as u64);
            let why = match reason {
                WholeReason::Chosen => "batch_size = \"none\"",
                WholeReason::SingleFile => "single_file writes each file once, whole",
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
            let once = matches!(table.mode, WriteMode::DeleteInsert { .. })
                && sql_adapter(spec.warehouse).publishes_ranges_once();
            let hint = match (large, reason, once) {
                (true, WholeReason::Unordered, false) => {
                    "; for a large first load, set batch_size (such as batch_size = \"1d\") to commit it window by window"
                }
                (true, WholeReason::Unordered, true) => {
                    "; for a large first load, set batch_size (such as batch_size = \"1d\") to read it window by window, or split it into runs with --end-cursor"
                }
                _ => "",
            };
            format!("one commit for the whole read ({why}){hint}")
        }
    };
    let logged = match (&table.logged, publishes_once(table)) {
        (Some(_), false) => ", and only when ctx.state has changed",
        _ => "",
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
    let encoding = &output.encoding;
    let mut format = match (encoding.format, encoding.compression) {
        (FilesFormat::Parquet, _) => "parquet (zstd)".to_owned(),
        (FilesFormat::Csv, OutputCompression::None) => "csv".to_owned(),
        (FilesFormat::Csv, OutputCompression::Gzip) => "csv (gzip)".to_owned(),
        (FilesFormat::Csv, OutputCompression::Zstd) => "csv (zstd)".to_owned(),
    };
    if encoding.single_file {
        format.push_str(", single file");
    }
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

/// How a table's data moves, in short: the warehouse's own read of the files where it reads them, otherwise each step.
fn route_text(table: &TablePlan, route: &Route) -> String {
    let warehouse = table.spec.warehouse;
    let (verb, load) = sql_adapter(warehouse).render_read_words();
    let real = table
        .spec
        .files
        .as_ref()
        .map(|files| files.encoding.format.to_string());
    let over_sftp = table
        .spec
        .files
        .as_ref()
        .is_some_and(|files| matches!(files.location.store, StoreKind::Sftp { .. }));
    match route.steps.as_slice() {
        [Step::UseFiles(files), Step::DestinationPulls(_)] => {
            format!(
                "{warehouse} {verb} directly from {} (nothing through the loader)",
                files.store
            )
        }
        [
            Step::UseFiles(files),
            Step::CopyFiles(Store::InternalStage(stage)),
            Step::DestinationPulls(_),
        ] => format!(
            "{} files from {} uploaded unchanged to {stage} -> {load}",
            real.unwrap_or_else(|| files.format.to_string()),
            files.store
        ),
        [Step::Unload(store), Step::DestinationPulls(_)] => {
            format!("unload Parquet to {store} -> {warehouse} {load} (nothing through the loader)")
        }
        [
            Step::Unload(store),
            Step::CopyFiles(Store::Local(folder)),
            Step::DestinationPulls(_),
        ] => format!(
            "unload Parquet to {store} -> download unchanged to {folder} -> {warehouse} {load} (nothing through the loader)"
        ),
        steps => over_sftp
            .then(|| "read over SFTP".to_owned())
            .into_iter()
            .chain(
                steps
                    .iter()
                    .filter(|step| !matches!(step, Step::UseFiles(_)))
                    .map(|step| match step {
                        Step::ReadArrow => "read as Arrow".to_owned(),
                        Step::ParquetWriter(Store::InternalStage(stage)) => {
                            format!("Parquet -> PUT to {stage}")
                        }
                        Step::ParquetWriter(store) => format!("Parquet -> {store}"),
                        Step::DestinationPulls(_) => load.to_owned(),
                        Step::DestinationTakesArrow => format!("{warehouse} takes Arrow"),
                        other => other.to_string(),
                    }),
            )
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

/// The loss of a column pinned as an integer array when the source reads lists as Arrow (a Python source or Parquet files): float elements are cut to whole numbers, as Arrow casts them.
fn cut_elements(table: &TablePlan, column: &DesiredColumn) -> Option<&'static str> {
    let arrow_lists = table.spec.python.is_some()
        || table
            .spec
            .files
            .as_ref()
            .is_some_and(|files| files.encoding.format == SourceFormat::Parquet);
    let integer_array = matches!(
        &column.column_type,
        ColumnType::Array(element) if matches!(**element, ColumnType::Int | ColumnType::SmallInt)
    );
    (arrow_lists && integer_array && column.origin == ColumnOrigin::Pinned)
        .then_some(CUT_ELEMENTS_NOTE)
}

/// A Python table's object line before its first load.
fn python_create(table: &TablePlan) -> String {
    let declared = table
        .schema
        .write_columns
        .iter()
        .filter(|column| {
            !matches!(
                column.origin,
                ColumnOrigin::Metadata | ColumnOrigin::Constant
            )
        })
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
    let flattened = match table.spec.flatten.is_empty() {
        true => String::new(),
        false => format!(", {} flattened one level", table.spec.flatten.join(", ")),
    };
    match table.exists {
        true => format!(
            "columns inferred at load and checked against the live table{declared}{flattened}"
        ),
        false => format!("columns inferred on first load{declared}{flattened}"),
    }
}

/// `count` and the noun for it, as `1 object` or `2 objects`.
fn counted(count: usize, (one, many): (&str, &str)) -> String {
    match count {
        1 => format!("1 {one}"),
        _ => format!("{count} {many}"),
    }
}

/// The plan's sections: what `apply` creates or changes, what `load` moves, and what stops or should be known; one `Errors` item per table or problem.
pub(crate) fn plan_view(plan: &ProjectPlan) -> PlanView {
    let ready: Vec<_> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .collect();
    let mut stages: Vec<&PlannedStage> = Vec::new();
    for table in &ready {
        if let Some(stage) = &table.planned_stage
            && !stages.iter().any(|seen| seen.name == stage.name)
        {
            stages.push(stage);
        }
    }
    let changed: Vec<_> = ready
        .iter()
        .filter(|table| table.schema.action != TableAction::Unchanged && table.output.is_none())
        .collect();
    let creates = stages.len()
        + changed
            .iter()
            .filter(|table| {
                matches!(
                    table.schema.action,
                    TableAction::Create | TableAction::Recreate
                )
            })
            .count();
    let alters = stages.len() + changed.len() - creates;
    let errors: Vec<PlanItem> = plan
        .tables
        .iter()
        .filter_map(blocked_item)
        .chain(plan.problems.iter().map(|problem| problem_item(problem)))
        .collect();
    let clauses = [
        (creates, ("object to create", "objects to create")),
        (alters, ("to alter", "to alter")),
        (ready.len(), ("to load", "to load")),
        (plan.skipped.len(), ("skipped", "skipped")),
    ];
    let mut summary = vec![format!("{} selected", plan.tables.len())];
    summary.extend(
        clauses
            .into_iter()
            .filter(|(count, _)| *count > 0)
            .map(|(count, words)| counted(count, words)),
    );
    PlanView {
        ok: errors.is_empty(),
        retryable: !errors.is_empty() && plan.only_retryable_problems(),
        summary: summary.join(", "),
        selected: plan.tables.len(),
        objects: stages
            .iter()
            .map(|stage| stage_item(stage))
            .chain(changed.iter().map(|table| object_item(table)))
            .collect(),
        loads: ready.iter().map(|table| load_item(table)).collect(),
        skipped: plan
            .skipped
            .iter()
            .map(|skipped| PlanItem {
                name: skipped.source_table.clone(),
                action: String::new(),
                details: vec![PlanDetail {
                    label: "reason".to_owned(),
                    text: skipped.reason.clone(),
                }],
            })
            .collect(),
        errors,
        warnings: plan
            .warnings
            .iter()
            .map(|warning| error_item(warning, &[]))
            .chain(ready.iter().filter_map(|table| warning_item(table)))
            .collect(),
    }
}

/// The lines `plan` prints for `view`: `Plan ready`, each section with items, then whether the plan is complete.
pub(crate) fn view_lines(view: &PlanView) -> Vec<String> {
    let mut lines = match view.selected {
        0 if !view.errors.is_empty() => Vec::new(),
        _ => vec![format!("Plan ready  {}", view.summary), String::new()],
    };
    lines.extend(tree("Objects", "sqlload apply", &view.objects));
    lines.extend(tree("Loads", "sqlload load", &view.loads));
    lines.extend(tree("Skipped", "", &view.skipped));
    let summary = match lines.is_empty() {
        true => String::new(),
        false => format!("  {}", view.summary),
    };
    lines.extend(tree("Errors", "", &view.errors));
    lines.extend(tree("Warnings", "", &view.warnings));
    lines.push(match view.ok {
        true => format!("✓ Plan complete{summary}"),
        false => format!("✗ Plan failed with errors{summary}"),
    });
    lines
}

/// `created  name  detail`, as `apply` and a Python table's load report a change, as a tally line.
pub(crate) fn change_line(line: &str) -> TallyLine {
    let mut parts = line.splitn(3, "  ");
    let verb = match parts.next().unwrap_or_default() {
        "created" => "created",
        "recreated" => "recreated",
        _ => "altered",
    };
    TallyLine {
        verb,
        name: parts.next().unwrap_or_default().to_owned(),
        detail: parts.next().unwrap_or_default().to_owned(),
    }
}

/// What `run` did after `plan`: the plan, each change it made, each table's outcome, and counts (a table altered by several changes counts once).
pub(crate) fn run_result(plan: &ProjectPlan, run: &RunReport<'_>) -> RunResult {
    let RunReport {
        command,
        applied,
        outcomes,
        started,
        elapsed,
    } = *run;
    let changes = outcomes
        .iter()
        .flat_map(|outcome| outcome.loaded.iter())
        .flat_map(|loaded| &loaded.changes);
    let objects: Vec<ObjectChange> = applied
        .iter()
        .chain(changes)
        .map(|line| {
            let change = change_line(line);
            ObjectChange {
                action: change.verb,
                name: change.name,
                detail: change.detail,
            }
        })
        .collect();
    let tables: Vec<TableResult> = outcomes.iter().map(table_result).collect();
    let actions = |action: &str| objects.iter().filter(|o| o.action == action).count();
    let distinct = |action: &str| {
        objects
            .iter()
            .filter(|object| object.action == action)
            .map(|object| object.name.as_str())
            .collect::<std::collections::BTreeSet<&str>>()
            .len()
    };
    let statuses = |status: &str| tables.iter().filter(|t| t.status == status).count();
    let summary = RunSummary {
        created: actions("created"),
        altered: distinct("altered"),
        recreated: actions("recreated"),
        loaded: statuses("loaded"),
        failed: statuses("failed"),
        rows: tables.iter().filter_map(|table| table.rows).sum(),
    };
    let view = plan_view(plan);
    let finished = started + chrono::Duration::from_std(elapsed).unwrap_or_default();
    let ok = view.ok && summary.failed == 0;
    RunResult {
        schema_version: RESULT_SCHEMA_VERSION,
        command,
        ok,
        retryable: !ok
            && (view.ok || view.retryable)
            && outcomes.iter().all(TableOutcome::passes_on_rerun),
        started_at: started.to_rfc3339_opts(SecondsFormat::Millis, true),
        finished_at: finished.to_rfc3339_opts(SecondsFormat::Millis, true),
        duration_seconds: seconds(elapsed),
        summary,
        plan: view,
        objects,
        tables,
    }
}

/// `outcome` as `--json` reports it.
fn table_result(outcome: &TableOutcome) -> TableResult {
    let (status, rows, note, error) = match &outcome.loaded {
        Ok(loaded) => ("loaded", Some(loaded.rows), loaded.note.clone(), None),
        Err(reason) => ("failed", None, None, Some(reason.clone())),
    };
    TableResult {
        table: outcome.table.clone(),
        source: outcome.origin.as_ref().map(|origin| origin.source.clone()),
        target: outcome.origin.as_ref().map(|origin| origin.target.clone()),
        write_strategy: outcome
            .origin
            .as_ref()
            .map(|origin| origin.strategy.to_string()),
        status,
        rows,
        duration_seconds: seconds(outcome.elapsed),
        note,
        error,
        retryable: outcome.retryable,
        after_load: match &outcome.loaded {
            Ok(loaded) => loaded.after_load.clone(),
            Err(_) => None,
        },
    }
}

/// `elapsed` in seconds, to the millisecond.
fn seconds(elapsed: Duration) -> f64 {
    (elapsed.as_secs_f64() * 1000.0).round() / 1000.0
}
