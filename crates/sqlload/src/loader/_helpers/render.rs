use crate::load_naming::models::Naming;
use crate::loader::constants::{
    FIX_LABEL, HOURS_OFFSET_LENGTH, REASON_LABEL, ROUTE_LABEL, SECOND_TIMESTAMP_LENGTH,
    SOURCE_LABEL,
};
use crate::loader::models::UnreadSource;
use crate::project::models::CheckName;
use crate::run_log::models::RunEvent;
use std::collections::HashMap;
use std::time::Duration;

use chrono::SecondsFormat;

use crate::adapters::contract::models::{ChangeApply, MergeDedupe, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::files::models::{PendingLoad, PendingPhase};
use crate::adapters::snowflake::models::PlannedStage;
use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::column_types::models::ColumnType;
use crate::load_naming::main::is_load_column::is_load_column;
use crate::loader::_helpers::python::list_choices;
use crate::loader::_helpers::python::unnested_table;
use crate::loader::_helpers::resolve::display_name;
use crate::loader::_helpers::sources::source_readers;
use crate::loader::_helpers::table_plan::{is_direct, publishes_once, pulls_unloaded};
use crate::loader::constants::{
    CONSTANT_NOTE, DRAINED_REASON, LABEL_WIDTH, PARALLEL_READERS, PLAN_DETAIL_COLUMN,
    PRIMARY_KEY_PLACEHOLDER, RESULT_SCHEMA_VERSION,
};
use crate::loader::models::{
    CommitRule, FilesOutput, ObjectChange, PlanDetail, PlanItem, PlanView, ProjectPlan,
    PythonTable, RunReport, RunResult, RunSummary, SourceEnd, TableOutcome, TablePlan, TableResult,
    TallyLine, WarehouseRoute, WarehouseSource, WholeReason,
};
use crate::number_text::main::thousands::thousands;
use crate::project::models::BatchStep;
use crate::project::models::MaskMethod;
use crate::project::models::{FilesFormat, OutputCompression};
use crate::routing::models::{Route, Step, Store};
use crate::row_batches::models::Unnest;
use crate::run_log::main::describe_state::describe_state;
use crate::schema_plan::models::{ColumnChange, ColumnOrigin, TableAction};
use crate::store_paths::models::StoreKind;

/// How many renamed columns the one-table view's `columns` line names before `...`.
const SHOWN_RENAMES: usize = 2;

/// Details only a plan of one table shows: why its rows travel the way they do.
const ONE_TABLE_LABELS: [&str; 1] = ["why"];

/// Details the project view leaves out where a table has the text most of its tables have.
const SHARED_LABELS: [&str; 2] = ["batch", "schema"];

/// Details any table can have, which a plan of several tables leaves out even when no other table shares them: the batches of a table read whole.
const DEFAULT_DETAILS: [(&str, &str); 2] = [
    ("batch", "about every 1 GB"),
    (
        "batch",
        "about 1 GB at a time into a new table, swapped in once at the end",
    ),
];

/// The details an incremental table's `reads` and `commits` stand in for.
const REPLACED_LABELS: [&str; 3] = ["cursor", "state", "batch"];

/// Words in a source's error that say it could not be reached, as opposed to read: such an error is headed `can't connect` and gets a fix.
const CONNECTION_WORDS: [&str; 6] = [
    "cannot connect",
    "connection refused",
    "login failed",
    "password authentication failed",
    "timed out",
    "could not connect",
];

/// What an error's reason says, and the headline `plan` gives it.
const HEADLINES: [(&str, &str); 2] = [
    ("the source reports no primary key", "no key to merge on"),
    ("no write_strategy is set", "no write_strategy"),
];

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
    let column_type = match change {
        ColumnChange::Add { column, .. } => &column.column_type,
        ColumnChange::Widen { to, .. } | ColumnChange::Rebuild { to, .. } => to,
    };
    let rendered = sql_adapter(table.spec.warehouse).render_loader_logical_type(column_type);
    let losses: Vec<&str> = rendered.lossy.as_deref().into_iter().collect();
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
        .partition(|column| is_load_column(table.spec.naming, &column.name));
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
        let name = table.spec.source_table.spelling(name);
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
            let losses: Vec<&str> = rendered.lossy.as_deref().into_iter().collect();
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
        WriteMode::Merge { unique_key, dedupe } => {
            let repeats = match dedupe {
                None => "; repeated keys fail".to_owned(),
                Some(MergeDedupe {
                    sort: Some((column, true)),
                    ..
                }) => format!("; repeated keys: highest {column} wins"),
                Some(MergeDedupe {
                    sort: Some((column, false)),
                    ..
                }) => format!("; repeated keys: lowest {column} wins"),
                Some(_) => String::new(),
            };
            let origin = match spec.key_from_primary_key {
                true => format!(" ← {PRIMARY_KEY_PLACEHOLDER}"),
                false => String::new(),
            };
            format!(
                "merge (unique_key: {}{origin}{repeats})",
                unique_key.join(", ")
            )
        }
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
    let unnested: Option<Vec<String>> = match &spec.unnest {
        Unnest::Paths(paths) => Some(paths.clone()),
        Unnest::All => Some(
            list_choices(spec)
                .into_iter()
                .filter_map(|(field, chosen)| chosen.then_some(field))
                .collect(),
        ),
        Unnest::None => None,
    };
    if let Some(paths) = unnested {
        let tables: Vec<String> = paths
            .iter()
            .map(|path| unnested_table(spec, path))
            .collect();
        let text = match (&spec.unnest, tables.is_empty()) {
            (Unnest::All, true) => format!(
                "every list field, {} levels down at most, into child tables as lists appear; columns inferred at load",
                spec.max_depth
            ),
            (Unnest::All, false) => format!(
                "every list field, {} levels down at most: {} and more as lists appear; columns inferred at load",
                spec.max_depth,
                tables.join(", ")
            ),
            _ => format!(
                "{}  child tables, columns inferred at load",
                tables.join(", ")
            ),
        };
        details.push(detail("unnest", text));
    }
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
            format!("reads over {PARALLEL_READERS} connections, in one snapshot"),
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
pub(crate) fn batch_line(table: &TablePlan) -> String {
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
            format!(
                "{}, up to {} files or about 1 GB per commit",
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

/// Each destination `tables` load into, as `Snowflake analytics`, in order.
fn destinations(tables: &[&TablePlan]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for table in tables {
        let name = format!("{} {}", table.spec.warehouse, table.spec.target);
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// `item` with its `route` naming `table`'s destination when the run loads into several.
fn routed_item(table: &TablePlan, mut item: PlanItem, several: bool) -> PlanItem {
    if several {
        let name = format!("{} {}", table.spec.warehouse, table.spec.target);
        for detail in item
            .details
            .iter_mut()
            .filter(|detail| detail.label == ROUTE_LABEL)
        {
            detail.text = format!("{name}: {}", detail.text);
        }
    }
    item
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
    sections(plan, false)
}

/// The plan as `plan` prints it: as `plan_view`, with tables named as they land and sorted by that name, and each load's details sharpened by `text_item`.
pub(crate) fn text_view(plan: &ProjectPlan) -> PlanView {
    sections(plan, true)
}

/// Why a files table that exists and has no new file to read is skipped, as `up to date: 0 new of 3 files in drop/stock_counts/ (csv in ZIP archives)`; `None` for any other table, for one with a schema change or an unfinished load to repair, and for a `delete_insert` over a `--start-cursor`/`--end-cursor` range (`ranged`), which clears that range even with no file in it.
pub(crate) fn up_to_date(table: &TablePlan, ranged: bool) -> Option<String> {
    let (source, read) = table.spec.files.as_ref().zip(table.files.as_ref())?;
    let incremental =
        table.exists && !matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let settled = table.schema.action == TableAction::Unchanged
        && table
            .output
            .as_ref()
            .is_none_or(|output| output.pending.is_empty());
    if !settled || ranged && matches!(table.mode, WriteMode::DeleteInsert { .. }) {
        return None;
    }
    (incremental && read.files.is_empty()).then(|| {
        let files = format!(
            "{} files in {} ({})",
            thousands(source.table.files.len() as u64),
            source.location,
            source.encoding
        );
        match (ranged, reads_line(table, false)) {
            (true, Some(reads)) => format!("nothing in range: 0 of {files} for {reads}"),
            _ => format!("up to date: 0 new of {files}"),
        }
    })
}

fn sections(plan: &ProjectPlan, text: bool) -> PlanView {
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
    let mut errors: Vec<PlanItem> = plan
        .tables
        .iter()
        .filter_map(|table| {
            let item = blocked_item(table)?;
            Some(match (text, &table.schema.blocked) {
                (true, Some(blocked)) => {
                    let name = display_name(table.spec.warehouse, &table.spec.destination);
                    let problem = blocked.reasons.join(" ");
                    let at = table.spec.settings_at.as_deref();
                    text_error(plan, (&problem, at), PlanItem { name, ..item })
                }
                _ => item,
            })
        })
        .collect();
    for (index, problem) in plan.problems.iter().enumerate() {
        errors.push(match text {
            true => {
                let nth = plan.problems[..index]
                    .iter()
                    .filter(|earlier| *earlier == problem)
                    .count();
                let item = problem_item(problem);
                let item = PlanItem {
                    name: nth_about(&plan.problem_names, problem, nth).unwrap_or(item.name),
                    ..item
                };
                let at = nth_about(&plan.problem_places, problem, nth).flatten();
                text_error(plan, (problem, at.as_deref()), item)
            }
            false => problem_item(problem),
        });
    }
    let clauses = [
        (creates, ("object to create", "objects to create")),
        (alters, ("to alter", "to alter")),
        (ready.len(), ("to load", "to load")),
        (plan.skipped.len(), ("skipped", "skipped")),
    ];
    let selected = plan.tables.len();
    let one = plan.tables.len() == 1 && plan.skipped.is_empty() && errors.is_empty();
    let named = match text && !one {
        true => destinations(&ready),
        false => Vec::new(),
    };
    let several = named.len() > 1;
    let mut summary = vec![format!("{selected} selected")];
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
        selected,
        destinations: named,
        objects: stages
            .iter()
            .map(|stage| stage_item(stage))
            .chain(changed.iter().map(|table| object_item(table)))
            .collect(),
        loads: match text {
            false => ready.iter().map(|table| load_item(table)).collect(),
            true => {
                let items: Vec<PlanItem> = ready.iter().map(|table| load_item(table)).collect();
                let usual = usual_details(&ready, &items);
                sorted(
                    ready
                        .iter()
                        .zip(items)
                        .map(|(table, item)| {
                            let item = routed_item(table, item, several);
                            text_item(table, item, one, &usual)
                        })
                        .collect(),
                )
            }
        },
        skipped: match text {
            false => plan
                .skipped
                .iter()
                .map(|skipped| skipped_item(skipped.source_table.clone(), skipped.reason.clone()))
                .collect(),
            true => sorted(
                plan.skipped
                    .iter()
                    .map(|skipped| {
                        let reason = match skipped.reason.strip_suffix(DRAINED_REASON) {
                            Some(drained) => {
                                drained.replacen("no files to load in", "no new files in", 1)
                            }
                            None => skipped.reason.clone(),
                        };
                        skipped_item(skipped.destination.clone(), reason)
                    })
                    .collect(),
            ),
        },
        errors,
        warnings: plan
            .warnings
            .iter()
            .filter(|warning| !(text && folded_warning(plan, warning)))
            .map(|warning| error_item(warning, &[]))
            .chain(ready.iter().filter_map(|table| warning_item(table)))
            .collect(),
    }
}

/// What the `nth` (from 0) of `pairs` about `problem` holds: one plan problem's, as the `nth` of the plan's problems with that text.
fn nth_about<T: Clone>(pairs: &[(String, T)], problem: &str, nth: usize) -> Option<T> {
    pairs
        .iter()
        .filter(|(about, _)| about == problem)
        .nth(nth)
        .map(|(_, value)| value.clone())
}

fn skipped_item(name: String, reason: String) -> PlanItem {
    PlanItem {
        name,
        action: String::new(),
        details: vec![detail("reason", reason)],
    }
}

/// `items` sorted by name, which groups them by schema.
fn sorted(mut items: Vec<PlanItem>) -> Vec<PlanItem> {
    items.sort_by(|left, right| left.name.cmp(&right.name));
    items
}

/// The lines `plan` prints for `view`: `Plan ready`, each section with items, then whether the plan is complete.
pub(crate) fn view_lines(view: &PlanView) -> Vec<String> {
    let mut lines = match view.selected {
        0 if !view.errors.is_empty() => Vec::new(),
        _ => vec![format!("Plan ready  {}", view.summary)],
    };
    lines.extend(view.destinations.iter().map(|name| format!("to  {name}")));
    if !lines.is_empty() {
        lines.push(String::new());
    }
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
        skipped: statuses("skipped"),
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
        Err(reason) if outcome.skipped => ("skipped", None, None, Some(reason.clone())),
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

/// For each of `SHARED_LABELS`, the text most of `items` show, when at least two share it; ties go to the text first in order. An incremental table's `batch`, which `reads` and `commits` replace, isn't counted.
pub(crate) fn usual_details(tables: &[&TablePlan], items: &[PlanItem]) -> HashMap<String, String> {
    let mut counts: Vec<(&str, &str, usize)> = Vec::new();
    for (table, item) in tables.iter().zip(items) {
        for detail in &item.details {
            let label = detail.label.as_str();
            if incremental(table) && REPLACED_LABELS.contains(&label)
                || !SHARED_LABELS.contains(&label)
            {
                continue;
            }
            match counts
                .iter_mut()
                .find(|(label, text, _)| *label == detail.label && *text == detail.text)
            {
                Some((_, _, count)) => *count += 1,
                None => counts.push((&detail.label, &detail.text, 1)),
            }
        }
    }
    let mut usual: HashMap<String, (String, usize)> = HashMap::new();
    for (label, text, count) in counts {
        let best = usual.get(label).map_or(1, |(_, best)| *best);
        if count > best {
            usual.insert(label.to_owned(), (text.to_owned(), count));
        }
    }
    usual
        .into_iter()
        .map(|(label, (text, _))| (label, text))
        .collect()
}

/// Whether `table` reads from a cursor or a logged state, so `reads` and `commits` describe it.
fn incremental(table: &TablePlan) -> bool {
    table.window.is_some() || table.logged.is_some()
}

/// `item`, `load_item`'s view of `table`, as `plan` prints it: named as it lands, `route` (and `why`) directly under `source`, and, for an incremental table, `reads` and `commits` in place of `cursor`, `state` and `batch`. With `one` (one table selected) every detail stays, with times to the millisecond; otherwise a detail with its `usual` text is left out, and `commits` only shows when it says more than the usual.
pub(crate) fn text_item(
    table: &TablePlan,
    item: PlanItem,
    one: bool,
    usual: &HashMap<String, String>,
) -> PlanItem {
    let incremental = incremental(table);
    let mut details: Vec<PlanDetail> = item
        .details
        .into_iter()
        .filter(|detail| match detail.label.as_str() {
            label if incremental && REPLACED_LABELS.contains(&label) => false,
            label if !one && ONE_TABLE_LABELS.contains(&label) => false,
            label => {
                one || (usual.get(label) != Some(&detail.text)
                    && !DEFAULT_DETAILS.contains(&(label, detail.text.as_str())))
            }
        })
        .collect();
    let routed_at = details
        .iter()
        .position(|detail| detail.label == ROUTE_LABEL)
        .unwrap_or(details.len());
    if let Some(text) = incremental.then(|| commits_line(table, one)).flatten() {
        details.insert(routed_at, detail("commits", text));
    }
    let (mut added, mut details): (Vec<PlanDetail>, Vec<PlanDetail>) = details
        .into_iter()
        .partition(|detail| matches!(detail.label.as_str(), "route" | "why"));
    let after_source = details
        .iter()
        .position(|detail| detail.label == SOURCE_LABEL)
        .map_or(0, |index| index + 1);
    if one {
        added.extend(key_line(table).map(|text| detail("key", text)));
    }
    if incremental {
        added.extend(reads_line(table, one).map(|text| detail("reads", text)));
    }
    if one {
        added.extend(columns_line(table).map(|text| detail("columns", text)));
        added.extend(checks_line(table).map(|text| detail("checks", text)));
    }
    details.splice(after_source..after_source, added);
    PlanItem {
        name: display_name(table.spec.warehouse, &table.spec.destination),
        action: item.action,
        details,
    }
}

/// How many columns the table loads from its source, and under dlt's naming the first names it changes: `6, named by naming = "dlt": company-no -> company_no, item-no -> item_no, ...`; `None` when they are only known at load.
fn columns_line(table: &TablePlan) -> Option<String> {
    let count = table
        .schema
        .write_columns
        .iter()
        .filter(|column| column.origin != ColumnOrigin::Metadata)
        .count();
    if count == 0 {
        return None;
    }
    let renamed: Vec<String> = table
        .spec
        .source_table
        .columns
        .iter()
        .filter(|_| table.spec.naming == Naming::Dlt)
        .filter_map(|column| {
            let spelled = column.source_spelling.as_deref()?;
            (spelled != column.name).then(|| format!("{spelled} -> {}", column.name))
        })
        .collect();
    Some(match renamed.as_slice() {
        [] => thousands(count as u64),
        shown => format!(
            "{}, named by naming = \"dlt\": {}{}",
            thousands(count as u64),
            shown
                .iter()
                .take(SHOWN_RENAMES)
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            match shown.len() > SHOWN_RENAMES {
                true => ", ...",
                false => "",
            }
        ),
    })
}

/// The checks its staged rows must pass, as `not_null company_no, item_no; min_rows 1,000 (warns)`.
fn checks_line(table: &TablePlan) -> Option<String> {
    let checks = table.spec.checks.as_ref()?;
    let warns = |name: CheckName| match checks.warn.contains(&name) {
        true => " (warns)",
        false => "",
    };
    let parts: Vec<String> = [
        (!checks.not_null.is_empty()).then(|| {
            format!(
                "not_null {}{}",
                checks.not_null.join(", "),
                warns(CheckName::NotNull)
            )
        }),
        checks
            .unique
            .as_ref()
            .map(|columns| format!("unique {}{}", columns.join(", "), warns(CheckName::Unique))),
        checks
            .min_rows
            .map(|rows| format!("min_rows {}{}", thousands(rows), warns(CheckName::MinRows))),
        checks
            .max_row_drop
            .map(|percent| format!("max_row_drop {percent}%{}", warns(CheckName::MaxRowDrop))),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Where a merge's key comes from: `unique_key`, or the primary key the source reports.
fn key_line(table: &TablePlan) -> Option<String> {
    let WriteMode::Merge { unique_key, .. } = &table.mode else {
        return None;
    };
    Some(match table.spec.key_from_primary_key {
        true => format!(
            "{}, the primary key the source reports (unique_key = \"{PRIMARY_KEY_PLACEHOLDER}\"{})",
            unique_key.join(", "),
            table
                .spec
                .unique_key_at
                .as_deref()
                .map(|at| format!(", {at}"))
                .unwrap_or_default()
        ),
        false => {
            let at = table
                .spec
                .unique_key_at
                .as_deref()
                .map(|at| format!(" ({at})"))
                .unwrap_or_default();
            let primary_key = &table.spec.source_table.primary_key;
            let agrees = !primary_key.is_empty()
                && primary_key
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    == unique_key.iter().collect();
            match agrees {
                true => format!("from unique_key{at}; the source reports the same primary key"),
                false => format!("from unique_key{at}"),
            }
        }
    })
}

/// Where the read starts: `updated_at >= 2026-11-14 07:49:41 (warehouse maximum minus lookback 5s)`, or the logged state; times to the second, or to the millisecond when `precise`.
pub(crate) fn reads_line(table: &TablePlan, precise: bool) -> Option<String> {
    if let Some(logged) = &table.logged {
        return Some(match &logged.current {
            Some(record) => {
                let cursor = record
                    .cursor_column
                    .as_ref()
                    .map(|column| {
                        let at = record.cursor_after.as_deref().unwrap_or("not set");
                        format!("{column} {}, ", trimmed_time(at, precise))
                    })
                    .unwrap_or_default();
                let origin = match record.event {
                    RunEvent::Reset => "state reset",
                    RunEvent::Reconcile => "reconciled",
                    RunEvent::Run | RunEvent::Backfill => "run",
                };
                format!(
                    "logged {cursor}state {} ({origin} {}, {})",
                    record.state.as_deref().unwrap_or("not set"),
                    record.run_id.chars().take(8).collect::<String>(),
                    clock_time(&record.recorded_at)
                )
            }
            None => "everything: no run is logged yet".to_owned(),
        });
    }
    let window = table.window.as_ref()?;
    let note = table.cursor_note.as_deref().unwrap_or_default();
    let said = note
        .strip_prefix(window.column.as_str())
        .unwrap_or(note)
        .trim();
    let Some(from) = &window.from else {
        return Some(format!("{}: {said}", window.column));
    };
    let (bound, after_bound) = match (said.strip_prefix("from "), said.strip_prefix("after ")) {
        (Some(rest), _) => (">=", rest.strip_prefix(from.as_str())),
        (None, Some(rest)) => (">", rest.strip_prefix(from.as_str())),
        (None, None) => (
            match window.inclusive {
                true => ">=",
                false => ">",
            },
            None,
        ),
    };
    let origin = match after_bound {
        Some(rest) => match rest
            .trim()
            .strip_prefix('(')
            .and_then(|o| o.strip_suffix(')'))
        {
            Some(origin) if !origin.contains('(') => {
                origin.replace("destination maximum", "warehouse maximum")
            }
            _ if rest.trim().is_empty() => String::new(),
            _ => return Some(note.to_owned()),
        },
        None => match said.strip_suffix(from.as_str()) {
            Some(before) if !before.contains('(') => before.trim_end().to_owned(),
            _ => return Some(note.to_owned()),
        },
    };
    let origin = match origin.is_empty() {
        true => String::new(),
        false => format!(" ({origin})"),
    };
    Some(format!(
        "{} {bound} {}{origin}",
        window.column,
        trimmed_time(from, precise)
    ))
}

/// How the read commits: once at its end, or after each window; with `precise`, any other rule too, as `batch` says it.
fn commits_line(table: &TablePlan, precise: bool) -> Option<String> {
    match &table.commits {
        CommitRule::Whole(WholeReason::Unordered | WholeReason::Blocked) => Some(format!(
            "once, at the end of the read ({} isn't read in order)",
            table.window.as_ref()?.column
        )),
        CommitRule::Whole(_) => Some(batch_line(table).replacen(
            "one commit for the whole read",
            "once, at the end of the read",
            1,
        )),
        CommitRule::Windows(_) if precise => Some(batch_line(table)),
        CommitRule::Windows(_) => Some(match table.spec.batch {
            Some(BatchStep::Duration(step)) => format!("after each {} window", window_length(step)),
            Some(BatchStep::Integer(step)) => format!("after each window of {step}"),
            Some(BatchStep::Whole) | None => batch_line(table),
        }),
        CommitRule::Ordered { .. } | CommitRule::Size | CommitRule::FileGroups(_) => {
            precise.then(|| batch_line(table))
        }
    }
}

/// A window's length as people say it: `30-day`, `6-hour`, or as written when it isn't whole days, hours or minutes.
fn window_length(step: std::time::Duration) -> String {
    let seconds = step.as_secs();
    let units = [(86_400, "day"), (3_600, "hour"), (60, "minute")];
    units
        .iter()
        .find(|(size, _)| seconds.is_multiple_of(*size) && seconds >= *size)
        .map_or_else(
            || humantime::format_duration(step).to_string(),
            |(size, unit)| format!("{}-{unit}", seconds / size),
        )
}

/// `value` with a timestamp's fraction of a second cut to the second, or to the millisecond when `precise`, and an offset of whole hours shortened (`+00:00` to `+00`); anything else as it is.
fn trimmed_time(value: &str, precise: bool) -> String {
    let bytes = value.as_bytes();
    let is_timestamp = bytes.len() >= SECOND_TIMESTAMP_LENGTH
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && matches!(bytes[10], b' ' | b'T')
        && bytes[13] == b':'
        && bytes[16] == b':'
        && value.is_char_boundary(SECOND_TIMESTAMP_LENGTH);
    if !is_timestamp {
        return value.to_owned();
    }
    let (seconds, rest) = value.split_at(SECOND_TIMESTAMP_LENGTH);
    let digits = match rest.strip_prefix('.') {
        Some(fraction) => fraction.bytes().take_while(u8::is_ascii_digit).count(),
        None => 0,
    };
    let (fraction, offset) = match digits {
        0 => ("", rest),
        _ => rest.split_at(digits + 1),
    };
    let fraction = match precise {
        true => fraction.get(..4).unwrap_or(fraction),
        false => "",
    };
    let offset = offset
        .strip_suffix(":00")
        .filter(|hours| hours.len() == HOURS_OFFSET_LENGTH)
        .unwrap_or(offset);
    format!("{seconds}{fraction}{offset}")
}

/// A recorded time's clock part: `2026-11-14T07:54:11.123456Z` as `07:54:11Z`.
fn clock_time(recorded_at: &str) -> String {
    let time = recorded_at
        .split_once('T')
        .map_or(recorded_at, |(_, time)| time);
    let clock = time.get(..8).unwrap_or(time);
    match time.ends_with('Z') {
        true => format!("{clock}Z"),
        false => clock.to_owned(),
    }
}

/// `item`, an `Errors` item for `problem` (a project problem, or a blocked table's reason), as `plan` prints it: a source that could not be read named by the source, with a headline, its driver's message, a fix and what it holds up (`blocks`); a table's problem named as its table lands, with a headline when its reason has a known one, and a fix that changes a setting starting with where the table's settings are written (`at`, as `sqlload.toml:93`).
pub(crate) fn text_error(
    plan: &ProjectPlan,
    (problem, at): (&str, Option<&str>),
    item: PlanItem,
) -> PlanItem {
    let source = item
        .name
        .strip_prefix("[sources.")
        .and_then(|name| name.strip_suffix(']'));
    let unread = source.and_then(|source| Some((source, plan.unread_sources.get(source)?)));
    match unread {
        Some((source, unread)) => {
            let mut details: Vec<PlanDetail> = item
                .details
                .into_iter()
                .map(|detail| match detail.label.as_str() {
                    "reason" => PlanDetail {
                        text: driver_message(&detail.text),
                        ..detail
                    },
                    _ => detail,
                })
                .collect();
            let reason = details
                .iter()
                .find(|detail| detail.label == REASON_LABEL)
                .map_or_else(String::new, |detail| detail.text.to_lowercase());
            let connects = CONNECTION_WORDS.iter().any(|word| reason.contains(word));
            if connects && !details.iter().any(|detail| detail.label == FIX_LABEL) {
                let check = match unread.variables.as_slice() {
                    [] => format!(
                        "check connection {}'s settings and credentials",
                        unread.connection
                    ),
                    [one] => format!("check {one} (connection {})", unread.connection),
                    [rest @ .., last] => format!(
                        "check {} and {last} (connection {})",
                        rest.join(", "),
                        unread.connection
                    ),
                };
                details.push(detail(
                    "fix",
                    format!("{check}; sqlload debug tests every connection"),
                ));
            }
            details.extend(blocks_line(plan, source, unread).map(|text| detail("blocks", text)));
            PlanItem {
                name: source.to_owned(),
                action: match connects {
                    true => "can't connect".to_owned(),
                    false => item.action,
                },
                details,
            }
        }
        None => PlanItem {
            name: item.name,
            action: HEADLINES
                .iter()
                .find(|(says, _)| problem.contains(says))
                .map_or(item.action, |(_, headline)| (*headline).to_owned()),
            details: item
                .details
                .into_iter()
                .map(|detail| match (detail.label.as_str(), at) {
                    ("fix", Some(at)) if detail.text.contains(" = ") => PlanDetail {
                        text: format!("{at} {}", detail.text),
                        ..detail
                    },
                    _ => detail,
                })
                .collect(),
        },
    }
}

/// Whether `warning` is one `text_error` folds into an unread source's `blocks`.
pub(crate) fn folded_warning(plan: &ProjectPlan, warning: &str) -> bool {
    for sharing in &plan.unread_sharing {
        if sharing.warning == warning
            && sharing
                .sources
                .iter()
                .all(|source| plan.unread_sources.contains_key(source))
        {
            return true;
        }
    }
    false
}

/// What `source`, which could not be read, holds up: its tables, any its include patterns match, and the shared-table check on other tables.
fn blocks_line(plan: &ProjectPlan, source: &str, unread: &UnreadSource) -> Option<String> {
    let mut parts: Vec<String> = unread.tables.clone();
    if unread.patterns {
        parts.push("any table its include patterns match".to_owned());
    }
    for sharing in &plan.unread_sharing {
        if sharing.sources.iter().any(|named| named == source) {
            parts.push(format!(
                "the shared-table check on {}",
                listed(&sharing.tables)
            ));
        }
    }
    match parts.as_slice() {
        [] => None,
        [one] => Some(one.clone()),
        [first, second] => Some(format!("{first} and {second}")),
        [rest @ .., last] => Some(format!("{}, and {last}", rest.join(", "))),
    }
}

/// `names` as `a`, `a and b`, or `a, b and c`.
fn listed(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// An ODBC error as its driver's message with its state and native error, `Login failed for user 'sa' (ODBC state 28000, native error 18456)`; other text as it is.
fn driver_message(text: &str) -> String {
    let field = |name: &str| -> Option<&str> {
        let (_, after) = text.split_once(name)?;
        Some(after.split([',', '\n']).next().unwrap_or(after).trim())
    };
    let message = text.split_once("Message: ").map(|(_, message)| {
        message
            .rsplit(']')
            .next()
            .unwrap_or(message)
            .trim()
            .trim_end_matches('.')
    });
    match (message, field("State: "), field("Native error: ")) {
        (Some(message), Some(state), Some(native)) => {
            format!("{message} (ODBC state {state}, native error {native})")
        }
        _ => text.to_owned(),
    }
}
