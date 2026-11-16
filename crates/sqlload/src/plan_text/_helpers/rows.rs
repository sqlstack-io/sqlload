//! What a table's line and its child lines say.

use crate::adapters::contract::models::{MergeDedupe, WriteMode};
use crate::loader::models::{CommitRule, PlanDetail, PlanItem, TablePlan};
use crate::number_text::main::memory_text::memory_text;
use crate::plan_text::constants::{
    BATCH, FILES, INFERRED_SCHEMA, MAXIMUM, MIB, OWN_ROUTES, QUIET_LABELS, ROUTE, SCHEMA,
};
use crate::plan_text::models::LoadedTable;
use crate::project::models::{BatchStep, NullCursor, OnCursorOutOfRange};
use crate::row_deletes::main::deletes_note::deletes_note;
use crate::run_log::models::RunEvent;
use crate::schema_plan::models::TableAction;

/// A child line with `label` (empty for none) and `text`.
fn child(label: &str, text: impl Into<String>) -> PlanDetail {
    PlanDetail {
        label: label.to_owned(),
        text: text.into(),
    }
}

/// A source's line: its name, then what it reads: the Python file, the files' folder, or the kind of database (`odbc`, `postgres`), with a `postgres_cdc` source's slot and an SSH tunnel's route.
pub(crate) fn source_line(source: &str, table: &TablePlan) -> String {
    let spec = &table.spec;
    let what = match (&spec.files, &spec.python, &spec.warehouse_source) {
        (Some(files), _, _) => files.location.to_string(),
        (None, Some(python), _) => python.module.clone(),
        (None, None, Some(warehouse)) => warehouse.system.to_owned(),
        (None, None, None) if spec.sheet.is_some() => "google_sheets".to_owned(),
        (None, None, None) => match &spec.cdc {
            Some(cdc) => format!("postgres_cdc slot {}", cdc.slot),
            None => "postgres".to_owned(),
        },
    };
    match &spec.tunnel {
        Some(route) => format!("{source} {what} via {route}"),
        None => format!("{source} {what}"),
    }
}

/// The names of a source's `tables` under its line: each as it lands, without the `<source>__` prefix and, where they share one, without their schema; with that schema when it isn't the source's name, for the source's line to say.
pub(crate) fn table_names(
    source: &str,
    tables: &[&LoadedTable<'_>],
) -> (Option<String>, Vec<String>) {
    let prefix = format!("{}__", source.to_lowercase());
    let mut split: Vec<(String, String)> = Vec::new();
    for loaded in tables {
        let (schema, table) = loaded.name.rsplit_once('.').unwrap_or(("", &loaded.name));
        let short = match table.get(..prefix.len()) {
            Some(head) if head.eq_ignore_ascii_case(&prefix) && table.len() > prefix.len() => {
                &table[prefix.len()..]
            }
            _ => table,
        };
        split.push((schema.to_owned(), short.to_owned()));
    }
    let first = split
        .first()
        .map(|(schema, _)| schema.clone())
        .unwrap_or_default();
    if split.iter().any(|(schema, _)| *schema != first) {
        let names: Vec<String> = split
            .into_iter()
            .map(|(schema, table)| match schema.is_empty() {
                true => table,
                false => format!("{schema}.{table}"),
            })
            .collect();
        return (None, names);
    }
    let names: Vec<String> = split.into_iter().map(|(_, table)| table).collect();
    match first.is_empty() || first.eq_ignore_ascii_case(source) {
        true => (None, names),
        false => (Some(first), names),
    }
}

/// A table's write strategy and key, short: `merge on company_no, item_no`, `delete_insert on dt`, `replace`; others as `-v` gives them.
pub(crate) fn action(loaded: &LoadedTable<'_>) -> String {
    match &loaded.table.mode {
        WriteMode::Merge { unique_key, .. } => format!("merge on {}", unique_key.join(", ")),
        WriteMode::DeleteInsert { replace_key, .. } if !replace_key.is_empty() => {
            format!("delete_insert on {}", replace_key.join(", "))
        }
        WriteMode::DeleteInsert { .. } => "delete_insert".to_owned(),
        WriteMode::Append => "append".to_owned(),
        _ => loaded.item.action.clone(),
    }
}

/// Where an incremental table's read starts: `updated_at \u{2265} 2026-11-16 08:41:12 (max \u{2212} 5s)`, or the logged state it carries on from; empty for a table read whole.
pub(crate) fn reads(loaded: &LoadedTable<'_>) -> String {
    let table = loaded.table;
    if table.logged.is_some() {
        return logged_reads(table);
    }
    if table.window.is_none() {
        return String::new();
    }
    let text = loaded
        .reads
        .clone()
        .unwrap_or_default()
        .replacen(" >= ", " \u{2265} ", 1);
    let (maximum, lookback) = MAXIMUM;
    match text.split_once(maximum) {
        Some((bound, rest)) => match rest.strip_prefix(lookback) {
            Some(lookback) => format!("{bound} (max \u{2212} {lookback}"),
            None => format!("{bound} (max{rest}"),
        },
        None => text,
    }
}

/// The logged state a table carries on from: `from state "2026-11-16T11:39:26Z" (run 20d2b872)`, with the cursor it logged too; `from the start` before its first logged run.
fn logged_reads(table: &TablePlan) -> String {
    let Some(record) = table
        .logged
        .as_ref()
        .and_then(|logged| logged.current.as_ref())
    else {
        return "from the start (no run logged yet)".to_owned();
    };
    let cursor = record
        .cursor_column
        .as_ref()
        .map(|column| {
            let at = record.cursor_after.as_deref().unwrap_or("not set");
            format!("{column} {at}, ")
        })
        .unwrap_or_default();
    let origin = match record.event {
        RunEvent::Reset => "state reset ",
        RunEvent::Reconcile => "reconciled ",
        RunEvent::Run | RunEvent::Backfill => "run ",
    };
    format!(
        "from {cursor}state {} ({origin}{})",
        record.state.as_deref().unwrap_or("not set"),
        record.run_id.chars().take(8).collect::<String>()
    )
}

/// A files table's count of files to read, for its line, as `12 new of 340 files`, and its other details; the count stays a detail when it says more, such as how many are in archives.
pub(crate) fn files_read(item: &PlanItem) -> (Option<String>, Vec<PlanDetail>) {
    let mut details: Vec<PlanDetail> = item.details.clone();
    let Some(index) = details
        .iter()
        .position(|detail| detail.label == FILES && !detail.text.contains([',', ':', '(']))
    else {
        return (None, details);
    };
    let text = details.remove(index).text;
    let read = match text.strip_suffix(" listed") {
        Some(read) => format!("{read} files"),
        None => text,
    };
    (Some(read), details)
}

/// A table's child lines: the rules its cursor follows where they aren't the defaults, how often a read in windows commits, its merge's own rules, its `deletes`, a `transform`, a fetch that won't fit its buffer with the fix, then the details `-v` shows that are its own (see [`shown`]).
pub(crate) fn children(table: &TablePlan, details: &[PlanDetail]) -> Vec<PlanDetail> {
    let spec = &table.spec;
    let mut lines: Vec<PlanDetail> = cursor_lines(table);
    lines.extend(merge_lines(table));
    if let Some(note) = deletes_note(spec).strip_prefix(", then ") {
        lines.push(child("deletes", note.replace(", then ", "; then ")));
    }
    if let Some(transform) = &spec.transform {
        lines.push(child("transform", transform.function.as_str()));
    }
    lines.extend(memory_line(table));
    for detail in details {
        if shown(detail) && !(detail.label == BATCH && spec.files.is_some()) {
            lines.push(detail.clone());
        }
    }
    lines
}

/// What an incremental table's cursor does unlike the defaults: NULL cursors included or skipped, a Python table's rows outside a window skipped or let through, a `timezone`; and its commits when it reads in windows, as `commits every 30 days`.
fn cursor_lines(table: &TablePlan) -> Vec<PlanDetail> {
    let spec = &table.spec;
    let mut lines: Vec<PlanDetail> = Vec::new();
    let (Some(cursor), Some(window)) = (spec.cursor.as_ref(), table.window.as_ref()) else {
        return lines;
    };
    match cursor.null_cursor {
        NullCursor::Include if spec.files.is_none() => {
            lines.push(child("", "NULL cursors included"))
        }
        NullCursor::Ignore if spec.files.is_none() => lines.push(child("", "NULL cursors skipped")),
        _ => {}
    }
    match cursor.out_of_range {
        OnCursorOutOfRange::Skip if spec.python.is_some() => {
            lines.push(child("", "rows outside a window are skipped"));
        }
        OnCursorOutOfRange::Warn if spec.python.is_some() => {
            lines.push(child("", "rows outside a window load, with a warning"));
        }
        _ => {}
    }
    if let Some((zone, at)) = &spec.timezone {
        lines.push(child(
            "",
            format!("times without an offset in {zone} (the {at}'s timezone)"),
        ));
    }
    if let (CommitRule::Windows(_), Some(step)) = (&table.commits, &spec.batch) {
        match step {
            BatchStep::Duration(step) => {
                lines.push(child("", format!("commits every {}", every(*step))));
            }
            BatchStep::Integer(step) => {
                lines.push(child(
                    "",
                    format!("commits every {step} of {}", window.column),
                ));
            }
            BatchStep::Whole => {}
        }
    }
    lines
}

/// A merge's own rules: repeated keys failing, the column ranking them, and a rebuild under `--reload`.
fn merge_lines(table: &TablePlan) -> Vec<PlanDetail> {
    let mut lines: Vec<PlanDetail> = Vec::new();
    let WriteMode::Merge { dedupe, .. } = &table.mode else {
        return lines;
    };
    match dedupe {
        None => lines.push(child("", "repeated keys fail")),
        Some(MergeDedupe { newest_by, .. }) if !newest_by.is_empty() => {
            lines.push(child(
                "",
                format!("newest by {}", newest_by.join(", then ")),
            ));
        }
        Some(_) => {}
    }
    if table.schema.action == TableAction::Recreate {
        lines.push(child(
            "",
            "into a new table, swapped in at the end (--reload)",
        ));
    }
    lines
}

/// An ODBC table whose single row is more than its fetch buffer holds: how much, and the `max_batch_memory` that holds it.
fn memory_line(table: &TablePlan) -> Option<PlanDetail> {
    let fetch = table.fetch_memory.as_ref()?;
    let estimate = fetch.estimate.filter(|estimate| *estimate > fetch.budget)?;
    Some(child(
        "memory",
        format!(
            "~{} per fetch, over the {} fetch buffer\nset max_batch_memory = \"{}\" on the source",
            memory_text(estimate as u64),
            memory_text(fetch.budget as u64),
            enough(estimate as u64)
        ),
    ))
}

/// Whether a detail `-v` shows is the table's own: not one of the [`QUIET_LABELS`], not the `schema` line every Python table has, and a `route` only when no row passes through SQLLoad or files are copied unchanged.
fn shown(detail: &PlanDetail) -> bool {
    let label = detail.label.as_str();
    if QUIET_LABELS.contains(&label) {
        return false;
    }
    match label {
        SCHEMA => !detail.text.starts_with(INFERRED_SCHEMA),
        ROUTE => OWN_ROUTES.iter().any(|words| detail.text.contains(words)),
        _ => true,
    }
}

/// A window's length as a commit's interval: `30 days`, `6 hours`, `day`, or as written when it isn't whole days, hours or minutes.
fn every(step: std::time::Duration) -> String {
    let seconds = step.as_secs();
    let units = [(86_400, "day"), (3_600, "hour"), (60, "minute")];
    let Some((size, unit)) = units
        .iter()
        .find(|(size, _)| seconds.is_multiple_of(*size) && seconds >= *size)
    else {
        return humantime::format_duration(step).to_string();
    };
    match seconds / size {
        1 => (*unit).to_owned(),
        count => format!("{count} {unit}s"),
    }
}

/// A `max_batch_memory` that holds `bytes`: the next power of two of MiB at or above it, as `2GiB` or `512MiB`.
fn enough(bytes: u64) -> String {
    let (mib, gib) = MIB;
    let needed = bytes.div_ceil(mib).next_power_of_two();
    match needed.checked_div(gib).filter(|whole| *whole > 0) {
        Some(whole) => format!("{whole}GiB"),
        None => format!("{needed}MiB"),
    }
}
