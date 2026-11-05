use std::cmp::Ordering;

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::ddl_guard::ddl_guard;
use crate::adapters::dispatch::main::execute::execute;
use crate::adapters::dispatch::main::live_columns::live_columns;
use crate::adapters::dispatch::main::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::models::PendingPhase;
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::read_file::read_file;
use crate::file_store::main::write_file::write_file;
use crate::loader::_helpers::python::batch_cursor_max;
use crate::loader::errors::LoaderError;
use crate::loader::models::{PythonLoad, RunEntry, TablePlan, TableSpec};
use crate::run_log::constants::RECORD_FORMAT_VERSION;
use crate::run_log::main::cursor_order::cursor_order;
use crate::run_log::main::decode_runs::decode_runs;
use crate::run_log::main::event_record::event_record;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::next_stamp::next_stamp;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::run_log::main::runs_setup::runs_setup;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::main::select_runs::select_runs;
use crate::run_log::models::{
    DecodedRuns, RecordTable, RunEvent, RunOutcome, RunRecord, StateEvent, UnreadableRecord,
};
use crate::store_paths::models::Location;
use crate::time_text::main::now_utc_text::now_utc_text;

/// Every record of the table, oldest first, and those that cannot be used, none when the log does not exist (never written, or deleted); and whether a warehouse's `_sqlload_runs` exists.
pub(crate) async fn read_run_log(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(DecodedRuns, bool), LoaderError> {
    let spec = &table.spec;
    let mut texts: Vec<String> = Vec::new();
    let mut exists = false;
    match destination {
        Destination::Files(_) => {
            let folder = open_folder(&records_location(table)?)?;
            for file in list_files(&folder).await? {
                let bytes = read_file(&folder, &file.relative_key).await?;
                texts.push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        destination => {
            let runs = runs_table(&spec.destination);
            exists = live_columns(destination, &runs).await?.is_some();
            if exists {
                let query = select_runs(&runs, &spec.destination.table, spec.warehouse);
                texts = text_values(destination, query).await?;
            }
        }
    }
    Ok((decode_runs(&texts), exists))
}

/// Run records waiting in `replacing` markers of a files table: their loads committed (repair finishes them), so they count before repair writes them.
pub(crate) fn committed_runs(plan: &TablePlan) -> DecodedRuns {
    let mut texts: Vec<String> = Vec::new();
    let pending = plan.output.as_ref().map(|output| output.pending.as_slice());
    for load in pending.unwrap_or_default() {
        if let (PendingPhase::Replacing, Some(run)) = (load.phase, &load.run) {
            texts.push(run.json.clone());
        }
    }
    decode_runs(&texts)
}

/// The data's cursor maximum when it disagrees with the logged cursor, compared as the cursor's type; never while the current record is a reset, which the next run reads from whatever flags this one has.
pub(crate) fn data_disagreement(plan: &TablePlan, current: Option<&RunRecord>) -> Option<String> {
    let current = current.filter(|record| record.event != RunEvent::Reset)?;
    let maximum = plan.window.as_ref()?.maximum.clone()?;
    let cursor = plan.spec.cursor.as_ref()?;
    let column_type = plan
        .read_columns
        .iter()
        .find(|read| read.name.eq_ignore_ascii_case(&cursor.column))
        .map_or(ColumnType::Text, |read| read.output.clone());
    let agrees = current
        .cursor_after
        .as_deref()
        .is_some_and(|logged| cursor_order(logged, &maximum, &column_type) == Ordering::Equal);
    (!agrees).then_some(maximum)
}

/// Why unreadable records block the table: each one no reset follows (an undated one only while there is no reset at all).
pub(crate) fn unreadable_problems(
    unreadable: &[UnreadableRecord],
    records: &[RunRecord],
) -> Vec<String> {
    let reset_at = records
        .iter()
        .rev()
        .find(|record| record.event == RunEvent::Reset)
        .map(|record| record.recorded_at.as_str());
    unreadable
        .iter()
        .filter(|record| match (record.recorded_at.as_deref(), reset_at) {
            (_, None) => true,
            (Some(at), Some(reset)) => at > reset,
            (None, Some(_)) => false,
        })
        .map(|record| {
            format!(
                "run log record {} cannot be used: {}; sqlload state show lists it, and sqlload state reset (--to VALUE or --clear) --yes starts a new position after it",
                record.run_id.as_deref().unwrap_or("without a run id"),
                record.reason
            )
        })
        .collect()
}

/// Append `record` on its own: a new object in a files target, a row in `_sqlload_runs` (created when missing) in a warehouse.
pub(crate) async fn append_run(
    table: &TablePlan,
    destination: &Destination,
    record: &RunRecord,
) -> Result<(), LoaderError> {
    let spec = &table.spec;
    match destination {
        Destination::Files(_) => {
            let folder = open_folder(&records_location(table)?)?;
            let json = record_json(record)?;
            Ok(write_file(&folder, &record_object_name(record), Bytes::from(json)).await?)
        }
        destination => {
            ensure_runs_table(table, destination).await?;
            let insert = insert_run(&runs_table(&spec.destination), record, spec.warehouse)
                .map_err(LoaderError::Invalid)?;
            execute(destination, vec![insert]).await
        }
    }
}

/// Log a logged table's run that wrote no rows (nothing to commit it with), keeping its position.
pub(crate) async fn log_empty_run(
    run: Option<&RunEntry>,
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    let Some(run) = run else {
        return Ok(());
    };
    let record = run.finished(RunOutcome {
        run_id: uuid::Uuid::new_v4().to_string(),
        recorded_at: now_utc_text(),
        ..RunOutcome::default()
    });
    append_run(table, destination, &record).await
}

/// Create the table's schema and `_sqlload_runs` beside the table when the plan saw no run log (on Snowflake with the longest time travel the edition allows); nothing once it exists.
pub(crate) async fn ensure_runs_table(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    let spec = &table.spec;
    let exists = table
        .logged
        .as_ref()
        .is_some_and(|logged| logged.runs_table);
    let Some(setup) = runs_setup(&spec.destination, spec.warehouse, exists) else {
        return Ok(());
    };
    let _held = ddl_guard(destination).await;
    execute(destination, vec![setup.schema]).await?;
    match (
        execute(destination, vec![setup.create]).await,
        setup.fallback,
    ) {
        (Ok(()), _) => Ok(()),
        (Err(_), Some(fallback)) => execute(destination, vec![fallback]).await,
        (Err(error), None) => Err(error),
    }
}

/// Before a logged table reads: when its logged cursor disagrees with the data, append a reconciliation adopting the data's position (the data wins) and warn; returns the reconciliation's time, which the run's record must follow.
pub(crate) async fn reconcile_before_read(
    table: &TablePlan,
    destination: &Destination,
) -> Result<Option<String>, LoaderError> {
    let Some(record) = reconcile_record(table) else {
        return Ok(None);
    };
    tracing::warn!(
        table = %table.spec.destination.table,
        "{}",
        record.note.as_deref().unwrap_or_default()
    );
    append_run(table, destination, &record).await?;
    Ok(Some(record.recorded_at))
}

/// The reconciliation a logged table needs, when its logged cursor disagrees with the data's maximum: the data's position, keeping the logged `ctx.state`.
pub(crate) fn reconcile_record(table: &TablePlan) -> Option<RunRecord> {
    let logged = table.logged.as_ref()?;
    let (data, current) = (logged.disagreement.as_ref()?, logged.current.as_ref()?);
    let logged_cursor = current.cursor_after.as_deref().unwrap_or("nothing");
    Some(event_record(
        &record_table(&table.spec),
        StateEvent {
            event: RunEvent::Reconcile,
            cursor_before: current.cursor_after.clone(),
            cursor_after: Some(data.clone()),
            state: current.state.clone(),
            note: format!(
                "the run log had {logged_cursor} but the data's maximum is {data}; the data wins"
            ),
            recorded_at: next_stamp(&now_utc_text(), newest_stamp(table)),
        },
    ))
}

/// A reset to `to`: a cursor value for tables with a cursor (checked against its type when known), else `ctx.state` as JSON (text that is not JSON is taken as a string); `None` forgets the position.
pub(crate) fn reset_record(table: &TablePlan, to: Option<&str>) -> Result<RunRecord, String> {
    let spec = &table.spec;
    let current = table
        .logged
        .as_ref()
        .and_then(|logged| logged.current.as_ref());
    let (cursor_after, state) = match (&spec.cursor, to) {
        (_, None) => (None, None),
        (Some(cursor), Some(to)) => {
            let column_type = table
                .read_columns
                .iter()
                .find(|read| read.name.eq_ignore_ascii_case(&cursor.column))
                .map(|read| read.output.clone());
            let normal = match column_type {
                Some(column_type) => cursor_text(to, &column_type)?,
                None => to.to_owned(),
            };
            (
                Some(normal),
                current.and_then(|record| record.state.clone()),
            )
        }
        (None, Some(to)) => {
            let json = match serde_json::from_str::<serde_json::Value>(to) {
                Ok(value) => value.to_string(),
                Err(_) => serde_json::Value::String(to.to_owned()).to_string(),
            };
            (None, Some(json))
        }
    };
    Ok(event_record(
        &record_table(spec),
        StateEvent {
            event: RunEvent::Reset,
            cursor_before: current.and_then(|record| record.cursor_after.clone()),
            cursor_after,
            state,
            note: "sqlload state reset".to_owned(),
            recorded_at: next_stamp(&now_utc_text(), newest_stamp(table)),
        },
    ))
}

/// The record a logged table's load writes with its data: a run (a backfill with `--start/--end-cursor-*`, which never moves the state), from where it read, with the position the function left in `ctx.state` (or the one it started from), stamped after the log's newest record or the reconciliation this load wrote (`reconciled`, with `state` the position the function left); `None` when the table is not logged.
pub(crate) fn run_entry(
    load: &PythonLoad<'_>,
    table: &TablePlan,
    (state, reconciled): (Option<String>, Option<String>),
) -> Option<RunEntry> {
    let logged = load.table.logged.as_ref()?;
    let spec = &table.spec;
    let options = load.options;
    let backfill = options.start_cursor.is_some() || options.end_cursor.is_some();
    let previous = logged
        .current
        .as_ref()
        .and_then(|record| record.state.clone())
        .filter(|_| !options.reload);
    let window = load.table.window.as_ref();
    let cursor_type = spec.cursor.as_ref().and_then(|cursor| {
        table
            .read_columns
            .iter()
            .find(|read| read.name.eq_ignore_ascii_case(&cursor.column))
            .map(|read| read.output.clone())
    });
    let about = record_table(spec);
    Some(RunEntry {
        record: RunRecord {
            format_version: RECORD_FORMAT_VERSION,
            run_id: String::new(),
            event: match backfill {
                true => RunEvent::Backfill,
                false => RunEvent::Run,
            },
            source: about.source,
            source_table: about.source_table,
            table_name: about.table_name,
            cursor_column: about.cursor_column,
            cursor_before: window.and_then(|window| window.from.clone()),
            cursor_after: None,
            state: state.or(previous),
            row_count: 0,
            files: Vec::new(),
            note: None,
            recorded_at: String::new(),
            batch: None,
        },
        cursor_type,
        before: window.and_then(|window| window.maximum.clone()),
        replaces: matches!(load.table.mode, WriteMode::Replace | WriteMode::Recreate),
        newest: reconciled.or_else(|| newest_stamp(load.table).map(str::to_owned)),
    })
}

/// `maximum` with `batch`'s largest cursor folded in, for a logged table with a cursor.
pub(crate) fn fold_maximum(
    run: Option<&RunEntry>,
    batch: &RecordBatch,
    maximum: Option<String>,
) -> Result<Option<String>, String> {
    let Some(run) = run else {
        return Ok(maximum);
    };
    let (Some(column), Some(column_type)) = (&run.record.cursor_column, &run.cursor_type) else {
        return Ok(maximum);
    };
    let batch_maximum = batch_cursor_max(batch, column, column_type)?;
    Ok(run.later(maximum, batch_maximum))
}

/// What a record says about the table of `spec`.
pub(crate) fn record_table(spec: &TableSpec) -> RecordTable {
    RecordTable {
        source: spec.source_name.clone(),
        source_table: spec.source_table.name.clone(),
        table_name: spec.destination.table.clone(),
        cursor_column: spec.cursor.as_ref().map(|cursor| cursor.column.clone()),
    }
}

/// Where a files table's run records live: under its folder, which the plan worked out.
fn records_location(table: &TablePlan) -> Result<Location, LoaderError> {
    match &table.output {
        Some(output) => Ok(runs_location(&output.path.folder)),
        None => Err(LoaderError::Invalid(format!(
            "{}: a files target table has no folder planned",
            table.spec.destination.table
        ))),
    }
}

/// The newest time in the log, readable or not, which a new record must follow (unreadable records carry a time only when it is RFC 3339).
fn newest_stamp(table: &TablePlan) -> Option<&str> {
    let logged = table.logged.as_ref()?;
    let readable = logged
        .records
        .iter()
        .map(|record| record.recorded_at.as_str());
    let unreadable = logged
        .unreadable
        .iter()
        .filter_map(|record| record.recorded_at.as_deref());
    readable.chain(unreadable).max()
}

fn record_json(record: &RunRecord) -> Result<String, LoaderError> {
    serde_json::to_string(record).map_err(|error| LoaderError::Invalid(error.to_string()))
}
