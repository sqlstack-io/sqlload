use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::ddl_guard::ddl_guard;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::models::PendingPhase;
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::read_file::read_file;
use crate::file_store::main::write_file::write_file;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, VERSION_SEPARATOR,
};
use crate::files_source::main::merge_order::merge_order;
use crate::files_source::models::{DataFile, FilesOrder, LoadedFile};
use crate::load_naming::models::NumberedName;
use crate::loader::_helpers::python::batch_cursor_max;
use crate::loader::_helpers::sources::{FilesPlanRequest, LoadedSource, column_fix, live_has};
use crate::loader::constants::TRANSFORM_REWROTE_NOTE;
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    MergeRebuild, ProjectPlan, PythonLoad, RunEntry, TablePlan, TableSpec, TransformRun,
};
use crate::project::models::WriteStrategy;
use crate::row_batches::models::Unnest;
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
use crate::schema_plan::models::{LiveColumn, TableAction};
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
    match sql_adapter(spec.warehouse).runs_sql() {
        false => {
            let folder = open_folder(&records_location(table)?)?;
            for file in list_files(&folder).await? {
                let bytes = read_file(&folder, &file.relative_key).await?;
                texts.push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        true => {
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
                "run log record {} cannot be used: {}; sqlload logged-state show lists it, and sqlload logged-state reset (--to VALUE or --clear) --yes starts a new position after it",
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
    match sql_adapter(spec.warehouse).runs_sql() {
        false => {
            let folder = open_folder(&records_location(table)?)?;
            let json = record_json(record)?;
            Ok(write_file(&folder, &record_object_name(record), Bytes::from(json)).await?)
        }
        true => {
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
                Some(column_type) => cursor_text(to, &column_type, spec.zone())?,
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
            note: "sqlload logged-state reset".to_owned(),
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
    let backfill = options.ranged();
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
            transform: None,
            numbered: recorded_names(spec),
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

/// Whether `spec`'s files source loads one file per commit, each recorded in the run log, which then picks the files it reads: a merge, or `delete_insert` by `replace_key`.
pub(crate) fn logs_files(spec: &TableSpec) -> bool {
    spec.files.is_some()
        && match spec.strategy {
            WriteStrategy::Merge => true,
            WriteStrategy::DeleteInsert => !spec.replace_key.is_empty(),
            WriteStrategy::Append | WriteStrategy::Replace => false,
        }
}

/// The files (`name|version`) a files source's merge recorded merging into the table of `spec`, since the table's newest reset; why they cannot be known when a record after it cannot be read; none when the run log does not exist.
pub(crate) async fn merged_files(
    spec: &TableSpec,
    destination: &Destination,
) -> Result<Result<Vec<LoadedFile>, String>, LoaderError> {
    let runs = runs_table(&spec.destination);
    if live_columns(destination, &runs).await?.is_none() {
        return Ok(Ok(Vec::new()));
    }
    let query = select_runs(&runs, &spec.destination.table, spec.warehouse);
    let decoded = decode_runs(&text_values(destination, query).await?);
    if let Some(problem) = unreadable_problems(&decoded.unreadable, &decoded.records)
        .into_iter()
        .next()
    {
        return Ok(Err(format!(
            "{problem}; until then merge cannot tell which files it merged"
        )));
    }
    let since = decoded
        .records
        .iter()
        .rposition(|record| record.event == RunEvent::Reset)
        .map_or(0, |reset| reset + 1);
    let files: Vec<LoadedFile> = decoded.records[since..]
        .iter()
        .filter(|record| {
            record.event == RunEvent::Run
                && record.source == spec.source_name
                && record.source_table == spec.source_table.name
        })
        .flat_map(|record| record.files.iter().cloned())
        .map(|text| loaded_file(text, true))
        .collect();
    Ok(Ok(files))
}

/// The newest record of each `merged` file (records come oldest first), kept when the file is still listed at that version or no longer listed: a file changed since, even back to an earlier version, merges again.
pub(crate) fn unchanged_files(
    merged: Vec<LoadedFile>,
    listed: &HashMap<String, String>,
) -> Vec<LoadedFile> {
    let mut newest: HashMap<String, LoadedFile> = HashMap::new();
    for file in merged {
        newest.insert(file.name.clone(), file);
    }
    newest
        .into_values()
        .filter(|file| {
            listed
                .get(&file.name)
                .is_none_or(|version| *version == file.version)
        })
        .collect()
}

/// The record a files source's merge commits with `file` (`name|version`), its `batch`th commit of run `run_id`, which wrote `rows`.
pub(crate) fn merged_record(
    spec: &TableSpec,
    (run_id, batch): (&str, u64),
    (file, rows): (String, u64),
) -> RunRecord {
    let about = record_table(spec);
    RunRecord {
        format_version: RECORD_FORMAT_VERSION,
        run_id: run_id.to_owned(),
        event: RunEvent::Run,
        source: about.source,
        source_table: about.source_table,
        table_name: about.table_name,
        cursor_column: about.cursor_column,
        cursor_before: None,
        cursor_after: None,
        state: None,
        row_count: rows,
        files: vec![file],
        note: Some(
            match spec.strategy {
                WriteStrategy::Merge => "files source merge",
                _ => "files source delete_insert",
            }
            .to_owned(),
        ),
        recorded_at: now_utc_text(),
        batch: Some(batch),
        transform: None,
        numbered: None,
    }
}

/// The time a load's `transform` took on each file or range, once the load succeeded. The calls no commit recorded (`recorded` are), as when batches commit straight into the table or into a files target, are recorded now: an insert into the run log `apply` made, never creating it, and only warned about when it fails, since the rows committed (a logged table's run log holds its runs only).
pub(crate) async fn transformed(
    table: &TablePlan,
    (destination, run, recorded): (&Destination, Option<TransformRun>, usize),
) -> Vec<crate::python_sources::models::TransformTime> {
    let Some(run) = run else {
        return Vec::new();
    };
    let times = std::mem::take(
        &mut *run
            .times
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    let spec = &table.spec;
    let record = spec
        .transform
        .as_ref()
        .filter(|_| times.len() > recorded && !spec.logged)
        .map(|transform| transform_record(table, &transform.version, &times[recorded..]));
    let written = match (record, sql_adapter(spec.warehouse).runs_sql()) {
        (None, _) => Ok(()),
        (Some(record), false) => append_run(table, destination, &record).await,
        (Some(record), true) => {
            match insert_run(&runs_table(&spec.destination), &record, spec.warehouse) {
                Ok(insert) => execute(destination, vec![insert]).await,
                Err(error) => Err(LoaderError::Invalid(error)),
            }
        }
    };
    if let Err(error) = written {
        tracing::warn!(table = %spec.destination.table, %error, "the rows loaded, but the run log did not record the transform's version, so plan cannot tell which files an earlier version loaded");
    }
    times
}

/// The record a commit writes with its rows for the `transform` calls since the last commit, which it counts as recorded; `None` without calls or a `transform`.
pub(crate) fn commit_transform(
    table: &TablePlan,
    transform: Option<&mut (TransformRun, usize)>,
) -> Option<RunRecord> {
    let (run, recorded) = transform?;
    let version = &table.spec.transform.as_ref()?.version;
    let times = run
        .times
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let since = times.get(*recorded..).filter(|since| !since.is_empty())?;
    let record = transform_record(table, version, since);
    *recorded = times.len();
    Some(record)
}

/// `record` (a logged table's, or a merged file's) carrying the version of the table's `transform`, if it has one.
pub(crate) fn with_transform(table: &TablePlan, record: RunRecord) -> RunRecord {
    RunRecord {
        transform: table
            .spec
            .transform
            .as_ref()
            .map(|transform| transform.version.clone()),
        ..record
    }
}

/// `plan` with what its `transform` needs from the run log: whether `apply` must create `_sqlload_runs`, and how many of the files or ranges the table holds loaded under another version; a table rewritten whole (`replace`, `--reload`) or a logged one is left as it is, and one blocked.
pub(crate) async fn with_transform_version(
    mut plan: TablePlan,
    destination: &Destination,
) -> Result<TablePlan, LoaderError> {
    let rewritten = matches!(plan.mode, WriteMode::Replace | WriteMode::Recreate)
        || plan.schema.action == TableAction::Recreate;
    let runs_sql = sql_adapter(plan.spec.warehouse).runs_sql();
    let compares = plan.exists && !rewritten;
    let Some(current) = plan
        .spec
        .transform
        .as_ref()
        .map(|transform| transform.version.clone())
    else {
        return Ok(plan);
    };
    if plan.spec.logged || plan.schema.blocked.is_some() || !(compares || runs_sql) {
        return Ok(plan);
    }
    let (log, exists) = read_run_log(&plan, destination).await?;
    let older = match compares {
        true => older_files(&log.records, &plan.spec, &current),
        false => 0,
    };
    if let Some(transform) = plan.spec.transform.as_mut() {
        transform.older = older;
        transform.creates_log = runs_sql && !exists;
    }
    Ok(plan)
}

/// How many of the files or ranges the table of `spec` holds loaded under a `transform` version other than `current`: the version of each one's newest record (a merged file's by its name, whatever its version) since the last reset or load that rewrote the table whole.
pub(crate) fn older_files(records: &[RunRecord], spec: &TableSpec, current: &str) -> usize {
    let mut ordered: Vec<&RunRecord> = records
        .iter()
        .filter(|record| {
            record.source == spec.source_name && record.source_table == spec.source_table.name
        })
        .collect();
    ordered.sort_by(|left, right| left.recorded_at.cmp(&right.recorded_at));
    let mut versions: BTreeMap<&str, &str> = BTreeMap::new();
    for record in ordered {
        if record.event == RunEvent::Reset || record.note.as_deref() == Some(TRANSFORM_REWROTE_NOTE)
        {
            versions.clear();
        }
        let merged = !record
            .note
            .as_deref()
            .is_some_and(|note| note.starts_with("transform"));
        if let Some(version) = &record.transform {
            versions.extend(record.files.iter().map(|file| match merged {
                true => (merged_name(file), version.as_str()),
                false => (file.as_str(), version.as_str()),
            }));
        }
    }
    versions
        .values()
        .filter(|version| **version != current)
        .count()
}

/// The record of a load of a table with a `transform`: the version it ran, and the files or ranges it ran on (`times`), which the next `plan` compares with.
pub(crate) fn transform_record(
    table: &TablePlan,
    version: &str,
    times: &[crate::python_sources::models::TransformTime],
) -> RunRecord {
    let rewrote = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let record = event_record(
        &record_table(&table.spec),
        StateEvent {
            event: RunEvent::Run,
            cursor_before: None,
            cursor_after: None,
            state: None,
            note: match rewrote {
                true => TRANSFORM_REWROTE_NOTE,
                false => "transform",
            }
            .to_owned(),
            recorded_at: now_utc_text(),
        },
    );
    RunRecord {
        files: times.iter().map(|time| time.file.clone()).collect(),
        transform: Some(version.to_owned()),
        ..record
    }
}

/// The names numbering gave `spec`'s columns and those its earlier loads gave that no column now loads as otherwise, for its run log; `None` when they are the ones the log holds already, and never then when the load rebuilds the table over an older record.
pub(crate) fn recorded_names(spec: &TableSpec) -> Option<Vec<NumberedName>> {
    let names = &spec.names;
    let taken = |column: &str| {
        names
            .numbered
            .iter()
            .any(|name| name.column.eq_ignore_ascii_case(column))
            || names
                .columns
                .iter()
                .any(|loaded| loaded.loaded.eq_ignore_ascii_case(column))
    };
    let mut recorded: Vec<NumberedName> = names.numbered.clone();
    recorded.extend(
        names
            .kept
            .iter()
            .filter(|kept| !taken(&kept.column))
            .cloned(),
    );
    recorded.sort();
    let mut kept = names.kept.clone();
    kept.sort();
    (recorded != kept || names.replaces_record).then_some(recorded)
}

/// Once a load that is not logged succeeded, the names numbering gave its columns, when they are not the ones its run log holds already, so the next plan keeps them; a logged table's own run records carry them instead (see [`recorded_names`]), so its current state stays its newest run.
pub(crate) async fn record_numbered(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    let numbered = recorded_names(&table.spec);
    if numbered.is_none() || table.spec.logged {
        return Ok(());
    }
    let record = event_record(
        &record_table(&table.spec),
        StateEvent {
            event: RunEvent::Run,
            cursor_before: None,
            cursor_after: None,
            state: None,
            note: "names".to_owned(),
            recorded_at: now_utc_text(),
        },
    );
    append_run(table, destination, &RunRecord { numbered, ..record }).await
}

/// The names numbering gave the table's columns in its earlier loads, from the newest record of its run log holding a list (empty once a rebuild left none); `None` when the table doesn't exist or none of its live columns look numbered (`x` beside `x_1`), so the log is read only then.
pub(crate) async fn kept_numbered(
    plan: &TablePlan,
    destination: &Destination,
) -> Result<Option<Vec<NumberedName>>, LoaderError> {
    let live = plan.live.as_deref().unwrap_or_default();
    let numbered = live
        .iter()
        .any(|column| numbered_beside(&column.name, live));
    if !plan.exists || !numbered || plan.schema.blocked.is_some() {
        return Ok(None);
    }
    let (log, _) = read_run_log(plan, destination).await?;
    let spec = &plan.spec;
    Ok(log
        .records
        .iter()
        .filter(|record| {
            record.source == spec.source_name
                && record.source_table == spec.source_table.name
                && record.numbered.is_some()
        })
        .max_by(|left, right| left.recorded_at.cmp(&right.recorded_at))
        .and_then(|record| record.numbered.clone()))
}

/// Whether `name` is a numbered name (`x_1`) whose base (`x`) is a column of `live` too, ignoring case.
fn numbered_beside(name: &str, live: &[LiveColumn]) -> bool {
    let Some((base, number)) = name.rsplit_once('_') else {
        return false;
    };
    !number.is_empty()
        && number.bytes().all(|digit| digit.is_ascii_digit())
        && live
            .iter()
            .any(|other| other.name.eq_ignore_ascii_case(base))
}

/// The reset a files source's merge writes when it creates the table: files it merged into an earlier table of the name no longer count.
pub(crate) fn merge_reset_record(spec: &TableSpec) -> RunRecord {
    event_record(
        &record_table(spec),
        StateEvent {
            event: RunEvent::Reset,
            cursor_before: None,
            cursor_after: None,
            state: None,
            note: "table created; files merged before no longer count".to_owned(),
            recorded_at: now_utc_text(),
        },
    )
}

/// Create the `_sqlload_runs` of each unblocked table with a `transform` whose plan found none, once per schema, before any load commits a record there; one `created` line each.
pub(crate) async fn create_run_logs(
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
) -> Result<Vec<String>, LoaderError> {
    let mut lines: Vec<String> = Vec::new();
    let mut created: BTreeSet<String> = BTreeSet::new();
    for table in plan.tables.iter().filter(|table| {
        let transform = table.spec.transform.as_ref();
        table.schema.blocked.is_none() && transform.is_some_and(|transform| transform.creates_log)
    }) {
        let spec = &table.spec;
        let name = sql_adapter(spec.warehouse).render_display_name(&runs_table(&spec.destination));
        if !created.insert(name.clone()) {
            continue;
        }
        let destination = destinations.get(&spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
        })?;
        ensure_runs_table(table, destination).await?;
        lines.push(format!("created  {name}  run log"));
    }
    Ok(lines)
}

/// Before a files source's merge creates its table: `_sqlload_runs` when it does not exist yet, and a reset, so records left from an earlier table of the name stop counting; written before the `CREATE TABLE`, so no table exists without it.
pub(crate) async fn reset_merge_log(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    ensure_merge_log(table, destination).await?;
    append_run(table, destination, &merge_reset_record(&table.spec)).await
}

/// Before a files source's merge writes: `_sqlload_runs` when it does not exist yet (checked first, so a role without CREATE on the schema loads once it exists).
pub(crate) async fn ensure_merge_log(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    let runs = runs_table(&table.spec.destination);
    if live_columns(destination, &runs).await?.is_none() {
        ensure_runs_table(table, destination).await?;
    }
    Ok(())
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

/// The file a group of a files merge or `delete_insert` by `replace_key` reads (it holds one), as its run record names it: `name|version`.
pub(crate) fn merged_file_text(table: &TablePlan, files: &[DataFile]) -> String {
    let location = table
        .spec
        .files
        .as_ref()
        .map(|files| files.location.to_string())
        .unwrap_or_default();
    files
        .iter()
        .map(|data| {
            format!(
                "{location}{}{VERSION_SEPARATOR}{}",
                data.file.relative_key,
                data.file.version()
            )
        })
        .collect::<Vec<String>>()
        .join(", ")
}

/// The name of the merged file `merged` (`name|version`).
pub(crate) fn merged_name(merged: &str) -> &str {
    merged
        .rsplit_once(VERSION_SEPARATOR)
        .map_or(merged, |(name, _)| name)
}

/// `error` naming the merged file (`name|version`) it happened in, as retryable as it was.
pub(crate) fn in_file(merged: &str, error: LoaderError) -> LoaderError {
    let name = merged_name(merged);
    let text = format!("{name}: {error}");
    match error.is_retryable() {
        true => LoaderError::Transient(text),
        false => LoaderError::Invalid(text),
    }
}

/// `earlier` (each `replace_key` value this run's commits of a files source's `delete_insert` into `table` inserted, with the file it came from, named below the source's location) with the key `values` of the commit of `file` (`name|version`) added; or why that commit cannot replace them: one of them came in an earlier commit of the same run, so this commit would delete that file's rows. A value must come whole in one file; [`listed_sources`] checks the files of earlier runs.
pub(crate) fn with_file_keys(
    table: &TablePlan,
    (file, values): (&str, &[String]),
    mut earlier: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    let location = table
        .spec
        .files
        .as_ref()
        .map(|files| files.location.to_string())
        .unwrap_or_default();
    let mut split: Vec<(&String, &String)> = Vec::new();
    for value in values {
        if let Some(first) = earlier.get(value) {
            split.push((value, first));
        }
    }
    let columns = table.spec.replace_key.join(", ");
    let this = merged_name(file);
    let this = this.strip_prefix(location.as_str()).unwrap_or(this);
    if let Some((value, first)) = split.first() {
        let more = match split.len() {
            1 => String::new(),
            count => format!(" (and {} more)", count - 1),
        };
        return Err(format!(
            "it holds the replace_key ({columns}) value ({value}){more}, which {first} also holds, loaded earlier in this run; each file replaces the rows of the values it holds, so it would delete the rows {first} loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows. Fix: if the files are parts of one export, put all the rows of each ({columns}) value in one file; if one restates the other, remove the older of {first} and {this} from the folder by hand, and set after_load = {{ move_to = ... }} (or \"delete\") so that each file leaves the folder once it loads and a file sent later replaces its values"
        ));
    }
    earlier.extend(values.iter().map(|value| (value.clone(), this.to_owned())));
    Ok(earlier)
}

/// Why a files table cannot load without the columns naming the file and version each row came from, by what its strategy uses them for: `append` skips the files loaded; `delete_insert` on a partition cursor (`guards_window`, not under `--reload` or `--start-cursor`) finds a file changed or added below its range; and `delete_insert` by `replace_key` finds the files a key value's rows come from (the file column alone). The table must write them, and an existing table hold them, except under `--reload`, which rebuilds it.
pub(crate) fn file_column_problem(
    request: &FilesPlanRequest<'_>,
    guards_window: bool,
) -> Option<String> {
    let spec = request.spec;
    let keyed = spec.strategy == WriteStrategy::DeleteInsert && !spec.replace_key.is_empty();
    let uses: &[(&str, &str, &str)] = match (spec.strategy, guards_window, keyed) {
        (WriteStrategy::Append, _, _) => &[
            (
                SOURCE_FILE_COLUMN,
                "tell which files are loaded and would load them all again",
                "skip files already loaded",
            ),
            (
                SOURCE_VERSION_COLUMN,
                "tell a file rewritten in place, or a new file under a loaded name, from one already loaded",
                "tell a file rewritten in place, or a new file under a loaded name, from one already loaded; stop excluding it",
            ),
        ],
        (_, true, _) => &[
            (
                SOURCE_FILE_COLUMN,
                "find a file changed or added below this load's range, which it skips, keeping the partition's old rows",
                "find a file changed or added below this load's range, which it would skip; stop excluding it",
            ),
            (
                SOURCE_VERSION_COLUMN,
                "find a file changed or added below this load's range, which it skips, keeping the partition's old rows",
                "find a file changed or added below this load's range, which it would skip; stop excluding it",
            ),
        ],
        (_, false, true) => &[(
            SOURCE_FILE_COLUMN,
            "find the files a replace_key value's rows come from, so a file holding a value another file of the folder holds would delete that file's rows",
            "find the files a replace_key value's rows come from; stop excluding it",
        )],
        _ => &[],
    };
    for (column, missing, needed) in uses {
        if !request.written.iter().any(|name| name == column) {
            return Some(format!(
                "{column} must be loaded: {} uses it to {needed}",
                spec.strategy
            ));
        }
        if request.live.is_some() && !live_has(request.live, column) && !request.options.reload {
            return Some(format!(
                "the table has no {column} column, so {} cannot {missing}; {}",
                spec.strategy,
                column_fix(spec.warehouse)
            ));
        }
    }
    None
}

/// Why the commit of `file` (`name|version`), a files source's `delete_insert` by `replace_key` into `table`, cannot replace the rows of its key values: some of them come from `sources` (other files the table's rows of those values came from, by name) that are still listed in the source's folder, so two files of the folder hold one value and which is current can't be told. A file that has left the folder (deleted, or moved by `after_load`) is replaced.
pub(crate) fn listed_sources(
    table: &TablePlan,
    file: &str,
    sources: &[String],
) -> Result<(), String> {
    let Some(source) = table.spec.files.as_ref() else {
        return Ok(());
    };
    let location = source.location.to_string();
    let listed: Vec<String> = source
        .table
        .files
        .iter()
        .map(|data| format!("{location}{}", data.file.relative_key))
        .collect();
    let held: Vec<&str> = sources
        .iter()
        .filter(|name| listed.contains(name))
        .map(|name| name.strip_prefix(location.as_str()).unwrap_or(name))
        .collect();
    let Some(first) = held.first() else {
        return Ok(());
    };
    let more = match held.len() {
        1 => String::new(),
        count => format!(" (and {} more)", count - 1),
    };
    let this = merged_name(file);
    let this = this.strip_prefix(location.as_str()).unwrap_or(this);
    let columns = table.spec.replace_key.join(", ");
    Err(format!(
        "the table holds rows of its replace_key ({columns}) values from {first}{more}, which is still in the folder; with two files of the folder holding one value, which of them is current can't be told, so replacing the value by {this} would delete the rows {first} loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows. Fix: if the files are parts of one export, put all the rows of each ({columns}) value in one file; if one restates the other, remove the older of {first} and {this} from the folder by hand, and set after_load = {{ move_to = ... }} (or \"delete\") so that each file leaves the folder once it loads and a file sent later replaces its values"
    ))
}

/// The files a merge already merged into `request`'s table at the version listed now, which it skips (one rewritten in place since is merged again, as the newest), and under `--reload` none, with every file the table's merges recorded instead (oldest first), whose order the reload keeps; or why a files source cannot load into it: only into a DuckDB or Postgres table, never with `state = "logged"`, nor `--reload` with `unnest` or of a `delete_insert` (one commit would lose the files' order); none while the table does not exist.
pub(crate) async fn merge_ledger(
    request: &FilesPlanRequest<'_>,
) -> Result<Result<(Vec<LoadedFile>, Vec<LoadedFile>), String>, LoaderError> {
    let spec = request.spec;
    let destination = match request.loaded {
        LoadedSource::Query(destination) => destination,
        LoadedSource::Recorded(_) => {
            return Ok(Err("merge is not supported for files targets".to_owned()));
        }
    };
    let what = match spec.strategy {
        WriteStrategy::Merge => "merge",
        _ => "load",
    };
    if !sql_adapter(spec.warehouse).merges_files() {
        return Ok(Err(format!(
            "{} from a files source loads into DuckDB and Postgres targets so far, not {}; use append, or delete_insert on a partition cursor",
            spec.strategy, spec.warehouse
        )));
    }
    if request.options.reload && spec.strategy != WriteStrategy::Merge {
        return Ok(Err(format!(
            "--reload would {what} every file in one commit and lose their order; drop the table and load again to {what} every file anew"
        )));
    }
    if request.options.reload && spec.unnest != Unnest::None {
        return Ok(Err(
            "--reload of a merge files table does not rebuild its child tables (unnest); drop the table and its child tables and load again to merge every file anew".to_owned(),
        ));
    }
    if spec.logged {
        return Ok(Err(format!(
            "state = \"logged\" does not apply to a files source's {}, which records each file it loads",
            spec.strategy
        )));
    }
    if let Some(cursor) = &spec.cursor {
        return Ok(Err(format!(
            "cursor_column {} does not apply to a files source's merge, which picks the files it has not merged from the run log, whatever their partition; remove it and set write_strategy = \"merge\", or keep it and use delete_insert",
            cursor.column
        )));
    }
    let merged = match request.live {
        None => return Ok(Ok((Vec::new(), Vec::new()))),
        Some(_) => merged_files(spec, destination).await?,
    };
    if request.options.reload {
        return Ok(merged.map(|merged| (Vec::new(), merged)));
    }
    let location = request.files.location.to_string();
    let listed: HashMap<String, String> = request
        .files
        .table
        .files
        .iter()
        .map(|data| {
            (
                format!("{location}{}", data.file.relative_key),
                data.file.version(),
            )
        })
        .collect();
    Ok(merged.map(|merged| (unchanged_files(merged, &listed), Vec::new())))
}

/// `files` in the order a merge `--reload` applies them: under `order = "path"`, by path, as names carry the order; otherwise those the table's merges recorded (`merged`, oldest first) in the order of their first record, matched by name and version, else, for a file read from `archive` (where `after_load` moved it, so it may have a new version), by name; then the others oldest modified first, as a load merges new files. A file in the source's folder that matches only by name is a newer file under a merged name, so it merges after the recorded ones, as a load would merge it.
pub(crate) fn reload_order(
    files: Vec<DataFile>,
    location: &str,
    merged: &[LoadedFile],
    (archive, order): (Option<&Location>, FilesOrder),
) -> Vec<DataFile> {
    if order == FilesOrder::Path {
        return merge_order(files, order);
    }
    let mut by_version: HashMap<(&str, &str), usize> = HashMap::new();
    let mut by_name: HashMap<&str, usize> = HashMap::new();
    for (index, file) in merged.iter().enumerate() {
        by_version
            .entry((file.name.as_str(), file.version.as_str()))
            .or_insert(index);
        by_name.entry(file.name.as_str()).or_insert(index);
    }
    let mut recorded: Vec<(usize, DataFile)> = Vec::new();
    let mut others: Vec<DataFile> = Vec::new();
    for data in files {
        let name = format!("{location}{}", data.file.relative_key);
        let version = data.file.version();
        let archived =
            archive.is_some_and(|archive| data.file.path.as_ref().starts_with(&archive.prefix));
        match by_version
            .get(&(name.as_str(), version.as_str()))
            .or_else(|| by_name.get(name.as_str()).filter(|_| archived))
        {
            Some(index) => recorded.push((*index, data)),
            None => others.push(data),
        }
    }
    recorded.sort_by_key(|(index, _)| *index);
    recorded
        .into_iter()
        .map(|(_, data)| data)
        .chain(merge_order(others, order))
        .collect()
}

/// Before a files source's merge or `delete_insert` by `replace_key` writes: `_sqlload_runs` when it does not exist yet; under a merge's `--reload` of an existing table, the table each file merges into, created beside it in its current columns, and the reset its swap commits first, stamped before any file's record, so the files merged into the old table stop counting.
pub(crate) async fn merge_rebuild(
    table: &TablePlan,
    destination: &Destination,
) -> Result<Option<MergeRebuild>, LoaderError> {
    ensure_merge_log(table, destination).await?;
    if table.schema.action != TableAction::Recreate || table.spec.strategy != WriteStrategy::Merge {
        return Ok(None);
    }
    let spec = &table.spec;
    let adapter = sql_adapter(spec.warehouse);
    let id = staging_id(
        &uuid::Uuid::new_v4().to_string(),
        chrono::Utc::now().naive_utc(),
    );
    let build = adapter.staging_relation_name(&spec.destination, &id);
    let create = adapter.render_create_build_table(
        &build,
        &spec.destination,
        (&table.schema.write_columns, table.live.as_deref(), None),
    );
    execute_ddl(destination, create).await?;
    Ok(Some(MergeRebuild {
        build,
        records: vec![merge_reset_record(spec)],
    }))
}

/// Swap a merge `--reload`'s table in, with its run records in the same transaction.
pub(crate) async fn swap_rebuild(
    table: &TablePlan,
    destination: &Destination,
    rebuild: &MergeRebuild,
) -> Result<(), LoaderError> {
    let spec = &table.spec;
    let adapter = sql_adapter(spec.warehouse);
    let runs = runs_table(&spec.destination);
    let mut statements = adapter.render_swap(&spec.destination, &rebuild.build, &[]);
    for record in &rebuild.records {
        let insert = insert_run(&runs, record, spec.warehouse).map_err(LoaderError::Invalid)?;
        statements = adapter.render_logged(statements, insert);
    }
    let _held = ddl_guard(destination).await;
    execute(destination, statements).await
}

/// A row of the loaded-files query: `file|version`, or just the file when `versioned` is false.
pub(crate) fn loaded_file(text: String, versioned: bool) -> LoadedFile {
    match (versioned, text.rsplit_once(VERSION_SEPARATOR)) {
        (true, Some((name, version))) => LoadedFile {
            name: name.to_owned(),
            version: version.to_owned(),
        },
        _ => LoadedFile {
            name: text,
            version: String::new(),
        },
    }
}
