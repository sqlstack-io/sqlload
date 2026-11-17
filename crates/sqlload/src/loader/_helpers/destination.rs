use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::contract::constants::{COMMIT_STATEMENT, ROLLBACK_STATEMENT};
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::clean_stage::clean_stage;
use crate::adapters::dispatch::main::session::drop_stale_staging::drop_stale_staging;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::constants::PARQUET_SUFFIX;
use crate::adapters::files::main::close_writer::close_writer;
use crate::adapters::files::main::commit_replaced::commit_replaced;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::main::open_writer::open_writer;
use crate::adapters::files::main::partitions_finished::partitions_finished;
use crate::adapters::files::main::swap_folder::swap_folder;
use crate::adapters::files::main::table_path::table_path;
use crate::adapters::files::main::write_loaded_files::write_loaded_files;
use crate::adapters::files::main::write_rows::write_rows;
use crate::adapters::files::models::{
    FilesDestination, FolderClaim, TableFiles, TablePath, TablePathRequest,
};
use crate::adapters::files::models::{FilesWriter, WriterRequest};
use crate::adapters::files::models::{PartitionFolder, PendingPhase, PendingRun, ReplacedFiles};
use crate::batch_plan::main::read_in_order::read_in_order;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::{CursorWindow, WindowPlan};
use crate::column_types::models::ColumnType;
use crate::commit_spool::main::held_rows::held_rows;
use crate::commit_spool::models::Spool;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::NANOSECOND_SCALE;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::files_source::main::text_charset::text_charset;
use crate::files_source::models::{DataFile, LoadedFile};
use crate::files_source::models::{FileColumn, SourceFormat, StoredRead};
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::MetadataRole;
use crate::loader::_helpers::publish::window_filter;
use crate::loader::_helpers::publish::{staged_columns, write_target};
use crate::loader::_helpers::resolve::display_name;
use crate::loader::_helpers::sources::{ReadPart, read_source};
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::loader::models::{CommitRange, Publisher};
use crate::loader::models::{Cuts, Pieces};
use crate::loader::models::{
    FilesSink, FilesSource, LiveFiles, LoadRun, ProjectPlan, TablePlan, TableSpec, UnreadSharing,
};
use crate::masking::main::mask_batch::mask_batch;
use crate::memory_budget::constants::{
    DUCKDB_COMMIT_PERCENT, DUCKDB_ROW_GROUP_PERCENT, DUCKDB_ROW_GROUP_ROWS, HOLDING_DIVISOR,
    LARGE_VALUE_BYTES, STAGED_COPIES,
};
use crate::memory_budget::main::large_values::large_values;
use crate::memory_budget::main::needed_budget::needed_budget;
use crate::memory_budget::main::table_memory::table_memory;
use crate::memory_budget::models::DuckDbShare;
use crate::number_text::main::memory_text::memory_text;
use crate::number_text::main::thousands::thousands;
use crate::postgres_source::main::end_snapshot::end_snapshot;
use crate::postgres_source::main::share_snapshot::share_snapshot;
use crate::postgres_source::models::ReadColumn;
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::project::models::WriteStrategy;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::run_log::main::unfinished_marker::unfinished_marker;
use crate::run_log::main::unfinished_recovery::unfinished_recovery;
use crate::run_log::models::UnfinishedMarker;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn, TableAction};
use crate::table_lease::constants::LEASE_REFRESH;
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::main::release_lease::release_lease;
use crate::table_lease::models::Lease;
use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

/// Before a load: drop the staging tables crashed loads left in the schemas it writes to. A failure only warns.
pub(crate) async fn clean_staging(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut schemas: BTreeMap<(String, Option<String>, String), &TablePlan> = BTreeMap::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none() && table.output.is_none())
    {
        let name = &table.spec.destination;
        schemas
            .entry((
                table.spec.target.clone(),
                name.database.clone(),
                name.schema.clone(),
            ))
            .or_insert(table);
    }
    for table in schemas.into_values() {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let name = &table.spec.destination;
        match drop_stale_staging(destination, name, table.spec.warehouse).await {
            Ok(0) => {}
            Ok(dropped) => {
                tracing::info!(dropped, schema = %name.schema, "dropped staging tables crashed loads left")
            }
            Err(error) => {
                tracing::warn!(%error, schema = %name.schema, "cannot clean up old staging tables")
            }
        }
    }
}

/// Remove the load folders failed runs left, over a day ago, on the stages this plan's tables use. A failure only warns.
pub(crate) async fn clean_stages(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let stage = (
            table.spec.target.as_str(),
            table.stage.as_ref(),
            &table.spec.destination,
        );
        let copies_files = table
            .spec
            .files
            .as_ref()
            .is_some_and(|files| files.copy_to_internal_stage);
        let (checked, cleaned) = clean_stage(destination, stage, copies_files, seen).await;
        seen = checked;
        for (stage, removed) in cleaned {
            match removed {
                Ok(0) => {}
                Ok(removed) => {
                    tracing::info!(removed, %stage, "removed load folders failed runs left on the stage")
                }
                Err(error) => tracing::warn!(%error, %stage, "cannot clean up the stage"),
            }
        }
    }
}

/// The table's folder and partition folder in `destination`; no I/O.
pub(crate) fn table_folder(
    spec: &TableSpec,
    destination: &FilesDestination,
) -> Result<TablePath, String> {
    table_path(&TablePathRequest {
        template: spec
            .destination_path
            .as_deref()
            .unwrap_or(DEFAULT_DESTINATION_PATH),
        connection: &destination.connection,
        store: &destination.store,
        source: &spec.source_name,
        schema: &spec.source_table.schema,
        table: &spec.source_table.name,
    })
}

/// The schema of the newest load's files in the newest partition, or why it cannot be read (too many footers, conflicting types).
pub(crate) async fn live_schema(
    folder: &OpenFolder,
    listed: &TableFiles,
    max_footer_reads: usize,
) -> Result<Result<LiveFiles, Vec<String>>, LoaderError> {
    let newest: Vec<StoredFile> = listed
        .partitions
        .last()
        .map(|partition| newest_load_files(&parquet_files(&partition.files)))
        .unwrap_or_default();
    if newest.len() > max_footer_reads {
        return Ok(Err(vec![format!(
            "the newest load in {} wrote {} files, more footers than max_footer_reads = {max_footer_reads} allows reading for its schema; raise max_footer_reads on the target",
            folder.location,
            newest.len()
        )]));
    }
    let footers = read_footers(folder, &newest).await?;
    let (columns, conflicts) = footer_columns(&footers);
    if !conflicts.is_empty() {
        return Ok(Err(conflicts));
    }
    let live: Option<Vec<LiveColumn>> = (!footers.is_empty()).then(|| {
        columns
            .into_iter()
            .map(|column| LiveColumn {
                name: column.name,
                column_type: column.column_type.unwrap_or_else(ColumnType::Native),
                fraction_digits: None,
            })
            .collect()
    });
    Ok(Ok(LiveFiles { footers, live }))
}

/// The Parquet files among `files`, whose footers hold their schema; CSV files have none.
pub(crate) fn parquet_files(files: &[StoredFile]) -> Vec<StoredFile> {
    files
        .iter()
        .filter(|file| {
            file.relative_key
                .to_ascii_lowercase()
                .ends_with(PARQUET_SUFFIX)
        })
        .cloned()
        .collect()
}

/// The plan with every selected table blocked that shares its destination table with another configured table (selected or not) when either sets `constant_columns` and the load could remove or overwrite the other's rows: either uses `replace` or `delete_insert`, this run reloads it, it keeps its position in the run log (which is per destination table), or it merges on a `unique_key` without a constant column.
pub(crate) fn block_shared_destinations(
    mut plan: ProjectPlan,
    configured: &[TableSpec],
) -> ProjectPlan {
    for table in plan
        .tables
        .iter_mut()
        .filter(|table| table.schema.blocked.is_none())
    {
        let others: Vec<&TableSpec> = configured
            .iter()
            .filter(|other| shares_destination(&table.spec, other))
            .collect();
        let opted_in = !table.spec.constants.is_empty()
            || others.iter().any(|other| !other.constants.is_empty());
        if others.is_empty() || !opted_in {
            continue;
        }
        let reasons = shared_problems(table, &others);
        if !reasons.is_empty() {
            table.schema = blocked_plan(reasons);
        }
    }
    plan
}

/// `specs` with `shared_destination` set on each that another of the `configured` tables loads into the same destination table as, or, when `unknown` (a source's tables could not be listed, so one of them might), on each with constant columns.
pub(crate) fn mark_shared_destinations(
    specs: Vec<TableSpec>,
    configured: &[TableSpec],
    unknown: bool,
) -> Vec<TableSpec> {
    specs
        .into_iter()
        .map(|spec| TableSpec {
            shared_destination: shared(&spec, configured)
                || (unknown && !spec.constants.is_empty()),
            ..spec
        })
        .collect()
}

/// A warning naming the `specs` whose cursors read within their constant values only because the `unresolved` sources' tables are unknown, if any do, with those tables' display names.
pub(crate) fn unknown_sharing(
    specs: &[TableSpec],
    configured: &[TableSpec],
    unresolved: &[String],
) -> Option<UnreadSharing> {
    let unknown: Vec<&TableSpec> = specs
        .iter()
        .filter(|spec| spec.shared_destination && !shared(spec, configured))
        .collect();
    let names: Vec<String> = unknown
        .iter()
        .map(|spec| {
            format!(
                "{}.{}",
                spec.source_name,
                spec.source_table.qualified_name()
            )
        })
        .collect();
    (!names.is_empty()).then(|| UnreadSharing {
        warning: format!(
            "{} could not be read, so {} may share a destination table with it: their incremental cursors read only rows with their own constant values",
            unresolved.join(", "),
            names.join(", ")
        ),
        sources: unresolved.to_vec(),
        tables: unknown
            .iter()
            .map(|spec| display_name(spec.warehouse, &spec.destination))
            .collect(),
    })
}

/// Whether another of the `configured` tables loads into `spec`'s destination table.
fn shared(spec: &TableSpec, configured: &[TableSpec]) -> bool {
    configured
        .iter()
        .any(|other| shares_destination(spec, other))
}

/// Whether `other` is a different table loading into `spec`'s destination table.
fn shares_destination(spec: &TableSpec, other: &TableSpec) -> bool {
    other.target == spec.target
        && other.destination == spec.destination
        && (other.source_name != spec.source_name
            || other.source_table.qualified_name() != spec.source_table.qualified_name())
}

/// Why `table` cannot load into a destination table `others` also load.
fn shared_problems(table: &TablePlan, others: &[&TableSpec]) -> Vec<String> {
    let spec = &table.spec;
    let names: Vec<String> = others
        .iter()
        .map(|other| {
            format!(
                "{}.{}",
                other.source_name,
                other.source_table.qualified_name()
            )
        })
        .collect();
    let place = format!(
        "{} is also loaded by {}, and constant_columns tell their rows apart",
        spec.destination.table,
        names.join(", ")
    );
    let mut problems: Vec<String> = Vec::new();
    let removes = std::iter::once(spec)
        .chain(others.iter().copied())
        .find(|each| {
            matches!(
                each.strategy,
                WriteStrategy::Replace | WriteStrategy::DeleteInsert
            )
        });
    if let Some(removing) = removes {
        problems.push(format!(
            "{place}; {}.{} uses write_strategy = \"{}\", which would remove the other tables' rows: use merge on a unique_key that includes a constant column, or append",
            removing.source_name,
            removing.source_table.qualified_name(),
            removing.strategy
        ));
    }
    if let Some(deleting) = std::iter::once(spec)
        .chain(others.iter().copied())
        .find(|each| each.deletes.is_some())
    {
        problems.push(format!(
            "{place}; {}.{} sets deletes, which would mark the other tables' rows deleted, as its source holds none of their keys: remove deletes",
            deleting.source_name,
            deleting.source_table.qualified_name()
        ));
    }
    if table.mode == WriteMode::Recreate || table.schema.action == TableAction::Recreate {
        problems.push(format!(
            "{place}; --reload would drop the other tables' rows"
        ));
    }
    if spec.logged {
        problems.push(format!(
            "{place}; state = \"logged\" keeps one position per destination table, which they would share"
        ));
    }
    let keyed = spec.unique_key.iter().any(|key| is_constant(spec, key));
    if spec.strategy == WriteStrategy::Merge && !keyed {
        problems.push(format!(
            "{place}; its unique_key ({}) has no constant column, so its merge could overwrite their rows: put a constant column in it",
            spec.unique_key.join(", ")
        ));
    }
    let merges = spec.strategy == WriteStrategy::Merge;
    let alike = others.iter().find(|other| !told_apart(spec, other, merges));
    if let Some(other) = alike.filter(|_| keyed || !merges) {
        problems.push(format!(
            "{place}; {}.{} has the same values for {}, so their rows and incremental cursors can't be told apart: give each table a different value",
            other.source_name,
            other.source_table.qualified_name(),
            match merges {
                true => "the constant columns in the unique_key",
                false => "every constant column",
            }
        ));
    }
    problems
}

/// Whether some constant column of `spec` (in its unique_key, when `in_key`) holds a different value in `other`.
fn told_apart(spec: &TableSpec, other: &TableSpec, in_key: bool) -> bool {
    spec.constants
        .iter()
        .filter(|(name, _, _)| !in_key || keyed_by(spec, name))
        .any(|(name, _, value)| differs(other, name, value))
}

/// Whether `name` is one of `spec`'s unique_key columns, ignoring case.
fn keyed_by(spec: &TableSpec, name: &str) -> bool {
    spec.unique_key
        .iter()
        .any(|key| key.eq_ignore_ascii_case(name))
}

/// Whether `other` has the constant column `name` with a value other than `value`.
fn differs(other: &TableSpec, name: &str, value: &str) -> bool {
    other
        .constants
        .iter()
        .any(|(theirs, _, their_value)| theirs.eq_ignore_ascii_case(name) && their_value != value)
}

/// Whether `name` is one of `spec`'s constant columns, ignoring case.
fn is_constant(spec: &TableSpec, name: &str) -> bool {
    spec.constants
        .iter()
        .any(|(constant, _, _)| constant.eq_ignore_ascii_case(name))
}

/// The folders files tables write to, whether or not they are selected, with the folder their files source (`files`) reads; warehouse tables and paths that do not resolve claim none.
pub(crate) fn folder_claims(
    specs: &[TableSpec],
    files: Option<&FilesSource>,
    destinations: &BTreeMap<String, Destination>,
) -> Vec<FolderClaim> {
    let mut claims: Vec<FolderClaim> = Vec::new();
    for spec in specs {
        let Some(target) = destinations.get(&spec.target).and_then(files_destination) else {
            continue;
        };
        let name = format!(
            "{}.{}",
            spec.source_name,
            spec.source_table.qualified_name()
        );
        if let Ok(path) = table_folder(spec, target) {
            claims.push(FolderClaim {
                id: name.clone(),
                name,
                folder: path.folder,
                template: spec.destination_path.clone().unwrap_or_default(),
                schema: spec.source_table.schema.clone(),
                source: files.map(|files| files.location.clone()),
            });
        }
    }
    claims
}

/// Refresh `lease` every `LEASE_REFRESH` until the returned guard is dropped, and the task doing it, which a release awaits so no refresh in flight extends the lease after it; a refresh that fails only warns, as the next commit checks the lease again before it deletes anything.
pub(crate) fn keep_in_background(
    folder: &OpenFolder,
    lease: &Lease,
) -> (tokio_util::sync::DropGuard, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let (stopped, folder, lease) = (stop.clone(), folder.clone(), lease.clone());
    let renewal = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stopped.cancelled() => return,
                () = tokio::time::sleep(LEASE_REFRESH) => {}
            }
            match keep_lease(&folder, &lease, lease_now()).await {
                Ok(Ok(())) => {}
                Ok(Err(lost)) => {
                    tracing::warn!(folder = %folder.location, "{lost}");
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, folder = %folder.location, "cannot refresh the table's lease; retrying");
                }
            }
        }
    });
    (stop.drop_guard(), renewal)
}

/// The table's own folder, where its lease lives: the live folder, also while the load writes a staged one.
pub(crate) fn leased_folder(sink: &FilesSink) -> &OpenFolder {
    sink.swap.as_ref().map_or(&sink.folder, |swap| &swap.live)
}

/// Release `lease` after the load, best effort: one left behind expires on its own.
pub(crate) async fn give_back(folder: &OpenFolder, lease: &Lease) {
    if let Err(error) = release_lease(folder, lease).await {
        tracing::warn!(%error, folder = %folder.location, "cannot release the table's lease; it expires on its own");
    }
}

/// What a files read reads, as the files spell it (the batches it reads are renamed to the loaded names after): each of `read_columns` in its read type, and those whose types were inferred from sampled values, not pinned, not read as text under `infer_types = false` and not filled from the path (`partition_types`) or the file.
pub(crate) fn files_read_as(
    spec: &TableSpec,
    read_columns: &[ReadColumn],
    partition_types: &[(String, ColumnType)],
) -> (Vec<DesiredColumn>, Vec<String>) {
    let spelled = |name: &str| spec.source_table.spelling(name).to_owned();
    let columns: Vec<DesiredColumn> = read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: spelled(&column.name),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let filled: Vec<&str> = partition_types
        .iter()
        .map(|(name, _)| name.as_str())
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
        .collect();
    let inferred: Vec<String> = read_columns
        .iter()
        .filter(|column| !spec.pinned.contains_key(&column.name))
        .filter(|column| spec.infer_types || column.output == ColumnType::Json)
        .map(|column| spelled(&column.name))
        .filter(|name| !filled.contains(&name.as_str()))
        .collect();
    (columns, inferred)
}

/// Whether unpinned column `name` of `column_type` reads as `text` under `infer_types = false`: a scalar SQLLoad's files or Sheets reader would type by inference (Parquet, a `transform`'s output and columns filled from the path or file keep their types).
pub(crate) fn read_as_text(spec: &TableSpec, name: &str, column_type: &ColumnType) -> bool {
    let reader_inferred = match (&spec.files, &spec.sheet) {
        (Some(files), _) => {
            files.encoding.format != SourceFormat::Parquet
                && files.transform.is_none()
                && !files
                    .partition_types
                    .iter()
                    .any(|(partition, _)| partition == spec.source_table.spelling(name))
        }
        (None, Some(_)) => true,
        (None, None) => false,
    };
    !spec.infer_types
        && reader_inferred
        && !matches!(column_type, ColumnType::Json | ColumnType::Array(_))
        && ![SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN]
            .contains(&spec.source_table.spelling(name))
}

/// The columns of `columns` SQLLoad's reader keeps at nanoseconds for `spec`'s warehouse: those the files store at nanoseconds, unless pinned or `live` holds fewer digits.
pub(crate) fn nanosecond_columns(
    spec: &TableSpec,
    columns: &[FileColumn],
    live: Option<&[LiveColumn]>,
) -> Vec<String> {
    match sql_adapter(spec.warehouse).keeps_nanoseconds() {
        true => columns
            .iter()
            .filter(|column| {
                matches!(
                    column.stored_read,
                    StoredRead::Nanos | StoredRead::Epoch(NANOSECOND_SCALE)
                )
            })
            .filter(|column| {
                let loaded = spec.source_table.loaded_as(&column.name);
                !spec.pinned.contains_key(loaded) && fewer_digits(live, loaded).is_none()
            })
            .map(|column| column.name.clone())
            .collect(),
        false => Vec::new(),
    }
}

/// A warning for each column the files store at nanoseconds but the live table holds at fewer digits, so the load cuts them; a column whose type is pinned is the user's choice and is not warned about.
pub(crate) fn truncated_nanoseconds(spec: &TableSpec, live: Option<&[LiveColumn]>) -> Vec<String> {
    let Some(files) = &spec.files else {
        return Vec::new();
    };
    files
        .table
        .columns
        .iter()
        .filter(|column| {
            matches!(
                column.stored_read,
                StoredRead::Nanos | StoredRead::Epoch(NANOSECOND_SCALE)
            ) && !spec
                .pinned
                .contains_key(spec.source_table.loaded_as(&column.name))
        })
        .filter_map(|column| truncated_warning(live, spec.source_table.loaded_as(&column.name)))
        .collect()
}

/// The warning for column `name` when the live column holds fewer than nanosecond digits.
fn truncated_warning(live: Option<&[LiveColumn]>, name: &str) -> Option<String> {
    let digits = fewer_digits(live, name)?;
    Some(format!(
        "{name}: nanoseconds in the files, but the column holds {digits} digits after the second, so they are truncated.\nRebuild the column at (9) (--reload) to keep them."
    ))
}

/// The digits after the second the live column `name` holds, when fewer than nanoseconds, so SQLLoad's reader cuts its values.
fn fewer_digits(live: Option<&[LiveColumn]>, name: &str) -> Option<i64> {
    live.into_iter()
        .flatten()
        .find(|existing| existing.name.eq_ignore_ascii_case(name))
        .and_then(|existing| existing.fraction_digits)
        .filter(|digits| *digits < i64::from(NANOSECOND_SCALE))
}

/// The replaced files a commit deletes: those of the partitions it wrote, and, at the last commit, every other file the load replaces (everything for `replace`, the window's partitions for `delete_insert`); never one already deleted.
pub(crate) fn commit_deletes(
    replaced: &ReplacedFiles,
    partition: Option<&PartitionFolder>,
    (written, last): (&BTreeSet<String>, bool),
    deleted: &BTreeSet<String>,
) -> Vec<String> {
    let folders: Vec<String> = match partition {
        Some(partition) => written
            .iter()
            .map(|value| format!("{}/", partition.name(value)))
            .collect(),
        None => Vec::new(),
    };
    let mut keys: Vec<String> = written
        .iter()
        .flat_map(|value| replaced.by_partition.get(value).into_iter().flatten())
        .cloned()
        .collect();
    for key in &replaced.always {
        let written_folder = folders.iter().any(|folder| key.starts_with(folder));
        if last || written_folder {
            keys.push(key.clone());
        }
    }
    keys.retain(|key| !deleted.contains(key));
    keys.sort();
    keys.dedup();
    keys
}

/// Once the load still holds the table's lease in `table_folder` (extending it), commit the files: record what they replace, then replace it.
pub(crate) async fn replace_files(
    (folder, (table_folder, lease)): (&OpenFolder, (&OpenFolder, &Lease)),
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), LoaderError> {
    keep_lease(table_folder, lease, lease_now())
        .await?
        .map_err(LoaderError::Invalid)?;
    Ok(commit_replaced(folder, file_id, (keys, run)).await?)
}

impl Publisher {
    /// Add a batch to the commit's files, starting them (and marking the commit as writing) on its first batch.
    pub(crate) async fn stage_files(&mut self, batch: RecordBatch) -> Result<(), LoaderError> {
        let writer = match self.files.as_mut().and_then(|sink| sink.writer.take()) {
            Some(writer) => writer,
            None => self.open_files().await?,
        };
        let mut writer = writer;
        let written = write_rows(&mut writer, batch).await.map(|_| ());
        if let Some(sink) = self.files.as_mut() {
            sink.writer = Some(writer);
        }
        Ok(written?)
    }

    /// A writer for the next commit's files, whose pending marker says they are being written.
    async fn open_files(&mut self) -> Result<FilesWriter, LoaderError> {
        let sink = self.sink()?;
        let file_id = format!("{}.{:05}", self.load_id, self.commits + 1);
        let spec = &self.table.spec;
        let writer = open_writer(WriterRequest {
            folder: sink.folder.clone(),
            path: &sink.output.path,
            cursor_column: spec.cursor.as_ref().map(|cursor| cursor.column.as_str()),
            columns: &self.table.schema.write_columns,
            compat: self.table.spec.compat,
            load_id: &self.load_id,
            file_id: &file_id,
            loaded_at: &self.loaded_at,
            encoding: &sink.output.encoding,
        })
        .map_err(LoaderError::Invalid)?;
        mark_pending(&sink.folder, &file_id, PendingPhase::Writing, (&[], None)).await?;
        Ok(writer)
    }

    /// Commit the files written since the last commit: store the open ones, record the files they replace (and a logged table's record) in the marker, which commits them; then write the record, delete the replaced files and the marker. The last commit of a `replace` with no rows still stores one empty file, so the table keeps its columns.
    pub(crate) async fn commit_files(&mut self, last: bool) -> Result<(), LoaderError> {
        let writer = match (
            self.files.as_mut().and_then(|sink| sink.writer.take()),
            last && self.at_end,
        ) {
            (Some(writer), _) => Some(writer),
            (None, true) if self.committed_rows == 0 => Some(self.open_files().await?),
            (None, _) => None,
        };
        let Some(mut writer) = writer else {
            return self.delete_leftovers(last).await;
        };
        let keep_schema = self.at_end && self.committed_rows == 0;
        close_writer(&mut writer, keep_schema).await?;
        let sink = self.sink()?;
        let mut keys = commit_deletes(
            &sink.replaced,
            sink.output.path.partition.as_ref(),
            (&writer.partitions, last),
            &sink.deleted,
        );
        keys.retain(|key| !writer.written.contains(key));
        let run = match &self.run {
            Some(_) => {
                let mut record = self.record(self.waiting_rows)?;
                record.files = writer.written.clone();
                Some(PendingRun {
                    prefix: runs_location(&self.sink()?.folder.location).prefix,
                    name: record_object_name(&record),
                    json: serde_json::to_string(&record)
                        .map_err(|error| LoaderError::Invalid(error.to_string()))?,
                })
            }
            None => None,
        };
        let sink = self.sink()?;
        let (folder, lease) = (sink.folder.clone(), sink.lease.clone());
        let table_folder = leased_folder(sink).clone();
        if !sink.reading.is_empty() {
            write_loaded_files(&folder, &writer, &sink.reading).await?;
        }
        replace_files(
            (&folder, (&table_folder, &lease)),
            &writer.file_id,
            (&keys, run.as_ref()),
        )
        .await?;
        if let Some(swap) = self.sink()?.swap.as_ref().filter(|_| last) {
            swap_folder((&swap.live, &lease), &folder, (&swap.replaced, &swap.held))
                .await?
                .map_err(|why| LoaderError::Transient(format!("{}: {why}", swap.live.location)))?;
        }
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
            sink.deleted.extend(writer.written.iter().cloned());
            sink.reading.clear();
        }
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        self.committed_state = self.state.clone();
        Ok(())
    }

    /// Whether a files target's `delete_insert` would end this commit inside a partition it wrote (a `batch_size` finer than the partition): the commit then waits for the rest of the partition, so a failure leaves its old files in place and none of the new ones.
    pub(crate) fn partition_unfinished(&self, range: &CommitRange) -> Result<bool, LoaderError> {
        let (
            Some(sink),
            WriteMode::DeleteInsert {
                cursor_column: Some(cursor_column),
                ..
            },
            CommitRange::Window { to: Some(to), .. },
        ) = (&self.files, &self.table.mode, range)
        else {
            return Ok(false);
        };
        let cursor_type = self
            .table
            .schema
            .write_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(cursor_column))
            .map(|column| &column.column_type);
        let (Some(partition), Some(writer), Some(cursor_type)) =
            (&sink.output.path.partition, &sink.writer, cursor_type)
        else {
            return Ok(false);
        };
        partitions_finished(&writer.partitions, partition.by, cursor_type, to)
            .map(|finished| !finished)
            .map_err(LoaderError::Invalid)
    }

    /// Note the source files about to be read, so a files target's commit records them as loaded.
    pub(crate) fn note_read(&mut self, files: &[DataFile]) {
        let (Some(sink), Some(source)) = (self.files.as_mut(), &self.table.spec.files) else {
            return;
        };
        let location = source.location.to_string();
        sink.reading.extend(files.iter().map(|data| LoadedFile {
            name: format!("{location}{}", data.file.relative_key),
            version: data.file.version(),
        }));
    }

    /// The columns grew while a files commit is being written: store its open files, and write later rows to new files holding the new columns too.
    pub(crate) async fn widen_files(&mut self) -> Result<(), LoaderError> {
        let Some(sink) = self.files.as_mut() else {
            return Ok(());
        };
        let Some(mut writer) = sink.writer.take() else {
            return Ok(());
        };
        if writer.encoding.single_file && !(writer.open.is_empty() && writer.written.is_empty()) {
            sink.writer = Some(writer);
            return Err(LoaderError::Invalid(format!(
                "{}: new columns appeared partway through the load, and single_file writes each file once, with one header; drop single_file, or have the source return every column from its first rows",
                self.table.spec.destination.table
            )));
        }
        let cursor_column = self
            .table
            .spec
            .cursor
            .as_ref()
            .map(|cursor| cursor.column.as_str());
        let opened = open_writer(WriterRequest {
            folder: sink.folder.clone(),
            path: &sink.output.path,
            cursor_column,
            columns: &self.table.schema.write_columns,
            compat: self.table.spec.compat,
            load_id: &self.load_id,
            file_id: &writer.file_id,
            loaded_at: &self.loaded_at,
            encoding: &sink.output.encoding,
        });
        let closed = close_writer(&mut writer, false).await.map(|_| ());
        let opened = opened.map(|opened| {
            writer.schema = opened.schema;
            writer.metadata = opened.metadata;
        });
        sink.writer = Some(writer);
        closed?;
        opened.map_err(LoaderError::Invalid)
    }

    /// Whether the last commit still has replaced files to delete that no partition written took with it.
    pub(crate) fn leftover_deletes(&self) -> bool {
        self.files.as_ref().is_some_and(|sink| {
            !commit_deletes(
                &sink.replaced,
                None,
                (&BTreeSet::new(), true),
                &sink.deleted,
            )
            .is_empty()
        })
    }

    /// At the last commit, delete the replaced files no commit deleted, under a marker of their own.
    async fn delete_leftovers(&mut self, last: bool) -> Result<(), LoaderError> {
        let sink = self.sink()?;
        let keys = commit_deletes(
            &sink.replaced,
            None,
            (&BTreeSet::new(), last),
            &sink.deleted,
        );
        if keys.is_empty() {
            return Ok(());
        }
        let file_id = format!("{}.{:05}", self.load_id, self.commits + 1);
        let (folder, lease) = (sink.folder.clone(), sink.lease.clone());
        let table_folder = leased_folder(sink).clone();
        replace_files((&folder, (&table_folder, &lease)), &file_id, (&keys, None)).await?;
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
        }
        Ok(())
    }

    /// After a failure: delete the files of the commit being written, best effort, keeping its `writing` marker so the next plan or load deletes any file a failed put still stored; a table staged beside its folder is deleted whole by the next load. Outside such a folder, `single_file` names are the table's own, so files already put stay: deleting them would delete the table's data.
    pub(crate) async fn abandon_files(&mut self) {
        let Some(sink) = self.files.as_mut() else {
            return;
        };
        if sink.swap.is_none() && sink.output.encoding.single_file {
            return;
        }
        let Some(writer) = sink.writer.take() else {
            return;
        };
        if let Err(error) = delete_files(&sink.folder, &writer.written).await {
            tracing::warn!(error = %error, folder = %sink.folder.location, "cannot delete the files of a failed load; the next plan or load deletes them");
        }
    }

    fn sink(&self) -> Result<&FilesSink, LoaderError> {
        self.files.as_ref().ok_or_else(|| {
            LoaderError::Invalid(format!(
                "{}: a files target table has no files planned",
                self.table.spec.destination.table
            ))
        })
    }
}

/// Why a files table's CSV files go through SQLLoad: `csv.encoding_check` reads the bytes of files read as `latin-1`, `cp1252` or `iso-8859-15`, which no destination's own read checks.
pub(crate) fn checked_encoding(spec: &TableSpec) -> Option<String> {
    let files = spec.files.as_ref()?;
    let csv = &files.encoding.csv;
    let checked = files.encoding.format == SourceFormat::Csv
        && csv.encoding_check
        && text_charset(&csv.encoding).is_ok_and(|charset| charset.checked().is_some());
    checked.then(|| {
        format!(
            "csv.encoding_check reads the bytes of each {} file, which no destination's own read checks; csv.encoding_check = false lets it read them",
            csv.encoding
        )
    })
}

impl Publisher {
    /// Read the table's cursor `windows` up to `limit` at once, each over its own Postgres connection in one shared snapshot and held in a local spool, and stage and commit each in window order as soon as it and every window before it are read; a failed window ends the load with only the windows before it committed, as a sequential read would leave it.
    pub(crate) async fn feed_windows(
        &mut self,
        reader: &SourceReader,
        (windows, limit, batch_bytes): (&WindowPlan, usize, Option<usize>),
    ) -> Result<(), LoaderError> {
        let SourceReader::Postgres(client, helpers) = reader else {
            return Err(LoaderError::Invalid(
                "batch_concurrency reads windows at once only from Postgres".to_owned(),
            ));
        };
        let connections: Vec<&Client> =
            std::iter::once(client).chain(helpers).take(limit).collect();
        share_snapshot(&connections).await?;
        let table = self.table.clone();
        let parent = table
            .spec
            .staging_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        let read = |index| {
            let window = (windows, index, connections.len());
            read_window(reader, &table, window, (&parent, batch_bytes))
        };
        let commit = async |held: HeldWindow| self.commit_window(held).await;
        read_in_order((windows.count, connections.len()), read, commit).await?;
        if let Err(error) = end_snapshot(&connections).await {
            tracing::warn!(%error, "ending the read-only snapshot of windows already committed failed");
        }
        Ok(())
    }

    /// Stage a window's held rows and commit them.
    async fn commit_window(&mut self, held: HeldWindow) -> Result<(), LoaderError> {
        let started = Instant::now();
        let HeldWindow {
            index,
            window,
            mut spool,
            read_for,
        } = held;
        for held in spool.take() {
            for file in &held.files {
                for batch in held_rows(file).map_err(LoaderError::Invalid)? {
                    let batch = batch.map_err(|error| {
                        LoaderError::Invalid(format!("cannot read held rows: {error}"))
                    })?;
                    self.stage_masked(batch).await?;
                }
            }
        }
        let range = CommitRange::Window {
            from: window.from,
            to: window.to,
        };
        self.cut(range).await?;
        tracing::debug!(
            table = %self.table.spec.destination.table,
            window = index,
            read_ms = read_for.as_millis(),
            commit_ms = started.elapsed().as_millis(),
            "window committed"
        );
        Ok(())
    }
}

/// One cursor window read ahead of its commit: its rows held in a spool, and how long reading them took.
struct HeldWindow {
    index: u64,
    window: CursorWindow,
    spool: Spool,
    read_for: Duration,
}

/// Read window `index` of `windows` over connection `index` modulo `connections` (the client, then its helpers), holding its rows, masked, in a spool made in `parent`.
async fn read_window(
    reader: &SourceReader,
    table: &TablePlan,
    (windows, index, connections): (&WindowPlan, u64, usize),
    (parent, batch_bytes): (&Path, Option<usize>),
) -> Result<HeldWindow, LoaderError> {
    let started = Instant::now();
    let window = window_at(windows, index).map_err(LoaderError::Invalid)?;
    let (filter, parameters) = window_filter(table, &window, index == 0);
    let connection = usize::try_from(index % connections.max(1) as u64).unwrap_or(0);
    let part = ReadPart {
        filter,
        parameters,
        batch_bytes,
        connection,
        ..ReadPart::default()
    };
    let mut spool = Spool::new(parent).map_err(LoaderError::Invalid)?;
    read_source(reader, table, part, async |batch| {
        let masked = mask_batch(batch, &table.spec.masked).map_err(LoaderError::Invalid)?;
        spool.hold(0, &masked).map_err(LoaderError::Invalid)
    })
    .await?;
    Ok(HeldWindow {
        index,
        window,
        spool,
        read_for: started.elapsed(),
    })
}

/// Stop the process at `point` when a crash test asks for it there (`SQLLOAD_TEST_CRASH_AT`); compiled into test builds only.
fn crash_point(point: &str) {
    #[cfg(feature = "container-tests")]
    if std::env::var("SQLLOAD_TEST_CRASH_AT").is_ok_and(|wanted| wanted == point) {
        std::process::abort();
    }
    #[cfg(not(feature = "container-tests"))]
    let _ = point;
}

/// The write mode of one commit: the planned one, with a `delete_insert` narrowed to the commit's window or partitions (its `replace_key` kept).
pub(in crate::loader) fn commit_mode(planned: &WriteMode, range: Option<CommitRange>) -> WriteMode {
    let narrowed = match range {
        Some(CommitRange::Window { from, to }) => (from, to, Vec::new()),
        Some(CommitRange::Values(values)) if !values.is_empty() => (None, None, values),
        _ => return planned.clone(),
    };
    match planned {
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            not_null_keys,
            ..
        } => WriteMode::DeleteInsert {
            cursor_column: cursor_column.clone(),
            from: narrowed.0,
            to: narrowed.1,
            values: narrowed.2,
            replace_key: replace_key.clone(),
            not_null_keys: not_null_keys.clone(),
        },
        planned => planned.clone(),
    }
}

/// The write mode of a piece: a `delete_insert` replaces only the rows of the `replace_key` values, or cursor values, its rows hold; its commit's range goes when the commit ends.
pub(in crate::loader) fn piece_mode(planned: &WriteMode) -> WriteMode {
    match planned {
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            not_null_keys,
            ..
        } => WriteMode::DeleteInsert {
            cursor_column: None,
            from: None,
            to: None,
            values: Vec::new(),
            replace_key: match replace_key.is_empty() {
                true => cursor_column.iter().cloned().collect(),
                false => replace_key.clone(),
            },
            not_null_keys: match replace_key.is_empty() {
                true => Vec::new(),
                false => not_null_keys.clone(),
            },
        },
        planned => planned.clone(),
    }
}

/// Whether the table's commits delete a cursor range (`delete_insert` with a cursor), so its last range commits even without rows: rows deleted upstream go too.
pub(in crate::loader) fn deletes_range(mode: &WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::DeleteInsert {
            cursor_column: Some(_),
            ..
        }
    )
}

/// Each row's value of `columns` in `batch`, as one text.
fn row_keys(batch: &RecordBatch, columns: &[String]) -> Result<Vec<String>, LoaderError> {
    let options = FormatOptions::default().with_null("\u{0}");
    let mut formatters: Vec<ArrayFormatter<'_>> = Vec::new();
    for name in columns {
        let column = batch.column_by_name(name).ok_or_else(|| {
            LoaderError::Invalid(format!("the rows have no column {name} to cut pieces by"))
        })?;
        let formatter = ArrayFormatter::try_new(column.as_ref(), &options)
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        formatters.push(formatter);
    }
    Ok((0..batch.num_rows())
        .map(|row| row_key(&formatters, row))
        .collect())
}

/// Row `row`'s value of the columns `formatters` show, as one text.
fn row_key(formatters: &[ArrayFormatter<'_>], row: usize) -> String {
    let values: Vec<String> = formatters
        .iter()
        .map(|formatter| formatter.value(row).to_string())
        .collect();
    values.join("\u{1f}")
}

impl Publisher {
    /// Write `batch` where the table's batches go. Under a DuckDB memory share, a batch with large values (see [`large_values`]) is written a piece at a time, each piece committed once the next row would take the commit past the share's commit bytes (a quarter of that while the watchdog holds the load), cut only where the table's commit may be cut (see [`Cuts`]); rows that must commit together past DuckDB's `memory_limit`, or a commit that can't be cut past it, stop the table, naming the fix.
    pub(in crate::loader) async fn write_rows(
        &mut self,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let share = self.writer.memory_share();
        if self.pieces.overflow {
            return self.count_overflow(&batch);
        }
        let large = share.and_then(|_| large_values(&batch));
        let (Some(share), Some(large)) = (share, large) else {
            return self.write_part(batch).await;
        };
        self.pieces.rows += batch.num_rows() as u64;
        self.pieces.large += large.iter().sum::<u64>();
        self.pieces.copies = match self.staging.is_some() && !self.builds {
            true => STAGED_COPIES,
            false => 1,
        };
        let copies = self.pieces.copies;
        let cuts = self.cuts();
        let keys = match &cuts {
            Cuts::Between(columns) => Some(row_keys(&batch, columns)?),
            _ => None,
        };
        let mut start = 0;
        let mut boundary: Option<usize> = None;
        let mut carried = self.pieces.held;
        let mut written = 0;
        for (row, bytes) in large.iter().enumerate() {
            let changed = match (&cuts, &keys) {
                (Cuts::Anywhere, _) => true,
                (Cuts::Between(_), Some(keys)) => self.pieces.last_key.as_ref() != Some(&keys[row]),
                _ => false,
            };
            if let Some(keys) = &keys {
                self.pieces.last_key = Some(keys[row].clone());
            }
            if changed {
                self.pieces.run = 0;
                if row > start || carried > 0 {
                    boundary = Some(row);
                }
            }
            self.pieces.run += bytes;
            if self.pieces.run * copies > share.commit_bytes {
                self.pieces.overflow = true;
                return match cuts {
                    Cuts::Anywhere => Err(self.too_large()),
                    _ => self.count_overflow(&batch.slice(row + 1, batch.num_rows() - row - 1)),
                };
            }
            written += bytes * copies;
            let held = carried + written;
            if held <= self.commit_bytes(share) {
                continue;
            }
            let Some(at) = boundary.take() else {
                continue;
            };
            if at > start {
                self.write_part(batch.slice(start, at - start)).await?;
            }
            let rest = (batch.num_rows() - at) as u64;
            self.batch_rows -= rest;
            self.commit_piece().await?;
            self.batch_rows = rest;
            carried = 0;
            written = large[at..=row].iter().sum::<u64>() * copies;
            start = at;
        }
        self.pieces.held = carried + written;
        self.report_held();
        match start {
            0 => self.write_part(batch).await,
            start if start < batch.num_rows() => {
                let rest = batch.num_rows() - start;
                self.write_part(batch.slice(start, rest)).await
            }
            _ => Ok(()),
        }
    }

    /// The most one piece holds: the share's commit bytes, or a quarter of them while the watchdog holds the load near its budget.
    fn commit_bytes(&self, share: DuckDbShare) -> u64 {
        let holding =
            table_memory().is_some_and(|watched| watched.guard.holding.load(Ordering::Relaxed));
        match holding {
            true => share.commit_bytes / HOLDING_DIVISOR,
            false => share.commit_bytes,
        }
    }

    /// Tell the watchdog what the table holds: its open piece's large values.
    fn report_held(&self) {
        if let Some(watched) = table_memory() {
            watched
                .table
                .held
                .store(self.pieces.held, Ordering::Relaxed);
        }
    }

    /// Where the table's commits may be cut: anywhere into a table this load builds or creates, or straight into the table; a `delete_insert` into a table with rows only between its values, so no value is half replaced.
    fn cuts(&self) -> Cuts {
        if self.children.is_some() {
            return Cuts::Never("a table with unnest's child tables commits whole");
        }
        match &self.table.mode {
            _ if self.builds || self.direct || !self.table.exists => Cuts::Anywhere,
            WriteMode::Append | WriteMode::Merge { .. } => Cuts::Anywhere,
            WriteMode::DeleteInsert { replace_key, .. } if !replace_key.is_empty() => {
                Cuts::Between(replace_key.clone())
            }
            WriteMode::DeleteInsert {
                cursor_column: Some(cursor),
                ..
            } => Cuts::Between(vec![cursor.clone()]),
            WriteMode::Changes(_) => Cuts::Never("captured changes commit whole"),
            WriteMode::MarkMissing(_) => Cuts::Never("a key scan commits whole"),
            _ => Cuts::Never("this write commits whole"),
        }
    }

    /// Why the table stops: rows that must commit together hold more large values than DuckDB's `memory_limit`.
    fn too_large(&self) -> LoaderError {
        let Some(share) = self.writer.memory_share() else {
            return LoaderError::Invalid(
                "a commit outgrew a memory share it has none of".to_owned(),
            );
        };
        let run = memory_text(self.pieces.run);
        let large = memory_text(LARGE_VALUE_BYTES as u64);
        let what = match self.cuts() {
            Cuts::Between(columns) => format!(
                "the rows of one {} value hold {run} of values over {large}, which a delete_insert replaces in one commit",
                columns.join(", ")
            ),
            Cuts::Never(why) => format!("one commit holds {run} of values over {large}, and {why}"),
            Cuts::Anywhere => format!("one row holds {run} of values over {large}"),
        };
        let staged = match self.pieces.copies {
            1 => String::new(),
            _ => " (through a staging table, they count three times)".to_owned(),
        };
        let needed =
            (self.pieces.run * self.pieces.copies).saturating_mul(100) / DUCKDB_COMMIT_PERCENT;
        LoaderError::Invalid(format!(
            "{}: {what}, past the {} of them DuckDB's {} memory_limit commits at once{staged}; {}",
            self.table.spec.destination.table,
            memory_text(share.commit_bytes),
            memory_text(share.memory_limit),
            needed_budget(needed)
        ))
    }

    /// After rows that must commit together outgrew DuckDB's share (see [`Pieces::overflow`]): count the rest of them in `batch`, writing nothing, so the stop names their whole size; the table stops at the first row of another value.
    fn count_overflow(&mut self, batch: &RecordBatch) -> Result<(), LoaderError> {
        let keys = match self.cuts() {
            Cuts::Between(columns) => Some(row_keys(batch, &columns)?),
            _ => None,
        };
        let large = large_values(batch).unwrap_or_else(|| vec![0; batch.num_rows()]);
        for (row, bytes) in large.iter().enumerate() {
            let other = keys
                .as_ref()
                .is_some_and(|keys| self.pieces.last_key.as_ref() != Some(&keys[row]));
            if other {
                return Err(self.too_large());
            }
            self.pieces.run += bytes;
        }
        Ok(())
    }

    /// The table's stop: rows that must commit together outgrew DuckDB's share, or the watchdog stopped it (an interrupt only reaches a statement running, so the table checks before each write and commit too).
    pub(in crate::loader) fn stopped(&self) -> Result<(), LoaderError> {
        if self.pieces.overflow {
            return Err(self.too_large());
        }
        let Some(watched) = table_memory() else {
            return Ok(());
        };
        let tripped = watched
            .table
            .tripped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        tripped.map_or(Ok(()), |why| Err(LoaderError::Invalid(why)))
    }

    /// Write `batch` where the table's batches go.
    async fn write_part(&mut self, batch: RecordBatch) -> Result<(), LoaderError> {
        self.stopped()?;
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.write_batch(&target, batch).await
    }

    /// Commit the rows written so far as a piece: a build table's or a direct append's transaction commits; staged rows commit into the table as a commit does, recording no run and keeping the load's position. An append's first piece marks the load unfinished, in the same transaction. Then DuckDB checkpoints once enough waits for it.
    async fn commit_piece(&mut self) -> Result<(), LoaderError> {
        self.stopped()?;
        let marker = self.marker();
        if let Some(marker) = &marker {
            execute_ddl(&self.destination, vec![marker.create.clone()]).await?;
        }
        match self.direct || self.builds {
            true => {
                if let Some(marker) = &marker {
                    execute(&self.destination, vec![marker.mark.clone()]).await?;
                }
                self.flush_batch().await?;
            }
            false => {
                self.flush_batch().await?;
                self.pieces.piece = true;
                let committed = Box::pin(self.commit(false)).await;
                self.pieces.piece = false;
                committed?;
            }
        }
        self.pieces.unfinished |= marker.is_some();
        self.pieces.held = 0;
        self.pieces.committed += 1;
        self.report_held();
        self.writer.checkpoint(false).await?;
        crash_point("after-first-piece");
        Ok(())
    }

    /// The recovery marker the next piece writes: an append's first piece, whose rows a rerun would not replace.
    fn marker(&self) -> Option<UnfinishedMarker> {
        let marks = !self.pieces.unfinished && self.table.mode == WriteMode::Append && !self.builds;
        marks.then(|| self.unfinished())
    }

    /// This load's recovery marker statements.
    fn unfinished(&self) -> UnfinishedMarker {
        let spec = &self.table.spec;
        let column = metadata_name(spec.compat, MetadataRole::LoadId);
        unfinished_marker(
            &spec.destination,
            (column, &self.load_id, &self.loaded_at),
            spec.warehouse,
        )
    }

    /// The statement a staged commit adds to its transaction for the marker: marking the load at its first piece, removing the mark at its last commit; `None` otherwise.
    pub(in crate::loader) fn marker_statement(&self, last: bool) -> Option<String> {
        match (self.pieces.piece, self.pieces.unfinished && last) {
            (true, _) => self.marker().map(|marker| marker.mark),
            (false, true) => Some(self.unfinished().unmark),
            (false, false) => None,
        }
    }

    /// Before the end of a direct append or a build: remove the marker inside the last transaction, so the table's last rows and the mark's removal commit together.
    pub(in crate::loader) async fn finish_pieces(&mut self) -> Result<(), LoaderError> {
        if self.pieces.unfinished && (self.direct || self.builds) {
            execute(&self.destination, vec![self.unfinished().unmark]).await?;
            self.pieces.unfinished = false;
        }
        Ok(())
    }

    /// After a failed load that marked itself unfinished: remove the rows its pieces committed, its run records and its mark, as the next load would.
    pub(in crate::loader) async fn abandon_pieces(&mut self) {
        if !self.pieces.unfinished {
            return;
        }
        self.writer.rollback_batch().await;
        let spec = &self.table.spec;
        let statements = unfinished_recovery(
            &spec.destination,
            (
                metadata_name(spec.compat, MetadataRole::LoadId),
                &self.load_id,
            ),
            (true, self.run.is_some(), spec.warehouse),
        );
        if let Err(error) = execute(&self.destination, statements).await {
            tracing::warn!(%error, table = %spec.destination.table, "cannot remove the rows of the failed load; the next load removes them");
        }
        self.pieces.unfinished = false;
    }

    /// Before a commit's transaction ends: under a DuckDB memory share, once the load wrote large values, check the table's last row group, which the next checkpoint rewrites whole, still fits DuckDB's `memory_limit` (see [`DUCKDB_ROW_GROUP_PERCENT`]); past it the commit rolls back and the table stops, naming its row width and the `memory_limit` it needs.
    pub(in crate::loader) async fn check_row_group(
        &mut self,
        into: &TableName,
    ) -> Result<(), LoaderError> {
        let Some(share) = self.writer.memory_share() else {
            return Ok(());
        };
        if self.pieces.large == 0 || self.pieces.rows == 0 {
            return Ok(());
        }
        let adapter = crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter(
            self.table.spec.warehouse,
        );
        let count = format!(
            "SELECT CAST(count(*) AS VARCHAR) FROM {}",
            adapter.render_qualified_name(into)
        );
        let rows: u64 = (single_text(&self.destination, count).await?)
            .map(|text| text.parse::<u64>())
            .transpose()
            .map_err(|error| LoaderError::Invalid(format!("the table's row count: {error}")))?
            .unwrap_or_default();
        let before = self.pieces.table_rows.unwrap_or(rows).min(rows);
        let group_rows = rows - before / DUCKDB_ROW_GROUP_ROWS * DUCKDB_ROW_GROUP_ROWS;
        let width = self.pieces.large / self.pieces.rows;
        let group = group_rows.saturating_mul(width);
        if group <= share.row_group_bytes {
            self.pieces.table_rows = Some(rows);
            return Ok(());
        }
        let needed = group.saturating_mul(100) / DUCKDB_ROW_GROUP_PERCENT;
        let full = (DUCKDB_ROW_GROUP_ROWS * width).saturating_mul(100) / DUCKDB_ROW_GROUP_PERCENT;
        Err(LoaderError::Invalid(format!(
            "{}: its last row group would hold {} rows of about {} each of values over {} ({}), more than DuckDB rewrites at a checkpoint within its {} memory_limit; {} (a row group fills at {} rows, and a table of rows this wide needs a memory_limit of {} by then)",
            self.table.spec.destination.table,
            thousands(group_rows),
            memory_text(width),
            memory_text(LARGE_VALUE_BYTES as u64),
            memory_text(group),
            memory_text(share.memory_limit),
            needed_budget(needed),
            thousands(DUCKDB_ROW_GROUP_ROWS),
            needed_budget(full).replace("raise memory_limit to ", "")
        )))
    }

    /// Run a staged commit's `statements`, with the recovery marker's statement in its transaction (see [`Self::marker_statement`]), checking the table's last row group (see [`Self::check_row_group`]) before the transaction ends.
    pub(in crate::loader) async fn execute_checked(
        &mut self,
        statements: Vec<String>,
        (into, last): (&TableName, bool),
    ) -> Result<(), LoaderError> {
        let adapter = crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter(
            self.table.spec.warehouse,
        );
        let mut statements = match self.marker_statement(last) {
            Some(marker) => adapter.render_logged(statements, marker),
            None => statements,
        };
        let checks = self.writer.memory_share().is_some()
            && self.pieces.large > 0
            && statements
                .last()
                .is_some_and(|last| last == COMMIT_STATEMENT);
        if !checks {
            return execute(&self.destination, statements).await;
        }
        let commit = statements.pop();
        execute(&self.destination, statements).await?;
        if let Err(error) = self.check_row_group(into).await {
            let _ = execute(&self.destination, vec![ROLLBACK_STATEMENT.to_owned()]).await;
            return Err(error);
        }
        execute(&self.destination, commit.into_iter().collect()).await
    }

    /// After a commit: a whole one moves the load's position on and starts the next commit's pieces afresh (the marker stays until the last); a piece leaves both.
    pub(in crate::loader) fn end_commit(&mut self, last: bool) {
        if self.pieces.piece {
            return;
        }
        self.committed_state = self.state.clone();
        self.pieces = Pieces {
            unfinished: self.pieces.unfinished && !last,
            table_rows: self.pieces.table_rows,
            rows: self.pieces.rows,
            large: self.pieces.large,
            ..Pieces::default()
        };
    }
}
