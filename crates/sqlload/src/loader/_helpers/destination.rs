use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::clean_stage::clean_stage;
use crate::adapters::dispatch::main::session::drop_stale_staging::drop_stale_staging;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::constants::PARQUET_SUFFIX;
use crate::adapters::files::main::clear_pending::clear_pending;
use crate::adapters::files::main::close_writer::close_writer;
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
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::NANOSECOND_SCALE;
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::files_source::models::{DataFile, LoadedFile};
use crate::files_source::models::{FileColumn, StoredRead};
use crate::loader::_helpers::resolve::display_name;
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::errors::LoaderError;
use crate::loader::models::{CommitRange, Publisher};
use crate::loader::models::{
    FilesSink, FilesSource, LiveFiles, LoadRun, ProjectPlan, TablePlan, TableSpec, UnreadSharing,
};
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::project::models::WriteStrategy;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::schema_plan::models::{LiveColumn, TableAction};
use crate::store_paths::models::Location;
use crate::table_lease::constants::LEASE_REFRESH;
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::main::release_lease::release_lease;
use crate::table_lease::models::Lease;
use arrow_array::RecordBatch;
use bytes::Bytes;
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

/// Once the load still holds the table's lease in `table_folder` (extending it), record the replaced files (and a logged table's run record) in the marker, which commits the files; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next plan or load.
pub(crate) async fn replace_files(
    (folder, (table_folder, lease)): (&OpenFolder, (&OpenFolder, &Lease)),
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), LoaderError> {
    keep_lease(table_folder, lease, lease_now())
        .await?
        .map_err(LoaderError::Invalid)?;
    mark_pending(folder, file_id, PendingPhase::Replacing, (keys, run)).await?;
    if let Some(run) = run {
        let records = OpenFolder {
            store: folder.store.clone(),
            location: Location {
                store: folder.location.store.clone(),
                prefix: run.prefix.clone(),
            },
        };
        write_file(&records, &run.name, Bytes::from(run.json.clone())).await?;
    }
    delete_files(folder, keys).await?;
    Ok(clear_pending(folder, file_id).await?)
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
            write_loaded_files(&folder, &writer.file_id, &sink.reading).await?;
        }
        replace_files(
            (&folder, (&table_folder, &lease)),
            &writer.file_id,
            (&keys, run.as_ref()),
        )
        .await?;
        if let Some(swap) = self.sink()?.swap.as_ref().filter(|_| last) {
            swap_folder(&swap.live, &folder, &swap.replaced).await?;
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
