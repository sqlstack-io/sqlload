use crate::adapters::contract::constants::COMMIT_STATEMENT;
use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::models::TableShape;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded, UnloadedBatch};
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::dispatch::main::session::ddl_guard::ddl_guard;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::table_destination::table_destination;
use crate::adapters::dispatch::main::session::table_shape::table_shape;
use crate::adapters::dispatch::main::write::file_keys::file_keys;
use crate::adapters::dispatch::main::write::load_files::load_files;
use crate::adapters::dispatch::main::write::load_unloaded::load_unloaded;
use crate::adapters::dispatch::main::write::open_table_writer::open_table_writer;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::dispatch::models::Writer;
use crate::adapters::files::main::clear_pending::clear_pending;
use crate::adapters::files::main::close_writer::close_writer;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::main::open_writer::open_writer;
use crate::adapters::files::main::partitions_finished::partitions_finished;
use crate::adapters::files::main::swap_folder::swap_folder;
use crate::adapters::files::main::write_rows::write_rows;
use crate::adapters::files::models::FilesWriter;
use crate::adapters::files::models::PartitionFolder;
use crate::adapters::files::models::PendingPhase;
use crate::adapters::files::models::PendingRun;
use crate::adapters::files::models::ReplacedFiles;
use crate::adapters::files::models::WriterRequest;
use crate::batch_plan::constants::BATCH_BYTES;
use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::CursorWindow;
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::file_store::models::OpenFolder as StoreFolder;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::main::batch_columns::batch_columns;
use crate::files_source::main::batch_footers::batch_footers;
use crate::files_source::main::batch_headers::batch_headers;
use crate::files_source::main::key_columns::key_columns;
use crate::files_source::models::BatchFooters;
use crate::files_source::models::DataFile;
use crate::files_source::models::{FilesRead, FilesTable, SourceFormat, TextChecks};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::run_log::append_run;
use crate::loader::_helpers::run_log::ensure_runs_table;
use crate::loader::_helpers::run_log::fold_maximum;
use crate::loader::_helpers::sources::ReadPart;
use crate::loader::_helpers::sources::files_source_parts;
use crate::loader::_helpers::sources::nanosecond_columns;
use crate::loader::_helpers::sources::read_source;
use crate::loader::_helpers::sources::unload_source;
use crate::loader::_helpers::table_plan::source_filter;
use crate::loader::_helpers::table_plan::{replan_columns, schema_statements};
use crate::loader::constants::EXCLUSIVE_BOUND;
use crate::loader::constants::INCLUSIVE_BOUND;
use crate::loader::constants::SNAPSHOT_BEGIN;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::CommitRange;
use crate::loader::models::CommitRule;
use crate::loader::models::FilesSink;
use crate::loader::models::FolderSwap;
use crate::loader::models::Publisher;
use crate::loader::models::RunEntry;
use crate::loader::models::SourceReader;
use crate::loader::models::TablePlan;
use crate::loader::models::{LoadRun, PlanOptions};
use crate::masking::main::mask_batch::mask_batch;
use crate::postgres_source::errors::PostgresError;
use crate::publish_checks::main::run_checks::run_checks;
use crate::publish_checks::models::CheckedCommit;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunOutcome;
use crate::run_log::models::RunRecord;
use crate::schema_plan::constants::LOAD_ID_COLUMN;
use crate::schema_plan::constants::LOADED_AT_COLUMN;
use crate::schema_plan::models::ColumnOrigin;
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_source::models::UnloadedFiles;
use crate::store_paths::models::Location;
use crate::table_lease::constants::LEASE_REFRESH;
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::main::release_lease::release_lease;
use crate::table_lease::main::take_lease::take_lease;
use crate::table_lease::models::Lease;
use crate::time_text::main::now_utc_text::now_utc_text;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::TimestampMicrosecondArray;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::Field;
use arrow_schema::Schema;
use bytes::Bytes;
use chrono::DateTime;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::loader::_helpers::table_plan::{is_direct, pulls_unloaded};

/// Whether `append` can write the batches straight into the table: not a logged table (its rows wait for a changed `ctx.state`), and a writer that can (on Snowflake, a route loading files into the table itself).
fn writes_directly(table: &TablePlan, writer: &Writer) -> bool {
    table.mode == WriteMode::Append && table.logged.is_none() && writer.writes_directly()
}

/// Where the batches go, from the publisher's fields: `staging` when there is one, and `columns` as the staged batches carry them.
fn write_target<'a>(
    table: &'a TablePlan,
    staging: Option<&'a TableName>,
    columns: &'a [DesiredColumn],
    ((load_id, batch), (direct, builds)): ((&'a str, u64), (bool, bool)),
) -> WriteTarget<'a> {
    WriteTarget {
        table: &table.spec.destination,
        staging,
        columns,
        write_columns: &table.schema.write_columns,
        load_id,
        batch,
        direct,
        builds,
    }
}

/// The columns the staged batches carry: the read columns as the source gives them, then the `_sqlload_*` metadata.
pub(crate) fn staged_columns(table: &TablePlan) -> Vec<DesiredColumn> {
    let mut columns: Vec<DesiredColumn> = table
        .read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let read: Vec<&str> = table
        .read_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    columns.extend(
        table
            .schema
            .write_columns
            .iter()
            .filter(|column| {
                column.origin == ColumnOrigin::Metadata && !read.contains(&column.name.as_str())
            })
            .cloned(),
    );
    columns
}

/// `batch` with this load's `_sqlload_load_id` and `_sqlload_loaded_at` added, so the staged files carry them.
fn with_metadata(
    batch: &RecordBatch,
    load_id: &str,
    loaded_at_micros: i64,
) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let loaded_at =
        TimestampMicrosecondArray::from(vec![loaded_at_micros; rows]).with_timezone("UTC");
    fields.push(Arc::new(Field::new(
        LOAD_ID_COLUMN,
        arrow_schema::DataType::Utf8,
        true,
    )));
    fields.push(Arc::new(Field::new(
        LOADED_AT_COLUMN,
        loaded_at.data_type().clone(),
        true,
    )));
    columns.push(Arc::new(StringArray::from(vec![load_id; rows])));
    columns.push(Arc::new(loaded_at));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| error.to_string())
}

impl Publisher {
    /// A table's load ready for its first batch: its own connection, and (unless the batches go straight into the table) a fresh staging table.
    pub(crate) async fn open(
        table: TablePlan,
        (destination, options): (&Destination, &PlanOptions),
        run: Option<RunEntry>,
    ) -> Result<Self, LoaderError> {
        let destination = table_destination(destination)?;
        let writer = open_table_writer(&destination, table.stage.clone())?;
        let runs_sql = sql_adapter(table.spec.warehouse).runs_sql();
        if run.is_some() && runs_sql {
            ensure_runs_table(&table, &destination).await?;
        }
        let load_id = uuid::Uuid::new_v4().to_string();
        let files = match (runs_sql, &table.output) {
            (false, Some(output)) => {
                let live = open_folder(&output.path.folder)?;
                let staged = output
                    .path
                    .staged_folder(&table.mode, &load_id)
                    .map(|staged| open_folder(&staged))
                    .transpose()?;
                let lease = take_lease(&live, &load_id, lease_now())
                    .await?
                    .map_err(LoaderError::Invalid)?;
                let replaced = match files_to_replace(&live, &table, output).await {
                    Ok(replaced) => replaced,
                    Err(error) => {
                        give_back(&live, &lease).await;
                        return Err(error);
                    }
                };
                let keeper = keep_in_background(&live, &lease);
                let (folder, replaced, swap) = match staged {
                    Some(staged) => (
                        staged,
                        ReplacedFiles::default(),
                        Some(FolderSwap {
                            live,
                            replaced: replaced.keys(&BTreeSet::new()).into_iter().collect(),
                        }),
                    ),
                    None => (live, replaced, None),
                };
                Some(FilesSink {
                    output: output.clone(),
                    folder,
                    replaced,
                    writer: None,
                    deleted: BTreeSet::new(),
                    lease,
                    keeper: Some(keeper),
                    swap,
                })
            }
            (false, None) => {
                return Err(LoaderError::Invalid(format!(
                    "{}: a files target table has no files planned",
                    table.spec.destination.table
                )));
            }
            _ => None,
        };
        let direct =
            files.is_none() && table.spec.checks.is_none() && writes_directly(&table, &writer);
        let at_end = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
        let keeps_tables = sql_adapter(table.spec.warehouse).keeps_tables();
        let builds =
            at_end && files.is_none() && !keeps_tables && !sends_rows(table.stage.as_ref());
        let mut shape: Option<TableShape> = None;
        let staging = match direct || files.is_some() {
            true => None,
            false if builds => {
                let id = staging_id(&load_id, chrono::Utc::now().naive_utc());
                shape = match table.mode {
                    WriteMode::Replace => {
                        table_shape(&destination, &table.spec.destination).await?
                    }
                    _ => None,
                };
                let differing = table
                    .live
                    .as_deref()
                    .filter(|_| table.mode == WriteMode::Recreate);
                let adapter = sql_adapter(table.spec.warehouse);
                let build = adapter.staging_relation_name(&table.spec.destination, &id);
                let create = adapter.render_create_build_table(
                    &build,
                    &table.spec.destination,
                    (
                        &table.schema.write_columns,
                        differing,
                        shape.as_ref().map(|shape| shape.definition.as_str()),
                    ),
                );
                execute_ddl(&destination, create).await?;
                Some(build)
            }
            false => {
                let (staging, create) = sql_adapter(table.spec.warehouse)
                    .render_create_staging_table(
                        &table.spec.destination,
                        &staged_columns(&table),
                        &staging_id(&load_id, chrono::Utc::now().naive_utc()),
                    );
                execute_ddl(&destination, vec![create]).await?;
                Some(staging)
            }
        };
        let committed_state = run.as_ref().and_then(|run| run.record.state.clone());
        Ok(Self {
            writer,
            at_end,
            builds,
            indexes: shape.map(|shape| shape.indexes).unwrap_or_default(),
            table,
            destination,
            run,
            load_id,
            loaded_at: now_utc_text(),
            direct,
            staging,
            batch: 1,
            arrow_bytes: 0,
            batch_rows: 0,
            waiting_rows: 0,
            waiting_range: None,
            committed_rows: 0,
            commits: 0,
            maximum: None,
            state: committed_state.clone(),
            committed_state,
            batch_bytes: options.batch_bytes.unwrap_or(BATCH_BYTES),
            files,
        })
    }

    /// Stage one batch of the source's rows (masked, with the load's metadata added); `state` is a Python table's `ctx.state` when its last row crossed.
    pub(crate) async fn stage(
        &mut self,
        batch: RecordBatch,
        state: Option<String>,
    ) -> Result<(), LoaderError> {
        if state.is_some() {
            self.state = state;
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.maximum = fold_maximum(self.run.as_ref(), &batch, self.maximum.take())
            .map_err(LoaderError::Invalid)?;
        let masked = mask_batch(batch, &self.table.spec.masked).map_err(LoaderError::Invalid)?;
        if self.files.is_some() {
            self.batch_rows += masked.num_rows() as u64;
            self.arrow_bytes += masked.get_array_memory_size();
            return self.stage_files(masked).await;
        }
        let micros = DateTime::parse_from_rfc3339(&self.loaded_at)
            .map(|time| time.timestamp_micros())
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        let batch = with_metadata(&masked, &self.load_id, micros).map_err(LoaderError::Invalid)?;
        self.batch_rows += batch.num_rows() as u64;
        self.arrow_bytes += batch.get_array_memory_size();
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.write_batch(&target, batch).await
    }

    /// Whether the batch holds about 1 GB (Parquet staged on Snowflake, Arrow elsewhere), where a size-cut table may end it.
    pub(crate) fn full(&self) -> bool {
        let bytes = self.writer.staged_bytes().unwrap_or(self.arrow_bytes);
        bytes >= self.batch_bytes
    }

    /// A point where the rows so far may commit, replacing `range` under `delete_insert`: the batch is loaded (one `COPY` for all its files), then committed unless the table commits only at the end, or is logged and its `ctx.state` has not changed since the last commit (its rows then wait in the staging table).
    pub(crate) async fn cut(&mut self, range: CommitRange) -> Result<(), LoaderError> {
        self.flush_batch().await?;
        let unfinished = self.partition_unfinished(&range)?;
        self.waiting_range = Some(match (self.waiting_range.take(), range) {
            (Some(CommitRange::Window { from, .. }), CommitRange::Window { to, .. }) => {
                CommitRange::Window { from, to }
            }
            (Some(CommitRange::Values(mut values)), CommitRange::Values(more)) => {
                values.extend(more);
                CommitRange::Values(values)
            }
            (_, range) => range,
        });
        let unchanged = self.run.is_some() && self.state == self.committed_state;
        if self.direct || self.at_end || unchanged || unfinished {
            return Ok(());
        }
        self.commit(false).await
    }

    /// The end of the read: the last batch is loaded and everything waiting commits (`replace` swaps the staging table in, `--reload` rebuilds the table from it, and a `delete_insert` read of no rows still clears its range, since its rows were deleted upstream); a logged table records `state`, where its function left `ctx.state`, even without rows; a swapped-in or rebuilt table gets its description as its comment again (a table refilled in place keeps its own), where a failure only warns, as its rows are already in.
    pub(crate) async fn finish(&mut self, state: Option<String>) -> Result<u64, LoaderError> {
        self.flush_batch().await?;
        if state.is_some() {
            self.state = state;
        }
        let empty_range = self.commits == 0
            && matches!(self.table.mode, WriteMode::DeleteInsert { .. })
            && !matches!(self.table.commits, CommitRule::Windows(_));
        let pending = self.waiting_rows > 0
            || (self.waiting_range.is_some() && self.files.is_none())
            || self.at_end
            || empty_range
            || matches!(&self.table.mode, WriteMode::Changes(change) if change.truncated.is_some())
            || self.leftover_deletes();
        match (self.direct, pending, &self.run) {
            (false, true, _) => self.commit(true).await?,
            (_, _, Some(_)) if self.commits == 0 || self.state != self.committed_state => {
                let record = self.record(0)?;
                append_run(&self.table, &self.destination, &record).await?;
            }
            _ => {}
        }
        let spec = &self.table.spec;
        let adapter = sql_adapter(spec.warehouse);
        let comment = adapter
            .render_table_comment(&spec.destination, &spec.description)
            .filter(|_| self.at_end && self.files.is_none() && !adapter.keeps_tables());
        if let Some(comment) = comment
            && let Err(error) = execute(&self.destination, vec![comment]).await
        {
            tracing::warn!(table = %spec.destination.table, %error, "the swapped-in table kept no description: setting its comment failed");
        }
        Ok(self.committed_rows)
    }

    /// Drop the staging table and its swap table, and the staged files of a batch that did not load; best effort, after success or failure.
    pub(crate) async fn close(&mut self) {
        self.abandon_files().await;
        if let Some(sink) = self.files.as_mut() {
            if let Some((stop, renewal)) = sink.keeper.take() {
                drop(stop);
                if let Err(error) = renewal.await {
                    tracing::warn!(%error, "the lease's renewal task failed");
                }
            }
            give_back(leased_folder(sink), &sink.lease).await;
        }
        self.writer.rollback_batch().await;
        let warehouse = self.table.spec.warehouse;
        if let Some(staging) = &self.staging {
            let aside = TableName {
                table: format!("{}{SWAP_SUFFIX}", staging.table),
                ..staging.clone()
            };
            let drops: Vec<String> = [staging.clone(), aside]
                .iter()
                .map(|table| {
                    format!(
                        "DROP TABLE IF EXISTS {}",
                        sql_adapter(warehouse).render_qualified_name(table)
                    )
                })
                .collect();
            if let Err(error) = execute_ddl(&self.destination, drops).await {
                tracing::warn!(%error, table = %staging.table, "cannot drop the staging table");
            }
        }
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.remove_batch_files(&target).await;
    }

    /// The live columns a build table started with, which stay; `None` when the table holds staged rows. A `--reload` rebuilds with the current columns only.
    pub(crate) fn started_with(&self) -> Option<&[LiveColumn]> {
        self.builds.then(|| match self.table.mode {
            WriteMode::Replace => self.table.live.as_deref().unwrap_or_default(),
            _ => &[],
        })
    }

    /// Wait until every row sent without files is in the staging table.
    pub(crate) async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        self.writer.settle_rows().await
    }

    /// The row channel again after a schema change, which may have replaced the staging table and its pipe.
    pub(crate) fn reopen_rows(&mut self) {
        self.writer.reopen_rows();
    }

    /// Load the batch where it goes: `COPY` its staged files (straight into the table, committing, or into the staging table) and remove them; DuckDB commits a straight batch's transaction. Then start the next batch.
    async fn flush_batch(&mut self) -> Result<(), LoaderError> {
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.finish_files(&target).await?;
        if self.batch_rows == 0 {
            return Ok(());
        }
        self.writer.settle_rows().await?;
        self.writer.load_batch(&target).await?;
        match self.direct {
            true => {
                self.committed_rows += self.batch_rows;
                self.commits += 1;
            }
            false => self.waiting_rows += self.batch_rows,
        }
        self.batch += 1;
        self.batch_rows = 0;
        self.arrow_bytes = 0;
        self.writer.next_batch();
        Ok(())
    }

    /// Load a files batch by having the destination `COPY` the files itself, where the plan chose that; a batch it cannot load that way fails, never going through SQLLoad. `None` when the plan has SQLLoad read the files (`hmac` masking, or a destination that does not read files itself).
    pub(crate) async fn load_direct(
        &mut self,
        (files, footers, folder): (&[DataFile], &BatchFooters, Option<&OpenFolder>),
        range: &CommitRange,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(read) = &self.table.files else {
            return Ok(None);
        };
        if !is_direct(self.table.route.as_ref()) {
            return Ok(None);
        }
        let columns = staged_columns(&self.table);
        let mode = commit_mode(&self.table.mode, Some(range.clone()));
        let batch = DirectBatch {
            spec: &self.table.spec,
            read,
            write_columns: &self.table.schema.write_columns,
            target: write_target(
                &self.table,
                self.staging.as_ref(),
                &columns,
                ((&self.load_id, self.batch), (self.direct, self.builds)),
            ),
            loaded_at: &self.loaded_at,
            files,
            footers,
            mode: &mode,
            live: self.table.live.as_deref(),
            source_folder: folder,
        };
        match load_files(&mut self.writer, &batch).await? {
            Some(loaded) => Ok(Some(loaded)),
            None => Err(LoaderError::Invalid(format!(
                "{}: the plan has the destination load these files itself, but it cannot, and files never go through SQLLoad instead",
                self.table.spec.destination.table
            ))),
        }
    }

    /// Load one unload's `files` by having the destination read them itself, where the plan chose that; the destination must load them so.
    pub(crate) async fn load_unloaded(
        &mut self,
        files: &UnloadedFiles,
    ) -> Result<DirectLoaded, LoaderError> {
        let columns = staged_columns(&self.table);
        let batch = UnloadedBatch {
            files,
            read: &self.table.read_columns,
            write_columns: &self.table.schema.write_columns,
            target: write_target(
                &self.table,
                self.staging.as_ref(),
                &columns,
                ((&self.load_id, self.batch), (self.direct, self.builds)),
            ),
            loaded_at: &self.loaded_at,
        };
        let loaded = load_unloaded(&mut self.writer, &batch).await?.ok_or_else(|| {
            LoaderError::Invalid(format!(
                "{}: the plan has the destination load the unloaded files itself, but it cannot",
                self.table.spec.destination.table
            ))
        })?;
        self.note_direct(&loaded);
        Ok(loaded)
    }

    /// Pass `part` of the table's rows on: the destination loads the unloaded files itself where the plan chose that, otherwise SQLLoad stages the rows.
    async fn pass_part(
        &mut self,
        reader: &SourceReader,
        part: ReadPart,
    ) -> Result<(), LoaderError> {
        let table = self.table.clone();
        if pulls_unloaded(table.route.as_ref()) {
            unload_source(reader, &table, part, async |files| {
                self.load_unloaded(files).await
            })
            .await?;
            return Ok(());
        }
        read_source(reader, &table, part, async |batch| stage(self, batch).await).await?;
        Ok(())
    }

    /// Note what a direct batch did; a batch that is not yet committed waits for `cut`.
    pub(crate) fn note_direct(&mut self, loaded: &DirectLoaded) {
        match loaded {
            DirectLoaded::Committed(rows) => {
                self.committed_rows += rows;
                self.commits += 1;
            }
            DirectLoaded::Staged(rows) => self.waiting_rows += rows,
        }
    }

    /// Apply the write strategy to the waiting rows in one transaction, with a logged table's record and the emptying of the staging table for the next batches.
    async fn commit(&mut self, last: bool) -> Result<(), LoaderError> {
        if self.files.is_some() {
            self.waiting_range = None;
            return self.commit_files(last).await;
        }
        let Some(staging) = self.staging.clone() else {
            return Ok(());
        };
        let spec = &self.table.spec;
        let mode = commit_mode(&self.table.mode, self.waiting_range.take());
        let warehouse = spec.warehouse;
        let runs = runs_table(&spec.destination);
        let run_insert = match &self.run {
            Some(_) => {
                let record = self.record(self.waiting_rows)?;
                Some(insert_run(&runs, &record, warehouse).map_err(LoaderError::Invalid)?)
            }
            None => None,
        };
        let spec = &self.table.spec;
        let statements = sql_adapter(warehouse).render_commit(
            &WriteStatementsRequest {
                table: &spec.destination,
                columns: &self.table.schema.write_columns,
                mode: &mode,
                hashed: &[],
                built: self.builds.then_some(self.indexes.as_slice()),
            },
            &staging,
            (run_insert, self.at_end),
        );
        if let Some(checks) = &spec.checks {
            let commit = CheckedCommit {
                destination: &self.destination,
                warehouse,
                staging: &staging,
                table: &spec.destination,
                exists: self.table.exists,
                columns: &self.table.schema.write_columns,
                committed: self.committed_rows,
            };
            run_checks(&commit, checks).await?;
        }
        let _held = match self.at_end && !sql_adapter(warehouse).keeps_tables() {
            true => ddl_guard(&self.destination).await,
            false => None,
        };
        execute(&self.destination, statements).await?;
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        self.committed_state = self.state.clone();
        Ok(())
    }

    /// The logged table's record for a commit of `rows`: this load's id, the commit's number, the position so far and `ctx.state` when its last row crossed.
    pub(crate) fn record(&mut self, rows: u64) -> Result<RunRecord, LoaderError> {
        let Some(run) = &mut self.run else {
            return Err(LoaderError::Invalid(
                "only a logged table has run records".to_owned(),
            ));
        };
        run.record.state = self.state.clone();
        let mut record = run.finished(RunOutcome {
            run_id: self.load_id.clone(),
            recorded_at: now_utc_text(),
            rows,
            loaded_maximum: self.maximum.clone(),
            files: Vec::new(),
        });
        record.batch = Some(self.commits + 1);
        run.newest = Some(record.recorded_at.clone());
        Ok(record)
    }
}

impl Publisher {
    /// Before a files batch: read its Parquet files' footers once, or have the destination list a JSON batch's keys where it reads the files itself (its `COPY` would skip a key it is not given), and combine them with the columns known so far; when they add columns, plan the table for them (one read of the live columns) and apply the schema changes once, to the table and the staging table. Returns the footers, which reading the files reuses.
    async fn widen_for(
        &mut self,
        run: &LoadRun<'_>,
        folder: &StoreFolder,
        files: &[DataFile],
    ) -> Result<BatchFooters, LoaderError> {
        let spec = &self.table.spec;
        let Some(source_files) = spec.files.as_ref() else {
            return Ok(BatchFooters::default());
        };
        if source_files.encoding.record.is_some() {
            let direct = self.table.files.is_some() && is_direct(self.table.route.as_ref());
            return match (source_files.encoding.format, direct) {
                (SourceFormat::Csv, true) => Ok(BatchFooters {
                    headers: batch_headers(folder, files, &source_files.encoding).await?,
                    ..BatchFooters::default()
                }),
                _ => Ok(BatchFooters::default()),
            };
        }
        let replaced: Vec<&str> = source_files
            .partition_types
            .iter()
            .map(|(name, _)| name.as_str())
            .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
            .collect();
        let (columns, footers) = match source_files.encoding.format {
            SourceFormat::Parquet => {
                let footers = batch_footers(folder, files).await?;
                let (columns, problems) =
                    batch_columns(&footers, files, &source_files.table.columns, &replaced);
                if !problems.is_empty() {
                    return Err(LoaderError::Invalid(problems.join("; ")));
                }
                (columns, footers)
            }
            SourceFormat::Csv => {
                let (Some(read), true) = (&self.table.files, is_direct(self.table.route.as_ref()))
                else {
                    return Ok(BatchFooters::default());
                };
                let headers = batch_headers(folder, files, &source_files.encoding).await?;
                let mut found: Vec<(String, Option<ColumnType>)> = Vec::new();
                for name in headers.values().flatten() {
                    if !found.iter().any(|(seen, _)| seen == name) {
                        found.push((name.clone(), None));
                    }
                }
                let columns = key_columns(read, &source_files.table.columns, found)
                    .map_err(LoaderError::Invalid)?;
                (
                    columns,
                    BatchFooters {
                        headers,
                        ..BatchFooters::default()
                    },
                )
            }
            SourceFormat::Json | SourceFormat::Ndjson => {
                let (Some(read), true) = (&self.table.files, is_direct(self.table.route.as_ref()))
                else {
                    return Ok(BatchFooters::default());
                };
                let Some(found) = file_keys(&self.writer, spec, files).await? else {
                    return Ok(BatchFooters::default());
                };
                let columns = key_columns(read, &source_files.table.columns, found)
                    .map_err(LoaderError::Invalid)?;
                (columns, BatchFooters::default())
            }
        };
        if columns == source_files.table.columns {
            return Ok(footers);
        }
        let source = run.project.sources.get(&spec.source_name).ok_or_else(|| {
            LoaderError::Invalid(format!("source {:?} does not exist", spec.source_name))
        })?;
        let found = FilesTable {
            columns,
            ..source_files.table.clone()
        };
        let (source_table, files_source) = files_source_parts(
            &spec.source_name,
            source,
            spec.warehouse,
            (source_files.location.clone(), found),
        )?;
        let resolved = resolve_tables(run.project, &spec.source_name, &[source_table]);
        let mut widened = match (resolved.problems.first(), resolved.specs.into_iter().next()) {
            (None, Some(widened)) => widened,
            (problem, _) => {
                return Err(LoaderError::Invalid(problem.cloned().unwrap_or_else(
                    || "the files' new columns leave the table unselected".to_owned(),
                )));
            }
        };
        widened.files = Some(files_source);
        widened.approved = spec.approved;
        let replanned_columns = replan_columns(
            &widened,
            (&self.destination, self.table.live.as_deref()),
            None,
            run.options,
        )
        .await?;
        let (read_columns, live, schema) = (
            replanned_columns.read_columns,
            replanned_columns.live,
            replanned_columns.schema,
        );
        let read_as: Vec<DesiredColumn> = read_columns
            .iter()
            .map(|column| DesiredColumn {
                name: column.name.clone(),
                column_type: column.output.clone(),
                origin: ColumnOrigin::Source,
            })
            .collect();
        let inferred: Vec<String> = read_columns
            .iter()
            .map(|column| column.name.clone())
            .filter(|name| !widened.pinned.contains_key(name) && !replaced.contains(&name.as_str()))
            .collect();
        let nanos = widened.files.as_ref().map_or_else(Vec::new, |files| {
            nanosecond_columns(&widened, &files.table.columns, live.as_deref())
        });
        let replanned = TablePlan {
            files: self.table.files.clone().map(|read| FilesRead {
                columns: read_as,
                checks: TextChecks {
                    inferred,
                    ..read.checks
                },
                nanos,
                ..read
            }),
            spec: widened,
            read_columns,
            live,
            schema,
            ..self.table.clone()
        };
        self.settle_rows().await?;
        self.reopen_rows();
        let old = staged_columns(&self.table);
        if let Some(statements) = schema_statements(&replanned) {
            execute_ddl(&self.destination, statements).await?;
        }
        if let Some(staging) = &self.staging {
            let changes = sql_adapter(replanned.spec.warehouse).render_staging_changes(
                staging,
                (&old, &staged_columns(&replanned)),
                self.started_with(),
            );
            execute_ddl(&self.destination, changes).await?;
        }
        self.table = replanned;
        self.widen_files().await?;
        Ok(footers)
    }
}

/// Run `statement` on a Postgres source: its windows' reads share one snapshot, so a row moving between windows mid-load is read once.
async fn snapshot(reader: &SourceReader, statement: &str) -> Result<(), LoaderError> {
    match reader {
        SourceReader::Postgres(client, _) => Ok(client
            .batch_execute(statement)
            .await
            .map_err(PostgresError::from)?),
        _ => Ok(()),
    }
}

/// Whether the table's commits delete a cursor range (`delete_insert`), so its last range commits even without rows: rows deleted upstream go too.
fn deletes_range(table: &TablePlan) -> bool {
    matches!(table.mode, WriteMode::DeleteInsert { .. })
}

/// The cursor value at row `index` of `values` as cursor text, where an ordered read's commit ends.
fn cut_value(values: &dyn Array, index: usize, cursor_type: &ColumnType) -> Result<String, String> {
    let formatter = ArrayFormatter::try_new(values, &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    cursor_text(&formatter.value(index).to_string(), cursor_type)
}

/// The write mode of one commit: the planned one, with a `delete_insert` narrowed to the commit's window or partitions.
fn commit_mode(planned: &WriteMode, range: Option<CommitRange>) -> WriteMode {
    match (planned, range) {
        (WriteMode::DeleteInsert { cursor_column, .. }, Some(CommitRange::Window { from, to })) => {
            WriteMode::DeleteInsert {
                cursor_column: cursor_column.clone(),
                from,
                to,
                values: Vec::new(),
            }
        }
        (WriteMode::DeleteInsert { cursor_column, .. }, Some(CommitRange::Values(values)))
            if !values.is_empty() =>
        {
            WriteMode::DeleteInsert {
                cursor_column: cursor_column.clone(),
                from: None,
                to: None,
                values,
            }
        }
        (planned, _) => planned.clone(),
    }
}

/// Load a table's rows from `reader` commit by commit, as its plan cuts them; a failure stops the table there, keeping what committed before.
pub(crate) async fn publish_table(
    reader: &SourceReader,
    table: &TablePlan,
    (destination, run): (&Destination, &LoadRun<'_>),
) -> Result<u64, LoaderError> {
    let publisher = Publisher::open(table.clone(), (destination, run.options), None).await?;
    let published = feed(reader, table, publisher, run).await;
    let (mut publisher, result) = published;
    publisher.close().await;
    result
}

/// Pass the rows to `publisher`, cutting commits where the plan says; returns the publisher for its clean-up, and the rows committed.
async fn feed(
    reader: &SourceReader,
    table: &TablePlan,
    mut publisher: Publisher,
    run: &LoadRun<'_>,
) -> (Publisher, Result<u64, LoaderError>) {
    let batch_bytes = run.options.batch_bytes;
    let fed = async {
        match &table.commits {
            CommitRule::Windows(windows) => {
                snapshot(reader, SNAPSHOT_BEGIN).await?;
                for index in 0..windows.count {
                    let window = &window_at(windows, index).map_err(LoaderError::Invalid)?;
                    let part = ReadPart {
                        filter: window_filter(table, window),
                        batch_bytes,
                        ..ReadPart::default()
                    };
                    publisher.pass_part(reader, part).await?;
                    publisher
                        .cut(CommitRange::Window {
                            from: window.from.clone(),
                            to: window.to.clone(),
                        })
                        .await?;
                }
                snapshot(reader, COMMIT_STATEMENT).await?;
            }
            CommitRule::FileGroups(groups) => {
                let planned = table.files.as_ref().map(|files| files.files.as_slice());
                for group in groups {
                    let files: Vec<DataFile> = group
                        .files
                        .iter()
                        .filter_map(|index| planned.and_then(|files| files.get(*index)).cloned())
                        .collect();
                    let footers = match reader {
                        SourceReader::Files(folder) => {
                            publisher.widen_for(run, folder, &files).await?
                        }
                        _ => BatchFooters::default(),
                    };
                    let range = match group.values.is_empty() {
                        true => CommitRange::Planned,
                        false => CommitRange::Values(group.values.clone()),
                    };
                    let folder = match reader {
                        SourceReader::Files(folder) => Some(folder),
                        _ => None,
                    };
                    match publisher
                        .load_direct((&files, &footers, folder), &range)
                        .await?
                    {
                        Some(DirectLoaded::Committed(rows)) => {
                            publisher.note_direct(&DirectLoaded::Committed(rows));
                            continue;
                        }
                        Some(staged) => {
                            publisher.note_direct(&staged);
                            publisher.cut(range).await?;
                            continue;
                        }
                        None => {}
                    }
                    let part = ReadPart {
                        files: Some(files),
                        footers,
                        ..ReadPart::default()
                    };
                    let current = publisher.table.clone();
                    read_source(reader, &current, part, async |batch| {
                        stage(&mut publisher, batch).await
                    })
                    .await?;
                    publisher.cut(range).await?;
                }
            }
            CommitRule::Ordered {
                column,
                cursor_type,
            } => {
                let part = ReadPart {
                    order_by: Some(format!("\"{}\" NULLS FIRST", column.replace('"', "\"\""))),
                    batch_bytes,
                    ..ReadPart::default()
                };
                let mut last: Option<String> = None;
                let mut cut_from = table.window.as_ref().and_then(|window| window.from.clone());
                read_source(reader, table, part, async |batch| {
                    let values = batch
                        .column_by_name(column)
                        .cloned()
                        .ok_or_else(|| format!("the read has no cursor column {column}"))?;
                    let at = match publisher.full() {
                        true => cut_index(values.as_ref(), last.as_deref())?,
                        false => batch.num_rows(),
                    };
                    last = last_value(values.as_ref())?.or(last.take());
                    stage(&mut publisher, batch.slice(0, at)).await?;
                    if at < batch.num_rows() {
                        let cut_at = cut_value(values.as_ref(), at, cursor_type)?;
                        let range = CommitRange::Window {
                            from: cut_from.replace(cut_at.clone()),
                            to: Some(cut_at),
                        };
                        publisher
                            .cut(range)
                            .await
                            .map_err(|error| error.to_string())?;
                        stage(&mut publisher, batch.slice(at, batch.num_rows() - at)).await?;
                    }
                    Ok(())
                })
                .await?;
                if deletes_range(table) {
                    let to = table.window.as_ref().and_then(|window| window.to.clone());
                    publisher
                        .cut(CommitRange::Window { from: cut_from, to })
                        .await?;
                }
            }
            CommitRule::Size => {
                let part = ReadPart {
                    batch_bytes,
                    ..ReadPart::default()
                };
                match pulls_unloaded(table.route.as_ref()) {
                    true => publisher.pass_part(reader, part).await?,
                    false => {
                        read_source(reader, table, part, async |batch| {
                            stage(&mut publisher, batch).await?;
                            match publisher.full() {
                                true => publisher
                                    .cut(CommitRange::Planned)
                                    .await
                                    .map_err(|error| error.to_string()),
                                false => Ok(()),
                            }
                        })
                        .await?;
                    }
                }
            }
            CommitRule::Whole(_) => {
                let part = ReadPart {
                    batch_bytes,
                    ..ReadPart::default()
                };
                publisher.pass_part(reader, part).await?;
                if deletes_range(table) {
                    publisher.cut(CommitRange::Planned).await?;
                }
            }
        }
        publisher.finish(None).await
    }
    .await;
    (publisher, fed)
}

async fn stage(publisher: &mut Publisher, batch: RecordBatch) -> Result<(), String> {
    publisher
        .stage(batch, None)
        .await
        .map_err(|error| error.to_string())
}

/// The Postgres filter reading one cursor window.
fn window_filter(table: &TablePlan, window: &CursorWindow) -> Option<String> {
    let cursor = table.spec.cursor.as_ref()?;
    let expression = table
        .read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
        .map(|column| column.expression.clone())?;
    let operator = match window.inclusive {
        true => INCLUSIVE_BOUND,
        false => EXCLUSIVE_BOUND,
    };
    source_filter(
        &expression,
        window.from.as_deref().map(|from| (operator, from)),
        window.to.as_deref(),
    )
}

/// The replaced files a commit deletes: those of the partitions it wrote, and, at the last commit, every other file the load replaces (everything for `replace`, the window's partitions for `delete_insert`); never one already deleted.
fn commit_deletes(
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
        }
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        self.committed_state = self.state.clone();
        Ok(())
    }

    /// Whether a files target's `delete_insert` would end this commit inside a partition it wrote (a `batch_size` finer than the partition): the commit then waits for the rest of the partition, so a failure leaves its old files in place and none of the new ones.
    fn partition_unfinished(&self, range: &CommitRange) -> Result<bool, LoaderError> {
        let (
            Some(sink),
            WriteMode::DeleteInsert { cursor_column, .. },
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
            load_id: &self.load_id,
            file_id: &writer.file_id,
            loaded_at: &self.loaded_at,
            encoding: &sink.output.encoding,
        });
        let closed = close_writer(&mut writer, false).await.map(|_| ());
        let schema = opened.map(|opened| opened.schema);
        if let Ok(schema) = &schema {
            writer.schema = schema.clone();
        }
        sink.writer = Some(writer);
        closed?;
        schema.map(|_| ()).map_err(LoaderError::Invalid)
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

/// Once the load still holds the table's lease in `table_folder` (extending it), record the replaced files (and a logged table's run record) in the marker, which commits the files; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next plan or load.
async fn replace_files(
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

/// Refresh `lease` every `LEASE_REFRESH` until the returned guard is dropped, and the task doing it, which a release awaits so no refresh in flight extends the lease after it; a refresh that fails only warns, as the next commit checks the lease again before it deletes anything.
fn keep_in_background(
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
fn leased_folder(sink: &FilesSink) -> &OpenFolder {
    sink.swap.as_ref().map_or(&sink.folder, |swap| &swap.live)
}

/// Release `lease` after the load, best effort: one left behind expires on its own.
async fn give_back(folder: &OpenFolder, lease: &Lease) {
    if let Err(error) = release_lease(folder, lease).await {
        tracing::warn!(%error, folder = %folder.location, "cannot release the table's lease; it expires on its own");
    }
}
