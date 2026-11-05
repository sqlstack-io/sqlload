use crate::batch_plan::constants::BATCH_BYTES;
use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::models::CursorWindow;
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::file_store::models::OpenFolder as StoreFolder;
use crate::files_destination::main::clear_pending::clear_pending;
use crate::files_destination::main::close_writer::close_writer;
use crate::files_destination::main::mark_pending::mark_pending;
use crate::files_destination::main::open_writer::open_writer;
use crate::files_destination::main::write_rows::write_rows;
use crate::files_destination::models::FilesWriter;
use crate::files_destination::models::PartitionFolder;
use crate::files_destination::models::PendingPhase;
use crate::files_destination::models::PendingRun;
use crate::files_destination::models::ReplacedFiles;
use crate::files_destination::models::WriterRequest;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::main::batch_columns::batch_columns;
use crate::files_source::models::DataFile;
use crate::files_source::models::{FilesRead, FilesTable, SourceFormat};
use crate::loader::_helpers::destination::append_rows;
use crate::loader::_helpers::destination::execute;
use crate::loader::_helpers::destination::stage_file;
use crate::loader::_helpers::destination::table_destination;
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::run_log::append_run;
use crate::loader::_helpers::run_log::ensure_runs_table;
use crate::loader::_helpers::run_log::fold_maximum;
use crate::loader::_helpers::sources::ReadPart;
use crate::loader::_helpers::sources::files_source_parts;
use crate::loader::_helpers::sources::read_source;
use crate::loader::_helpers::table_plan::source_filter;
use crate::loader::_helpers::table_plan::{replan_columns, schema_statements};
use crate::loader::constants::EXCLUSIVE_BOUND;
use crate::loader::constants::INCLUSIVE_BOUND;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::CommitRange;
use crate::loader::models::CommitRule;
use crate::loader::models::Destination;
use crate::loader::models::FilesSink;
use crate::loader::models::LoadRun;
use crate::loader::models::Publisher;
use crate::loader::models::RunEntry;
use crate::loader::models::SourceReader;
use crate::loader::models::TablePlan;
use crate::masking::main::mask_batch::mask_batch;
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;
use crate::run_log::constants::BEGIN_STATEMENT;
use crate::run_log::constants::COMMIT_STATEMENT;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::logged_statements::logged_statements;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunOutcome;
use crate::run_log::models::RunRecord;
use crate::schema_plan::constants::LOAD_ID_COLUMN;
use crate::schema_plan::constants::LOADED_AT_COLUMN;
use crate::schema_plan::models::ColumnOrigin;
use crate::schema_plan::models::DesiredColumn;
use crate::snowflake_destination::constants::STAGE_FOLDER;
use crate::snowflake_destination::main::copy_staged::copy_staged;
use crate::snowflake_destination::main::remove_staged::remove_staged;
use crate::store_paths::models::Location;
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::staging_changes::staging_changes;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::swap_table::swap_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::TableName;
use crate::write_sql::models::WriteMode;
use crate::write_sql::models::WriteStatementsRequest;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::TimestampMicrosecondArray;
use arrow_schema::Field;
use arrow_schema::Schema;
use bytes::Bytes;
use chrono::DateTime;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Whether `append` can write the batches straight into the table: not a logged table (its rows wait for a changed `ctx.state`), and no column the write must convert in SQL (JSON on Snowflake, which `PARSE_JSON` turns into a `VARIANT`; JSON and UUIDs on DuckDB, which arrive as text).
fn writes_directly(table: &TablePlan, destination: &Destination) -> bool {
    let converted = |column: &DesiredColumn| match destination {
        Destination::Snowflake(_) => column.column_type == ColumnType::Json,
        _ => matches!(column.column_type, ColumnType::Json | ColumnType::Uuid),
    };
    table.mode == WriteMode::Append
        && table.logged.is_none()
        && !table.schema.write_columns.iter().any(converted)
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
    columns.extend(
        table
            .schema
            .write_columns
            .iter()
            .filter(|column| column.origin == ColumnOrigin::Metadata)
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
        destination: &Destination,
        run: Option<RunEntry>,
    ) -> Result<Self, LoaderError> {
        let destination = table_destination(destination)?;
        if run.is_some() && !matches!(destination, Destination::Files(_)) {
            ensure_runs_table(&table, &destination).await?;
        }
        let load_id = uuid::Uuid::new_v4().to_string();
        let files = match (&destination, &table.output) {
            (Destination::Files(_), Some(output)) => {
                let folder = open_folder(&output.path.folder)?;
                let replaced = files_to_replace(&folder, &table, output).await?;
                Some(FilesSink {
                    output: output.clone(),
                    folder,
                    replaced,
                    writer: None,
                    deleted: BTreeSet::new(),
                })
            }
            (Destination::Files(_), None) => {
                return Err(LoaderError::Invalid(format!(
                    "{}: a files target table has no files planned",
                    table.spec.destination.table
                )));
            }
            _ => None,
        };
        let direct = files.is_none() && writes_directly(&table, &destination);
        let staging = match direct || files.is_some() {
            true => None,
            false => {
                let (staging, create) = staging_table(
                    &table.spec.destination,
                    &staged_columns(&table),
                    &load_id,
                    table.spec.warehouse,
                );
                execute(&destination, vec![create]).await?;
                Some(staging)
            }
        };
        let committed_state = run.as_ref().and_then(|run| run.record.state.clone());
        Ok(Self {
            at_end: matches!(table.mode, WriteMode::Replace | WriteMode::Recreate),
            table,
            destination,
            run,
            load_id,
            loaded_at: now_utc_text(),
            direct,
            staging,
            batch: 1,
            parts: PartWriter::default(),
            staged: Vec::new(),
            arrow_bytes: 0,
            batch_rows: 0,
            waiting_rows: 0,
            waiting_range: None,
            committed_rows: 0,
            commits: 0,
            maximum: None,
            state: committed_state.clone(),
            committed_state,
            open_transaction: false,
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
        match &self.destination {
            Destination::Snowflake(_) => {
                let parts = std::mem::take(&mut self.parts);
                let (parts, files) = write_part(parts, &batch).map_err(LoaderError::Invalid)?;
                self.parts = parts;
                for body in files {
                    self.put_file(body).await?;
                }
                Ok(())
            }
            _ => {
                let into = self.batch_table();
                if self.direct && !self.open_transaction {
                    execute(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
                    self.open_transaction = true;
                }
                append_rows(&self.destination, &into, batch).await
            }
        }
    }

    /// Whether the batch holds about 1 GB (Parquet staged on Snowflake, Arrow elsewhere), where a size-cut table may end it.
    pub(crate) fn full(&self) -> bool {
        let bytes = match &self.destination {
            Destination::Snowflake(_) => self.parts.bytes(),
            _ => self.arrow_bytes,
        };
        bytes >= BATCH_BYTES
    }

    /// A point where the rows so far may commit, replacing `range` under `delete_insert`: the batch is loaded (one `COPY` for all its files), then committed unless the table commits only at the end, or is logged and its `ctx.state` has not changed since the last commit (its rows then wait in the staging table).
    pub(crate) async fn cut(&mut self, range: CommitRange) -> Result<(), LoaderError> {
        self.flush_batch().await?;
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
        if self.direct || self.at_end || unchanged {
            return Ok(());
        }
        self.commit(false).await
    }

    /// The end of the read: the last batch is loaded and everything waiting commits (`replace` swaps the staging table in, `--reload` rebuilds the table from it); a logged table records `state`, where its function left `ctx.state`, even without rows.
    pub(crate) async fn finish(&mut self, state: Option<String>) -> Result<u64, LoaderError> {
        self.flush_batch().await?;
        if state.is_some() {
            self.state = state;
        }
        let pending = self.waiting_rows > 0
            || (self.waiting_range.is_some() && self.files.is_none())
            || self.at_end
            || self.leftover_deletes();
        match (self.direct, pending, &self.run) {
            (false, true, _) => self.commit(true).await?,
            (_, _, Some(_)) if self.commits == 0 || self.state != self.committed_state => {
                let record = self.record(0)?;
                append_run(&self.table, &self.destination, &record).await?;
            }
            _ => {}
        }
        Ok(self.committed_rows)
    }

    /// Drop the staging table and its swap table, and the staged files of a batch that did not load; best effort, after success or failure.
    pub(crate) async fn close(&mut self) {
        self.abandon_files().await;
        if self.open_transaction
            && let Err(error) = execute(&self.destination, vec!["ROLLBACK".to_owned()]).await
        {
            tracing::warn!(%error, "cannot roll back the open batch");
        }
        let warehouse = self.table.spec.warehouse;
        if let Some(staging) = &self.staging {
            let drops: Vec<String> = [staging.clone(), swap_table(staging)]
                .iter()
                .map(|table| format!("DROP TABLE IF EXISTS {}", qualified_name(table, warehouse)))
                .collect();
            if let Err(error) = execute(&self.destination, drops).await {
                tracing::warn!(%error, table = %staging.table, "cannot drop the staging table");
            }
        }
        if let Destination::Snowflake(snowflake) = &self.destination
            && !self.staged.is_empty()
            && let Err(error) = remove_staged(snowflake, &self.batch_folder()).await
        {
            tracing::warn!(%error, load_id = %self.load_id, "cannot remove the staged files; the next load removes them after a day");
        }
    }

    /// Where the batch's rows go: the table itself, or the staging table.
    fn batch_table(&self) -> TableName {
        match &self.staging {
            Some(staging) => staging.clone(),
            None => self.table.spec.destination.clone(),
        }
    }

    /// `sqlload/<load id>/<batch>/` on the user stage.
    fn batch_folder(&self) -> String {
        format!("{STAGE_FOLDER}/{}/{:05}/", self.load_id, self.batch)
    }

    async fn put_file(&mut self, body: Vec<u8>) -> Result<(), LoaderError> {
        let name = format!("part-{:05}.parquet", self.staged.len() + 1);
        let key = format!("{}{name}", self.batch_folder());
        stage_file(&self.destination, &key, &body).await?;
        self.staged.push(name);
        Ok(())
    }

    /// Load the batch where it goes: `COPY` its staged files (straight into the table, committing, or into the staging table) and remove them; DuckDB commits a straight batch's transaction. Then start the next batch.
    async fn flush_batch(&mut self) -> Result<(), LoaderError> {
        if let Destination::Snowflake(_) = &self.destination {
            let parts = std::mem::take(&mut self.parts);
            let (_, file) = finish_part(parts).map_err(LoaderError::Invalid)?;
            if let Some(body) = file {
                self.put_file(body).await?;
            }
        }
        if self.batch_rows == 0 {
            return Ok(());
        }
        if let Destination::Snowflake(snowflake) = &self.destination {
            let copies = copy_staged(&self.batch_table(), &self.batch_folder(), &self.staged);
            let statements = match self.direct {
                true => [BEGIN_STATEMENT.to_owned()]
                    .into_iter()
                    .chain(copies)
                    .chain([COMMIT_STATEMENT.to_owned()])
                    .collect(),
                false => copies,
            };
            execute(&self.destination, statements).await?;
            if let Err(error) = remove_staged(snowflake, &self.batch_folder()).await {
                tracing::warn!(%error, load_id = %self.load_id, "cannot remove a loaded batch's staged files; the next load removes them after a day");
            }
        }
        if self.open_transaction {
            execute(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            self.open_transaction = false;
        }
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
        self.staged.clear();
        self.parts = PartWriter::default();
        Ok(())
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
        let statements = write_statements(
            &WriteStatementsRequest {
                warehouse: spec.warehouse,
                table: &spec.destination,
                columns: &self.table.schema.write_columns,
                mode: &mode,
            },
            &staging,
        );
        let warehouse = spec.warehouse;
        let runs = runs_table(&spec.destination);
        let mut statements = match &self.run {
            Some(_) => {
                let record = self.record(self.waiting_rows)?;
                let insert = insert_run(&runs, &record, warehouse).map_err(LoaderError::Invalid)?;
                logged_statements(statements, insert, warehouse)
            }
            None => statements,
        };
        if !self.at_end {
            let empty = format!("DELETE FROM {}", qualified_name(&staging, warehouse));
            statements = logged_statements(statements, empty, warehouse);
        }
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
    /// Before a files batch: combine its Parquet files' footers with the columns known so far; when they add columns, plan the table for them (one read of the live columns) and apply the schema changes once, to the table and the staging table.
    async fn widen_for(
        &mut self,
        run: &LoadRun<'_>,
        folder: &StoreFolder,
        files: &[DataFile],
    ) -> Result<(), LoaderError> {
        let spec = &self.table.spec;
        let Some(source_files) = spec
            .files
            .as_ref()
            .filter(|source| source.encoding.format == SourceFormat::Parquet)
        else {
            return Ok(());
        };
        let replaced: Vec<&str> = source_files
            .partition_types
            .iter()
            .map(|(name, _)| name.as_str())
            .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN])
            .collect();
        let (columns, problems) =
            batch_columns(folder, files, &source_files.table.columns, &replaced).await?;
        if !problems.is_empty() {
            return Err(LoaderError::Invalid(problems.join("; ")));
        }
        if columns == source_files.table.columns {
            return Ok(());
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
        widened.listed = spec.listed;
        let (read_columns, live, schema) =
            replan_columns(&widened, &self.destination, run.options).await?;
        let read_as: Vec<DesiredColumn> = read_columns
            .iter()
            .map(|column| DesiredColumn {
                name: column.name.clone(),
                column_type: column.output.clone(),
                origin: ColumnOrigin::Source,
            })
            .collect();
        let replanned = TablePlan {
            spec: widened,
            files: self.table.files.clone().map(|read| FilesRead {
                columns: read_as,
                ..read
            }),
            read_columns,
            live,
            schema,
            ..self.table.clone()
        };
        let old = staged_columns(&self.table);
        if let Some(statements) = schema_statements(&replanned) {
            execute(&self.destination, statements).await?;
        }
        if let Some(staging) = &self.staging {
            let changes = staging_changes(
                staging,
                (&old, &staged_columns(&replanned)),
                replanned.spec.warehouse,
            );
            execute(&self.destination, changes).await?;
        }
        self.table = replanned;
        Ok(())
    }
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
    let publisher = Publisher::open(table.clone(), destination, None).await?;
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
    let fed = async {
        match &table.commits {
            CommitRule::Windows(windows) => {
                for window in windows {
                    let part = ReadPart {
                        filter: window_filter(table, window),
                        ..ReadPart::default()
                    };
                    read_source(reader, table, part, async |batch| {
                        stage(&mut publisher, batch).await
                    })
                    .await?;
                    publisher
                        .cut(CommitRange::Window {
                            from: window.from.clone(),
                            to: window.to.clone(),
                        })
                        .await?;
                }
            }
            CommitRule::FileGroups(groups) => {
                let planned = table.files.as_ref().map(|files| files.files.as_slice());
                for group in groups {
                    let files: Vec<DataFile> = group
                        .files
                        .iter()
                        .filter_map(|index| planned.and_then(|files| files.get(*index)).cloned())
                        .collect();
                    if let SourceReader::Files(folder) = reader {
                        publisher.widen_for(run, folder, &files).await?;
                    }
                    let part = ReadPart {
                        files: Some(files),
                        ..ReadPart::default()
                    };
                    let current = publisher.table.clone();
                    read_source(reader, &current, part, async |batch| {
                        stage(&mut publisher, batch).await
                    })
                    .await?;
                    let range = match group.values.is_empty() {
                        true => CommitRange::Planned,
                        false => CommitRange::Values(group.values.clone()),
                    };
                    publisher.cut(range).await?;
                }
            }
            CommitRule::Ordered { column } => {
                let part = ReadPart {
                    order_by: Some(format!("\"{}\"", column.replace('"', "\"\""))),
                    ..ReadPart::default()
                };
                let mut last: Option<String> = None;
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
                        publisher
                            .cut(CommitRange::Planned)
                            .await
                            .map_err(|error| error.to_string())?;
                        stage(&mut publisher, batch.slice(at, batch.num_rows() - at)).await?;
                    }
                    Ok(())
                })
                .await?;
            }
            CommitRule::Size => {
                read_source(reader, table, ReadPart::default(), async |batch| {
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
            CommitRule::Whole => {
                read_source(reader, table, ReadPart::default(), async |batch| {
                    stage(&mut publisher, batch).await
                })
                .await?;
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
        let keys = commit_deletes(
            &sink.replaced,
            sink.output.path.partition.as_ref(),
            (&writer.partitions, last),
            &sink.deleted,
        );
        let run = match &self.run {
            Some(_) => {
                let mut record = self.record(self.waiting_rows)?;
                record.files = writer.written.clone();
                Some(PendingRun {
                    prefix: runs_location(&self.sink()?.output.path.folder).prefix,
                    name: record_object_name(&record),
                    json: serde_json::to_string(&record)
                        .map_err(|error| LoaderError::Invalid(error.to_string()))?,
                })
            }
            None => None,
        };
        let sink = self.sink()?;
        let folder = sink.folder.clone();
        replace_files(&folder, &writer.file_id, (&keys, run.as_ref())).await?;
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
        }
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        self.committed_state = self.state.clone();
        Ok(())
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
        replace_files(&sink.folder.clone(), &file_id, (&keys, None)).await?;
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
        }
        Ok(())
    }

    /// After a failure: delete the files of the commit being written, best effort, keeping its `writing` marker so the next plan or load deletes any file a failed put still stored.
    pub(crate) async fn abandon_files(&mut self) {
        let Some(sink) = self.files.as_mut() else {
            return;
        };
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

/// Record the replaced files (and a logged table's run record) in the marker, which commits the files; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next plan or load.
async fn replace_files(
    folder: &OpenFolder,
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), LoaderError> {
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
