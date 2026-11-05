use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{Field, Schema};
use chrono::DateTime;

use crate::batch_plan::constants::BATCH_BYTES;
use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::loader::_helpers::destination::{append_rows, execute, stage_file, table_destination};
use crate::loader::_helpers::run_log::{append_run, ensure_runs_table, fold_maximum};
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::{CommitRange, Destination, FilesSink, Publisher, RunEntry, TablePlan};
use crate::masking::main::mask_batch::mask_batch;
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;
use crate::run_log::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::logged_statements::logged_statements;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::{RunOutcome, RunRecord};
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::snowflake_destination::constants::STAGE_FOLDER;
use crate::snowflake_destination::main::copy_staged::copy_staged;
use crate::snowflake_destination::main::remove_staged::remove_staged;
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::swap_table::swap_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::{TableName, WriteMode, WriteStatementsRequest};

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
        if run.is_some() {
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
