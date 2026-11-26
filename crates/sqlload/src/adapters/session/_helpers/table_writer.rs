//! A table's write session through the caller's session: each batch goes into the staging table, by the way the warehouse takes rows from a caller's session.

use std::path::PathBuf;

use arrow_array::RecordBatch;

use crate::adapters::contract::models::{SessionRows, SessionStage, TableName, WriteTarget};
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::session::_helpers::blocking::on_session;
use crate::adapters::session::main::_run_statements::run_statements;
use crate::adapters::session::models::SessionWriter;
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::DesiredColumn;

impl TableWriter for SessionWriter {
    /// Never: every batch waits in the staging table, so each commit is one transaction on the table, in statements SQLLoad renders.
    fn writes_directly(&self) -> bool {
        false
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let Some(staging) = target.staging.cloned() else {
            return Err(LoaderError::Invalid(format!(
                "{}: a caller's session loads every batch through a staging table",
                target.table.table
            )));
        };
        let dialect = sql_adapter(self.destination.connection.warehouse);
        let columns = batch_columns(target, &batch);
        match dialect.session_rows() {
            SessionRows::CopyIn => {
                let (statement, data) = encode(move || {
                    dialect
                        .session_copy_in(&staging, &batch, &columns)
                        .unwrap_or_else(|| Err("the warehouse takes no COPY FROM STDIN".to_owned()))
                })
                .await?;
                on_session(&self.destination, move |session| {
                    session.copy_in(&statement, &data)
                })
                .await
            }
            SessionRows::LocalParquet => {
                let file = self.write_file(&batch, &columns).await?;
                let names: Vec<String> = columns.iter().map(|column| column.name.clone()).collect();
                let statement = dialect
                    .render_insert_local_parquet(&staging, &names, &file.to_string_lossy())
                    .ok_or_else(|| {
                        LoaderError::Invalid(
                            "the warehouse cannot read this machine's files".to_owned(),
                        )
                    })?;
                let loaded = run_statements(&self.destination, vec![statement]).await;
                remove_file(&file);
                loaded
            }
            SessionRows::CallerLoadsParquet => {
                let file = self.write_file(&batch, &columns).await?;
                let into = file.clone();
                let loaded = on_session(&self.destination, move |session| {
                    session.load_parquet(&staging, &into)
                })
                .await;
                remove_file(&file);
                loaded
            }
            SessionRows::StagedParquet => {
                let file = self.write_file(&batch, &columns).await?;
                let put = dialect.render_session_stage(SessionStage::Put {
                    load_id: target.load_id,
                    batch: target.batch,
                    file: &file.to_string_lossy(),
                });
                let name = file
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default();
                self.staged.push(name);
                let loaded = run_statements(&self.destination, put).await;
                remove_file(&file);
                loaded
            }
        }
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    /// `COPY` the batch's staged files into the staging table, then remove them, where the warehouse loads them together.
    async fn load_batch(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        if self.staged.is_empty() {
            return Ok(());
        }
        let dialect = sql_adapter(self.destination.connection.warehouse);
        let into: &TableName = target.staging.unwrap_or(target.table);
        let holds_json = target.builds
            && target.columns.iter().any(|column| {
                matches!(column.column_type, ColumnType::Json | ColumnType::Array(_))
            });
        let copies = dialect.render_session_stage(SessionStage::Copy {
            load_id: target.load_id,
            batch: target.batch,
            table: into,
            names: &self.staged,
            selected: holds_json.then_some(target.columns),
        });
        run_statements(&self.destination, copies).await?;
        self.remove_batch_files(target).await;
        Ok(())
    }

    fn next_batch(&mut self) {
        self.staged.clear();
    }

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        Ok(())
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {}

    /// Remove the batch's files from the stage; a failure only warns.
    async fn remove_batch_files(&mut self, target: &WriteTarget<'_>) {
        if self.staged.is_empty() {
            return;
        }
        let dialect = sql_adapter(self.destination.connection.warehouse);
        let remove = dialect.render_session_stage(SessionStage::Remove {
            load_id: target.load_id,
            batch: target.batch,
        });
        if let Err(error) = run_statements(&self.destination, remove).await {
            tracing::warn!(%error, load_id = %target.load_id, "cannot remove a batch's staged files");
        }
        self.staged.clear();
    }
}

impl SessionWriter {
    /// Write `batch` as the warehouse's Parquet file in the writer's folder, on a blocking thread.
    async fn write_file(
        &mut self,
        batch: &RecordBatch,
        columns: &[DesiredColumn],
    ) -> Result<PathBuf, LoaderError> {
        self.written += 1;
        let file = self
            .folder
            .path()
            .join(format!("part-{:05}.parquet", self.written));
        let dialect = sql_adapter(self.destination.connection.warehouse);
        let (batch, columns, path) = (batch.clone(), columns.to_vec(), file.clone());
        encode(move || {
            let body = dialect.session_parquet(&batch, &columns)?;
            std::fs::write(&path, body)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))
        })
        .await?;
        Ok(file)
    }
}

/// The staged columns `batch` carries, in its order.
fn batch_columns(target: &WriteTarget<'_>, batch: &RecordBatch) -> Vec<DesiredColumn> {
    let schema = batch.schema();
    let mut columns: Vec<DesiredColumn> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.extend(
            target
                .columns
                .iter()
                .find(|column| column.name == *field.name())
                .cloned(),
        );
    }
    columns
}

/// Run encoding on a blocking thread, so tables loading at once encode in parallel.
async fn encode<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, LoaderError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| LoaderError::Invalid(format!("the encoding thread failed: {error}")))?
        .map_err(LoaderError::Invalid)
}

/// Delete a file the session has loaded; one left behind goes with the writer's folder.
fn remove_file(file: &PathBuf) {
    let _ = std::fs::remove_file(file);
}
