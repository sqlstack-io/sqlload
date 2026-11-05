//! Snowflake's write session: Parquet parts staged and loaded by `COPY`, or rows sent without files.

use arrow_array::RecordBatch;

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::snowflake::_helpers::row_stream::{send_rows, settle_stream};
use crate::adapters::snowflake::constants::{EXTERNAL_STAGE_FOLDER, STAGE_FOLDER};
use crate::adapters::snowflake::main::copy::copy_staged::copy_staged;
use crate::adapters::snowflake::main::session::_run_statements::run_statements;
use crate::adapters::snowflake::main::stage::_stage_parquet::stage_parquet;
use crate::adapters::snowflake::main::stage::remove_staged::remove_staged;
use crate::adapters::snowflake::main::stage::route_reference::route_reference;
use crate::adapters::snowflake::models::{CopyStaged, SnowflakeWriter, StageRoute, StagedFile};
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::loader::errors::LoaderError;
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;

impl TableWriter for SnowflakeWriter {
    fn writes_directly(&self) -> bool {
        !matches!(
            self.route,
            Some(StageRoute::Streaming { .. } | StageRoute::Insert | StageRoute::StagingTable)
        )
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        if sends_rows(self.route.as_ref()) {
            return send_rows(self, target, &batch).await;
        }
        let parts = std::mem::take(&mut self.parts);
        let (parts, files) = encode(move || write_part(parts, &batch)).await?;
        self.parts = parts;
        for body in files {
            put_file(self, target, body).await?;
        }
        Ok(())
    }

    fn staged_bytes(&self) -> Option<usize> {
        (!sends_rows(self.route.as_ref())).then(|| self.parts.bytes())
    }

    async fn finish_files(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        let parts = std::mem::take(&mut self.parts);
        let (_, file) = encode(move || finish_part(parts)).await?;
        if let Some(body) = file {
            put_file(self, target, body).await?;
        }
        Ok(())
    }

    async fn load_batch(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        if !sends_rows(self.route.as_ref()) {
            copy_batch_files(self, target).await?;
        }
        Ok(())
    }

    fn next_batch(&mut self) {
        self.staged.clear();
        self.stage = None;
        self.parts = PartWriter::default();
    }

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        if let Some(stream) = &self.stream {
            settle_stream(stream).await?;
        }
        Ok(())
    }

    fn reopen_rows(&mut self) {
        self.stream = None;
    }

    async fn rollback_batch(&mut self) {}

    async fn remove_batch_files(&mut self, target: &WriteTarget<'_>) {
        if !self.staged.is_empty() {
            remove_files(self, target).await;
        }
    }
}

/// Whether rows go in without files.
fn sends_rows(route: Option<&StageRoute>) -> bool {
    matches!(
        route,
        Some(StageRoute::Streaming { .. } | StageRoute::Insert)
    )
}

/// Run Parquet encoding on a blocking thread, so tables loading at once encode in parallel.
async fn encode<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, LoaderError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| {
            LoaderError::Invalid(format!("the Parquet encoding thread failed: {error}"))
        })?
        .map_err(LoaderError::Invalid)
}

/// The stage this load's files go through, as SQL.
fn stage_reference(
    writer: &SnowflakeWriter,
    target: &WriteTarget<'_>,
) -> Result<String, LoaderError> {
    let route = writer
        .route
        .as_ref()
        .ok_or_else(|| LoaderError::Invalid("no stage was planned".to_owned()))?;
    route_reference(route, target.table, target.staging)
        .ok_or_else(|| LoaderError::Invalid("this route has no stage".to_owned()))
}

/// `sqlload/<load id>/<batch>/` on an internal stage, `_sqlload/<load id>/<batch>/` under an external stage's URL.
fn batch_folder(writer: &SnowflakeWriter, target: &WriteTarget<'_>) -> String {
    let root = match writer.external_folder {
        Some(_) => EXTERNAL_STAGE_FOLDER,
        None => STAGE_FOLDER,
    };
    format!("{root}/{}/{:05}/", target.load_id, target.batch)
}

async fn put_file(
    writer: &mut SnowflakeWriter,
    target: &WriteTarget<'_>,
    body: Vec<u8>,
) -> Result<(), LoaderError> {
    let name = format!("part-{:05}.parquet", writer.staged.len() + 1);
    let key = format!("{}{name}", batch_folder(writer, target));
    match &writer.external_folder {
        Some(folder) => write_file(folder, &key, bytes::Bytes::from(body)).await?,
        None => {
            let reference = stage_reference(writer, target)?;
            let file = StagedFile {
                stage: reference.trim_start_matches('@'),
                key: &key,
                body: &body,
            };
            let known = writer.stage.take();
            writer.stage = Some(stage_parquet(&writer.destination, known, file).await?);
        }
    }
    writer.staged.push(name);
    Ok(())
}

/// `COPY` the batch's staged files (straight into the table, committing, or into the staging table), then remove them.
async fn copy_batch_files(
    writer: &mut SnowflakeWriter,
    target: &WriteTarget<'_>,
) -> Result<(), LoaderError> {
    let stage = stage_reference(writer, target)?;
    let folder = batch_folder(writer, target);
    let holds_json = (target.direct || target.builds)
        && target
            .columns
            .iter()
            .any(|column| column.column_type == ColumnType::Json);
    let copies = copy_staged(&CopyStaged {
        table: target.staging.unwrap_or(target.table),
        stage: &stage,
        folder: &folder,
        names: &writer.staged,
        selected: holds_json.then_some(target.columns),
    });
    let statements: Vec<String> = match target.direct {
        true => [BEGIN_STATEMENT.to_owned()]
            .into_iter()
            .chain(copies)
            .chain([COMMIT_STATEMENT.to_owned()])
            .collect(),
        false => copies,
    };
    run_statements(&writer.destination, &statements).await?;
    remove_files(writer, target).await;
    Ok(())
}

/// Delete the batch's staged files, unless the stage keeps them; a failure only warns, and the next load removes them after a day.
async fn remove_files(writer: &mut SnowflakeWriter, target: &WriteTarget<'_>) {
    let removed = match (&writer.route, &writer.external_folder) {
        (Some(StageRoute::External(stage)), _) if stage.keep_staged_files => Ok(()),
        (_, Some(folder)) => {
            let keys: Vec<String> = writer
                .staged
                .iter()
                .map(|name| format!("{}{name}", batch_folder(writer, target)))
                .collect();
            delete_files(folder, &keys)
                .await
                .map_err(|error| error.to_string())
        }
        (_, None) => match stage_reference(writer, target) {
            Ok(stage) => remove_staged(&writer.destination, &stage, &batch_folder(writer, target))
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        },
    };
    if let Err(error) = removed {
        tracing::warn!(%error, load_id = %target.load_id, "cannot remove a batch's staged files; the next load removes them after a day");
    }
}
