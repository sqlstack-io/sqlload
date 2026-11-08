//! Snowflake's write session: Parquet parts staged and loaded by `COPY`, or rows sent without files.

use arrow_array::RecordBatch;

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::snowflake::_helpers::write::row_stream::settle_stream;
use crate::adapters::snowflake::constants::{
    CONCURRENT_UPLOADS, EXTERNAL_STAGE_FOLDER, STAGE_FOLDER,
};
use crate::adapters::snowflake::main::copy::_copy_staged::copy_staged;
use crate::adapters::snowflake::main::session::_run_statements::run_statements;
use crate::adapters::snowflake::main::stage::_remove_staged::remove_staged;
use crate::adapters::snowflake::main::stage::_route_reference::route_reference;
use crate::adapters::snowflake::main::stage::_sends_rows::sends_rows;
use crate::adapters::snowflake::main::stage::_stage_parquet::stage_parquet;
use crate::adapters::snowflake::models::{CopyStaged, SnowflakeWriter, StageRoute, StagedFile};
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::loader::errors::LoaderError;
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;
use crate::row_batches::main::normalise_batch::normalise_batch;

impl TableWriter for SnowflakeWriter {
    fn writes_directly(&self) -> bool {
        !matches!(
            self.route,
            Some(StageRoute::Streaming | StageRoute::Insert | StageRoute::StagingTable)
        )
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let batch = lists_as_json(batch).map_err(LoaderError::Invalid)?;
        if sends_rows(self.route.as_ref()) {
            return self.send_rows(target, &batch).await;
        }
        let parts = std::mem::take(&mut self.parts);
        let (parts, files) = encode(move || write_part(parts, &batch)).await?;
        self.parts = parts;
        for body in files {
            self.put_file(target, body).await?;
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
            self.put_file(target, body).await?;
        }
        Ok(())
    }

    async fn load_batch(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        self.settle_uploads().await?;
        if !sends_rows(self.route.as_ref()) {
            self.copy_batch_files(target).await?;
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

    /// Waits for the batch's uploads rather than aborting them: an aborted PUT whose body S3 already received can still land after the files are removed.
    async fn remove_batch_files(&mut self, target: &WriteTarget<'_>) {
        while self.uploads.join_next().await.is_some() {}
        if !self.staged.is_empty() {
            self.remove_files(target).await;
        }
    }
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

impl SnowflakeWriter {
    /// Stage one Parquet file of the batch in the background, at most a few at once: on the external stage's folder with SQLLoad's own client, or uploaded to the internal stage (its first file inline, fetching the credentials the rest share).
    async fn put_file(
        &mut self,
        target: &WriteTarget<'_>,
        body: Vec<u8>,
    ) -> Result<(), LoaderError> {
        let name = format!("part-{:05}.parquet", self.staged.len() + 1);
        let key = format!("{}{name}", batch_folder(self, target));
        self.staged.push(name);
        while self.uploads.len() >= CONCURRENT_UPLOADS {
            self.join_upload().await?;
        }
        if let Some(folder) = &self.external_folder {
            let folder = folder.clone();
            self.uploads.spawn(async move {
                write_file(&folder, &key, bytes::Bytes::from(body)).await?;
                Ok(())
            });
            return Ok(());
        }
        let reference = stage_reference(self, target)?;
        let Some(known) = self.stage.clone() else {
            let file = StagedFile {
                stage: reference.trim_start_matches('@'),
                key: &key,
                body: &body,
            };
            self.stage = Some(stage_parquet(&self.destination, None, file).await?);
            return Ok(());
        };
        let destination = self.destination.clone();
        self.uploads.spawn(async move {
            let file = StagedFile {
                stage: reference.trim_start_matches('@'),
                key: &key,
                body: &body,
            };
            stage_parquet(&destination, Some(known), file).await?;
            Ok(())
        });
        Ok(())
    }

    /// Wait for one upload; its failure fails the batch.
    async fn join_upload(&mut self) -> Result<(), LoaderError> {
        match self.uploads.join_next().await {
            Some(joined) => joined.map_err(|error| {
                LoaderError::Invalid(format!("a staged file's upload failed: {error}"))
            })?,
            None => Ok(()),
        }
    }

    /// Wait for every upload of the batch.
    async fn settle_uploads(&mut self) -> Result<(), LoaderError> {
        while !self.uploads.is_empty() {
            self.join_upload().await?;
        }
        Ok(())
    }

    /// `COPY` the batch's staged files (straight into the table, committing, or into the staging table), then remove them.
    async fn copy_batch_files(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        let stage = stage_reference(self, target)?;
        let folder = batch_folder(self, target);
        let holds_json = (target.direct || target.builds)
            && target.columns.iter().any(|column| {
                matches!(column.column_type, ColumnType::Json | ColumnType::Array(_))
            });
        let copies = copy_staged(&CopyStaged {
            table: target.staging.unwrap_or(target.table),
            stage: &stage,
            folder: &folder,
            names: &self.staged,
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
        run_statements(&self.destination, &statements).await?;
        self.remove_files(target).await;
        Ok(())
    }

    /// Delete the batch's staged files, unless the stage keeps them; a failure only warns, and the next load removes them after a day.
    async fn remove_files(&mut self, target: &WriteTarget<'_>) {
        let removed = match (&self.route, &self.external_folder) {
            (Some(StageRoute::External(stage)), _) if stage.keep_staged_files => Ok(()),
            (_, Some(folder)) => {
                let keys: Vec<String> = self
                    .staged
                    .iter()
                    .map(|name| format!("{}{name}", batch_folder(self, target)))
                    .collect();
                delete_files(folder, &keys)
                    .await
                    .map_err(|error| error.to_string())
            }
            (_, None) => match stage_reference(self, target) {
                Ok(stage) => remove_staged(&self.destination, &stage, &batch_folder(self, target))
                    .await
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error.to_string()),
            },
        };
        if let Err(error) = removed {
            tracing::warn!(%error, load_id = %target.load_id, "cannot remove a batch's staged files; the next load removes them after a day");
        }
    }
}

/// An error naming the first row of float list column `name` that holds NaN or an infinity, which a Snowflake array of JSON values cannot keep.
fn refuse_non_finite(name: &str, column: &arrow_array::ArrayRef) -> Result<(), String> {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    let lists = column.as_list::<i32>();
    let offsets = lists.offsets();
    let first = usize::try_from(offsets[0]).map_err(|error| error.to_string())?;
    let last = usize::try_from(offsets[offsets.len() - 1]).map_err(|error| error.to_string())?;
    let values = arrow_cast::cast(
        &lists.values().slice(first, last - first),
        &arrow_schema::DataType::Float64,
    )
    .map_err(|error| error.to_string())?;
    let Some(position) = values
        .as_primitive::<arrow_array::types::Float64Type>()
        .iter()
        .position(|value| matches!(value, Some(value) if !value.is_finite()))
    else {
        return Ok(());
    };
    let found = i32::try_from(first + position).map_err(|error| error.to_string())?;
    let row = offsets.partition_point(|offset| *offset <= found);
    Err(format!(
        "column {name}, row {row}: an array holds NaN or an infinity, which a Snowflake array holds as JSON and cannot keep; pin array(text) to keep such values as text"
    ))
}

/// `batch` with each list column as its JSON text, the way Snowflake reads an array exactly (its own reading of a Parquet list cuts timestamps to milliseconds and decimals to their digits).
pub(crate) fn lists_as_json(batch: RecordBatch) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), arrow_schema::DataType::List(_)))
    {
        return Ok(batch);
    }
    let mut fields: Vec<arrow_schema::Field> = Vec::with_capacity(schema.fields().len());
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        match field.data_type() {
            arrow_schema::DataType::List(item) => {
                if item.data_type().is_floating() {
                    refuse_non_finite(field.name(), column)?;
                }
                let one = RecordBatch::try_from_iter([(field.name().as_str(), column.clone())])
                    .map_err(|error| error.to_string())?;
                let text = normalise_batch(&one, &[])?;
                fields.push(text.schema().field(0).clone());
                columns.push(text.column(0).clone());
            }
            _ => {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
    }
    let schema = arrow_schema::Schema::new(fields).with_metadata(schema.metadata().clone());
    RecordBatch::try_new(std::sync::Arc::new(schema), columns).map_err(|error| error.to_string())
}
