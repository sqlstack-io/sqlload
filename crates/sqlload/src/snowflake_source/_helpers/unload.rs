//! The phases of one unload: the `COPY INTO` statement, its row count, and reading the files back.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::snowflake_source::_helpers::convert::convert_batch;
use crate::snowflake_source::_helpers::folders::Unloaded;
use crate::snowflake_source::constants::PARALLEL_DOWNLOADS;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{SnowflakeReader, UnloadFolder};
use crate::sql_api::models::SqlRows;

/// The `COPY INTO` that writes `query`'s rows to `folder` as Parquet with column names, and the folder as it names it.
pub(crate) fn copy_statement(folder: &UnloadFolder, query: &str) -> (String, String) {
    let (target, options) = match folder {
        UnloadFolder::Internal { stage_path } => (stage_path.as_str(), ""),
        UnloadFolder::External {
            copy_target,
            options,
            ..
        } => (copy_target.as_str(), options.as_str()),
    };
    (
        target.to_owned(),
        format!(
            "COPY INTO {target} FROM ({query}){options} FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE"
        ),
    )
}

/// The `rows_unloaded` a `COPY INTO` reports.
pub(crate) fn rows_unloaded(result: &SqlRows) -> Result<u64, SnowflakeSourceError> {
    let count = result
        .rows
        .first()
        .and_then(|row| result.value(row, "rows_unloaded"))
        .ok_or_else(|| {
            SnowflakeSourceError::Invalid("COPY INTO reported no rows_unloaded".to_owned())
        })?;
    count.parse::<u64>().map_err(|error| {
        SnowflakeSourceError::Invalid(format!(
            "COPY INTO reported rows_unloaded {count:?}: {error}"
        ))
    })
}

/// Every row of `files`, fetched several at a time and handed to `on_batch` in `schema`, in batches of at most `batch_rows`; returns the row count.
pub(crate) async fn read_unloaded(
    reader: &SnowflakeReader,
    files: &Unloaded,
    (schema, batch_rows): (&SchemaRef, usize),
    on_batch: &mut impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, SnowflakeSourceError> {
    let mut fetched = futures_util::stream::iter(0..files.len())
        .map(|index| async move { (index, files.fetch(reader, index).await) })
        .buffered(PARALLEL_DOWNLOADS);
    let mut rows: u64 = 0;
    while let Some((index, bytes)) = fetched.next().await {
        let failed = |reason: String| SnowflakeSourceError::File {
            file: files.name(index),
            reason,
        };
        let batches = ParquetRecordBatchReaderBuilder::try_new(bytes?)
            .and_then(|builder| builder.with_batch_size(batch_rows).build())
            .map_err(|error| failed(error.to_string()))?;
        for batch in batches {
            let batch = batch.map_err(|error| failed(error.to_string()))?;
            let converted = convert_batch(&batch, schema).map_err(failed)?;
            rows += converted.num_rows() as u64;
            on_batch(converted)
                .await
                .map_err(SnowflakeSourceError::Consumer)?;
        }
    }
    Ok(rows)
}
