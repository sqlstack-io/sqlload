//! Run a files table's `transform` on each file of a read, in order.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::files_source::models::DataFile;
use crate::python_sources::errors::ReadError;
use crate::python_sources::main::_file_context::file_context;
use crate::python_sources::main::_run_transform::run_transform;
use crate::python_sources::models::{TransformCall, TransformFiles, TransformTime};

/// For each of `request`'s files in order: its rows from `read`, the function run on them, and each batch it returned or yielded passed to `on_rows` as it crosses, with the file, its name as `_sqlload_source_file` has it, and the rows returned for it before. A file whose function fails stops there, naming the file; returns how long each file's call took.
pub async fn transform_files<E: From<ReadError>>(
    request: &TransformFiles<'_>,
    mut read: impl AsyncFnMut(&DataFile) -> Result<(Vec<RecordBatch>, SchemaRef), E>,
    mut on_rows: impl AsyncFnMut((&DataFile, &str, u64), RecordBatch) -> Result<(), E>,
) -> Result<Vec<TransformTime>, E> {
    let mut times: Vec<TransformTime> = Vec::new();
    for data in request.files {
        let file_name = format!("{}{}", request.location, data.file.relative_key);
        let call = TransformCall {
            context: file_context(
                &request.call.context.table,
                (&file_name, data),
                request.encoding,
            ),
            ..request.call.clone()
        };
        let mut rows_before: u64 = 0;
        let ran = run_transform(
            request.host.clone(),
            call,
            read(data).await?,
            async |batch: RecordBatch| {
                let rows = batch.num_rows() as u64;
                on_rows((data, &file_name, rows_before), batch).await?;
                rows_before += rows;
                Ok::<(), E>(())
            },
        )
        .await?
        .map_err(|reason| {
            ReadError::from(format!(
                "{file_name}: transform {} failed: {reason}",
                request.call.function
            ))
        })?;
        times.push(TransformTime {
            file: file_name,
            milliseconds: (ran.seconds * 1000.0).round() as u64,
            rows: ran.rows_in,
            dicts: ran.dicts,
        });
    }
    Ok(times)
}
