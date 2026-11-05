//! Write an Arrow batch as a Parquet file under the external stage.

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::external_stage::main::staged_key::staged_key;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::snowflake_destination::_helpers::parquet_file::parquet_bytes;
use crate::snowflake_destination::models::ExternalStage;

/// Write `batch` as part `part` of load `load_id` below the stage URL, with SQLLoad's own S3 client.
pub async fn stage_external(
    stage: &ExternalStage,
    load_id: &str,
    part: usize,
    batch: &RecordBatch,
) -> Result<(), String> {
    let body = parquet_bytes(batch)?;
    let folder = open_folder(&stage.location).map_err(|error| error.to_string())?;
    write_file(&folder, &staged_key(load_id, part), Bytes::from(body))
        .await
        .map_err(|error| error.to_string())
}
