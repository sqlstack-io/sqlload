//! Upload an Arrow batch to the user stage as a Parquet file.

use arrow_array::RecordBatch;

use crate::http::errors::ApiError;
use crate::snowflake_destination::_helpers::parquet_file::parquet_bytes;
use crate::snowflake_destination::constants::{STAGE_FOLDER, USER_STAGE};
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::PutOutcome;

/// Write `batch` as file number `part` of load `load_id` on the user stage.
pub async fn stage_parquet(
    destination: &SnowflakeDestination,
    load_id: &str,
    part: usize,
    batch: &RecordBatch,
) -> Result<(), ApiError> {
    let body = parquet_bytes(batch).map_err(ApiError::Protocol)?;
    let key = format!("{STAGE_FOLDER}/{load_id}/part-{part:05}.parquet");
    for _ in 0..2 {
        let stage = destination.driver.stage_info(USER_STAGE).await?;
        if put_stage_file(&destination.http, &stage, &key, &body).await? == PutOutcome::Uploaded {
            return Ok(());
        }
    }
    Err(ApiError::Protocol(
        "stage credentials expired twice in a row".to_owned(),
    ))
}
