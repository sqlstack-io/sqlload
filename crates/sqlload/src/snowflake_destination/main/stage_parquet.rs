//! Upload a Parquet file to the user stage.

use crate::http::errors::ApiError;
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::PutOutcome;

/// Write `body` to `key` on the user stage (`key` is below `@~/`).
pub async fn stage_parquet(
    destination: &SnowflakeDestination,
    key: &str,
    body: &[u8],
) -> Result<(), ApiError> {
    for _ in 0..2 {
        let stage = destination.driver.stage_info(USER_STAGE).await?;
        if put_stage_file(&destination.http, &stage, key, body).await? == PutOutcome::Uploaded {
            return Ok(());
        }
    }
    Err(ApiError::Protocol(
        "stage credentials expired twice in a row".to_owned(),
    ))
}
