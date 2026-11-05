//! Upload a Parquet file to the user stage.

use crate::http::errors::ApiError;
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::{PutOutcome, StageInfo};

/// Write `body` to `key` on the user stage (`key` is below `@~/`) with `known` credentials, fetching new ones when there are none or they expired; returns the credentials that worked, so a batch's files need one fetch.
pub async fn stage_parquet(
    destination: &SnowflakeDestination,
    mut known: Option<StageInfo>,
    key: &str,
    body: &[u8],
) -> Result<StageInfo, ApiError> {
    for _ in 0..2 {
        let stage = match known.take() {
            Some(stage) => stage,
            None => destination.driver.stage_info(USER_STAGE).await?,
        };
        if put_stage_file(&destination.http, &stage, key, body).await? == PutOutcome::Uploaded {
            return Ok(stage);
        }
    }
    Err(ApiError::Protocol(
        "stage credentials expired twice in a row".to_owned(),
    ))
}
