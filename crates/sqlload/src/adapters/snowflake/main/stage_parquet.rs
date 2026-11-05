//! Upload a Parquet file to an internal stage.

use crate::adapters::snowflake::models::{SnowflakeDestination, StagedFile};
use crate::http::errors::ApiError;
use crate::stage_api::main::put_stage_file::put_stage_file;
use crate::stage_api::models::{PutOutcome, StageInfo};

/// Write the file with `known` credentials, fetching new ones when there are none or they expired; returns the credentials that worked, so a batch's files need one fetch.
pub async fn stage_parquet(
    destination: &SnowflakeDestination,
    mut known: Option<StageInfo>,
    file: StagedFile<'_>,
) -> Result<StageInfo, ApiError> {
    for _ in 0..2 {
        let stage = match known.take() {
            Some(stage) => stage,
            None => destination.driver.stage_info(file.stage).await?,
        };
        let outcome = put_stage_file(&destination.http, &stage, file.key, file.body).await?;
        if outcome == PutOutcome::Uploaded {
            return Ok(stage);
        }
    }
    Err(ApiError::Protocol(
        "stage credentials expired twice in a row".to_owned(),
    ))
}
