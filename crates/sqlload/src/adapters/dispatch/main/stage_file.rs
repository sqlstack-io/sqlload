//! Upload a staged Parquet file.

use crate::adapters::dispatch::_helpers::no_sql::no_sql;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_stage_parquet::stage_parquet;
use crate::adapters::snowflake::models::StagedFile;
use crate::loader::errors::LoaderError;
use crate::stage_api::models::StageInfo;

/// Upload one staged Parquet file to an internal stage (Snowflake), with the batch's `known` credentials; returns them for its next file.
pub async fn stage_file(
    destination: &Destination,
    known: Option<StageInfo>,
    file: StagedFile<'_>,
) -> Result<StageInfo, LoaderError> {
    match destination {
        Destination::Snowflake(snowflake) => Ok(stage_parquet(snowflake, known, file).await?),
        _ => Err(no_sql()),
    }
}
