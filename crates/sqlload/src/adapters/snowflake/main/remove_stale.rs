//! Clean up the load folders failed runs left on an internal stage.

use crate::http::errors::ApiError;
use crate::adapters::snowflake::_helpers::stale_folders::stale_folders;
use crate::adapters::snowflake::constants::{STAGE_FOLDER, STALE_STAGE_HOURS};
use crate::adapters::snowflake::main::remove_staged::remove_staged;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// Remove every load folder under `sqlload/` of `stage` (as SQL) whose files are all older than a day (a running load's folder is never that old); returns how many.
pub async fn remove_stale(
    destination: &SnowflakeDestination,
    stage: &str,
) -> Result<usize, ApiError> {
    let listed = execute_sql(&destination.sql, &format!("LIST {stage}/{STAGE_FOLDER}/")).await?;
    let (Some(name), Some(modified)) = (listed.column("name"), listed.column("last_modified"))
    else {
        return Ok(0);
    };
    let files: Vec<(String, String)> = listed
        .rows
        .iter()
        .map(|row| {
            let text = |index: usize| row.get(index).cloned().flatten().unwrap_or_default();
            (text(name), text(modified))
        })
        .collect();
    let before = chrono::Utc::now() - chrono::Duration::hours(STALE_STAGE_HOURS);
    let folders = stale_folders(&files, before);
    for folder in &folders {
        remove_staged(destination, stage, folder).await?;
    }
    Ok(folders.len())
}
