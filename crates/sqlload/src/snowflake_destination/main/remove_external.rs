//! Delete a load's staged Parquet from the external stage's bucket.

use crate::external_stage::main::staged_key::staged_key;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::snowflake_destination::models::ExternalStage;

/// Delete parts `1..=parts` of load `load_id`; a part already gone is not an error.
pub async fn remove_external(
    stage: &ExternalStage,
    load_id: &str,
    parts: usize,
) -> Result<(), String> {
    let folder = open_folder(&stage.location).map_err(|error| error.to_string())?;
    let keys: Vec<String> = (1..=parts).map(|part| staged_key(load_id, part)).collect();
    delete_files(&folder, &keys)
        .await
        .map_err(|error| error.to_string())
}
