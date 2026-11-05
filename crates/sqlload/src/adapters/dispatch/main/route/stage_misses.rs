//! Whether a stage whose URL names a files source's folder still cannot read it.

use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::models::StageRoute;
use crate::store_paths::models::Location;

/// Whether the external `stage` names the bucket and folder of `files` but cannot read them, being on another S3 endpoint: the paths compare equal, so routing alone would take the stage to cover them.
pub fn stage_misses(stage: Option<&StageRoute>, files: &Location) -> bool {
    match stage {
        Some(StageRoute::External(stage)) => {
            !stage_covers(&stage.location, files)
                && files.to_string().starts_with(&stage.location.to_string())
        }
        _ => false,
    }
}
