//! Why the destination cannot load a files table's files exactly by reading them itself.

use crate::adapters::snowflake::main::copy::_direct_check::direct_check;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::models::StageRoute;
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// Why the destination cannot load the planned `read` of a files table exactly from its files: under the external `stage` that covers them, or copied unchanged to an internal stage (`copy_to_internal_stage`); `None` when it can, or when neither applies.
pub fn direct_blocked(
    spec: &TableSpec,
    read: &FilesRead,
    stage: Option<&StageRoute>,
) -> Option<String> {
    let files = spec.files.as_ref()?;
    let covering = match stage {
        Some(StageRoute::External(stage)) if stage_covers(&stage.location, &files.location) => {
            Some(stage)
        }
        _ => None,
    };
    if covering.is_none() && !files.copy_to_internal_stage {
        return None;
    }
    direct_check(spec, (read, read.files.as_slice(), &[]), covering)
}
