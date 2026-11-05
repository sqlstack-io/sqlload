//! Why the destination must not read a files table's files itself.

use crate::adapters::snowflake::main::copy::_direct_check::direct_check;
use crate::adapters::snowflake::models::StageRoute;
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// Why the destination must not load the planned `read` of a files table straight from its files over `stage`; `None` when it may, or when the route reads no files itself.
pub fn direct_blocked(
    spec: &TableSpec,
    read: &FilesRead,
    stage: Option<&StageRoute>,
) -> Option<String> {
    let Some(StageRoute::External(stage)) = stage else {
        return None;
    };
    direct_check(spec, (read, read.files.as_slice(), &[]), stage)
}
