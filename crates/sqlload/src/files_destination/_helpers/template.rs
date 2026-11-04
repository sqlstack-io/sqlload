use crate::files_destination::constants::{
    CURSOR_DATE_PLACEHOLDER, CURSOR_PLACEHOLDER, CURSOR_PLACEHOLDER_START,
};
use crate::files_destination::models::{PartitionBy, PartitionFolder};
use crate::store_paths::main::render_template::render_template;

/// The partition folder `segment` describes, with every other placeholder filled from `values`.
pub(crate) fn partition_folder(
    template: &str,
    segment: &str,
    values: &[(&str, &str)],
) -> Result<PartitionFolder, String> {
    let (placeholder, by) = match (
        segment.contains(CURSOR_DATE_PLACEHOLDER),
        segment.contains(CURSOR_PLACEHOLDER),
    ) {
        (true, false) => (CURSOR_DATE_PLACEHOLDER, PartitionBy::Date),
        (false, true) => (CURSOR_PLACEHOLDER, PartitionBy::Value),
        _ => {
            return Err(format!(
                "path {template:?}: the partition folder needs exactly one {CURSOR_PLACEHOLDER} or {CURSOR_DATE_PLACEHOLDER}"
            ));
        }
    };
    let (prefix, suffix) = segment.split_once(placeholder).unwrap_or((segment, ""));
    let render = |part: &str| render_template(part, values).map_err(|error| error.to_string());
    let folder = PartitionFolder {
        prefix: render(prefix)?,
        suffix: render(suffix)?,
        by,
    };
    match (
        folder.prefix.contains(CURSOR_PLACEHOLDER_START),
        folder.suffix.contains(CURSOR_PLACEHOLDER_START),
    ) {
        (false, false) => Ok(folder),
        _ => Err(format!(
            "path {template:?}: the partition folder needs exactly one {CURSOR_PLACEHOLDER} or {CURSOR_DATE_PLACEHOLDER}"
        )),
    }
}
