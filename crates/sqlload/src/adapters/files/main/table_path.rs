//! Work out where a table's files go from its path template.

use crate::adapters::files::_helpers::layout::template::partition_folder;
use crate::adapters::files::constants::{CURSOR_PLACEHOLDER, CURSOR_PLACEHOLDER_START};
use crate::adapters::files::models::{TablePath, TablePathRequest};
use crate::store_paths::main::render_template::render_template;
use crate::store_paths::main::resolve_location::resolve_location;

/// The table's folder and partition folder: placeholders filled, the cursor allowed only in the last folder.
pub fn table_path(request: &TablePathRequest<'_>) -> Result<TablePath, String> {
    let template = request.template;
    if template.contains("://") {
        return Err(format!(
            "path {template:?} must be relative to connection {:?}, not a URL",
            request.connection
        ));
    }
    let segments: Vec<&str> = template
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let values = [
        ("source", request.source),
        ("schema", request.schema),
        ("table", request.table),
    ];
    let cursor_at: Vec<usize> = segments
        .iter()
        .enumerate()
        .filter(|(_, segment)| segment.contains(CURSOR_PLACEHOLDER_START))
        .map(|(index, _)| index)
        .collect();
    let (folder_segments, partition) = match cursor_at.as_slice() {
        [] => (segments.as_slice(), None),
        [last] if *last + 1 == segments.len() => (
            &segments[..*last],
            Some(partition_folder(template, segments[*last], &values)?),
        ),
        _ => {
            return Err(format!(
                "path {template:?}: {CURSOR_PLACEHOLDER} may appear once, in the last folder"
            ));
        }
    };
    let root = match template.starts_with('/') {
        true => "/",
        false => "",
    };
    let rendered = render_template(&format!("{root}{}", folder_segments.join("/")), &values)
        .map_err(|error| error.to_string())?;
    let folder = resolve_location(Some((request.connection, request.store)), &rendered)
        .map_err(|error| error.to_string())?;
    match folder.prefix.is_empty() {
        true => Err(format!(
            "path {template:?} must give the table a folder of its own, not the connection's root"
        )),
        false => Ok(TablePath { folder, partition }),
    }
}
