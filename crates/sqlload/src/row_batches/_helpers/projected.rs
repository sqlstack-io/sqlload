//! The parts of a projection: column names, paths and name clashes.

use crate::row_batches::constants::{PROJECTION_ROOT, PROJECTION_SEPARATOR};
use crate::row_batches::models::{PathNode, ProjectedField};

/// Whether one of `fields` loads as `name`, ignoring case as the destinations do.
pub(crate) fn names_field(fields: &[ProjectedField], name: &str) -> bool {
    fields
        .iter()
        .any(|field| field.name.eq_ignore_ascii_case(name))
}

pub(crate) fn column_name(name: &str) -> Result<String, String> {
    match name.trim().is_empty() {
        true => Err("a field's column name is empty".to_owned()),
        false => Ok(name.to_owned()),
    }
}

/// `$.a.b` as its keys; `$` alone is the whole object.
pub(crate) fn path(text: &str) -> Result<Vec<String>, String> {
    let invalid =
        || format!("field path {text:?} is not a path such as \"$.id\" or \"$.address.city\"");
    let rest = text.strip_prefix(PROJECTION_ROOT).ok_or_else(invalid)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }
    let keys = rest
        .strip_prefix(PROJECTION_SEPARATOR)
        .ok_or_else(invalid)?;
    let keys: Vec<String> = keys
        .split(PROJECTION_SEPARATOR)
        .map(str::to_owned)
        .collect();
    match keys.iter().any(String::is_empty) {
        true => Err(invalid()),
        false => Ok(keys),
    }
}

/// The fields' paths as a tree; a field with path `$` ends at the root.
pub(crate) fn path_tree(fields: &[ProjectedField]) -> PathNode {
    let mut root = PathNode::default();
    for (index, field) in fields.iter().enumerate() {
        let mut node = &mut root;
        for key in &field.path {
            node = node.children.entry(key.clone()).or_default();
            node.under.push(index);
        }
        node.ends.push(index);
    }
    root
}
