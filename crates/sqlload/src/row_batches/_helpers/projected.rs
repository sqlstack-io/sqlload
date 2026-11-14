//! The parts of a projection: column names, paths and name clashes.

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

/// The digits `n` or `-n` between a list index's brackets, as whether it counts from the end and the number, a number too big for a position held at the largest (past the end of any list); `None` for anything else.
pub(crate) fn list_index(inside: &str) -> Option<(bool, usize)> {
    let (from_end, digits) = match inside.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, inside),
    };
    if digits.is_empty() {
        return None;
    }
    let mut number: usize = 0;
    for byte in digits.bytes() {
        if !byte.is_ascii_digit() {
            return None;
        }
        number = number
            .saturating_mul(10)
            .saturating_add(usize::from(byte - b'0'));
    }
    Some((from_end, number))
}
