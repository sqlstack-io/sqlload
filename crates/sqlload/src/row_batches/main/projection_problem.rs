//! Whether a table's projected rows still load what the table needs.

use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::Naming;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::models::{Projection, ProjectionRules};

/// Why rows projected by `projection` cannot load for a table with `rules`, if they cannot: its cursor column must be a field, `keep_record` or a column flattening a field makes, and `keep_record` would carry masked values unmasked.
pub fn projection_problem(projection: &Projection, rules: ProjectionRules<'_>) -> Option<String> {
    if projection.keep_record.is_some() && !rules.masked.is_empty() {
        return Some(format!(
            "keep_record would hold the masked columns ({}) unmasked; drop keep_record or the mask",
            rules.masked.join(", ")
        ));
    }
    let column = rules.cursor_column?;
    let loads = projection
        .fields
        .iter()
        .map(|field| &field.name)
        .chain(&projection.keep_record)
        .any(|name| loads_as(name, (column, rules.naming), rules.flatten));
    (!loads).then(|| {
        format!("the table's cursor_column {column} is not among the fields; add it to fields")
    })
}

/// Whether the projected column `name` loads `column`, a loaded name under `naming`: itself, or a `<name>__<key>` column when `flatten` lists it (exactly, as flattening matches it); `name` as it loads under `naming`.
fn loads_as(
    name: &str,
    (column, (naming, limit)): (&str, (Naming, Option<usize>)),
    flatten: &[String],
) -> bool {
    let flattened = flatten.iter().any(|listed| listed == name);
    let name = loaded_name(naming, name, limit);
    match flattened {
        true => column
            .get(..name.len() + FLATTEN_SEPARATOR.len())
            .is_some_and(|prefix| {
                prefix.eq_ignore_ascii_case(&format!("{name}{FLATTEN_SEPARATOR}"))
            }),
        false => name.eq_ignore_ascii_case(column),
    }
}
