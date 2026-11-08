//! Whether a table's projected rows still load what the table needs.

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
        .any(|name| loads_as(name, column, rules.flatten));
    (!loads).then(|| {
        format!("the table's cursor_column {column} is not among the fields; add it to fields")
    })
}

/// Whether the projected column `name` loads `column`: itself, or a `<name>__<key>` column when `flatten` lists it (exactly, as flattening matches it).
fn loads_as(name: &str, column: &str, flatten: &[String]) -> bool {
    match flatten.iter().any(|listed| listed == name) {
        true => column
            .get(..name.len() + FLATTEN_SEPARATOR.len())
            .is_some_and(|prefix| {
                prefix.eq_ignore_ascii_case(&format!("{name}{FLATTEN_SEPARATOR}"))
            }),
        false => name.eq_ignore_ascii_case(column),
    }
}
