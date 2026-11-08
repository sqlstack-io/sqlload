//! Whether projected rows keep a table's cursor column.

use crate::row_batches::models::Projection;

/// Why `column`, a table's cursor column, would not load under `projection`, if it would not.
pub fn cursor_problem(projection: &Projection, column: &str) -> Option<String> {
    let loads = projection
        .fields
        .iter()
        .map(|field| &field.name)
        .chain(&projection.keep_record)
        .any(|name| name.eq_ignore_ascii_case(column));
    (!loads).then(|| {
        format!(
            "the table's cursor_column {column} is not among the job's fields; add it to fields"
        )
    })
}
