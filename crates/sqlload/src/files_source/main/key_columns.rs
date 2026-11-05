//! Combine the keys a JSON batch's records hold with the columns known so far.

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::text::checks::ColumnChecks;
use crate::files_source::models::{FileColumn, FilesRead, StoredRead};

/// `known` plus a column for each of `found` (a key and the type its values suggest) that `read` neither knows nor drops (`exclude_columns`, the loader's own columns), in the order found; a key whose values were all NULL is text, as in sampling.
pub fn key_columns(
    read: &FilesRead,
    known: &[FileColumn],
    found: Vec<(String, Option<ColumnType>)>,
) -> Result<Vec<FileColumn>, String> {
    let checks = ColumnChecks::new(read)?;
    let mut columns = known.to_vec();
    for (name, column_type) in found {
        let seen = columns.iter().any(|column| column.name == name);
        if seen || checks.is_known(&name) {
            continue;
        }
        columns.push(FileColumn {
            name,
            column_type: Ok(column_type.unwrap_or(ColumnType::Text)),
            direct_refusal: None,
            stored_read: StoredRead::Plain,
        });
    }
    Ok(columns)
}
