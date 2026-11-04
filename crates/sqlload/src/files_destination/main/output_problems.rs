//! Check a table's strategy and cursor against its files path.

use crate::column_types::models::ColumnType;
use crate::files_destination::constants::{CURSOR_DATE_PLACEHOLDER, CURSOR_PLACEHOLDER};
use crate::files_destination::models::{OutputCheck, PartitionBy};
use crate::project::models::WriteStrategy;

/// Why the table cannot be written as files with its strategy, cursor and path; empty when it can.
pub fn output_problems(check: &OutputCheck<'_>) -> Vec<String> {
    let incremental = match check.strategy {
        WriteStrategy::Merge => {
            return vec![
                "merge needs a table format to update rows in place; a files target takes append, replace or delete_insert".to_owned(),
            ];
        }
        WriteStrategy::DeleteInsert => true,
        WriteStrategy::Append => check.cursor_column.is_some(),
        WriteStrategy::Replace => false,
    };
    match (incremental, check.partition, check.cursor_column) {
        (true, None, _) => vec![format!(
            "{} with a cursor_column writes partitions by the cursor, so the path needs {CURSOR_PLACEHOLDER} or {CURSOR_DATE_PLACEHOLDER} in its last folder",
            check.strategy
        )],
        (false, Some(partition), _) => vec![format!(
            "the path's partition folder {partition} needs an incremental strategy: delete_insert, or append with a cursor_column"
        )],
        (true, Some(partition), Some(column)) => {
            cursor_type_problems(partition.by, column, check.cursor_type)
        }
        (_, _, _) => Vec::new(),
    }
}

/// Problems with the cursor's type for partitions `by` it.
fn cursor_type_problems(
    by: PartitionBy,
    column: &str,
    cursor_type: Option<&ColumnType>,
) -> Vec<String> {
    let fits = match (by, cursor_type) {
        (_, None) => return Vec::new(),
        (
            PartitionBy::Date,
            Some(ColumnType::Date | ColumnType::TimestampTz | ColumnType::TimestampNtz),
        ) => true,
        (
            PartitionBy::Value,
            Some(
                ColumnType::SmallInt
                | ColumnType::Int
                | ColumnType::Date
                | ColumnType::Text
                | ColumnType::Varchar(_),
            ),
        ) => true,
        (_, Some(_)) => false,
    };
    let expected = match by {
        PartitionBy::Date => format!("{CURSOR_DATE_PLACEHOLDER} needs a date or timestamp cursor"),
        PartitionBy::Value => format!(
            "{CURSOR_PLACEHOLDER} needs an int, date or text cursor (use {CURSOR_DATE_PLACEHOLDER} for timestamps)"
        ),
    };
    match (fits, cursor_type) {
        (false, Some(cursor_type)) => vec![format!("{expected}, but {column} is {cursor_type}")],
        _ => Vec::new(),
    }
}
