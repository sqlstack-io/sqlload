//! Load a batch's staged files into a table.

use crate::column_types::models::Warehouse;
use crate::snowflake_destination::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES, USER_STAGE};
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::models::TableName;

/// `COPY` statements loading `names` from `folder` on the user stage into `table` by column name, at most 1,000 files each; always `FORCE = TRUE` (SQLLoad never relies on Snowflake's load history) and stopping at the first bad value.
pub fn copy_staged(table: &TableName, folder: &str, names: &[String]) -> Vec<String> {
    let literals: Vec<String> = names
        .iter()
        .map(|name| sql_literal(name, Warehouse::Snowflake))
        .collect();
    literals
        .chunks(MAX_COPY_FILES)
        .map(|chunk| {
            format!(
                "COPY INTO {} FROM @{USER_STAGE}/{folder} FILES = ({}) {COPY_FILE_FORMAT} MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
                qualified_name(table, Warehouse::Snowflake),
                chunk.join(", ")
            )
        })
        .collect()
}
