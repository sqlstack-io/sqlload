//! Load a batch's staged files into a table.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::schema_plan::models::DesiredColumn;
use crate::snowflake_destination::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES};
use crate::snowflake_destination::models::CopyStaged;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::main::sql_literal::sql_literal;

/// `COPY` statements loading the files, at most 1,000 each; always `FORCE = TRUE` (SQLLoad never relies on Snowflake's load history) and stopping at the first bad value.
pub fn copy_staged(request: &CopyStaged<'_>) -> Vec<String> {
    let literals: Vec<String> = request
        .names
        .iter()
        .map(|name| sql_literal(name, Warehouse::Snowflake))
        .collect();
    let table = qualified_name(request.table, Warehouse::Snowflake);
    let (stage, folder) = (request.stage, request.folder);
    let (into, from, by_name) = match request.selected {
        Some(columns) => {
            let names: Vec<String> = columns
                .iter()
                .map(|column| quoted_identifier(&column.name, Warehouse::Snowflake))
                .collect();
            let values: Vec<String> = columns.iter().map(staged_value).collect();
            (
                format!(" ({})", names.join(", ")),
                format!("(SELECT {} FROM {stage}/{folder})", values.join(", ")),
                "",
            )
        }
        None => (
            String::new(),
            format!("{stage}/{folder}"),
            " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        ),
    };
    literals
        .chunks(MAX_COPY_FILES)
        .map(|chunk| {
            format!(
                "COPY INTO {table}{into} FROM {from} FILES = ({}) {COPY_FILE_FORMAT}{by_name} ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
                chunk.join(", ")
            )
        })
        .collect()
}

/// A column read from a staged file: JSON, which the files hold as text, parsed; anything else cast to its type.
fn staged_value(column: &DesiredColumn) -> String {
    let snowflake = Warehouse::Snowflake;
    let field = format!("GET($1, {})", sql_literal(&column.name, snowflake));
    match column.column_type {
        ColumnType::Json => format!("PARSE_JSON(CAST({field} AS VARCHAR))"),
        _ => format!(
            "CAST({field} AS {})",
            warehouse_type(&column.column_type, snowflake).sql
        ),
    }
}
