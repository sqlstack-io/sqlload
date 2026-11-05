//! Load a batch's staged files into a table.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use crate::adapters::snowflake::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES};
use crate::adapters::snowflake::models::CopyStaged;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::schema_plan::models::DesiredColumn;

/// `COPY` statements loading the files, at most 1,000 each; always `FORCE = TRUE` (SQLLoad never relies on Snowflake's load history) and stopping at the first bad value.
pub fn copy_staged(request: &CopyStaged<'_>) -> Vec<String> {
    let literals: Vec<String> = request
        .names
        .iter()
        .map(|name| SnowflakeSql.render_loader_value_literal(name))
        .collect();
    let table = SnowflakeSql.render_qualified_name(request.table);
    let (stage, folder) = (request.stage, request.folder);
    let (into, from, by_name) = match request.selected {
        Some(columns) => {
            let names: Vec<String> = columns
                .iter()
                .map(|column| SnowflakeSql.render_identifier(&column.name))
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
    let field = format!(
        "GET($1, {})",
        sql_adapter(snowflake).render_loader_value_literal(&column.name)
    );
    match column.column_type {
        ColumnType::Json => format!("PARSE_JSON(CAST({field} AS VARCHAR))"),
        _ => format!(
            "CAST({field} AS {})",
            sql_adapter(snowflake)
                .render_loader_logical_type(&column.column_type)
                .sql
        ),
    }
}
