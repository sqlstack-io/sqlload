//! The statements loading a Snowflake source's unloaded Parquet files into a table, inside Snowflake.

use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES};
use crate::adapters::snowflake::models::{SnowflakeSql, UnloadedCopy};
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// The `COPY`s loading the unload's files into `request.into`, every one of its columns filled as [`unloaded_value`] says; at most 1,000 files each. Each stops at the first bad value and never relies on Snowflake's load history.
pub(in crate::adapters) fn copy_unloaded(request: &UnloadedCopy<'_>) -> Vec<String> {
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| SnowflakeSql.render_identifier(&column.name))
        .collect();
    request
        .keys
        .chunks(MAX_COPY_FILES)
        .enumerate()
        .map(|(chunk, keys)| {
            let values: Vec<String> = request
                .columns
                .iter()
                .map(|column| column_value(request, column, (chunk * MAX_COPY_FILES, keys)))
                .collect();
            let files: Vec<String> = keys
                .iter()
                .map(|key| SnowflakeSql.render_loader_value_literal(key))
                .collect();
            format!(
                "COPY INTO {} ({}) FROM (SELECT {} FROM {}/{}) FILES = ({}) {COPY_FILE_FORMAT} ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
                SnowflakeSql.render_qualified_name(request.into),
                names.join(", "),
                values.join(", "),
                request.stage,
                request.folder,
                files.join(", ")
            )
        })
        .collect()
}

/// Where `column` comes from in the `COPY` of `keys`, the unload's files from the `first`th on: the batch's load id or load time, a merge's row number (the batch, then the file, then the row in it), the value of the constant it is, or the files' column it is read as; NULL when the files lack it.
fn column_value(
    request: &UnloadedCopy<'_>,
    column: &DesiredColumn,
    (first, keys): (usize, &[String]),
) -> String {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let constant = request
        .constants
        .iter()
        .find(|(name, _, _)| *name == column.name);
    match (metadata_role(request.compat, &column.name), constant) {
        (Some(MetadataRole::LoadId), _) => literal(request.load_id),
        (Some(MetadataRole::LoadedAt), _) => {
            format!("CAST({} AS TIMESTAMP_TZ(6))", literal(request.loaded_at))
        }
        _ if column.name == MERGE_ROW_COLUMN => {
            let files: Vec<String> = keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    format!(
                        "WHEN {} THEN {}",
                        literal(&format!("{}{key}", request.filename_prefix)),
                        first + index
                    )
                })
                .collect();
            format!(
                "{} * {UNLOADED_BATCH_ROWS} + (CASE METADATA$FILENAME {} END) * {UNLOADED_FILE_ROWS} + METADATA$FILE_ROW_NUMBER",
                request.batch,
                files.join(" ")
            )
        }
        (_, Some((_, _, value))) => cast(column, &literal(value)),
        _ => request
            .read
            .iter()
            .find(|read| read.name == column.name)
            .map_or_else(
                || "NULL".to_owned(),
                |read| {
                    let field = format!("GET($1, {})", literal(&read.name));
                    unloaded_value(column, &read.output, &field)
                },
            ),
    }
}

/// An unload's `field`, read as `read`, as the value it was unloaded from, then as `column` holds it: a time or timestamp from its whole microseconds (a timestamp with time zone in UTC, as SQLLoad's reader gives it), JSON and arrays parsed from their text, anything else cast to its type.
pub(in crate::adapters) fn unloaded_value(
    column: &DesiredColumn,
    read: &ColumnType,
    field: &str,
) -> String {
    let micros = format!("CAST({field} AS NUMBER(38,0))");
    let value = match read {
        ColumnType::TimestampNtz => format!("TO_TIMESTAMP_NTZ({micros}, 6)"),
        ColumnType::TimestampTz => {
            format!("CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ({micros}, 6))")
        }
        ColumnType::Time => format!("TIMEADD(MICROSECOND, {micros}, '00:00:00'::TIME)"),
        _ => field.to_owned(),
    };
    cast(column, &value)
}

/// `value` as `column` holds it: JSON and arrays parsed from their text, anything else cast to its type.
fn cast(column: &DesiredColumn, value: &str) -> String {
    match &column.column_type {
        ColumnType::Json => format!("PARSE_JSON(CAST({value} AS VARCHAR))"),
        ColumnType::Array(_) => format!("CAST(PARSE_JSON(CAST({value} AS VARCHAR)) AS ARRAY)"),
        column_type => format!(
            "CAST({value} AS {})",
            SnowflakeSql.render_loader_logical_type(column_type).sql
        ),
    }
}
