//! The value each column of a Snowflake source's batch loaded inside Snowflake takes, from the source's unloaded files or its query.

use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::models::{SnowflakeSql, UnloadedFrom, UnloadedLoad, UnloadedRows};
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// Where `column` comes from in a statement reading `rows`: the batch's load id or load time, a merge's row number (the batch, then the file, then the row in it; a query's rows in the order it gives them), the value of the constant it is, or the source's column it is read as; NULL when the source lacks it.
pub(crate) fn column_value(
    request: &UnloadedLoad<'_>,
    column: &DesiredColumn,
    rows: &UnloadedRows<'_>,
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
            let in_batch = match (rows, &request.from) {
                (
                    UnloadedRows::Files(first, keys),
                    UnloadedFrom::Files {
                        filename_prefix, ..
                    },
                ) => {
                    let files: Vec<String> = keys
                        .iter()
                        .enumerate()
                        .map(|(index, key)| {
                            format!(
                                "WHEN {} THEN {}",
                                literal(&format!("{filename_prefix}{key}")),
                                first + index
                            )
                        })
                        .collect();
                    format!(
                        "(CASE METADATA$FILENAME {} ELSE TO_NUMBER('sqlload: unexpected unloaded file ' || METADATA$FILENAME) END) * {UNLOADED_FILE_ROWS} + METADATA$FILE_ROW_NUMBER",
                        files.join(" ")
                    )
                }
                _ => "SEQ8()".to_owned(),
            };
            format!("{} * {UNLOADED_BATCH_ROWS} + {in_batch}", request.batch)
        }
        (_, Some((_, _, value))) => cast(column, &literal(value)),
        _ => request
            .read
            .iter()
            .find(|read| read.name == column.name)
            .map_or_else(
                || "NULL".to_owned(),
                |read| {
                    let field = match rows {
                        UnloadedRows::Files(..) => format!("GET($1, {})", literal(&read.name)),
                        UnloadedRows::Query => format!("u.{}", quoted(&read.name)),
                    };
                    unloaded_value(column, &read.output, &field)
                },
            ),
    }
}

/// `name` as the unload's query names its column: quoted exactly as written.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// An unloaded `field`, read as `read`, as the value it was unloaded from, then as `column` holds it: a time or timestamp from its whole microseconds (a timestamp with time zone in UTC, as SQLLoad's reader gives it), anything else as it is.
fn unloaded_value(column: &DesiredColumn, read: &ColumnType, field: &str) -> String {
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
