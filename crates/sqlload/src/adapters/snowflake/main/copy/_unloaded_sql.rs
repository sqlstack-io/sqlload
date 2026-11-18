//! The statements loading a Snowflake source's rows into a table inside Snowflake: from its unloaded Parquet files, or selected from the source in the same session.

use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES};
use crate::adapters::snowflake::models::{SnowflakeSql, UnloadedFrom, UnloadedLoad};
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// The statements loading `request`'s rows into `request.into`, every one of its columns filled as [`column_value`] says: from files, `COPY`s of at most 1,000 files each, stopping at the first bad value and never relying on Snowflake's load history; from a query, one `INSERT ... SELECT`.
pub(in crate::adapters) fn unloaded_statements(request: &UnloadedLoad<'_>) -> Vec<String> {
    let into = SnowflakeSql.render_qualified_name(request.into);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| SnowflakeSql.render_identifier(&column.name))
        .collect();
    let values = |rows: &Rows<'_>| -> Vec<String> {
        request
            .columns
            .iter()
            .map(|column| column_value(request, column, rows))
            .collect()
    };
    match &request.from {
        UnloadedFrom::Files {
            stage,
            folder,
            keys,
            ..
        } => keys
            .chunks(MAX_COPY_FILES)
            .enumerate()
            .map(|(chunk, chunk_keys)| {
                let files: Vec<String> = chunk_keys
                    .iter()
                    .map(|key| SnowflakeSql.render_loader_value_literal(key))
                    .collect();
                format!(
                    "COPY INTO {into} ({}) FROM (SELECT {} FROM {stage}/{folder}) FILES = ({}) {COPY_FILE_FORMAT} ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
                    names.join(", "),
                    values(&Rows::Files(chunk * MAX_COPY_FILES, chunk_keys)).join(", "),
                    files.join(", ")
                )
            })
            .collect(),
        UnloadedFrom::Query(query) => vec![format!(
            "INSERT INTO {into} ({}) SELECT {} FROM ({query}) AS u",
            names.join(", "),
            values(&Rows::Query).join(", ")
        )],
    }
}

/// Which rows one statement reads: the unload's files `keys`, the `first`th file on, or the query.
enum Rows<'a> {
    Files(usize, &'a [String]),
    Query,
}

/// Where `column` comes from in a statement reading `rows`: the batch's load id or load time, a merge's row number (the batch, then the file, then the row in it; a query's rows in the order it gives them), the value of the constant it is, or the source's column it is read as; NULL when the source lacks it.
fn column_value(request: &UnloadedLoad<'_>, column: &DesiredColumn, rows: &Rows<'_>) -> String {
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
                    Rows::Files(first, keys),
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
                        "(CASE METADATA$FILENAME {} END) * {UNLOADED_FILE_ROWS} + METADATA$FILE_ROW_NUMBER",
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
                        Rows::Files(..) => format!("GET($1, {})", literal(&read.name)),
                        Rows::Query => format!("u.{}", quoted(&read.name)),
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
