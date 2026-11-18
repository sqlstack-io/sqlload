//! The statement that has ClickHouse read one file of a warehouse source's unload from S3 itself.

use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, SqlTypeOf, s3_parquet,
};
use crate::adapters::clickhouse::_helpers::dialect::write::typed_literal;
use crate::adapters::clickhouse::constants::ORDERED_READ_SETTINGS;
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// One file of an unload that ClickHouse reads: its URL, and its place among the unload's files, from 0, which a merge's row number counts by.
pub(crate) struct UnloadedFile<'a> {
    pub url: &'a str,
    pub index: usize,
}

/// `INSERT INTO into` every one of `columns` (each typed by `sql_type`) from one of a warehouse's unloaded Parquet `file`s, read by [`s3_parquet`] with `credentials`. A column of `read` comes from the file, turned back from whole microseconds where it is read as a timestamp (a time never loads this way); a column of `target`'s constants from its value; a merge's row number from the batch, the file and the row in it, read in stored order; the load id and load time (`loaded_at`) are the batch's; any other column is NULL.
pub(crate) fn unloaded_insert(
    (into, columns, sql_type): (&str, &[DesiredColumn], SqlTypeOf<'_>),
    target: &WriteTarget<'_>,
    (read, loaded_at): (&[ReadColumn], &str),
    (file, credentials): (&UnloadedFile<'_>, Option<&PullCredentials>),
) -> String {
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "CAST({} AS {})",
                column_value(column, target, (read, loaded_at), file),
                sql_type(&column.column_type)
            )
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| ClickHouseSql.render_identifier(&column.name))
        .collect();
    let settings = match columns.iter().any(|column| column.name == MERGE_ROW_COLUMN) {
        true => format!(" SETTINGS {ORDERED_READ_SETTINGS}"),
        false => String::new(),
    };
    format!(
        "INSERT INTO {into} ({}) SELECT {} FROM {}{settings}",
        targets.join(", "),
        values.join(", "),
        s3_parquet(file.url, credentials)
    )
}

/// Where `column`'s value comes from, before its cast: the load id or load time, a merge's row number, a constant's value, the file's column of `read` it is, or NULL when the file lacks it.
fn column_value(
    column: &DesiredColumn,
    target: &WriteTarget<'_>,
    (read, loaded_at): (&[ReadColumn], &str),
    file: &UnloadedFile<'_>,
) -> String {
    let constant = target
        .constants
        .iter()
        .find(|(name, _, _)| *name == column.name);
    let held = read.iter().find(|read| read.name == column.name);
    match (metadata_role(target.compat, &column.name), constant, held) {
        (Some(MetadataRole::LoadId), _, _) => typed_literal(target.load_id, &column.column_type),
        (Some(MetadataRole::LoadedAt), _, _) => typed_literal(loaded_at, &column.column_type),
        _ if column.name == MERGE_ROW_COLUMN => format!(
            "{} * {UNLOADED_BATCH_ROWS} + {} * {UNLOADED_FILE_ROWS} + rowNumberInAllBlocks()",
            target.batch, file.index
        ),
        (_, Some((_, column_type, value)), _) => typed_literal(value, column_type),
        (_, None, Some(read)) => {
            unloaded_value(&read.output, &ClickHouseSql.render_identifier(&read.name))
        }
        (_, None, None) => "NULL".to_owned(),
    }
}

/// File column `quoted` as the value it was unloaded from: a timestamp, with or without time zone, from its whole microseconds as the UTC instant SQLLoad's reader gives (ClickHouse stores both as an instant); a binary through `substring`, which keeps a fixed-size value's trailing zero bytes; anything else as it is.
fn unloaded_value(output: &ColumnType, quoted: &str) -> String {
    match output {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => {
            format!("fromUnixTimestamp64Micro(CAST({quoted} AS Int64), 'UTC')")
        }
        ColumnType::Binary => format!("substring({quoted}, 1)"),
        _ => quoted.to_owned(),
    }
}
