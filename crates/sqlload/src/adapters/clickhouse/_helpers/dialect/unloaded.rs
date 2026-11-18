//! The statement that has ClickHouse read one file of a warehouse source's unload from S3 itself.

use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, SqlTypeOf, s3_parquet,
};
use crate::adapters::clickhouse::_helpers::dialect::write::typed_literal;
use crate::adapters::clickhouse::constants::ORDERED_READ_SETTINGS;
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// One file of an unload that ClickHouse reads: its URL, and its place among the unload's files, from 0, which a merge's row number counts by.
pub(crate) struct UnloadedFile<'a> {
    pub url: &'a str,
    pub index: usize,
}

/// `INSERT INTO into` every one of `columns` (each typed by `sql_type`) from one of a warehouse's unloaded Parquet `file`s, read by [`s3_parquet`] with `credentials`. A column of `read` comes from the file, turned back from whole microseconds where it is read as a timestamp (a time never loads this way); a column of `constants` from its value; a merge's row number from `batch`, the file and the row in it, read in stored order; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn unloaded_insert(
    into: &str,
    (columns, sql_type): (&[DesiredColumn], SqlTypeOf<'_>),
    (read, constants): (&[ReadColumn], &[(String, ColumnType, String)]),
    (file, credentials): (&UnloadedFile<'_>, Option<&PullCredentials>),
    (compat, batch, load_id, loaded_at): (Compat, u64, &str, &str),
) -> String {
    let identifier = |name: &str| ClickHouseSql.render_identifier(name);
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            let constant = constants.iter().find(|(name, _, _)| *name == column.name);
            let value = match (metadata_role(compat, &column.name), column.name.as_str()) {
                (Some(MetadataRole::LoadId), _) => typed_literal(load_id, &column.column_type),
                (Some(MetadataRole::LoadedAt), _) => {
                    typed_literal(loaded_at, &column.column_type)
                }
                (None, MERGE_ROW_COLUMN) => format!(
                    "{batch} * {UNLOADED_BATCH_ROWS} + {} * {UNLOADED_FILE_ROWS} + rowNumberInAllBlocks()",
                    file.index
                ),
                _ if constant.is_some() => constant
                    .map(|(_, column_type, value)| typed_literal(value, column_type))
                    .unwrap_or_default(),
                (_, name) => read
                    .iter()
                    .find(|read| read.name == name)
                    .map_or_else(
                        || "NULL".to_owned(),
                        |read| unloaded_value(&read.output, &identifier(name)),
                    ),
            };
            format!("CAST({value} AS {})", sql_type(&column.column_type))
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
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
