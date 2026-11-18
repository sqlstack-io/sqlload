//! The `COPY INTO` that has Databricks read files a warehouse source unloaded to S3.

use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::write::copy_select::typed_value;
use crate::adapters::databricks::_helpers::write::s3_copy::FILE_PATH;
use crate::adapters::databricks::constants::S3_COPY_OPTIONS;
use crate::adapters::databricks::models::DatabricksSql;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// Some of one unload's files: the folder `COPY INTO` reads (`s3://bucket/prefix/`), their keys below it, and the place of the first among the unload's files, from 0, which a merge's row number counts by.
pub(crate) struct UnloadedGroup<'a> {
    pub folder: &'a str,
    pub keys: &'a [String],
    pub first: usize,
}

/// `COPY INTO into` every one of `columns`, typed as `target` lands them, from `group`'s files: a column of `read` from the files, turned back from whole microseconds where it is read as a timestamp (a time never loads this way); a column of `target`'s constants from its value; a merge's row number from the batch, the file (picked by its path, and a row from any other path fails the statement) and the row in it; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn unloaded_copy_statement(
    (into, columns): (&str, &[DesiredColumn]),
    target: &WriteTarget<'_>,
    (read, group): (&[ReadColumn], &UnloadedGroup<'_>),
    loaded_at: &str,
) -> String {
    let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            let value = column_value(target, (read, group), column, loaded_at);
            format!(
                "{} AS {}",
                typed_value(target, column, &value),
                DatabricksSql.render_identifier(&column.name)
            )
        })
        .collect();
    let keys: Vec<String> = group.keys.iter().map(|key| literal(key)).collect();
    format!(
        "COPY INTO {into} FROM (SELECT {} FROM {}) FILEFORMAT = PARQUET FILES = ({}) {S3_COPY_OPTIONS}",
        values.join(", "),
        literal(group.folder),
        keys.join(", ")
    )
}

/// Where `column`'s value comes from, before it is typed: the load id or load time, a merge's row number, a constant's value, the files' column of `read` it is, or NULL when the files lack it.
fn column_value(
    target: &WriteTarget<'_>,
    (read, group): (&[ReadColumn], &UnloadedGroup<'_>),
    column: &DesiredColumn,
    loaded_at: &str,
) -> String {
    let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
    let constant = target
        .constants
        .iter()
        .find(|(name, _, _)| *name == column.name);
    match (metadata_role(target.compat, &column.name), constant) {
        (Some(MetadataRole::LoadId), _) => literal(target.load_id),
        (Some(MetadataRole::LoadedAt), _) => literal(loaded_at),
        _ if column.name == MERGE_ROW_COLUMN => {
            let files: Vec<String> = group
                .keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    format!(
                        "WHEN {} THEN {}",
                        literal(&format!("{}{key}", group.folder)),
                        group.first + index
                    )
                })
                .collect();
            format!(
                "{} * {UNLOADED_BATCH_ROWS} + (CASE {FILE_PATH} {} ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END) * {UNLOADED_FILE_ROWS} + _metadata.row_index",
                target.batch,
                files.join(" ")
            )
        }
        (_, Some((_, _, value))) => literal(value),
        _ => read
            .iter()
            .find(|read| read.name == column.name)
            .map_or_else(
                || "NULL".to_owned(),
                |read| {
                    unloaded_value(
                        &read.output,
                        &DatabricksSql.render_exact_identifier(&read.name),
                    )
                },
            ),
    }
}

/// File column `quoted` as the value it was unloaded from: a timestamp with time zone as the instant its whole microseconds count from the epoch, one without as the wall-clock time they count from midnight of 1970-01-01, neither depending on the session's time zone; anything else as it is.
fn unloaded_value(output: &ColumnType, quoted: &str) -> String {
    match output {
        ColumnType::TimestampTz => format!("timestamp_micros(CAST({quoted} AS BIGINT))"),
        ColumnType::TimestampNtz => format!(
            "timestampadd(MICROSECOND, CAST({quoted} AS BIGINT), TIMESTAMP_NTZ'1970-01-01 00:00:00')"
        ),
        _ => quoted.to_owned(),
    }
}
