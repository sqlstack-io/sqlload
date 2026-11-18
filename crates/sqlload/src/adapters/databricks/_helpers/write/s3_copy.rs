//! The `COPY INTO` that has Databricks read a files batch's Parquet files where they lie in S3.

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::write::copy_select::typed_value;
use crate::adapters::databricks::constants::S3_COPY_OPTIONS;
use crate::adapters::databricks::models::DatabricksSql;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::schema_plan::models::DesiredColumn;

/// The path `COPY INTO` reads each row from, as SQLLoad names it: `_metadata.file_path` is percent-encoded, except for `+`, which `url_decode` alone would read as a space.
const FILE_PATH: &str = "url_decode(replace(_metadata.file_path, '+', '%2B'))";

/// One batch read from S3: the folder `COPY INTO` reads (`s3://bucket/prefix/`), and for each file its key below it and the values SQLLoad gives every row of it (`None` for NULL), by column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3Batch {
    pub folder: String,
    pub files: Vec<S3File>,
    /// Each table column the batch's files hold, with its name in them, which `naming`, `rename` or numbering may have changed.
    pub in_files: Vec<(String, String)>,
}

/// One file of an [`S3Batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3File {
    pub key: String,
    pub constants: Vec<(String, Option<String>)>,
}

/// `COPY INTO into` every one of `columns`, typed as `target` lands them, from `batch`'s files: a column takes the file's own value when SQLLoad gives it one (picked by the file's path, and a row from any other path fails the statement), the files' column when they hold it, and NULL otherwise; `_sqlload_source_row` numbers each file's rows from 1 in stored order; the load id and load time are the batch's.
pub(crate) fn s3_copy_statement(
    (into, columns): (&str, &[DesiredColumn]),
    target: &WriteTarget<'_>,
    batch: &S3Batch,
    loaded_at: &str,
) -> String {
    let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
    let mut values: Vec<String> = Vec::with_capacity(columns.len());
    for column in columns {
        values.push(format!(
            "{} AS {}",
            typed_value(
                target,
                column,
                &column_value(target, batch, column, loaded_at)
            ),
            DatabricksSql.render_identifier(&column.name)
        ));
    }
    let keys: Vec<String> = batch.files.iter().map(|file| literal(&file.key)).collect();
    format!(
        "COPY INTO {into} FROM (SELECT {} FROM {}) FILEFORMAT = PARQUET FILES = ({}) {S3_COPY_OPTIONS}",
        values.join(", "),
        literal(&batch.folder),
        keys.join(", ")
    )
}

/// Where `column`'s value comes from in `batch`, before it is typed: the value SQLLoad gives each file, the load id or load time, the row's position in its file, the files' own column, or NULL when no file holds it.
fn column_value(
    target: &WriteTarget<'_>,
    batch: &S3Batch,
    column: &DesiredColumn,
    loaded_at: &str,
) -> String {
    let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
    let mut given: Vec<(&S3File, Option<&String>)> = Vec::new();
    for file in &batch.files {
        if let Some((_, value)) = file.constants.iter().find(|(name, _)| *name == column.name) {
            given.push((file, value.as_ref()));
        }
    }
    let held = batch.in_files.iter().find(|(name, _)| *name == column.name);
    match (metadata_role(target.compat, &column.name), held) {
        _ if !given.is_empty() => per_file(batch, &given),
        (Some(MetadataRole::LoadId), _) => literal(target.load_id),
        (Some(MetadataRole::LoadedAt), _) => literal(loaded_at),
        _ if column.name == SOURCE_ROW_COLUMN => "_metadata.row_index + 1".to_owned(),
        (_, Some((_, held))) => DatabricksSql.render_exact_identifier(held),
        (_, None) => "NULL".to_owned(),
    }
}

/// The value each of `given`'s files gives a column, picked by the path a row was read from; a path not in the batch fails the statement rather than load a row without it.
fn per_file(batch: &S3Batch, given: &[(&S3File, Option<&String>)]) -> String {
    let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
    let picks: Vec<String> = given
        .iter()
        .map(|(file, value)| {
            format!(
                "WHEN {} THEN {}",
                literal(&format!("{}{}", batch.folder, file.key)),
                value.map_or_else(|| "NULL".to_owned(), |value| literal(value))
            )
        })
        .collect();
    format!(
        "CASE {FILE_PATH} {} ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END",
        picks.join(" ")
    )
}
