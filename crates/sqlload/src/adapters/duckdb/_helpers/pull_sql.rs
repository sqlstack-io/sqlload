//! The statement that has DuckDB load Parquet files itself.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS, UTC_OFFSET};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::{MetadataRole, Naming};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// One file DuckDB reads: where it reads it, and the values SQLLoad gives every row of it (its name and version as the files source records them, and its partition values), by column (`None` for a partition folder the file lacks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledFile {
    pub url: String,
    pub constants: Vec<(String, Option<String>)>,
}

/// `INSERT INTO into` every one of `columns` from the Parquet `files`, matched by name so a file that lacks a column gives NULL. A column comes from the file's constants when it has one of that name (a list of every file's value, indexed by the row's `file_index`: no join, so DuckDB streams the rows into the table instead of holding them), otherwise from the files when at least one of them holds it (`in_files`), and is cast to its type; `_sqlload_source_row` counts each file's rows from 1; the load id and load time are the batch's; any other column is NULL. Every file must carry the same constant columns.
pub(crate) fn pull_insert(
    into: &TableName,
    (columns, in_files): (&[DesiredColumn], &[String]),
    files: &[PulledFile],
    (naming, load_id, loaded_at): (Naming, &str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let cast = |value: String, column: &DesiredColumn| {
        format!(
            "CAST({value} AS {})",
            DuckDbSql
                .render_loader_logical_type(&column.column_type)
                .sql
        )
    };
    let values: Vec<String> = columns
        .iter()
        .map(
            |column| match (metadata_role(naming, &column.name), column.name.as_str()) {
                (Some(MetadataRole::LoadId), _) => cast(literal(load_id), column),
                (Some(MetadataRole::LoadedAt), _) => cast(literal(loaded_at), column),
                (None, SOURCE_ROW_COLUMN) => cast("f.file_row_number + 1".to_owned(), column),
                (_, name)
                    if files
                        .first()
                        .is_some_and(|file| constant(file, name).is_some()) =>
                {
                    cast(
                        format!(
                            "[{}][CAST(f.file_index AS BIGINT) + 1]",
                            constant_list(files, name)
                        ),
                        column,
                    )
                }
                (_, name) if in_files.contains(&name.to_owned()) => {
                    cast(format!("f.{}", identifier(name)), column)
                }
                _ => "NULL".to_owned(),
            },
        )
        .collect();
    let urls: Vec<String> = files.iter().map(|file| literal(&file.url)).collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], union_by_name = true, file_row_number = true, hive_partitioning = false) AS f",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", ")
    )
}

/// `file`'s constant `name`: `Some(None)` when its folder lacks it, `None` when it has no such constant.
fn constant<'a>(file: &'a PulledFile, name: &str) -> Option<Option<&'a str>> {
    file.constants
        .iter()
        .find(|(constant, _)| constant == name)
        .map(|(_, value)| value.as_deref())
}

/// Every file's value of constant `name`, in file order, as literals (NULL when absent).
fn constant_list(files: &[PulledFile], name: &str) -> String {
    files
        .iter()
        .map(|file| {
            constant(file, name).flatten().map_or_else(
                || "NULL".to_owned(),
                |value| DuckDbSql.render_loader_value_literal(value),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What an unloaded batch's insert fills: the columns it writes, those the files hold as they are read, and the table's `constant_columns` (name, type and value as text).
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnloadedColumns<'a> {
    pub columns: &'a [DesiredColumn],
    pub read: &'a [ReadColumn],
    pub constants: &'a [(String, ColumnType, String)],
    /// How the table names the columns a load adds.
    pub naming: Naming,
    /// The batch, from 1: a merge's `_sqlload_row` numbers its rows after earlier batches', by file and row in file.
    pub batch: u64,
}

/// `INSERT INTO into` every one of `columns` from a warehouse's unloaded Parquet files at `urls`: a column of `read` from the files, turned back from whole microseconds where it is read as a time or timestamp, then cast to its type, exactly whatever the session's time zone; a column of `constants` from its value; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn unloaded_insert(
    into: &TableName,
    UnloadedColumns {
        columns,
        read,
        constants,
        naming,
        batch,
    }: UnloadedColumns<'_>,
    urls: &[String],
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            match column_value(
                column,
                (read, constants),
                (naming, load_id, loaded_at),
                batch,
            ) {
                Some(value) => format!(
                    "CAST({value} AS {})",
                    DuckDbSql
                        .render_loader_logical_type(&column.column_type)
                        .sql
                ),
                None => "NULL".to_owned(),
            }
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    let urls: Vec<String> = urls.iter().map(|url| literal(url)).collect();
    let numbered = match columns.iter().any(|column| column.name == MERGE_ROW_COLUMN) {
        true => ", file_row_number = true",
        false => "",
    };
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], hive_partitioning = false{numbered}) AS f",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", ")
    )
}

/// Where an unloaded batch's `column` comes from: the batch's load id or load time, a merge's row number (`batch`, then the file, then the row in it), the value of the constant it is, or the files' column of `read` it is, before its cast; `None` when the files lack it.
fn column_value(
    column: &DesiredColumn,
    (read, constants): (&[ReadColumn], &[(String, ColumnType, String)]),
    (naming, load_id, loaded_at): (Naming, &str, &str),
    batch: u64,
) -> Option<String> {
    let constant = constants
        .iter()
        .find(|(name, _, _)| *name == column.name)
        .map(|(_, _, value)| DuckDbSql.render_loader_value_literal(value));
    match (metadata_role(naming, &column.name), column.name.as_str()) {
        (Some(MetadataRole::LoadId), _) => Some(DuckDbSql.render_loader_value_literal(load_id)),
        (Some(MetadataRole::LoadedAt), _) => Some(DuckDbSql.render_loader_value_literal(loaded_at)),
        (None, MERGE_ROW_COLUMN) => Some(format!(
            "{batch} * {UNLOADED_BATCH_ROWS} + CAST(f.file_index AS BIGINT) * {UNLOADED_FILE_ROWS} + f.file_row_number"
        )),
        _ if constant.is_some() => constant,
        (_, name) => read
            .iter()
            .find(|read| read.name == name)
            .map(|read| unloaded_value(&read.output, &DuckDbSql.render_identifier(name))),
    }
}

/// File column `quoted` as the value it was unloaded from: a time or timestamp from its whole microseconds, anything else as it is. A timestamp with time zone goes through its UTC text with an explicit offset, the one conversion that is exact both with ICU loaded (whatever the session's time zone) and without it.
fn unloaded_value(output: &ColumnType, quoted: &str) -> String {
    let timestamp = format!("make_timestamp(CAST(f.{quoted} AS BIGINT))");
    match output {
        ColumnType::TimestampNtz => timestamp,
        ColumnType::TimestampTz => {
            format!("CAST(CAST({timestamp} AS VARCHAR) || '{UTC_OFFSET}' AS TIMESTAMPTZ)")
        }
        ColumnType::Time => format!("CAST({timestamp} AS TIME)"),
        _ => format!("f.{quoted}"),
    }
}
