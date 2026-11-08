//! The statement that has DuckDB load Parquet files itself.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::constants::UTC_OFFSET;
use crate::adapters::duckdb::models::DuckDbSql;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::DesiredColumn;

/// One file DuckDB reads: where it reads it, and the values SQLLoad gives every row of it (its name and version as the files source records them, and its partition values), by column (`None` for a partition folder the file lacks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledFile {
    pub url: String,
    pub constants: Vec<(String, Option<String>)>,
}

/// `INSERT INTO into` every one of `columns` from the Parquet `files`, matched by name so a file that lacks a column gives NULL. A column comes from the file's constants when it has one of that name, otherwise from the files when at least one of them holds it (`in_files`), and is cast to its type; `_sqlload_source_row` counts each file's rows from 1; the load id and load time are the batch's; any other column is NULL. Every file must carry the same constant columns.
pub(crate) fn pull_insert(
    into: &TableName,
    (columns, in_files): (&[DesiredColumn], &[String]),
    files: &[PulledFile],
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let constant_names: Vec<&str> = files.first().map_or_else(Vec::new, constant_names);
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
        .map(|column| match column.name.as_str() {
            LOAD_ID_COLUMN => cast(literal(load_id), column),
            LOADED_AT_COLUMN => cast(literal(loaded_at), column),
            SOURCE_ROW_COLUMN => cast("f.file_row_number + 1".to_owned(), column),
            name if constant_names.contains(&name) => {
                cast(format!("m.{}", identifier(name)), column)
            }
            name if in_files.contains(&name.to_owned()) => {
                cast(format!("f.{}", identifier(name)), column)
            }
            _ => "NULL".to_owned(),
        })
        .collect();
    let urls: Vec<String> = files.iter().map(|file| literal(&file.url)).collect();
    let rows: Vec<String> = files.iter().map(values_row).collect();
    let names: Vec<String> = std::iter::once(identifier("_sqlload_url"))
        .chain(constant_names.iter().map(|name| identifier(name)))
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], union_by_name = true, filename = true, file_row_number = true, hive_partitioning = false) AS f JOIN (VALUES {}) AS m({}) ON f.filename = m.{}",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", "),
        rows.join(", "),
        names.join(", "),
        identifier("_sqlload_url")
    )
}

/// The names of `file`'s constant columns, in order.
fn constant_names(file: &PulledFile) -> Vec<&str> {
    file.constants
        .iter()
        .map(|(name, _)| name.as_str())
        .collect()
}

/// `file`'s row of the per-file values: its URL, then each constant as a literal (NULL when absent).
fn values_row(file: &PulledFile) -> String {
    let mut row = vec![DuckDbSql.render_loader_value_literal(&file.url)];
    for (_, value) in &file.constants {
        row.push(value.as_deref().map_or_else(
            || "NULL".to_owned(),
            |value| DuckDbSql.render_loader_value_literal(value),
        ));
    }
    format!("({})", row.join(", "))
}

/// `INSERT INTO into` every one of `columns` from a warehouse's unloaded Parquet files at `urls`: a column of `read` from the files, turned back from whole microseconds where it is read as a time or timestamp, then cast to its type, exactly whatever the session's time zone; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn unloaded_insert(
    into: &TableName,
    (columns, read): (&[DesiredColumn], &[ReadColumn]),
    urls: &[String],
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let values: Vec<String> = columns
        .iter()
        .map(
            |column| match column_value(column, read, (load_id, loaded_at)) {
                Some(value) => format!(
                    "CAST({value} AS {})",
                    DuckDbSql
                        .render_loader_logical_type(&column.column_type)
                        .sql
                ),
                None => "NULL".to_owned(),
            },
        )
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    let urls: Vec<String> = urls.iter().map(|url| literal(url)).collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], hive_partitioning = false) AS f",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", ")
    )
}

/// Where an unloaded batch's `column` comes from: the batch's load id or load time, or the files' column of `read` it is, before its cast; `None` when the files lack it.
fn column_value(
    column: &DesiredColumn,
    read: &[ReadColumn],
    (load_id, loaded_at): (&str, &str),
) -> Option<String> {
    match column.name.as_str() {
        LOAD_ID_COLUMN => Some(DuckDbSql.render_loader_value_literal(load_id)),
        LOADED_AT_COLUMN => Some(DuckDbSql.render_loader_value_literal(loaded_at)),
        name => read
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
