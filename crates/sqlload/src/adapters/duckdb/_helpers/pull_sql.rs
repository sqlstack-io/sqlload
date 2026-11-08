//! The statement that has DuckDB load Parquet files itself.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::models::DuckDbSql;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
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
    (load_id, loaded_at): (&str, &str),
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
        .map(|column| match column.name.as_str() {
            LOAD_ID_COLUMN => cast(literal(load_id), column),
            LOADED_AT_COLUMN => cast(literal(loaded_at), column),
            SOURCE_ROW_COLUMN => cast("f.file_row_number + 1".to_owned(), column),
            name if files
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
            name if in_files.contains(&name.to_owned()) => {
                cast(format!("f.{}", identifier(name)), column)
            }
            _ => "NULL".to_owned(),
        })
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
