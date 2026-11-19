//! The query that has BigQuery load Parquet files in GCS itself.

use serde_json::{Map, Value, json};

use crate::adapters::bigquery::constants::{
    DECIMAL_TARGET_TYPES, MAX_COLUMN_NAME, RESERVED_COLUMN_PREFIXES,
};
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::schema_plan::models::DesiredColumn;

/// One file BigQuery reads: its `gs://` URL, as `_FILE_NAME` reports it, and the values SQLLoad gives every row of it (its name and version as the files source records them, and its partition values, `None` for a partition folder it lacks), by column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledFile {
    pub url: String,
    pub constants: Vec<(String, Option<String>)>,
}

/// Files BigQuery reads through one temporary external table, which takes one file's schema for all of its files: files whose footers hold the same columns, and the table's columns they hold, each with its name in the files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledGroup {
    pub files: Vec<PulledFile>,
    pub in_files: Vec<(String, String)>,
}

/// The `INSERT INTO into` of every one of `columns` from the Parquet files of `groups`, and the `tableDefinitions` of the query job running it: one temporary external table per group, its rows appended to the others'. A column comes from the row's file's constants when the files have one of that name (picked by `_FILE_NAME`), otherwise from the group's files when they hold it, and is cast to its type as `sql_type` names it, without its parameters (`NUMERIC(12,2)` as `NUMERIC`), which a cast can't name and the column then checks; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn pull_query(
    into: &TableName,
    (columns, sql_type): (&[DesiredColumn], &dyn Fn(&ColumnType) -> String),
    groups: &[PulledGroup],
    (compat, load_id, loaded_at): (Compat, &str, &str),
) -> (String, Value) {
    let literal = |text: &str| BigQuerySql.render_loader_value_literal(text);
    let selects: Vec<String> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let values: Vec<String> = columns
                .iter()
                .map(|column| {
                    let value = match (metadata_role(compat, &column.name), column.name.as_str()) {
                        (Some(MetadataRole::LoadId), _) => literal(load_id),
                        (Some(MetadataRole::LoadedAt), _) => literal(loaded_at),
                        (_, name)
                            if group
                                .files
                                .first()
                                .is_some_and(|file| constant(file, name).is_some()) =>
                        {
                            by_file(&group.files, name)
                        }
                        (_, name) if let Some(file) = in_file(&group.in_files, name) => {
                            format!("f.{}", BigQuerySql.render_exact_identifier(file))
                        }
                        _ => "NULL".to_owned(),
                    };
                    let sql_type = sql_type(&column.column_type);
                    let base = sql_type
                        .split_once('(')
                        .map_or(sql_type.as_str(), |(base, _)| base);
                    format!("CAST({value} AS {base})")
                })
                .collect();
            format!(
                "SELECT {} FROM {} AS f",
                values.join(", "),
                group_table(index)
            )
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| BigQuerySql.render_identifier(&column.name))
        .collect();
    let query = format!(
        "INSERT INTO {} ({}) {}",
        BigQuerySql.render_qualified_name(into),
        targets.join(", "),
        selects.join(" UNION ALL ")
    );
    let definitions: Map<String, Value> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let urls: Vec<&str> = group.files.iter().map(|file| file.url.as_str()).collect();
            (group_table(index), table_definition(&urls))
        })
        .collect();
    (query, Value::Object(definitions))
}

/// A temporary external table over the Parquet files at `urls`, its decimals read as the first of BigQuery's types holding their precision and scale.
pub(crate) fn table_definition(urls: &[&str]) -> Value {
    json!({
        "sourceFormat": "PARQUET",
        "sourceUris": urls,
        "decimalTargetTypes": DECIMAL_TARGET_TYPES,
    })
}

/// The temporary external table of the `index`th group of files.
pub(crate) fn group_table(index: usize) -> String {
    format!("sqlload_files_{index}")
}

/// The name in the files of the column the table holds as `name`, when the files hold it.
fn in_file<'a>(in_files: &'a [(String, String)], name: &str) -> Option<&'a str> {
    in_files
        .iter()
        .find(|(held, _)| held == name)
        .map(|(_, file)| file.as_str())
}

/// `file`'s constant `name`: `Some(None)` when its folder lacks it, `None` when it has no such constant.
fn constant<'a>(file: &'a PulledFile, name: &str) -> Option<Option<&'a str>> {
    file.constants
        .iter()
        .find(|(constant, _)| constant == name)
        .map(|(_, value)| value.as_deref())
}

/// The row's file's value of constant `name`, as text: each file's value picked by `_FILE_NAME`, a typed NULL where its folder lacks it, so a column no file has a value of still casts to its type.
fn by_file(files: &[PulledFile], name: &str) -> String {
    let cases: Vec<String> = files
        .iter()
        .map(|file| {
            let value = constant(file, name).flatten().map_or_else(
                || "CAST(NULL AS STRING)".to_owned(),
                |value| BigQuerySql.render_loader_value_literal(value),
            );
            format!(
                "WHEN {} THEN {value}",
                BigQuerySql.render_loader_value_literal(&file.url)
            )
        })
        .collect();
    format!("CASE _FILE_NAME {} END", cases.join(" "))
}

/// Why BigQuery would not read Parquet columns named `names` under those names: one that is not letters, digits and underscores starting with a letter or underscore, which BigQuery renames (`Order ID` reads as `Order_ID`), starts with a prefix BigQuery reserves, or differs from another only in case; `None` when it reads every one as it is.
pub(crate) fn unread_name<'a>(names: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut seen: Vec<String> = Vec::new();
    for name in names {
        let folded = name.to_ascii_uppercase();
        let plain = name.len() <= MAX_COLUMN_NAME
            && name
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_');
        let problem = match (
            plain,
            RESERVED_COLUMN_PREFIXES
                .iter()
                .find(|prefix| folded.starts_with(*prefix)),
            seen.contains(&folded),
        ) {
            (false, _, _) => "BigQuery renames a name that isn't letters, digits and underscores starting with a letter or underscore".to_owned(),
            (true, Some(prefix), _) => format!("{prefix} is a prefix BigQuery reserves"),
            (true, None, true) => "another column has the same name in another case, and BigQuery's names ignore case".to_owned(),
            (true, None, false) => {
                seen.push(folded);
                continue;
            }
        };
        return Some(format!(
            "BigQuery would not read column {name:?} as it is: {problem}"
        ));
    }
    None
}
