//! The statement that has BigQuery select a BigQuery source's rows itself.

use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::contract::constants::UNLOADED_BATCH_ROWS;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;

/// One batch of a BigQuery source's rows BigQuery selects itself, into the staging table or, when it `builds`, a build table in the final types.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SelectedLoad<'a> {
    pub into: &'a TableName,
    pub columns: &'a [DesiredColumn],
    /// The source's read columns, each with its quoted name in `query`'s result.
    pub read: &'a [ReadColumn],
    /// The table's `constant_columns`: name, type and value as text.
    pub constants: &'a [(String, ColumnType, String)],
    pub compat: Compat,
    /// The batch, from 1: a merge's row number counts after earlier batches'.
    pub batch: u64,
    pub builds: bool,
    pub load_id: &'a str,
    pub loaded_at: &'a str,
    /// The `SELECT` of the source's rows.
    pub query: &'a str,
}

/// `INSERT INTO into` every one of `columns` from the rows of `query`: a read column by its name in the query's result, a constant from its value, a merge's row number after the earlier batches' in the order the query gives the rows, the load id and load time the batch's; any other column NULL. Each is cast to its staging or build type without parameters, which a cast can't name and the column then checks; JSON goes into a staging table as its text, as SQLLoad's reader stages it.
pub(crate) fn selected_insert(load: &SelectedLoad<'_>) -> String {
    let literal = |text: &str| BigQuerySql.render_loader_value_literal(text);
    let mut values: Vec<String> = Vec::with_capacity(load.columns.len());
    for column in load.columns {
        let constant = load
            .constants
            .iter()
            .find(|(name, _, _)| *name == column.name)
            .map(|(_, _, value)| literal(value));
        let read = load
            .read
            .iter()
            .find(|read| read.name == column.name)
            .map(|read| format!("u.{}", read.expression));
        let value = match (
            metadata_role(load.compat, &column.name),
            column.name.as_str(),
        ) {
            (Some(MetadataRole::LoadId), _) => literal(load.load_id),
            (Some(MetadataRole::LoadedAt), _) => literal(load.loaded_at),
            (None, MERGE_ROW_COLUMN) => {
                values.push(format!(
                    "{} * {UNLOADED_BATCH_ROWS} + ROW_NUMBER() OVER () - 1",
                    load.batch
                ));
                continue;
            }
            _ => constant.or(read).unwrap_or_else(|| "NULL".to_owned()),
        };
        values.push(typed(&value, &column.column_type, load.builds));
    }
    let targets: Vec<String> = load
        .columns
        .iter()
        .map(|column| BigQuerySql.render_identifier(&column.name))
        .collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM ({}) AS u",
        BigQuerySql.render_qualified_name(load.into),
        targets.join(", "),
        values.join(", "),
        load.query
    )
}

/// `value` as a column of `column_type` holds it in the staging table, or in a build table (`builds`): JSON as its text in staging and as JSON in a build table (parsed from a constant's text), anything else cast to its type without parameters.
fn typed(value: &str, column_type: &ColumnType, builds: bool) -> String {
    let from_text = value.starts_with('\'') || value == "NULL";
    match (column_type, builds, from_text) {
        (ColumnType::Json, false, false) => format!("TO_JSON_STRING({value})"),
        (ColumnType::Json, true, false) => value.to_owned(),
        (ColumnType::Json, true, true) => format!("PARSE_JSON({value})"),
        _ => {
            let sql_type = match builds {
                true => BigQuerySql.render_loader_logical_type(column_type).sql,
                false => BigQuerySql.render_staging_type(column_type),
            };
            let base = sql_type
                .split_once('(')
                .map_or(sql_type.as_str(), |(base, _)| base);
            format!("CAST({value} AS {base})")
        }
    }
}
