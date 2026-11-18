//! The `INSERT` that has BigQuery read a files batch's Parquet files where they lie in GCS.

use crate::adapters::bigquery::_helpers::dialect::type_names::decimal_digits;
use crate::adapters::bigquery::constants::GCS_FILES_TABLE;
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::MetadataRole;
use crate::schema_plan::models::DesiredColumn;

/// One batch read from GCS: each file's `gs://` URI with the values SQLLoad gives every row of it (`None` for NULL), by column, and each table column the files hold with its name in them, which `naming`, `rename` or numbering may have changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcsBatch {
    pub files: Vec<(String, Vec<(String, Option<String>)>)>,
    pub in_files: Vec<(String, String)>,
}

/// `INSERT INTO into` every one of `columns`, cast to the type it lands as ([`cast_type`]), from [`GCS_FILES_TABLE`] over `batch`'s files: a column takes the file's own value when SQLLoad gives it one (picked by `_FILE_NAME`, and a row from any other file fails the statement), the files' column when they hold it, and NULL otherwise; the load id and load time are the batch's. `_sqlload_source_row` is never among `columns`: BigQuery reads only tables that leave it out.
pub(crate) fn gcs_insert_statement(
    (into, columns): (&str, &[DesiredColumn]),
    target: &WriteTarget<'_>,
    batch: &GcsBatch,
    loaded_at: &str,
) -> String {
    let adapter = BigQuerySql;
    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    let mut values: Vec<String> = Vec::with_capacity(columns.len());
    for column in columns {
        names.push(adapter.render_identifier(&column.name));
        values.push(format!(
            "CAST({} AS {})",
            column_value(target, batch, column, loaded_at),
            cast_type(&column.column_type)
        ));
    }
    format!(
        "INSERT INTO {into} ({}) SELECT {} FROM {GCS_FILES_TABLE}",
        names.join(", "),
        values.join(", ")
    )
}

/// The type a value is cast to before it lands as `column_type`: its BigQuery type without parameters, which `CAST` refuses (the column's own then apply, and plan lets BigQuery read only files holding each column as the type it lands as).
fn cast_type(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Decimal { precision, scale } => match decimal_digits(*precision, *scale).2 {
            true => "BIGNUMERIC".to_owned(),
            false => "NUMERIC".to_owned(),
        },
        ColumnType::Varchar(_) => "STRING".to_owned(),
        other => BigQuerySql.render_loader_logical_type(other).sql,
    }
}

/// Where `column`'s value comes from in `batch`, before it is cast: the value SQLLoad gives each file, the load id or load time, the files' own column, or NULL when no file holds it.
fn column_value(
    target: &WriteTarget<'_>,
    batch: &GcsBatch,
    column: &DesiredColumn,
    loaded_at: &str,
) -> String {
    let literal = |text: &str| BigQuerySql.render_loader_value_literal(text);
    let mut given: Vec<(&str, Option<&String>)> = Vec::new();
    for (uri, constants) in &batch.files {
        if let Some((_, value)) = constants.iter().find(|(name, _)| *name == column.name) {
            given.push((uri, value.as_ref()));
        }
    }
    let held = batch.in_files.iter().find(|(name, _)| *name == column.name);
    match (metadata_role(target.compat, &column.name), held) {
        _ if !given.is_empty() => per_file(&given),
        (Some(MetadataRole::LoadId), _) => literal(target.load_id),
        (Some(MetadataRole::LoadedAt), _) => literal(loaded_at),
        (_, Some((_, held))) => format!(
            "{GCS_FILES_TABLE}.{}",
            BigQuerySql.render_exact_identifier(held)
        ),
        (_, None) => "NULL".to_owned(),
    }
}

/// The value each of `given`'s files gives a column, picked by the file a row was read from; a file not in the batch fails the statement rather than load a row without it.
fn per_file(given: &[(&str, Option<&String>)]) -> String {
    let literal = |text: &str| BigQuerySql.render_loader_value_literal(text);
    let picks: Vec<String> = given
        .iter()
        .map(|(uri, value)| {
            format!(
                "WHEN {} THEN {}",
                literal(uri),
                value.map_or_else(|| "NULL".to_owned(), |value| literal(value))
            )
        })
        .collect();
    format!(
        "CASE _FILE_NAME {} ELSE ERROR(CONCAT('sqlload: the query read a file outside the batch: ', _FILE_NAME)) END",
        picks.join(" ")
    )
}
