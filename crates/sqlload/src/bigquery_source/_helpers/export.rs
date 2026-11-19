//! The `EXPORT DATA` statement a BigQuery source exports a table's rows to Parquet with, and which columns it can export.

use crate::bigquery_source::_helpers::query::quote_identifier;
use crate::bigquery_source::models::BigQueryReadRequest;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, SourceColumn};
use crate::store_paths::models::Location;

/// How a column of BigQuery type `held` goes into the export, `expression` being how SQLLoad's reader selects it: as it is, which Parquet holds exactly; a `TIMESTAMP` as its microseconds since the epoch, a `DATETIME` as its microseconds since midnight of 1970-01-01, and a `TIME` as its microseconds since midnight, as the destination reads an unloaded time; `None` for a type it doesn't export exactly.
pub(crate) fn export_expression(held: &str, expression: &str) -> Option<String> {
    match held {
        "INT64" | "INTEGER" | "FLOAT64" | "FLOAT" | "BOOL" | "BOOLEAN" | "STRING" | "BYTES"
        | "DATE" | "NUMERIC" => Some(expression.to_owned()),
        "TIMESTAMP" => Some(format!("UNIX_MICROS({expression})")),
        "DATETIME" => Some(format!("UNIX_MICROS(TIMESTAMP({expression}, 'UTC'))")),
        "TIME" => Some(format!(
            "TIME_DIFF({expression}, TIME '00:00:00', MICROSECOND)"
        )),
        _ => None,
    }
}

/// Why SQLLoad's reader reads `column`, rather than an export, when it is `pinned` to a type: a type the export writes inexactly or refuses (`JSON`, `RANGE` and `INTERVAL` aren't exported to Parquet, `BIGNUMERIC` holds more digits than DuckDB, and records, repeated columns and geographies are converted to JSON or text by the reader), or a pin to another type than its own, which the reader converts.
pub(crate) fn column_refusal(column: &SourceColumn, pinned: Option<&ColumnType>) -> Option<String> {
    let held = column.postgres_type.as_str();
    match (export_expression(held, ""), pinned) {
        (None, _) => Some(format!(
            "{} is BigQuery {held}, which BigQuery doesn't export to Parquet as SQLLoad loads it",
            column.name
        )),
        (Some(_), Some(pinned)) if column.column_type.as_ref() != Ok(pinned) => Some(format!(
            "{} is pinned as {pinned}, which SQLLoad converts from BigQuery {held}",
            column.name
        )),
        (Some(_), _) => None,
    }
}

/// `request`'s read columns as the export selects them, each under its read name.
pub(crate) fn export_columns(request: &BigQueryReadRequest<'_>) -> Result<Vec<ReadColumn>, String> {
    request
        .columns
        .iter()
        .map(|read| {
            let column = request
                .table
                .columns
                .iter()
                .find(|column| column.name == read.name)
                .ok_or_else(|| format!("{} is not a column of the source", read.name))?;
            let expression = export_expression(&column.postgres_type, &read.expression)
                .ok_or_else(|| {
                    column_refusal(column, None).unwrap_or_else(|| read.name.clone())
                })?;
            Ok(ReadColumn {
                expression: format!("{expression} AS {}", quote_identifier(&read.name)),
                ..read.clone()
            })
        })
        .collect()
}

/// `EXPORT DATA` of `select` as Parquet files in `folder`, which must be new: BigQuery names the files.
pub(crate) fn export_statement(folder: &Location, select: &str) -> String {
    let uri = format!("{folder}*.parquet")
        .replace('\\', "\\\\")
        .replace('\'', "\\'");
    format!("EXPORT DATA OPTIONS (uri = '{uri}', format = 'PARQUET') AS {select}")
}
