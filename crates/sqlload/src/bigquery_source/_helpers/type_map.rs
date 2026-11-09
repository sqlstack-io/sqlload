//! BigQuery column types as portable ones.

use serde_json::Value;

use crate::bigquery_source::constants::NUMERIC_DEFAULT;
use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, SourceColumn};

/// A column as `tables.get` or a dry run describes it (`name`, `type`, `mode`, and its sizes): records and repeated columns as `json` (an `array(T)` pin reads a repeated scalar as a list), `GEOGRAPHY` as its WKT text, `RANGE` as `json`; a type with no portable one (`INTERVAL`, a `BIGNUMERIC` wider than 38 digits) must be pinned or excluded.
pub(crate) fn source_column(field: &Value) -> SourceColumn {
    let name = field["name"].as_str().unwrap_or_default().to_owned();
    let base = match field["type"].as_str().unwrap_or_default() {
        "RECORD" | "STRUCT" => "STRUCT".to_owned(),
        other => other.to_ascii_uppercase(),
    };
    let repeated = field["mode"].as_str() == Some("REPEATED");
    let shown = match repeated {
        true => format!("ARRAY<{base}>"),
        false => base.clone(),
    };
    let sizes = (
        size(&field["precision"]),
        size(&field["scale"]),
        size(&field["maxLength"]),
    );
    let column_type = match (repeated, base.as_str(), sizes) {
        (true, _, _) | (false, "STRUCT" | "RANGE", _) => Ok(ColumnType::Json),
        (false, "GEOGRAPHY", _) => Ok(ColumnType::Text),
        (false, _, (Some(precision), Some(scale), Some(char_length))) => {
            scalar_type(&base, (precision, scale, char_length)).ok_or_else(|| {
                format!(
                    "BigQuery {shown} has no portable type; pin one in columns, such as text, or exclude the column"
                )
            })
        }
        (false, _, _) => Err(format!(
            "BigQuery reported a size for {shown} that is not a number; pin a type in columns"
        )),
    };
    SourceColumn {
        name,
        postgres_type: shown,
        pg_type: PgType::Other,
        column_type,
        nullable: field["mode"].as_str() != Some("REQUIRED"),
        source_spelling: None,
    }
}

/// The portable type of a scalar BigQuery type with its reported sizes (`INT64` or its legacy name `INTEGER`, `NUMERIC` with its precision and scale or [`NUMERIC_DEFAULT`], `STRING` with its `maxLength`); `None` for a type with no portable one, such as `INTERVAL` or a `BIGNUMERIC` wider than the portable range.
fn scalar_type(
    base: &str,
    (precision, scale, char_length): (Option<i64>, Option<i64>, Option<i64>),
) -> Option<ColumnType> {
    let decimal = |default: Option<(u8, u8)>| {
        let (precision, scale) = match (precision, default) {
            (Some(precision), _) => {
                match (u8::try_from(precision), u8::try_from(scale.unwrap_or(0))) {
                    (Ok(precision), Ok(scale)) => (precision, scale),
                    _ => return None,
                }
            }
            (None, Some(default)) => default,
            (None, None) => return None,
        };
        (precision <= MAX_DECIMAL_PRECISION).then_some(ColumnType::Decimal { precision, scale })
    };
    Some(match base {
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "INT64" | "INTEGER" => ColumnType::Int,
        "NUMERIC" => decimal(Some(NUMERIC_DEFAULT))?,
        "BIGNUMERIC" => decimal(None)?,
        "FLOAT64" | "FLOAT" => ColumnType::Float,
        "STRING" => match char_length.map(u32::try_from) {
            Some(Ok(length)) => ColumnType::Varchar(length),
            _ => ColumnType::Text,
        },
        "BYTES" => ColumnType::Binary,
        "DATE" => ColumnType::Date,
        "TIME" => ColumnType::Time,
        "DATETIME" => ColumnType::TimestampNtz,
        "TIMESTAMP" => ColumnType::TimestampTz,
        "JSON" => ColumnType::Json,
        _ => return None,
    })
}

/// A column size, which the REST API gives as a string: `Some(None)` when it gives none, `None` when it isn't a number.
fn size(value: &Value) -> Option<Option<i64>> {
    match value {
        Value::Null => Some(None),
        Value::String(text) => match text.parse::<i64>() {
            Ok(number) => Some(Some(number)),
            Err(_) => None,
        },
        other => other.as_i64().map(Some),
    }
}
