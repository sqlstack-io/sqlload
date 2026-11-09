//! BigQuery column types as portable ones.

use serde_json::Value;

use crate::adapters::bigquery::_helpers::dialect::type_names::portable_type;
use crate::column_types::models::{ColumnType, LiveColumnType};
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
    let column_type = match (repeated, base.as_str()) {
        (true, _) | (false, "STRUCT" | "RANGE") => Ok(ColumnType::Json),
        (false, "GEOGRAPHY") => Ok(ColumnType::Text),
        (false, _) => portable_type(&LiveColumnType {
            data_type: base.clone(),
            precision: size(&field["precision"]),
            scale: size(&field["scale"]),
            char_length: size(&field["maxLength"]),
        })
        .ok_or_else(|| {
            format!("BigQuery {shown} has no portable type; pin one in columns, such as text, or exclude the column")
        }),
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

/// A column size, which the REST API gives as a string.
fn size(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => text.parse().ok(),
        other => other.as_i64(),
    }
}
