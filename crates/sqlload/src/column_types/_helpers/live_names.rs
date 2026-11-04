use crate::column_types::constants::{MAX_DECIMAL_PRECISION, SNOWFLAKE_MAX_TEXT_LENGTH};
use crate::column_types::models::{ColumnType, LiveColumnType};

fn decimal(live: &LiveColumnType) -> Option<ColumnType> {
    let precision = live.precision?;
    match (
        u8::try_from(precision),
        u8::try_from(live.scale.unwrap_or(0)),
    ) {
        (Ok(precision), Ok(scale)) if precision <= MAX_DECIMAL_PRECISION => {
            Some(ColumnType::Decimal { precision, scale })
        }
        _ => None,
    }
}

/// A Snowflake `INFORMATION_SCHEMA.COLUMNS` type.
pub(crate) fn snowflake(live: &LiveColumnType) -> Option<ColumnType> {
    Some(match live.data_type.to_ascii_uppercase().as_str() {
        "NUMBER" | "DECIMAL" | "NUMERIC" => match decimal(live)? {
            ColumnType::Decimal {
                precision: MAX_DECIMAL_PRECISION,
                scale: 0,
            } => ColumnType::Int,
            other => other,
        },
        "FLOAT" | "DOUBLE" | "REAL" => ColumnType::Float,
        "TEXT" | "VARCHAR" | "STRING" => match live.char_length {
            Some(length) if length < SNOWFLAKE_MAX_TEXT_LENGTH => match u32::try_from(length) {
                Ok(length) => ColumnType::Varchar(length),
                Err(_) => ColumnType::Text,
            },
            _ => ColumnType::Text,
        },
        "BOOLEAN" => ColumnType::Bool,
        "DATE" => ColumnType::Date,
        "TIME" => ColumnType::Time,
        "TIMESTAMP_NTZ" => ColumnType::TimestampNtz,
        "TIMESTAMP_TZ" => ColumnType::TimestampTz,
        "VARIANT" | "OBJECT" | "ARRAY" => ColumnType::Json,
        "BINARY" => ColumnType::Binary,
        _ => return None,
    })
}

/// A DuckDB `information_schema.columns` type, such as `DECIMAL(18,2)` or `TIMESTAMP WITH TIME ZONE`.
pub(crate) fn duckdb(live: &LiveColumnType) -> Option<ColumnType> {
    let upper = live.data_type.to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    Some(match base {
        "BOOLEAN" => ColumnType::Bool,
        "SMALLINT" | "TINYINT" | "UTINYINT" => ColumnType::SmallInt,
        "INTEGER" | "BIGINT" | "USMALLINT" | "UINTEGER" => ColumnType::Int,
        "DECIMAL" => decimal(live)?,
        "DOUBLE" | "FLOAT" => ColumnType::Float,
        "VARCHAR" => ColumnType::Text,
        "DATE" => ColumnType::Date,
        "TIME" => ColumnType::Time,
        "TIMESTAMP WITH TIME ZONE" => ColumnType::TimestampTz,
        "TIMESTAMP" | "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" => ColumnType::TimestampNtz,
        "JSON" => ColumnType::Json,
        "UUID" => ColumnType::Uuid,
        "BLOB" => ColumnType::Binary,
        _ => return None,
    })
}
