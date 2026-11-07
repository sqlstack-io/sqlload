//! BigQuery's spelling of each portable type, and the way back from what it reports.

use crate::adapters::bigquery::constants::{
    NUMERIC_DEFAULT, NUMERIC_MAX_INTEGER_DIGITS, NUMERIC_MAX_SCALE,
};
use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type BigQuery holds exactly.
fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

/// A type BigQuery holds with a loss, and why.
fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(reason),
    }
}

/// `decimal(precision,scale)` as a parameterised `NUMERIC` when it fits one, else `BIGNUMERIC`; both need a digit before the point, so a decimal of only fraction digits gets one more digit of precision.
fn decimal_type(precision: u8, scale: u8) -> String {
    let precision = precision.max(scale + 1);
    let name = match precision - scale <= NUMERIC_MAX_INTEGER_DIGITS && scale <= NUMERIC_MAX_SCALE {
        true => "NUMERIC",
        false => "BIGNUMERIC",
    };
    format!("{name}({precision},{scale})")
}

/// BigQuery's DDL types.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOL"),
        ColumnType::SmallInt => lossy("INT64", "stored as a 64-bit integer"),
        ColumnType::Int => exact("INT64"),
        ColumnType::Decimal { precision, scale } => exact(decimal_type(*precision, *scale)),
        ColumnType::Float => exact("FLOAT64"),
        ColumnType::Text => exact("STRING"),
        ColumnType::Varchar(length) => exact(format!("STRING({length})")),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("DATETIME"),
        ColumnType::Json => exact("JSON"),
        ColumnType::Uuid => lossy("STRING", "stored as text"),
        ColumnType::Binary => exact("BYTES"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

/// A decimal with the reported precision and scale (`default` when the column declares none), when it fits the portable range.
fn decimal(live: &LiveColumnType, default: Option<(u8, u8)>) -> Option<ColumnType> {
    let (precision, scale) = match (live.precision, default) {
        (Some(precision), _) => (
            u8::try_from(precision).ok()?,
            u8::try_from(live.scale.unwrap_or(0)).ok()?,
        ),
        (None, Some(default)) => default,
        (None, None) => return None,
    };
    (precision <= MAX_DECIMAL_PRECISION).then_some(ColumnType::Decimal { precision, scale })
}

/// A column type as `tables.get` reports it (`INT64` or its legacy name `INTEGER`, `NUMERIC` with its precision and scale, `STRING` with its `maxLength`); a repeated or nested column, or a type with no portable name, has none.
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    Some(match live.data_type.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "INT64" | "INTEGER" => ColumnType::Int,
        "NUMERIC" => decimal(live, Some(NUMERIC_DEFAULT))?,
        "BIGNUMERIC" => decimal(live, None)?,
        "FLOAT64" | "FLOAT" => ColumnType::Float,
        "STRING" => match live
            .char_length
            .and_then(|length| u32::try_from(length).ok())
        {
            Some(length) => ColumnType::Varchar(length),
            None => ColumnType::Text,
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

/// Whether `ALTER COLUMN ... SET DATA TYPE` changes `live` to `target` without rewriting the table: a longer or unlimited string, an integer to a decimal, or a decimal to a wider one.
pub(crate) fn widens_in_place(live: &ColumnType, target: &ColumnType) -> bool {
    matches!(
        (live, target),
        (
            ColumnType::Varchar(_),
            ColumnType::Text | ColumnType::Varchar(_)
        ) | (
            ColumnType::SmallInt | ColumnType::Int | ColumnType::Decimal { .. },
            ColumnType::Decimal { .. }
        )
    )
}
