//! BigQuery's spelling of each portable type, and the way back from what it reports.

use crate::adapters::bigquery::constants::{
    NUMERIC_DEFAULT, NUMERIC_MAX_INTEGER_DIGITS, NUMERIC_MAX_SCALE,
};
use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::main::array_element::array_element;
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

/// The digits `decimal(precision,scale)` is stored with, and whether that needs `BIGNUMERIC` rather than `NUMERIC`: both need a digit before the point, so a decimal of only fraction digits gets one more digit of precision.
pub(crate) fn decimal_digits(precision: u8, scale: u8) -> (u8, u8, bool) {
    let precision = precision.max(scale + 1);
    let big = precision - scale > NUMERIC_MAX_INTEGER_DIGITS || scale > NUMERIC_MAX_SCALE;
    (precision, scale, big)
}

/// `decimal(precision,scale)` as a parameterised `NUMERIC` when it fits one, else `BIGNUMERIC`.
fn decimal_type(precision: u8, scale: u8) -> String {
    let (precision, scale, big) = decimal_digits(precision, scale);
    let name = match big {
        true => "BIGNUMERIC",
        false => "NUMERIC",
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
        ColumnType::Array(element) => WarehouseType {
            sql: format!("ARRAY<{}>", logical_type(element).sql),
            lossy: Some(
                "BigQuery arrays hold no NULLs: a NULL array loads as an empty one, and a NULL element fails the load",
            ),
        },
    }
}

/// A decimal with the reported precision and scale (`default` when the column declares none), when it fits the portable range.
fn decimal(live: &LiveColumnType, default: Option<(u8, u8)>) -> Option<ColumnType> {
    let reported = live.precision.map(|precision| {
        (
            u8::try_from(precision),
            u8::try_from(live.scale.unwrap_or(0)),
        )
    });
    let (precision, scale) = match (reported, default) {
        (Some((Ok(precision), Ok(scale))), _) => (precision, scale),
        (None, Some(default)) => default,
        _ => return None,
    };
    (precision <= MAX_DECIMAL_PRECISION).then_some(ColumnType::Decimal { precision, scale })
}

/// A `STRING` column, `varchar(n)` when it reports a `maxLength` that fits.
fn string(live: &LiveColumnType) -> ColumnType {
    match live.char_length.map(u32::try_from) {
        Some(Ok(length)) => ColumnType::Varchar(length),
        _ => ColumnType::Text,
    }
}

/// A column type as `tables.get` reports it (`INT64` or its legacy name `INTEGER`, `NUMERIC` with its precision and scale, `STRING` with its `maxLength`); a repeated or nested column, or a type with no portable name, has none.
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    let upper = live.data_type.trim().to_ascii_uppercase();
    if let Some(element) = upper
        .strip_prefix("ARRAY<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        let base = element.split('(').next().unwrap_or_default();
        let element = portable_type(&LiveColumnType {
            data_type: base.to_owned(),
            ..array_element(live, element)
        })?;
        return Some(ColumnType::Array(Box::new(element)));
    }
    Some(match live.data_type.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "INT64" | "INTEGER" => ColumnType::Int,
        "NUMERIC" => decimal(live, Some(NUMERIC_DEFAULT))?,
        "BIGNUMERIC" => decimal(live, None)?,
        "FLOAT64" | "FLOAT" => ColumnType::Float,
        "STRING" => string(live),
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
