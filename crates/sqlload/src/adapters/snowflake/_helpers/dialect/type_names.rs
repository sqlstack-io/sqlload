//! Snowflake's spelling of each portable type, and the way back from what it reports.

use std::borrow::Cow;

use crate::column_types::main::array_loss::array_loss;

use crate::adapters::snowflake::constants::MAX_TEXT_LENGTH;
use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type Snowflake holds exactly.
fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

/// A type Snowflake holds with a loss, and why.
fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(Cow::Borrowed(reason)),
    }
}

/// Snowflake, spelled as `DESCRIBE TABLE` reports the types.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => lossy("NUMBER(38,0)", "stored as a 38-digit integer"),
        ColumnType::Int => exact("NUMBER(38,0)"),
        ColumnType::Decimal { precision, scale } => exact(format!("NUMBER({precision},{scale})")),
        ColumnType::Float => exact("FLOAT"),
        ColumnType::Text => exact("VARCHAR"),
        ColumnType::Varchar(length) => exact(format!("VARCHAR({length})")),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME(9)"),
        ColumnType::TimestampTz => exact("TIMESTAMP_TZ(9)"),
        ColumnType::TimestampNtz => exact("TIMESTAMP_NTZ(9)"),
        ColumnType::Json => exact("VARIANT"),
        ColumnType::Uuid => lossy("VARCHAR(36)", "stored as text"),
        ColumnType::Binary => exact("BINARY"),
        ColumnType::Native(name) => exact(name.clone()),
        ColumnType::Array(element) => WarehouseType {
            sql: "ARRAY".to_owned(),
            lossy: array_loss(None, json_element_loss(element)),
        },
    }
}

/// A decimal with the reported precision and scale, when it fits the portable range.
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

/// A Snowflake `INFORMATION_SCHEMA.COLUMNS` type. `ARRAY` stays native, so a pinned array fits the `ARRAY` column it created but not a `VARIANT` json one.
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
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
            Some(length) if length < MAX_TEXT_LENGTH => match u32::try_from(length) {
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
        "VARIANT" | "OBJECT" => ColumnType::Json,
        "ARRAY" => return None,
        "BINARY" => ColumnType::Binary,
        _ => return None,
    })
}

/// Whether Snowflake changes `live` to `target` without rewriting the table: a longer text, or more digits at the same scale.
pub(crate) fn widens_in_place(live: &ColumnType, target: &ColumnType) -> bool {
    match (live, target) {
        (ColumnType::Varchar(_), ColumnType::Varchar(_) | ColumnType::Text) => true,
        (
            ColumnType::Decimal { scale, .. },
            ColumnType::Decimal {
                scale: target_scale,
                ..
            },
        ) => scale == target_scale,
        _ => false,
    }
}

/// What an element of type `element` loses as the JSON value a Snowflake `ARRAY` holds it as: a decimal, date, time, timestamp or uuid becomes a JSON string, a float holds no NaN or infinity, and a `varchar(n)`'s length limit is not enforced; bools, integers and text keep their values.
fn json_element_loss(element: &ColumnType) -> Option<Cow<'static, str>> {
    match element {
        ColumnType::Decimal { .. }
        | ColumnType::Date
        | ColumnType::Time
        | ColumnType::TimestampTz
        | ColumnType::TimestampNtz
        | ColumnType::Uuid => Some(Cow::Owned(format!("each {element} is a JSON string"))),
        ColumnType::Float => Some(Cow::Borrowed("NaN and infinities can't be held")),
        ColumnType::Varchar(_) => Some(Cow::Borrowed("the length limit is not enforced")),
        _ => None,
    }
}
