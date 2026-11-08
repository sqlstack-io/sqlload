//! Databricks' spelling of each portable type, and the way back from what it reports.

use std::borrow::Cow;

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::main::array_element::array_element;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type Databricks holds exactly.
fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

/// A type Databricks holds with a loss, and why.
fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(Cow::Borrowed(reason)),
    }
}

/// Databricks' DDL types.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => exact("SMALLINT"),
        ColumnType::Int => exact("BIGINT"),
        ColumnType::Decimal { precision, scale } => exact(format!("DECIMAL({precision},{scale})")),
        ColumnType::Float => exact("DOUBLE"),
        ColumnType::Text => exact("STRING"),
        ColumnType::Varchar(_) => lossy("STRING", "the length limit is not enforced"),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => lossy("STRING", "stored as text; Databricks has no time type"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("TIMESTAMP_NTZ"),
        ColumnType::Json => exact("VARIANT"),
        ColumnType::Uuid => lossy("STRING", "stored as text"),
        ColumnType::Binary => exact("BINARY"),
        ColumnType::Native(name) => exact(name.clone()),
        ColumnType::Array(element) => {
            let element = logical_type(element);
            WarehouseType {
                sql: format!("ARRAY<{}>", element.sql),
                lossy: element.lossy,
            }
        }
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

/// A Unity Catalog `information_schema.columns` type, read from `full_data_type` (`bigint`, `decimal(18,2)`, `varchar(10)`, `array<string>`).
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    let lower = live.data_type.to_ascii_lowercase();
    if let Some(element) = lower
        .trim()
        .strip_prefix("array<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        let element = portable_type(&array_element(live, element))?;
        return Some(ColumnType::Array(Box::new(element)));
    }
    let (base, argument) = match lower.split_once('(') {
        Some((base, rest)) => (base.trim(), rest.trim_end_matches(')').trim()),
        None => (lower.trim(), ""),
    };
    Some(match base {
        "boolean" => ColumnType::Bool,
        "tinyint" | "byte" | "smallint" | "short" => ColumnType::SmallInt,
        "int" | "integer" | "bigint" | "long" => ColumnType::Int,
        "decimal" | "numeric" | "dec" => decimal(live)?,
        "float" | "real" | "double" => ColumnType::Float,
        "string" => ColumnType::Text,
        "varchar" | "char" => match argument.parse::<u32>() {
            Ok(length) => ColumnType::Varchar(length),
            Err(_) => ColumnType::Text,
        },
        "date" => ColumnType::Date,
        "timestamp" => ColumnType::TimestampTz,
        "timestamp_ntz" => ColumnType::TimestampNtz,
        "variant" => ColumnType::Json,
        "binary" => ColumnType::Binary,
        _ => return None,
    })
}

/// Whether Delta changes `live` to `target` without rewriting the table (type widening): a wider integer, or more digits at the same scale.
pub(crate) fn widens_in_place(live: &ColumnType, target: &ColumnType) -> bool {
    match (live, target) {
        (ColumnType::SmallInt, ColumnType::Int) => true,
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
