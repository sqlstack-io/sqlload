//! DuckDB's spelling of each portable type, and the way back from what it reports.

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::main::array_element::array_element;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type DuckDB holds exactly.
fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

/// A type DuckDB holds with a loss, and why.
fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(reason),
    }
}

/// DuckDB's DDL types.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => exact("SMALLINT"),
        ColumnType::Int => exact("BIGINT"),
        ColumnType::Decimal { precision, scale } => exact(format!("DECIMAL({precision},{scale})")),
        ColumnType::Float => exact("DOUBLE"),
        ColumnType::Text => exact("VARCHAR"),
        ColumnType::Varchar(_) => lossy("VARCHAR", "the length limit is not enforced"),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP WITH TIME ZONE",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("TIMESTAMP"),
        ColumnType::Json => exact("JSON"),
        ColumnType::Uuid => exact("UUID"),
        ColumnType::Binary => exact("BLOB"),
        ColumnType::Native(name) => exact(name.clone()),
        ColumnType::Array(element) => {
            let element = logical_type(element);
            WarehouseType {
                sql: format!("{}[]", element.sql),
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

/// A DuckDB `information_schema.columns` type, such as `DECIMAL(18,2)` or `TIMESTAMP WITH TIME ZONE`.
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    if let Some(element) = live.data_type.trim().strip_suffix("[]") {
        let element = portable_type(&array_element(live, element))?;
        return Some(ColumnType::Array(Box::new(element)));
    }
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
