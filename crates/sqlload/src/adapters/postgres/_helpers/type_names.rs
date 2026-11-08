//! Postgres's spelling of each portable type, and the way back from what its catalog prints.

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::main::array_element::array_element;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type Postgres holds exactly.
fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

/// A type Postgres holds with a loss, and why.
fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(reason),
    }
}

/// Postgres's DDL types.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("boolean"),
        ColumnType::SmallInt => exact("smallint"),
        ColumnType::Int => exact("bigint"),
        ColumnType::Decimal { precision, scale } => exact(format!("numeric({precision},{scale})")),
        ColumnType::Float => exact("double precision"),
        ColumnType::Text => exact("text"),
        ColumnType::Varchar(length) => exact(format!("varchar({length})")),
        ColumnType::Date => exact("date"),
        ColumnType::Time => exact("time"),
        ColumnType::TimestampTz => lossy(
            "timestamptz",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("timestamp"),
        ColumnType::Json => lossy(
            "jsonb",
            "objects keep neither their key order nor duplicate keys",
        ),
        ColumnType::Uuid => exact("uuid"),
        ColumnType::Binary => exact("bytea"),
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

/// The type a staging column has: what the binary `COPY` sends, so text for JSON, UUIDs, bounded text and native types, which the write converts.
pub(crate) fn staging_type(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Json
        | ColumnType::Uuid
        | ColumnType::Varchar(_)
        | ColumnType::Native(_)
        | ColumnType::Array(_) => logical_type(&ColumnType::Text).sql,
        other => logical_type(other).sql,
    }
}

/// A decimal with the reported precision and scale, when it fits the portable range.
fn decimal(live: &LiveColumnType) -> Option<ColumnType> {
    let precision = live.precision?;
    match (
        u8::try_from(precision),
        u8::try_from(live.scale.unwrap_or(0)),
    ) {
        (Ok(precision), Ok(scale)) if precision <= MAX_DECIMAL_PRECISION && scale <= precision => {
            Some(ColumnType::Decimal { precision, scale })
        }
        _ => None,
    }
}

/// A type as `format_type` prints it, such as `numeric(18,2)`, `character varying(20)` or `timestamp(3) with time zone`; a one-dimensional array as `bigint[]`; `None` for types with no portable equivalent (arrays of other types, enums, `json`, `real`, `interval`, times holding fewer than six digits after the second, and so on).
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    if let Some(element) = live.data_type.trim().strip_suffix("[]") {
        let element = portable_type(&array_element(live, element))?;
        return Some(ColumnType::Array(Box::new(element)));
    }
    let fewer_digits = live.data_type.starts_with("time")
        && live.data_type.contains('(')
        && !live.data_type.contains("(6)");
    if fewer_digits {
        return None;
    }
    let base: String = live
        .data_type
        .split('(')
        .enumerate()
        .map(|(index, part)| match index {
            0 => part,
            _ => part.split_once(')').map_or("", |(_, rest)| rest),
        })
        .collect();
    let bounded = |length: Option<i64>| match length.map(u32::try_from) {
        Some(Ok(length)) => ColumnType::Varchar(length),
        _ => ColumnType::Text,
    };
    Some(match base.trim() {
        "boolean" => ColumnType::Bool,
        "smallint" => ColumnType::SmallInt,
        "integer" | "bigint" => ColumnType::Int,
        "numeric" => decimal(live)?,
        "double precision" => ColumnType::Float,
        "text" => ColumnType::Text,
        "character varying" | "character" => bounded(live.char_length),
        "date" => ColumnType::Date,
        "time without time zone" => ColumnType::Time,
        "timestamp with time zone" => ColumnType::TimestampTz,
        "timestamp without time zone" => ColumnType::TimestampNtz,
        "jsonb" => ColumnType::Json,
        "uuid" => ColumnType::Uuid,
        "bytea" => ColumnType::Binary,
        _ => return None,
    })
}
