//! ClickHouse's spelling of each portable type, and the way back from what it reports.

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::{ColumnType, LiveColumnType, WarehouseType};

/// A type ClickHouse holds exactly, as a `Nullable` column.
fn exact(sql: &str) -> WarehouseType {
    WarehouseType {
        sql: format!("Nullable({sql})"),
        lossy: None,
    }
}

/// A type ClickHouse holds with a loss, and why, as a `Nullable` column.
fn lossy(sql: &str, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: format!("Nullable({sql})"),
        lossy: Some(reason),
    }
}

/// ClickHouse's DDL types: every loaded column is `Nullable`; a `native(...)` type is used as written.
pub(crate) fn logical_type(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("Bool"),
        ColumnType::SmallInt => exact("Int16"),
        ColumnType::Int => exact("Int64"),
        ColumnType::Decimal { precision, scale } => {
            exact(&format!("Decimal({precision}, {scale})"))
        }
        ColumnType::Float => exact("Float64"),
        ColumnType::Text => exact("String"),
        ColumnType::Varchar(_) => lossy("String", "the length limit is not enforced"),
        ColumnType::Date => lossy(
            "Date32",
            "holds dates from 1900 to 2299; a date outside them fails the load",
        ),
        ColumnType::Time => lossy(
            "String",
            "stored as text, HH:MM:SS with a fraction when there is one",
        ),
        ColumnType::TimestampTz => lossy(
            "DateTime64(6, 'UTC')",
            "stored as a UTC instant from 1900 to 2299; the offset is not kept",
        ),
        ColumnType::TimestampNtz => lossy(
            "DateTime64(6)",
            "holds times from 1900 to 2299, shown in the server's time zone",
        ),
        ColumnType::Json => lossy(
            "String",
            "stored as text; pin native(\"JSON\") for the JSON type",
        ),
        ColumnType::Uuid => exact("UUID"),
        ColumnType::Binary => exact("String"),
        ColumnType::Native(name) => WarehouseType {
            sql: name.clone(),
            lossy: None,
        },
    }
}

/// `type_text` without its `Nullable(...)` and `LowCardinality(...)` wrappers, which do not change the values it holds.
pub(crate) fn unwrapped(type_text: &str) -> &str {
    let mut text = type_text.trim();
    loop {
        let inner = ["Nullable(", "LowCardinality("].iter().find_map(|wrapper| {
            text.strip_prefix(wrapper)
                .and_then(|rest| rest.strip_suffix(')'))
        });
        match inner {
            Some(inner) => text = inner.trim(),
            None => return text,
        }
    }
}

/// The arguments of `Name(a, b)`, trimmed, with quotes kept.
fn arguments(text: &str) -> Vec<&str> {
    text.split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .map(|inside| inside.split(',').map(str::trim).collect())
        .unwrap_or_default()
}

/// A decimal of `precision` and `scale` when it fits the portable range.
fn decimal(precision: u8, scale: Option<&str>) -> Option<ColumnType> {
    let scale: u8 = scale?.parse().ok()?;
    (precision <= MAX_DECIMAL_PRECISION && scale <= precision)
        .then_some(ColumnType::Decimal { precision, scale })
}

/// A column type as `system.columns` reports it, such as `Nullable(Decimal(18, 2))` or `DateTime64(6, 'UTC')`: a time with a zone argument is a zoned timestamp, one without a wall-clock one. Only a type holding every value of its portable type has a portable name; a narrower one (`Int32`, `UInt16`, `Float32`, `Date`, `DateTime`, `DateTime64(3)`) has none, as ClickHouse would wrap or cut the values put into it rather than fail.
pub(crate) fn portable_type(live: &LiveColumnType) -> Option<ColumnType> {
    let text = unwrapped(&live.data_type);
    let base = text.split('(').next().unwrap_or_default().trim();
    let arguments = arguments(text);
    Some(match base {
        "Bool" => ColumnType::Bool,
        "Int16" => ColumnType::SmallInt,
        "Int64" => ColumnType::Int,
        "Float64" => ColumnType::Float,
        "String" => ColumnType::Text,
        "UUID" => ColumnType::Uuid,
        "Date32" => ColumnType::Date,
        "DateTime64" if arguments.first() == Some(&"6") => match arguments.get(1) {
            Some(_) => ColumnType::TimestampTz,
            None => ColumnType::TimestampNtz,
        },
        "Decimal" => decimal(arguments.first()?.parse().ok()?, arguments.get(1).copied())?,
        "Decimal32" => decimal(9, arguments.first().copied())?,
        "Decimal64" => decimal(18, arguments.first().copied())?,
        "Decimal128" => decimal(38, arguments.first().copied())?,
        "JSON" => ColumnType::Json,
        _ => return None,
    })
}
