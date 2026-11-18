//! DuckDB column types as portable ones.

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, SourceColumn};

/// A column from its name, its type as DuckDB writes it (`duckdb_columns().data_type`, or a result's column type) and whether it may be NULL: lists, arrays, structs and maps as `json`; a type with no portable one (`HUGEINT`, `UBIGINT`, `INTERVAL`, `BIT`, `UNION`, `TIME WITH TIME ZONE`, a nanosecond timestamp, a decimal wider than 38 digits) must be pinned or excluded.
pub(crate) fn source_column(name: &str, data_type: &str, nullable: bool) -> SourceColumn {
    let shown = data_type.trim().to_owned();
    let column_type = scalar_type(&shown.to_ascii_uppercase()).ok_or_else(|| {
        format!(
            "DuckDB {shown} has no portable type; pin one in columns, such as text, or exclude the column"
        )
    });
    SourceColumn {
        name: name.to_owned(),
        postgres_type: shown,
        pg_type: PgType::Other,
        column_type,
        nullable,
        source_spelling: None,
        repeat_of: None,
    }
}

/// Whether an upper-cased DuckDB type is a list or fixed-size array, which a pinned `array(T)` reads as a list.
pub(crate) fn is_list(shown: &str) -> bool {
    shown.ends_with(']')
}

/// The portable type of an upper-cased DuckDB type, or `None` for one with none.
fn scalar_type(shown: &str) -> Option<ColumnType> {
    if is_list(shown) {
        return Some(ColumnType::Json);
    }
    let end = shown.find('(').unwrap_or(shown.len());
    let base = shown[..end].trim();
    Some(match base {
        "BOOLEAN" => ColumnType::Bool,
        "TINYINT" | "SMALLINT" | "UTINYINT" => ColumnType::SmallInt,
        "INTEGER" | "BIGINT" | "USMALLINT" | "UINTEGER" => ColumnType::Int,
        "FLOAT" | "DOUBLE" => ColumnType::Float,
        "DECIMAL" => {
            let sizes = shown[end..].trim_start_matches('(').trim_end_matches(')');
            let mut parts = sizes.split(',').map(|part| part.trim().parse::<u8>());
            let (Some(Ok(precision)), Some(Ok(scale))) = (parts.next(), parts.next()) else {
                return None;
            };
            (precision <= MAX_DECIMAL_PRECISION)
                .then_some(ColumnType::Decimal { precision, scale })?
        }
        "VARCHAR" | "ENUM" => ColumnType::Text,
        "BLOB" => ColumnType::Binary,
        "DATE" => ColumnType::Date,
        "TIME" => ColumnType::Time,
        "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" => ColumnType::TimestampNtz,
        "TIMESTAMP WITH TIME ZONE" => ColumnType::TimestampTz,
        "UUID" => ColumnType::Uuid,
        "JSON" => ColumnType::Json,
        "STRUCT" | "MAP" => ColumnType::Json,
        _ => return None,
    })
}
