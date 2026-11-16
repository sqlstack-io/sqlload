//! Databricks column types as portable ones.

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, SourceColumn};

/// A column from its name, its full SQL type as Databricks writes it (`information_schema.columns.full_data_type`, or a result's `type_text`, in either case) and whether it may be NULL: `ARRAY`, `MAP`, `STRUCT` and `VARIANT` as `json`; a type with no portable one (`INTERVAL`, `GEOMETRY`, `VOID`) must be pinned or excluded.
pub(crate) fn source_column(name: &str, sql_type: &str, nullable: bool) -> SourceColumn {
    let shown = sql_type.trim().to_ascii_uppercase();
    let column_type = scalar_type(&shown).ok_or_else(|| {
        format!(
            "Databricks {shown} has no portable type; pin one in columns, such as text, or exclude the column"
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

/// The portable type of an upper-cased Databricks type, or `None` for one with none.
fn scalar_type(shown: &str) -> Option<ColumnType> {
    let end = shown.find(['(', '<', ' ']).unwrap_or(shown.len());
    let base = &shown[..end];
    let sizes = match shown[end..].strip_prefix('(') {
        Some(rest) => rest.trim_end_matches(')'),
        None => "",
    };
    let number = |text: Option<&str>| text.and_then(|text| text.trim().parse::<u32>().ok());
    let mut parts = sizes.split(',');
    let (first, second) = (number(parts.next()), number(parts.next()));
    Some(match base {
        "BOOLEAN" => ColumnType::Bool,
        "TINYINT" | "BYTE" | "SMALLINT" | "SHORT" => ColumnType::SmallInt,
        "INT" | "INTEGER" | "BIGINT" | "LONG" => ColumnType::Int,
        "FLOAT" | "REAL" | "DOUBLE" => ColumnType::Float,
        "DECIMAL" | "DEC" | "NUMERIC" => {
            let precision = u8::try_from(first.unwrap_or(10)).ok()?;
            let scale = u8::try_from(second.unwrap_or(0)).ok()?;
            (precision <= MAX_DECIMAL_PRECISION).then_some(ColumnType::Decimal { precision, scale })?
        }
        "STRING" => ColumnType::Text,
        "VARCHAR" | "CHAR" => match first {
            Some(length) => ColumnType::Varchar(length),
            None => ColumnType::Text,
        },
        "BINARY" => ColumnType::Binary,
        "DATE" => ColumnType::Date,
        "TIMESTAMP" | "TIMESTAMP_LTZ" => ColumnType::TimestampTz,
        "TIMESTAMP_NTZ" => ColumnType::TimestampNtz,
        "ARRAY" | "MAP" | "STRUCT" | "VARIANT" | "OBJECT" => ColumnType::Json,
        _ => return None,
    })
}
