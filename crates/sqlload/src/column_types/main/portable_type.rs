//! Map an Arrow type (from a Parquet footer or a batch) to the portable vocabulary.

use arrow_schema::{DataType, Field, TimeUnit};

use crate::column_types::constants::{ARROW_TYPE_METADATA_KEY, MAX_DECIMAL_PRECISION};
use crate::column_types::models::ColumnType;

/// The portable type holding every value of `field`, or why there is none (then a type must be pinned).
pub fn portable_type(field: &Field) -> Result<ColumnType, String> {
    if let Some(marked) = field.metadata().get(ARROW_TYPE_METADATA_KEY) {
        match marked.as_str() {
            "json" => return Ok(ColumnType::Json),
            "uuid" => return Ok(ColumnType::Uuid),
            _ => {}
        }
    }
    Ok(match field.data_type() {
        DataType::Boolean => ColumnType::Bool,
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => ColumnType::SmallInt,
        DataType::Int32 | DataType::Int64 | DataType::UInt16 | DataType::UInt32 => ColumnType::Int,
        DataType::UInt64 => ColumnType::Decimal {
            precision: 20,
            scale: 0,
        },
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ColumnType::Float,
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            match (u8::try_from(*scale), *precision <= MAX_DECIMAL_PRECISION) {
                (Ok(scale), true) => ColumnType::Decimal {
                    precision: *precision,
                    scale,
                },
                _ => {
                    return Err(format!(
                        "decimal({precision},{scale}) does not fit decimal({MAX_DECIMAL_PRECISION},s); pin \"text\" or a narrower decimal"
                    ));
                }
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ColumnType::Text,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => ColumnType::Binary,
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::Time,
        DataType::Timestamp(_, Some(_)) => ColumnType::TimestampTz,
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            None,
        ) => ColumnType::TimestampNtz,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => {
            return Err(format!(
                "nested type {} is not loadable from files yet; add the column to exclude_columns",
                field.data_type()
            ));
        }
        DataType::Dictionary(_, value) => {
            return portable_type(&Field::new(field.name(), value.as_ref().clone(), true));
        }
        other => return Err(format!("Arrow type {other} has no portable type; pin one")),
    })
}
