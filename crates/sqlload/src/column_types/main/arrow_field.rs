//! Map a column type to the Arrow field batches carry it in.

use std::collections::HashMap;

use arrow_schema::{DataType, Field, TimeUnit};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;

/// The nullable Arrow field for column `name`; `None` for `native` types, which have no portable representation.
pub fn arrow_field(name: &str, column_type: &ColumnType) -> Option<Field> {
    let data_type = match column_type {
        ColumnType::Bool => DataType::Boolean,
        ColumnType::SmallInt => DataType::Int16,
        ColumnType::Int => DataType::Int64,
        ColumnType::Decimal { precision, scale } => {
            DataType::Decimal128(*precision, i8::try_from(*scale).unwrap_or(i8::MAX))
        }
        ColumnType::Float => DataType::Float64,
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Json | ColumnType::Uuid => {
            DataType::Utf8
        }
        ColumnType::Date => DataType::Date32,
        ColumnType::Time => DataType::Time64(TimeUnit::Microsecond),
        ColumnType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ColumnType::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Binary => DataType::Binary,
        ColumnType::Native(_) => return None,
    };
    let field = Field::new(name, data_type, true);
    Some(match column_type {
        ColumnType::Json | ColumnType::Uuid => field.with_metadata(HashMap::from([(
            ARROW_TYPE_METADATA_KEY.to_owned(),
            column_type.to_string(),
        )])),
        _ => field,
    })
}
