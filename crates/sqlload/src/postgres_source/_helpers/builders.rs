use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int16Builder,
    Int64Builder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::postgres_source::_helpers::values::{
    read_date, read_f32, read_f64, read_i16, read_i32, read_i64, read_numeric, read_timestamp,
    read_uuid,
};
use crate::postgres_source::models::WireType;
use crate::row_batches::main::json_lists::json_lists;

/// An Arrow array under construction for one output column.
pub(crate) enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Decimal {
        builder: Decimal128Builder,
        precision: u8,
        scale: u8,
    },
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Time64(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Binary(BinaryBuilder),
    /// An array read as its JSON text, converted to `list_type` when the batch is finished.
    List {
        texts: StringBuilder,
        list_type: arrow_schema::DataType,
    },
}

impl ColumnBuilder {
    /// An empty builder producing `output`, or why there is none (`native` types).
    pub(crate) fn for_type(output: &ColumnType) -> Result<Self, String> {
        Ok(match output {
            ColumnType::Bool => Self::Bool(BooleanBuilder::new()),
            ColumnType::SmallInt => Self::Int16(Int16Builder::new()),
            ColumnType::Int => Self::Int64(Int64Builder::new()),
            ColumnType::Float => Self::Float64(Float64Builder::new()),
            ColumnType::Decimal { precision, scale } => Self::Decimal {
                builder: Decimal128Builder::new()
                    .with_precision_and_scale(
                        *precision,
                        i8::try_from(*scale).map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?,
                precision: *precision,
                scale: *scale,
            },
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Json | ColumnType::Uuid => {
                Self::Utf8(StringBuilder::new())
            }
            ColumnType::Date => Self::Date32(Date32Builder::new()),
            ColumnType::Time => Self::Time64(Time64MicrosecondBuilder::new()),
            ColumnType::TimestampTz => {
                Self::Timestamp(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
            ColumnType::TimestampNtz => Self::Timestamp(TimestampMicrosecondBuilder::new()),
            ColumnType::Binary => Self::Binary(BinaryBuilder::new()),
            ColumnType::Native(name) => {
                return Err(format!("native({name:?}) has no Arrow representation"));
            }
            ColumnType::Array(_) => Self::List {
                texts: StringBuilder::new(),
                list_type: arrow_field("", output)
                    .ok_or("an array of this type has no Arrow form")?
                    .data_type()
                    .clone(),
            },
        })
    }

    /// Append one value that arrived as `wire`; `None` is SQL `NULL`.
    pub(crate) fn append(&mut self, wire: WireType, value: Option<&[u8]>) -> Result<(), String> {
        let Some(bytes) = value else {
            self.append_null();
            return Ok(());
        };
        match (self, wire) {
            (Self::Bool(builder), WireType::Bool) => {
                builder.append_value(bytes.first().is_some_and(|byte| *byte != 0));
            }
            (Self::Int16(builder), WireType::Int2) => builder.append_value(read_i16(bytes)?),
            (Self::Int64(builder), WireType::Int2) => builder.append_value(read_i16(bytes)?.into()),
            (Self::Int64(builder), WireType::Int4) => builder.append_value(read_i32(bytes)?.into()),
            (Self::Int64(builder), WireType::Int8) => builder.append_value(read_i64(bytes)?),
            (Self::Float64(builder), WireType::Float4) => {
                builder.append_value(read_f32(bytes)?.into());
            }
            (Self::Float64(builder), WireType::Float8) => builder.append_value(read_f64(bytes)?),
            (
                Self::Decimal {
                    builder,
                    precision,
                    scale,
                },
                WireType::Numeric,
            ) => builder.append_value(read_numeric(bytes, *precision, *scale)?),
            (Self::Utf8(builder), WireType::Text) => builder.append_value(
                std::str::from_utf8(bytes).map_err(|_| "is not valid UTF-8".to_owned())?,
            ),
            (Self::Utf8(builder), WireType::Uuid) => builder.append_value(read_uuid(bytes)?),
            (Self::Date32(builder), WireType::Date) => builder.append_value(read_date(bytes)?),
            (Self::Time64(builder), WireType::Time) => builder.append_value(read_i64(bytes)?),
            (Self::Timestamp(builder), WireType::Timestamp) => {
                builder.append_value(read_timestamp(bytes)?);
            }
            (Self::Binary(builder), WireType::Bytea) => builder.append_value(bytes),
            (Self::List { texts, .. }, WireType::Text) => texts.append_value(
                std::str::from_utf8(bytes).map_err(|_| "is not valid UTF-8".to_owned())?,
            ),
            (_, wire) => return Err(format!("cannot store a {wire:?} value in this column")),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(builder) => builder.append_null(),
            Self::Int16(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Time64(builder) => builder.append_null(),
            Self::Timestamp(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
            Self::List { texts, .. } => texts.append_null(),
        }
    }

    /// The finished array; the builder is empty afterwards. An array's JSON text that does not convert to its list type is an error.
    pub(crate) fn finish(&mut self) -> Result<ArrayRef, String> {
        Ok(match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Int16(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Decimal { builder, .. } => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::Time64(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::List { texts, list_type } => {
                let texts: ArrayRef = Arc::new(texts.finish());
                json_lists(&texts, list_type)?
            }
        })
    }
}
