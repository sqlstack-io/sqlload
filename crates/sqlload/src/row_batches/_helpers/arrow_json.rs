use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_cast::cast::{CastOptions, cast, cast_with_options};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, TimeUnit};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::row_batches::constants::UTC_ZONE;
use crate::row_batches::models::JsonWriter;

/// How a yielded Arrow column is carried on.
enum Normal {
    /// Already in a type the loader carries.
    Keep,
    /// Cast to this type.
    Cast(DataType),
    /// Nested: written as JSON text.
    Json,
}

/// Column `name` in the types the loader carries: 64-bit ints and floats, UTC microsecond timestamps, and JSON text for nested values.
pub(crate) fn normalise_column(name: &str, array: &ArrayRef) -> Result<(Field, ArrayRef), String> {
    let plain = |data_type: DataType| Field::new(name, data_type, true);
    match normal_form(array.data_type()) {
        Normal::Keep => Ok((plain(array.data_type().clone()), array.clone())),
        Normal::Cast(DataType::Int64) if *array.data_type() == DataType::UInt64 => {
            let strict = CastOptions {
                safe: false,
                ..CastOptions::default()
            };
            match cast_with_options(array, &DataType::Int64, &strict) {
                Ok(cast_array) => Ok((plain(DataType::Int64), cast_array)),
                Err(_) => cast_column(array, plain(DataType::Utf8)),
            }
        }
        Normal::Cast(DataType::Utf8) if matches!(array.data_type(), DataType::Dictionary(..)) => {
            let DataType::Dictionary(_, value_type) = array.data_type() else {
                return cast_column(array, plain(DataType::Utf8));
            };
            let values = cast(array, value_type).map_err(|error| error.to_string())?;
            normalise_column(name, &values)
        }
        Normal::Cast(data_type) => cast_column(array, plain(data_type)),
        Normal::Json => {
            let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
            let mut json = ArrowJson::default();
            for row in 0..array.len() {
                match array.is_null(row) {
                    true => builder.append_null(),
                    false => builder.append_value(json.text(array.as_ref(), row)),
                }
            }
            let field =
                arrow_field(name, &ColumnType::Json).unwrap_or_else(|| plain(DataType::Utf8));
            Ok((field, Arc::new(builder.finish())))
        }
    }
}

/// Column `name` as a list of the element types the loader carries, when it is a list of scalars; `None` for anything else (it then crosses as JSON text). An element that does not convert exactly is an error.
fn scalar_list(name: &str, array: &ArrayRef) -> Result<Option<(Field, ArrayRef)>, String> {
    let element = match array.data_type() {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => element.data_type(),
        _ => return Ok(None),
    };
    let carried = match normal_form(element) {
        _ if *element == DataType::Null => return Ok(None),
        Normal::Keep => element.clone(),
        Normal::Cast(data_type) => data_type,
        Normal::Json => return Ok(None),
    };
    let list_type = DataType::List(Arc::new(Field::new("item", carried, true)));
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let cast_array = cast_with_options(array, &list_type, &strict)
        .map_err(|error| format!("column {name}: an element does not convert: {error}"))?;
    Ok(Some((Field::new(name, list_type, true), cast_array)))
}

/// Column `name` as a list of carried element types when `lists` names it and it is a list of scalars, otherwise as [`normalise_column`] makes it.
pub(crate) fn list_or_normal(
    name: &str,
    array: &ArrayRef,
    lists: &[String],
) -> Result<(Field, ArrayRef), String> {
    let list = match lists.iter().any(|list| list == name) {
        true => scalar_list(name, array)?,
        false => None,
    };
    match list {
        Some(list) => Ok(list),
        None => normalise_column(name, array),
    }
}

fn cast_column(array: &ArrayRef, field: Field) -> Result<(Field, ArrayRef), String> {
    let cast_array = cast(array, field.data_type()).map_err(|error| error.to_string())?;
    Ok((field, cast_array))
}

fn normal_form(data_type: &DataType) -> Normal {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32
        | DataType::Decimal128(..)
        | DataType::Time64(TimeUnit::Microsecond) => Normal::Keep,
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == UTC_ZONE => {
            Normal::Keep
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => Normal::Keep,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Normal::Cast(DataType::Int64),
        DataType::Float16 | DataType::Float32 => Normal::Cast(DataType::Float64),
        DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(..) => {
            Normal::Cast(DataType::Utf8)
        }
        DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            Normal::Cast(DataType::Binary)
        }
        DataType::Timestamp(_, Some(_)) => Normal::Cast(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(UTC_ZONE.into()),
        )),
        DataType::Timestamp(_, None) => {
            Normal::Cast(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        DataType::Date64 => Normal::Cast(DataType::Date32),
        DataType::Time32(_) | DataType::Time64(_) => {
            Normal::Cast(DataType::Time64(TimeUnit::Microsecond))
        }
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(..) => Normal::Json,
        _ => Normal::Cast(DataType::Utf8),
    }
}

/// Writes Arrow values as JSON text.
#[derive(Debug, Default)]
struct ArrowJson {
    writer: JsonWriter,
}

impl ArrowJson {
    /// The JSON text of `array[row]`, object keys in field order.
    fn text(&mut self, array: &dyn Array, row: usize) -> String {
        self.value(array, row);
        self.writer.take()
    }

    /// Write `array[row]`; types JSON has no form for become their display text.
    fn value(&mut self, array: &dyn Array, row: usize) {
        if array.is_null(row) || *array.data_type() == DataType::Null {
            self.writer.raw("null");
            return;
        }
        match array.data_type() {
            DataType::Dictionary(_, value_type) => match cast(&array.slice(row, 1), value_type) {
                Ok(values) => self.value(values.as_ref(), 0),
                Err(_) => self.writer.string(&display_text(array, row)),
            },
            DataType::Boolean => self.writer.raw(match array.as_boolean().value(row) {
                true => "true",
                false => "false",
            }),
            DataType::Int8 => self.number(array.as_primitive::<Int8Type>().value(row)),
            DataType::Int16 => self.number(array.as_primitive::<Int16Type>().value(row)),
            DataType::Int32 => self.number(array.as_primitive::<Int32Type>().value(row)),
            DataType::Int64 => self.number(array.as_primitive::<Int64Type>().value(row)),
            DataType::UInt8 => self.number(array.as_primitive::<UInt8Type>().value(row)),
            DataType::UInt16 => self.number(array.as_primitive::<UInt16Type>().value(row)),
            DataType::UInt32 => self.number(array.as_primitive::<UInt32Type>().value(row)),
            DataType::UInt64 => self.number(array.as_primitive::<UInt64Type>().value(row)),
            DataType::Float32 => self.writer.float(
                array
                    .as_primitive::<Float32Type>()
                    .value(row)
                    .to_string()
                    .parse()
                    .unwrap_or(f64::NAN),
            ),
            DataType::Float64 => self
                .writer
                .float(array.as_primitive::<Float64Type>().value(row)),
            DataType::Utf8 => self.writer.string(array.as_string::<i32>().value(row)),
            DataType::LargeUtf8 => self.writer.string(array.as_string::<i64>().value(row)),
            DataType::Utf8View => self.writer.string(array.as_string_view().value(row)),
            DataType::Struct(fields) => {
                self.writer.raw("{");
                let columns = array.as_struct().columns();
                for (position, (field, column)) in fields.iter().zip(columns).enumerate() {
                    if position > 0 {
                        self.writer.raw(",");
                    }
                    self.writer.string(field.name());
                    self.writer.raw(":");
                    self.value(column.as_ref(), row);
                }
                self.writer.raw("}");
            }
            DataType::List(_) => self.list(&array.as_list::<i32>().value(row)),
            DataType::LargeList(_) => self.list(&array.as_list::<i64>().value(row)),
            DataType::FixedSizeList(..) => self.list(&array.as_fixed_size_list().value(row)),
            DataType::Map(..) => {
                let entries = array.as_map().value(row);
                self.writer.raw("{");
                for entry in 0..entries.len() {
                    if entry > 0 {
                        self.writer.raw(",");
                    }
                    self.writer
                        .string(&display_text(entries.column(0).as_ref(), entry));
                    self.writer.raw(":");
                    self.value(entries.column(1).as_ref(), entry);
                }
                self.writer.raw("}");
            }
            _ => self.writer.string(&display_text(array, row)),
        }
    }

    fn list(&mut self, values: &ArrayRef) {
        self.writer.raw("[");
        for position in 0..values.len() {
            if position > 0 {
                self.writer.raw(",");
            }
            self.value(values.as_ref(), position);
        }
        self.writer.raw("]");
    }

    fn number(&mut self, value: impl std::fmt::Display) {
        self.writer.raw(&value.to_string());
    }
}

fn display_text(array: &dyn Array, row: usize) -> String {
    let options = FormatOptions::default();
    ArrayFormatter::try_new(array, &options).map_or_else(
        |_| String::new(),
        |formatter| formatter.value(row).to_string(),
    )
}
