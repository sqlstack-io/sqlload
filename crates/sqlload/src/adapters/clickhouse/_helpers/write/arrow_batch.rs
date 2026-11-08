//! Rows as ClickHouse's Arrow input reads them.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::column_types::constants::ARRAY_ITEM;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::DesiredColumn;

/// The Arrow type ClickHouse reads into a column of `column_type`, given the type the rows arrived as, or why it cannot: times as their `HH:MM:SS.ffffff` text (the column is a `String`), decimals at their column's precision so a value too wide fails here by name, and view, large and dictionary types as plain ones, which its Arrow reader does not take. A nested value loads only into a `native(...)` column, as a cast to text would load its display text.
fn clickhouse_type(column_type: &ColumnType, arrived: &DataType) -> Result<DataType, String> {
    if let (ColumnType::Array(element), DataType::List(item)) = (column_type, arrived) {
        let element = clickhouse_type(element, item.data_type())?;
        return Ok(DataType::List(Arc::new(Field::new(
            ARRAY_ITEM, element, true,
        ))));
    }
    if arrived.is_nested() && !matches!(column_type, ColumnType::Native(_)) {
        return Err(format!(
            "a nested value loads only into a native(...) column, not {column_type}"
        ));
    }
    Ok(match column_type {
        ColumnType::Bool => DataType::Boolean,
        ColumnType::SmallInt => DataType::Int16,
        ColumnType::Int => DataType::Int64,
        ColumnType::Float => DataType::Float64,
        ColumnType::Decimal { precision, scale } => {
            let scale = i8::try_from(*scale)
                .map_err(|_| format!("a decimal scale of {scale} has no Arrow type"))?;
            DataType::Decimal128(*precision, scale)
        }
        ColumnType::Time
        | ColumnType::Text
        | ColumnType::Varchar(_)
        | ColumnType::Uuid
        | ColumnType::Json => DataType::Utf8,
        ColumnType::Binary => DataType::Binary,
        ColumnType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ColumnType::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Date => DataType::Date32,
        ColumnType::Native(_) => plain_type(arrived),
        ColumnType::Array(_) => return Err("an array arrives as a list".to_owned()),
    })
}

/// `arrived` without the view, large and dictionary encodings ClickHouse's Arrow reader does not take.
fn plain_type(arrived: &DataType) -> DataType {
    match arrived {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Dictionary(_, value) => plain_type(value),
        other => other.clone(),
    }
}

/// `batch` in the Arrow types ClickHouse reads for `columns`, matched by name, every field nullable. A field not among `columns` is left out for a build table (which holds only the table's own columns) and an error otherwise; a value that does not convert is an error, never a silent NULL.
pub(crate) fn clickhouse_batch(
    batch: &RecordBatch,
    (columns, builds): (&[DesiredColumn], bool),
) -> Result<RecordBatch, String> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let name = field.name();
        let column = match (columns.iter().find(|column| &column.name == name), builds) {
            (Some(column), _) => column,
            (None, true) => continue,
            (None, false) => {
                return Err(format!(
                    "column {name} is not among the table's planned columns"
                ));
            }
        };
        let target = clickhouse_type(&column.column_type, field.data_type()).map_err(|reason| {
            format!(
                "column {name} ({}) cannot load into ClickHouse: {reason}",
                field.data_type()
            )
        })?;
        let cast = match field.data_type() == &target {
            true => Arc::clone(array),
            false => cast_with_options(array, &target, &options).map_err(|error| {
                format!("column {name} cannot be sent to ClickHouse as {target}: {error}")
            })?,
        };
        fields.push(Field::new(name, target, true));
        arrays.push(cast);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}
