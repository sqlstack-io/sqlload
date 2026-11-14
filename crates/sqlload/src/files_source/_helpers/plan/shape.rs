use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, new_null_array};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat;

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::exact_cast::exact_cast;
use crate::files_source::constants::{
    MAX_TEXT_ARRAY_BYTES, NESTED_JSON_SLICE_ROWS, SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN,
    SOURCE_VERSION_COLUMN,
};
use crate::files_source::errors::ShapeError;
use crate::files_source::models::DataFile;
use crate::row_batches::main::json_lists::json_lists;
use crate::row_batches::main::normalise_batch::normalise_batch;
use crate::schema_plan::models::DesiredColumn;

/// The Arrow schema of the batches handed on: one field per output column, at nanoseconds for the `nanos` columns.
pub(crate) fn output_schema(
    columns: &[DesiredColumn],
    nanos: &[String],
) -> Result<SchemaRef, String> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let field = arrow_field(&column.name, &column.column_type)
                .ok_or_else(|| format!("{} has no Arrow representation", column.name))?;
            let nanos_type = match (field.data_type(), nanos.contains(&column.name)) {
                (DataType::Timestamp(_, zone), true) => {
                    Some(DataType::Timestamp(TimeUnit::Nanosecond, zone.clone()))
                }
                (DataType::Time64(_), true) => Some(DataType::Time64(TimeUnit::Nanosecond)),
                _ => None,
            };
            Ok(match nanos_type {
                Some(data_type) => field.with_data_type(data_type),
                None => field,
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// `batch` from `file` reshaped to `schema`: data columns cast, missing ones NULL, partition and file columns filled in, and rows numbered after the file's `rows_before`.
pub(crate) fn shape_batch(
    batch: &RecordBatch,
    file: &DataFile,
    (file_name, rows_before): (&str, u64),
    schema: &SchemaRef,
) -> Result<RecordBatch, ShapeError> {
    let mut constants: Vec<(&str, &str)> = file
        .partitions
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let version = file.file.version();
    constants.push((SOURCE_FILE_COLUMN, file_name));
    constants.push((SOURCE_VERSION_COLUMN, &version));
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            SOURCE_ROW_COLUMN => Ok(row_numbers(rows_before, batch.num_rows())),
            _ => shape_column(batch, &constants, field),
        })
        .collect::<Result<_, ShapeError>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| ShapeError {
        row: None,
        reason: error.to_string(),
    })
}

/// `rows` row numbers after `rows_before`.
fn row_numbers(rows_before: u64, rows: usize) -> ArrayRef {
    let first = i64::try_from(rows_before).unwrap_or(i64::MAX);
    Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|row| first + row as i64 + 1),
    ))
}

/// One output column: a constant (partition value or file name), the file's column cast, or NULLs; a value that does not convert is an error, never NULL.
fn shape_column(
    batch: &RecordBatch,
    constants: &[(&str, &str)],
    field: &Field,
) -> Result<ArrayRef, ShapeError> {
    let rows = batch.num_rows();
    let constant = constants.iter().find(|(key, _)| key == field.name());
    let source: ArrayRef = match (constant, batch.column_by_name(field.name())) {
        (Some((_, value)), _) => Arc::new(StringArray::from(vec![*value; rows])),
        (None, Some(column)) if nested(column.data_type()) && !is_list(field) => {
            json_text(field.name(), column)?
        }
        (None, Some(column)) => column.clone(),
        (None, None) => return Ok(new_null_array(field.data_type(), rows)),
    };
    if is_list(field) && matches!(source.data_type(), DataType::Utf8) {
        return json_lists(&source, field.data_type()).map_err(|reason| ShapeError {
            row: None,
            reason: format!(
                "{}: cannot convert to {}: {reason}",
                field.name(),
                field.data_type()
            ),
        });
    }
    if is_list(field)
        && !matches!(
            source.data_type(),
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(..)
                | DataType::Null
        )
    {
        return Err(ShapeError {
            row: None,
            reason: format!(
                "{}: is {} in the files, which is neither a list nor JSON text, so it does not load as an array",
                field.name(),
                source.data_type()
            ),
        });
    }
    exact_cast(&source, field.data_type()).map_err(|error| ShapeError {
        row: failing_row(&source, field.data_type()),
        reason: format!(
            "{}: cannot convert to {}: {error}",
            field.name(),
            field.data_type()
        ),
    })
}

/// Whether `field` is an `array(...)` column's list.
fn is_list(field: &Field) -> bool {
    matches!(field.data_type(), DataType::List(_))
}

/// Whether a column of `data_type` holds lists, structs or maps, which load as JSON.
fn nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
    )
}

/// Nested column `name` as JSON text: a list as an array, a struct as an object with its fields in order, a map as an object keyed by each key's text. Written a slice of rows at a time, so a batch whose text would pass the 2 GiB one text array holds fails by name instead of overflowing.
fn json_text(name: &str, column: &ArrayRef) -> Result<ArrayRef, ShapeError> {
    let failed = |reason: String| ShapeError {
        row: None,
        reason: format!("{name}: cannot convert to JSON: {reason}"),
    };
    let mut slices: Vec<ArrayRef> = Vec::new();
    let mut bytes: usize = 0;
    let mut start = 0;
    while start < column.len() {
        let rows = NESTED_JSON_SLICE_ROWS.min(column.len() - start);
        let batch = RecordBatch::try_from_iter([(name, column.slice(start, rows))])
            .map_err(|error| failed(error.to_string()))?;
        let text = normalise_batch(&batch, &[])
            .map_err(failed)?
            .column(0)
            .clone();
        bytes += text.as_string::<i32>().values().len();
        if bytes > MAX_TEXT_ARRAY_BYTES {
            return Err(ShapeError {
                row: Some(start),
                reason: format!(
                    "{name}: this batch's values pass 2 GiB as JSON text; leave the column out with exclude_columns"
                ),
            });
        }
        slices.push(text);
        start += rows;
    }
    match slices.as_slice() {
        [] => Ok(new_null_array(&DataType::Utf8, 0)),
        [one] => Ok(one.clone()),
        _ => {
            let parts: Vec<&dyn Array> = slices.iter().map(AsRef::as_ref).collect();
            concat(&parts).map_err(|error| failed(error.to_string()))
        }
    }
}

/// The first value of `source` that does not convert to `data_type`.
fn failing_row(source: &ArrayRef, data_type: &DataType) -> Option<usize> {
    match cast_with_options(source, data_type, &CastOptions::default()) {
        Ok(converted) => {
            (0..source.len()).find(|row| source.is_valid(*row) && converted.is_null(*row))
        }
        Err(_) => None,
    }
}
