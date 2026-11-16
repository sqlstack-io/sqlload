//! Binary values as text.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::column_types::constants::PYTHON_BYTES_METADATA_KEY;

/// `batch` with each binary column built from Python bytes (marked by `columns_batch`) that `target` holds as text turned into base64 text, as dict rows mixing bytes and text write them; other columns, binary from Arrow data included, are unchanged.
pub fn binary_columns_as_text(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| as_text(field, column, target))
        .unzip();
    let schema = Schema::new_with_metadata(fields, schema.metadata().clone());
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(schema), arrays, &options)
        .map_err(|error| error.to_string())
}

/// The column, as base64 text when it is marked as Python bytes and `target` holds it as text.
fn as_text(field: &Field, column: &ArrayRef, target: &SchemaRef) -> (Field, ArrayRef) {
    let wants_text = field.metadata().contains_key(PYTHON_BYTES_METADATA_KEY)
        && target
            .field_with_name(field.name())
            .is_ok_and(|wanted| wanted.data_type() == &DataType::Utf8);
    match binary_text(column).filter(|_| wants_text) {
        Some(text) => (Field::new(field.name(), DataType::Utf8, true), text),
        None => (field.clone(), column.clone()),
    }
}

/// `array`'s values as base64 text when it is binary, the one text every byte string has; `None` for any other array.
fn binary_text(array: &ArrayRef) -> Option<ArrayRef> {
    let encode = |value: Option<&[u8]>| value.map(|bytes| STANDARD.encode(bytes));
    let text: StringArray = match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().iter().map(encode).collect(),
        DataType::LargeBinary => array.as_binary::<i64>().iter().map(encode).collect(),
        DataType::BinaryView => array.as_binary_view().iter().map(encode).collect(),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().iter().map(encode).collect(),
        _ => return None,
    };
    Some(Arc::new(text))
}
