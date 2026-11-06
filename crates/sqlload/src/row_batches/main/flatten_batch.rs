//! Expand the listed top-level objects of a batch one level into `<object>__<key>` columns.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::main::columns_batch::columns_batch;
use crate::row_batches::main::json_rows::json_rows;
use crate::row_batches::models::{JsonLayout, JsonRead};

/// `batch` with each column named in `flatten` replaced, where it stands, by one column per key of its objects (`customer` -> `customer__country`, ...), typed by inference; values that are objects or lists stay `json`, and a NULL or missing object gives NULLs. A listed column that holds anything but objects, or a new name that is already a column, is an error naming the column, the row and both shapes.
pub fn flatten_batch(batch: &RecordBatch, flatten: &[String]) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(schema.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        match flatten.contains(field.name()) {
            false => {
                fields.push(field.clone());
                arrays.push(array.clone());
            }
            true => {
                let expanded = expand(field, array, batch.num_rows())?;
                for (key_field, key_array) in
                    expanded.schema().fields().iter().zip(expanded.columns())
                {
                    let name = format!("{}{FLATTEN_SEPARATOR}{}", field.name(), key_field.name());
                    if schema.fields().iter().any(|other| *other.name() == name) {
                        return Err(format!(
                            "flatten {}: the key {} gives column {name}, which the data already has; rename one of them or leave {} unflattened",
                            field.name(),
                            key_field.name(),
                            field.name()
                        ));
                    }
                    fields.push(Arc::new(key_field.as_ref().clone().with_name(name)));
                    arrays.push(key_array.clone());
                }
            }
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())
}

/// The listed column's objects as their own batch of `rows` rows, one column per key.
fn expand(field: &Field, array: &ArrayRef, rows: usize) -> Result<RecordBatch, String> {
    let is_json = field
        .metadata()
        .get(ARROW_TYPE_METADATA_KEY)
        .is_some_and(|kind| *kind == ColumnType::Json.to_string());
    let texts = match (array.data_type(), is_json) {
        (DataType::Null, _) => None,
        (DataType::Utf8, true) => Some(array.as_string::<i32>()),
        (data_type, _) => {
            let row = (0..rows)
                .find(|row| array.is_valid(*row))
                .unwrap_or_default();
            return Err(format!(
                "flatten {}: row {} holds a {data_type} value, not an object; the shape changed, so leave {} unflattened or fix the source",
                field.name(),
                row + 1,
                field.name()
            ));
        }
    };
    let mut document = String::new();
    for row in 0..rows {
        let text = texts
            .filter(|texts| texts.is_valid(row))
            .map(|texts| texts.value(row).trim());
        match text {
            None | Some("null") => document.push_str("{}\n"),
            Some(object) if object.starts_with('{') => {
                document.push_str(object);
                document.push('\n');
            }
            Some(other) => {
                let shape = match other.starts_with('[') {
                    true => "a list",
                    false => "a scalar",
                };
                return Err(format!(
                    "flatten {}: row {} holds {shape}, not an object; the shape changed, so leave {} unflattened or fix the source",
                    field.name(),
                    row + 1,
                    field.name()
                ));
            }
        }
    }
    let read = JsonRead {
        layout: JsonLayout::Document,
        path: None,
        batch_rows: rows.max(1),
    };
    let mut gathered = None;
    json_rows(document.as_bytes(), &read, |columns| {
        gathered = Some(columns);
        Ok(())
    })
    .map_err(|error| format!("flatten {}: {error}", field.name()))?;
    match gathered {
        Some(columns) => columns_batch(columns),
        None => Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
    }
}
