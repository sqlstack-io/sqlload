//! Expand the listed top-level objects of a batch one level into `<object>__<key>` columns.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Field;

use crate::row_batches::_helpers::flatten::{Names, flattened_names, key_columns, rebuilt};

/// `batch` with each column named in `flatten` replaced, where it stands, by one column per key of its objects (`customer` -> `customer__country`, ...): a struct's children keep their types, JSON objects are typed by inference, values that are objects or lists stay `json`, and a NULL or missing object gives NULLs. The schema's metadata lists the columns flattening made. A listed column holding anything but objects, or a new name another column or object already gives, is an error naming the column, the row and the shape.
pub fn flatten_batch(batch: &RecordBatch, flatten: &[String]) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|field| flatten.contains(field.name()))
    {
        return Ok(batch.clone());
    }
    let mut names = Names {
        plain: &schema,
        made: HashMap::new(),
    };
    let mut columns: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        match flatten.contains(field.name()) {
            false => columns.push((field.clone(), array.clone())),
            true => columns.extend(key_columns(field, array, &mut names)?),
        }
    }
    let mut flattened = flattened_names(&schema);
    flattened.extend(names.made.into_keys());
    flattened.sort();
    rebuilt(columns, batch.num_rows(), &flattened)
}
